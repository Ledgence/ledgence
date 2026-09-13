//! Real PostgreSQL + application service + loopback HTTP with a deliberately
//! controlled runtime. These prove cleanup/reconciliation composition, not Python
//! process termination or production lease duration. Expiry tests adjust stored
//! deadlines under task locks to reach the durable state boundary deterministically.

use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, completed, descriptor, scope};
use ledgence_adapter_http::{HttpTaskService, server::router};
use ledgence_orchestration_service::ApplicationService;
use ledgence_worker_api::{
    self as worker_api, ArtifactCache, Digest, Error, ErrorKind, ExecutionContext,
    ExecutionFailure, ExecutionRuntime, ExecutionSession, InvocationIdentity, Phase, Platform,
    PortFuture, PreparedArtifact, ProgramDescriptor, ProgramManifest, ProgramOutcome, ProgramRef,
    ProgramStore, PythonRuntime, RunControl, StartOutcome,
};
use ledgence_worker_core::{Worker, WorkerConfig};
use ledgence_worker_delivery::{DeliveryConfig, DeliveryDriver};
use serde_json::json;
use std::{
    collections::HashMap,
    future::IntoFuture,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    sync::{Notify, oneshot},
    task::JoinHandle,
};

const WAIT: Duration = Duration::from_secs(10);

struct Programs;
impl ProgramStore for Programs {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async move {
            assert_eq!(program, &descriptor().program);
            Ok(descriptor())
        })
    }

    fn fetch<'a>(&'a self, requested: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async move {
            assert_eq!(requested, &descriptor());
            Ok(vec![0; requested.size as usize])
        })
    }
}

struct HttpServer {
    client: Arc<HttpTaskService>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl HttpServer {
    async fn new(store: PostgresStore) -> Self {
        let service = Arc::new(ApplicationService::new(Arc::new(store), Arc::new(Programs)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(
            axum::serve(listener, router(service))
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .into_future(),
        );
        Self {
            client: Arc::new(
                HttpTaskService::with_timeout(&format!("http://{address}"), WAIT).unwrap(),
            ),
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn finish(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(WAIT, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Default)]
struct Cache(Mutex<HashMap<Digest, PreparedArtifact>>);
impl ArtifactCache for Cache {
    fn lookup<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
    ) -> PortFuture<'a, Option<PreparedArtifact>> {
        Box::pin(async { Ok(self.0.lock().unwrap().get(&descriptor.digest).cloned()) })
    }

    fn publish<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        Box::pin(async {
            let artifact = PreparedArtifact::new(
                PathBuf::from("/controlled-http-runtime-artifact"),
                ProgramManifest {
                    schema_version: 1,
                    program: descriptor.program.clone(),
                    runtime: PythonRuntime {
                        kind: "python".into(),
                        python: "3.12".into(),
                        protocol: 1,
                    },
                    handler: "program:handle".into(),
                    platform: Platform {
                        os: std::env::consts::OS.into(),
                        arch: std::env::consts::ARCH.into(),
                    },
                },
                descriptor.digest.clone(),
                Arc::new(()),
            );
            self.0
                .lock()
                .unwrap()
                .insert(descriptor.digest.clone(), artifact.clone());
            Ok(artifact)
        })
    }
}

#[derive(Default)]
struct RuntimeState {
    executions: AtomicUsize,
    live: AtomicUsize,
    implicit_drops: AtomicUsize,
    fail_close: AtomicBool,
    slow_closes: AtomicUsize,
    close_entered: Notify,
    close_release: Notify,
}

struct Runtime(Arc<RuntimeState>);
impl ExecutionRuntime for Runtime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            control.check()?;
            self.0.live.fetch_add(1, Ordering::SeqCst);
            Ok(StartOutcome::Ready(Box::new(Session {
                state: self.0.clone(),
                artifact,
                alive: true,
            })))
        })
    }
}

struct Session {
    state: Arc<RuntimeState>,
    artifact: PreparedArtifact,
    alive: bool,
}
impl ExecutionSession for Session {
    fn pid(&self) -> u32 {
        123
    }

    fn execute<'a>(
        &'a mut self,
        invocation: ledgence_worker_api::RuntimeInvocation,
        control: RunControl,
    ) -> PortFuture<'a, ProgramOutcome> {
        let event = invocation.event;
        Box::pin(async move {
            control.check()?;
            assert_eq!(self.artifact.manifest().program, descriptor().program);
            assert_eq!(event.value()["data"], command().input.data);
            self.state.executions.fetch_add(1, Ordering::SeqCst);
            Err(Error::new(
                ErrorKind::Runtime,
                "controlled runtime interruption",
            ))
        })
    }

    fn close(&mut self) -> PortFuture<'_, ()> {
        Box::pin(async {
            if self.state.fail_close.load(Ordering::SeqCst) {
                return Err(Error::new(
                    ErrorKind::Io,
                    "controlled cleanup remains unresolved",
                ));
            }
            self.state.slow_closes.fetch_add(1, Ordering::SeqCst);
            self.state.close_entered.notify_one();
            self.state.close_release.notified().await;
            self.alive = false;
            self.state.live.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if self.alive {
            self.state.implicit_drops.fetch_add(1, Ordering::SeqCst);
            self.state.live.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

async fn settled_attempt(service: &dyn TaskService, task: &TaskSnapshot) -> AttemptSnapshot {
    tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.inspect(&scope(), &task.task_id).await.unwrap();
            if let Some(attempt) = snapshot.current_attempt_id {
                let snapshot = service
                    .inspect_attempt(&scope(), &task.task_id, &attempt)
                    .await
                    .unwrap();
                if snapshot.settlement.is_some() {
                    return snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("controlled runtime did not produce an accepted report")
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18; real HTTP and controlled cleanup runtime"]
async fn http_delivery_retains_slow_cleanup_and_confirms_without_rewriting_report() {
    let db = TestDb::new().await;
    let server = HttpServer::new(db.store.clone()).await;
    let first = server.client.submit(&command()).await.unwrap();
    let mut second_input = command();
    second_input.idempotency_key = "queued_behind_unconfirmed_cleanup".into();
    let second = server.client.submit(&second_input).await.unwrap();
    let state = Arc::new(RuntimeState::default());
    state.fail_close.store(true, Ordering::SeqCst);
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            fetch_timeout: WAIT,
        },
        Arc::new(Programs),
        Arc::new(Cache::default()),
        Arc::new(Runtime(state.clone())),
    )
    .unwrap();
    let mut config = DeliveryConfig::new(scope(), "python");
    config.idle_delay = Duration::from_millis(10);
    config.retry_delay = Duration::from_millis(10);
    config.request_timeout = WAIT;
    let mut handle = DeliveryDriver::new(worker.clone(), server.client.clone(), config)
        .unwrap()
        .start();
    let accepted = settled_attempt(server.client.as_ref(), &first).await;
    let report = &accepted.settlement.as_ref().unwrap().command;
    assert_eq!(report.quiescence, Quiescence::Unconfirmed);
    assert_eq!(accepted.state, AttemptState::Active);
    let original = serde_json::to_vec(report).unwrap();
    state.fail_close.store(false, Ordering::SeqCst);
    tokio::time::timeout(WAIT, state.close_entered.notified())
        .await
        .unwrap();
    for _ in 0..4 {
        assert!(handle.shutdown(Duration::from_millis(40)).await.is_err());
        assert_eq!(worker.stats().await.active_consumers, 1);
        assert_eq!(worker.stats().await.process_slots, 1);
        assert_eq!(state.live.load(Ordering::SeqCst), 1);
        let history = server
            .client
            .history(&scope(), &first.task_id, 0)
            .await
            .unwrap();
        assert!(
            !history
                .iter()
                .any(|entry| entry.event.reason == TransitionReason::CleanupConfirmed)
        );
    }
    assert_eq!(state.slow_closes.load(Ordering::SeqCst), 1);
    assert_eq!(state.executions.load(Ordering::SeqCst), 1);
    state.close_release.notify_one();
    let status = handle.shutdown(WAIT).await.unwrap();
    assert_eq!(status.settled_attempts, 1);
    assert_eq!(status.lost_attempts, 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(state.implicit_drops.load(Ordering::SeqCst), 0);
    assert_eq!(state.slow_closes.load(Ordering::SeqCst), 1);
    let after = server
        .client
        .inspect_attempt(&scope(), &first.task_id, &report.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(after.quiescence, Quiescence::Confirmed);
    assert_eq!(
        serde_json::to_vec(&after.settlement.as_ref().unwrap().command).unwrap(),
        original
    );
    let history = server
        .client
        .history(&scope(), &first.task_id, 0)
        .await
        .unwrap();
    for reason in [
        TransitionReason::ReportAccepted,
        TransitionReason::CleanupConfirmed,
        TransitionReason::RetryScheduled,
    ] {
        assert_eq!(
            history
                .iter()
                .filter(|entry| entry.event.reason == reason)
                .count(),
            1,
            "{reason:?}"
        );
    }
    let waiting = server
        .client
        .inspect(&scope(), &second.task_id)
        .await
        .unwrap();
    assert_eq!(waiting.state, TaskState::Queued);
    assert_eq!(waiting.attempt_count, 0);
    server.finish().await;
    db.finish().await;
}

async fn expire_recorded_lease(store: &PostgresStore, owner: &LeaseOwner) {
    // This is a deterministic state test. Real worker-crash acceptance separately
    // waits for the production sixty-second lease and automatic service scanner.
    let mut transaction = store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&owner.task_id)
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    let expiry: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint - 1")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=$2 WHERE attempt_id=$1")
        .bind(&owner.attempt_id)
        .bind(expiry)
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=$2 WHERE task_id=$1")
        .bind(&owner.task_id)
        .bind(expiry)
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(store.expire_batch(100).await.unwrap().expired, 1);
}

async fn acquire_first(service: &dyn TaskService) -> (TaskSnapshot, WorkerSession, Assignment) {
    let task = service.submit(&command()).await.unwrap();
    let session = service.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        service
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    service
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    (task, session, assigned)
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18; real HTTP with test-controlled stored expiry"]
async fn http_unconfirmed_success_survives_expiry_reconnect_and_receipt_replay() {
    let db = TestDb::new().await;
    let server = HttpServer::new(db.store.clone()).await;
    let (task, _, assigned) = acquire_first(server.client.as_ref()).await;
    let report = completed(
        &assigned,
        Quiescence::Unconfirmed,
        json!({"preserved":true}),
    );
    let accepted = server.client.settle(&report).await.unwrap();
    assert_eq!(accepted.task_state, TaskState::Active);
    expire_recorded_lease(&db.store, &report.owner).await;
    server.finish().await;
    db.store.close().await;
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let server = HttpServer::new(reopened.clone()).await;
    let replay = server.client.settle(&report).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.receipt.accepted_at, accepted.receipt.accepted_at);
    assert_eq!(replay.task_state, TaskState::Succeeded);
    let before = server
        .client
        .inspect_attempt(&scope(), &task.task_id, &report.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(before.state, AttemptState::Succeeded);
    assert_eq!(before.quiescence, Quiescence::Unconfirmed);
    assert!(matches!(
        server.client.confirm_quiescence(&report.owner).await,
        Err(ContractError::OwnershipLost)
    ));
    let after = server
        .client
        .inspect_attempt(&scope(), &task.task_id, &report.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&after.settlement).unwrap(),
        serde_json::to_vec(&before.settlement).unwrap()
    );
    assert_eq!(
        server
            .client
            .inspect(&scope(), &task.task_id)
            .await
            .unwrap()
            .attempt_count,
        1
    );
    server.finish().await;
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18; real HTTP with test-controlled stored expiry"]
async fn http_old_unconfirmed_failure_receipt_and_cleanup_leave_later_attempt_unchanged() {
    let db = TestDb::new().await;
    let server = HttpServer::new(db.store.clone()).await;
    let (task, session, first) = acquire_first(server.client.as_ref()).await;
    let report = SettleCommand {
        owner: first.lease.owner.clone(),
        operation_id: "controlled_failed_attempt".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            context: Box::new(ExecutionContext {
                identity: InvocationIdentity::from(&first.event),
                program: first.descriptor.program.clone(),
                digest: first.descriptor.digest.clone(),
            }),
            error: worker_api::Error::new(ErrorKind::Runtime, "controlled retryable interruption"),
            cleanup_error: Some(worker_api::Error::new(
                ErrorKind::Io,
                "controlled unresolved cleanup",
            )),
            phase: Phase::Execution,
            execution_may_have_started: true,
        }),
        quiescence: Quiescence::Unconfirmed,
        processing_trace: None,
    };
    let accepted = server.client.settle(&report).await.unwrap();
    expire_recorded_lease(&db.store, &report.owner).await;
    let second = assignment(
        server
            .client
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_ne!(first.lease.owner.attempt_id, second.lease.owner.attempt_id);
    assert_eq!(second.lease.owner.generation, 2);
    assert_eq!(second.descriptor, first.descriptor);
    server.finish().await;
    db.store.close().await;
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let server = HttpServer::new(reopened.clone()).await;
    let before = server
        .client
        .inspect_attempt(&scope(), &task.task_id, &second.lease.owner.attempt_id)
        .await
        .unwrap();
    let history = server
        .client
        .history(&scope(), &task.task_id, 0)
        .await
        .unwrap();
    let replay = server.client.settle(&report).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.receipt.accepted_at, accepted.receipt.accepted_at);
    assert_eq!(replay.task_state, TaskState::Active);
    assert!(matches!(
        server.client.confirm_quiescence(&report.owner).await,
        Err(ContractError::OwnershipLost)
    ));
    let after = server
        .client
        .inspect_attempt(&scope(), &task.task_id, &second.lease.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&after).unwrap()
    );
    assert_eq!(
        serde_json::to_vec(&history).unwrap(),
        serde_json::to_vec(
            &server
                .client
                .history(&scope(), &task.task_id, 0)
                .await
                .unwrap()
        )
        .unwrap()
    );
    assert_eq!(
        server
            .client
            .inspect(&scope(), &task.task_id)
            .await
            .unwrap()
            .current_attempt_id
            .as_deref(),
        Some(second.lease.owner.attempt_id.as_str())
    );
    server.finish().await;
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18; real expiry batch with a test-only database failure"]
async fn http_inspection_preserves_partial_expiry_batch_progress_after_database_failure() {
    let db = TestDb::new().await;
    let server = HttpServer::new(db.store.clone()).await;
    let session = server
        .client
        .open_session(&scope(), "python", 3)
        .await
        .unwrap();
    let mut assignments = Vec::new();
    for consumer in 0..3 {
        let mut command = command();
        command.idempotency_key = format!("partial_expiry_{consumer}");
        server.client.submit(&command).await.unwrap();
        assignments.push(assignment(
            server
                .client
                .acquire(&acquire_command(&session, consumer, 1))
                .await
                .unwrap(),
        ));
    }

    // Deterministically order the production shortlist without waiting for real
    // lease duration. The separate-process crash gate covers production timing.
    let mut transaction = db.store.pool.begin().await.unwrap();
    let cutoff: i64 = sqlx::query_scalar(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint - 3000",
    )
    .fetch_one(&mut *transaction)
    .await
    .unwrap();
    for (index, assigned) in assignments.iter().enumerate() {
        sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
            .bind(&assigned.lease.owner.task_id)
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
        let expiry = cutoff + index as i64;
        sqlx::query("UPDATE attempts SET expires_at_ms=$2 WHERE attempt_id=$1")
            .bind(&assigned.lease.owner.attempt_id)
            .bind(expiry)
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query("UPDATE tasks SET next_expiry_ms=$2 WHERE task_id=$1")
            .bind(&assigned.lease.owner.task_id)
            .bind(expiry)
            .execute(&mut *transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();

    // Fail on history insertion, after this candidate's task and attempt writes.
    // The fault lives only in the fixture DB; no production hook or scanner
    // wrapper substitutes for the real expire_batch transaction sequence.
    sqlx::raw_sql(
        "CREATE TABLE controlled_expiry_fault (task_id text PRIMARY KEY);
         CREATE FUNCTION fail_controlled_expiry() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.reason = 'lease_expired' AND EXISTS (
                 SELECT 1 FROM controlled_expiry_fault WHERE task_id = NEW.task_id
             ) THEN
                 RAISE EXCEPTION 'controlled expiry database failure' USING ERRCODE = '08006';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER controlled_expiry_failure BEFORE INSERT ON task_history
             FOR EACH ROW EXECUTE FUNCTION fail_controlled_expiry();",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO controlled_expiry_fault (task_id) VALUES ($1)")
        .bind(&assignments[1].lease.owner.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let mut before = Vec::new();
    for assigned in &assignments {
        before.push(expiry_snapshot(server.client.as_ref(), &assigned.lease.owner).await);
    }

    assert!(matches!(
        db.store.expire_batch(3).await,
        Err(ContractError::Unavailable(_))
    ));
    let first = expiry_snapshot(server.client.as_ref(), &assignments[0].lease.owner).await;
    assert_recovered_expiry(&first);
    for index in 1..3 {
        let after = expiry_snapshot(server.client.as_ref(), &assignments[index].lease.owner).await;
        assert_eq!(
            serde_json::to_vec(&after).unwrap(),
            serde_json::to_vec(&before[index]).unwrap(),
            "the failed candidate rolls back atomically and the later candidate is untouched"
        );
    }
    server.finish().await;
    db.store.close().await;

    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let server = HttpServer::new(reopened.clone()).await;
    assert_eq!(
        serde_json::to_vec(
            &expiry_snapshot(server.client.as_ref(), &assignments[0].lease.owner).await
        )
        .unwrap(),
        serde_json::to_vec(&first).unwrap(),
        "the first candidate committed despite the batch returning unavailable"
    );
    sqlx::raw_sql(
        "DROP TRIGGER controlled_expiry_failure ON task_history;
         DROP FUNCTION fail_controlled_expiry();
         DROP TABLE controlled_expiry_fault;",
    )
    .execute(&reopened.pool)
    .await
    .unwrap();
    let recovered = reopened.expire_batch(3).await.unwrap();
    assert_eq!((recovered.examined, recovered.expired), (2, 2));
    let empty = reopened.expire_batch(3).await.unwrap();
    assert_eq!((empty.examined, empty.expired), (0, 0));
    assert_eq!(
        serde_json::to_vec(
            &expiry_snapshot(server.client.as_ref(), &assignments[0].lease.owner).await
        )
        .unwrap(),
        serde_json::to_vec(&first).unwrap(),
        "retrying the failed batch must not duplicate committed history"
    );
    for assigned in &assignments[1..] {
        assert_recovered_expiry(
            &expiry_snapshot(server.client.as_ref(), &assigned.lease.owner).await,
        );
    }
    server.finish().await;
    reopened.close().await;
    db.finish().await;
}

async fn expiry_snapshot(
    service: &dyn TaskService,
    owner: &LeaseOwner,
) -> (TaskSnapshot, AttemptSnapshot, Vec<RecordedHistoryEvent>) {
    (
        service.inspect(&owner.scope, &owner.task_id).await.unwrap(),
        service
            .inspect_attempt(&owner.scope, &owner.task_id, &owner.attempt_id)
            .await
            .unwrap(),
        service
            .history(&owner.scope, &owner.task_id, 0)
            .await
            .unwrap(),
    )
}

fn assert_recovered_expiry(
    (task, attempt, history): &(TaskSnapshot, AttemptSnapshot, Vec<RecordedHistoryEvent>),
) {
    assert_eq!(task.state, TaskState::Queued);
    assert_eq!(task.attempt_count, 1);
    assert_eq!(attempt.state, AttemptState::Lost);
    assert_eq!(task.descriptor, attempt.descriptor);
    for reason in [
        TransitionReason::LeaseExpired,
        TransitionReason::RetryScheduled,
    ] {
        assert_eq!(
            history
                .iter()
                .filter(|row| row.event.reason == reason)
                .count(),
            1,
            "{reason:?}"
        );
    }
    assert_eq!(
        history.iter().map(|row| row.sequence).collect::<Vec<_>>(),
        (1..=history.len() as u64).collect::<Vec<_>>()
    );
}
