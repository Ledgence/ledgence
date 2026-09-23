//! Service waiting with the real store and an observed post-rollback boundary.
use super::*;
use ledgence_orchestration_service::ApplicationService;
use ledgence_worker_api::{PortFuture, ProgramDescriptor, ProgramRef, ProgramStore};
use tokio::sync::mpsc;

struct Programs;
impl ProgramStore for Programs {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async { panic!("acquisition must use the already resolved program") })
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("orchestration acquisition does not download programs") })
    }
}

struct Observed {
    inner: PostgresStore,
    pending: mpsc::Sender<(u32, u64)>,
    completed: Mutex<Vec<(u32, AcquisitionCompletion)>>,
}
impl TaskStore for Observed {
    fn probe_acquisition<'a>(
        &'a self,
        command: &'a AcquireCommand,
        finish_empty: bool,
        deadline: Instant,
    ) -> ContractFuture<'a, AcquisitionProbe> {
        Box::pin(async move {
            let result = self
                .inner
                .probe_acquisition(command, finish_empty, deadline)
                .await?;
            if let AcquisitionProbe::Pending {
                session_remaining_ms,
            } = &result
            {
                // The real transaction has already rolled back. This observation
                // synchronizes test mutations; it does not send a wake hint.
                let _ = self
                    .pending
                    .try_send((command.consumer_id, *session_remaining_ms));
            }
            if let AcquisitionProbe::Completed { kind, .. } = &result {
                self.completed
                    .lock()
                    .unwrap()
                    .push((command.consumer_id, *kind));
            }
            Ok(result)
        })
    }
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        self.inner.open_session(scope, queue, concurrency)
    }
    fn extend_session<'a>(&'a self, id: &'a str) -> ContractFuture<'a, WorkerSession> {
        self.inner.extend_session(id)
    }
    fn lookup_submission<'a>(
        &'a self,
        scope: &'a Scope,
        key: &'a str,
    ) -> ContractFuture<'a, Option<TaskSnapshot>> {
        self.inner.lookup_submission(scope, key)
    }
    fn accept_resolved_submission<'a>(
        &'a self,
        command: &'a SubmitCommand,
        descriptor: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, TaskSnapshot> {
        self.inner.accept_resolved_submission(command, descriptor)
    }
    fn list_tasks<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a TaskListQuery,
    ) -> ContractFuture<'a, TaskPage> {
        self.inner.list_tasks(scope, query)
    }
    fn status<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskStatus> {
        self.inner.status(scope, id)
    }
    fn result<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskResult> {
        self.inner.result(scope, id)
    }
    fn inspect<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        self.inner.inspect(scope, id)
    }
    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task: &'a str,
        attempt: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        self.inner.inspect_attempt(scope, task, attempt)
    }
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
        after: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        self.inner.history(scope, id, after)
    }
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        self.inner.renew(command)
    }
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        self.inner.settle(command)
    }
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        self.inner.confirm_quiescence(owner)
    }
    fn cancel<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskState> {
        self.inner.cancel(scope, id)
    }
}

fn service(store: &PostgresStore) -> (Arc<ApplicationService>, mpsc::Receiver<(u32, u64)>) {
    let (pending, observations) = mpsc::channel(16);
    // Deliberately periodic-only: no local sink or PostgreSQL listener is attached.
    let service = ApplicationService::new(
        Arc::new(Observed {
            inner: store.clone(),
            pending,
            completed: Mutex::new(Vec::new()),
        }),
        Arc::new(Programs),
    );
    (Arc::new(service), observations)
}
fn wait(
    service: Arc<ApplicationService>,
    command: AcquireCommand,
    max_wait: Duration,
) -> tokio::task::JoinHandle<Result<AcquireReply>> {
    let options = AcquireOptions::new(max_wait, Instant::now() + Duration::from_secs(30)).unwrap();
    tokio::spawn(async move { service.acquire(&command, options).await })
}
async fn pending(observations: &mut mpsc::Receiver<(u32, u64)>) -> u64 {
    tokio::time::timeout(Duration::from_secs(5), observations.recv())
        .await
        .expect("probe never reached Pending")
        .expect("wait ended before Pending")
        .1
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn session_expiry_during_service_wait_rejects_later_ready_work() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let (service, mut observations) = service(&db.store);
    let request = wait(
        service.clone(),
        acquire_command(&session, 0, 1),
        Duration::from_secs(10),
    );
    pending(&mut observations).await;
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0 WHERE session_id=$1")
        .bind(&session.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let submitted = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), request)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(ContractError::SessionExpired)));
    assert_eq!(
        db.store
            .inspect(&scope(), &submitted.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Queued
    );
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM attempts),(SELECT count(*) FROM consumer_cursors)",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0));
    assert_eq!(service.acquisition_statistics().waiters, 0);
    drop(service);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn session_extension_seen_during_wait_does_not_restart_its_deadline() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+5000 WHERE session_id=$1").bind(&session.id).execute(&db.store.pool).await.unwrap();
    let (service, mut observations) = service(&db.store);
    let started = Instant::now();
    let request = wait(
        service.clone(),
        acquire_command(&session, 0, 1),
        Duration::from_secs(3),
    );
    assert!(pending(&mut observations).await <= 5_000);
    // Observe a real fallback probe before extending; no sleep establishes order.
    assert!(pending(&mut observations).await <= 5_000);
    db.store.extend_session(&session.id).await.unwrap();
    assert!(
        pending(&mut observations).await > 60_000,
        "a subsequent probe must observe the extension"
    );
    let result = tokio::time::timeout_at((started + Duration::from_millis(3_800)).into(), request)
        .await
        .expect("extension restarted the three-second wait")
        .unwrap()
        .unwrap();
    assert!(matches!(result, AcquireReply::Empty { sequence: 1 }));
    assert!(started.elapsed() >= Duration::from_millis(2_900));
    assert_eq!(service.acquisition_statistics().waiters, 0);
    drop(service);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn locked_candidate_is_claimed_after_release_without_a_wake_hint() {
    let db = TestDb::new().await;
    let submitted = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let mut connection = db.store.pool.acquire().await.unwrap();
    let mut transaction = sqlx::Connection::begin(&mut *connection).await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&submitted.task_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let (service, mut observations) = service(&db.store);
    let request = wait(
        service.clone(),
        acquire_command(&session, 0, 1),
        Duration::from_secs(10),
    );
    pending(&mut observations).await;
    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(attempts, 0);
    transaction.rollback().await.unwrap();
    drop(connection);
    let assigned = assignment(
        tokio::time::timeout(Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    assert_eq!(assigned.lease.owner.task_id, submitted.task_id);
    assert_eq!(service.acquisition_statistics().waiters, 0);
    drop(service);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn delayed_retry_becomes_claimable_without_another_write_or_hint() {
    use ledgence_worker_api::{Error, ErrorKind, ExecutionFailure, Phase};
    let db = TestDb::new().await;
    let mut input = command();
    input.input.retry_policy.retry_delay_ms = 5_000;
    let submitted = db
        .store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let first = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    db.store
        .renew(&RenewCommand {
            owner: first.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    let mut failed =
        crate::tests::completed(&first, Quiescence::Confirmed, serde_json::Value::Null);
    let AttemptReport::Completed(report) = failed.report else {
        unreachable!()
    };
    failed.report = AttemptReport::Failed(ExecutionFailure {
        context: report.context,
        error: Error::new(ErrorKind::Runtime, "retry later"),
        cleanup_error: None,
        phase: Phase::Execution,
        execution_may_have_started: true,
    });
    assert_eq!(
        db.store.settle(&failed).await.unwrap().task_state,
        TaskState::Queued
    );
    let available_at = db
        .store
        .inspect(&scope(), &submitted.task_id)
        .await
        .unwrap()
        .available_at;
    let (service, mut observations) = service(&db.store);
    let request = wait(
        service.clone(),
        acquire_command(&session, 0, 2),
        Duration::from_secs(12),
    );
    pending(&mut observations).await;
    // No mutation or notification occurs from this boundary until acquisition.
    let assigned = assignment(
        tokio::time::timeout(Duration::from_secs(8), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    assert_eq!(assigned.lease.owner.task_id, submitted.task_id);
    assert_eq!(assigned.lease.owner.generation, 2);
    let claimed_at = assigned.attempt_deadline - input.input.attempt_timeout_ms;
    assert!(
        claimed_at >= available_at,
        "fallback must never bypass the database availability time"
    );
    assert_eq!(service.acquisition_statistics().waiters, 0);
    drop(service);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn one_connection_serves_many_waiters_controls_duplicate_completion_and_drain() {
    use std::collections::HashSet;
    let db = TestDb::new().await;
    let store = PostgresStore::connect(
        &db.url,
        PostgresOptions {
            max_connections: 1,
            ..PostgresOptions::default()
        },
    )
    .await
    .unwrap();
    let mut input = command();
    input.input.queue = "control".into();
    let control_task = store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    let control_session = store.open_session(&scope(), "control", 1).await.unwrap();
    let control = assignment(
        store
            .acquire(&acquire_command(&control_session, 0, 1))
            .await
            .unwrap(),
    );
    let session = store.open_session(&scope(), "python", 8).await.unwrap();
    // Record actual cursor UPDATEs, so duplicate replay cannot hide another
    // durable finalization behind the same final cursor row.
    sqlx::raw_sql("CREATE TABLE acquisition_completion_audit(session_id text, consumer_id bigint); CREATE FUNCTION audit_acquisition_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO acquisition_completion_audit VALUES(NEW.session_id,NEW.consumer_id); RETURN NEW; END $$; CREATE TRIGGER audit_acquisition_completion AFTER UPDATE ON consumer_cursors FOR EACH ROW EXECUTE FUNCTION audit_acquisition_completion()")
        .execute(&db.store.pool).await.unwrap();
    let (pending, mut observations) = mpsc::channel(32);
    let observed = Arc::new(Observed {
        inner: store.clone(),
        pending,
        completed: Mutex::new(Vec::new()),
    });
    let service = Arc::new(ApplicationService::new(
        observed.clone(),
        Arc::new(Programs),
    ));
    store.set_acquisition_wake(service.acquisition_wake());
    let mut requests: Vec<_> = (0..8)
        .map(|consumer| {
            wait(
                service.clone(),
                acquire_command(&session, consumer, 1),
                Duration::from_secs(20),
            )
        })
        .collect();
    bounded(async {
        let mut consumers = HashSet::new();
        while consumers.len() < 8 {
            let (consumer, _) = observations
                .recv()
                .await
                .expect("wait ended before Pending");
            consumers.insert(consumer);
        }
    })
    .await;
    assert_eq!(service.acquisition_statistics().waiters, 8);
    assert_eq!(store.pool.size(), 1);
    let counts: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM consumer_cursors WHERE session_id=$1),(SELECT count(*) FROM acquisition_completion_audit WHERE session_id=$1),(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='ledgence' AND state='idle in transaction')")
        .bind(&session.id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    // These calls use the same service and its only lifecycle connection while
    // all eight consumers are parked; the observer pool is used only for audits.
    bounded(async {
        assert_eq!(
            service
                .inspect(&scope(), &control_task.task_id)
                .await
                .unwrap()
                .state,
            TaskState::Active
        );
        assert_eq!(
            service.extend_session(&session.id).await.unwrap().id,
            session.id
        );
        let permission = service
            .renew(&RenewCommand {
                owner: control.lease.owner.clone(),
                sequence: 1,
                intent: RenewIntent::Dispatch,
            })
            .await
            .unwrap();
        assert!(permission.dispatch_allowed);
        let report =
            crate::tests::completed(&control, Quiescence::Confirmed, serde_json::Value::Null);
        assert_eq!(
            service.settle(&report).await.unwrap().task_state,
            TaskState::Succeeded
        );
    })
    .await;
    assert!(requests.iter().all(|request| !request.is_finished()));
    let duplicate = acquire_command(&session, 0, 1);
    let immediate = bounded(service.acquire(
        &duplicate,
        AcquireOptions::for_wait(Duration::ZERO).unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(immediate, AcquireReply::Empty { sequence: 1 }));
    let original = requests.remove(0);
    assert!(matches!(
        bounded(original).await.unwrap().unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    {
        let completed = observed.completed.lock().unwrap();
        assert_eq!(
            completed
                .iter()
                .filter(|(consumer, kind)| *consumer == 0
                    && *kind == AcquisitionCompletion::FinalizedEmpty)
                .count(),
            1
        );
        assert_eq!(
            completed
                .iter()
                .filter(
                    |(consumer, kind)| *consumer == 0 && *kind == AcquisitionCompletion::Replayed
                )
                .count(),
            1
        );
    }
    service.stop_acquisitions();
    for request in requests {
        assert!(matches!(
            bounded(request).await.unwrap().unwrap(),
            AcquireReply::Empty { sequence: 1 }
        ));
    }
    let statistics = service.acquisition_statistics();
    assert_eq!(
        (
            statistics.waiters,
            statistics.keys,
            statistics.queues,
            statistics.nominated,
            statistics.probes
        ),
        (0, 0, 0, 0, 0)
    );
    let finalized: Vec<(i64, i64)> = sqlx::query_as("SELECT consumer_id,count(*) FROM acquisition_completion_audit WHERE session_id=$1 GROUP BY consumer_id ORDER BY consumer_id")
        .bind(&session.id).fetch_all(&db.store.pool).await.unwrap();
    assert_eq!(
        finalized,
        (0..8).map(|consumer| (consumer, 1)).collect::<Vec<_>>()
    );
    let cursors: i64 = sqlx::query_scalar("SELECT count(*) FROM consumer_cursors WHERE session_id=$1 AND sequence=1 AND task_id IS NULL AND attempt_id IS NULL")
        .bind(&session.id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(cursors, 8);
    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        attempts, 1,
        "only the independent control task may have an attempt"
    );
    assert_eq!(store.pool.size(), 1);
    drop(service);
    drop(observed);
    bounded(store.close()).await;
    db.finish().await;
}
