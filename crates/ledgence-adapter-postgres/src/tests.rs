//! Real PostgreSQL acceptance tests. Each test owns an isolated database.

use super::*;
use ledgence_worker_api::{
    Digest, Error, ErrorKind, ExecutionContext, ExecutionFailure, ExecutionReport,
    InvocationIdentity, Phase, ProgramDescriptor, ProgramOutcome,
};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, ConnectOptions, postgres::PgConnectOptions};
use std::{collections::HashSet, future::Future, str::FromStr};

pub(crate) struct TestDb {
    pub(crate) store: PostgresStore,
    pub(crate) url: String,
    admin_url: String,
    name: String,
    finished: bool,
}

impl TestDb {
    pub(crate) async fn new() -> Self {
        let fixture = Self::without_migrations().await;
        fixture.store.migrate().await.unwrap();
        fixture
    }

    pub(crate) async fn without_migrations() -> Self {
        let admin_url = std::env::var("LEDGENCE_POSTGRES_URL")
            .expect("set LEDGENCE_POSTGRES_URL to a PostgreSQL 18 test admin database");
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&admin_url)
            .await
            .unwrap();
        let suffix: String = sqlx::query_scalar("SELECT replace(gen_random_uuid()::text,'-','')")
            .fetch_one(&admin)
            .await
            .unwrap();
        let name = format!("ldg_test_{suffix}");
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .unwrap();
        let url = PgConnectOptions::from_str(&admin_url)
            .unwrap()
            .database(&name)
            .to_url_lossy()
            .to_string();
        let store = match PostgresStore::connect(&url, PostgresOptions::default()).await {
            Ok(store) => store,
            Err(error) => {
                sqlx::query(AssertSqlSafe(format!("DROP DATABASE {name} WITH (FORCE)")))
                    .execute(&admin)
                    .await
                    .unwrap();
                panic!("test database connection failed: {error}");
            }
        };
        admin.close().await;
        Self {
            store,
            url,
            admin_url,
            name,
            finished: false,
        }
    }

    pub(crate) async fn finish(mut self) {
        self.store.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&self.admin_url)
            .await
            .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.name
        )))
        .execute(&admin)
        .await
        .unwrap();
        admin.close().await;
        self.finished = true;
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Panic cleanup cannot depend on the test runtime continuing to poll.
        // A separate bounded runtime removes only this fixture's unique DB.
        let admin_url = self.admin_url.clone();
        let name = self.name.clone();
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                runtime.block_on(async {
                    let cleanup = async {
                        if let Ok(admin) = PgPoolOptions::new()
                            .max_connections(1)
                            .acquire_timeout(Duration::from_secs(2))
                            .connect(&admin_url)
                            .await
                        {
                            let _ = sqlx::query(AssertSqlSafe(format!(
                                "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
                            )))
                            .execute(&admin)
                            .await;
                            admin.close().await;
                        }
                    };
                    let _ = tokio::time::timeout(Duration::from_secs(4), cleanup).await;
                });
            }
        })
        .join();
    }
}

pub(crate) fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}

pub(crate) fn command() -> SubmitCommand {
    SubmitCommand {
        idempotency_key: "invoice:1042".into(),
        input: SubmitTask::decode(
            br#"{"tenant_id":"acme","namespace":"billing","queue":"python",
                "program":{"id":"invoice","version":"1.0.0"},
                "data":{"amount":9007199254740993,"details":[null,"left\u0000right",-0.0]},
                "retry_policy":{"max_attempts":3,"retry_delay_ms":0}}"#,
        )
        .unwrap(),
        origin_trace: Some(TraceContext {
            traceparent: "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-01".into(),
            tracestate: Some("vendor=value".into()),
        }),
    }
}

pub(crate) fn descriptor() -> ProgramDescriptor {
    ProgramDescriptor {
        program: command().input.program,
        digest: Digest(format!("sha256:{}", "a".repeat(64))),
        size: 1234,
    }
}

pub(crate) fn assignment(reply: AcquireReply) -> Assignment {
    match reply {
        AcquireReply::Assigned { assignment, .. } => *assignment,
        other => panic!("expected assignment, received {other:?}"),
    }
}

pub(crate) fn acquire_command(
    session: &WorkerSession,
    consumer_id: u32,
    sequence: u64,
) -> AcquireCommand {
    AcquireCommand {
        scope: session.scope.clone(),
        queue: session.queue.clone(),
        worker_session_id: session.id.clone(),
        consumer_id,
        sequence,
    }
}

pub(crate) async fn claimed(store: &PostgresStore) -> (TaskSnapshot, WorkerSession, Assignment) {
    let task = store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = store.open_session(&scope(), "python", 2).await.unwrap();
    let assigned = assignment(
        store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    (task, session, assigned)
}

pub(crate) fn completed(
    assigned: &Assignment,
    quiescence: Quiescence,
    output: Value,
) -> SettleCommand {
    SettleCommand {
        owner: assigned.lease.owner.clone(),
        operation_id: "settle_1".into(),
        report: AttemptReport::Completed(ExecutionReport {
            context: Box::new(execution_context(assigned)),
            process_id: 42,
            reused_process: false,
            outcome: ProgramOutcome::Success { output },
            elapsed_ms: 10,
        }),
        quiescence,
        processing_trace: None,
    }
}

fn execution_context(assigned: &Assignment) -> ExecutionContext {
    ExecutionContext {
        identity: InvocationIdentity::from(&assigned.event),
        program: assigned.descriptor.program.clone(),
        digest: assigned.descriptor.digest.clone(),
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("PostgreSQL test operation stalled")
}

async fn history_count(store: &PostgresStore, task_id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM task_history WHERE task_id=$1")
        .bind(task_id)
        .fetch_one(&store.pool)
        .await
        .unwrap()
}

async fn wait_for_lock_waiter(store: &PostgresStore) {
    bounded(async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid())",
            )
            .fetch_one(&store.pool)
            .await
            .unwrap();
            if waiting {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn wait_until_db_time(store: &PostgresStore, deadline: i64) {
    bounded(async {
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint >= $1",
            )
            .bind(deadline)
            .fetch_one(&store.pool)
            .await
            .unwrap();
            if expired {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_submission_preserves_one_binding_and_scoped_identity() {
    let db = TestDb::new().await;
    let first = command();
    let mut second = first.clone();
    second.origin_trace = None;
    let first_descriptor = descriptor();
    let mut second_descriptor = descriptor();
    second_descriptor.digest = Digest(format!("sha256:{}", "b".repeat(64)));
    let (left, right) = tokio::join!(
        db.store
            .accept_resolved_submission(&first, &first_descriptor),
        db.store
            .accept_resolved_submission(&second, &second_descriptor)
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.task_id, right.task_id);
    assert_eq!(left.descriptor, right.descriptor);
    assert_eq!(left.origin_trace, right.origin_trace);
    assert_eq!(history_count(&db.store, &left.task_id).await, 1);

    second.input.data = json!("conflicting input");
    assert!(matches!(
        db.store
            .accept_resolved_submission(&second, &second_descriptor)
            .await,
        Err(ContractError::Conflict)
    ));
    second.input.namespace = "shipping".into();
    let independent = db
        .store
        .accept_resolved_submission(&second, &second_descriptor)
        .await
        .unwrap();
    assert_ne!(independent.task_id, left.task_id);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_first_cursor_and_competing_consumers_claim_once() {
    let db = TestDb::new().await;
    for number in 0..6 {
        let mut input = command();
        input.idempotency_key = format!("task_{number}");
        db.store
            .accept_resolved_submission(&input, &descriptor())
            .await
            .unwrap();
    }
    let session = db.store.open_session(&scope(), "python", 6).await.unwrap();
    let first_command = acquire_command(&session, 0, 1);
    let (first, replay) = tokio::join!(
        db.store.acquire(&first_command),
        db.store.acquire(&first_command)
    );
    let first = assignment(first.unwrap());
    let replay = assignment(replay.unwrap());
    assert_eq!(first.lease.owner, replay.lease.owner);
    assert_eq!(first.event.value(), replay.event.value());

    let mut tasks = tokio::task::JoinSet::new();
    for consumer in 1..6 {
        let store = db.store.clone();
        let command = acquire_command(&session, consumer, 1);
        tasks.spawn(async move { assignment(store.acquire(&command).await.unwrap()) });
    }
    let mut claimed_ids = HashSet::from([first.lease.owner.task_id]);
    while let Some(result) = bounded(tasks.join_next()).await {
        assert!(claimed_ids.insert(result.unwrap().lease.owner.task_id));
    }
    assert_eq!(claimed_ids.len(), 6);
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 0, 2)).await,
        Err(ContractError::Busy)
    ));
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 6, 1)).await,
        Err(ContractError::InvalidInput(_))
    ));
    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(attempts, 6);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn empty_replay_does_not_consume_later_work_or_reset_sequence() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let request = acquire_command(&session, 0, 1);
    assert!(matches!(
        db.store.acquire(&request).await.unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    db.store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    assert!(matches!(
        db.store.acquire(&request).await.unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_eq!(assigned.event.value()["ldgattemptno"], 1);
    assert!(matches!(
        db.store.acquire(&request).await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 0, 4)).await,
        Err(ContractError::OutOfOrder)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn accepted_unconfirmed_success_survives_reconnect_and_cleanup_replay() {
    let db = TestDb::new().await;
    let (task, session, assigned) = claimed(&db.store).await;
    let renew = RenewCommand {
        owner: assigned.lease.owner.clone(),
        sequence: 1,
        intent: RenewIntent::Dispatch,
    };
    let authority = db.store.renew(&renew).await.unwrap();
    assert!(authority.dispatch_allowed);
    let after_dispatch = history_count(&db.store, &task.task_id).await;
    let replay = db.store.renew(&renew).await.unwrap();
    assert_eq!(replay.expires_at, authority.expires_at);
    assert_eq!(
        history_count(&db.store, &task.task_id).await,
        after_dispatch
    );
    let report = completed(
        &assigned,
        Quiescence::Unconfirmed,
        json!({"invoice":"issued"}),
    );
    let receipt = db.store.settle(&report).await.unwrap();
    assert_eq!(receipt.task_state, TaskState::Active);
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 0, 2)).await,
        Err(ContractError::Busy)
    ));
    db.store.close().await;
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let replay = reopened.settle(&report).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.receipt.accepted_at, receipt.receipt.accepted_at);
    assert_eq!(
        reopened.confirm_quiescence(&report.owner).await.unwrap(),
        TaskState::Succeeded
    );
    let count = history_count(&reopened, &task.task_id).await;
    assert_eq!(
        reopened.confirm_quiescence(&report.owner).await.unwrap(),
        TaskState::Succeeded
    );
    assert_eq!(history_count(&reopened, &task.task_id).await, count);
    assert_eq!(
        reopened.settle(&report).await.unwrap().task_state,
        TaskState::Succeeded
    );
    let events = reopened.history(&scope(), &task.task_id, 0).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        (1..=events.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(
        events.last().unwrap().event.reason,
        TransitionReason::Succeeded
    );
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn retry_keeps_digest_and_old_receipt_does_not_change_new_ownership() {
    let db = TestDb::new().await;
    let (task, session, first) = claimed(&db.store).await;
    let report = SettleCommand {
        owner: first.lease.owner.clone(),
        operation_id: "failed_1".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            context: Box::new(execution_context(&first)),
            phase: Phase::Execution,
            error: Error::new(ErrorKind::Io, "temporary failure"),
            execution_may_have_started: true,
            cleanup_error: None,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    };
    let receipt = db.store.settle(&report).await.unwrap();
    assert_eq!(receipt.task_state, TaskState::Queued);
    let second = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_ne!(first.lease.owner.attempt_id, second.lease.owner.attempt_id);
    assert_eq!(first.descriptor, second.descriptor);
    assert_eq!(second.lease.owner.generation, 2);
    let before: i64 = sqlx::query_scalar("SELECT next_expiry_ms FROM tasks WHERE task_id=$1")
        .bind(&task.task_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    let replay = db.store.settle(&report).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.receipt.accepted_at, receipt.receipt.accepted_at);
    assert_eq!(replay.task_state, TaskState::Active);
    assert_eq!(
        db.store
            .inspect(&scope(), &task.task_id)
            .await
            .unwrap()
            .current_attempt_id
            .as_deref(),
        Some(second.lease.owner.attempt_id.as_str())
    );
    let after: i64 = sqlx::query_scalar("SELECT next_expiry_ms FROM tasks WHERE task_id=$1")
        .bind(&task.task_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    let mut changed = report;
    changed.operation_id = "different_operation".into();
    assert!(matches!(
        db.store.settle(&changed).await,
        Err(ContractError::Conflict)
    ));
    db.finish().await;
}

async fn shorten_lease(
    connection: &mut sqlx::PgConnection,
    assigned: &Assignment,
    offset_ms: i64,
) -> i64 {
    let expiry: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint + $1")
            .bind(offset_ms)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=$2 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .bind(expiry)
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=$2 WHERE task_id=$1")
        .bind(&assigned.lease.owner.task_id)
        .bind(expiry)
        .execute(connection)
        .await
        .unwrap();
    expiry
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn lease_expiry_during_task_lock_wait_rejects_renewal_and_first_settlement() {
    for renewing in [true, false] {
        let db = TestDb::new().await;
        let (task, _, assigned) = claimed(&db.store).await;
        let mut blocker = db.store.pool.begin().await.unwrap();
        sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
            .bind(&task.task_id)
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let expiry = shorten_lease(&mut blocker, &assigned, 250).await;
        let store = db.store.clone();
        let pending = tokio::spawn(async move {
            if renewing {
                store
                    .renew(&RenewCommand {
                        owner: assigned.lease.owner.clone(),
                        sequence: 1,
                        intent: RenewIntent::Dispatch,
                    })
                    .await
                    .map(|_| ())
            } else {
                store
                    .settle(&completed(&assigned, Quiescence::Confirmed, json!("done")))
                    .await
                    .map(|_| ())
            }
        });
        wait_for_lock_waiter(&db.store).await;
        wait_until_db_time(&db.store, expiry).await;
        blocker.commit().await.unwrap();
        assert!(matches!(
            bounded(pending).await.unwrap(),
            Err(ContractError::OwnershipLost)
        ));
        assert_eq!(history_count(&db.store, &task.task_id).await, 2);
        let progress = db.store.expire_batch(1).await.unwrap();
        assert_eq!(progress.expired, 1);
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_racing_confirmed_success_has_one_legal_terminal_outcome() {
    let db = TestDb::new().await;
    let (task, _, assigned) = claimed(&db.store).await;
    let mut blocker = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&task.task_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let cancelled_store = db.store.clone();
    let task_id = task.task_id.clone();
    let cancel = tokio::spawn(async move { cancelled_store.cancel(&scope(), &task_id).await });
    let settled_store = db.store.clone();
    let report = completed(&assigned, Quiescence::Confirmed, json!("observed success"));
    let settle = tokio::spawn(async move { settled_store.settle(&report).await });
    bounded(async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'")
                .fetch_one(&db.store.pool).await.unwrap();
            if waiting >= 2 { break; }
            tokio::task::yield_now().await;
        }
    }).await;
    blocker.commit().await.unwrap();
    bounded(cancel).await.unwrap().unwrap();
    bounded(settle).await.unwrap().unwrap();
    let final_task = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert!(matches!(
        final_task.state,
        TaskState::Succeeded | TaskState::Cancelled
    ));
    let attempt = db
        .store
        .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    assert!(attempt.settlement.is_some());
    assert_eq!(attempt.quiescence, Quiescence::Confirmed);
    let events = db.store.history(&scope(), &task.task_id, 0).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.event.reason,
                TransitionReason::Succeeded | TransitionReason::Cancelled
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event.reason == TransitionReason::ReportAccepted)
            .count(),
        1
    );
    assert_eq!(db.store.expire_batch(100).await.unwrap().expired, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn expiry_scanners_skip_locks_preserve_accepted_success_and_allow_consumer_advance() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 4).await.unwrap();
    let mut assignments = Vec::new();
    for consumer in 0..4 {
        let mut input = command();
        input.idempotency_key = format!("recovery_{consumer}");
        db.store
            .accept_resolved_submission(&input, &descriptor())
            .await
            .unwrap();
        assignments.push(assignment(
            db.store
                .acquire(&acquire_command(&session, consumer, 1))
                .await
                .unwrap(),
        ));
    }
    db.store
        .settle(&completed(
            &assignments[1],
            Quiescence::Unconfirmed,
            json!("done"),
        ))
        .await
        .unwrap();
    for assigned in &assignments {
        let mut tx = db.store.pool.begin().await.unwrap();
        sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
            .bind(&assigned.lease.owner.task_id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        shorten_lease(&mut tx, assigned, -1).await;
        tx.commit().await.unwrap();
    }
    let mut extra = command();
    extra.idempotency_key = "new_work".into();
    db.store
        .accept_resolved_submission(&extra, &descriptor())
        .await
        .unwrap();
    let advanced = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_ne!(
        advanced.lease.owner.task_id,
        assignments[0].lease.owner.task_id
    );
    // The old attempt is still state-active but its authority already expired.
    assert_eq!(
        db.store
            .inspect_attempt(
                &scope(),
                &assignments[0].lease.owner.task_id,
                &assignments[0].lease.owner.attempt_id
            )
            .await
            .unwrap()
            .state,
        AttemptState::Active
    );
    let mut blocker = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&assignments[0].lease.owner.task_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let (left, right) = tokio::join!(db.store.expire_batch(100), db.store.expire_batch(100));
    let (left, right) = (left.unwrap(), right.unwrap());
    assert_eq!(left.expired + right.expired, 3);
    assert!(left.examined <= 100 && right.examined <= 100);
    blocker.rollback().await.unwrap();
    assert_eq!(db.store.expire_batch(1).await.unwrap().expired, 1);
    assert_eq!(db.store.expire_batch(100).await.unwrap().expired, 0);
    assert_eq!(
        db.store
            .inspect(&scope(), &assignments[1].lease.owner.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Succeeded
    );
    for assigned in [&assignments[0], &assignments[2], &assignments[3]] {
        let task = db
            .store
            .inspect(&scope(), &assigned.lease.owner.task_id)
            .await
            .unwrap();
        assert_eq!(task.state, TaskState::Queued);
        assert_eq!(task.descriptor, descriptor());
    }
    assert!(matches!(
        db.store.expire_batch(0).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert!(matches!(
        db.store.expire_batch(101).await,
        Err(ContractError::InvalidInput(_))
    ));
    db.finish().await;
}

async fn install_history_failure(store: &PostgresStore, reason: &'static str) {
    assert!(matches!(
        reason,
        "submitted" | "claimed" | "report_accepted"
    ));
    let sql = format!(
        "CREATE FUNCTION fail_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.reason='{reason}' THEN RAISE EXCEPTION 'injected history failure' USING ERRCODE='P0001'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_history BEFORE INSERT ON task_history FOR EACH ROW EXECUTE FUNCTION fail_history();"
    );
    sqlx::raw_sql(AssertSqlSafe(sql))
        .execute(&store.pool)
        .await
        .unwrap();
}

async fn remove_history_failure(store: &PostgresStore) {
    sqlx::raw_sql("DROP TRIGGER fail_history ON task_history; DROP FUNCTION fail_history();")
        .execute(&store.pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn submission_rolls_back_task_and_history_when_final_write_fails() {
    let db = TestDb::new().await;
    install_history_failure(&db.store, "submitted").await;
    assert!(matches!(
        db.store
            .accept_resolved_submission(&command(), &descriptor())
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(
        db.store
            .lookup_submission(&scope(), &command().idempotency_key)
            .await
            .unwrap()
            .is_none()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_history")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    remove_history_failure(&db.store).await;
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    assert_eq!(history_count(&db.store, &task.task_id).await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn acquisition_rolls_back_cursor_attempt_and_task_when_history_fails() {
    let db = TestDb::new().await;
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    install_history_failure(&db.store, "claimed").await;
    let request = acquire_command(&session, 0, 1);
    assert!(matches!(
        db.store.acquire(&request).await,
        Err(ContractError::Unavailable(_))
    ));
    let loaded = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert_eq!(loaded.state, TaskState::Queued);
    assert_eq!(loaded.attempt_count, 0);
    for query in [
        "SELECT count(*) FROM attempts",
        "SELECT count(*) FROM consumer_cursors",
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    assert_eq!(history_count(&db.store, &task.task_id).await, 1);
    remove_history_failure(&db.store).await;
    let assigned = assignment(db.store.acquire(&request).await.unwrap());
    assert_eq!(assigned.lease.owner.generation, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn settlement_rolls_back_receipt_attempt_and_final_state_when_history_fails() {
    let db = TestDb::new().await;
    let (task, _, assigned) = claimed(&db.store).await;
    install_history_failure(&db.store, "report_accepted").await;
    let report = completed(&assigned, Quiescence::Confirmed, json!("done"));
    assert!(matches!(
        db.store.settle(&report).await,
        Err(ContractError::Unavailable(_))
    ));
    let loaded = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert_eq!(loaded.state, TaskState::Active);
    let attempt = db
        .store
        .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(attempt.state, AttemptState::Active);
    assert!(attempt.settlement.is_none());
    assert_eq!(history_count(&db.store, &task.task_id).await, 2);
    remove_history_failure(&db.store).await;
    let receipt = db.store.settle(&report).await.unwrap();
    assert!(!receipt.already_accepted);
    assert_eq!(receipt.task_state, TaskState::Succeeded);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn payload_bytes_preserve_numbers_nul_and_maximum_contract_sizes() {
    let db = TestDb::new().await;
    let mut input = command();
    input.input.data = serde_json::from_str(
        r#"[9007199254740993,18446744073709551615,-9223372036854775808,1,1.0,0.0,-0.0,"a\u0000b"]"#,
    )
    .unwrap();
    let task = db
        .store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    assert_eq!(
        db.store
            .inspect(&scope(), &task.task_id)
            .await
            .unwrap()
            .input
            .canonical_bytes()
            .unwrap(),
        input.input.canonical_bytes().unwrap()
    );
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let first = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    assert_eq!(
        canonical_json_bytes(&first.event.value()["data"]).unwrap(),
        canonical_json_bytes(&input.input.data).unwrap()
    );
    let report = completed(&first, Quiescence::Confirmed, input.input.data.clone());
    db.store.settle(&report).await.unwrap();
    let stored = db
        .store
        .inspect_attempt(&scope(), &task.task_id, &first.lease.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        canonical_json_bytes(&serde_json::to_value(stored.settlement.unwrap().command).unwrap())
            .unwrap(),
        canonical_json_bytes(&serde_json::to_value(&report).unwrap()).unwrap()
    );

    input.idempotency_key = "maximum_payload".into();
    input.input.data = Value::String("x".repeat(SUBMISSION_DATA_MAX_BYTES - 2));
    let large_task = db
        .store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_eq!(
        serde_json::to_vec(&assigned.event.value()["data"])
            .unwrap()
            .len(),
        SUBMISSION_DATA_MAX_BYTES
    );
    let mut maximum = completed(&assigned, Quiescence::Confirmed, json!(""));
    let overhead = serde_json::to_vec(&maximum).unwrap().len();
    if let AttemptReport::Completed(report) = &mut maximum.report {
        report.outcome = ProgramOutcome::Success {
            output: Value::String("y".repeat(SETTLEMENT_MAX_BYTES - overhead)),
        };
    }
    assert_eq!(
        serde_json::to_vec(&maximum).unwrap().len(),
        SETTLEMENT_MAX_BYTES
    );
    let mut too_large = maximum.clone();
    if let AttemptReport::Completed(report) = &mut too_large.report
        && let ProgramOutcome::Success {
            output: Value::String(output),
        } = &mut report.outcome
    {
        output.push('y');
    }
    assert!(matches!(
        db.store.settle(&too_large).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        db.store
            .inspect(&scope(), &large_task.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Active
    );
    db.store.settle(&maximum).await.unwrap();
    let stored = db
        .store
        .inspect_attempt(
            &scope(),
            &large_task.task_id,
            &assigned.lease.owner.attempt_id,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&stored.settlement.unwrap().command)
            .unwrap()
            .len(),
        SETTLEMENT_MAX_BYTES
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn expired_sessions_cannot_extend_or_recreate_consumers() {
    let db = TestDb::new().await;
    let (_, session, assigned) = claimed(&db.store).await;
    let extended = db.store.extend_session(&session.id).await.unwrap();
    assert!(extended.expires_at >= session.expires_at);
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0 WHERE session_id=$1")
        .bind(&session.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.extend_session(&session.id).await,
        Err(ContractError::SessionExpired)
    ));
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 1, 1)).await,
        Err(ContractError::SessionExpired)
    ));
    assert!(matches!(
        db.store
            .renew(&RenewCommand {
                owner: assigned.lease.owner,
                sequence: 1,
                intent: RenewIntent::KeepAlive
            })
            .await,
        Err(ContractError::SessionExpired)
    ));
    let unknown = WorkerSession {
        id: "never_registered".into(),
        ..session
    };
    assert!(matches!(
        db.store.acquire(&acquire_command(&unknown, 0, 1)).await,
        Err(ContractError::UnknownSession)
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM consumer_cursors")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    db.finish().await;
}

struct MigrationDirectory(std::path::PathBuf);
impl Drop for MigrationDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn migrations_are_repeatable_checksum_checked_and_transactional() {
    let db = TestDb::new().await;
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let (left, right) = tokio::join!(db.store.migrate(), db.store.migrate());
    left.unwrap();
    right.unwrap();
    assert_eq!(
        db.store
            .inspect(&scope(), &task.task_id)
            .await
            .unwrap()
            .task_id,
        task.task_id
    );
    let checksum: Vec<u8> =
        sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version=20260913000000")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    sqlx::query("UPDATE _sqlx_migrations SET checksum=$1 WHERE version=20260913000000")
        .bind(vec![0_u8])
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.migrate().await,
        Err(ContractError::Unavailable(_))
    ));
    sqlx::query("UPDATE _sqlx_migrations SET checksum=$1 WHERE version=20260913000000")
        .bind(checksum)
        .execute(&db.store.pool)
        .await
        .unwrap();

    let directory =
        MigrationDirectory(std::env::temp_dir().join(format!("{}_migrations", db.name)));
    std::fs::create_dir(&directory.0).unwrap();
    for migration in MIGRATOR.iter() {
        std::fs::write(
            directory
                .0
                .join(format!("{}_applied.sql", migration.version)),
            migration.sql.as_str().as_bytes(),
        )
        .unwrap();
    }
    std::fs::write(
        directory.0.join("20260914000000_injected_failure.sql"),
        "CREATE TABLE migration_rollback_probe(id integer); SELECT 1/0;",
    )
    .unwrap();
    let migrator = sqlx::migrate::Migrator::new(directory.0.as_path())
        .await
        .unwrap();
    // SQLx's failed migrator can retain its session advisory lock. Isolate this
    // intentionally failing custom migration just like the adapter's migrator.
    let mut migration_connection = db.store.pool.acquire().await.unwrap();
    migration_connection.close_on_drop();
    let failure = migrator.run(&mut *migration_connection).await.unwrap_err();
    migration_connection.close().await.unwrap();
    assert!(
        matches!(&failure, sqlx::migrate::MigrateError::ExecuteMigration(error, 20260914000000) if error.as_database_error().and_then(|error| error.code()).as_deref() == Some("22012")),
        "unexpected migration error: {failure:?}"
    );
    let table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('migration_rollback_probe')::text")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(table.is_none());
    let migration_count: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(migration_count as usize, MIGRATOR.iter().count());
    db.store.migrate().await.unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn previous_assignment_revocation_during_new_claim_is_not_overwritten() {
    let db = TestDb::new().await;
    let (old_task, session, old_assignment) = claimed(&db.store).await;
    let mut expiry = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&old_task.task_id)
        .fetch_one(&mut *expiry)
        .await
        .unwrap();
    shorten_lease(&mut expiry, &old_assignment, -1).await;
    expiry.commit().await.unwrap();
    let mut next_input = command();
    next_input.idempotency_key = "next_task".into();
    let next_task = db
        .store
        .accept_resolved_submission(&next_input, &descriptor())
        .await
        .unwrap();

    // Pause after the adapter has read the old snapshot and decided the new
    // claim, without locking the old task as part of the test barrier.
    let mut gate = db.store.pool.acquire().await.unwrap();
    gate.close_on_drop();
    sqlx::query("SELECT pg_advisory_lock(42424242)")
        .execute(&mut *gate)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION gate_attempt_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(42424242); RETURN NEW; END $$; CREATE TRIGGER gate_attempt_insert AFTER INSERT ON attempts FOR EACH ROW EXECUTE FUNCTION gate_attempt_insert();",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    let advancing_store = db.store.clone();
    let request = acquire_command(&session, 0, 2);
    let advancing_request = request.clone();
    let advancing = tokio::spawn(async move { advancing_store.acquire(&advancing_request).await });
    bounded(async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.datname=current_database() AND l.locktype='advisory' AND NOT l.granted)",
            )
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(
        bounded(db.store.cancel(&scope(), &old_task.task_id))
            .await
            .unwrap(),
        TaskState::Cancelled
    );
    let cancelled_history = history_count(&db.store, &old_task.task_id).await;
    assert!(!advancing.is_finished());

    sqlx::query("SELECT pg_advisory_unlock(42424242)")
        .execute(&mut *gate)
        .await
        .unwrap();
    gate.close().await.unwrap();
    let assigned = assignment(bounded(advancing).await.unwrap().unwrap());
    assert_eq!(assigned.lease.owner.task_id, next_task.task_id);
    let replay = assignment(db.store.acquire(&request).await.unwrap());
    assert_eq!(replay.lease.owner, assigned.lease.owner);
    assert_eq!(
        db.store
            .inspect(&scope(), &old_task.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Cancelled
    );
    assert_eq!(
        history_count(&db.store, &old_task.task_id).await,
        cancelled_history
    );
    assert_eq!(
        db.store
            .inspect_attempt(
                &scope(),
                &old_task.task_id,
                &old_assignment.lease.owner.attempt_id
            )
            .await
            .unwrap()
            .state,
        AttemptState::Lost
    );
    db.finish().await;
}

mod dispatch;
