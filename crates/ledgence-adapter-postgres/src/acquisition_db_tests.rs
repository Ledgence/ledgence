//! Durable acquisition probes independent of HTTP waiting and advisory hints.
use crate::{
    tests::{TestDb, acquire_command, assignment, command, descriptor, scope},
    *,
};
use std::sync::Mutex;

#[derive(Default)]
struct Hints(Mutex<Vec<AcquisitionHint>>);
impl AcquisitionWake for Hints {
    fn wake(&self, hint: AcquisitionHint) {
        self.0.lock().unwrap().push(hint);
    }
}
impl Hints {
    fn take(&self) -> Vec<AcquisitionHint> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("database acquisition test did not reach its synchronized boundary")
}
fn reply(probe: AcquisitionProbe, expected: AcquisitionCompletion) -> AcquireReply {
    match probe {
        AcquisitionProbe::Completed { reply, kind } => {
            assert_eq!(kind, expected);
            reply
        }
        AcquisitionProbe::Pending { .. } => panic!("expected completion"),
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn pending_rolls_back_every_cursor_and_releases_a_single_connection_pool() {
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
    let session = store.open_session(&scope(), "python", 1).await.unwrap();
    let request = acquire_command(&session, 0, 1);
    let hints = Arc::new(Hints::default());
    store.set_acquisition_wake(hints.clone());
    for _ in 0..3 {
        assert!(
            matches!(store.probe_acquisition(&request,false,deadline()).await.unwrap(), AcquisitionProbe::Pending { session_remaining_ms } if session_remaining_ms>0)
        );
        store.check_connection().await.unwrap();
    }
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM consumer_cursors),(SELECT count(*) FROM attempts),(SELECT count(*) FROM task_history)").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    assert!(hints.take().is_empty());
    assert!(matches!(
        reply(
            store
                .probe_acquisition(&request, true, deadline())
                .await
                .unwrap(),
            AcquisitionCompletion::FinalizedEmpty
        ),
        AcquireReply::Empty { sequence: 1 }
    ));
    assert_eq!(
        hints.take(),
        vec![AcquisitionHint::AcquisitionCompleted((&request).into())]
    );
    store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    hints.take();
    assert!(matches!(
        reply(
            store
                .probe_acquisition(&request, false, deadline())
                .await
                .unwrap(),
            AcquisitionCompletion::Replayed
        ),
        AcquireReply::Empty { sequence: 1 }
    ));
    assert!(hints.take().is_empty());
    let next = acquire_command(&session, 0, 2);
    assert!(matches!(
        reply(
            store
                .probe_acquisition(&next, false, deadline())
                .await
                .unwrap(),
            AcquisitionCompletion::Claimed
        ),
        AcquireReply::Assigned { .. }
    ));
    assert_eq!(
        hints.take(),
        vec![AcquisitionHint::AcquisitionCompleted((&next).into())]
    );
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_first_probe_and_finalizer_allocate_only_one_attempt() {
    let db = TestDb::new().await;
    for number in 0..2 {
        let mut input = command();
        input.idempotency_key = format!("first-probe-{number}");
        db.store
            .accept_resolved_submission(&input, &descriptor())
            .await
            .unwrap();
    }
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let request = acquire_command(&session, 0, 1);
    let (left, right) = tokio::join!(
        db.store.probe_acquisition(&request, false, deadline()),
        db.store.probe_acquisition(&request, true, deadline())
    );
    let AcquisitionProbe::Completed {
        reply: left,
        kind: left_kind,
    } = left.unwrap()
    else {
        panic!("no left completion")
    };
    let AcquisitionProbe::Completed {
        reply: right,
        kind: right_kind,
    } = right.unwrap()
    else {
        panic!("no right completion")
    };
    assert!(matches!(
        (left_kind, right_kind),
        (
            AcquisitionCompletion::Claimed,
            AcquisitionCompletion::Replayed
        ) | (
            AcquisitionCompletion::Replayed,
            AcquisitionCompletion::Claimed
        )
    ));
    assert_eq!(assignment(left).lease.owner, assignment(right).lease.owner);
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM attempts),(SELECT count(*) FROM tasks WHERE state='queued')",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn caller_deadline_during_contention_never_finalizes_empty_or_leaks_locks() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let request = acquire_command(&session, 0, 1);
    db.store.acquire(&request).await.unwrap();
    let mut connection = db.store.pool.acquire().await.unwrap();
    let mut transaction = sqlx::Connection::begin(&mut *connection).await.unwrap();
    sqlx::query("SELECT session_id FROM worker_sessions WHERE session_id=$1 FOR UPDATE")
        .bind(&session.id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let next = acquire_command(&session, 0, 2);
    let started = Instant::now();
    assert!(matches!(
        db.store
            .probe_acquisition(&next, true, started + Duration::from_millis(100))
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
    transaction.rollback().await.unwrap();
    drop(connection);
    assert!(matches!(
        reply(
            db.store
                .probe_acquisition(&request, false, deadline())
                .await
                .unwrap(),
            AcquisitionCompletion::Replayed
        ),
        AcquireReply::Empty { sequence: 1 }
    ));
    assert!(matches!(
        db.store
            .probe_acquisition(&next, false, deadline())
            .await
            .unwrap(),
        AcquisitionProbe::Pending { .. }
    ));
    assert!(matches!(
        db.store
            .probe_acquisition(&next, true, Instant::now())
            .await,
        Err(ContractError::Unavailable(_))
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn only_committed_retry_transitions_emit_queue_wakes() {
    use ledgence_worker_api::{Error, ErrorKind, ExecutionFailure, Phase};
    let db = TestDb::new().await;
    let mut input = command();
    input.input.retry_policy.max_attempts = 4;
    db.store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let hints = Arc::new(Hints::default());
    db.store.set_acquisition_wake(hints.clone());
    for (number, quiescence) in [(1, Quiescence::Confirmed), (2, Quiescence::Unconfirmed)] {
        let assigned = assignment(
            db.store
                .acquire(&acquire_command(&session, 0, number))
                .await
                .unwrap(),
        );
        db.store
            .renew(&RenewCommand {
                owner: assigned.lease.owner.clone(),
                sequence: 1,
                intent: RenewIntent::Dispatch,
            })
            .await
            .unwrap();
        hints.take();
        let mut failed = crate::tests::completed(&assigned, quiescence, serde_json::Value::Null);
        let AttemptReport::Completed(report) = failed.report else {
            unreachable!()
        };
        failed.report = AttemptReport::Failed(ExecutionFailure {
            context: report.context,
            error: Error::new(ErrorKind::Runtime, "controlled retry"),
            cleanup_error: None,
            phase: Phase::Execution,
            execution_may_have_started: true,
        });
        db.store.settle(&failed).await.unwrap();
        if quiescence == Quiescence::Unconfirmed {
            assert!(hints.take().is_empty());
            db.store
                .confirm_quiescence(&assigned.lease.owner)
                .await
                .unwrap();
        }
        assert_eq!(
            hints.take(),
            vec![AcquisitionHint::QueueChanged(AcquisitionQueue {
                scope: scope(),
                queue: "python".into()
            })]
        );
        db.store.settle(&failed).await.unwrap();
        assert!(
            hints.take().is_empty(),
            "receipt replay must not republish a queue change"
        );
    }
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 3))
            .await
            .unwrap(),
    );
    hints.take();
    sqlx::query("UPDATE attempts SET expires_at_ms=0 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=0 WHERE task_id=$1")
        .bind(&assigned.lease.owner.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let progress = db.store.expire_batch(10).await.unwrap();
    assert_eq!(progress.expired, 1);
    assert_eq!(
        hints.take(),
        vec![AcquisitionHint::QueueChanged(AcquisitionQueue {
            scope: scope(),
            queue: "python".into()
        })]
    );
    db.finish().await;
}

mod waits;

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn finalized_empty_wins_arrival_before_commit_and_a_duplicate_claim_probe() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let request = acquire_command(&session, 0, 1);
    sqlx::raw_sql("CREATE FUNCTION pause_empty_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(7469921); RETURN NEW; END $$; CREATE TRIGGER pause_empty_cursor BEFORE UPDATE ON consumer_cursors FOR EACH ROW EXECUTE FUNCTION pause_empty_cursor()")
        .execute(&db.store.pool).await.unwrap();
    let mut gate = db.store.pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(7469921)")
        .execute(&mut *gate)
        .await
        .unwrap();
    let final_store = db.store.clone();
    let final_command = request.clone();
    let finalizer = tokio::spawn(async move {
        final_store
            .probe_acquisition(&final_command, true, deadline())
            .await
    });
    bounded(async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=7469921 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))")
                .fetch_one(&db.store.pool).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await;
    // The finalizer already selected no candidate and owns the uncommitted
    // first cursor. Arriving work cannot revise that chosen Empty result.
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let claim_store = db.store.clone();
    let claim_command = request.clone();
    let duplicate = tokio::spawn(async move {
        claim_store
            .probe_acquisition(&claim_command, false, deadline())
            .await
    });
    bounded(async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO consumer_cursors%')")
                .fetch_one(&db.store.pool).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await;
    assert!(!finalizer.is_finished() && !duplicate.is_finished());
    sqlx::query("SELECT pg_advisory_unlock(7469921)")
        .execute(&mut *gate)
        .await
        .unwrap();
    drop(gate);
    for (operation, expected) in [
        (finalizer, AcquisitionCompletion::FinalizedEmpty),
        (duplicate, AcquisitionCompletion::Replayed),
    ] {
        assert!(matches!(
            reply(bounded(operation).await.unwrap().unwrap(), expected),
            AcquireReply::Empty { sequence: 1 }
        ));
    }
    let snapshot = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert_eq!(snapshot.state, TaskState::Queued);
    assert_eq!(snapshot.attempt_count, 0);
    let counts: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM consumer_cursors WHERE sequence=1),(SELECT count(*) FROM attempts)")
        .fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (1, 0));
    let next = acquire_command(&session, 0, 2);
    assert_eq!(
        assignment(db.store.acquire(&next).await.unwrap())
            .lease
            .owner
            .task_id,
        task.task_id
    );
    db.finish().await;
}
