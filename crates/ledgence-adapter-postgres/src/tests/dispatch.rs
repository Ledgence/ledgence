//! Durable dispatch correctness, using isolated real PostgreSQL databases.

use super::*;
use sqlx::Acquire;

const DESTINATION: &str = "billing-primary";
fn route() -> DispatchRoute {
    DispatchRoute {
        scope: scope(),
        queue: "python".into(),
        destination: DESTINATION.into(),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
async fn external_db() -> TestDb {
    let db = TestDb::new().await;
    db.store.configure_route(&route()).await.unwrap();
    db
}
async fn submit(store: &PostgresStore, key: &str) -> TaskSnapshot {
    let mut input = command();
    input.idempotency_key = key.into();
    store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap()
}
fn reference(task: &TaskSnapshot, generation: u32) -> DispatchRef {
    DispatchRef {
        scope: task.scope(),
        queue: task.input.queue.clone(),
        task_id: task.task_id.clone(),
        generation,
    }
}
fn claim(
    task: &TaskSnapshot,
    generation: u32,
    session: &WorkerSession,
    sequence: u64,
) -> ClaimCommand {
    ClaimCommand {
        acquisition: acquire_command(session, 0, sequence),
        dispatch: reference(task, generation),
    }
}
fn claimed_assignment(reply: ClaimReply) -> Assignment {
    let ClaimDisposition::Claimed { reply } = reply.disposition else {
        panic!("expected own claim")
    };
    assignment(reply)
}
fn completion(lease: &PublicationLease, outcome: PublicationOutcome) -> PublicationCompletion {
    PublicationCompletion {
        dispatch: lease.record.dispatch.clone(),
        publication_id: lease.record.publication_id.clone(),
        lease_token: lease.lease_token.clone(),
        outcome,
    }
}
async fn leases(store: &PostgresStore) -> Vec<PublicationLease> {
    store
        .lease_publications(DESTINATION, 100, deadline())
        .await
        .unwrap()
}
async fn intent_count(store: &PostgresStore) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM dispatch_intents")
        .fetch_one(&store.pool)
        .await
        .unwrap()
}
async fn make_due(store: &PostgresStore) {
    sqlx::query("UPDATE dispatch_intents SET next_publish_at_ms=0,lease_until_ms=CASE WHEN lease_token IS NOT NULL THEN 0 ELSE NULL END")
        .execute(&store.pool).await.unwrap();
}
fn failed(assignment: &Assignment) -> SettleCommand {
    SettleCommand {
        owner: assignment.lease.owner.clone(),
        operation_id: "retry_failure".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            context: Box::new(execution_context(assignment)),
            phase: Phase::Execution,
            error: Error::new(ErrorKind::Io, "retryable failure"),
            execution_may_have_started: true,
            cleanup_error: None,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn external_submission_and_idempotent_replay_commit_one_stable_intent() {
    let db = external_db().await;
    let first = submit(&db.store, "external_1").await;
    let lease = leases(&db.store).await.pop().unwrap();
    let replay = submit(&db.store, "external_1").await;
    assert_eq!(replay.task_id, first.task_id);
    assert_eq!(history_count(&db.store, &first.task_id).await, 1);
    assert_eq!(intent_count(&db.store).await, 1);
    let binding: String =
        sqlx::query_scalar("SELECT dispatch_destination FROM tasks WHERE task_id=$1")
            .bind(&first.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(binding, DESTINATION);
    assert!(
        leases(&db.store).await.is_empty(),
        "submission replay must not reset publication lease"
    );
    let saved: String =
        sqlx::query_scalar("SELECT publication_id FROM dispatch_intents WHERE task_id=$1")
            .bind(&first.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(saved, lease.record.publication_id);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn route_activation_rejects_live_tasks_and_preserves_old_terminal_binding() {
    let db = TestDb::new().await;
    let task = submit(&db.store, "integrated").await;
    assert_eq!(intent_count(&db.store).await, 0);
    assert!(matches!(
        db.store.configure_route(&route()).await,
        Err(ContractError::InvalidInput(_))
    ));
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    assert!(matches!(
        db.store.configure_route(&route()).await,
        Err(ContractError::InvalidInput(_))
    ));
    db.store
        .settle(&completed(&assigned, Quiescence::Confirmed, json!(true)))
        .await
        .unwrap();
    db.store.configure_route(&route()).await.unwrap();
    db.store.configure_route(&route()).await.unwrap();
    let old: Option<String> =
        sqlx::query_scalar("SELECT dispatch_destination FROM tasks WHERE task_id=$1")
            .bind(&task.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(old.is_none());
    submit(&db.store, "external").await;
    assert_eq!(intent_count(&db.store).await, 1);
    let changed = DispatchRoute {
        destination: "other-destination".into(),
        ..route()
    };
    assert!(matches!(
        db.store.configure_route(&changed).await,
        Err(ContractError::Conflict)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn one_destination_cannot_mix_independent_logical_routes() {
    let db = external_db().await;
    for changed in [
        DispatchRoute {
            queue: "other".into(),
            ..route()
        },
        DispatchRoute {
            scope: Scope {
                namespace: "other".into(),
                ..scope()
            },
            ..route()
        },
    ] {
        assert!(matches!(
            db.store.configure_route(&changed).await,
            Err(ContractError::Conflict)
        ));
    }
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dispatch_routes WHERE destination IS NOT NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn submission_rolls_back_task_history_and_intent_together() {
    let db = external_db().await;
    sqlx::raw_sql("CREATE FUNCTION reject_intent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected intent failure' USING ERRCODE='23514'; END $$; CREATE TRIGGER reject_intent BEFORE INSERT ON dispatch_intents FOR EACH ROW EXECUTE FUNCTION reject_intent();")
        .execute(&db.store.pool).await.unwrap();
    assert!(
        db.store
            .accept_resolved_submission(&command(), &descriptor())
            .await
            .is_err()
    );
    let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM tasks),(SELECT count(*) FROM task_history),(SELECT count(*) FROM dispatch_intents)").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    sqlx::query("DROP TRIGGER reject_intent ON dispatch_intents")
        .execute(&db.store.pool)
        .await
        .unwrap();
    submit(&db.store, "recovered").await;
    assert_eq!(intent_count(&db.store).await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn targeted_claim_ignores_queue_order_and_exact_replay_preserves_assignment() {
    let db = external_db().await;
    let first = submit(&db.store, "first").await;
    let target = submit(&db.store, "target").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let command = claim(&target, 1, &session, 1);
    let assigned = claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
    let replay = claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
    assert_eq!(assigned.lease.owner, replay.lease.owner);
    assert_eq!(assigned.event, replay.event);
    assert_eq!(assigned.lease.owner.task_id, target.task_id);
    assert!(
        matches!(
            db.store.acquire(&command.acquisition).await,
            Err(ContractError::Conflict)
        ),
        "a targeted operation cannot replay through integrated acquisition"
    );
    assert_eq!(
        db.store
            .status(&scope(), &first.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Queued
    );
    assert_eq!(intent_count(&db.store).await, 1);
    assert_eq!(history_count(&db.store, &target.task_id).await, 2);
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 0, 2)).await,
        Err(ContractError::Busy)
    ));
    let unused = db.store.open_session(&scope(), "python", 1).await.unwrap();
    assert!(matches!(
        db.store.acquire(&acquire_command(&unused, 0, 1)).await,
        Err(ContractError::ExternalDispatchRequired)
    ));
    let wrong = claim(&first, 1, &session, 1);
    assert!(matches!(
        db.store.claim_dispatch(&wrong).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(
        db.store
            .status(&scope(), &first.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Queued
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_identical_and_competing_claims_create_one_attempt() {
    let db = external_db().await;
    let task = submit(&db.store, "race").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let command = claim(&task, 1, &session, 1);
    let (left, right) = tokio::join!(
        db.store.claim_dispatch(&command),
        db.store.claim_dispatch(&command)
    );
    let left = claimed_assignment(left.unwrap());
    let right = claimed_assignment(right.unwrap());
    assert_eq!(left.lease.owner, right.lease.owner);
    let other = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let duplicate = db
        .store
        .claim_dispatch(&claim(&task, 1, &other, 1))
        .await
        .unwrap();
    assert!(
        matches!(duplicate.disposition, ClaimDisposition::AlreadyHandedOff { attempt } if attempt.attempt_id == left.lease.owner.attempt_id)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let receipts: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_claim_receipts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(receipts, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_distinct_workers_receive_one_authority_and_one_handoff_proof() {
    let db = external_db().await;
    let task = submit(&db.store, "race").await;
    let a = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let b = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let ca = claim(&task, 1, &a, 1);
    let cb = claim(&task, 1, &b, 1);
    let (a, b) = tokio::join!(db.store.claim_dispatch(&ca), db.store.claim_dispatch(&cb));
    let replies = [a.unwrap(), b.unwrap()];
    assert_eq!(
        replies
            .iter()
            .filter(|r| matches!(r.disposition, ClaimDisposition::Claimed { .. }))
            .count(),
        1
    );
    assert_eq!(
        replies
            .iter()
            .filter(|r| matches!(r.disposition, ClaimDisposition::AlreadyHandedOff { .. }))
            .count(),
        1
    );
    assert_eq!(intent_count(&db.store).await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn historical_claim_receipt_survives_cursor_advance_without_returning_new_authority() {
    let db = external_db().await;
    let first = submit(&db.store, "first").await;
    let next = submit(&db.store, "next").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let original = claim(&first, 1, &session, 1);
    let old = claimed_assignment(db.store.claim_dispatch(&original).await.unwrap());
    db.store
        .settle(&completed(&old, Quiescence::Confirmed, json!(true)))
        .await
        .unwrap();
    let current = claimed_assignment(
        db.store
            .claim_dispatch(&claim(&next, 1, &session, 2))
            .await
            .unwrap(),
    );
    let replay = db.store.claim_dispatch(&original).await.unwrap();
    assert!(
        matches!(replay.disposition, ClaimDisposition::Claimed { reply: AcquireReply::OwnershipLost { assignment, sequence: 1 } } if assignment.attempt_id == old.lease.owner.attempt_id)
    );
    let status = db.store.inspect(&scope(), &next.task_id).await.unwrap();
    assert_eq!(
        status.current_attempt_id.as_deref(),
        Some(current.lease.owner.attempt_id.as_str())
    );
    let mut wrong = original;
    wrong.dispatch = reference(&next, 1);
    assert!(matches!(
        db.store.claim_dispatch(&wrong).await,
        Err(ContractError::Conflict)
    ));
    db.finish().await;
}

/// Pause a duplicate after its first receipt lookup misses, then let the primary
/// client complete that operation and any subsequent work before resuming it.
async fn delayed_claim_replay<F, Fut>(
    db: &TestDb,
    original: &ClaimCommand,
    duplicate: &ClaimCommand,
    after_primary: F,
) -> Result<ClaimReply>
where
    F: FnOnce(ClaimReply) -> Fut,
    Fut: Future<Output = ()>,
{
    let replay_store = PostgresStore::connect(
        &db.url,
        PostgresOptions {
            max_connections: 1,
            ..PostgresOptions::default()
        },
    )
    .await
    .unwrap();
    // This fixture owns a single connection, so only the duplicate enters the
    // trigger's advisory-lock barrier. The production store remains unmodified.
    let duplicate_pid: i32 = sqlx::query_scalar(
        "SELECT pg_backend_pid() FROM set_config('ledgence.test_claim_replay','true',false)",
    )
    .fetch_one(&replay_store.pool)
    .await
    .unwrap();
    sqlx::raw_sql("CREATE FUNCTION pause_duplicate_cursor_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('ledgence.test_claim_replay', true) = 'true' THEN PERFORM pg_advisory_xact_lock(48112977); END IF; RETURN NEW; END $$; CREATE TRIGGER pause_duplicate_cursor_insert BEFORE INSERT ON consumer_cursors FOR EACH ROW EXECUTE FUNCTION pause_duplicate_cursor_insert();")
        .execute(&db.store.pool).await.unwrap();
    let mut task_blocker = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&original.dispatch.task_id)
        .fetch_one(&mut *task_blocker)
        .await
        .unwrap();
    let mut duplicate_blocker = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(48112977)")
        .execute(&mut *duplicate_blocker)
        .await
        .unwrap();
    let mut primary = db.store.claim_dispatch(original);
    tokio::select! {
        result = &mut primary => panic!("primary crossed held task lock: {result:?}"),
        _ = wait_for_lock_waiter(&db.store) => {}
    }
    let mut replay = replay_store.claim_dispatch(duplicate);
    tokio::select! {
        result = &mut replay => panic!("duplicate crossed held replay barrier: {result:?}"),
        _ = bounded(async {
            loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='advisory')")
                    .bind(duplicate_pid).fetch_one(&db.store.pool).await.unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }) => {}
    }
    task_blocker.rollback().await.unwrap();
    let primary = bounded(primary).await.unwrap();
    bounded(after_primary(primary)).await;
    duplicate_blocker.rollback().await.unwrap();
    let result = bounded(replay).await;
    replay_store.close().await;
    sqlx::raw_sql("DROP TRIGGER pause_duplicate_cursor_insert ON consumer_cursors; DROP FUNCTION pause_duplicate_cursor_insert();")
        .execute(&db.store.pool).await.unwrap();
    result
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn delayed_nonauthority_claim_replay_survives_ordered_cursor_advance() {
    for mismatched_target in [false, true] {
        let db = external_db().await;
        let terminal = submit(&db.store, "terminal").await;
        let next = submit(&db.store, "next").await;
        db.store.cancel(&scope(), &terminal.task_id).await.unwrap();
        let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
        db.store
            .claim_dispatch(&claim(&terminal, 1, &session, 1))
            .await
            .unwrap();
        let original = claim(&terminal, 1, &session, 2);
        let duplicate = if mismatched_target {
            claim(&next, 1, &session, 2)
        } else {
            original.clone()
        };
        let replay = delayed_claim_replay(&db, &original, &duplicate, |primary| {
            assert!(matches!(
                primary.disposition,
                ClaimDisposition::TerminalOrSuperseded
            ));
            async {
                // The primary issues sequence 3 only after sequence 2 succeeds.
                claimed_assignment(
                    db.store
                        .claim_dispatch(&claim(&next, 1, &session, 3))
                        .await
                        .unwrap(),
                );
            }
        })
        .await;
        if mismatched_target {
            assert!(matches!(replay, Err(ContractError::Conflict)), "{replay:?}");
        } else {
            assert!(
                matches!(
                    replay,
                    Ok(ClaimReply {
                        disposition: ClaimDisposition::TerminalOrSuperseded,
                        ..
                    })
                ),
                "the exact historical receipt must not acquire sequence 3's authority: {replay:?}"
            );
        }
        assert!(matches!(
            db.store
                .claim_dispatch(&original)
                .await
                .unwrap()
                .disposition,
            ClaimDisposition::TerminalOrSuperseded
        ));
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM attempts),(SELECT count(*) FROM dispatch_claim_receipts)",
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert_eq!(counts, (1, 3));
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn delayed_owned_claim_replay_refreshes_authority_and_fences_cursor_advance() {
    for advance_cursor in [false, true] {
        let db = external_db().await;
        let terminal = submit(&db.store, "terminal").await;
        let target = submit(&db.store, "target").await;
        let next = submit(&db.store, "next").await;
        db.store.cancel(&scope(), &terminal.task_id).await.unwrap();
        let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
        db.store
            .claim_dispatch(&claim(&terminal, 1, &session, 1))
            .await
            .unwrap();
        let original = claim(&target, 1, &session, 2);
        let mut first = None;
        let replay = delayed_claim_replay(&db, &original, &original, |primary| async {
            let assigned = claimed_assignment(primary);
            if advance_cursor {
                db.store
                    .settle(&completed(&assigned, Quiescence::Confirmed, json!(true)))
                    .await
                    .unwrap();
                claimed_assignment(
                    db.store
                        .claim_dispatch(&claim(&next, 1, &session, 3))
                        .await
                        .unwrap(),
                );
            } else {
                let mut connection = db.store.pool.acquire().await.unwrap();
                let mut tx = connection.begin().await.unwrap();
                shorten_lease(&mut tx, &assigned, 10_000).await;
                tx.commit().await.unwrap();
            }
            first = Some(assigned);
        })
        .await
        .unwrap();
        let first = first.unwrap();
        if advance_cursor {
            assert!(
                matches!(replay.disposition, ClaimDisposition::Claimed { reply: AcquireReply::OwnershipLost { assignment, sequence: 2 } } if assignment.task_id == target.task_id && assignment.attempt_id == first.lease.owner.attempt_id)
            );
        } else {
            let refreshed = claimed_assignment(replay);
            assert_eq!(refreshed.lease.owner, first.lease.owner);
            assert_eq!(refreshed.event, first.event);
            assert!(refreshed.authority.remaining_ms > 0);
            assert!(refreshed.authority.remaining_ms <= 10_000);
            assert!(refreshed.authority.remaining_ms < first.authority.remaining_ms);
        }
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM attempts),(SELECT count(*) FROM dispatch_claim_receipts)",
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert_eq!(counts, if advance_cursor { (2, 3) } else { (1, 2) });
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn claim_receipt_failure_rolls_back_attempt_cursor_history_and_intent_invalidation() {
    let db = external_db().await;
    let task = submit(&db.store, "rollback").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_claim_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected claim receipt failure' USING ERRCODE='23514'; END $$; CREATE TRIGGER reject_claim_receipt BEFORE INSERT ON dispatch_claim_receipts FOR EACH ROW EXECUTE FUNCTION reject_claim_receipt();")
        .execute(&db.store.pool).await.unwrap();
    let command = claim(&task, 1, &session, 1);
    assert!(db.store.claim_dispatch(&command).await.is_err());
    let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM attempts),(SELECT count(*) FROM consumer_cursors),(SELECT count(*) FROM dispatch_claim_receipts)").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0));
    assert_eq!(intent_count(&db.store).await, 1);
    assert_eq!(history_count(&db.store, &task.task_id).await, 1);
    assert_eq!(
        db.store
            .status(&scope(), &task.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Queued
    );
    sqlx::query("DROP TRIGGER reject_claim_receipt ON dispatch_claim_receipts")
        .execute(&db.store.pool)
        .await
        .unwrap();
    claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn retry_creates_new_generation_and_deferred_claim_requires_durable_future_intent() {
    let db = external_db().await;
    let mut input = command();
    input.input.retry_policy.retry_delay_ms = 60_000;
    let task = db
        .store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    let initial = leases(&db.store).await.pop().unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = claimed_assignment(
        db.store
            .claim_dispatch(&claim(&task, 1, &session, 1))
            .await
            .unwrap(),
    );
    assert_eq!(
        db.store
            .settle(&failed(&assigned))
            .await
            .unwrap()
            .task_state,
        TaskState::Queued
    );
    let retry = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert_eq!(retry.attempt_count, 1);
    assert!(
        leases(&db.store).await.is_empty(),
        "future retry is not publishable early"
    );
    let row: (i64, String) =
        sqlx::query_as("SELECT generation,publication_id FROM dispatch_intents WHERE task_id=$1")
            .bind(&task.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(row.0, 2);
    assert_ne!(row.1, initial.record.publication_id);
    let other = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let old = db
        .store
        .claim_dispatch(&claim(&task, 1, &other, 1))
        .await
        .unwrap();
    assert!(matches!(
        old.disposition,
        ClaimDisposition::TerminalOrSuperseded
    ));
    let future = claim(&task, 2, &other, 2);
    let reply = db.store.claim_dispatch(&future).await.unwrap();
    assert!(
        matches!(reply.disposition, ClaimDisposition::Deferred { available_at } if available_at == retry.available_at)
    );
    sqlx::query("DELETE FROM dispatch_intents WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let missing = claim(&task, 2, &other, 3);
    assert!(matches!(
        db.store.claim_dispatch(&missing).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        db.store
            .status(&scope(), &task.task_id)
            .await
            .unwrap()
            .attempt_count,
        1
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_invalidates_intent_and_stale_publication_never_recreates_it() {
    let db = external_db().await;
    let task = submit(&db.store, "cancelled").await;
    let lease = leases(&db.store).await.pop().unwrap();
    assert_eq!(
        db.store.cancel(&scope(), &task.task_id).await.unwrap(),
        TaskState::Cancelled
    );
    assert_eq!(intent_count(&db.store).await, 0);
    db.store
        .complete_publications(
            &[completion(&lease, PublicationOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(intent_count(&db.store).await, 0);
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    assert!(matches!(
        db.store
            .claim_dispatch(&claim(&task, 1, &session, 1))
            .await
            .unwrap()
            .disposition,
        ClaimDisposition::TerminalOrSuperseded
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn lease_expiry_recovery_reissues_generation_and_fences_old_claim() {
    let db = external_db().await;
    let task = submit(&db.store, "expired").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let command = claim(&task, 1, &session, 1);
    let assigned = claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
    let mut connection = db.store.pool.acquire().await.unwrap();
    let mut tx = connection.begin().await.unwrap();
    shorten_lease(&mut tx, &assigned, -1).await;
    tx.commit().await.unwrap();
    drop(connection);
    assert_eq!(db.store.expire_batch(1).await.unwrap().expired, 1);
    let retry = leases(&db.store).await.pop().unwrap();
    assert_eq!(retry.record.dispatch.generation, 2);
    assert!(matches!(
        db.store.claim_dispatch(&command).await.unwrap().disposition,
        ClaimDisposition::Claimed {
            reply: AcquireReply::OwnershipLost { .. }
        }
    ));
    let next = claimed_assignment(
        db.store
            .claim_dispatch(&claim(&task, 2, &session, 2))
            .await
            .unwrap(),
    );
    assert_eq!(next.lease.owner.generation, 2);
    assert_ne!(next.lease.owner.attempt_id, assigned.lease.owner.attempt_id);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn publication_retry_and_unknown_lease_preserve_identity_but_confirmed_repair_advances_it() {
    let db = external_db().await;
    submit(&db.store, "publication").await;
    let first = leases(&db.store).await.pop().unwrap();
    let clock: (i64, i64) =
        sqlx::query_as("SELECT next_publish_at_ms,lease_until_ms FROM dispatch_intents")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(
        clock.0, clock.1,
        "leased intents must leave the due index range until lease expiry"
    );
    assert!(leases(&db.store).await.is_empty());
    make_due(&db.store).await;
    let unknown = leases(&db.store).await.pop().unwrap();
    assert_eq!(unknown.record, first.record);
    assert_ne!(unknown.lease_token, first.lease_token);
    db.store
        .complete_publications(
            &[completion(&unknown, PublicationOutcome::Retry)],
            deadline(),
        )
        .await
        .unwrap();
    make_due(&db.store).await;
    let retry = leases(&db.store).await.pop().unwrap();
    assert_eq!(retry.record, first.record);
    db.store
        .complete_publications(
            &[completion(&retry, PublicationOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(intent_count(&db.store).await, 1);
    assert!(leases(&db.store).await.is_empty());
    make_due(&db.store).await;
    let repair = leases(&db.store).await.pop().unwrap();
    assert_eq!(repair.record.dispatch, first.record.dispatch);
    assert_ne!(repair.record.publication_id, first.record.publication_id);
    let epoch: String =
        sqlx::query_scalar("SELECT trunc(publication_epoch)::text FROM dispatch_intents")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(epoch, "2");
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn stale_publication_completion_cannot_override_reissued_lease_or_wrong_scope() {
    let db = external_db().await;
    submit(&db.store, "publication").await;
    let original = leases(&db.store).await.pop().unwrap();
    make_due(&db.store).await;
    let current = leases(&db.store).await.pop().unwrap();
    let mut wrong = completion(&current, PublicationOutcome::Confirmed);
    wrong.dispatch.scope.namespace = "wrong".into();
    db.store
        .complete_publications(
            &[completion(&original, PublicationOutcome::Confirmed), wrong],
            deadline(),
        )
        .await
        .unwrap();
    let token: Option<String> = sqlx::query_scalar("SELECT lease_token FROM dispatch_intents")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(token.as_deref(), Some(current.lease_token.as_str()));
    db.store
        .complete_publications(
            &[completion(&current, PublicationOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    let saved: (Option<String>, Option<i64>) =
        sqlx::query_as("SELECT lease_token,last_confirmed_at_ms FROM dispatch_intents")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(saved.0.is_none());
    assert!(saved.1.is_some());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn parallel_publishers_lease_disjoint_bounded_batches() {
    let db = external_db().await;
    for key in ["a", "b", "c", "d"] {
        submit(&db.store, key).await;
    }
    let (left, right) = tokio::join!(
        db.store.lease_publications(DESTINATION, 2, deadline()),
        db.store.lease_publications(DESTINATION, 2, deadline())
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.len(), 2);
    assert_eq!(right.len(), 2);
    let ids: HashSet<_> = left
        .iter()
        .chain(&right)
        .map(|item| &item.record.dispatch.task_id)
        .collect();
    assert_eq!(ids.len(), 4);
    assert!(leases(&db.store).await.is_empty());
    for limit in [0, 101] {
        assert!(matches!(
            db.store
                .lease_publications(DESTINATION, limit, deadline())
                .await,
            Err(ContractError::InvalidInput(_))
        ));
    }
    assert!(
        db.store
            .lease_publications(DESTINATION, 1, Instant::now())
            .await
            .is_err()
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn publication_completion_batch_rolls_back_if_a_later_update_fails() {
    let db = external_db().await;
    submit(&db.store, "a").await;
    submit(&db.store, "b").await;
    let leased = leases(&db.store).await;
    sqlx::raw_sql("CREATE FUNCTION reject_last_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_id=(SELECT max(task_id) FROM dispatch_intents) THEN RAISE EXCEPTION 'injected completion failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_last_completion BEFORE UPDATE ON dispatch_intents FOR EACH ROW EXECUTE FUNCTION reject_last_completion();")
        .execute(&db.store.pool).await.unwrap();
    let completions: Vec<_> = leased
        .iter()
        .map(|lease| completion(lease, PublicationOutcome::Confirmed))
        .collect();
    assert!(
        db.store
            .complete_publications(&completions, deadline())
            .await
            .is_err()
    );
    let still_leased: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_intents WHERE lease_token IS NOT NULL AND last_confirmed_at_ms IS NULL").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(still_leased, 2);
    sqlx::query("DROP TRIGGER reject_last_completion ON dispatch_intents")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store
        .complete_publications(&completions, deadline())
        .await
        .unwrap();
    assert_eq!(intent_count(&db.store).await, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn malformed_or_future_target_does_not_consume_cursor_or_acknowledge_other_work() {
    let db = external_db().await;
    let task = submit(&db.store, "target").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let mut command = claim(&task, 1, &session, 1);
    command.dispatch.task_id = "missing-task".into();
    assert!(matches!(
        db.store.claim_dispatch(&command).await,
        Err(ContractError::NotFound)
    ));
    command.dispatch = reference(&task, 2);
    assert!(matches!(
        db.store.claim_dispatch(&command).await,
        Err(ContractError::InvalidInput(_))
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM consumer_cursors")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    command.dispatch = reference(&task, 1);
    claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn integrated_acquisition_replay_survives_external_route_activation() {
    let db = TestDb::new().await;
    let idle = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let idle_command = acquire_command(&idle, 0, 1);
    assert!(matches!(
        db.store.acquire(&idle_command).await.unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    let task = submit(&db.store, "integrated").await;
    let worker = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let original = acquire_command(&worker, 0, 1);
    let assigned = assignment(db.store.acquire(&original).await.unwrap());
    db.store
        .settle(&completed(&assigned, Quiescence::Confirmed, json!(true)))
        .await
        .unwrap();
    db.store.configure_route(&route()).await.unwrap();
    let external = submit(&db.store, "external_after_activation").await;
    assert!(
        matches!(
            db.store
                .claim_dispatch(&claim(&external, 1, &idle, 1))
                .await,
            Err(ContractError::Conflict)
        ),
        "an integrated operation cannot be rebound to targeted acquisition"
    );
    assert!(matches!(
        db.store.acquire(&idle_command).await.unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    assert!(
        matches!(db.store.acquire(&original).await.unwrap(), AcquireReply::OwnershipLost { sequence: 1, assignment } if assignment.task_id == task.task_id)
    );
    assert!(matches!(
        db.store.acquire(&acquire_command(&worker, 0, 2)).await,
        Err(ContractError::ExternalDispatchRequired)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn missing_claim_receipts_preserve_obsolete_and_current_cross_mode_errors() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    for sequence in [1, 2] {
        assert!(matches!(
            db.store
                .acquire(&acquire_command(&session, 0, sequence))
                .await
                .unwrap(),
            AcquireReply::Empty { sequence: actual } if actual == sequence
        ));
    }
    db.store.configure_route(&route()).await.unwrap();
    let task = submit(&db.store, "external_after_integration").await;
    assert!(matches!(
        db.store.claim_dispatch(&claim(&task, 1, &session, 1)).await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert!(matches!(
        db.store.claim_dispatch(&claim(&task, 1, &session, 2)).await,
        Err(ContractError::Conflict)
    ));
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM attempts),(SELECT count(*) FROM dispatch_claim_receipts)",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0));
    claimed_assignment(
        db.store
            .claim_dispatch(&claim(&task, 1, &session, 3))
            .await
            .unwrap(),
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn stale_publication_after_retry_does_not_change_new_generation_or_repair_clock() {
    let db = external_db().await;
    let task = submit(&db.store, "retry").await;
    let old = leases(&db.store).await.pop().unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = claimed_assignment(
        db.store
            .claim_dispatch(&claim(&task, 1, &session, 1))
            .await
            .unwrap(),
    );
    db.store.settle(&failed(&assigned)).await.unwrap();
    let before: (i64, String, i64) =
        sqlx::query_as("SELECT generation,publication_id,next_publish_at_ms FROM dispatch_intents")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    db.store
        .complete_publications(
            &[completion(&old, PublicationOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    let after: (i64, String, i64) =
        sqlx::query_as("SELECT generation,publication_id,next_publish_at_ms FROM dispatch_intents")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(before.0, 2);
    assert_eq!(before, after);
    assert_eq!(leases(&db.store).await[0].record.dispatch.generation, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancelling_a_contended_targeted_claim_rolls_back_and_retries_same_operation() {
    let db = external_db().await;
    let task = submit(&db.store, "contended").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let command = claim(&task, 1, &session, 1);
    let mut connection = db.store.pool.acquire().await.unwrap();
    let mut lock = connection.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&task.task_id)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let mut pending = db.store.claim_dispatch(&command);
    tokio::select! {
        result = &mut pending => panic!("claim crossed held task lock: {result:?}"),
        _ = wait_for_lock_waiter(&db.store) => {}
    }
    // The boxed future owns the uncommitted consumer placeholder and rollback.
    drop(pending);
    lock.rollback().await.unwrap();
    drop(connection);
    let assigned = claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
    assert_eq!(assigned.lease.owner.task_id, task.task_id);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn handoff_disposition_rechecks_session_expiry_after_waiting_for_target_lock() {
    let db = external_db().await;
    let task = submit(&db.store, "session-expiry").await;
    let owner = db.store.open_session(&scope(), "python", 1).await.unwrap();
    claimed_assignment(
        db.store
            .claim_dispatch(&claim(&task, 1, &owner, 1))
            .await
            .unwrap(),
    );
    let duplicate = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let expires: i64 = sqlx::query_scalar("UPDATE worker_sessions SET expires_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+200 WHERE session_id=$1 RETURNING expires_at_ms")
        .bind(&duplicate.id).fetch_one(&db.store.pool).await.unwrap();
    let mut connection = db.store.pool.acquire().await.unwrap();
    let mut blocker = connection.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&task.task_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let command = claim(&task, 1, &duplicate, 1);
    let operation = db.store.claim_dispatch(&command);
    let (result, ()) = tokio::join!(operation, async {
        wait_for_lock_waiter(&db.store).await;
        wait_until_db_time(&db.store, expires).await;
        blocker.rollback().await.unwrap();
    });
    assert!(
        matches!(result, Err(ContractError::SessionExpired)),
        "expired session returned handoff disposition: {result:?}"
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM dispatch_claim_receipts WHERE session_id=$1")
            .bind(&duplicate.id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
    drop(connection);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn active_lifecycle_and_exact_replays_do_not_issue_intent_maintenance_statements() {
    let db = external_db().await;
    let mut assignments = Vec::new();
    for key in [
        "separate_cleanup",
        "immediate_cleanup",
        "active_cancellation",
    ] {
        let task = submit(&db.store, key).await;
        let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
        let command = claim(&task, 1, &session, 1);
        let assigned = claimed_assignment(db.store.claim_dispatch(&command).await.unwrap());
        assignments.push((command, assigned));
    }
    assert_eq!(intent_count(&db.store).await, 0);
    // Statement-level auditing detects even DELETE/INSERT/UPSERT statements that
    // affect no rows; a row-level trigger would miss the redundant hot-path SQL.
    sqlx::raw_sql("CREATE TABLE intent_statement_audit(operation text NOT NULL); CREATE FUNCTION audit_intent_statement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO intent_statement_audit(operation) VALUES(TG_OP); RETURN NULL; END $$; CREATE TRIGGER audit_intent_statement AFTER INSERT OR UPDATE OR DELETE ON dispatch_intents FOR EACH STATEMENT EXECUTE FUNCTION audit_intent_statement();")
        .execute(&db.store.pool).await.unwrap();
    sqlx::query("DELETE FROM dispatch_intents WHERE false")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let control: i64 = sqlx::query_scalar("SELECT count(*) FROM intent_statement_audit")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        control, 1,
        "audit must observe a zero-row maintenance statement"
    );
    sqlx::query("TRUNCATE intent_statement_audit")
        .execute(&db.store.pool)
        .await
        .unwrap();

    let (original, assigned) = &assignments[0];
    let dispatch = RenewCommand {
        owner: assigned.lease.owner.clone(),
        sequence: 1,
        intent: RenewIntent::Dispatch,
    };
    assert!(db.store.renew(&dispatch).await.unwrap().dispatch_allowed);
    assert!(db.store.renew(&dispatch).await.unwrap().dispatch_allowed);
    assert_eq!(
        claimed_assignment(db.store.claim_dispatch(original).await.unwrap())
            .lease
            .owner,
        assigned.lease.owner
    );
    let report = completed(assigned, Quiescence::Unconfirmed, json!(true));
    assert_eq!(
        db.store.settle(&report).await.unwrap().task_state,
        TaskState::Active
    );
    assert!(db.store.settle(&report).await.unwrap().already_accepted);
    assert_eq!(
        db.store
            .confirm_quiescence(&assigned.lease.owner)
            .await
            .unwrap(),
        TaskState::Succeeded
    );
    assert_eq!(
        db.store
            .confirm_quiescence(&assigned.lease.owner)
            .await
            .unwrap(),
        TaskState::Succeeded
    );
    assert!(db.store.settle(&report).await.unwrap().already_accepted);
    assert!(matches!(
        db.store.claim_dispatch(original).await.unwrap().disposition,
        ClaimDisposition::Claimed {
            reply: AcquireReply::OwnershipLost { .. }
        }
    ));

    let (_, assigned) = &assignments[1];
    let report = completed(assigned, Quiescence::Confirmed, json!(true));
    assert_eq!(
        db.store.settle(&report).await.unwrap().task_state,
        TaskState::Succeeded
    );
    assert!(db.store.settle(&report).await.unwrap().already_accepted);
    let (_, assigned) = &assignments[2];
    assert_eq!(
        db.store
            .cancel(&scope(), &assigned.lease.owner.task_id)
            .await
            .unwrap(),
        TaskState::Active
    );
    let report = completed(assigned, Quiescence::Confirmed, json!(true));
    assert_eq!(
        db.store.settle(&report).await.unwrap().task_state,
        TaskState::Cancelled
    );
    assert_eq!(
        db.store
            .cancel(&scope(), &assigned.lease.owner.task_id)
            .await
            .unwrap(),
        TaskState::Cancelled
    );

    let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM intent_statement_audit")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        calls, 0,
        "active transitions and receipt replays must not even issue intent maintenance SQL"
    );
    assert_eq!(intent_count(&db.store).await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn queued_cancellation_after_a_retry_invalidates_the_new_generation_intent() {
    let db = external_db().await;
    let task = submit(&db.store, "cancel_queued_retry").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = claimed_assignment(
        db.store
            .claim_dispatch(&claim(&task, 1, &session, 1))
            .await
            .unwrap(),
    );
    assert_eq!(
        db.store
            .settle(&failed(&assigned))
            .await
            .unwrap()
            .task_state,
        TaskState::Queued
    );
    let retry = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert_eq!(retry.attempt_count, 1);
    assert!(retry.current_attempt_id.is_none());
    let lease = leases(&db.store).await.pop().unwrap();
    assert_eq!(lease.record.dispatch.generation, 2);
    assert_eq!(
        db.store.cancel(&scope(), &task.task_id).await.unwrap(),
        TaskState::Cancelled
    );
    assert_eq!(intent_count(&db.store).await, 0);
    db.store
        .complete_publications(
            &[completion(&lease, PublicationOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        intent_count(&db.store).await,
        0,
        "stale publication cannot recreate a cancelled retry"
    );
    let history = db.store.history(&scope(), &task.task_id, 0).await.unwrap();
    assert!(
        history
            .iter()
            .any(|entry| entry.event.reason == TransitionReason::Cancelled
                && entry.event.attempt_id.is_none())
    );
    db.finish().await;
}
