//! Durable subscriptions and delivery recovery against real PostgreSQL 18.
use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, completed, descriptor, scope};
use serde_json::json;

fn destination() -> CompletionDestination {
    CompletionDestination {
        scope: scope(),
        destination: "accounting".into(),
        binding: "http://127.0.0.1:9000/completed".into(),
    }
}
fn subscription_command(target: CompletionTarget) -> CompletionSubscribeCommand {
    CompletionSubscribeCommand {
        scope: scope(),
        target,
        destination: destination().destination,
        idempotency_key: "receipt".into(),
    }
}
async fn task(store: &PostgresStore, key: &str) -> TaskSnapshot {
    let mut input = command();
    input.idempotency_key = key.into();
    store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap()
}
async fn subscribe_task(store: &PostgresStore, id: &str) -> CompletionSubscription {
    store
        .subscribe_completion(&subscription_command(CompletionTarget::Task {
            id: id.into(),
        }))
        .await
        .unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn lease(store: &PostgresStore) -> Vec<CompletionLease> {
    store
        .lease_completions(&destination(), MAX_COMPLETION_BATCH, deadline())
        .await
        .unwrap()
}
async fn status(store: &PostgresStore, id: &str) -> CompletionSubscription {
    store.completion_status(&scope(), id).await.unwrap()
}
async fn force_due(store: &PostgresStore, id: &str) {
    sqlx::query("UPDATE completion_subscriptions SET next_attempt_at_ms=CASE WHEN next_attempt_at_ms IS NOT NULL THEN created_at_ms ELSE NULL END,lease_until_ms=CASE WHEN lease_until_ms IS NOT NULL THEN created_at_ms ELSE NULL END WHERE subscription_id=$1")
        .bind(id).execute(&store.pool).await.unwrap();
}
fn outcome(
    lease: &CompletionLease,
    outcome: CompletionDeliveryOutcome,
) -> CompletionDeliveryResult {
    CompletionDeliveryResult {
        subscription_id: lease.subscription.subscription_id.clone(),
        generation: lease.subscription.generation,
        lease_token: lease.lease_token.clone(),
        outcome,
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn registration_replays_per_target_and_preserves_immutable_destination_and_event() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let mut changed = destination();
    changed.binding.push_str("/changed");
    assert!(matches!(
        db.store.configure_completion_destination(&changed).await,
        Err(ContractError::Conflict)
    ));
    let first = task(&db.store, "first").await;
    let second = task(&db.store, "second").await;
    let accepted = subscribe_task(&db.store, &first.task_id).await;
    assert_eq!(accepted.state, CompletionState::Waiting);
    assert_eq!(subscribe_task(&db.store, &first.task_id).await, accepted);
    assert_ne!(
        subscribe_task(&db.store, &second.task_id)
            .await
            .subscription_id,
        accepted.subscription_id
    );
    let mut conflict = accepted.command.clone();
    conflict.destination = "changed".into();
    assert!(matches!(
        db.store.subscribe_completion(&conflict).await,
        Err(ContractError::Conflict)
    ));
    let mut wrong_scope = scope();
    wrong_scope.namespace = "other".into();
    assert!(matches!(
        db.store
            .completion_status(&wrong_scope, &accepted.subscription_id)
            .await,
        Err(ContractError::NotFound)
    ));
    db.store.cancel(&scope(), &first.task_id).await.unwrap();
    let pending = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(pending.state, CompletionState::Pending);
    let mut late = accepted.command.clone();
    late.idempotency_key = "late".into();
    let late = db.store.subscribe_completion(&late).await.unwrap();
    assert_eq!(late.event, pending.event);
    assert!(late.created_at >= pending.created_at);
    let event = late.event.unwrap();
    assert_eq!(event.value()["ldgstate"], "cancelled");
    assert!(event.value().get("ldgattemptid").is_none());
    let bytes: Vec<Vec<u8>> = sqlx::query_scalar("SELECT event_bytes FROM completion_subscriptions WHERE task_id=$1 ORDER BY subscription_id")
        .bind(&first.task_id).fetch_all(&db.store.pool).await.unwrap();
    assert_eq!(bytes.len(), 2);
    assert_eq!(bytes[0], bytes[1]);
    let mut mismatch = destination();
    mismatch.binding.push('x');
    assert!(matches!(
        db.store.lease_completions(&mismatch, 1, deadline()).await,
        Err(ContractError::Conflict)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn subscription_cap_is_serialized_but_replays_still_succeed() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let target = task(&db.store, "cap").await;
    let original = subscribe_task(&db.store, &target.task_id).await;
    let mut jobs = Vec::new();
    for n in 0..20 {
        let store = db.store.clone();
        let mut input = original.command.clone();
        input.idempotency_key = format!("subscription-{n}");
        jobs.push(tokio::spawn(async move {
            store.subscribe_completion(&input).await
        }));
    }
    let mut accepted = 1;
    for job in jobs {
        match job.await.unwrap() {
            Ok(_) => accepted += 1,
            Err(ContractError::InvalidInput(_)) => {}
            other => panic!("unexpected cap result: {other:?}"),
        }
    }
    assert_eq!(accepted, MAX_COMPLETION_SUBSCRIPTIONS);
    assert_eq!(subscribe_task(&db.store, &target.task_id).await, original);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_registration_and_completion_cannot_lose_obligation() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    for n in 0..12 {
        let target = task(&db.store, &format!("race-{n}")).await;
        let a = db.store.clone();
        let b = db.store.clone();
        let first_id = target.task_id.clone();
        let second_id = target.task_id.clone();
        let registered = tokio::spawn(async move { subscribe_task(&a, &first_id).await });
        let cancelled = tokio::spawn(async move { b.cancel(&scope(), &second_id).await.unwrap() });
        let accepted = registered.await.unwrap();
        cancelled.await.unwrap();
        let final_state = status(&db.store, &accepted.subscription_id).await;
        assert_eq!(final_state.state, CompletionState::Pending);
        assert_eq!(final_state.event.unwrap().value()["ldgstate"], "cancelled");
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn report_acceptance_waits_for_quiescence_and_atomic_notification_failure_rolls_back() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let (target, _, assigned) = crate::tests::claimed(&db.store).await;
    let accepted = subscribe_task(&db.store, &target.task_id).await;
    let report = completed(&assigned, Quiescence::Unconfirmed, json!({"value": 7}));
    assert_eq!(
        db.store.settle(&report).await.unwrap().task_state,
        TaskState::Active
    );
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    sqlx::raw_sql("CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected notification failure'; END $$; CREATE TRIGGER reject_completion BEFORE UPDATE ON completion_subscriptions FOR EACH ROW EXECUTE FUNCTION reject_completion();")
        .execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.confirm_quiescence(&assigned.lease.owner).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        db.store
            .status(&scope(), &target.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Active
    );
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    sqlx::query("DROP TRIGGER reject_completion ON completion_subscriptions")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .confirm_quiescence(&assigned.lease.owner)
            .await
            .unwrap(),
        TaskState::Succeeded
    );
    let ready = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(ready.state, CompletionState::Pending);
    assert!(db.store.settle(&report).await.unwrap().already_accepted);
    db.store
        .confirm_quiescence(&assigned.lease.owner)
        .await
        .unwrap();
    assert_eq!(status(&db.store, &accepted.subscription_id).await, ready);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn expired_lease_recovery_fences_stale_outcomes_and_exhausts_final_uncertain_send() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let target = task(&db.store, "recovery").await;
    let accepted = subscribe_task(&db.store, &target.task_id).await;
    db.store.cancel(&scope(), &target.task_id).await.unwrap();
    let first = lease(&db.store).await.remove(0);
    first.validate().unwrap();
    assert!(lease(&db.store).await.is_empty());
    force_due(&db.store, &accepted.subscription_id).await;
    let second = lease(&db.store).await.remove(0);
    assert_ne!(first.lease_token, second.lease_token);
    assert_eq!(first.event_bytes, second.event_bytes);
    db.store
        .complete_deliveries(
            &[outcome(&first, CompletionDeliveryOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.attempts,
        2
    );
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Delivering
    );
    for expected in 3..=COMPLETION_MAX_ATTEMPTS {
        force_due(&db.store, &accepted.subscription_id).await;
        let current = lease(&db.store).await.remove(0);
        assert_eq!(current.subscription.attempts, expected);
        assert_eq!(first.event_bytes, current.event_bytes);
    }
    force_due(&db.store, &accepted.subscription_id).await;
    assert!(lease(&db.store).await.is_empty());
    let exhausted = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(exhausted.state, CompletionState::Exhausted);
    assert_eq!(exhausted.total_attempts, 8);
    let request = CompletionRetryCommand {
        scope: scope(),
        subscription_id: accepted.subscription_id.clone(),
        expected_generation: 1,
    };
    let rearmed = db.store.retry_completion(&request).await.unwrap();
    assert_eq!(rearmed.generation, 2);
    assert_eq!(rearmed.attempts, 0);
    assert_eq!(rearmed.total_attempts, 8);
    assert_eq!(db.store.retry_completion(&request).await.unwrap(), rearmed);
    let sent = lease(&db.store).await.remove(0);
    db.store
        .complete_deliveries(
            &[outcome(&sent, CompletionDeliveryOutcome::Confirmed)],
            deadline(),
        )
        .await
        .unwrap();
    let delivered = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(delivered.state, CompletionState::Delivered);
    assert_eq!(
        db.store.retry_completion(&request).await.unwrap(),
        delivered
    );
    let invalid = CompletionRetryCommand {
        expected_generation: 2,
        ..request
    };
    assert!(matches!(
        db.store.retry_completion(&invalid).await,
        Err(ContractError::Conflict)
    ));
    assert!(lease(&db.store).await.is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn partial_batch_results_retries_and_reopen_preserve_independent_delivery_state() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let mut subscribed = Vec::new();
    for n in 0..4 {
        let target = task(&db.store, &format!("batch-{n}")).await;
        subscribed.push(subscribe_task(&db.store, &target.task_id).await);
        db.store.cancel(&scope(), &target.task_id).await.unwrap();
    }
    let leases = lease(&db.store).await;
    assert_eq!(leases.len(), 4);
    let retry = outcome(
        &leases[1],
        CompletionDeliveryOutcome::Retry {
            reason: "http_503".into(),
            retry_after_ms: Some(120_000),
        },
    );
    db.store
        .complete_deliveries(
            &[
                outcome(&leases[0], CompletionDeliveryOutcome::Confirmed),
                retry,
            ],
            deadline(),
        )
        .await
        .unwrap();
    let first = status(&db.store, &leases[0].subscription.subscription_id).await;
    let second = status(&db.store, &leases[1].subscription.subscription_id).await;
    assert_eq!(first.state, CompletionState::Delivered);
    assert_eq!(second.state, CompletionState::Retrying);
    assert!(second.next_attempt_at.unwrap() >= second.created_at + 120_000);
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    assert_eq!(status(&reopened, &first.subscription_id).await, first);
    assert_eq!(status(&reopened, &second.subscription_id).await, second);
    assert!(lease(&reopened).await.is_empty());
    for item in leases.iter().skip(2) {
        force_due(&db.store, &item.subscription.subscription_id).await;
    }
    let recovered = lease(&reopened).await;
    assert_eq!(recovered.len(), 2);
    for item in recovered {
        assert_eq!(item.subscription.attempts, 2);
    }
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn workflow_controller_completion_is_not_workflow_completion_and_cancel_drains() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let run = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let accepted = db
        .store
        .subscribe_completion(&subscription_command(CompletionTarget::Workflow {
            id: run.workflow_id.clone(),
        }))
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let decision = WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision: 0,
        action: WorkflowAction::Complete {
            output: json!({"done": true}),
        },
    };
    db.store
        .settle(&completed(
            &assigned,
            Quiescence::Confirmed,
            serde_json::to_value(decision).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    let work = db.store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 1);
    db.store.apply_work(&work[0], &[]).await.unwrap();
    let final_state = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(final_state.state, CompletionState::Pending);
    assert_eq!(
        final_state.event.unwrap().value()["type"],
        "com.ledgence.workflow.completed.v1"
    );
    let mut next = command();
    next.idempotency_key = "cancel-workflow".into();
    let run = db
        .store
        .accept_resolved_workflow(&next, &descriptor())
        .await
        .unwrap();
    let accepted = db
        .store
        .subscribe_completion(&subscription_command(CompletionTarget::Workflow {
            id: run.workflow_id.clone(),
        }))
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    db.store
        .cancel_workflow(&scope(), &run.workflow_id)
        .await
        .unwrap();
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    let work = db.store.claim_work(16).await.unwrap();
    for item in work {
        db.store.apply_work(&item, &[]).await.unwrap();
    }
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    db.store
        .settle(&completed(
            &assigned,
            Quiescence::Confirmed,
            serde_json::Value::Null,
        ))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_work SET available_at_ms=created_at_ms WHERE processed_at_ms IS NULL",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    for _ in 0..3 {
        let work = db.store.claim_work(16).await.unwrap();
        for item in work {
            db.store.apply_work(&item, &[]).await.unwrap();
        }
    }
    let final_state = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(final_state.state, CompletionState::Pending);
    assert_eq!(final_state.event.unwrap().value()["ldgstate"], "cancelled");
    db.finish().await;
}

async fn wait_for_target_locks(store: &PostgresStore, expected: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND wait_event_type='Lock' AND query LIKE 'SELECT * FROM tasks%FOR NO KEY UPDATE'")
                .fetch_one(&store.pool).await.unwrap();
            if waiting >= expected { return; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("target operations did not reach deterministic lock barrier");
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn subscription_completion_race_is_correct_in_both_serialized_lock_orders() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    for registration_first in [true, false] {
        let target = task(
            &db.store,
            if registration_first {
                "register-first"
            } else {
                "terminal-first"
            },
        )
        .await;
        let mut barrier = db.store.pool.begin().await.unwrap();
        sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
            .bind(&target.task_id)
            .fetch_one(&mut *barrier)
            .await
            .unwrap();
        let register_store = db.store.clone();
        let cancel_store = db.store.clone();
        let register_id = target.task_id.clone();
        let cancel_id = target.task_id.clone();
        let register = async move { subscribe_task(&register_store, &register_id).await };
        let cancel = async move { cancel_store.cancel(&scope(), &cancel_id).await.unwrap() };
        let (registered, cancelled) = if registration_first {
            let registered = tokio::spawn(register);
            wait_for_target_locks(&db.store, 1).await;
            let cancelled = tokio::spawn(cancel);
            (registered, cancelled)
        } else {
            let cancelled = tokio::spawn(cancel);
            wait_for_target_locks(&db.store, 1).await;
            let registered = tokio::spawn(register);
            (registered, cancelled)
        };
        wait_for_target_locks(&db.store, 2).await;
        barrier.commit().await.unwrap();
        let accepted = registered.await.unwrap();
        cancelled.await.unwrap();
        assert_eq!(
            accepted.state,
            if registration_first {
                CompletionState::Waiting
            } else {
                CompletionState::Pending
            }
        );
        assert_eq!(
            status(&db.store, &accepted.subscription_id).await.state,
            CompletionState::Pending
        );
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn retryable_execution_failures_only_notify_after_retry_policy_exhaustion() {
    use ledgence_worker_api::{Error, ErrorKind, ExecutionFailure, Phase};
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let (target, session, mut assigned) = crate::tests::claimed(&db.store).await;
    let accepted = subscribe_task(&db.store, &target.task_id).await;
    for sequence in 1..=3 {
        let mut report = completed(&assigned, Quiescence::Confirmed, serde_json::Value::Null);
        let AttemptReport::Completed(original) = report.report else {
            unreachable!()
        };
        report.report = AttemptReport::Failed(ExecutionFailure {
            context: original.context,
            error: Error::new(ErrorKind::Runtime, "transient fixture"),
            phase: Phase::Execution,
            cleanup_error: None,
            execution_may_have_started: true,
        });
        let result = db.store.settle(&report).await.unwrap();
        if sequence < 3 {
            assert_eq!(result.task_state, TaskState::Queued);
            assert_eq!(
                status(&db.store, &accepted.subscription_id).await.state,
                CompletionState::Waiting
            );
            assigned = assignment(
                db.store
                    .acquire(&acquire_command(&session, 0, sequence + 1))
                    .await
                    .unwrap(),
            );
        } else {
            assert_eq!(result.task_state, TaskState::Failed);
            let final_state = status(&db.store, &accepted.subscription_id).await;
            assert_eq!(final_state.state, CompletionState::Pending);
            let event = final_state.event.unwrap();
            assert_eq!(event.value()["ldgstate"], "failed");
            let result = db.store.result(&scope(), &target.task_id).await.unwrap();
            assert!(
                matches!(result.outcome, Some(TaskOutcome::Failed { attempt_id, .. }) if attempt_id == assigned.lease.owner.attempt_id)
            );
        }
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn expiry_recovery_notifies_final_lost_attempt_without_a_report() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let mut input = command();
    input.input.retry_policy.max_attempts = 1;
    let target = db
        .store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    let accepted = subscribe_task(&db.store, &target.task_id).await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let mut tx = db.store.pool.begin().await.unwrap();
    let now = db::now(&mut tx).await.unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=$2 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .bind(codec::ms(now - 1).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=$2 WHERE task_id=$1")
        .bind(&target.task_id)
        .bind(codec::ms(now - 1).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(db.store.expire_batch(1).await.unwrap().expired, 1);
    let final_state = status(&db.store, &accepted.subscription_id).await;
    assert_eq!(final_state.state, CompletionState::Pending);
    assert_eq!(final_state.event.unwrap().value()["ldgstate"], "failed");
    let stored = db
        .store
        .inspect_attempt(&scope(), &target.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    assert!(stored.settlement.is_none());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn notification_failure_rolls_back_first_settlement_and_workflow_terminalization() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let (target, _, assigned) = crate::tests::claimed(&db.store).await;
    let accepted = subscribe_task(&db.store, &target.task_id).await;
    sqlx::raw_sql("CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'notification write failed'; END $$; CREATE TRIGGER reject_completion BEFORE UPDATE ON completion_subscriptions FOR EACH ROW EXECUTE FUNCTION reject_completion();")
        .execute(&db.store.pool).await.unwrap();
    let report = completed(&assigned, Quiescence::Confirmed, json!("done"));
    assert!(matches!(
        db.store.settle(&report).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(
        db.store
            .inspect_attempt(&scope(), &target.task_id, &assigned.lease.owner.attempt_id)
            .await
            .unwrap()
            .settlement
            .is_none()
    );
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    sqlx::query("DROP TRIGGER reject_completion ON completion_subscriptions")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.settle(&report).await.unwrap();
    let run = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let accepted = db
        .store
        .subscribe_completion(&subscription_command(CompletionTarget::Workflow {
            id: run.workflow_id.clone(),
        }))
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let decision = WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision: 0,
        action: WorkflowAction::Complete {
            output: json!(null),
        },
    };
    db.store
        .settle(&completed(
            &assigned,
            Quiescence::Confirmed,
            serde_json::to_value(decision).unwrap(),
        ))
        .await
        .unwrap();
    let work = db.store.claim_work(16).await.unwrap().remove(0);
    sqlx::query("CREATE TRIGGER reject_completion BEFORE UPDATE ON completion_subscriptions FOR EACH ROW EXECUTE FUNCTION reject_completion()")
        .execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        db.store
            .workflow_status(&scope(), &run.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Running
    );
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Waiting
    );
    sqlx::query("DROP TRIGGER reject_completion ON completion_subscriptions")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.apply_work(&work, &[]).await.unwrap();
    assert_eq!(
        status(&db.store, &accepted.subscription_id).await.state,
        CompletionState::Pending
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn due_lease_query_uses_index_range_without_scanning_future_backlog() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let target = task(&db.store, "query-plan").await;
    sqlx::query("INSERT INTO completion_subscriptions(subscription_id,tenant_id,namespace,task_id,destination,idempotency_key,command_bytes,state,created_at_ms,activated_at_ms,next_attempt_at_ms,event_bytes) SELECT 'sub_synthetic_'||n,'acme','billing',$1,'accounting','key_'||n,'{}','pending',1,1,253402300799000,'{}' FROM generate_series(1,20000) n")
        .bind(&target.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("VACUUM ANALYZE completion_subscriptions")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let explain = sqlx::AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, BUFFERS) {}",
        include_str!("../../queries/completion_leases.sql")
    ));
    let plan: Vec<String> = sqlx::query_scalar(explain)
        .bind("acme")
        .bind("billing")
        .bind("accounting")
        .bind(1000_i64)
        .bind(16_i64)
        .bind(31000_i64)
        .fetch_all(&db.store.pool)
        .await
        .unwrap();
    let plan = plan.join("\n");
    println!("Completion lease query against 20,000 future obligations:\n{plan}");
    assert!(
        plan.lines()
            .any(|line| line.contains("Index Cond:") && line.contains("COALESCE")),
        "due cutoff must be an index condition:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan"),
        "due scan must not visit all retained obligations:\n{plan}"
    );
    assert!(lease(&db.store).await.is_empty());
    db.finish().await;
}

async fn acquire_from(store: &PostgresStore, queue: &str) -> Assignment {
    let session = store.open_session(&scope(), queue, 1).await.unwrap();
    assignment(
        store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    )
}
async fn workflow_decision(
    store: &PostgresStore,
    assigned: &Assignment,
    revision: u64,
    action: WorkflowAction,
) {
    let value = WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision,
        action,
    };
    store
        .settle(&completed(
            assigned,
            Quiescence::Confirmed,
            serde_json::to_value(value).unwrap(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn owned_subworkflow_completion_notifies_child_before_resuming_and_finishing_parent() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let parent = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let parent_subscription = db
        .store
        .subscribe_completion(&subscription_command(CompletionTarget::Workflow {
            id: parent.workflow_id.clone(),
        }))
        .await
        .unwrap();
    let assigned = acquire_from(&db.store, "python").await;
    workflow_decision(
        &db.store,
        &assigned,
        0,
        WorkflowAction::Suspend {
            state: json!({"phase":"join"}),
            continuation: "joined".into(),
            until: vec!["nested".into()],
            commands: vec![WorkflowChildCommand {
                kind: WorkflowChildKind::Workflow,
                key: "nested".into(),
                program: descriptor().program,
                queue: "subflows".into(),
                data: json!({}),
                retry_policy: command().input.retry_policy,
                attempt_timeout_ms: command().input.attempt_timeout_ms,
            }],
        },
    )
    .await;
    let work = db.store.claim_work(16).await.unwrap().remove(0);
    db.store
        .apply_work(
            &work,
            &[ResolvedWorkflowChild {
                key: "nested".into(),
                kind: WorkflowChildKind::Workflow,
                descriptor: descriptor(),
            }],
        )
        .await
        .unwrap();
    let child: String = sqlx::query_scalar(
        "SELECT child_workflow_id FROM owned_workflow_links WHERE parent_workflow_id=$1",
    )
    .bind(&parent.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    let child_subscription = db
        .store
        .subscribe_completion(&subscription_command(CompletionTarget::Workflow {
            id: child.clone(),
        }))
        .await
        .unwrap();
    let assigned = acquire_from(&db.store, "subflows").await;
    workflow_decision(
        &db.store,
        &assigned,
        0,
        WorkflowAction::Complete {
            output: json!("child-result"),
        },
    )
    .await;
    assert_eq!(
        status(&db.store, &child_subscription.subscription_id)
            .await
            .state,
        CompletionState::Waiting
    );
    let work = db.store.claim_work(16).await.unwrap().remove(0);
    db.store.apply_work(&work, &[]).await.unwrap();
    let child_notification = status(&db.store, &child_subscription.subscription_id).await;
    assert_eq!(child_notification.state, CompletionState::Pending);
    let child_event = child_notification.event.unwrap();
    assert_eq!(
        child_event.value()["ldgparentworkflowid"],
        parent.workflow_id
    );
    assert_eq!(child_event.value()["ldgrootworkflowid"], parent.workflow_id);
    assert_eq!(
        status(&db.store, &parent_subscription.subscription_id)
            .await
            .state,
        CompletionState::Waiting
    );
    let work = db.store.claim_work(16).await.unwrap().remove(0);
    db.store.apply_work(&work, &[]).await.unwrap();
    let assigned = acquire_from(&db.store, "python").await;
    workflow_decision(
        &db.store,
        &assigned,
        1,
        WorkflowAction::Complete {
            output: json!("parent-result"),
        },
    )
    .await;
    let work = db.store.claim_work(16).await.unwrap().remove(0);
    db.store.apply_work(&work, &[]).await.unwrap();
    let parent_notification = status(&db.store, &parent_subscription.subscription_id).await;
    assert_eq!(parent_notification.state, CompletionState::Pending);
    assert_ne!(parent_notification.event.unwrap().id(), child_event.id());
    let mut late_command = child_subscription.command.clone();
    late_command.idempotency_key = "late-child".into();
    let late = db.store.subscribe_completion(&late_command).await.unwrap();
    assert_eq!(late.event.unwrap(), child_event);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_dispatchers_skip_locked_rows_and_do_not_share_live_leases() {
    let db = TestDb::new().await;
    db.store
        .configure_completion_destination(&destination())
        .await
        .unwrap();
    let mut ids = Vec::new();
    for n in 0..10 {
        let target = task(&db.store, &format!("parallel-{n}")).await;
        ids.push(
            subscribe_task(&db.store, &target.task_id)
                .await
                .subscription_id,
        );
        db.store.cancel(&scope(), &target.task_id).await.unwrap();
    }
    let mut blocked = db.store.pool.begin().await.unwrap();
    sqlx::query(
        "SELECT subscription_id FROM completion_subscriptions WHERE subscription_id=$1 FOR UPDATE",
    )
    .bind(&ids[0])
    .fetch_one(&mut *blocked)
    .await
    .unwrap();
    let mut calls = Vec::new();
    for _ in 0..4 {
        let store = db.store.clone();
        calls.push(tokio::spawn(async move {
            store
                .lease_completions(&destination(), 3, deadline())
                .await
                .unwrap()
        }));
    }
    let mut leased = std::collections::HashSet::new();
    for call in calls {
        for item in call.await.unwrap() {
            assert!(leased.insert(item.subscription.subscription_id));
        }
    }
    assert_eq!(leased.len(), 9);
    assert!(!leased.contains(&ids[0]));
    blocked.commit().await.unwrap();
    let last = lease(&db.store).await;
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].subscription.subscription_id, ids[0]);
    let duplicate = outcome(&last[0], CompletionDeliveryOutcome::Confirmed);
    assert!(matches!(
        db.store
            .complete_deliveries(&[duplicate.clone(), duplicate], deadline())
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        status(&db.store, &ids[0]).await.state,
        CompletionState::Delivering
    );
    db.finish().await;
}
