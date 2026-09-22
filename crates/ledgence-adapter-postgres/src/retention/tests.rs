//! Real PostgreSQL checks, always in fresh disposable databases. Fixture-only
//! timestamp edits model old history without weakening the 90-day public floor.
use super::*;
use crate::tests::{
    TestDb, acquire_command, assignment, claimed, command, completed, descriptor, scope,
};
use serde_json::json;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
async fn rounds(store: &PostgresStore, count: usize, size: u32) -> Vec<RetentionProgress> {
    let policy = RetentionPolicy {
        batch_size: size,
        ..Default::default()
    };
    let mut result = Vec::new();
    for _ in 0..count {
        let progress = store
            .retain_batch(&scope(), &policy, deadline())
            .await
            .unwrap();
        // Metadata cleanup may remove up to 16 subscriptions or three final
        // identity rows. Unbounded history/receipt pages obey batch_size.
        assert!(progress.deleted_rows <= size.max(16));
        result.push(progress);
    }
    result
}
async fn cancelled(store: &PostgresStore, key: &str) -> TaskSnapshot {
    let mut input = command();
    input.idempotency_key = key.into();
    let task = store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    store.cancel(&scope(), &task.task_id).await.unwrap();
    task
}
async fn age_task(store: &PostgresStore, id: &str) {
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2,cancel_requested_at_ms=CASE WHEN cancel_requested_at_ms IS NOT NULL THEN 2 ELSE NULL END WHERE task_id=$1")
        .bind(id).execute(&store.pool).await.unwrap();
    sqlx::query(
        "UPDATE attempts SET finished_at_ms=2 WHERE task_id=$1 AND finished_at_ms IS NOT NULL",
    )
    .bind(id)
    .execute(&store.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE accepted_settlements SET accepted_at=2 WHERE attempt_id IN (SELECT attempt_id FROM attempts WHERE task_id=$1)").bind(id).execute(&store.pool).await.unwrap();
}
async fn exists(store: &PostgresStore, id: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_id=$1)")
        .bind(id)
        .fetch_one(&store.pool)
        .await
        .unwrap()
}
async fn destination(store: &PostgresStore) -> CompletionDestination {
    let destination = CompletionDestination {
        scope: scope(),
        destination: "retention-test".into(),
        binding: "http://localhost/test".into(),
    };
    store
        .configure_completion_destination(&destination)
        .await
        .unwrap();
    destination
}
async fn subscribe(store: &PostgresStore, id: &str) -> CompletionSubscription {
    store
        .subscribe_completion(&CompletionSubscribeCommand {
            scope: scope(),
            target: CompletionTarget::Task { id: id.into() },
            destination: "retention-test".into(),
            idempotency_key: "test".into(),
        })
        .await
        .unwrap()
}
async fn exhaust(store: &PostgresStore, id: &str, old: bool) {
    sqlx::query("UPDATE completion_subscriptions SET state='exhausted',attempts=8,total_attempts=8,next_attempt_at_ms=NULL,lease_token=NULL,lease_until_ms=NULL,exhausted_at_ms=CASE WHEN $2 THEN 2 ELSE created_at_ms END,created_at_ms=CASE WHEN $2 THEN 1 ELSE created_at_ms END,activated_at_ms=CASE WHEN $2 THEN 2 ELSE activated_at_ms END WHERE subscription_id=$1")
        .bind(id).bind(old).execute(&store.pool).await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn scoped_preview_is_read_only_and_retirement_is_bounded_resumable() {
    let db = TestDb::new().await;
    let old = cancelled(&db.store, "old").await;
    age_task(&db.store, &old.task_id).await;
    let recent = cancelled(&db.store, "recent").await;
    let mut other = command();
    other.input.namespace = "another".into();
    other.idempotency_key = "other".into();
    let other = db
        .store
        .accept_resolved_submission(&other, &descriptor())
        .await
        .unwrap();
    db.store
        .cancel(
            &Scope {
                tenant_id: "acme".into(),
                namespace: "another".into(),
            },
            &other.task_id,
        )
        .await
        .unwrap();
    age_task(&db.store, &other.task_id).await;
    sqlx::query("INSERT INTO task_history(task_id,sequence,at_ms,reason) SELECT $1,n,2,'submitted' FROM generate_series(100,149) n").bind(&old.task_id).execute(&db.store.pool).await.unwrap();
    let preview = db
        .store
        .retention_preview(&scope(), &RetentionPolicy::default(), deadline())
        .await
        .unwrap();
    assert_eq!(preview.task_candidates, vec![old.task_id.clone()]);
    let markers: i64 =
        sqlx::query_scalar("SELECT count(*) FROM tasks WHERE retiring_at_ms IS NOT NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(markers, 0);
    rounds(&db.store, 1, 3).await;
    assert!(matches!(
        db.store.status(&scope(), &old.task_id).await,
        Err(ContractError::NotFound)
    ));
    assert!(matches!(
        db.store.result(&scope(), &old.task_id).await,
        Err(ContractError::NotFound)
    ));
    assert!(exists(&db.store, &old.task_id).await);
    let page = db
        .store
        .list_tasks(&scope(), &TaskListQuery::default())
        .await
        .unwrap();
    assert!(page.items.iter().all(|t| t.task_id != old.task_id));
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    rounds(&reopened, 500, 3).await;
    assert!(!exists(&reopened, &old.task_id).await);
    assert!(exists(&reopened, &recent.task_id).await);
    assert!(exists(&reopened, &other.task_id).await);
    let mut retry = command();
    retry.idempotency_key = "old".into();
    let new = reopened
        .accept_resolved_submission(&retry, &descriptor())
        .await
        .unwrap();
    assert_ne!(new.task_id, old.task_id);
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn live_cursor_and_active_attempt_survive_until_expired_session_is_reconciled() {
    let db = TestDb::new().await;
    let (task, session, assignment) = claimed(&db.store).await;
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0 WHERE session_id=$1")
        .bind(&session.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    rounds(&db.store, 24, 2).await;
    let cursor: i64 =
        sqlx::query_scalar("SELECT count(*) FROM consumer_cursors WHERE session_id=$1")
            .bind(&session.id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(cursor, 1);
    // Session expiry does not remove an owned attempt. Reconcile via normal expiry.
    sqlx::query("UPDATE attempts SET expires_at_ms=0,authority_deadline_ms=0 WHERE attempt_id=$1")
        .bind(&assignment.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=0 WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.expire_batch(10).await.unwrap();
    db.store.cancel(&scope(), &task.task_id).await.unwrap();
    age_task(&db.store, &task.task_id).await;
    rounds(&db.store, 180, 2).await;
    assert!(!exists(&db.store, &task.task_id).await);
    assert!(matches!(
        db.store.acquire(&acquire_command(&session, 0, 1)).await,
        Err(ContractError::UnknownSession)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn live_cursor_retains_a_terminal_result_and_rotates_past_blocked_targets() {
    let db = TestDb::new().await;
    let (task, session, assignment) = claimed(&db.store).await;
    db.store
        .renew(&RenewCommand {
            owner: assignment.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    db.store
        .settle(&completed(
            &assignment,
            Quiescence::Confirmed,
            json!({"ok":true}),
        ))
        .await
        .unwrap();
    age_task(&db.store, &task.task_id).await;
    let other = cancelled(&db.store, "other").await;
    age_task(&db.store, &other.task_id).await;
    rounds(&db.store, 180, 2).await;
    assert!(exists(&db.store, &task.task_id).await);
    assert!(!exists(&db.store, &other.task_id).await);
    assert!(db.store.result(&scope(), &task.task_id).await.is_ok());
    db.store
        .acquire(&acquire_command(&session, 0, 2))
        .await
        .unwrap();
    rounds(&db.store, 180, 2).await;
    assert!(!exists(&db.store, &task.task_id).await);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn pending_callbacks_and_recent_exhaustion_extend_retention_until_expired() {
    let db = TestDb::new().await;
    destination(&db.store).await;
    let task = cancelled(&db.store, "callback").await;
    age_task(&db.store, &task.task_id).await;
    let subscription = subscribe(&db.store, &task.task_id).await;
    rounds(&db.store, 30, 2).await;
    assert!(exists(&db.store, &task.task_id).await);
    exhaust(&db.store, &subscription.subscription_id, false).await;
    rounds(&db.store, 30, 2).await;
    assert!(exists(&db.store, &task.task_id).await);
    exhaust(&db.store, &subscription.subscription_id, true).await;
    rounds(&db.store, 150, 2).await;
    assert!(!exists(&db.store, &task.task_id).await);
    assert!(matches!(
        db.store
            .retry_completion(&CompletionRetryCommand {
                scope: scope(),
                subscription_id: subscription.subscription_id,
                expected_generation: 1
            })
            .await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

async fn wait_query(store: &PostgresStore, pattern: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)")
                .bind(pattern).fetch_one(&store.pool).await.unwrap();
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn manual_redelivery_committing_before_cleanup_preserves_task() {
    let db = TestDb::new().await;
    destination(&db.store).await;
    let task = cancelled(&db.store, "rearm").await;
    age_task(&db.store, &task.task_id).await;
    let subscription = subscribe(&db.store, &task.task_id).await;
    exhaust(&db.store, &subscription.subscription_id, true).await;
    let command = CompletionRetryCommand {
        scope: scope(),
        subscription_id: subscription.subscription_id.clone(),
        expected_generation: 1,
    };
    let mut lock = db.store.pool.begin().await.unwrap();
    sqlx::query(
        "SELECT subscription_id FROM completion_subscriptions WHERE subscription_id=$1 FOR UPDATE",
    )
    .bind(&subscription.subscription_id)
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    let rearm_store = db.store.clone();
    let rearm_command = command.clone();
    let rearm = tokio::spawn(async move { rearm_store.retry_completion(&rearm_command).await });
    wait_query(
        &db.store,
        "SELECT * FROM completion_subscriptions WHERE tenant_id=%",
    )
    .await;
    let store = db.store.clone();
    let scope = scope();
    let policy = RetentionPolicy::default();
    let cleanup =
        tokio::spawn(async move { store.retain_batch(&scope, &policy, deadline()).await });
    // Both real public operations now contend for the same immutable receipt;
    // the earlier manual rearm gets the row lock first when the fixture releases.
    wait_query(&db.store, "SELECT subscription_id,state,%").await;
    lock.commit().await.unwrap();
    assert_eq!(rearm.await.unwrap().unwrap().generation, 2);
    cleanup.await.unwrap().unwrap();
    assert_eq!(
        db.store
            .retry_completion(&command)
            .await
            .unwrap()
            .generation,
        2
    );
    rounds(&db.store, 30, 2).await;
    assert!(exists(&db.store, &task.task_id).await);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn retirement_rolls_back_on_final_write_failure_and_parallel_collectors_are_safe() {
    let db = TestDb::new().await;
    destination(&db.store).await;
    let task = cancelled(&db.store, "rollback").await;
    age_task(&db.store, &task.task_id).await;
    let subscription = subscribe(&db.store, &task.task_id).await;
    exhaust(&db.store, &subscription.subscription_id, true).await;
    sqlx::raw_sql("CREATE FUNCTION fail_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.retiring_at_ms IS NOT NULL THEN RAISE EXCEPTION 'injected retirement failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_retirement BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION fail_retirement()")
        .execute(&db.store.pool).await.unwrap();
    assert!(
        db.store
            .retain_batch(&scope(), &RetentionPolicy::default(), deadline())
            .await
            .is_err()
    );
    assert!(
        db.store
            .completion_status(&scope(), &subscription.subscription_id)
            .await
            .is_ok()
    );
    assert!(db.store.status(&scope(), &task.task_id).await.is_ok());
    sqlx::raw_sql("DROP TRIGGER fail_retirement ON tasks; DROP FUNCTION fail_retirement()")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let a = db.store.clone();
    let b = db.store.clone();
    tokio::join!(rounds(&a, 90, 2), rounds(&b, 90, 2));
    assert!(!exists(&db.store, &task.task_id).await);
    db.finish().await;
}

async fn workflow_assignment(store: &PostgresStore, queue: &str) -> Assignment {
    let session = store.open_session(&scope(), queue, 1).await.unwrap();
    let assigned = assignment(
        store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    store
        .record_local_result(&LocalResultCommand {
            owner: assigned.lease.owner.clone(),
            record: LocalStepRecord {
                key: "local".into(),
                callable: "local.v1".into(),
                input: json!(1),
                output: json!(2),
            },
        })
        .await
        .unwrap();
    assigned
}
async fn workflow_decision(
    store: &PostgresStore,
    assigned: &Assignment,
    action: WorkflowAction,
    resolved: &[ResolvedWorkflowChild],
) {
    let decision = WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision: 0,
        action,
    };
    store
        .settle(&completed(
            assigned,
            Quiescence::Confirmed,
            serde_json::to_value(decision).unwrap(),
        ))
        .await
        .unwrap();
    let work = store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 1);
    store.apply_work(&work[0], resolved).await.unwrap();
}
async fn drain_work(store: &PostgresStore) {
    for _ in 0..60 {
        sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE processed_at_ms IS NULL AND lease_token IS NULL").execute(&store.pool).await.unwrap();
        let work = store.claim_work(16).await.unwrap();
        if work.is_empty() {
            return;
        }
        for item in work {
            store.apply_work(&item, &[]).await.unwrap();
        }
    }
    panic!("workflow fixture did not drain");
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn active_tree_journals_and_callback_obligations_survive_then_closed_tree_is_collected_leaf_first()
 {
    let db = TestDb::new().await;
    destination(&db.store).await;
    let root = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let first = workflow_assignment(&db.store, "python").await;
    let mut children = Vec::new();
    let mut resolved = Vec::new();
    for (key, kind, queue) in [
        ("flow", WorkflowChildKind::Workflow, "subflows"),
        ("task", WorkflowChildKind::Task, "children"),
    ] {
        children.push(WorkflowChildCommand {
            kind,
            key: key.into(),
            program: descriptor().program,
            queue: queue.into(),
            data: json!({}),
            retry_policy: command().input.retry_policy,
            attempt_timeout_ms: command().input.attempt_timeout_ms,
        });
        resolved.push(ResolvedWorkflowChild {
            kind,
            key: key.into(),
            descriptor: descriptor(),
        });
    }
    workflow_decision(
        &db.store,
        &first,
        WorkflowAction::Suspend {
            state: json!({"saved":1}),
            continuation: "joined".into(),
            commands: children,
            until: vec!["flow".into(), "task".into()],
        },
        &resolved,
    )
    .await;
    let child: String = sqlx::query_scalar(
        "SELECT child_workflow_id FROM owned_workflow_links WHERE parent_workflow_id=$1",
    )
    .bind(&root.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    let child_assignment = workflow_assignment(&db.store, "subflows").await;
    workflow_decision(
        &db.store,
        &child_assignment,
        WorkflowAction::Complete {
            output: json!("child result"),
        },
        &[],
    )
    .await;
    drain_work(&db.store).await;
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE workflow_id=$1")
        .bind(&child)
        .execute(&db.store.pool)
        .await
        .unwrap();
    age_task(&db.store, &first.lease.owner.task_id).await;
    age_task(&db.store, &child_assignment.lease.owner.task_id).await;
    rounds(&db.store, 120, 2).await;
    assert!(db.store.workflow_result(&scope(), &child).await.is_ok());
    let journals: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_local_results")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(journals, 2);
    let subscription = db
        .store
        .subscribe_completion(&CompletionSubscribeCommand {
            scope: scope(),
            target: CompletionTarget::Workflow { id: child.clone() },
            destination: "retention-test".into(),
            idempotency_key: "workflow".into(),
        })
        .await
        .unwrap();
    db.store
        .cancel_workflow(&scope(), &root.workflow_id)
        .await
        .unwrap();
    drain_work(&db.store).await;
    assert!(
        db.store
            .workflow_status(&scope(), &root.workflow_id)
            .await
            .unwrap()
            .state
            .is_terminal()
    );
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    let task_ids: Vec<String> =
        sqlx::query_scalar("SELECT task_id FROM tasks WHERE terminal_at_ms IS NOT NULL")
            .fetch_all(&db.store.pool)
            .await
            .unwrap();
    for id in task_ids {
        age_task(&db.store, &id).await;
    }
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0")
        .execute(&db.store.pool)
        .await
        .unwrap();
    rounds(&db.store, 180, 2).await;
    let journals: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_local_results")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(journals, 2);
    assert!(db.store.workflow_status(&scope(), &child).await.is_ok());
    let root_subscription = db
        .store
        .subscribe_completion(&CompletionSubscribeCommand {
            scope: scope(),
            target: CompletionTarget::Workflow {
                id: root.workflow_id.clone(),
            },
            destination: "retention-test".into(),
            idempotency_key: "root".into(),
        })
        .await
        .unwrap();
    let root_status = serde_json::to_value(
        db.store
            .workflow_status(&scope(), &root.workflow_id)
            .await
            .unwrap(),
    )
    .unwrap();
    let root_result = serde_json::to_value(
        db.store
            .workflow_result(&scope(), &root.workflow_id)
            .await
            .unwrap(),
    )
    .unwrap();
    exhaust(&db.store, &subscription.subscription_id, true).await;
    rounds(&db.store, 1500, 2).await;
    assert!(matches!(
        db.store.workflow_status(&scope(), &child).await,
        Err(ContractError::NotFound)
    ));
    assert_eq!(
        serde_json::to_value(
            db.store
                .workflow_status(&scope(), &root.workflow_id)
                .await
                .unwrap()
        )
        .unwrap(),
        root_status
    );
    assert_eq!(
        serde_json::to_value(
            db.store
                .workflow_result(&scope(), &root.workflow_id)
                .await
                .unwrap()
        )
        .unwrap(),
        root_result
    );
    let destination = destination(&db.store).await;
    let deliveries = db
        .store
        .lease_completions(&destination, 16, deadline())
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(
        deliveries[0].subscription.subscription_id,
        root_subscription.subscription_id
    );
    db.store
        .complete_deliveries(
            &[CompletionDeliveryResult {
                subscription_id: root_subscription.subscription_id.clone(),
                generation: deliveries[0].subscription.generation,
                lease_token: deliveries[0].lease_token.clone(),
                outcome: CompletionDeliveryOutcome::Confirmed,
            }],
            deadline(),
        )
        .await
        .unwrap();
    rounds(&db.store, 30, 2).await;
    assert!(
        db.store
            .workflow_result(&scope(), &root.workflow_id)
            .await
            .is_ok(),
        "recent successful delivery retains result"
    );
    sqlx::query("UPDATE completion_subscriptions SET created_at_ms=1,activated_at_ms=2,delivered_at_ms=2 WHERE subscription_id=$1").bind(&root_subscription.subscription_id).execute(&db.store.pool).await.unwrap();
    rounds(&db.store, 1000, 2).await;
    let counts:(i64,i64,i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM workflow_runs),(SELECT count(*) FROM tasks),(SELECT count(*) FROM workflow_local_results),(SELECT count(*) FROM workflow_activations),(SELECT count(*) FROM owned_workflow_links),(SELECT count(*) FROM workflow_work),(SELECT count(*) FROM worker_sessions)").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0, 0, 0, 0, 0));
    assert!(matches!(
        db.store.workflow_result(&scope(), &root.workflow_id).await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn retained_dispatch_receipts_expire_without_resetting_the_live_consumer_sequence() {
    let db = TestDb::new().await;
    db.store
        .configure_route(&DispatchRoute {
            scope: scope(),
            queue: "python".into(),
            destination: "test-broker".into(),
        })
        .await
        .unwrap();
    let task = cancelled(&db.store, "external").await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let claim = ClaimCommand {
        acquisition: acquire_command(&session, 0, 1),
        dispatch: DispatchRef {
            scope: scope(),
            queue: "python".into(),
            task_id: task.task_id.clone(),
            generation: 1,
        },
    };
    let accepted = db.store.claim_dispatch(&claim).await.unwrap();
    accepted.validate_reply_against(&claim).unwrap();
    rounds(&db.store, 30, 2).await;
    assert!(
        db.store.claim_dispatch(&claim).await.is_ok(),
        "young terminal claim receipts are retained"
    );
    age_task(&db.store, &task.task_id).await;
    rounds(&db.store, 180, 2).await;
    assert!(!exists(&db.store, &task.task_id).await);
    assert!(matches!(
        db.store.claim_dispatch(&claim).await,
        Err(ContractError::Conflict) | Err(ContractError::ObsoleteOperation)
    ));
    let cursor: Option<String> = sqlx::query_scalar(
        "SELECT trunc(sequence)::text FROM consumer_cursors WHERE session_id=$1 AND consumer_id=0",
    )
    .bind(&session.id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(cursor.as_deref(), Some("1"));
    let other = cancelled(&db.store, "fresh").await;
    let mut next = claim;
    next.acquisition.sequence = 2;
    next.dispatch.task_id = other.task_id;
    assert!(db.store.claim_dispatch(&next).await.is_ok());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn retirement_serializes_with_late_subscriptions_without_false_acceptance() {
    let db = TestDb::new().await;
    destination(&db.store).await;
    let task = cancelled(&db.store, "race").await;
    age_task(&db.store, &task.task_id).await;
    let command = CompletionSubscribeCommand {
        scope: scope(),
        target: CompletionTarget::Task {
            id: task.task_id.clone(),
        },
        destination: "retention-test".into(),
        idempotency_key: "race".into(),
    };
    let scope = scope();
    let policy = RetentionPolicy::default();
    let (cleanup, subscription) = tokio::join!(
        db.store.retain_batch(&scope, &policy, deadline()),
        db.store.subscribe_completion(&command)
    );
    cleanup.unwrap();
    match subscription {
        Ok(subscription) => {
            assert_eq!(subscription.state, CompletionState::Pending);
            rounds(&db.store, 60, 2).await;
            assert!(db.store.status(&scope, &task.task_id).await.is_ok());
            assert!(
                db.store
                    .completion_status(&scope, &subscription.subscription_id)
                    .await
                    .is_ok()
            );
        }
        Err(ContractError::NotFound) => assert!(matches!(
            db.store.status(&scope, &task.task_id).await,
            Err(ContractError::NotFound)
        )),
        other => panic!("unexpected concurrent registration {other:?}"),
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn workflow_event_receipts_remain_replayable_until_run_retirement() {
    let db = TestDb::new().await;
    let workflow = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let event=WorkflowEventCommand {scope:scope(),workflow_id:workflow.workflow_id.clone(),key:"approval".into(),event:WorkflowEvent::new(json!({"specversion":"1.0","id":"approval-1","source":"urn:test","type":"approval","datacontenttype":"application/json","data":{"ok":true}})).unwrap()};
    db.store.send_workflow_event(&event).await.unwrap();
    db.store
        .cancel_workflow(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    drain_work(&db.store).await;
    rounds(&db.store, 30, 1).await;
    assert!(
        db.store
            .send_workflow_event(&event)
            .await
            .unwrap()
            .already_accepted
    );
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE workflow_id=$1")
        .bind(&workflow.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let tasks: Vec<String> = sqlx::query_scalar("SELECT task_id FROM tasks WHERE workflow_id=$1")
        .bind(&workflow.workflow_id)
        .fetch_all(&db.store.pool)
        .await
        .unwrap();
    for id in tasks {
        age_task(&db.store, &id).await;
    }
    rounds(&db.store, 6, 1).await;
    assert!(matches!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await,
        Err(ContractError::NotFound)
    ));
    assert!(matches!(
        db.store.send_workflow_event(&event).await,
        Err(ContractError::NotFound)
    ));
    rounds(&db.store, 180, 1).await;
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_events")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(events, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn age_selection_uses_scoped_range_indexes_even_when_no_old_rows_remain() {
    let db = TestDb::new().await;
    let task = cancelled(&db.store, "template").await;
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,terminal_at_ms,attempt_count) SELECT 'retention_task_'||n,'retention_run_'||n,t.tenant_id,t.namespace,t.queue,'retention_key_'||n,t.input_bytes,t.descriptor_bytes,'cancelled',n,n,n,0 FROM tasks t CROSS JOIN generate_series(1,20000) n WHERE t.task_id=$1").bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("ANALYZE tasks")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let query = candidate_sql(
        "tasks",
        "task_id",
        "terminal_at_ms",
        " AND terminal_at_ms IS NOT NULL AND retiring_at_ms IS NULL",
    );
    for (after, cutoff) in [(0_i64, 500_i64), (500, 500)] {
        let plan: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {query}"
        )))
        .bind(after)
        .bind("")
        .bind(cutoff)
        .bind("acme")
        .bind("billing")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        let text = plan.to_string();
        assert!(text.contains("tasks_retention"), "{text}");
        assert!(!text.contains("Seq Scan"), "{text}");
    }
    let policy = RetentionPolicy {
        retain_for_ms: 253_402_300_799_999,
        batch_size: 2,
    };
    let preview = db
        .store
        .retention_preview(&scope(), &policy, deadline())
        .await
        .unwrap();
    assert!(preview.task_candidates.is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn journal_pages_resume_after_rollback_and_reopen_without_skipping_results() {
    let db = TestDb::new().await;
    let run = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let assigned = workflow_assignment(&db.store, "python").await;
    workflow_decision(
        &db.store,
        &assigned,
        WorkflowAction::Complete {
            output: json!({"done":true}),
        },
        &[],
    )
    .await;
    // A live cursor keeps the activation/task row, independently of journal cleanup.
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE workflow_id=$1")
        .bind(&run.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms) SELECT activation_id,'extra_'||n,record_bytes,attempt_id,accepted_at_ms FROM workflow_local_results CROSS JOIN generate_series(1,6) n").execute(&db.store.pool).await.unwrap();
    rounds(&db.store, 6, 2).await;
    let mut tx = db.store.pool.begin().await.unwrap();
    let mut progress = RetentionProgress::default();
    assert!(
        collect_workflow_journals(&mut tx, &run.workflow_id, 2, &mut progress)
            .await
            .unwrap()
    );
    assert_eq!(progress.deleted_rows, 2);
    tx.rollback().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_local_results")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 7);
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let mut connection = reopened.pool.acquire().await.unwrap();
    for expected in [2, 2, 2, 1, 0] {
        let mut progress = RetentionProgress::default();
        assert!(
            collect_workflow_journals(&mut connection, &run.workflow_id, 2, &mut progress)
                .await
                .unwrap()
        );
        assert_eq!(progress.deleted_rows, expected);
    }
    assert!(
        !collect_workflow_journals(
            &mut connection,
            &run.workflow_id,
            2,
            &mut RetentionProgress::default()
        )
        .await
        .unwrap()
    );
    let cursor: Option<String> = sqlx::query_scalar(
        "SELECT trunc(retention_activation_after_revision)::text FROM workflow_runs WHERE workflow_id=$1",
    )
    .bind(&run.workflow_id)
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert_eq!(cursor, None);
    let activations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
            .bind(&run.workflow_id)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    assert_eq!(
        activations, 1,
        "the cursor still protects its task and activation"
    );
    drop(connection);
    reopened.close().await;
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let mut connection = reopened.pool.acquire().await.unwrap();
    assert!(
        !collect_workflow_journals(
            &mut connection,
            &run.workflow_id,
            2,
            &mut RetentionProgress::default()
        )
        .await
        .unwrap()
    );
    drop(connection);
    reopened.close().await;
    db.finish().await;
}

mod bounds;

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_collection_shares_one_row_budget_across_callback_history_and_receipt_tables() {
    let db = TestDb::new().await;
    let (task, session, assigned) = claimed(&db.store).await;
    db.store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    db.store
        .settle(&completed(
            &assigned,
            Quiescence::Confirmed,
            json!({"done":true}),
        ))
        .await
        .unwrap();
    db.store
        .acquire(&acquire_command(&session, 0, 2))
        .await
        .unwrap();
    age_task(&db.store, &task.task_id).await;
    destination(&db.store).await;
    let subscription = subscribe(&db.store, &task.task_id).await;
    exhaust(&db.store, &subscription.subscription_id, true).await;
    let policy = RetentionPolicy {
        batch_size: 3,
        ..Default::default()
    };
    let first = db
        .store
        .retain_batch(&scope(), &policy, deadline())
        .await
        .unwrap();
    assert_eq!(first.retired, 1);
    assert_eq!(
        first.deleted_rows, 3,
        "one callback plus two history rows spend one shared budget"
    );
    assert!(exists(&db.store, &task.task_id).await);
    let mut deleted = first.deleted_rows;
    for _ in 0..100 {
        let page = db
            .store
            .retain_batch(&scope(), &policy, deadline())
            .await
            .unwrap();
        assert!(
            page.deleted_rows <= 3,
            "small ledgers must not each receive a fresh row budget"
        );
        deleted += page.deleted_rows;
        if page.deleted_executions == 1 {
            break;
        }
    }
    assert!(!exists(&db.store, &task.task_id).await);
    assert!(
        deleted > 3,
        "fixture crosses history, settlement, attempt and task pages"
    );
    db.finish().await;
}
