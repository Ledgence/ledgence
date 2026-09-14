//! Real PostgreSQL workflow acceptance tests, each using an isolated database.
use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, completed, descriptor, scope};
use ledgence_worker_api::{
    Error, ErrorKind, ExecutionContext, ExecutionFailure, InvocationIdentity, Phase,
};

macro_rules! assert_json_eq {
    ($a:expr, $b:expr) => {
        assert_eq!(
            serde_json::to_value($a).unwrap(),
            serde_json::to_value($b).unwrap()
        )
    };
}
use serde_json::json;

async fn start(store: &PostgresStore) -> WorkflowSnapshot {
    store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap()
}
async fn acquire(store: &PostgresStore, queue: &str) -> Assignment {
    let session = store.open_session(&scope(), queue, 1).await.unwrap();
    assignment(
        store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    )
}
async fn dispatched(store: &PostgresStore, assigned: &Assignment) {
    store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
}
fn local(assigned: &Assignment, key: &str) -> LocalResultCommand {
    LocalResultCommand {
        owner: assigned.lease.owner.clone(),
        record: LocalStepRecord {
            key: key.into(),
            callable: "billing.calculate.v1".into(),
            input: json!({"invoice": "INV-1042"}),
            output: json!({"total": 9007199254740993_u64}),
        },
    }
}
fn child(key: &str) -> WorkflowTaskCommand {
    WorkflowTaskCommand {
        key: key.into(),
        program: descriptor().program,
        queue: "children".into(),
        data: json!({"invoice": "INV-1042"}),
        retry_policy: command().input.retry_policy,
        attempt_timeout_ms: command().input.attempt_timeout_ms,
    }
}
fn resolved(key: &str) -> ResolvedWorkflowChild {
    ResolvedWorkflowChild {
        key: key.into(),
        descriptor: descriptor(),
    }
}
fn decision(assigned: &Assignment, revision: u64, action: WorkflowAction) -> WorkflowDecision {
    WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision,
        action,
    }
}
async fn report(store: &PostgresStore, assigned: &Assignment, value: Value) {
    assert_eq!(
        store
            .settle(&completed(assigned, Quiescence::Confirmed, value))
            .await
            .unwrap()
            .task_state,
        TaskState::Succeeded
    );
}
async fn one_work(store: &PostgresStore) -> WorkflowWork {
    let mut work = store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 1);
    work.remove(0)
}
async fn apply_decision(
    store: &PostgresStore,
    assigned: &Assignment,
    revision: u64,
    action: WorkflowAction,
    resolved: &[ResolvedWorkflowChild],
) -> WorkflowProgress {
    report(
        store,
        assigned,
        serde_json::to_value(decision(assigned, revision, action)).unwrap(),
    )
    .await;
    store
        .apply_work(&one_work(store).await, resolved)
        .await
        .unwrap()
}
async fn drain_available(store: &PostgresStore) {
    for _ in 0..8 {
        let work = store.claim_work(16).await.unwrap();
        if work.is_empty() {
            return;
        }
        for item in work {
            store.apply_work(&item, &[]).await.unwrap();
        }
    }
    panic!("workflow recovery failed to drain bounded fixture");
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn submission_namespace_replay_and_concurrent_descriptor_winner() {
    let db = TestDb::new().await;
    let ordinary = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let original = command();
    let first_descriptor = descriptor();
    let mut changed_descriptor = descriptor();
    changed_descriptor.digest.0 = format!("sha256:{}", "b".repeat(64));
    let (a, b) = tokio::join!(
        db.store
            .accept_resolved_workflow(&original, &first_descriptor),
        db.store
            .accept_resolved_workflow(&original, &changed_descriptor)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.workflow_id, b.workflow_id);
    a.validate().unwrap();
    assert_ne!(a.activation_id.as_deref(), Some(ordinary.task_id.as_str()));
    let tasks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
            .bind(&a.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(tasks, 1);
    let accepted = db
        .store
        .inspect(&scope(), a.activation_id.as_deref().unwrap())
        .await
        .unwrap();
    let again = db
        .store
        .accept_resolved_workflow(&original, &changed_descriptor)
        .await
        .unwrap();
    assert_json_eq!(&a, &again);
    assert_eq!(
        accepted.descriptor,
        db.store
            .inspect(&scope(), a.activation_id.as_deref().unwrap())
            .await
            .unwrap()
            .descriptor
    );
    let mut conflict = original.clone();
    conflict.input.data = json!({"other": true});
    assert!(matches!(
        db.store.replay_workflow_submission(&conflict).await,
        Err(ContractError::Conflict)
    ));
    let mut reordered = original;
    reordered.origin_trace = None;
    assert_json_eq!(
        db.store
            .replay_workflow_submission(&reordered)
            .await
            .unwrap()
            .unwrap(),
        &a
    );
    let unrelated = Scope {
        tenant_id: "other".into(),
        namespace: "billing".into(),
    };
    assert!(matches!(
        db.store.workflow_status(&unrelated, &a.workflow_id).await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn durable_locals_child_suspend_resume_and_terminal_quiescence() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    assert_eq!(controller.workflow_activation_id, workflow.activation_id);
    assert_eq!(
        controller.event.value()["ldgworkflowid"],
        workflow.workflow_id
    );
    let initial = db
        .store
        .activation_context(&controller.lease.owner)
        .await
        .unwrap();
    assert_eq!(initial.continuation, "start");
    assert!(initial.local_steps.is_empty());
    assert!(matches!(
        db.store
            .record_local_result(&local(&controller, "before-dispatch"))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    dispatched(&db.store, &controller).await;
    let a = local(&controller, "a");
    let b = local(&controller, "b");
    let (a_reply, b_reply) = tokio::join!(
        db.store.record_local_result(&a),
        db.store.record_local_result(&b)
    );
    assert!(!a_reply.unwrap().already_accepted);
    assert!(!b_reply.unwrap().already_accepted);
    let context = db
        .store
        .activation_context(&controller.lease.owner)
        .await
        .unwrap();
    assert_eq!(context.revision, 0);
    assert_eq!(context.local_steps.len(), 2);
    let decision = decision(
        &controller,
        0,
        WorkflowAction::Suspend {
            state: json!({"sum": 7}),
            continuation: "finish".into(),
            commands: vec![child("issue")],
            until: vec!["issue".into()],
        },
    );
    let report = completed(
        &controller,
        Quiescence::Unconfirmed,
        serde_json::to_value(decision).unwrap(),
    );
    assert_eq!(
        db.store.settle(&report).await.unwrap().task_state,
        TaskState::Active
    );
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    assert!(matches!(
        db.store
            .record_local_result(&local(&controller, "too-late"))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    assert!(
        db.store
            .record_local_result(&a)
            .await
            .unwrap()
            .already_accepted
    );
    db.store.confirm_quiescence(&report.owner).await.unwrap();
    let work = one_work(&db.store).await;
    let progress = db
        .store
        .apply_work(&work, &[resolved("issue")])
        .await
        .unwrap();
    assert_eq!(progress.children_scheduled, 1);
    assert_eq!(progress.activations_scheduled, 0);
    assert_json_eq!(
        db.store
            .apply_work(&work, &[resolved("issue")])
            .await
            .unwrap(),
        WorkflowProgress::default()
    );
    let waiting = db
        .store
        .workflow_status(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(waiting.state, WorkflowState::Waiting);
    assert!(waiting.activation_id.is_none());
    waiting.validate().unwrap();
    assert!(matches!(
        db.store.activation_context(&controller.lease.owner).await,
        Err(ContractError::OwnershipLost)
    ));
    let child_task = acquire(&db.store, "children").await;
    assert!(child_task.workflow_activation_id.is_none());
    assert_eq!(
        child_task.event.value()["ldgworkflowid"],
        workflow.workflow_id
    );
    report_success_and_apply_child(&db.store, &child_task).await;
    let resumed = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&resumed.lease.owner)
        .await
        .unwrap();
    assert_eq!(context.revision, 1);
    assert_eq!(context.continuation, "finish");
    assert_eq!(context.state, json!({"sum": 7}));
    assert_eq!(
        context.inputs["issue"].task_id,
        child_task.lease.owner.task_id
    );
    assert!(context.local_steps.is_empty());
    assert!(
        db.store
            .record_local_result(&a)
            .await
            .unwrap()
            .already_accepted
    );
    apply_decision(
        &db.store,
        &resumed,
        1,
        WorkflowAction::Complete {
            output: json!({"done": true}),
        },
        &[],
    )
    .await;
    let result = db
        .store
        .workflow_result(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(result.workflow.state, WorkflowState::Succeeded);
    assert_eq!(result.workflow.revision, 2);
    assert!(result.workflow.activation_id.is_none());
    assert_json_eq!(
        &result.outcome,
        Some(WorkflowOutcome::Succeeded {
            output: json!({"done": true})
        })
    );
    result.validate().unwrap();
    db.finish().await;
}
async fn report_success_and_apply_child(store: &PostgresStore, assigned: &Assignment) {
    report(store, assigned, json!({"issued": true})).await;
    store.apply_work(&one_work(store).await, &[]).await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn continue_preserves_child_receipts_and_completion_before_suspend() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "wait".into(),
            commands: vec![child("issue")],
        },
        &[resolved("issue")],
    )
    .await;
    let second = acquire(&db.store, "python").await;
    let frozen = db
        .store
        .activation_context(&second.lease.owner)
        .await
        .unwrap();
    assert!(frozen.inputs.is_empty());
    let child_task = acquire(&db.store, "children").await;
    report_success_and_apply_child(&db.store, &child_task).await;
    let still_frozen = db
        .store
        .activation_context(&second.lease.owner)
        .await
        .unwrap();
    assert_json_eq!(frozen, still_frozen);
    let snapshot = db
        .store
        .workflow_status(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(snapshot.revision, 1);
    assert_eq!(
        snapshot.activation_id.as_deref(),
        Some(second.lease.owner.task_id.as_str())
    );
    let second_decision = decision(
        &second,
        1,
        WorkflowAction::Suspend {
            state: json!({"ready": true}),
            continuation: "finish".into(),
            commands: vec![child("issue")],
            until: vec!["issue".into()],
        },
    );
    report(
        &db.store,
        &second,
        serde_json::to_value(second_decision).unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert_eq!(work.resolved_children.len(), 1);
    assert_eq!(work.resolved_children[0].key, "issue");
    assert_eq!(work.resolved_children[0].descriptor, descriptor());
    let mut losing = resolved("issue");
    losing.descriptor.digest.0 = format!("sha256:{}", "c".repeat(64));
    let progress = db.store.apply_work(&work, &[losing]).await.unwrap();
    assert_eq!(progress.children_scheduled, 0);
    assert_eq!(progress.activations_scheduled, 1);
    let third = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&third.lease.owner)
        .await
        .unwrap();
    assert_eq!(
        context.inputs["issue"].task_id,
        child_task.lease.owner.task_id
    );
    // Explicitly awaiting the same completed child again is allowed even after its input was consumed.
    apply_decision(
        &db.store,
        &third,
        2,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "finish-again".into(),
            commands: vec![],
            until: vec!["issue".into()],
        },
        &[],
    )
    .await;
    let fourth = acquire(&db.store, "python").await;
    assert_eq!(
        db.store
            .activation_context(&fourth.lease.owner)
            .await
            .unwrap()
            .inputs["issue"]
            .task_id,
        child_task.lease.owner.task_id
    );
    let child_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_task_links WHERE workflow_id=$1 AND NOT is_activation",
    )
    .bind(&workflow.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(child_count, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn coordinator_claim_recovery_fences_old_token_and_commit_replay() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    report(
        &db.store,
        &controller,
        serde_json::to_value(decision(
            &controller,
            0,
            WorkflowAction::Suspend {
                state: Value::Null,
                continuation: "finish".into(),
                commands: vec![child("issue")],
                until: vec!["issue".into()],
            },
        ))
        .unwrap(),
    )
    .await;
    let old = one_work(&db.store).await;
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    sqlx::query("UPDATE workflow_work SET lease_until_ms=0 WHERE id=$1")
        .bind(&old.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let recovered = one_work(&db.store).await;
    assert_ne!(old.token, recovered.token);
    assert!(matches!(
        db.store.apply_work(&old, &[resolved("issue")]).await,
        Err(ContractError::OwnershipLost)
    ));
    assert_eq!(
        db.store
            .apply_work(&recovered, &[resolved("issue")])
            .await
            .unwrap()
            .children_scheduled,
        1
    );
    assert_json_eq!(
        db.store
            .apply_work(&recovered, &[resolved("issue")])
            .await
            .unwrap(),
        WorkflowProgress::default()
    );
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    assert!(reopened.claim_work(16).await.unwrap().is_empty());
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn decision_rejection_rolls_back_all_children_and_drains_visibly() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    report(
        &db.store,
        &controller,
        serde_json::to_value(decision(
            &controller,
            0,
            WorkflowAction::Suspend {
                state: json!({"partial": true}),
                continuation: "bad".into(),
                commands: vec![child("first"), child("second")],
                until: vec!["unknown".into()],
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store
            .apply_work(&work, &[resolved("first"), resolved("second")])
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_task_links WHERE workflow_id=$1 AND NOT is_activation",
    )
    .bind(&workflow.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .revision,
        0
    );
    db.store
        .reject_work(
            &work,
            &ApplicationError {
                kind: "invalid_decision".into(),
                message: "unknown wait member".into(),
            },
        )
        .await
        .unwrap();
    drain_available(&db.store).await;
    let result = db
        .store
        .workflow_result(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(result.workflow.state, WorkflowState::Failed);
    assert!(matches!(
        result.outcome,
        Some(WorkflowOutcome::Failed { .. })
    ));
    result.validate().unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_while_waiting_drains_queued_and_running_children() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &controller,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "finish".into(),
            commands: vec![child("queued"), child("active")],
            until: vec!["queued".into(), "active".into()],
        },
        &[resolved("queued"), resolved("active")],
    )
    .await;
    let running = acquire(&db.store, "children").await;
    dispatched(&db.store, &running).await;
    let cancelling = db
        .store
        .cancel_workflow(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(cancelling.state, WorkflowState::Cancelling);
    drain_available(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelling
    );
    assert!(
        db.store
            .inspect(&scope(), &running.lease.owner.task_id)
            .await
            .unwrap()
            .cancel_requested_at
            .is_some()
    );
    assert_eq!(
        db.store
            .settle(&completed(&running, Quiescence::Confirmed, Value::Null))
            .await
            .unwrap()
            .task_state,
        TaskState::Cancelled
    );
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE workflow_id=$1 AND processed_at_ms IS NULL").bind(&workflow.workflow_id).execute(&db.store.pool).await.unwrap();
    drain_available(&db.store).await;
    let result = db
        .store
        .workflow_result(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(result.workflow.state, WorkflowState::Cancelled);
    assert_json_eq!(&result.outcome, Some(WorkflowOutcome::Cancelled {}));
    result.validate().unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn local_journal_survives_retry_and_exact_receipts_outlive_authority() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    dispatched(&db.store, &first).await;
    let accepted = local(&first, "committed");
    db.store.record_local_result(&accepted).await.unwrap();
    let retryable = SettleCommand {
        owner: first.lease.owner.clone(),
        operation_id: "failed_activation".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            context: Box::new(ExecutionContext {
                identity: InvocationIdentity::from(&first.event),
                program: first.descriptor.program.clone(),
                digest: first.descriptor.digest.clone(),
            }),
            error: Error::new(ErrorKind::Runtime, "retry me"),
            phase: Phase::Execution,
            cleanup_error: None,
            execution_may_have_started: true,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    };
    assert_eq!(
        db.store.settle(&retryable).await.unwrap().task_state,
        TaskState::Queued
    );
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    let second = acquire(&db.store, "python").await;
    assert_eq!(first.lease.owner.task_id, second.lease.owner.task_id);
    assert_ne!(first.lease.owner.attempt_id, second.lease.owner.attempt_id);
    assert_eq!(
        db.store
            .activation_context(&second.lease.owner)
            .await
            .unwrap()
            .local_steps,
        vec![accepted.record.clone()]
    );
    assert!(
        db.store
            .record_local_result(&accepted)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store
            .record_local_result(&local(&first, "stale-new"))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    let mut changed_owner = accepted.clone();
    changed_owner.owner.lease_id = "other".into();
    assert!(matches!(
        db.store.record_local_result(&changed_owner).await,
        Err(ContractError::OwnershipLost)
    ));
    dispatched(&db.store, &second).await;
    let mut second_receipt = accepted.clone();
    second_receipt.owner = second.lease.owner.clone();
    assert!(
        db.store
            .record_local_result(&second_receipt)
            .await
            .unwrap()
            .already_accepted
    );
    let mut changed = accepted.clone();
    changed.record.output = json!({"total": 12});
    assert!(matches!(
        db.store.record_local_result(&changed).await,
        Err(ContractError::Conflict)
    ));
    db.store
        .cancel_workflow(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert!(
        db.store
            .record_local_result(&accepted)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store.record_local_result(&second_receipt).await,
        Err(ContractError::OwnershipLost)
    ));
    assert!(matches!(
        db.store
            .record_local_result(&local(&second, "after-cancel"))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn compact_reads_do_not_decode_submission_checkpoint_or_controller_payloads() {
    let db = TestDb::new().await;
    let mut command = command();
    command.input.correlation_key = Some("INV-1042".into());
    let workflow = db
        .store
        .accept_resolved_workflow(&command, &descriptor())
        .await
        .unwrap();
    // Deliberately corrupt excluded columns: this verifies the compact read path
    // has no hidden dependency on decoding large application payloads.
    sqlx::query("UPDATE workflow_runs SET submission_bytes=$2,controller_bytes=$2,checkpoint_bytes=$2 WHERE workflow_id=$1").bind(&workflow.workflow_id).bind(b"invalid".as_slice()).execute(&db.store.pool).await.unwrap();
    assert_json_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap(),
        &workflow
    );
    assert_json_eq!(
        db.store
            .lookup_workflow_submission(&scope(), &command.idempotency_key)
            .await
            .unwrap()
            .unwrap(),
        &workflow
    );
    let result = db
        .store
        .workflow_result(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_json_eq!(result.workflow, &workflow);
    assert!(result.outcome.is_none());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_fan_in_completion_schedules_exactly_one_activation() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &controller,
        0,
        WorkflowAction::Suspend {
            state: json!({"stage": 1}),
            continuation: "joined".into(),
            commands: vec![child("a"), child("b")],
            until: vec!["a".into(), "b".into()],
        },
        &[resolved("a"), resolved("b")],
    )
    .await;
    let a = acquire(&db.store, "children").await;
    let b = acquire(&db.store, "children").await;
    report(&db.store, &a, json!("a")).await;
    report(&db.store, &b, json!("b")).await;
    let work = db.store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 2);
    assert!(work.iter().all(|work| work.outcome.is_none()));
    let (first, second) = tokio::join!(
        db.store.apply_work(&work[0], &[]),
        db.store.apply_work(&work[1], &[])
    );
    assert_eq!(
        first.unwrap().activations_scheduled + second.unwrap().activations_scheduled,
        1
    );
    let context = db
        .store
        .activation_context(&acquire(&db.store, "python").await.lease.owner)
        .await
        .unwrap();
    assert_eq!(context.inputs.len(), 2);
    assert_eq!(context.revision, 1);
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .revision,
        1
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn malformed_revision_is_permanent_rejection_not_lost_coordinator_authority() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    report(
        &db.store,
        &controller,
        serde_json::to_value(decision(
            &controller,
            42,
            WorkflowAction::Complete {
                output: Value::Null,
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::Conflict)
    ));
    db.store
        .reject_work(
            &work,
            &ApplicationError {
                kind: "invalid_revision".into(),
                message: "controller revision differs from immutable activation".into(),
            },
        )
        .await
        .unwrap();
    drain_available(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Failed
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn complete_cannot_orphan_child_and_intentional_fail_advances_revision() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "next".into(),
            commands: vec![child("outstanding")],
        },
        &[resolved("outstanding")],
    )
    .await;
    let second = acquire(&db.store, "python").await;
    report(
        &db.store,
        &second,
        serde_json::to_value(decision(
            &second,
            1,
            WorkflowAction::Complete {
                output: Value::Null,
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .revision,
        1
    );
    db.store
        .reject_work(
            &work,
            &ApplicationError {
                kind: "unfinished_children".into(),
                message: "child remains queued".into(),
            },
        )
        .await
        .unwrap();
    drain_available(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Failed
    );
    let mut second_command = command();
    second_command.idempotency_key = "intentional-failure".into();
    let other = db
        .store
        .accept_resolved_workflow(&second_command, &descriptor())
        .await
        .unwrap();
    let activation = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &activation,
        0,
        WorkflowAction::Fail {
            error: ApplicationError {
                kind: "business_rejection".into(),
                message: "invoice cannot be issued".into(),
            },
        },
        &[],
    )
    .await;
    drain_available(&db.store).await;
    let result = db
        .store
        .workflow_result(&scope(), &other.workflow_id)
        .await
        .unwrap();
    assert_eq!(result.workflow.revision, 1);
    assert_eq!(result.workflow.state, WorkflowState::Failed);
    result.validate().unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn local_ledger_limits_are_atomic_and_exact_receipt_ignores_expiry() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &controller).await;
    let original = local(&controller, "0");
    db.store.record_local_result(&original).await.unwrap();
    for i in 1..WORKFLOW_MAX_LOCAL_STEPS {
        db.store
            .record_local_result(&local(&controller, &i.to_string()))
            .await
            .unwrap();
    }
    assert!(matches!(
        db.store
            .record_local_result(&local(&controller, "overflow"))
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        db.store
            .activation_context(&controller.lease.owner)
            .await
            .unwrap()
            .local_steps
            .len(),
        WORKFLOW_MAX_LOCAL_STEPS
    );
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .revision,
        0
    );
    sqlx::query("UPDATE attempts SET expires_at_ms=0 WHERE attempt_id=$1")
        .bind(&controller.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(
        db.store
            .record_local_result(&original)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store
            .record_local_result(&local(&controller, "expired"))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    let mut second_command = command();
    second_command.idempotency_key = "ledger-bytes".into();
    db.store
        .accept_resolved_workflow(&second_command, &descriptor())
        .await
        .unwrap();
    let large_controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &large_controller).await;
    let mut large = local(&large_controller, "large-a");
    large.record.output = json!("x".repeat(120 * 1024));
    db.store.record_local_result(&large).await.unwrap();
    large.record.key = "large-b".into();
    db.store.record_local_result(&large).await.unwrap();
    large.record.key = "large-c".into();
    assert!(matches!(
        db.store.record_local_result(&large).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        db.store
            .activation_context(&large_controller.lease.owner)
            .await
            .unwrap()
            .local_steps
            .len(),
        2
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn child_completion_and_local_receipt_do_not_hydrate_unneeded_payloads() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &controller,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "next".into(),
            commands: vec![child("issued")],
        },
        &[resolved("issued")],
    )
    .await;
    let current = acquire(&db.store, "python").await;
    dispatched(&db.store, &current).await;
    let child = acquire(&db.store, "children").await;
    report(
        &db.store,
        &child,
        json!({"payload":"large in real workloads"}),
    )
    .await;
    // These payloads are irrelevant to completion bookkeeping and local-result
    // acceptance. Invalid bytes make accidental hydration observable.
    sqlx::query("UPDATE accepted_settlements SET accepted_command=$2 WHERE attempt_id=$1")
        .bind(&child.lease.owner.attempt_id)
        .bind(b"invalid".as_slice())
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_runs SET submission_bytes=$2,controller_bytes=$2,checkpoint_bytes=$2 WHERE workflow_id=$1").bind(&workflow.workflow_id).bind(b"invalid".as_slice()).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET input_bytes=$2 WHERE task_id=$1")
        .bind(&current.lease.owner.task_id)
        .bind(b"invalid".as_slice())
        .execute(&db.store.pool)
        .await
        .unwrap();
    let work = one_work(&db.store).await;
    assert!(work.outcome.is_none());
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    assert!(
        !db.store
            .record_local_result(&local(&current, "record"))
            .await
            .unwrap()
            .already_accepted
    );
    db.finish().await;
}
