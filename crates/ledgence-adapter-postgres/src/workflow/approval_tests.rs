use super::*;

fn action() -> ApprovalAction {
    ApprovalAction {
        name: "app:send".into(),
        version: "v1".into(),
        arguments: json!({"recipient":"a@example.test","count":1,"amount":9007199254740993_u64,"note":"a\u{0000}b"}),
    }
}
fn ordinary_event(workflow: &WorkflowSnapshot, key: &str) -> WorkflowEventCommand {
    WorkflowEventCommand {scope:scope(),workflow_id:workflow.workflow_id.clone(),key:key.into(),event:WorkflowEvent::new(json!({"specversion":"1.0","id":format!("evt_{key}"),"source":"urn:test","type":"approved","datacontenttype":"application/json","data":{"approved":true}})).unwrap()}
}
fn wait(key: &str, timeout_ms: u64) -> WorkflowAction {
    WorkflowAction::Wait {
        state: json!({"checkpoint":"saved"}),
        continuation: "after_review".into(),
        commands: vec![],
        wait: WorkflowWait::Approval {
            key: key.into(),
            action: action(),
            proposed_arguments: Some(json!({"count":3})),
            timeout_ms,
        },
    }
}
fn decide(
    workflow: &WorkflowSnapshot,
    activation: &Assignment,
    key: &str,
) -> ApprovalDecisionCommand {
    ApprovalDecisionCommand {
        scope: scope(),
        workflow_id: workflow.workflow_id.clone(),
        key: key.into(),
        activation_id: activation.lease.owner.task_id.clone(),
        revision: 0,
        action: action(),
        decision_id: format!("review_{key}"),
        decision: ApprovalDecision::Approve,
        reviewer: "operator@example.test".into(),
        reason: Some("Reviewed effective arguments".into()),
    }
}
async fn inspect(
    store: &PostgresStore,
    workflow: &WorkflowSnapshot,
    key: &str,
) -> ApprovalSnapshot {
    store
        .approval(&scope(), &workflow.workflow_id, key)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn request_precedes_decision_and_restart_preserves_exact_logical_consume() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    let command = decide(&workflow, &initial, "send");
    assert!(matches!(
        db.store.decide_approval(&command).await,
        Err(ContractError::NotFound)
    ));
    let progress = apply_decision(&db.store, &initial, 0, wait("send", 60_000), &[]).await;
    assert_eq!(progress.activations_scheduled, 0);
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    let pending = inspect(&db.store, &workflow, "send").await;
    assert_eq!(pending.status, ApprovalStatus::Pending);
    assert_eq!(pending.revision, 0);
    assert_eq!(pending.action.arguments["count"], json!(1));
    assert_eq!(pending.proposed_arguments, Some(json!({"count":3})));
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    let running: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM tasks WHERE workflow_id=$1 AND state IN ('queued','active')",
    )
    .bind(&workflow.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(running, 0);
    let mut changed = command.clone();
    changed.action.arguments["count"] = json!(1.0);
    assert!(matches!(
        db.store.decide_approval(&changed).await,
        Err(ContractError::Conflict)
    ));
    changed = command.clone();
    changed.revision = 1;
    assert!(matches!(
        db.store.decide_approval(&changed).await,
        Err(ContractError::Conflict)
    ));
    changed = command.clone();
    changed.scope.namespace = "foreign".into();
    assert!(matches!(
        db.store.decide_approval(&changed).await,
        Err(ContractError::NotFound)
    ));
    assert!(matches!(
        db.store
            .send_workflow_event(&ordinary_event(&workflow, "send"))
            .await,
        Err(ContractError::ObsoleteOperation)
    ));
    let first = db.store.decide_approval(&command).await.unwrap();
    assert!(!first.already_accepted);
    let recovered = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let replay = recovered.decide_approval(&command).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.approval.decision, first.approval.decision);
    changed = command.clone();
    changed.decision = ApprovalDecision::Reject;
    assert!(matches!(
        recovered.decide_approval(&changed).await,
        Err(ContractError::Conflict)
    ));
    let work = one_work(&recovered).await;
    assert_eq!(
        recovered
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    assert_eq!(
        recovered
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let resumed = acquire(&recovered, "python").await;
    let frozen = recovered
        .activation_context(&resumed.lease.owner)
        .await
        .unwrap();
    let Some(WorkflowWake::Approval { approval }) = &frozen.wake else {
        panic!("missing approval")
    };
    assert_eq!(
        approval.resumed_activation_id.as_deref(),
        Some(resumed.lease.owner.task_id.as_str())
    );
    assert_eq!(approval.status, ApprovalStatus::Approved);
    assert!(approval.action.matches(&command.action).unwrap());
    assert_eq!(
        approval.action.arguments["amount"],
        json!(9007199254740993_u64)
    );
    let metadata:Vec<u8>=sqlx::query_scalar("SELECT metadata_bytes FROM workflow_explorer_records WHERE activation_id=$1 AND kind='external_wait'")
        .bind(&initial.lease.owner.task_id).fetch_one(&recovered.pool).await.unwrap();
    let graph: ledgence_orchestration_api::console::ConsoleExplorerData =
        codec::decode(&metadata).unwrap();
    assert!(
        matches!(graph,ledgence_orchestration_api::console::ConsoleExplorerData::ExternalWait{wait_kind:ledgence_orchestration_api::console::ConsoleWaitKind::Approval,wake_reason:Some(ledgence_orchestration_api::console::ConsoleWakeReason::Approved),resumed_activation_id:Some(id),..} if id==resumed.lease.owner.task_id)
    );
    assert!(!String::from_utf8(metadata).unwrap().contains("recipient"));
    dispatched(&recovered, &resumed).await;
    let accepted = local(&resumed, "approved:send");
    recovered.record_local_result(&accepted).await.unwrap();
    let retry = SettleCommand {
        owner: resumed.lease.owner.clone(),
        operation_id: "retry".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            observations: None,
            context: Box::new(ExecutionContext {
                identity: InvocationIdentity::from(&resumed.event),
                program: resumed.descriptor.program.clone(),
                digest: resumed.descriptor.digest.clone(),
            }),
            error: Error::new(ErrorKind::Runtime, "retry"),
            phase: Phase::Execution,
            cleanup_error: None,
            execution_may_have_started: true,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    };
    assert_eq!(
        recovered.settle(&retry).await.unwrap().task_state,
        TaskState::Queued
    );
    let again = acquire(&recovered, "python").await;
    let retried = recovered
        .activation_context(&again.lease.owner)
        .await
        .unwrap();
    assert_eq!(retried.wake, frozen.wake);
    assert_eq!(retried.local_steps, vec![accepted.record]);
    assert_eq!(again.lease.owner.task_id, resumed.lease.owner.task_id);
    assert_ne!(again.lease.owner.attempt_id, resumed.lease.owner.attempt_id);
    apply_decision(
        &recovered,
        &again,
        1,
        WorkflowAction::Complete {
            output: json!("done"),
        },
        &[],
    )
    .await;
    assert!(
        recovered
            .decide_approval(&command)
            .await
            .unwrap()
            .already_accepted
    );
    recovered.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn zero_deadline_freezes_expired_wake_and_cancellation_closes_pending_request() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    let command = decide(&workflow, &initial, "expire");
    let progress = apply_decision(&db.store, &initial, 0, wait("expire", 0), &[]).await;
    assert_eq!(progress.activations_scheduled, 1);
    let expired = inspect(&db.store, &workflow, "expire").await;
    assert_eq!(expired.created_at, expired.deadline);
    assert_eq!(expired.status, ApprovalStatus::Expired);
    assert!(matches!(
        db.store.decide_approval(&command).await,
        Err(ContractError::ObsoleteOperation)
    ));
    let resumed = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&resumed.lease.owner)
        .await
        .unwrap();
    assert!(
        matches!(context.wake,Some(WorkflowWake::Approval{approval}) if approval.status==ApprovalStatus::Expired && approval.decision.is_none())
    );
    apply_decision(&db.store, &resumed, 1, wait("cancel", 60_000), &[]).await;
    db.store
        .cancel_workflow(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(
        inspect(&db.store, &workflow, "cancel").await.status,
        ApprovalStatus::Cancelled
    );
    let mut cancel = decide(&workflow, &resumed, "cancel");
    cancel.revision = 1;
    assert!(matches!(
        db.store.decide_approval(&cancel).await,
        Err(ContractError::ObsoleteOperation)
    ));
    drain_available(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    let page = db
        .store
        .list_approvals(&scope(), &workflow.workflow_id, None, 1)
        .await
        .unwrap();
    assert_eq!(page.items[0].key, "cancel");
    assert_eq!(page.next_cursor.as_deref(), Some("cancel"));
    let last = db
        .store
        .list_approvals(
            &scope(),
            &workflow.workflow_id,
            page.next_cursor.as_deref(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(last.items[0].key, "expire");
    assert!(last.next_cursor.is_none());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_opposing_decisions_and_cancel_are_serialized() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    apply_decision(&db.store, &initial, 0, wait("race", 60_000), &[]).await;
    let approve = decide(&workflow, &initial, "race");
    let mut reject = approve.clone();
    reject.decision = ApprovalDecision::Reject;
    reject.decision_id = "reject".into();
    let (a, b) = tokio::join!(
        db.store.decide_approval(&approve),
        db.store.decide_approval(&reject)
    );
    assert!(matches!(
        (&a, &b),
        (Ok(_), Err(ContractError::Conflict)) | (Err(ContractError::Conflict), Ok(_))
    ));
    drain_available(&db.store).await;
    let resumed = acquire(&db.store, "python").await;
    let expected = if a.is_ok() {
        ApprovalStatus::Approved
    } else {
        ApprovalStatus::Rejected
    };
    assert!(
        matches!(db.store.activation_context(&resumed.lease.owner).await.unwrap().wake,Some(WorkflowWake::Approval{approval}) if approval.status==expected)
    );
    apply_decision(&db.store, &resumed, 1, wait("cancel-race", 60_000), &[]).await;
    let mut command = decide(&workflow, &resumed, "cancel-race");
    command.revision = 1;
    let fixture_scope = scope();
    let (decision, cancel) = tokio::join!(
        db.store.decide_approval(&command),
        db.store
            .cancel_workflow(&fixture_scope, &workflow.workflow_id)
    );
    cancel.unwrap();
    let snapshot = inspect(&db.store, &workflow, "cancel-race").await;
    match decision {
        Ok(_) => {
            assert_eq!(snapshot.status, ApprovalStatus::Approved);
            assert!(
                db.store
                    .decide_approval(&command)
                    .await
                    .unwrap()
                    .already_accepted
            );
        }
        Err(ContractError::ObsoleteOperation) => {
            assert_eq!(snapshot.status, ApprovalStatus::Cancelled)
        }
        other => panic!("unexpected race {other:?}"),
    }
    assert!(snapshot.resumed_activation_id.is_none());
    drain_available(&db.store).await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn buffered_ordinary_event_cannot_become_an_approval() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    db.store
        .send_workflow_event(&ordinary_event(&workflow, "collision"))
        .await
        .unwrap();
    report(
        &db.store,
        &initial,
        serde_json::to_value(decision(&initial, 0, wait("collision", 60_000))).unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        db.store
            .approval(&scope(), &workflow.workflow_id, "collision")
            .await,
        Err(ContractError::NotFound)
    ));
    db.store
        .reject_work(
            &work,
            &ApplicationError {
                kind: "invalid".into(),
                message: "collision".into(),
            },
        )
        .await
        .unwrap();
    drain_available(&db.store).await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn one_shot_keys_retained_decisions_and_bounded_retention() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    apply_decision(&db.store, &initial, 0, wait("send", 60_000), &[]).await;
    let command = decide(&workflow, &initial, "send");
    db.store.decide_approval(&command).await.unwrap();
    drain_available(&db.store).await;
    let resumed = acquire(&db.store, "python").await;
    report(
        &db.store,
        &resumed,
        serde_json::to_value(decision(&resumed, 1, wait("send", 60_000))).unwrap(),
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
                kind: "reused".into(),
                message: "one-shot".into(),
            },
        )
        .await
        .unwrap();
    drain_available(&db.store).await;
    assert!(
        db.store
            .decide_approval(&command)
            .await
            .unwrap()
            .already_accepted
    );
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE workflow_id=$1")
        .bind(&workflow.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2,cancel_requested_at_ms=CASE WHEN cancel_requested_at_ms IS NOT NULL THEN 2 ELSE NULL END WHERE workflow_id=$1")
        .bind(&workflow.workflow_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE attempts SET finished_at_ms=2 WHERE task_id IN (SELECT task_id FROM tasks WHERE workflow_id=$1) AND finished_at_ms IS NOT NULL")
        .bind(&workflow.workflow_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE accepted_settlements SET accepted_at=2 WHERE attempt_id IN (SELECT attempt_id FROM attempts WHERE task_id IN (SELECT task_id FROM tasks WHERE workflow_id=$1))")
        .bind(&workflow.workflow_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0")
        .execute(&db.store.pool)
        .await
        .unwrap();
    // Three retention lanes and separate discovery/collection cursors rotate
    // across both activation tasks; a one-row page deliberately needs many turns.
    for _ in 0..600 {
        let progress = db
            .store
            .retain_batch(
                &scope(),
                &RetentionPolicy {
                    batch_size: 1,
                    ..Default::default()
                },
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(progress.deleted_rows <= 16);
    }
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM workflow_approvals),(SELECT count(*) FROM workflow_runs)",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(counts, (0, 0));
    assert!(matches!(
        db.store.decide_approval(&command).await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn approval_pages_use_utf8_binary_order_for_unicode_and_punctuation() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let keys = ["é", "Z", "!", "😀", "中", "a"];
    for (revision, key) in keys.iter().enumerate() {
        let assigned = acquire(&db.store, "python").await;
        apply_decision(&db.store, &assigned, revision as u64, wait(key, 0), &[]).await;
    }
    let mut expected = keys.iter().map(|key| key.to_string()).collect::<Vec<_>>();
    expected.sort();
    let mut actual = Vec::new();
    let mut cursor = None;
    loop {
        let page = db
            .store
            .list_approvals(&scope(), &workflow.workflow_id, cursor.as_deref(), 1)
            .await
            .unwrap();
        assert!(page.matches(&scope(), &workflow.workflow_id, cursor.as_deref(), 1));
        actual.extend(page.items.iter().map(|item| item.key.clone()));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(actual, expected);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn inspection_and_pages_serialize_authoritative_status_with_decisions() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    apply_decision(&db.store, &initial, 0, wait("review", 60_000), &[]).await;
    for list in [false, true] {
        // Stop the read after it acquires workflow authority but before it can
        // fetch request bytes, without sleeps or relying on query timing.
        let mut blocker = db.store.pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE workflow_approvals IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let store = db.store.clone();
        let id = workflow.workflow_id.clone();
        let reader = tokio::spawn(async move {
            if list {
                store
                    .list_approvals(&scope(), &id, None, 1)
                    .await
                    .map(|page| page.items[0].clone())
            } else {
                store.approval(&scope(), &id, "review").await
            }
        });
        tokio::time::timeout(Duration::from_secs(5),async {
            loop {
                let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT snapshot_bytes FROM workflow_approvals%')")
                    .fetch_one(&db.store.pool).await.unwrap();
                if blocked {break;}
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let mutation = sqlx::query(
            "SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE NOWAIT",
        )
        .bind(&workflow.workflow_id)
        .fetch_one(&db.store.pool)
        .await;
        let error = mutation.unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("55P03")
        );
        blocker.commit().await.unwrap();
        assert_eq!(
            reader.await.unwrap().unwrap().status,
            ApprovalStatus::Pending
        );
    }
    db.store
        .decide_approval(&decide(&workflow, &initial, "review"))
        .await
        .unwrap();
    assert_eq!(
        inspect(&db.store, &workflow, "review").await.status,
        ApprovalStatus::Approved
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn overdue_reads_remain_pending_until_expiry_is_durably_applied() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    let progress = apply_decision(&db.store, &initial, 0, wait("overdue", 25), &[]).await;
    assert_eq!(progress.activations_scheduled, 0);
    let pending = inspect(&db.store, &workflow, "overdue").await;
    assert_eq!(pending.status, ApprovalStatus::Pending);
    // Deliberately leave the durable coordinator idle until the database clock
    // reaches the deadline. Observation alone must not invent an expiry record.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let now: i64 = sqlx::query_scalar(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            )
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
            if now as u64 >= pending.deadline {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(inspect(&db.store, &workflow, "overdue").await, pending);
    let page = db
        .store
        .list_approvals(&scope(), &workflow.workflow_id, None, 10)
        .await
        .unwrap();
    assert_eq!(page.items, vec![pending.clone()]);
    assert!(matches!(
        db.store
            .decide_approval(&decide(&workflow, &initial, "overdue"))
            .await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert_eq!(inspect(&db.store, &workflow, "overdue").await, pending);
    let work = one_work(&db.store).await;
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let expired = inspect(&db.store, &workflow, "overdue").await;
    assert_eq!(expired.status, ApprovalStatus::Expired);
    assert!(expired.decision.is_none());
    let resumed = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&resumed.lease.owner)
        .await
        .unwrap();
    assert_eq!(
        context.wake,
        Some(WorkflowWake::Approval {
            approval: Box::new(expired.clone())
        })
    );
    let page = db
        .store
        .list_approvals(&scope(), &workflow.workflow_id, None, 10)
        .await
        .unwrap();
    assert_eq!(page.items, vec![expired]);
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    let activations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
            .bind(&workflow.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(activations, 2);
    db.finish().await;
}

// Exercise both serialization orders after the approval wake has already been
// claimed. Root cancellation propagates through owned-child work, so fencing
// only the root's current activation would not protect this resumed child.
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn root_cancellation_fences_claimed_owned_approval_resume() {
    for resume_first in [false, true] {
        let db = TestDb::new().await;
        let root = start(&db.store).await;
        let initial = acquire(&db.store, "python").await;
        let mut command = child("reviewed-child");
        command.kind = WorkflowChildKind::Workflow;
        command.queue = "subflows".into();
        let resolved = ResolvedWorkflowChild {
            kind: WorkflowChildKind::Workflow,
            ..resolved("reviewed-child")
        };
        apply_decision(
            &db.store,
            &initial,
            0,
            WorkflowAction::Suspend {
                state: Value::Null,
                continuation: "joined".into(),
                commands: vec![command],
                until: vec!["reviewed-child".into()],
            },
            &[resolved],
        )
        .await;
        let child_activation = acquire(&db.store, "subflows").await;
        let child_id = child_activation.event.value()["ldgworkflowid"]
            .as_str()
            .unwrap();
        let child = db.store.workflow_status(&scope(), child_id).await.unwrap();
        apply_decision(&db.store, &child_activation, 0, wait("send", 60_000), &[]).await;
        let approval_command = decide(&child, &child_activation, "send");
        db.store.decide_approval(&approval_command).await.unwrap();
        let claimed = one_work(&db.store).await;
        if resume_first {
            assert_eq!(
                db.store
                    .apply_work(&claimed, &[])
                    .await
                    .unwrap()
                    .activations_scheduled,
                1
            );
        }
        db.store
            .cancel_workflow(&scope(), &root.workflow_id)
            .await
            .unwrap();
        super::owned::recover(&db.store).await;
        assert_eq!(
            db.store
                .apply_work(&claimed, &[])
                .await
                .unwrap()
                .activations_scheduled,
            0
        );
        super::owned::recover(&db.store).await;
        for workflow in [&root, &child] {
            assert_eq!(
                db.store
                    .workflow_status(&scope(), &workflow.workflow_id)
                    .await
                    .unwrap()
                    .state,
                WorkflowState::Cancelled
            );
        }
        let approval = inspect(&db.store, &child, "send").await;
        // The accepted human decision remains immutable even when its execution
        // is cancelled; cancellation must not fabricate a different decision.
        assert_eq!(approval.status, ApprovalStatus::Approved);
        assert_eq!(approval.resumed_activation_id.is_some(), resume_first);
        assert!(
            db.store
                .decide_approval(&approval_command)
                .await
                .unwrap()
                .already_accepted
        );
        let activations: i64 =
            sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
                .bind(child_id)
                .fetch_one(&db.store.pool)
                .await
                .unwrap();
        assert_eq!(activations, if resume_first { 2 } else { 1 });
        let unfinished: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks WHERE workflow_id IN ($1,$2) AND state IN ('queued','active')")
            .bind(&root.workflow_id).bind(child_id).fetch_one(&db.store.pool).await.unwrap();
        assert_eq!(unfinished, 0);
        let open_waits: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM workflow_waits WHERE workflow_id=$1 AND closed_at_ms IS NULL",
        )
        .bind(child_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert_eq!(open_waits, 0);
        assert!(db.store.claim_work(16).await.unwrap().is_empty());
        db.finish().await;
    }
}
