//! Owned workflow regressions against independent real PostgreSQL databases.
use super::*;

fn workflow_child(key: &str) -> WorkflowChildCommand {
    let mut command = child(key);
    command.kind = WorkflowChildKind::Workflow;
    command.queue = "subflows".into();
    command
}
fn resolved_workflow(key: &str) -> ResolvedWorkflowChild {
    ResolvedWorkflowChild {
        kind: WorkflowChildKind::Workflow,
        ..resolved(key)
    }
}
fn workflow_input(input: &WorkflowChildResult) -> &WorkflowSubworkflowResult {
    match input {
        WorkflowChildResult::Workflow(result) => result,
        _ => panic!("expected workflow input"),
    }
}
async fn child_id(store: &PostgresStore, parent: &str, key: &str) -> String {
    sqlx::query_scalar("SELECT child_workflow_id FROM owned_workflow_links WHERE parent_workflow_id=$1 AND command_key=$2")
        .bind(parent).bind(key).fetch_one(&store.pool).await.unwrap()
}
async fn make_due(store: &PostgresStore) {
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE processed_at_ms IS NULL AND lease_token IS NULL")
        .execute(&store.pool).await.unwrap();
}
async fn recover(store: &PostgresStore) {
    for _ in 0..80 {
        make_due(store).await;
        let batch = store.claim_work(16).await.unwrap();
        if batch.is_empty() {
            return;
        }
        for work in batch {
            store.apply_work(&work, &[]).await.unwrap();
        }
        let remaining: i64 =
            sqlx::query_scalar("SELECT count(*) FROM workflow_work WHERE processed_at_ms IS NULL")
                .fetch_one(&store.pool)
                .await
                .unwrap();
        if remaining == 0 {
            return;
        }
    }
    panic!("owned workflow recovery exceeded bounded fixture");
}
async fn failed_attempt(store: &PostgresStore, assigned: &Assignment) {
    let failed = SettleCommand {
        owner: assigned.lease.owner.clone(),
        operation_id: "retry-controller".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            context: Box::new(ExecutionContext {
                identity: InvocationIdentity::from(&assigned.event),
                program: assigned.descriptor.program.clone(),
                digest: assigned.descriptor.digest.clone(),
            }),
            error: Error::new(ErrorKind::Runtime, "retry fixture"),
            phase: Phase::Execution,
            cleanup_error: None,
            execution_may_have_started: true,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    };
    assert_eq!(
        store.settle(&failed).await.unwrap().task_state,
        TaskState::Queued
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn mixed_join_waits_for_workflow_terminal_and_freezes_retry_inputs() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: json!({"checkpoint":1}),
            continuation: "joined".into(),
            commands: vec![child("task"), workflow_child("flow")],
            until: vec!["task".into(), "flow".into()],
        },
        &[resolved("task"), resolved_workflow("flow")],
    )
    .await;
    let flow_id = child_id(&db.store, &parent.workflow_id, "flow").await;
    let flow = acquire(&db.store, "subflows").await;
    let flow_context = db
        .store
        .activation_context(&flow.lease.owner)
        .await
        .unwrap();
    assert_eq!(
        flow_context.parent_workflow_id.as_deref(),
        Some(parent.workflow_id.as_str())
    );
    assert_eq!(
        flow_context.root_workflow_id.as_deref(),
        Some(parent.workflow_id.as_str())
    );
    assert_eq!(
        flow.event.value()["ldgparentworkflowid"],
        parent.workflow_id
    );
    assert_eq!(flow.event.value()["ldgrootworkflowid"], parent.workflow_id);
    apply_decision(
        &db.store,
        &flow,
        0,
        WorkflowAction::Continue {
            state: json!({"child":1}),
            continuation: "finish".into(),
            commands: vec![],
        },
        &[],
    )
    .await;
    // A successful child controller checkpoint is not a completed child run.
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    let ordinary = acquire(&db.store, "children").await;
    report(&db.store, &ordinary, json!({"ordinary":true})).await;
    let task_work = one_work(&db.store).await;
    let flow_next = acquire(&db.store, "subflows").await;
    report(
        &db.store,
        &flow_next,
        serde_json::to_value(decision(
            &flow_next,
            1,
            WorkflowAction::Complete {
                output: json!({"nested":true}),
            },
        ))
        .unwrap(),
    )
    .await;
    let child_controller_work = one_work(&db.store).await;
    assert_eq!(
        db.store
            .apply_work(&child_controller_work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let flow_work = one_work(&db.store).await;
    assert_eq!(
        flow_work.source,
        WorkflowWorkSource::WorkflowTerminal {
            workflow_id: flow_id.clone()
        }
    );
    let (a, b) = tokio::join!(
        db.store.apply_work(&task_work, &[]),
        db.store.apply_work(&flow_work, &[])
    );
    assert_eq!(
        a.unwrap().activations_scheduled + b.unwrap().activations_scheduled,
        1
    );
    assert_eq!(
        db.store
            .apply_work(&flow_work, &[])
            .await
            .unwrap()
            .processed,
        0
    );
    let joined = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&joined.lease.owner)
        .await
        .unwrap();
    assert_eq!(context.inputs.len(), 2);
    assert_eq!(
        task_input_id(&context.inputs["task"]),
        ordinary.lease.owner.task_id
    );
    assert_eq!(workflow_input(&context.inputs["flow"]).workflow_id, flow_id);
    assert_eq!(
        workflow_input(&context.inputs["flow"]).state,
        WorkflowState::Succeeded
    );
    failed_attempt(&db.store, &joined).await;
    let retried = acquire(&db.store, "python").await;
    assert_ne!(
        retried.lease.owner.attempt_id,
        joined.lease.owner.attempt_id
    );
    assert_json_eq!(
        db.store
            .activation_context(&retried.lease.owner)
            .await
            .unwrap(),
        context
    );
    apply_decision(
        &db.store,
        &retried,
        1,
        WorkflowAction::Complete {
            output: json!("joined"),
        },
        &[],
    )
    .await;
    let finished = db
        .store
        .workflow_status(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    assert_eq!(finished.state, WorkflowState::Succeeded);
    let (applied_at, processed_at): (i64, i64) = sqlx::query_as("SELECT a.applied_at_ms,w.processed_at_ms FROM workflow_activations a JOIN workflow_work w ON w.task_id=a.task_id AND w.kind='terminal' WHERE a.activation_id=$1")
        .bind(&retried.lease.owner.task_id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(applied_at as u64, finished.terminal_at.unwrap());
    assert_eq!(processed_at as u64, finished.terminal_at.unwrap());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn completion_before_wait_reuses_pinned_binding_and_public_keys_are_separate() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "wait".into(),
            commands: vec![workflow_child("flow")],
        },
        &[resolved_workflow("flow")],
    )
    .await;
    let flow_id = child_id(&db.store, &parent.workflow_id, "flow").await;
    let controller = acquire(&db.store, "subflows").await;
    apply_decision(
        &db.store,
        &controller,
        0,
        WorkflowAction::Complete { output: json!(7) },
        &[],
    )
    .await;
    assert_eq!(
        db.store
            .apply_work(&one_work(&db.store).await, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let second = acquire(&db.store, "python").await;
    report(
        &db.store,
        &second,
        serde_json::to_value(decision(
            &second,
            1,
            WorkflowAction::Suspend {
                state: Value::Null,
                continuation: "finish".into(),
                commands: vec![workflow_child("flow")],
                until: vec!["flow".into()],
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert_eq!(work.resolved_children.len(), 1);
    assert_eq!(work.resolved_children[0].kind, WorkflowChildKind::Workflow);
    assert_eq!(work.resolved_children[0].descriptor, descriptor());
    // Even a changed catalog resolution cannot replace the accepted descriptor.
    let mut changed = resolved_workflow("flow");
    changed.descriptor.digest.0 = format!("sha256:{}", "b".repeat(64));
    let progress = db.store.apply_work(&work, &[changed]).await.unwrap();
    assert_eq!(progress.children_scheduled, 0);
    assert_eq!(progress.activations_scheduled, 1);
    assert_eq!(
        child_id(&db.store, &parent.workflow_id, "flow").await,
        flow_id
    );
    let pinned = db
        .store
        .inspect(&scope(), &controller.lease.owner.task_id)
        .await
        .unwrap();
    assert_eq!(pinned.descriptor, descriptor());
    let nested_bytes: Vec<u8> =
        sqlx::query_scalar("SELECT submission_bytes FROM workflow_runs WHERE workflow_id=$1")
            .bind(&flow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let nested: SubmitCommand = codec::decode(&nested_bytes).unwrap();
    assert!(
        db.store
            .lookup_workflow_submission(&scope(), &nested.idempotency_key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.store
            .replay_workflow_submission(&nested)
            .await
            .unwrap()
            .is_none()
    );
    let root = db
        .store
        .accept_resolved_workflow(&nested, &descriptor())
        .await
        .unwrap();
    assert_ne!(root.workflow_id, flow_id);
    assert!(root.parent_workflow_id.is_none());
    assert_eq!(
        db.store
            .lookup_workflow_submission(&scope(), &nested.idempotency_key)
            .await
            .unwrap()
            .unwrap()
            .workflow_id,
        root.workflow_id
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn owned_keys_reject_changed_kind_or_submission_atomically() {
    for change_kind in [false, true] {
        let db = TestDb::new().await;
        let parent = start(&db.store).await;
        let first = acquire(&db.store, "python").await;
        apply_decision(
            &db.store,
            &first,
            0,
            WorkflowAction::Continue {
                state: Value::Null,
                continuation: "next".into(),
                commands: vec![workflow_child("stable")],
            },
            &[resolved_workflow("stable")],
        )
        .await;
        let second = acquire(&db.store, "python").await;
        let mut altered = workflow_child("stable");
        if change_kind {
            altered.kind = WorkflowChildKind::Task;
        } else {
            altered.data = json!({"changed":true});
        }
        report(
            &db.store,
            &second,
            serde_json::to_value(decision(
                &second,
                1,
                WorkflowAction::Continue {
                    state: json!("must roll back"),
                    continuation: "bad".into(),
                    commands: vec![workflow_child("new-before-error"), altered],
                },
            ))
            .unwrap(),
        )
        .await;
        let work = one_work(&db.store).await;
        assert!(matches!(
            db.store
                .apply_work(&work, &[resolved_workflow("new-before-error")])
                .await,
            Err(ContractError::Conflict)
        ));
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM owned_workflow_links WHERE parent_workflow_id=$1",
        )
        .bind(&parent.workflow_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            db.store
                .workflow_status(&scope(), &parent.workflow_id)
                .await
                .unwrap()
                .revision,
            1
        );
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_drains_descendants_and_retains_running_effect_authority() {
    let db = TestDb::new().await;
    let root = start(&db.store).await;
    let parent = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &parent,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "done".into(),
            commands: vec![workflow_child("child")],
            until: vec!["child".into()],
        },
        &[resolved_workflow("child")],
    )
    .await;
    let child_id = child_id(&db.store, &root.workflow_id, "child").await;
    let child = acquire(&db.store, "subflows").await;
    let mut grandchild = workflow_child("grandchild");
    grandchild.queue = "grandflows".into();
    apply_decision(
        &db.store,
        &child,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "done".into(),
            commands: vec![grandchild, super::child("effect")],
            until: vec!["grandchild".into(), "effect".into()],
        },
        &[resolved_workflow("grandchild"), resolved("effect")],
    )
    .await;
    let grand_id = self::child_id(&db.store, &child_id, "grandchild").await;
    let grand = acquire(&db.store, "grandflows").await;
    apply_decision(
        &db.store,
        &grand,
        0,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "never".into(),
            commands: vec![],
            wait: WorkflowWait::Event {
                key: "release".into(),
                timeout_ms: None,
            },
        },
        &[],
    )
    .await;
    let effect = acquire(&db.store, "children").await;
    assert_eq!(effect.event.value()["ldgworkflowid"], child_id);
    assert_eq!(
        effect.event.value()["ldgparentworkflowid"],
        root.workflow_id
    );
    assert_eq!(effect.event.value()["ldgrootworkflowid"], root.workflow_id);
    assert_eq!(grand.event.value()["ldgparentworkflowid"], child_id);
    assert_eq!(grand.event.value()["ldgrootworkflowid"], root.workflow_id);
    dispatched(&db.store, &effect).await;
    db.store
        .cancel_workflow(&scope(), &root.workflow_id)
        .await
        .unwrap();
    for _ in 0..8 {
        make_due(&db.store).await;
        drain_available(&db.store).await;
    }
    assert_eq!(
        db.store
            .workflow_status(&scope(), &grand_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    assert_eq!(
        db.store
            .workflow_status(&scope(), &root.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelling
    );
    assert_eq!(
        db.store
            .workflow_status(&scope(), &child_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelling
    );
    assert!(
        db.store
            .inspect(&scope(), &effect.lease.owner.task_id)
            .await
            .unwrap()
            .cancel_requested_at
            .is_some()
    );
    assert_eq!(
        db.store
            .settle(&completed(&effect, Quiescence::Confirmed, Value::Null))
            .await
            .unwrap()
            .task_state,
        TaskState::Cancelled
    );
    recover(&db.store).await;
    for id in [&root.workflow_id, &child_id, &grand_id] {
        assert_eq!(
            db.store.workflow_status(&scope(), id).await.unwrap().state,
            WorkflowState::Cancelled
        );
    }
    let root_terminal = db
        .store
        .workflow_status(&scope(), &root.workflow_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    let child_terminal = db
        .store
        .workflow_status(&scope(), &child_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    let grand_terminal = db
        .store
        .workflow_status(&scope(), &grand_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    assert!(root_terminal >= child_terminal && child_terminal >= grand_terminal);
    let late = WorkflowEventCommand { scope:scope(),workflow_id:grand_id,key:"release".into(),event:WorkflowEvent::new(json!({"specversion":"1.0","id":"late","source":"urn:tests","type":"release","datacontenttype":"application/json","data":{}})).unwrap() };
    assert!(matches!(
        db.store.send_workflow_event(&late).await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn early_parent_complete_rejects_and_failure_drains_owned_children() {
    for complete in [true, false] {
        let db = TestDb::new().await;
        let parent = start(&db.store).await;
        let first = acquire(&db.store, "python").await;
        apply_decision(
            &db.store,
            &first,
            0,
            WorkflowAction::Continue {
                state: Value::Null,
                continuation: "finish".into(),
                commands: vec![workflow_child("child")],
            },
            &[resolved_workflow("child")],
        )
        .await;
        let child = child_id(&db.store, &parent.workflow_id, "child").await;
        let next = acquire(&db.store, "python").await;
        let error = ApplicationError {
            kind: "parent_failed".into(),
            message: "expected fixture".into(),
        };
        let action = if complete {
            WorkflowAction::Complete {
                output: Value::Null,
            }
        } else {
            WorkflowAction::Fail {
                error: error.clone(),
            }
        };
        report(
            &db.store,
            &next,
            serde_json::to_value(decision(&next, 1, action)).unwrap(),
        )
        .await;
        let work = one_work(&db.store).await;
        if complete {
            assert!(matches!(
                db.store.apply_work(&work, &[]).await,
                Err(ContractError::InvalidInput(_))
            ));
            db.store.reject_work(&work, &error).await.unwrap();
        } else {
            db.store.apply_work(&work, &[]).await.unwrap();
        }
        recover(&db.store).await;
        assert_eq!(
            db.store
                .workflow_status(&scope(), &parent.workflow_id)
                .await
                .unwrap()
                .state,
            WorkflowState::Failed
        );
        assert_eq!(
            db.store
                .workflow_status(&scope(), &child)
                .await
                .unwrap()
                .state,
            WorkflowState::Cancelled
        );
        assert!(
            matches!(db.store.workflow_result(&scope(),&parent.workflow_id).await.unwrap().outcome,Some(WorkflowOutcome::Failed { error:actual }) if actual==error)
        );
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn child_failure_and_independent_cancellation_are_inspectable_parent_results() {
    for cancel in [true, false] {
        let db = TestDb::new().await;
        let parent = start(&db.store).await;
        let first = acquire(&db.store, "python").await;
        apply_decision(
            &db.store,
            &first,
            0,
            WorkflowAction::Suspend {
                state: Value::Null,
                continuation: "inspect".into(),
                commands: vec![workflow_child("child")],
                until: vec!["child".into()],
            },
            &[resolved_workflow("child")],
        )
        .await;
        let child = child_id(&db.store, &parent.workflow_id, "child").await;
        if cancel {
            db.store.cancel_workflow(&scope(), &child).await.unwrap();
        } else {
            let assigned = acquire(&db.store, "subflows").await;
            apply_decision(
                &db.store,
                &assigned,
                0,
                WorkflowAction::Fail {
                    error: ApplicationError {
                        kind: "child_failed".into(),
                        message: "expected".into(),
                    },
                },
                &[],
            )
            .await;
        }
        recover(&db.store).await;
        let next = acquire(&db.store, "python").await;
        let context = db
            .store
            .activation_context(&next.lease.owner)
            .await
            .unwrap();
        assert_eq!(
            workflow_input(&context.inputs["child"]).state,
            if cancel {
                WorkflowState::Cancelled
            } else {
                WorkflowState::Failed
            }
        );
        apply_decision(
            &db.store,
            &next,
            1,
            WorkflowAction::Complete {
                output: json!("handled"),
            },
            &[],
        )
        .await;
        assert_eq!(
            db.store
                .workflow_status(&scope(), &parent.workflow_id)
                .await
                .unwrap()
                .state,
            WorkflowState::Succeeded
        );
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn live_child_limit_rolls_back_and_terminal_children_release_capacity() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    let commands = (0..WORKFLOW_MAX_LIVE_SUBWORKFLOWS)
        .map(|i| workflow_child(&format!("child-{i}")))
        .collect();
    let bindings: Vec<_> = (0..WORKFLOW_MAX_LIVE_SUBWORKFLOWS)
        .map(|i| resolved_workflow(&format!("child-{i}")))
        .collect();
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "more".into(),
            commands,
        },
        &bindings,
    )
    .await;
    let next = acquire(&db.store, "python").await;
    report(
        &db.store,
        &next,
        serde_json::to_value(decision(
            &next,
            1,
            WorkflowAction::Continue {
                state: Value::Null,
                continuation: "next".into(),
                commands: vec![workflow_child("overflow")],
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store
            .apply_work(&work, &[resolved_workflow("overflow")])
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM owned_workflow_links WHERE parent_workflow_id=$1")
            .bind(&parent.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(count, 64);
    let child = acquire(&db.store, "subflows").await;
    apply_decision(
        &db.store,
        &child,
        0,
        WorkflowAction::Complete {
            output: Value::Null,
        },
        &[],
    )
    .await;
    // Its terminal marker is committed before the parent notification is applied.
    assert_eq!(
        db.store
            .apply_work(&work, &[resolved_workflow("overflow")])
            .await
            .unwrap()
            .children_scheduled,
        1
    );
    let counts:(i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE NOT terminal) FROM owned_workflow_links WHERE parent_workflow_id=$1").bind(&parent.workflow_id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (65, 64));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn depth_limit_rejects_the_entire_next_generation() {
    let db = TestDb::new().await;
    let root = start(&db.store).await;
    let mut parent = root.workflow_id.clone();
    for depth in 0..=WORKFLOW_MAX_DEPTH {
        let assigned = acquire(&db.store, if depth == 0 { "python" } else { "subflows" }).await;
        report(
            &db.store,
            &assigned,
            serde_json::to_value(decision(
                &assigned,
                0,
                WorkflowAction::Suspend {
                    state: Value::Null,
                    continuation: "joined".into(),
                    commands: vec![workflow_child("child")],
                    until: vec!["child".into()],
                },
            ))
            .unwrap(),
        )
        .await;
        let work = one_work(&db.store).await;
        let result = db
            .store
            .apply_work(&work, &[resolved_workflow("child")])
            .await;
        if depth == WORKFLOW_MAX_DEPTH {
            assert!(matches!(result, Err(ContractError::InvalidInput(_))));
            assert_eq!(
                db.store
                    .workflow_status(&scope(), &parent)
                    .await
                    .unwrap()
                    .revision,
                0
            );
        } else {
            result.unwrap();
            let child = child_id(&db.store, &parent, "child").await;
            let status = db.store.workflow_status(&scope(), &child).await.unwrap();
            assert_eq!(status.parent_workflow_id.as_deref(), Some(parent.as_str()));
            assert_eq!(
                status.root_workflow_id.as_deref(),
                Some(root.workflow_id.as_str())
            );
            parent = child;
        }
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_runs")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, i64::from(WORKFLOW_MAX_DEPTH) + 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn stale_terminal_work_is_fenced_and_recovered_after_reconnect() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "joined".into(),
            commands: vec![workflow_child("child")],
            until: vec!["child".into()],
        },
        &[resolved_workflow("child")],
    )
    .await;
    let child = acquire(&db.store, "subflows").await;
    apply_decision(
        &db.store,
        &child,
        0,
        WorkflowAction::Complete {
            output: json!("survives"),
        },
        &[],
    )
    .await;
    let stale = one_work(&db.store).await;
    sqlx::query("UPDATE workflow_work SET lease_until_ms=0 WHERE id=$1")
        .bind(&stale.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let reconnected = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let replacement = one_work(&reconnected).await;
    assert_ne!(replacement.token, stale.token);
    assert!(matches!(
        db.store.apply_work(&stale, &[]).await,
        Err(ContractError::OwnershipLost)
    ));
    let mut forged = replacement.clone();
    forged.source = WorkflowWorkSource::Drain;
    assert!(matches!(
        reconnected.apply_work(&forged, &[]).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(
        reconnected
            .apply_work(&replacement, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    assert_eq!(db.store.apply_work(&stale, &[]).await.unwrap().processed, 0);
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .revision,
        1
    );
    reconnected.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn spawning_trace_uses_accepted_attempt_and_lineage_context_is_checked() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    failed_attempt(&db.store, &first).await;
    let retry = acquire(&db.store, "python").await;
    let trace = TraceContext {
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01".into(),
        tracestate: Some("vendor=accepted".into()),
    };
    let mut accepted = completed(
        &retry,
        Quiescence::Confirmed,
        serde_json::to_value(decision(
            &retry,
            0,
            WorkflowAction::Continue {
                state: Value::Null,
                continuation: "next".into(),
                commands: vec![workflow_child("flow"), child("task")],
            },
        ))
        .unwrap(),
    );
    accepted.processing_trace = Some(trace.clone());
    db.store.settle(&accepted).await.unwrap();
    let work = one_work(&db.store).await;
    db.store
        .apply_work(&work, &[resolved_workflow("flow"), resolved("task")])
        .await
        .unwrap();
    let flow = acquire(&db.store, "subflows").await;
    let task = acquire(&db.store, "children").await;
    for assigned in [&flow, &task] {
        assert_eq!(
            db.store
                .inspect(&scope(), &assigned.lease.owner.task_id)
                .await
                .unwrap()
                .origin_trace,
            Some(trace.clone())
        );
    }
    let next = acquire(&db.store, "python").await;
    assert_eq!(
        db.store
            .inspect(&scope(), &next.lease.owner.task_id)
            .await
            .unwrap()
            .origin_trace,
        command().origin_trace
    );
    let mut context = db
        .store
        .activation_context(&flow.lease.owner)
        .await
        .unwrap();
    context.parent_workflow_id = Some("wf_wrong_parent".into());
    sqlx::query("UPDATE workflow_activations SET context_bytes=$2 WHERE activation_id=$1")
        .bind(&flow.lease.owner.task_id)
        .bind(codec::encode(&context).unwrap())
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.activation_context(&flow.lease.owner).await,
        Err(ContractError::Unavailable(_))
    ));
    let original: Vec<u8> =
        sqlx::query_scalar("SELECT submission_bytes FROM workflow_runs WHERE workflow_id=$1")
            .bind(&parent.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(
        codec::decode::<SubmitCommand>(&original)
            .unwrap()
            .origin_trace,
        command().origin_trace
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn child_terminal_commit_does_not_wait_for_its_parents_mutation_lock() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "joined".into(),
            commands: vec![workflow_child("child")],
            until: vec!["child".into()],
        },
        &[resolved_workflow("child")],
    )
    .await;
    let child = acquire(&db.store, "subflows").await;
    report(
        &db.store,
        &child,
        serde_json::to_value(decision(
            &child,
            0,
            WorkflowAction::Complete {
                output: Value::Null,
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    let mut held = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&parent.workflow_id)
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let completion = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        db.store.apply_work(&work, &[]),
    )
    .await;
    held.rollback().await.unwrap();
    completion
        .expect("child terminal commit tried to lock its parent")
        .unwrap();
    assert_eq!(
        db.store
            .apply_work(&one_work(&db.store).await, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_wins_over_previously_claimed_spawning_decision() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    report(
        &db.store,
        &first,
        serde_json::to_value(decision(
            &first,
            0,
            WorkflowAction::Suspend {
                state: Value::Null,
                continuation: "joined".into(),
                commands: vec![workflow_child("must-not-spawn")],
                until: vec!["must-not-spawn".into()],
            },
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    db.store
        .cancel_workflow(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    let progress = db
        .store
        .apply_work(&work, &[resolved_workflow("must-not-spawn")])
        .await
        .unwrap();
    assert_eq!(progress.children_scheduled, 0);
    recover(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM owned_workflow_links")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn mixed_input_budget_failure_never_partially_resumes_the_parent() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "joined".into(),
            commands: vec![workflow_child("flow"), child("task")],
            until: vec!["flow".into(), "task".into()],
        },
        &[resolved_workflow("flow"), resolved("task")],
    )
    .await;
    let flow = acquire(&db.store, "subflows").await;
    apply_decision(
        &db.store,
        &flow,
        0,
        WorkflowAction::Complete {
            output: json!("x".repeat(140 * 1024)),
        },
        &[],
    )
    .await;
    let flow_work = one_work(&db.store).await;
    assert_eq!(
        db.store
            .apply_work(&flow_work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let task = acquire(&db.store, "children").await;
    report(&db.store, &task, json!("y".repeat(140 * 1024))).await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
            .bind(&parent.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    db.store
        .reject_work(
            &work,
            &ApplicationError {
                kind: "input_budget".into(),
                message: "joined inputs exceed context budget".into(),
            },
        )
        .await
        .unwrap();
    recover(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Failed
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn owned_cancellation_work_does_not_age_out_and_cannot_be_rejected() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "joined".into(),
            commands: vec![workflow_child("child")],
            until: vec!["child".into()],
        },
        &[resolved_workflow("child")],
    )
    .await;
    let child = child_id(&db.store, &parent.workflow_id, "child").await;
    db.store
        .cancel_workflow(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    sqlx::query(
        "UPDATE workflow_work SET created_at_ms=0 WHERE workflow_id=$1 AND kind='cancel_owned'",
    )
    .bind(&child)
    .execute(&db.store.pool)
    .await
    .unwrap();
    let work = one_work(&db.store).await;
    assert_eq!(work.source, WorkflowWorkSource::CancelOwned);
    assert!(matches!(
        db.store
            .reject_work(
                &work,
                &ApplicationError {
                    kind: "reject".into(),
                    message: "must remain durable".into()
                }
            )
            .await,
        Err(ContractError::Unavailable(_))
    ));
    db.store
        .retry_work(&work, "recover across an old obligation")
        .await
        .unwrap();
    assert_eq!(
        db.store
            .workflow_status(&scope(), &child)
            .await
            .unwrap()
            .state,
        WorkflowState::Running
    );
    recover(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    assert_eq!(
        db.store
            .workflow_status(&scope(), &child)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_shares_a_bounded_batch_across_tasks_and_subworkflows() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    let mut commands: Vec<_> = (0..17)
        .map(|i| workflow_child(&format!("flow-{i}")))
        .collect();
    commands.extend([child("task-a"), child("task-b")]);
    let keys = commands.iter().map(|c| c.key.clone()).collect();
    let mut bindings: Vec<_> = (0..17)
        .map(|i| resolved_workflow(&format!("flow-{i}")))
        .collect();
    bindings.extend([resolved("task-a"), resolved("task-b")]);
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "joined".into(),
            commands,
            until: keys,
        },
        &bindings,
    )
    .await;
    db.store
        .cancel_workflow(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    let enqueued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM owned_workflow_links WHERE parent_workflow_id=$1 AND cancel_enqueued",
    )
    .bind(&parent.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    // Two direct tasks and fourteen owned workflow obligations share batch16.
    assert_eq!(enqueued, 14);
    let tasks:i64=sqlx::query_scalar("SELECT count(*) FROM workflow_task_links l JOIN tasks t USING(task_id) WHERE l.workflow_id=$1 AND NOT l.is_activation AND t.state='cancelled'").bind(&parent.workflow_id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(tasks, 2);
    recover(&db.store).await;
    let states: (i64, i64) = sqlx::query_as(
        "SELECT count(*),count(*) FILTER(WHERE state='cancelled') FROM workflow_runs",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(states, (18, 18));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn drain_terminal_timestamp_follows_the_last_owned_task_transition() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "never".into(),
            commands: vec![child("first"), child("second")],
            until: vec!["first".into(), "second".into()],
        },
        &[resolved("first"), resolved("second")],
    )
    .await;
    // Force the second task cancellation into a later database millisecond than
    // the drain start. The delay is fixture-only and leaves production clocks
    // and timestamps untouched.
    sqlx::raw_sql("CREATE FUNCTION delay_cancelled_task() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.025); RETURN NEW; END $$; CREATE TRIGGER delay_cancelled_task AFTER UPDATE OF state ON tasks FOR EACH ROW WHEN (NEW.state='cancelled' AND OLD.state<>'cancelled') EXECUTE FUNCTION delay_cancelled_task();")
        .execute(&db.store.pool).await.unwrap();
    db.store
        .cancel_workflow(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    let work = one_work(&db.store).await;
    db.store.apply_work(&work, &[]).await.unwrap();
    let terminal = db
        .store
        .workflow_status(&scope(), &parent.workflow_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    let child_terminal:i64 = sqlx::query_scalar("SELECT max(t.terminal_at_ms) FROM workflow_task_links l JOIN tasks t USING(task_id) WHERE l.workflow_id=$1 AND NOT l.is_activation")
        .bind(&parent.workflow_id).fetch_one(&db.store.pool).await.unwrap();
    let history: i64 = sqlx::query_scalar(
        "SELECT at_ms FROM workflow_history WHERE workflow_id=$1 AND reason='terminalized'",
    )
    .bind(&parent.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    let completed: i64 =
        sqlx::query_scalar("SELECT processed_at_ms FROM workflow_work WHERE id=$1")
            .bind(&work.id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(
        terminal >= child_terminal as u64,
        "parent terminal timestamp preceded its final child task"
    );
    assert_eq!(history as u64, terminal);
    assert_eq!(completed as u64, terminal);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn complete_resamples_time_after_observing_a_terminal_owned_workflow() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "finish".into(),
            commands: vec![workflow_child("child")],
        },
        &[resolved_workflow("child")],
    )
    .await;
    let next = acquire(&db.store, "python").await;
    let child = acquire(&db.store, "subflows").await;
    let child_id = self::child_id(&db.store, &parent.workflow_id, "child").await;
    let earlier = {
        let mut connection = db.store.pool.acquire().await.unwrap();
        let at = crate::persistence::now(&mut connection).await.unwrap();
        sqlx::query("SELECT pg_sleep(0.01)")
            .execute(&mut *connection)
            .await
            .unwrap();
        at
    };
    apply_decision(
        &db.store,
        &child,
        0,
        WorkflowAction::Complete {
            output: json!("child"),
        },
        &[],
    )
    .await;
    let child_terminal = db
        .store
        .workflow_status(&scope(), &child_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    assert!(child_terminal > earlier);
    let decision = decision(
        &next,
        1,
        WorkflowAction::Complete {
            output: json!("parent"),
        },
    );
    report(&db.store, &next, serde_json::to_value(&decision).unwrap()).await;
    // Exercise the permitted interleaving deterministically: the coordinator
    // sampled time before the child committed, then checks ownership afterward.
    let mut tx = db.store.pool.begin().await.unwrap();
    let mut run =
        super::super::data::load_run(&mut tx, &scope(), Some(&parent.workflow_id), None, true)
            .await
            .unwrap();
    let applied = super::super::work::apply_decision(
        &mut tx,
        &mut run,
        &decision,
        &[],
        None,
        earlier,
        &mut WorkflowProgress::default(),
    )
    .await
    .unwrap();
    assert!(applied.wakes.is_empty());
    tx.commit().await.unwrap();
    let terminal = db
        .store
        .workflow_status(&scope(), &parent.workflow_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    let history:i64 = sqlx::query_scalar("SELECT at_ms FROM workflow_history WHERE workflow_id=$1 AND activation_id=$2 AND reason='decision_applied'")
        .bind(&parent.workflow_id).bind(&next.lease.owner.task_id).fetch_one(&db.store.pool).await.unwrap();
    assert!(terminal >= child_terminal);
    assert_eq!(history as u64, terminal);
    db.finish().await;
}

async fn check_resume_timestamp_after_child_terminal(wait_for_child: bool) {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "join".into(),
            commands: vec![workflow_child("child")],
        },
        &[resolved_workflow("child")],
    )
    .await;
    let next = acquire(&db.store, "python").await;
    let child = acquire(&db.store, "subflows").await;
    let child_id = self::child_id(&db.store, &parent.workflow_id, "child").await;
    let earlier = {
        let mut connection = db.store.pool.acquire().await.unwrap();
        let at = crate::persistence::now(&mut connection).await.unwrap();
        sqlx::query("SELECT pg_sleep(0.01)")
            .execute(&mut *connection)
            .await
            .unwrap();
        at
    };
    apply_decision(
        &db.store,
        &child,
        0,
        WorkflowAction::Complete {
            output: json!("child"),
        },
        &[],
    )
    .await;
    let child_terminal = db
        .store
        .workflow_status(&scope(), &child_id)
        .await
        .unwrap()
        .terminal_at
        .unwrap();
    assert!(child_terminal > earlier);
    let action = if wait_for_child {
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "joined".into(),
            commands: Vec::new(),
            until: vec!["child".into()],
        }
    } else {
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "joined".into(),
            commands: Vec::new(),
        }
    };
    let decision = decision(&next, 1, action);
    report(&db.store, &next, serde_json::to_value(&decision).unwrap()).await;
    // A child may commit after the coordinator samples time because it never
    // needs a parent mutation lock. Exercise that interleaving deterministically.
    let mut tx = db.store.pool.begin().await.unwrap();
    let mut run =
        super::super::data::load_run(&mut tx, &scope(), Some(&parent.workflow_id), None, true)
            .await
            .unwrap();
    let applied = super::super::work::apply_decision(
        &mut tx,
        &mut run,
        &decision,
        &[],
        None,
        earlier,
        &mut WorkflowProgress::default(),
    )
    .await
    .unwrap();
    assert_eq!(applied.wakes.len(), 1);
    let task = &applied.wakes[0];
    assert!(
        task.submitted_at >= child_terminal,
        "resumed activation timestamp preceded its observed child outcome"
    );
    assert_eq!(task.submitted_at, applied.at);
    let context: WorkflowActivationContext = codec::decode(
        &sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT context_bytes FROM workflow_activations WHERE activation_id=$1",
        )
        .bind(&task.task_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        workflow_input(&context.inputs["child"]).workflow_id,
        child_id
    );
    let history: i64 = sqlx::query_scalar(
        "SELECT at_ms FROM workflow_history WHERE workflow_id=$1 AND activation_id=$2 AND reason='decision_applied'",
    ).bind(&parent.workflow_id).bind(&next.lease.owner.task_id)
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(history as u64, task.submitted_at);
    tx.commit().await.unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn immediate_child_join_timestamp_follows_observed_terminal_result() {
    check_resume_timestamp_after_child_terminal(true).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn continued_child_inputs_timestamp_follows_observed_terminal_result() {
    check_resume_timestamp_after_child_terminal(false).await;
}
