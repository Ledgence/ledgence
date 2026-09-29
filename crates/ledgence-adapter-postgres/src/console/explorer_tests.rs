use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, completed, descriptor, scope};
use ledgence_worker_api::{InvocationObservations, LocalStepObservation, LocalStepObservedState};
use serde_json::json;

#[path = "explorer_entrypoint_tests.rs"]
mod entrypoint_tests;

async fn acquire(db: &TestDb, queue: &str) -> Assignment {
    let session = db.store.open_session(&scope(), queue, 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
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
    assigned
}
async fn start(db: &TestDb, key: &str) -> (WorkflowSnapshot, Assignment) {
    let mut submit = command();
    submit.idempotency_key = key.into();
    let workflow = db
        .store
        .accept_resolved_workflow(&submit, &descriptor())
        .await
        .unwrap();
    let assigned = acquire(db, "python").await;
    (workflow, assigned)
}
async fn apply(db: &TestDb, assigned: &Assignment, revision: u64, action: WorkflowAction) {
    let decision = WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision,
        action,
    };
    db.store
        .settle(&completed(
            assigned,
            Quiescence::Confirmed,
            serde_json::to_value(decision).unwrap(),
        ))
        .await
        .unwrap();
    for _ in 0..8 {
        let work = db.store.claim_work(16).await.unwrap();
        if work.is_empty() {
            return;
        }
        for item in work {
            db.store.apply_work(&item, &[]).await.unwrap();
        }
    }
    panic!("workflow obligations failed to quiesce");
}
async fn all(db: &TestDb, workflow: &str, limit: u32) -> Vec<ConsoleExplorerNode> {
    let mut page = ConsolePagination {
        limit,
        cursor: None,
    };
    let mut nodes = Vec::new();
    for _ in 0..512 {
        let request = ConsoleQuery::Explorer {
            workflow_id: workflow.into(),
            page: page.clone(),
        };
        let ConsoleQueryReply::Explorer(result) =
            db.store.query_console(&scope(), &request).await.unwrap()
        else {
            panic!("explorer response")
        };
        assert_eq!(result.workflow.observed_at, result.page.observed_at);
        assert_eq!(result.evidence, "retained_records_only");
        nodes.extend(result.page.items);
        page.cursor = result.page.next_cursor;
        if page.cursor.is_none() {
            return nodes;
        }
    }
    panic!("unbounded explorer pagination");
}
async fn rebuild_historical_projection(db: &TestDb) {
    // Recreate exactly the upgrade projection from the authoritative historical
    // rows, without calling its current lifecycle writers during the backfill.
    let mut tx = db.store.pool.begin().await.unwrap();
    sqlx::query("DROP TABLE workflow_explorer_records")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/20260928020000_workflow_explorer.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../migrations/20260929000000_console_entrypoints.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
fn local(assigned: &Assignment) -> LocalResultCommand {
    LocalResultCommand {
        owner: assigned.lease.owner.clone(),
        record: LocalStepRecord {
            key: "tests:0".into(),
            callable: "program:tests".into(),
            input: json!({"private":"input"}),
            output: json!({"private":"output"}),
        },
    }
}
fn observations(
    state: LocalStepObservedState,
    started: u64,
    elapsed: u64,
) -> InvocationObservations {
    InvocationObservations {
        runtime_started_at_ms: started,
        runtime_elapsed_us: elapsed,
        process_cpu_user_us: None,
        process_cpu_system_us: None,
        process_lifetime_peak_rss_bytes: None,
        local_steps: vec![LocalStepObservation {
            key: "tests:0".into(),
            callable: "program:tests".into(),
            started_at_ms: started,
            elapsed_us: elapsed,
            state,
        }],
        local_steps_truncated: false,
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_fork_local_and_join_follow_recorded_semantics_with_singleton_cursors() {
    let db = TestDb::new().await;
    let (root, assigned) = start(&db, "fork-local").await;
    assert_eq!(all(&db, &root.workflow_id, 1).await.len(), 1);
    let fork = WorkflowForkCommand {
        owner: assigned.lease.owner.clone(),
        processing_trace: None,
        fork: WorkflowForkRequest {
            key: "validate:0".into(),
            branches: vec![WorkflowBranch {
                key: "review:0".into(),
                entrypoint: "review".into(),
                queue: "branches".into(),
                data: json!({"candidate":"private"}),
                retry_policy: command().input.retry_policy,
                attempt_timeout_ms: 300_000,
            }],
        },
    };
    db.store.fork_workflow(&fork).await.unwrap();
    db.store
        .record_local_result(&local(&assigned))
        .await
        .unwrap();
    assert!(
        db.store
            .fork_workflow(&fork)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        db.store
            .record_local_result(&local(&assigned))
            .await
            .unwrap()
            .already_accepted
    );
    let before = all(&db, &root.workflow_id, 1).await;
    assert_eq!(
        before.len(),
        4,
        "one entrypoint, child, fork and local occurrence despite replay"
    );
    assert_eq!(
        db.store
            .workflow_status(&scope(), &root.workflow_id)
            .await
            .unwrap()
            .revision,
        0
    );
    let fork_node = before
        .iter()
        .find(|node| matches!(node.data, ConsoleExplorerData::Fork { .. }))
        .unwrap();
    let ConsoleExplorerData::Fork { branch_keys, .. } = &fork_node.data else {
        unreachable!()
    };
    assert_eq!(branch_keys, &["review:0"]);
    let local_node = before
        .iter()
        .find(|node| matches!(node.data, ConsoleExplorerData::Local { .. }))
        .unwrap();
    assert_eq!(local_node.activation_id, assigned.lease.owner.task_id);
    assert!(!serde_json::to_string(&before).unwrap().contains("private"));
    apply(
        &db,
        &assigned,
        0,
        WorkflowAction::Suspend {
            state: json!({"private":"checkpoint"}),
            continuation: "collect".into(),
            commands: vec![],
            until: vec!["review:0".into()],
        },
    )
    .await;
    let branch = acquire(&db, "branches").await;
    apply(
        &db,
        &branch,
        0,
        WorkflowAction::Complete {
            output: json!({"approved":true}),
        },
    )
    .await;
    let after = all(&db, &root.workflow_id, 1).await;
    assert_eq!(
        after.len(),
        6,
        "join and resumed entrypoint augment existing identities"
    );
    assert!(
        before
            .iter()
            .all(|old| after.iter().any(|new| new.id == old.id))
    );
    let next = db
        .store
        .workflow_status(&scope(), &root.workflow_id)
        .await
        .unwrap()
        .activation_id
        .unwrap();
    assert!(after.iter().any(|node| matches!(&node.data, ConsoleExplorerData::ChildWait { member_keys, resumed_activation_id: Some(id), .. } if member_keys == &["review:0"] && id == &next)));
    assert!(after.iter().any(|node| matches!(&node.data, ConsoleExplorerData::Entrypoint { decision_kind: Some(ConsoleDecisionKind::Suspend), resumed_activation_id: Some(id), .. } if id == &next)));
    assert!(after.iter().any(|node| node.activation_id == next
        && matches!(
            node.data,
            ConsoleExplorerData::Entrypoint {
                state: Some(TaskState::Queued),
                ..
            }
        )));
    rebuild_historical_projection(&db).await;
    let upgraded = all(&db, &root.workflow_id, 1).await;
    assert_eq!(
        serde_json::to_value(&upgraded).unwrap(),
        serde_json::to_value(&after).unwrap(),
        "upgrade retains exactly the same recorded fork, local, join and continuation evidence"
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_local_replay_preserves_execution_interval_and_accepted_journal() {
    let db = TestDb::new().await;
    let (root, assigned) = start(&db, "local-observation").await;
    let original = local(&assigned);
    db.store.record_local_result(&original).await.unwrap();
    let before: Vec<u8> = sqlx::query_scalar("SELECT record_bytes FROM workflow_local_results WHERE activation_id=$1 AND step_key='tests:0'").bind(&assigned.lease.owner.task_id).fetch_one(&db.store.pool).await.unwrap();
    for (attempt, state, started, elapsed) in [
        (
            "observed-first",
            LocalStepObservedState::Returned,
            10,
            20_000,
        ),
        ("observed-replay", LocalStepObservedState::Replayed, 30, 1),
    ] {
        let mut connection = db.store.pool.acquire().await.unwrap();
        crate::workflow::explorer::observe_locals(
            &mut connection,
            &assigned.lease.owner.task_id,
            attempt,
            &observations(state, started, elapsed),
        )
        .await
        .unwrap();
    }
    let nodes = all(&db, &root.workflow_id, 100).await;
    let record = nodes
        .iter()
        .find(|node| matches!(node.data, ConsoleExplorerData::Local { .. }))
        .unwrap();
    let ConsoleExplorerData::Local {
        accepted_at,
        accepting_attempt_id,
        observation: Some(observation),
        ..
    } = &record.data
    else {
        panic!("expected local evidence")
    };
    assert!(accepted_at.is_some());
    assert_eq!(
        accepting_attempt_id.as_deref(),
        Some(assigned.lease.owner.attempt_id.as_str())
    );
    assert_eq!(
        (
            observation.state.as_str(),
            observation.attempt_id.as_str(),
            observation.started_at,
            observation.elapsed_us.0
        ),
        ("returned", "observed-first", 10, 20_000)
    );
    let after: Vec<u8> = sqlx::query_scalar("SELECT record_bytes FROM workflow_local_results WHERE activation_id=$1 AND step_key='tests:0'").bind(&assigned.lease.owner.task_id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(before, after);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_unaccepted_retry_callable_changes_cannot_relabel_accepted_evidence() {
    let db = TestDb::new().await;
    let (root, assigned) = start(&db, "local-callable-change").await;
    for name in ["program:first", "program:second"] {
        let mut values = observations(LocalStepObservedState::Failed, 10, 100);
        values.local_steps[0].callable = name.into();
        let mut connection = db.store.pool.acquire().await.unwrap();
        crate::workflow::explorer::observe_locals(
            &mut connection,
            &assigned.lease.owner.task_id,
            "observed",
            &values,
        )
        .await
        .unwrap();
    }
    let before = all(&db, &root.workflow_id, 100).await;
    assert!(before.iter().any(|node| matches!(&node.data, ConsoleExplorerData::Local { callable, accepted_at: None, observation: Some(_), .. } if callable == "program:second")));
    db.store
        .record_local_result(&local(&assigned))
        .await
        .unwrap();
    let after = all(&db, &root.workflow_id, 100).await;
    assert!(after.iter().any(|node| matches!(&node.data, ConsoleExplorerData::Local { callable, accepted_at: Some(_), observation: None, .. } if callable == "program:tests")), "acceptance of another callable cannot inherit a failed callable's interval");
    let mut wrong = observations(LocalStepObservedState::Returned, 20, 200);
    wrong.local_steps[0].callable = "program:wrong".into();
    let mut connection = db.store.pool.acquire().await.unwrap();
    crate::workflow::explorer::observe_locals(
        &mut connection,
        &assigned.lease.owner.task_id,
        "wrong-observation",
        &wrong,
    )
    .await
    .unwrap();
    drop(connection);
    let final_nodes = all(&db, &root.workflow_id, 100).await;
    assert!(final_nodes.iter().any(|node| matches!(&node.data, ConsoleExplorerData::Local { callable, observation: None, .. } if callable == "program:tests")));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_external_wait_distinguishes_event_resume_from_cancellation() {
    let db = TestDb::new().await;
    for (name, event) in [("resumed", true), ("cancelled", false)] {
        let (root, assigned) = start(&db, name).await;
        if event {
            db.store.send_workflow_event(&WorkflowEventCommand {
                scope: scope(), workflow_id: root.workflow_id.clone(), key: "approval:0".into(),
                event: WorkflowEvent::new(json!({"specversion":"1.0","id":"evt:1","source":"urn:tests","type":"approved","datacontenttype":"application/json","data":{"private":true}})).unwrap(),
            }).await.unwrap();
        }
        apply(
            &db,
            &assigned,
            0,
            WorkflowAction::Wait {
                state: json!(null),
                continuation: "after".into(),
                commands: vec![],
                wait: WorkflowWait::Event {
                    key: "approval:0".into(),
                    timeout_ms: None,
                },
            },
        )
        .await;
        if !event {
            db.store
                .cancel_workflow(&scope(), &root.workflow_id)
                .await
                .unwrap();
        }
        let nodes = all(&db, &root.workflow_id, 1).await;
        let wait = nodes
            .iter()
            .find(|node| matches!(node.data, ConsoleExplorerData::ExternalWait { .. }))
            .unwrap();
        let ConsoleExplorerData::ExternalWait {
            closed_at,
            wake_reason,
            resumed_activation_id,
            ..
        } = &wait.data
        else {
            unreachable!()
        };
        assert!(closed_at.is_some());
        assert_eq!(*wake_reason, event.then_some(ConsoleWakeReason::Event));
        assert_eq!(resumed_activation_id.is_some(), event);
        rebuild_historical_projection(&db).await;
        assert_eq!(
            serde_json::to_value(all(&db, &root.workflow_id, 1).await).unwrap(),
            serde_json::to_value(&nodes).unwrap()
        );
        if event {
            let next = acquire(&db, "python").await;
            apply(
                &db,
                &next,
                1,
                WorkflowAction::Complete {
                    output: json!(null),
                },
            )
            .await;
        }
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_rejected_decision_has_no_effect_or_continuation_before_or_after_upgrade() {
    let db = TestDb::new().await;
    let (root, assigned) = start(&db, "rejected-decision").await;
    let decision = WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision: 0,
        action: WorkflowAction::Continue {
            state: json!(null),
            continuation: "never-ran".into(),
            commands: vec![],
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
    let accepted = all(&db, &root.workflow_id, 1).await;
    assert!(matches!(
        &accepted[0].data,
        ConsoleExplorerData::Entrypoint {
            state: Some(TaskState::Succeeded),
            applied_at: None,
            decision_kind: None,
            resumed_activation_id: None,
            ..
        }
    ));
    let work = db.store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 1);
    db.store
        .reject_work(
            &work[0],
            &ApplicationError {
                kind: "rejected".into(),
                message: "Explicit permanent rejection".into(),
            },
        )
        .await
        .unwrap();
    let rejected = all(&db, &root.workflow_id, 1).await;
    assert_eq!(rejected.len(), 1);
    assert!(matches!(
        &rejected[0].data,
        ConsoleExplorerData::Entrypoint {
            applied_at: Some(_),
            decision_kind: None,
            error: Some(_),
            resumed_activation_id: None,
            ..
        }
    ));
    rebuild_historical_projection(&db).await;
    assert_eq!(
        serde_json::to_value(all(&db, &root.workflow_id, 1).await).unwrap(),
        serde_json::to_value(rejected).unwrap()
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_unavailable_children_keep_identity_without_reading_application_payloads() {
    let db = TestDb::new().await;
    let (root, assigned) = start(&db, "retained-parent").await;
    let fork = WorkflowForkCommand {
        owner: assigned.lease.owner.clone(),
        processing_trace: None,
        fork: WorkflowForkRequest {
            key: "fork:0".into(),
            branches: vec![WorkflowBranch {
                key: "review:0".into(),
                entrypoint: "review".into(),
                queue: "branches".into(),
                data: json!(null),
                retry_policy: command().input.retry_policy,
                attempt_timeout_ms: 300_000,
            }],
        },
    };
    db.store.fork_workflow(&fork).await.unwrap();
    let nodes = all(&db, &root.workflow_id, 100).await;
    let child = nodes
        .iter()
        .find(|node| matches!(node.data, ConsoleExplorerData::Child { .. }))
        .unwrap();
    let ConsoleExplorerData::Child { execution, .. } = &child.data else {
        unreachable!()
    };
    let identity = execution.clone();
    let request = ConsoleQuery::Ancestry {
        execution: identity.clone(),
    };
    let ConsoleQueryReply::Ancestry(ancestry) =
        db.store.query_console(&scope(), &request).await.unwrap()
    else {
        panic!("ancestry")
    };
    assert_eq!(
        ancestry
            .path
            .iter()
            .map(|entry| entry.execution.id.as_str())
            .collect::<Vec<_>>(),
        [&root.workflow_id, &identity.id]
    );
    sqlx::query("UPDATE workflow_runs SET state='cancelled',terminal_at_ms=20,retiring_at_ms=20 WHERE workflow_id=$1").bind(&identity.id).execute(&db.store.pool).await.unwrap();
    sqlx::query(
        "UPDATE workflow_runs SET submission_bytes=decode('00','hex') WHERE workflow_id=$1",
    )
    .bind(&root.workflow_id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE tasks SET input_bytes=decode('00','hex') WHERE workflow_id=$1")
        .bind(&root.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let refreshed = all(&db, &root.workflow_id, 1).await;
    let missing = refreshed.iter().find(|node| node.id == child.id).unwrap();
    assert!(
        matches!(&missing.data, ConsoleExplorerData::Child { availability: ConsoleEvidenceAvailability::Unavailable, state: None, execution, .. } if execution == &identity)
    );
    let other = Scope {
        tenant_id: "other".into(),
        namespace: scope().namespace,
    };
    assert!(matches!(
        db.store
            .query_console(
                &other,
                &ConsoleQuery::Explorer {
                    workflow_id: root.workflow_id,
                    page: Default::default()
                }
            )
            .await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn explorer_byte_limited_page_continues_without_skipping_retained_records() {
    let db = TestDb::new().await;
    let workflow = "byte-budget";
    let mut submission = command();
    submission.idempotency_key = workflow.into();
    // Exercise the read projection's contract bounds independently of workflow
    // transitions. These typed retained records have no live activation targets;
    // this fixture does not claim that one workflow can reject 100 decisions.
    sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,revision,continuation,checkpoint_bytes,outcome_bytes,submitted_at_ms,terminal_at_ms,queue) VALUES($1,'acme','billing',$1,$2,$3,'failed',99,'review',$4,$5,1,3,'python')")
        .bind(workflow)
        .bind(codec::encode(&submission).unwrap())
        .bind(codec::encode(&descriptor()).unwrap())
        .bind(codec::encode(&json!(null)).unwrap())
        .bind(codec::encode(&WorkflowOutcome::Failed {
            error: ApplicationError {
                kind: "fixture".into(),
                message: "Retained projection boundary fixture".into(),
            },
        }).unwrap())
        .execute(&db.store.pool).await.unwrap();
    let message = "\0".repeat(4096);
    let data = ConsoleExplorerData::Entrypoint {
        state: None,
        availability: ConsoleEvidenceAvailability::Unavailable,
        submitted_at: 1,
        terminal_at: Some(2),
        applied_at: Some(3),
        decision_kind: None,
        error: Some(ApplicationError {
            kind: "retained".into(),
            message: message.clone(),
        }),
        resumed_activation_id: None,
    };
    data.validate().unwrap();
    let bytes = codec::encode(&data).unwrap();
    assert!(bytes.len() * 100 > CONSOLE_METADATA_MAX_BYTES);
    sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) SELECT $1,n,'entrypoint','','retained_'||lpad(n::text,3,'0'),'review',$2 FROM generate_series(0,99) n")
        .bind(workflow).bind(bytes).execute(&db.store.pool).await.unwrap();
    let expected: Vec<_> = (0..100)
        .map(|revision| {
            serde_json::to_string(&(
                "entrypoint",
                workflow,
                format!("retained_{revision:03}"),
                "",
            ))
            .unwrap()
        })
        .collect();
    let mut request = ConsolePagination {
        limit: 100,
        cursor: None,
    };
    let mut found = Vec::new();
    let mut finished = false;
    for page_number in 0..4 {
        let ConsoleQueryReply::Explorer(result) = db
            .store
            .query_console(
                &scope(),
                &ConsoleQuery::Explorer {
                    workflow_id: workflow.into(),
                    page: request.clone(),
                },
            )
            .await
            .unwrap()
        else {
            panic!("explorer response")
        };
        assert!(codec::encode(&result).unwrap().len() <= CONSOLE_METADATA_MAX_BYTES);
        assert!(!result.page.items.is_empty());
        if page_number == 0 {
            assert!(result.page.items.len() < request.limit as usize);
            assert!(
                result.page.next_cursor.is_some(),
                "a page shortened by bytes must still expose its continuation"
            );
        }
        for node in result.page.items {
            assert!(
                matches!(node.data, ConsoleExplorerData::Entrypoint { error: Some(error), .. } if error.message == message)
            );
            found.push(node.id);
        }
        request.cursor = result.page.next_cursor;
        if request.cursor.is_none() {
            finished = true;
            break;
        }
    }
    assert!(finished, "bounded traversal must reach the final page");
    assert_eq!(
        found, expected,
        "following the real SQL cursor must preserve every record exactly once in order"
    );
    db.finish().await;
}
