//! Upgrade an actually populated released schema, preserving durable identities.
use super::*;
use crate::MigrationOptions;
use sqlx::{AssertSqlSafe, migrate::Migrator};

fn released_schema() -> Migrator {
    // Pin the released bytes, not merely an arbitrary prefix of today's schema.
    // No Git checkout/network is needed when this acceptance test runs in CI.
    let released: Vec<_> = include_str!("../../tests/fixtures/schema-v0.2.0.sha384")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let (version, checksum) = line.split_once(' ').unwrap();
            (version.parse::<i64>().unwrap(), checksum)
        })
        .collect();
    assert_eq!(released.len(), 18);
    let migrations: Vec<_> = crate::MIGRATOR
        .iter()
        .filter(|migration| migration.version <= released.last().unwrap().0)
        .cloned()
        .collect();
    assert_eq!(migrations.len(), released.len());
    for (migration, (version, checksum)) in migrations.iter().zip(released) {
        assert_eq!(migration.version, version);
        let actual: String = migration
            .checksum
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(actual, checksum, "released migration {version} was changed");
    }
    Migrator::with_migrations(migrations)
}

#[test]
fn historical_migration_checksums_match_released_020() {
    let _ = released_schema();
}

async fn persisted_rows(store: &PostgresStore) -> BTreeMap<&'static str, Vec<String>> {
    let mut snapshot = BTreeMap::new();
    for table in [
        "tasks",
        "attempts",
        "accepted_settlements",
        "task_history",
        "worker_sessions",
        "consumer_cursors",
        "workflow_runs",
        "workflow_activations",
        "workflow_task_links",
        "workflow_local_results",
        "workflow_history",
        "workflow_waits",
        "workflow_events",
        "workflow_work",
        "workflow_explorer_records",
    ] {
        // Identifiers are a fixed list of fixture tables; encoded bytea payloads
        // and complete rows must remain identical across the schema migration.
        let rows: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT row_to_json(r)::text FROM {table} r ORDER BY row_to_json(r)::text"
        )))
        .fetch_all(&store.pool)
        .await
        .unwrap();
        assert!(!rows.is_empty(), "upgrade fixture did not populate {table}");
        snapshot.insert(table, rows);
    }
    snapshot
}

fn external_event(workflow: &WorkflowSnapshot, key: &str) -> WorkflowEventCommand {
    WorkflowEventCommand {
        scope: scope(),
        workflow_id: workflow.workflow_id.clone(),
        key: key.into(),
        event: WorkflowEvent::new(json!({
            "specversion": "1.0", "id": format!("evt_{key}"), "source": "urn:upgrade-test",
            "type": "invoice.ready", "datacontenttype": "application/json",
            "data": {"invoice": "INV-1042", "amount": u64::MAX, "zero": -0.0}
        }))
        .unwrap(),
    }
}

async fn recover_ready(store: &PostgresStore) {
    for _ in 0..8 {
        let work = store.claim_work(16).await.unwrap();
        if work.is_empty() {
            return;
        }
        for item in work {
            store.apply_work(&item, &[]).await.unwrap();
        }
    }
    panic!("bounded upgrade fixture did not settle coordinator work");
}

async fn apply_recovered_decision(
    store: &PostgresStore,
    assigned: &Assignment,
    revision: u64,
    action: WorkflowAction,
    resolved: &[ResolvedWorkflowChild],
) {
    assert!(resolved.is_empty());
    report(
        store,
        assigned,
        serde_json::to_value(decision(assigned, revision, action)).unwrap(),
    )
    .await;
    // The historical timer may become due alongside this activation's report.
    recover_ready(store).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn populated_020_upgrade_preserves_state_resumes_waits_and_enables_approvals() {
    let db = TestDb::without_migrations().await;
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &released_schema())
        .await
        .unwrap();
    assert!(
        db.store.verify_schema().await.is_err(),
        "new server must not serve the old schema"
    );
    let approvals: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('workflow_approvals')::text")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(approvals.is_none());

    // Ordinary completed and queued tasks coexist with dormant workflows.
    let terminal = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let completed_attempt = acquire(&db.store, "python").await;
    let output = json!({"amount": u64::MAX, "text": "left\u{0000}right", "zero": -0.0});
    report(&db.store, &completed_attempt, output.clone()).await;
    let completed_before = db.store.result(&scope(), &terminal.task_id).await.unwrap();
    let mut queued_command = command();
    queued_command.idempotency_key = "upgrade-queued".into();
    queued_command.input.queue = "queued".into();
    let queued = db
        .store
        .accept_resolved_submission(&queued_command, &descriptor())
        .await
        .unwrap();

    let event_workflow = start(&db.store).await;
    let initial = acquire(&db.store, "python").await;
    dispatched(&db.store, &initial).await;
    let journal = local(&initial, "prepared");
    db.store.record_local_result(&journal).await.unwrap();
    let checkpoint = json!({"invoice": "INV-1042", "amount": u64::MAX, "zero": -0.0});
    apply_decision(
        &db.store,
        &initial,
        0,
        WorkflowAction::Wait {
            state: checkpoint.clone(),
            continuation: "after_event".into(),
            commands: vec![],
            wait: WorkflowWait::Event {
                key: "external".into(),
                timeout_ms: None,
            },
        },
        &[],
    )
    .await;
    let early = external_event(&event_workflow, "early");
    let early_receipt = db.store.send_workflow_event(&early).await.unwrap();

    let mut timer_command = command();
    timer_command.idempotency_key = "upgrade-timer".into();
    timer_command.input.queue = "timer".into();
    let timer = db
        .store
        .accept_resolved_workflow(&timer_command, &descriptor())
        .await
        .unwrap();
    let timer_initial = acquire(&db.store, "timer").await;
    apply_decision(
        &db.store,
        &timer_initial,
        0,
        WorkflowAction::Wait {
            state: json!({"timer": "persisted"}),
            continuation: "after_timer".into(),
            commands: vec![],
            wait: WorkflowWait::Timer {
                key: "delay".into(),
                delay_ms: 100,
            },
        },
        &[],
    )
    .await;
    let waiting_before = db
        .store
        .workflow_status(&scope(), &event_workflow.workflow_id)
        .await
        .unwrap();
    let timer_before = db
        .store
        .workflow_status(&scope(), &timer.workflow_id)
        .await
        .unwrap();
    assert_eq!(waiting_before.state, WorkflowState::Waiting);
    assert_eq!(timer_before.state, WorkflowState::Waiting);
    let before = persisted_rows(&db.store).await;

    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    assert_eq!(before, persisted_rows(&db.store).await);
    // Running the deployment command again is harmless on the populated schema.
    db.store.migrate().await.unwrap();
    assert_eq!(before, persisted_rows(&db.store).await);
    let recovered = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    recovered.verify_schema().await.unwrap();
    assert_json_eq!(
        recovered.result(&scope(), &terminal.task_id).await.unwrap(),
        completed_before
    );
    assert_json_eq!(
        recovered
            .workflow_status(&scope(), &event_workflow.workflow_id)
            .await
            .unwrap(),
        waiting_before
    );
    assert_json_eq!(
        recovered
            .workflow_status(&scope(), &timer.workflow_id)
            .await
            .unwrap(),
        timer_before
    );
    let replay = recovered
        .accept_resolved_submission(&queued_command, &descriptor())
        .await
        .unwrap();
    assert_eq!(replay.task_id, queued.task_id);
    assert_eq!(replay.state, TaskState::Queued);
    assert!(
        recovered
            .record_local_result(&journal)
            .await
            .unwrap()
            .already_accepted
    );
    let early_replay = recovered.send_workflow_event(&early).await.unwrap();
    assert!(early_replay.already_accepted);
    assert_eq!(early_replay.accepted_at, early_receipt.accepted_at);

    let event = external_event(&event_workflow, "external");
    let receipt = recovered.send_workflow_event(&event).await.unwrap();
    recover_ready(&recovered).await;
    let resumed = acquire(&recovered, "python").await;
    let context = recovered
        .activation_context(&resumed.lease.owner)
        .await
        .unwrap();
    assert_eq!(context.revision, 1);
    assert_eq!(context.continuation, "after_event");
    assert_eq!(
        canonical_json_bytes(&context.state).unwrap(),
        canonical_json_bytes(&checkpoint).unwrap()
    );
    assert_eq!(
        context.wake,
        Some(WorkflowWake::Event {
            key: event.key,
            event: event.event,
            accepted_at: receipt.accepted_at,
        })
    );

    // An old workflow can now use the new approval ledger with the same lineage.
    let action = ApprovalAction {
        name: "invoice:issue".into(),
        version: "v1".into(),
        arguments: output.clone(),
    };
    apply_recovered_decision(
        &recovered,
        &resumed,
        1,
        WorkflowAction::Wait {
            state: checkpoint,
            continuation: "after_approval".into(),
            commands: vec![],
            wait: WorkflowWait::Approval {
                key: "review".into(),
                action: action.clone(),
                proposed_arguments: None,
                timeout_ms: 60_000,
            },
        },
        &[],
    )
    .await;
    let approval = recovered
        .approval(&scope(), &event_workflow.workflow_id, "review")
        .await
        .unwrap();
    assert_eq!(approval.status, ApprovalStatus::Pending);
    assert_eq!(approval.revision, 1);
    let decision = ApprovalDecisionCommand {
        scope: scope(),
        workflow_id: event_workflow.workflow_id.clone(),
        key: "review".into(),
        activation_id: resumed.lease.owner.task_id.clone(),
        revision: 1,
        action,
        decision_id: "upgrade-review".into(),
        decision: ApprovalDecision::Approve,
        reviewer: "release-test".into(),
        reason: None,
    };
    assert!(
        !recovered
            .decide_approval(&decision)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        recovered
            .decide_approval(&decision)
            .await
            .unwrap()
            .already_accepted
    );
    recover_ready(&recovered).await;
    let approved = acquire(&recovered, "python").await;
    let context = recovered
        .activation_context(&approved.lease.owner)
        .await
        .unwrap();
    let Some(WorkflowWake::Approval { approval }) = &context.wake else {
        panic!("approval resume missing")
    };
    assert_eq!(approval.status, ApprovalStatus::Approved);
    assert_eq!(
        approval.resumed_activation_id.as_deref(),
        Some(approved.lease.owner.task_id.as_str())
    );
    assert!(approval.action.matches(&decision.action).unwrap());
    apply_recovered_decision(
        &recovered,
        &approved,
        2,
        WorkflowAction::Complete {
            output: output.clone(),
        },
        &[],
    )
    .await;
    assert_eq!(
        recovered
            .workflow_result(&scope(), &event_workflow.workflow_id)
            .await
            .unwrap()
            .outcome,
        Some(WorkflowOutcome::Succeeded {
            output: output.clone()
        })
    );

    // Recover the pre-upgrade delayed obligation using its original deadline.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            recover_ready(&recovered).await;
            if recovered
                .workflow_status(&scope(), &timer.workflow_id)
                .await
                .unwrap()
                .state
                == WorkflowState::Running
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let timer_resumed = acquire(&recovered, "timer").await;
    let timer_context = recovered
        .activation_context(&timer_resumed.lease.owner)
        .await
        .unwrap();
    assert_eq!(timer_context.state, json!({"timer": "persisted"}));
    assert!(matches!(timer_context.wake, Some(WorkflowWake::Timer { key, .. }) if key == "delay"));
    apply_recovered_decision(
        &recovered,
        &timer_resumed,
        1,
        WorkflowAction::Complete {
            output: json!("timer completed"),
        },
        &[],
    )
    .await;
    assert_eq!(
        recovered
            .workflow_status(&scope(), &timer.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Succeeded
    );
    let queued_attempt = acquire(&recovered, "queued").await;
    assert_eq!(queued_attempt.lease.owner.task_id, queued.task_id);
    report(&recovered, &queued_attempt, output).await;
    recovered.close().await;
    db.finish().await;
}
