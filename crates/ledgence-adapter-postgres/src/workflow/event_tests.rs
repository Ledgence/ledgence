use super::*;

fn event(workflow: &WorkflowSnapshot, key: &str) -> WorkflowEventCommand {
    WorkflowEventCommand {
        scope: scope(), workflow_id: workflow.workflow_id.clone(), key:key.into(),
        event: WorkflowEvent::new(json!({"specversion":"1.0","id":format!("evt_{key}"),"source":"urn:billing:callbacks","type":"invoice.approved","datacontenttype":"application/json","data":{"invoice":"INV-1042","amount":9007199254740993_u64,"other":"left\u{0000}right"}})).unwrap(),
    }
}
fn wait_action(wait: WorkflowWait) -> WorkflowAction {
    WorkflowAction::Wait {
        state: json!({"phase":"waiting"}),
        continuation: "after_wait".into(),
        commands: Vec::new(),
        wait,
    }
}
async fn install(
    store: &PostgresStore,
    assigned: &Assignment,
    revision: u64,
    wait: WorkflowWait,
) -> WorkflowProgress {
    apply_decision(store, assigned, revision, wait_action(wait), &[]).await
}
async fn counts(store: &PostgresStore, id: &str) -> (i32, i32) {
    sqlx::query_as(
        "SELECT pending_event_count,pending_event_bytes FROM workflow_runs WHERE workflow_id=$1",
    )
    .bind(id)
    .fetch_one(&store.pool)
    .await
    .unwrap()
}
async fn force_due(store: &PostgresStore, workflow: &str) {
    sqlx::query("UPDATE workflow_waits SET registered_at_ms=0,deadline_ms=1 WHERE workflow_id=$1 AND closed_at_ms IS NULL")
        .bind(workflow).execute(&store.pool).await.unwrap();
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE workflow_id=$1 AND kind='wait' AND processed_at_ms IS NULL")
        .bind(workflow).execute(&store.pool).await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn early_event_freezes_once_and_receipt_survives_completion() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    let command = event(&workflow, "approval");
    let receipt = db.store.send_workflow_event(&command).await.unwrap();
    assert!(!receipt.already_accepted);
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    assert!(
        db.store
            .activation_context(&assigned.lease.owner)
            .await
            .unwrap()
            .wake
            .is_none()
    );
    let progress = install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: command.key.clone(),
            timeout_ms: None,
        },
    )
    .await;
    assert_eq!(progress.activations_scheduled, 1);
    let next = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&next.lease.owner)
        .await
        .unwrap();
    assert_eq!(
        context.wake,
        Some(WorkflowWake::Event {
            key: command.key.clone(),
            event: command.event.clone(),
            accepted_at: receipt.accepted_at
        })
    );
    assert_eq!(context.revision, 1);
    assert!(context.inputs.is_empty());
    assert_eq!(counts(&db.store, &workflow.workflow_id).await, (0, 0));
    apply_decision(
        &db.store,
        &next,
        1,
        WorkflowAction::Complete {
            output: json!("done"),
        },
        &[],
    )
    .await;
    let replay = db.store.send_workflow_event(&command).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.accepted_at, receipt.accepted_at);
    assert!(matches!(
        db.store.send_workflow_event(&event(&workflow, "new")).await,
        Err(ContractError::ObsoleteOperation)
    ));
    let mut altered = command.clone();
    let mut data = altered.event.value().clone();
    data["data"]["amount"] = json!(42);
    altered.event = WorkflowEvent::new(data).unwrap();
    assert!(matches!(
        db.store.send_workflow_event(&altered).await,
        Err(ContractError::Conflict)
    ));
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn external_wait_ignores_child_completion_and_preserves_pending_child_input() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &assigned,
        0,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "event".into(),
            commands: vec![child("compute")],
            wait: WorkflowWait::Event {
                key: "approval".into(),
                timeout_ms: None,
            },
        },
        &[resolved("compute")],
    )
    .await;
    let child_task = acquire(&db.store, "children").await;
    report(&db.store, &child_task, json!({"answer":42})).await;
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    db.store
        .send_workflow_event(&event(&workflow, "approval"))
        .await
        .unwrap();
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    let next = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&next.lease.owner)
        .await
        .unwrap();
    assert!(context.inputs.is_empty());
    assert!(matches!(context.wake, Some(WorkflowWake::Event { .. })));
    apply_decision(
        &db.store,
        &next,
        1,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "child".into(),
            commands: Vec::new(),
        },
        &[],
    )
    .await;
    let after = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&after.lease.owner)
        .await
        .unwrap();
    assert!(context.wake.is_none());
    assert_eq!(
        task_input_id(&context.inputs["compute"]),
        child_task.lease.owner.task_id
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn timed_event_uses_acceptance_deadline_despite_delayed_coordinator() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: "approval".into(),
            timeout_ms: Some(60_000),
        },
    )
    .await;
    let receipt = db
        .store
        .send_workflow_event(&event(&workflow, "approval"))
        .await
        .unwrap();
    // Move the stored deadline into the past but still after this accepted
    // event. No wall-clock sleep or race decides the expected winner.
    sqlx::query("UPDATE workflow_waits SET registered_at_ms=0,deadline_ms=$2 WHERE workflow_id=$1")
        .bind(&workflow.workflow_id)
        .bind(codec::ms(receipt.accepted_at + 1).unwrap())
        .execute(&db.store.pool)
        .await
        .unwrap();
    let work = one_work(&db.store).await;
    assert_eq!(work.source, WorkflowWorkSource::Wait);
    assert!(work.outcome.is_none());
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
    let next = acquire(&db.store, "python").await;
    assert!(
        matches!(db.store.activation_context(&next.lease.owner).await.unwrap().wake,Some(WorkflowWake::Event {accepted_at,..}) if accepted_at==receipt.accepted_at)
    );
    let work_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_work WHERE workflow_id=$1 AND kind='wait'",
    )
    .bind(&workflow.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(work_count, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn deadline_equality_times_out_and_expired_events_are_not_accepted() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: "approval".into(),
            timeout_ms: Some(60_000),
        },
    )
    .await;
    let command = event(&workflow, "approval");
    let receipt = db.store.send_workflow_event(&command).await.unwrap();
    sqlx::query("UPDATE workflow_waits SET registered_at_ms=0,deadline_ms=$2 WHERE workflow_id=$1")
        .bind(&workflow.workflow_id)
        .bind(codec::ms(receipt.accepted_at).unwrap())
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    let next = acquire(&db.store, "python").await;
    assert_eq!(
        db.store
            .activation_context(&next.lease.owner)
            .await
            .unwrap()
            .wake,
        Some(WorkflowWake::Timeout {
            key: "approval".into(),
            deadline: receipt.accepted_at
        })
    );
    assert_eq!(counts(&db.store, &workflow.workflow_id).await, (0, 0));
    assert!(
        db.store
            .send_workflow_event(&command)
            .await
            .unwrap()
            .already_accepted
    );
    install(
        &db.store,
        &next,
        1,
        WorkflowWait::Event {
            key: "later".into(),
            timeout_ms: Some(60_000),
        },
    )
    .await;
    force_due(&db.store, &workflow.workflow_id).await;
    assert!(matches!(
        db.store
            .send_workflow_event(&event(&workflow, "later"))
            .await,
        Err(ContractError::ObsoleteOperation)
    ));
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    let after = acquire(&db.store, "python").await;
    assert_eq!(
        db.store
            .activation_context(&after.lease.owner)
            .await
            .unwrap()
            .wake,
        Some(WorkflowWake::Timeout {
            key: "later".into(),
            deadline: 1
        })
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn timers_are_durable_due_work_and_do_not_age_out_while_dormant() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Timer {
            key: "month".into(),
            delay_ms: 30 * 24 * 60 * 60 * 1_000,
        },
    )
    .await;
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    assert!(matches!(
        db.store
            .send_workflow_event(&event(&workflow, "month"))
            .await,
        Err(ContractError::ObsoleteOperation)
    ));
    force_due(&db.store, &workflow.workflow_id).await;
    sqlx::query("UPDATE workflow_work SET created_at_ms=0 WHERE workflow_id=$1 AND kind='wait'")
        .bind(&workflow.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let first = one_work(&db.store).await;
    db.store
        .retry_work(&first, "transient database issue after a long dormant wait")
        .await
        .unwrap();
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE id=$1")
        .bind(&first.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let (left, right) = tokio::join!(db.store.claim_work(16), reopened.claim_work(16));
    let mut batch = left.unwrap();
    batch.extend(right.unwrap());
    assert_eq!(batch.len(), 1);
    assert!(matches!(
        db.store.apply_work(&first, &[]).await,
        Err(ContractError::OwnershipLost)
    ));
    reopened.apply_work(&batch[0], &[]).await.unwrap();
    let next = acquire(&db.store, "python").await;
    assert_eq!(
        db.store
            .activation_context(&next.lease.owner)
            .await
            .unwrap()
            .wake,
        Some(WorkflowWake::Timer {
            key: "month".into(),
            deadline: 1
        })
    );
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_fences_claimed_wait_and_preserves_event_receipt() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: "approval".into(),
            timeout_ms: Some(60_000),
        },
    )
    .await;
    let command = event(&workflow, "approval");
    db.store.send_workflow_event(&command).await.unwrap();
    let claimed = one_work(&db.store).await;
    db.store
        .cancel_workflow(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .apply_work(&claimed, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    drain_available(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    assert!(
        db.store
            .send_workflow_event(&command)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store.send_workflow_event(&event(&workflow, "new")).await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert_eq!(counts(&db.store, &workflow.workflow_id).await, (0, 0));
    let activations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
            .bind(&workflow.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(activations, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn receipt_identity_deduplicates_concurrent_senders_and_accepts_maximum_index_values() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let mut command = event(&workflow, "approval");
    let mut value = command.event.value().clone();
    // A deterministic, mostly nonrepeating printable URI keeps this index
    // boundary test independent of PostgreSQL's TOAST compression savings.
    let source = format!(
        "urn:test:{}",
        (0..2048)
            .map(|i| char::from(b'a' + ((i * 17 + i / 26) % 26) as u8))
            .collect::<String>()
    );
    value["source"] = json!(&source[..2048]);
    value["id"] = json!("i".repeat(128));
    command.event = WorkflowEvent::new(value).unwrap();
    let (a, b) = tokio::join!(
        db.store.send_workflow_event(&command),
        db.store.send_workflow_event(&command)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a.already_accepted, b.already_accepted);
    assert_eq!(a.accepted_at, b.accepted_at);
    let mut moved = command.clone();
    moved.key = "different-key".into();
    assert!(matches!(
        db.store.send_workflow_event(&moved).await,
        Err(ContractError::Conflict)
    ));
    let mut changed = command.clone();
    let mut value = changed.event.value().clone();
    value["id"] = json!("new_id");
    changed.event = WorkflowEvent::new(value).unwrap();
    assert!(matches!(
        db.store.send_workflow_event(&changed).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(counts(&db.store, &workflow.workflow_id).await.0, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn inbox_enforces_pending_count_and_byte_bounds_but_receipts_remain_replayable() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    for i in 0..WORKFLOW_MAX_PENDING_EVENTS {
        db.store
            .send_workflow_event(&event(&workflow, &format!("key{i}")))
            .await
            .unwrap();
    }
    assert!(matches!(
        db.store
            .send_workflow_event(&event(&workflow, "overflow"))
            .await,
        Err(ContractError::Busy)
    ));
    assert!(
        db.store
            .send_workflow_event(&event(&workflow, "key0"))
            .await
            .unwrap()
            .already_accepted
    );
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: "key0".into(),
            timeout_ms: None,
        },
    )
    .await;
    db.store
        .send_workflow_event(&event(&workflow, "replacement"))
        .await
        .unwrap();
    assert_eq!(counts(&db.store, &workflow.workflow_id).await.0, 128);
    let mut submit = command();
    submit.idempotency_key = "byte-bound".into();
    let other = db
        .store
        .accept_resolved_workflow(&submit, &descriptor())
        .await
        .unwrap();
    for i in 0..4 {
        let mut large = event(&other, &format!("large{i}"));
        let mut value = large.event.value().clone();
        value["data"] = json!("x".repeat(62 * 1024));
        large.event = WorkflowEvent::new(value).unwrap();
        db.store.send_workflow_event(&large).await.unwrap();
    }
    let mut overflow = event(&other, "overflow");
    let mut value = overflow.event.value().clone();
    value["data"] = json!("x".repeat(62 * 1024));
    overflow.event = WorkflowEvent::new(value).unwrap();
    assert!(matches!(
        db.store.send_workflow_event(&overflow).await,
        Err(ContractError::Busy)
    ));
    assert_eq!(counts(&db.store, &other.workflow_id).await.0, 4);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn one_shot_wait_keys_reject_reuse_and_early_event_timer_collision() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    assert_eq!(
        install(
            &db.store,
            &assigned,
            0,
            WorkflowWait::Timer {
                key: "once".into(),
                delay_ms: 0
            }
        )
        .await
        .activations_scheduled,
        1
    );
    let next = acquire(&db.store, "python").await;
    report(
        &db.store,
        &next,
        serde_json::to_value(decision(
            &next,
            1,
            wait_action(WorkflowWait::Timer {
                key: "once".into(),
                delay_ms: 0,
            }),
        ))
        .unwrap(),
    )
    .await;
    let work = one_work(&db.store).await;
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::Conflict)
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
                kind: "invalid_wait".into(),
                message: "one-shot key was reused".into(),
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
    let mut submit = command();
    submit.idempotency_key = "collision".into();
    let other = db
        .store
        .accept_resolved_workflow(&submit, &descriptor())
        .await
        .unwrap();
    db.store
        .send_workflow_event(&event(&other, "collision"))
        .await
        .unwrap();
    let current = acquire(&db.store, "python").await;
    report(
        &db.store,
        &current,
        serde_json::to_value(decision(
            &current,
            0,
            wait_action(WorkflowWait::Timer {
                key: "collision".into(),
                delay_ms: 0,
            }),
        ))
        .unwrap(),
    )
    .await;
    assert!(matches!(
        db.store.apply_work(&one_work(&db.store).await, &[]).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(counts(&db.store, &other.workflow_id).await.0, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn failed_resume_transaction_keeps_event_and_wait_recoverable() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: "approval".into(),
            timeout_ms: None,
        },
    )
    .await;
    db.store
        .send_workflow_event(&event(&workflow, "approval"))
        .await
        .unwrap();
    let work = one_work(&db.store).await;
    sqlx::raw_sql("CREATE FUNCTION reject_next_activation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture activation insert failure'; END $$; CREATE TRIGGER reject_next_activation BEFORE INSERT ON workflow_activations FOR EACH ROW EXECUTE FUNCTION reject_next_activation();")
        .execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    assert_eq!(counts(&db.store, &workflow.workflow_id).await.0, 1);
    let closed: Option<i64> =
        sqlx::query_scalar("SELECT closed_at_ms FROM workflow_waits WHERE workflow_id=$1")
            .bind(&workflow.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(closed.is_none());
    sqlx::raw_sql("DROP TRIGGER reject_next_activation ON workflow_activations; DROP FUNCTION reject_next_activation();").execute(&db.store.pool).await.unwrap();
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    assert_eq!(counts(&db.store, &workflow.workflow_id).await, (0, 0));
    let tasks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_activations WHERE workflow_id=$1")
            .bind(&workflow.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(tasks, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn prematurely_ready_timer_rearms_original_deadline_without_losing_wait() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Timer {
            key: "later".into(),
            delay_ms: 60_000,
        },
    )
    .await;
    let deadline: i64 =
        sqlx::query_scalar("SELECT deadline_ms FROM workflow_waits WHERE workflow_id=$1")
            .bind(&workflow.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    // Simulate a due work observation followed by a backwards clock step.
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE workflow_id=$1 AND kind='wait'")
        .bind(&workflow.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let work = one_work(&db.store).await;
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let (available, processed): (i64, Option<i64>) =
        sqlx::query_as("SELECT available_at_ms,processed_at_ms FROM workflow_work WHERE id=$1")
            .bind(&work.id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(available, deadline);
    assert!(processed.is_none());
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    force_due(&db.store, &workflow.workflow_id).await;
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
async fn sender_samples_acceptance_time_after_waiting_for_workflow_authority() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    install(
        &db.store,
        &assigned,
        0,
        WorkflowWait::Event {
            key: "approval".into(),
            timeout_ms: Some(60_000),
        },
    )
    .await;
    let mut lock = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&workflow.workflow_id)
        .execute(&mut *lock)
        .await
        .unwrap();
    let sender = db.store.clone();
    let command = event(&workflow, "approval");
    let sending = tokio::spawn(async move { sender.send_workflow_event(&command).await });
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT state,external_wait_key,%')")
                .fetch_one(&db.store.pool).await.unwrap();
            if blocked {break;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    // The request already reached PostgreSQL but has no workflow authority.
    // Acceptance must use its post-lock time, not its arrival/start time.
    sqlx::query("UPDATE workflow_waits SET deadline_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE workflow_id=$1")
        .bind(&workflow.workflow_id).execute(&mut *lock).await.unwrap();
    lock.commit().await.unwrap();
    assert!(matches!(
        sending.await.unwrap(),
        Err(ContractError::ObsoleteOperation)
    ));
    assert_eq!(counts(&db.store, &workflow.workflow_id).await, (0, 0));
    db.finish().await;
}
