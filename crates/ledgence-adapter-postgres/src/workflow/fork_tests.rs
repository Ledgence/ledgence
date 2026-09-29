//! Durable non-blocking fork acceptance against isolated PostgreSQL databases.
use super::*;

fn fork(assigned: &Assignment, key: &str, keys: &[&str]) -> WorkflowForkCommand {
    WorkflowForkCommand {
        owner: assigned.lease.owner.clone(),
        fork: WorkflowForkRequest {
            key: key.into(),
            branches: keys
                .iter()
                .map(|key| WorkflowBranch {
                    key: (*key).into(),
                    entrypoint: format!("calculate_{key}"),
                    queue: "branches".into(),
                    data: json!({"x":1,"y":2}),
                    retry_policy: command().input.retry_policy,
                    attempt_timeout_ms: 300_000,
                })
                .collect(),
        },
        processing_trace: None,
    }
}
async fn count(store: &PostgresStore, table: &str) -> i64 {
    let query = match table {
        "forks" => "SELECT count(*) FROM workflow_forks",
        "links" => "SELECT count(*) FROM owned_workflow_links",
        "runs" => "SELECT count(*) FROM workflow_runs",
        _ => panic!("unsupported fixture table"),
    };
    sqlx::query_scalar(query)
        .fetch_one(&store.pool)
        .await
        .unwrap()
}

/// Upgrade fixtures describe historical rows directly. Calling current workflow
/// creation against an old schema would invoke projections not installed yet.
async fn historical_workflow_fixture(store: &PostgresStore) -> WorkflowSnapshot {
    let mut connection = store.pool.begin().await.unwrap();
    let now = db::now(&mut connection).await.unwrap();
    let submit = command();
    let controller = descriptor();
    sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,continuation,checkpoint_bytes,submitted_at_ms,correlation_key,queue) VALUES('wf_upgrade',$1,$2,$3,$4,$5,'running','start',$6,$7,$8,$9)")
        .bind(&submit.input.tenant_id).bind(&submit.input.namespace).bind(&submit.idempotency_key)
        .bind(codec::encode(&submit).unwrap()).bind(codec::encode(&controller).unwrap()).bind(b"null".as_slice())
        .bind(codec::ms(now).unwrap()).bind(&submit.input.correlation_key).bind(&submit.input.queue)
        .execute(&mut *connection).await.unwrap();
    let mut run = load_run(&mut connection, &scope(), Some("wf_upgrade"), None, false)
        .await
        .unwrap();
    let context = WorkflowActivationContext {
        v: 1,
        workflow_id: "wf_upgrade".into(),
        parent_workflow_id: None,
        root_workflow_id: None,
        activation_id: "task_upgrade".into(),
        revision: 0,
        continuation: "start".into(),
        state: Value::Null,
        inputs: BTreeMap::new(),
        wake: None,
        local_steps: vec![],
    };
    let mut task_command = submit;
    task_command.idempotency_key = "workflow:wf_upgrade:task_upgrade".into();
    insert_task(
        &mut connection,
        &task_command,
        &controller,
        "task_upgrade",
        &run.snapshot,
        true,
        now,
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO workflow_activations(activation_id,workflow_id,revision,task_id,context_bytes) VALUES('task_upgrade','wf_upgrade',0,'task_upgrade',$1)")
        .bind(codec::encode(&context).unwrap()).execute(&mut *connection).await.unwrap();
    link_task(
        &mut connection,
        "task_upgrade",
        "wf_upgrade",
        "task_upgrade",
        true,
        "controller",
    )
    .await
    .unwrap();
    run.snapshot.activation_id = Some("task_upgrade".into());
    save_run(&mut connection, &run).await.unwrap();
    connection.commit().await.unwrap();
    run.snapshot
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_migration_preserves_existing_activation_and_local_receipts() {
    let db = TestDb::without_migrations().await;
    let previous = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20260928000000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    let parent = historical_workflow_fixture(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &controller).await;
    let old_local = local(&controller, "existing");
    sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms,callable) VALUES($1,$2,$3,$4,1,$5)")
        .bind(&old_local.owner.task_id).bind(&old_local.record.key).bind(codec::encode(&old_local.record).unwrap())
        .bind(&old_local.owner.attempt_id).bind(&old_local.record.callable).execute(&db.store.pool).await.unwrap();
    let before = db
        .store
        .activation_context(&controller.lease.owner)
        .await
        .unwrap();
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    assert_json_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap(),
        parent
    );
    assert_json_eq!(
        db.store
            .activation_context(&controller.lease.owner)
            .await
            .unwrap(),
        before
    );
    assert!(
        db.store
            .record_local_result(&local(&controller, "existing"))
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        !db.store
            .fork_workflow(&fork(&controller, "after-upgrade", &["a"]))
            .await
            .unwrap()
            .already_accepted
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_dispatches_pinned_entrypoints_while_parent_is_active_and_joins_early_results() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &controller).await;
    let before = db
        .store
        .activation_context(&controller.lease.owner)
        .await
        .unwrap();
    let mut command = fork(&controller, "parallel", &["a", "b"]);
    command.processing_trace = Some(TraceContext {
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01".into(),
        tracestate: Some("vendor=branch".into()),
    });
    let (first, concurrent) = tokio::join!(
        db.store.fork_workflow(&command),
        db.store.fork_workflow(&command)
    );
    let first = first.unwrap();
    let concurrent = concurrent.unwrap();
    assert_ne!(first.already_accepted, concurrent.already_accepted);
    assert_eq!(first.branch_keys, vec!["a", "b"]);
    assert_eq!(count(&db.store, "forks").await, 1);
    assert_eq!(count(&db.store, "links").await, 2);
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    assert!(
        reopened
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    reopened.close().await;
    assert_json_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap(),
        &parent
    );
    assert_json_eq!(
        db.store
            .activation_context(&controller.lease.owner)
            .await
            .unwrap(),
        before
    );
    db.store
        .record_local_result(&local(&controller, "during-fork"))
        .await
        .unwrap();
    for _ in 0..2 {
        let branch = acquire(&db.store, "branches").await;
        assert_eq!(branch.descriptor, controller.descriptor);
        assert_eq!(branch.event.value()["data"], json!({"x":1,"y":2}));
        let context = db
            .store
            .activation_context(&branch.lease.owner)
            .await
            .unwrap();
        assert!(matches!(
            context.continuation.as_str(),
            "calculate_a" | "calculate_b"
        ));
        assert_eq!(context.revision, 0);
        assert_eq!(
            context.parent_workflow_id.as_deref(),
            Some(parent.workflow_id.as_str())
        );
        assert_eq!(context.root_workflow_id, context.parent_workflow_id);
        let task = db
            .store
            .inspect(&scope(), &branch.lease.owner.task_id)
            .await
            .unwrap();
        assert_eq!(task.origin_trace, command.processing_trace);
        apply_decision(
            &db.store,
            &branch,
            0,
            WorkflowAction::Complete {
                output: json!(context.continuation),
            },
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
    }
    let active = db
        .store
        .activation_context(&controller.lease.owner)
        .await
        .unwrap();
    assert!(active.inputs.is_empty());
    assert_eq!(active.continuation, "start");
    assert_eq!(active.revision, 0);
    assert_eq!(active.local_steps.len(), 1);
    assert_eq!(
        apply_decision(
            &db.store,
            &controller,
            0,
            WorkflowAction::Suspend {
                state: json!({"joined":true}),
                continuation: "join".into(),
                commands: vec![],
                until: first.branch_keys,
            },
            &[]
        )
        .await
        .activations_scheduled,
        1
    );
    let joined = acquire(&db.store, "python").await;
    dispatched(&db.store, &joined).await;
    let inputs = db
        .store
        .activation_context(&joined.lease.owner)
        .await
        .unwrap();
    assert_eq!(inputs.inputs.len(), 2);
    assert_eq!(inputs.revision, 1);
    assert_eq!(inputs.continuation, "join");
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    // Workflow-wide keys remain replayable from later live activations.
    command.owner = joined.lease.owner.clone();
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(count(&db.store, "links").await, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_receipts_reconcile_expiry_and_cancellation_but_new_work_is_fenced() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    let command = fork(&controller, "one", &["a"]);
    assert!(matches!(
        db.store.fork_workflow(&command).await,
        Err(ContractError::OwnershipLost)
    ));
    dispatched(&db.store, &controller).await;
    let mut wrong = command.clone();
    wrong.owner.lease_id = "wrong".into();
    assert!(matches!(
        db.store.fork_workflow(&wrong).await,
        Err(ContractError::OwnershipLost)
    ));
    assert!(
        !db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    let mut replay = command.clone();
    replay.fork.branches[0].data = json!({"y":2,"x":1});
    replay.processing_trace = super::command().origin_trace;
    assert!(
        db.store
            .fork_workflow(&replay)
            .await
            .unwrap()
            .already_accepted
    );
    for case in 0..6 {
        let mut changed = command.clone();
        match case {
            0 => changed.fork.branches[0].data = json!({"x":1.0,"y":2}),
            1 => changed.fork.branches[0].entrypoint = "other".into(),
            2 => changed.fork.branches[0].queue = "other".into(),
            3 => changed.fork.branches[0].key = "b".into(),
            4 => changed.fork.branches[0].retry_policy.max_attempts = 4,
            _ => changed.fork.branches[0].attempt_timeout_ms = 60_000,
        }
        assert!(
            matches!(
                db.store.fork_workflow(&changed).await,
                Err(ContractError::Conflict)
            ),
            "case {case}"
        );
    }
    assert!(matches!(
        db.store
            .fork_workflow(&fork(
                &controller,
                "other-fork",
                &["new-before-collision", "a"]
            ))
            .await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(count(&db.store, "forks").await, 1);
    assert_eq!(count(&db.store, "links").await, 1);
    sqlx::query("UPDATE attempts SET expires_at_ms=0 WHERE attempt_id=$1")
        .bind(&controller.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store
            .fork_workflow(&fork(&controller, "expired", &["b"]))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    db.store
        .cancel_workflow(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store
            .fork_workflow(&fork(&controller, "cancelled", &["b"]))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_lost_ack_reconciles_from_new_attempt_then_owned_cancellation_and_retention_drain() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    dispatched(&db.store, &first).await;
    let command = fork(&first, "stable", &["a"]);
    db.store.fork_workflow(&command).await.unwrap();
    super::owned::failed_attempt(&db.store, &first).await;
    let second = acquire(&db.store, "python").await;
    let mut replay = command.clone();
    replay.owner = second.lease.owner.clone();
    assert!(matches!(
        db.store.fork_workflow(&replay).await,
        Err(ContractError::OwnershipLost)
    ));
    dispatched(&db.store, &second).await;
    assert!(
        db.store
            .fork_workflow(&replay)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store
            .fork_workflow(&fork(&first, "stale-new", &["b"]))
            .await,
        Err(ContractError::OwnershipLost)
    ));
    assert_eq!(count(&db.store, "links").await, 1);
    db.store
        .cancel_workflow(&scope(), &parent.workflow_id)
        .await
        .unwrap();
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store.fork_workflow(&replay).await,
        Err(ContractError::OwnershipLost)
    ));
    // Controller cancellation settlement releases its execution authority;
    // owned queued branches are then durably cancelled by parent drain.
    db.store
        .settle(&completed(&second, Quiescence::Confirmed, Value::Null))
        .await
        .unwrap();
    super::owned::recover(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    let child_state: String = sqlx::query_scalar("SELECT w.state FROM owned_workflow_links l JOIN workflow_runs w ON w.workflow_id=l.child_workflow_id WHERE l.parent_workflow_id=$1").bind(&parent.workflow_id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(child_state, "cancelled");
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2,cancel_requested_at_ms=CASE WHEN cancel_requested_at_ms IS NOT NULL THEN 2 ELSE NULL END WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE attempts SET finished_at_ms=2 WHERE finished_at_ms IS NOT NULL")
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE accepted_settlements SET accepted_at=2")
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0")
        .execute(&db.store.pool)
        .await
        .unwrap();
    for _ in 0..300 {
        let progress = db
            .store
            .retain_batch(
                &scope(),
                &RetentionPolicy {
                    batch_size: 2,
                    ..Default::default()
                },
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(progress.deleted_rows <= 16);
    }
    assert_eq!(count(&db.store, "runs").await, 0);
    assert_eq!(count(&db.store, "links").await, 0);
    assert_eq!(count(&db.store, "forks").await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_transaction_rolls_back_partial_children_and_dispatch_then_commits_all_obligations() {
    let db = TestDb::new().await;
    start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &controller).await;
    db.store
        .configure_route(&DispatchRoute {
            scope: scope(),
            queue: "broker".into(),
            destination: "fork-destination".into(),
        })
        .await
        .unwrap();
    let mut command = fork(&controller, "atomic", &["a", "b"]);
    command.fork.branches[0].queue = "broker".into();
    command.fork.branches[1].queue = "reject".into();
    sqlx::raw_sql("CREATE FUNCTION reject_fork_task() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.queue='reject' THEN RAISE EXCEPTION 'fork fixture rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_fork_task BEFORE INSERT ON tasks FOR EACH ROW EXECUTE FUNCTION reject_fork_task()").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.fork_workflow(&command).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(count(&db.store, "forks").await, 0);
    assert_eq!(count(&db.store, "links").await, 0);
    assert_eq!(count(&db.store, "runs").await, 1);
    let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_intents")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(intents, 0);
    sqlx::raw_sql("DROP TRIGGER reject_fork_task ON tasks; DROP FUNCTION reject_fork_task()")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(
        !db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    let obligations: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_intents d JOIN tasks t ON t.task_id=d.task_id JOIN workflow_runs w ON w.workflow_id=t.workflow_id WHERE w.parent_workflow_id IS NOT NULL AND t.queue='broker'").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(obligations, 1);
    assert_eq!(count(&db.store, "forks").await, 1);
    assert_eq!(count(&db.store, "links").await, 2);
    assert!(
        db.store
            .fork_workflow(&command)
            .await
            .unwrap()
            .already_accepted
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_children_cannot_be_adopted_by_legacy_commands_and_all_writes_roll_back() {
    let db = TestDb::new().await;
    let parent = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &controller).await;
    let command = fork(&controller, "stable", &["a"]);
    db.store.fork_workflow(&command).await.unwrap();
    let branch = &command.fork.branches[0];
    let adopt = WorkflowChildCommand {
        kind: WorkflowChildKind::Workflow,
        key: branch.key.clone(),
        program: descriptor().program,
        queue: branch.queue.clone(),
        data: branch.data.clone(),
        retry_policy: branch.retry_policy.clone(),
        attempt_timeout_ms: branch.attempt_timeout_ms,
    };
    report(
        &db.store,
        &controller,
        serde_json::to_value(decision(
            &controller,
            0,
            WorkflowAction::Continue {
                state: Value::Null,
                continuation: "next".into(),
                commands: vec![child("uncommitted"), adopt],
            },
        ))
        .unwrap(),
    )
    .await;
    assert!(matches!(
        db.store
            .apply_work(
                &one_work(&db.store).await,
                &[
                    resolved("uncommitted"),
                    ResolvedWorkflowChild {
                        kind: WorkflowChildKind::Workflow,
                        ..resolved("a")
                    }
                ]
            )
            .await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(
        db.store
            .workflow_status(&scope(), &parent.workflow_id)
            .await
            .unwrap()
            .revision,
        0
    );
    let registered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_task_links WHERE NOT is_activation")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(registered, 0);
    assert_eq!(count(&db.store, "links").await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_does_not_adopt_preexisting_ordinary_or_owned_children() {
    let db = TestDb::new().await;
    start(&db.store).await;
    let first = acquire(&db.store, "python").await;
    let mut ordinary = child("ordinary");
    ordinary.data = json!({"x":1,"y":2});
    ordinary.queue = "branches".into();
    let mut nested = ordinary.clone();
    nested.kind = WorkflowChildKind::Workflow;
    nested.key = "owned".into();
    apply_decision(
        &db.store,
        &first,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "next".into(),
            commands: vec![ordinary, nested],
        },
        &[
            resolved("ordinary"),
            ResolvedWorkflowChild {
                kind: WorkflowChildKind::Workflow,
                ..resolved("owned")
            },
        ],
    )
    .await;
    let current = acquire(&db.store, "python").await;
    dispatched(&db.store, &current).await;
    for existing in ["ordinary", "owned"] {
        let mut command = fork(&current, existing, &["fresh", existing]);
        command.fork.branches[1].entrypoint = "start".into();
        assert!(matches!(
            db.store.fork_workflow(&command).await,
            Err(ContractError::Conflict)
        ));
    }
    assert_eq!(count(&db.store, "forks").await, 0);
    assert_eq!(count(&db.store, "links").await, 1);
    assert_eq!(count(&db.store, "runs").await, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fork_ledger_and_combined_child_budgets_are_atomic() {
    let db = TestDb::new().await;
    start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    dispatched(&db.store, &controller).await;
    for index in 0..2 {
        let key = format!("large-{index}");
        let mut command = fork(&controller, &key, &[&key]);
        command.fork.branches[0].data = json!("x".repeat(100 * 1024));
        db.store.fork_workflow(&command).await.unwrap();
    }
    let mut overflow = fork(&controller, "overflow", &["overflow"]);
    overflow.fork.branches[0].data = json!("x".repeat(100 * 1024));
    assert!(matches!(
        db.store.fork_workflow(&overflow).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(count(&db.store, "forks").await, 2);
    assert_eq!(count(&db.store, "links").await, 2);
    let keys: Vec<String> = (0..61).map(|index| format!("small-{index}")).collect();
    db.store
        .fork_workflow(&fork(
            &controller,
            "remaining",
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .fork_workflow(&fork(&controller, "too-many", &["extra", "overflow"]))
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    report(
        &db.store,
        &controller,
        serde_json::to_value(decision(
            &controller,
            0,
            WorkflowAction::Continue {
                state: Value::Null,
                continuation: "next".into(),
                // The first fits; the second must account for the first one's
                // pending insert and atomically roll the entire decision back.
                commands: vec![child("extra"), child("overflow")],
            },
        ))
        .unwrap(),
    )
    .await;
    assert!(matches!(
        db.store
            .apply_work(
                &one_work(&db.store).await,
                &[resolved("extra"), resolved("overflow")]
            )
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(count(&db.store, "links").await, 63);
    assert_eq!(count(&db.store, "forks").await, 3);
    let ordinary: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_task_links WHERE NOT is_activation")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(ordinary, 0);
    db.finish().await;
}
