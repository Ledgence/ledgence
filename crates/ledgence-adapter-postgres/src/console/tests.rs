use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, completed, descriptor, scope};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, Execute};
mod executions;
fn pagination() -> ConsolePagination {
    ConsolePagination {
        limit: 1,
        cursor: None,
    }
}
async fn query(db: &TestDb, query: ConsoleQuery) -> ConsoleQueryReply {
    db.store.query_console(&scope(), &query).await.unwrap()
}
async fn workflow(db: &TestDb, key: &str) -> (WorkflowSnapshot, Assignment) {
    let mut submit = command();
    submit.idempotency_key = key.into();
    submit.input.correlation_key = Some("shared".into());
    let workflow = db
        .store
        .accept_resolved_workflow(&submit, &descriptor())
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
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    (workflow, assigned)
}
async fn apply(
    db: &TestDb,
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
    db.store
        .settle(&completed(
            assigned,
            Quiescence::Confirmed,
            serde_json::to_value(decision).unwrap(),
        ))
        .await
        .unwrap();
    let work = db.store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 1);
    db.store.apply_work(&work[0], resolved).await.unwrap();
}
fn child(key: &str, kind: WorkflowChildKind) -> WorkflowChildCommand {
    WorkflowChildCommand {
        kind,
        key: key.into(),
        program: descriptor().program,
        queue: "children".into(),
        data: json!({"private":"child input"}),
        retry_policy: RetryPolicy::default(),
        attempt_timeout_ms: 300000,
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_tasks_page_exact_filters_ties_and_metadata_without_payload_reads() {
    let db = TestDb::new().await;
    let mut ids = Vec::new();
    for (key, correlation) in [("one", Some("")), ("two", Some("")), ("three", None)] {
        let mut input = command();
        input.idempotency_key = key.into();
        input.input.correlation_key = correlation.map(str::to_owned);
        ids.push(
            db.store
                .accept_resolved_submission(&input, &descriptor())
                .await
                .unwrap()
                .task_id,
        );
    }
    sqlx::query("UPDATE tasks SET submitted_at_ms=10,input_bytes=decode('00','hex')")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let filters = TaskFilters {
        correlation_key: Some("".into()),
        ..TaskFilters::default()
    };
    let first = ConsoleQuery::Tasks {
        filters: filters.clone(),
        page: pagination(),
    };
    let ConsoleQueryReply::Tasks(first) = query(&db, first).await else {
        panic!()
    };
    assert_eq!(first.items.len(), 1);
    assert!(first.next_cursor.is_some());
    let ConsoleQueryReply::Tasks(second) = query(
        &db,
        ConsoleQuery::Tasks {
            filters: filters.clone(),
            page: ConsolePagination {
                limit: 1,
                cursor: first.next_cursor.clone(),
            },
        },
    )
    .await
    else {
        panic!()
    };
    assert!(second.next_cursor.is_none());
    assert_ne!(first.items[0].task.task_id, second.items[0].task.task_id);
    assert_eq!(first.items[0].descriptor.program, descriptor().program);
    let mut changed = filters;
    changed.queue = Some("different".into());
    assert!(
        db.store
            .query_console(
                &scope(),
                &ConsoleQuery::Tasks {
                    filters: changed,
                    page: ConsolePagination {
                        limit: 1,
                        cursor: first.next_cursor
                    }
                }
            )
            .await
            .is_err()
    );
    let bytes = serde_json::to_string(&first.items).unwrap();
    assert!(!bytes.contains("amount"));
    assert!(!bytes.contains("data"));
    assert_eq!(ids.len(), 3);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_attempts_report_actual_history_and_exclude_all_lease_authority() {
    let db = TestDb::new().await;
    let (_, session, assigned) = crate::tests::claimed(&db.store).await;
    db.store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    db.store
        .settle(&completed(&assigned, Quiescence::Confirmed, json!(null)))
        .await
        .unwrap();
    let task_id = assigned.lease.owner.task_id.clone();
    let attempt_id = assigned.lease.owner.attempt_id.clone();
    let ConsoleQueryReply::Attempts(attempts) = query(
        &db,
        ConsoleQuery::Attempts {
            task_id: task_id.clone(),
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(attempts.items[0].attempt_id, attempt_id);
    assert_eq!(attempts.items[0].worker_session_id, session.id);
    assert!(attempts.items[0].claimed_at.is_some());
    assert!(attempts.items[0].dispatch_authorized_at.is_some());
    assert!(attempts.items[0].finished_at.is_some());
    let ConsoleQueryReply::Attempt(detail) = query(&db, ConsoleQuery::Attempt { attempt_id }).await
    else {
        panic!()
    };
    assert_eq!(detail.process_id, Some(42));
    assert_eq!(detail.worker_elapsed_ms, Some(ConsoleU64(10)));
    assert_eq!(detail.process_instance_id, None);
    let value = serde_json::to_string(&detail).unwrap();
    for forbidden in [
        "lease_id",
        "owner",
        "event",
        "output",
        "accepted_command",
        "traceparent",
    ] {
        assert!(!value.contains(forbidden), "{forbidden}");
    }
    sqlx::query("UPDATE tasks SET retiring_at_ms=terminal_at_ms WHERE task_id=$1")
        .bind(&task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .query_console(
                &scope(),
                &ConsoleQuery::Attempts {
                    task_id,
                    page: pagination()
                }
            )
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_workflow_relations_locals_and_child_wait_use_persisted_membership() {
    let db = TestDb::new().await;
    let (root, assigned) = workflow(&db, "root").await;
    let local = LocalResultCommand {
        owner: assigned.lease.owner.clone(),
        record: LocalStepRecord {
            key: "local".into(),
            callable: "billing.read".into(),
            input: json!({"secret":"input"}),
            output: json!({"secret":"output"}),
        },
    };
    db.store.record_local_result(&local).await.unwrap();
    apply(
        &db,
        &assigned,
        WorkflowAction::Suspend {
            state: json!({"secret":"checkpoint"}),
            continuation: "join".into(),
            commands: vec![
                child("task", WorkflowChildKind::Task),
                child("flow", WorkflowChildKind::Workflow),
            ],
            until: vec!["task".into(), "flow".into()],
        },
        &[
            ResolvedWorkflowChild {
                kind: WorkflowChildKind::Task,
                key: "task".into(),
                descriptor: descriptor(),
            },
            ResolvedWorkflowChild {
                kind: WorkflowChildKind::Workflow,
                key: "flow".into(),
                descriptor: descriptor(),
            },
        ],
    )
    .await;
    // A standalone task sharing the correlation is not an owned child.
    let mut standalone = command();
    standalone.idempotency_key = "standalone".into();
    standalone.input.correlation_key = Some("shared".into());
    db.store
        .accept_resolved_submission(&standalone, &descriptor())
        .await
        .unwrap();
    let id = root.workflow_id.clone();
    let ConsoleQueryReply::Children(first) = query(
        &db,
        ConsoleQuery::Children {
            workflow_id: id.clone(),
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(first.items[0].kind, ConsoleChildKind::Task);
    assert_eq!(first.items[0].creating_revision, ConsoleU64(0));
    let ConsoleQueryReply::Children(second) = query(
        &db,
        ConsoleQuery::Children {
            workflow_id: id.clone(),
            page: ConsolePagination {
                limit: 1,
                cursor: first.next_cursor,
            },
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(second.items[0].kind, ConsoleChildKind::Workflow);
    assert!(second.next_cursor.is_none());
    let child_id = second.items[0].target_id.clone();
    let ConsoleQueryReply::Workflow(detail) = query(
        &db,
        ConsoleQuery::Workflow {
            workflow_id: id.clone(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(detail.summary.workflow.state, WorkflowState::Waiting);
    assert_eq!(detail.summary.queue, "python");
    assert_eq!(detail.child_wait.as_ref().unwrap().command_keys.len(), 2);
    let ConsoleQueryReply::Waits(waits) = query(
        &db,
        ConsoleQuery::Waits {
            workflow_id: id.clone(),
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert!(waits.page.items.is_empty());
    assert_eq!(waits.child_wait.unwrap().command_keys.len(), 2);
    let ConsoleQueryReply::Activations(activations) = query(
        &db,
        ConsoleQuery::Activations {
            workflow_id: id.clone(),
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(
        activations.items[0].activation_id,
        assigned.lease.owner.task_id
    );
    assert!(activations.items[0].applied_at.is_some());
    let ConsoleQueryReply::LocalSteps(locals) = query(
        &db,
        ConsoleQuery::LocalSteps {
            workflow_id: id.clone(),
            activation_id: assigned.lease.owner.task_id.clone(),
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(locals.items[0].callable, "billing.read");
    assert_eq!(locals.items[0].attempt_id, assigned.lease.owner.attempt_id);
    let ConsoleQueryReply::History(history) = query(
        &db,
        ConsoleQuery::History {
            workflow_id: id.clone(),
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(history.items[0].reason, "started");
    assert!(history.next_cursor.is_some());
    let ConsoleQueryReply::Workflows(children) = query(
        &db,
        ConsoleQuery::Workflows {
            filters: ConsoleWorkflowFilters {
                parent_workflow_id: Some(id.clone()),
                ..Default::default()
            },
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(children.items[0].workflow.workflow_id, child_id);
    // Corrupt heavy payloads only after authoritative execution setup; console
    // metadata must keep working from explicit columns, not parse those blobs.
    sqlx::query("UPDATE workflow_runs SET submission_bytes=decode('00','hex'),checkpoint_bytes=decode('00','hex')").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE workflow_local_results SET record_bytes=decode('00','hex')")
        .execute(&db.store.pool)
        .await
        .unwrap();
    query(
        &db,
        ConsoleQuery::Workflow {
            workflow_id: id.clone(),
        },
    )
    .await;
    query(
        &db,
        ConsoleQuery::LocalSteps {
            workflow_id: id.clone(),
            activation_id: assigned.lease.owner.task_id,
            page: pagination(),
        },
    )
    .await;
    let other = Scope {
        tenant_id: "other".into(),
        namespace: "other".into(),
    };
    assert_eq!(
        db.store
            .query_console(
                &other,
                &ConsoleQuery::Children {
                    workflow_id: id.clone(),
                    page: pagination()
                }
            )
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .query_console(
                &scope(),
                &ConsoleQuery::LocalSteps {
                    workflow_id: id.clone(),
                    activation_id: "missing".into(),
                    page: pagination()
                }
            )
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    sqlx::query("UPDATE workflow_runs SET state='cancelled',terminal_at_ms=submitted_at_ms,retiring_at_ms=submitted_at_ms WHERE workflow_id=$1").bind(&id).execute(&db.store.pool).await.unwrap();
    for q in [
        ConsoleQuery::Workflow {
            workflow_id: id.clone(),
        },
        ConsoleQuery::Children {
            workflow_id: id.clone(),
            page: pagination(),
        },
        ConsoleQuery::Activations {
            workflow_id: id.clone(),
            page: pagination(),
        },
        ConsoleQuery::Waits {
            workflow_id: id.clone(),
            page: pagination(),
        },
        ConsoleQuery::History {
            workflow_id: id.clone(),
            page: pagination(),
        },
    ] {
        assert_eq!(
            db.store.query_console(&scope(), &q).await.unwrap_err(),
            ContractError::NotFound
        );
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_external_wait_is_observed_without_waking_or_claiming_work() {
    let db = TestDb::new().await;
    let (root, assigned) = workflow(&db, "timer").await;
    apply(
        &db,
        &assigned,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "after_timer".into(),
            commands: vec![],
            wait: WorkflowWait::Timer {
                key: "timer".into(),
                delay_ms: 60000,
            },
        },
        &[],
    )
    .await;
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_work WHERE lease_token IS NOT NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let ConsoleQueryReply::Waits(waits) = query(
        &db,
        ConsoleQuery::Waits {
            workflow_id: root.workflow_id,
            page: pagination(),
        },
    )
    .await
    else {
        panic!()
    };
    assert_eq!(waits.page.items[0].kind, ConsoleWaitKind::Timer);
    assert_eq!(waits.page.items[0].closed_at, None);
    assert!(waits.page.items[0].deadline.is_some());
    assert!(waits.child_wait.is_none());
    let after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_work WHERE lease_token IS NOT NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_children_deep_seek_uses_ordered_indexes_and_bounded_hydration() {
    let db = TestDb::new().await;
    let mut connection = db.store.pool.acquire().await.unwrap();
    // Query-shape fixture with 50,000 persisted relation metadata records. No
    // application payloads are necessary to assess the planner's access path.
    for sql in [
        "CREATE TEMP TABLE tasks(task_id text COLLATE \"C\" PRIMARY KEY,tenant_id text,namespace text,state text,retiring_at_ms bigint)",
        "CREATE TEMP TABLE workflow_runs(workflow_id text COLLATE \"C\" PRIMARY KEY,tenant_id text,namespace text,state text,retiring_at_ms bigint)",
        "CREATE TEMP TABLE workflow_task_links(workflow_id text COLLATE \"C\",creating_revision numeric,command_key text COLLATE \"C\",task_id text COLLATE \"C\",activation_id text,is_activation boolean,consumed boolean)",
        "CREATE TEMP TABLE owned_workflow_links(parent_workflow_id text COLLATE \"C\",creating_revision numeric,command_key text COLLATE \"C\",child_workflow_id text COLLATE \"C\",creating_activation_id text,consumed boolean)",
        "CREATE INDEX fixture_task_seek ON workflow_task_links(workflow_id,creating_revision,command_key,task_id) WHERE NOT is_activation",
        "CREATE INDEX fixture_workflow_seek ON owned_workflow_links(parent_workflow_id,creating_revision,command_key,child_workflow_id)",
        "INSERT INTO tasks SELECT 'task_'||lpad(n::text,8,'0'),'acme','billing','queued',NULL FROM generate_series(1,50000)n",
        "INSERT INTO workflow_task_links SELECT 'parent',n/20,'key_'||lpad(n::text,8,'0'),'task_'||lpad(n::text,8,'0'),'activation_'||(n/20),false,false FROM generate_series(1,50000)n",
        "INSERT INTO workflow_runs SELECT 'workflow_'||lpad(n::text,8,'0'),'acme','billing','running',NULL FROM generate_series(1,50000)n",
        "INSERT INTO owned_workflow_links SELECT 'parent',n/20,'key_'||lpad(n::text,8,'0'),'workflow_'||lpad(n::text,8,'0'),'activation_'||(n/20),false FROM generate_series(1,50000)n",
        "ANALYZE tasks",
        "ANALYZE workflow_runs",
        "ANALYZE workflow_task_links",
        "ANALYZE owned_workflow_links",
    ] {
        sqlx::query(sql).execute(&mut *connection).await.unwrap();
    }
    for kind in ["task", "workflow"] {
        let position = vec![
            ConsoleKey::Number(ConsoleU64(2450)),
            ConsoleKey::Text(kind.into()),
            ConsoleKey::Text("key_00049000".into()),
            ConsoleKey::Text(format!("{kind}_00049000")),
        ];
        let mut builder =
            workflows::children_query(&scope(), "parent", 50, Some(&position)).unwrap();
        let mut query = builder.build();
        let args = query.take_arguments().unwrap().unwrap();
        let sql = format!("EXPLAIN (ANALYZE,BUFFERS) {}", query.sql().as_str());
        let plan = sqlx::query_scalar_with::<_, String, _>(AssertSqlSafe(sql), args)
            .fetch_all(&mut *connection)
            .await
            .unwrap()
            .join("\n");
        println!("Console child deep {kind} plan:\n{plan}");
        assert!(plan.contains("fixture_task_seek"));
        assert!(plan.contains("fixture_workflow_seek"));
        assert!(!plan.contains("Seq Scan"));
        assert!(!plan.contains("Rows Removed by Filter: 49000"));
    }
    drop(connection);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_query_migration_backfills_existing_metadata_without_rewriting_payloads() {
    let db = TestDb::without_migrations().await;
    let previous = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|m| m.version < 20260926010000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    let (_, _, assigned) = crate::tests::claimed(&db.store).await;
    let submission = codec::encode(&command()).unwrap();
    let controller = codec::encode(&descriptor()).unwrap();
    sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,continuation,checkpoint_bytes,submitted_at_ms) VALUES('wf_upgrade','acme','billing','upgrade',$1,$2,'running','start',$3,1)")
        .bind(&submission).bind(&controller).bind(b"null".as_slice()).execute(&db.store.pool).await.unwrap();
    let context = WorkflowActivationContext {
        parent_workflow_id: None,
        root_workflow_id: None,
        v: 1,
        workflow_id: "wf_upgrade".into(),
        activation_id: assigned.lease.owner.task_id.clone(),
        revision: 0,
        continuation: "start".into(),
        state: Value::Null,
        inputs: Default::default(),
        local_steps: vec![],
        wake: None,
    };
    context.validate().unwrap();
    sqlx::query("INSERT INTO workflow_activations(activation_id,workflow_id,revision,task_id,context_bytes) VALUES($1,'wf_upgrade',0,$1,$2)")
        .bind(&assigned.lease.owner.task_id).bind(codec::encode(&context).unwrap()).execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO workflow_task_links(task_id,workflow_id,activation_id,is_activation,command_key) VALUES($1,'wf_upgrade',$1,true,'controller')")
        .bind(&assigned.lease.owner.task_id).execute(&db.store.pool).await.unwrap();
    let record = codec::encode(&LocalStepRecord {
        key: "step".into(),
        callable: "billing.old".into(),
        input: json!({"integer":9007199254740993_u64,"nul":"\u{0}","callable":"spoof"}),
        output: json!(null),
    })
    .unwrap();
    sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms) VALUES($1,'step',$2,$3,1)")
        .bind(&assigned.lease.owner.task_id).bind(&record).bind(&assigned.lease.owner.attempt_id).execute(&db.store.pool).await.unwrap();
    // Canonical payloads may contain arbitrary escaped strings. Even and odd
    // runs must neither break JSON extraction nor alter literal identifiers.
    let mut edge_cases = Vec::new();
    for slashes in 0..=8 {
        let prefix = "\\".repeat(slashes);
        let identifier = format!("literal{prefix}u0000");
        let data = json!({
            "queue":"spoof", "callable":"spoof", "雪":{
                "actual":format!("{prefix}\u{0}\u{0}"),
                "literal":format!("{prefix}u0000"),
                "nested":[{"queue":"wrong","callable":"wrong","nul":"\u{0}"}]
            }
        });
        let mut submit = command();
        submit.input.queue = identifier.clone();
        submit.input.data = data.clone();
        let submission = codec::encode(&submit).unwrap();
        let key = format!("edge_{slashes}");
        sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,continuation,checkpoint_bytes,submitted_at_ms) VALUES($1,'acme','billing',$1,$2,$3,'running','start',$4,1)")
            .bind(&key).bind(&submission).bind(&controller).bind(b"null".as_slice()).execute(&db.store.pool).await.unwrap();
        let record = codec::encode(&LocalStepRecord {
            key: key.clone(),
            callable: identifier.clone(),
            input: data,
            output: json!(null),
        })
        .unwrap();
        sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms) VALUES($1,$2,$3,$4,1)")
            .bind(&assigned.lease.owner.task_id).bind(&key).bind(&record).bind(&assigned.lease.owner.attempt_id).execute(&db.store.pool).await.unwrap();
        edge_cases.push((key, identifier, submission, record));
    }
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    for (key, identifier, submission, record) in edge_cases {
        let (queue, bytes): (String, Vec<u8>) =
            sqlx::query_as("SELECT queue,submission_bytes FROM workflow_runs WHERE workflow_id=$1")
                .bind(&key)
                .fetch_one(&db.store.pool)
                .await
                .unwrap();
        assert_eq!(queue, identifier);
        assert_eq!(bytes, submission);
        let (callable, bytes): (String, Vec<u8>) = sqlx::query_as(
            "SELECT callable,record_bytes FROM workflow_local_results WHERE step_key=$1",
        )
        .bind(&key)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert_eq!(callable, identifier);
        assert_eq!(bytes, record);
    }
    let (queue, bytes): (String, Vec<u8>) = sqlx::query_as(
        "SELECT queue,submission_bytes FROM workflow_runs WHERE workflow_id='wf_upgrade'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(queue, "python");
    assert_eq!(bytes, submission);
    let (callable, bytes): (String, Vec<u8>) = sqlx::query_as(
        "SELECT callable,record_bytes FROM workflow_local_results WHERE step_key='step'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(callable, "billing.old");
    assert_eq!(bytes, record);
    let revision:String=sqlx::query_scalar("SELECT trunc(creating_revision)::text FROM workflow_task_links WHERE workflow_id='wf_upgrade'").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(revision, "0");
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_attempt_inspection_distinguishes_application_and_runtime_failures() {
    use ledgence_worker_api::{Error, ErrorKind, ExecutionFailure, Phase, ProgramOutcome};
    for runtime_failure in [false, true] {
        let db = TestDb::new().await;
        let (_, _, assigned) = crate::tests::claimed(&db.store).await;
        db.store
            .renew(&RenewCommand {
                owner: assigned.lease.owner.clone(),
                sequence: 1,
                intent: RenewIntent::Dispatch,
            })
            .await
            .unwrap();
        let mut report = completed(&assigned, Quiescence::Confirmed, json!(null));
        let AttemptReport::Completed(completed) = &mut report.report else {
            panic!()
        };
        if runtime_failure {
            report.report = AttemptReport::Failed(ExecutionFailure {
                observations: None,
                context: completed.context.clone(),
                phase: Phase::Execution,
                error: Error::new(ErrorKind::Io, "runtime failed"),
                execution_may_have_started: true,
                cleanup_error: Some(Error::new(ErrorKind::Io, "cleanup failed")),
            });
        } else {
            completed.outcome = ProgramOutcome::Failure {
                kind: "invoice_failed".into(),
                message: "invoice rejected".into(),
            };
        }
        db.store.settle(&report).await.unwrap();
        let ConsoleQueryReply::Attempt(detail) = query(
            &db,
            ConsoleQuery::Attempt {
                attempt_id: assigned.lease.owner.attempt_id.clone(),
            },
        )
        .await
        else {
            panic!()
        };
        if runtime_failure {
            assert_eq!(detail.phase, Some(Phase::Execution));
            assert_eq!(detail.error.unwrap().message, "runtime failed");
            assert_eq!(detail.cleanup_error.unwrap().message, "cleanup failed");
            assert!(detail.application_error.is_none());
            assert!(detail.process_id.is_none());
        } else {
            assert_eq!(detail.application_error.unwrap().kind, "invoice_failed");
            assert!(detail.error.is_none());
            assert!(detail.cleanup_error.is_none());
            assert!(detail.phase.is_none());
            assert_eq!(detail.process_id, Some(42));
        }
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn console_activations_preserve_maximum_valid_rejection_errors() {
    let db = TestDb::new().await;
    // Custom workflow coordinators can reject work through the public store
    // contract. Its error bound is UTF-8 bytes, before JSON escaping.
    for (index, character) in ['\0', '\"', '\\'].into_iter().enumerate() {
        let (workflow, assigned) = workflow(&db, &format!("escaped-error-{index}")).await;
        db.store
            .settle(&completed(&assigned, Quiescence::Confirmed, Value::Null))
            .await
            .unwrap();
        let work = db.store.claim_work(16).await.unwrap();
        let work = work
            .iter()
            .find(|work| work.workflow_id == workflow.workflow_id)
            .expect("completed activation should produce work");
        let error = ApplicationError {
            kind: "\"".repeat(128),
            message: character.to_string().repeat(4096),
        };
        validate_workflow_error(&error).unwrap();
        assert!(codec::encode(&error).unwrap().len() > 8192);
        db.store.reject_work(work, &error).await.unwrap();
        let ConsoleQueryReply::Activations(activations) = query(
            &db,
            ConsoleQuery::Activations {
                workflow_id: workflow.workflow_id,
                page: pagination(),
            },
        )
        .await
        else {
            panic!("expected activation metadata");
        };
        assert_eq!(activations.items.len(), 1);
        assert_eq!(activations.items[0].error.as_ref(), Some(&error));
        assert!(activations.items[0].applied_at.is_some());
    }
    db.finish().await;
}
