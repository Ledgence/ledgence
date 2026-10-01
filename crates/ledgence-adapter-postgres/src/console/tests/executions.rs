use super::*;

async fn executions(
    db: &TestDb,
    filters: ConsoleExecutionFilters,
    page: ConsolePagination,
) -> ConsolePage<ConsoleExecutionSummary> {
    let ConsoleQueryReply::Executions(value) =
        query(db, ConsoleQuery::Executions { filters, page }).await
    else {
        panic!("executions")
    };
    value
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn execution_discovery_typed_ties_traverse_once_and_bind_filters() {
    let db = TestDb::new().await;
    let mut expected = Vec::new();
    for key in ["a", "b", "c"] {
        let mut submit = command();
        submit.idempotency_key = key.into();
        submit.input.correlation_key = Some("shared".into());
        let t = db
            .store
            .accept_resolved_submission(&submit, &descriptor())
            .await
            .unwrap();
        let w = db
            .store
            .accept_resolved_workflow(&submit, &descriptor())
            .await
            .unwrap();
        expected.push(("task".to_owned(), t.task_id));
        expected.push(("workflow".to_owned(), w.workflow_id));
    }
    sqlx::query("UPDATE tasks SET submitted_at_ms=10,input_bytes=decode('00','hex')")
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=10,submission_bytes=decode('00','hex')")
        .execute(&db.store.pool)
        .await
        .unwrap();
    expected.sort_by(|a, b| b.cmp(a));
    let filters = ConsoleExecutionFilters {
        correlation_key: Some("shared".into()),
        ..Default::default()
    };
    let mut page = pagination();
    let mut actual = Vec::new();
    let mut first_cursor = None;
    loop {
        let result = executions(&db, filters.clone(), page.clone()).await;
        actual.extend(
            result
                .items
                .iter()
                .map(|v| (v.kind.as_str().to_owned(), v.id.clone())),
        );
        if first_cursor.is_none() {
            first_cursor = result.next_cursor.clone();
        }
        page.cursor = result.next_cursor;
        if page.cursor.is_none() {
            break;
        }
    }
    assert_eq!(actual, expected);
    // Payload bytes are deliberately unreadable; metadata and pinned identity
    // survive without any catalog registration or application input decoding.
    assert_eq!(actual.len(), 6);
    let changed = ConsoleQuery::Executions {
        filters: ConsoleExecutionFilters {
            include_children: true,
            ..filters.clone()
        },
        page: ConsolePagination {
            limit: 1,
            cursor: first_cursor,
        },
    };
    assert!(matches!(
        db.store.query_console(&scope(), &changed).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert!(
        executions(
            &db,
            ConsoleExecutionFilters {
                submitted_from: Some(10),
                submitted_until: Some(11),
                ..filters.clone()
            },
            ConsolePagination::default()
        )
        .await
        .items
        .len()
            == 6
    );
    assert!(
        executions(
            &db,
            ConsoleExecutionFilters {
                submitted_until: Some(10),
                ..filters
            },
            ConsolePagination::default()
        )
        .await
        .items
        .is_empty()
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn execution_discovery_root_scope_program_and_exact_ids_expose_owned_children() {
    let db = TestDb::new().await;
    let (root, assigned) = workflow(&db, "root").await;
    let mut child_descriptor = descriptor();
    child_descriptor.program.id = "child-only".into();
    let commands = [WorkflowChildKind::Task, WorkflowChildKind::Workflow].map(|kind| {
        let mut c = child(
            if kind == WorkflowChildKind::Task {
                "task"
            } else {
                "flow"
            },
            kind,
        );
        c.program = child_descriptor.program.clone();
        c
    });
    let resolved = commands
        .iter()
        .map(|c| ResolvedWorkflowChild {
            kind: c.kind,
            key: c.key.clone(),
            descriptor: child_descriptor.clone(),
        })
        .collect::<Vec<_>>();
    apply(
        &db,
        &assigned,
        WorkflowAction::Suspend {
            state: json!({}),
            continuation: "join".into(),
            commands: commands.to_vec(),
            until: vec!["task".into(), "flow".into()],
        },
        &resolved,
    )
    .await;
    let roots = executions(&db, Default::default(), Default::default()).await;
    assert_eq!(roots.items.len(), 1);
    assert_eq!(roots.items[0].id, root.workflow_id);
    let all = executions(
        &db,
        ConsoleExecutionFilters {
            include_children: true,
            ..Default::default()
        },
        Default::default(),
    )
    .await;
    assert_eq!(all.items.len(), 3);
    assert!(
        all.items
            .iter()
            .all(|v| v.id != assigned.lease.owner.task_id)
    );
    let children = executions(
        &db,
        ConsoleExecutionFilters {
            program_id: Some("child-only".into()),
            version: Some(child_descriptor.program.version),
            ..Default::default()
        },
        Default::default(),
    )
    .await;
    assert_eq!(children.items.len(), 2);
    for c in children.items {
        assert_eq!(c.parent_workflow_id, Some(root.workflow_id.clone()));
        assert_eq!(c.root_workflow_id, Some(root.workflow_id.clone()));
        let exact = executions(
            &db,
            ConsoleExecutionFilters {
                execution_id: Some(c.id.clone()),
                kind: Some(c.kind),
                ..Default::default()
            },
            Default::default(),
        )
        .await;
        assert_eq!(exact.items.len(), 1);
        assert_eq!(exact.items[0].id, c.id);
    }
    assert!(
        executions(
            &db,
            ConsoleExecutionFilters {
                execution_id: Some(assigned.lease.owner.task_id),
                ..Default::default()
            },
            Default::default()
        )
        .await
        .items
        .is_empty()
    );
    let children = executions(
        &db,
        ConsoleExecutionFilters {
            queue: Some("children".into()),
            kind: Some(ConsoleExecutionKind::Task),
            include_children: true,
            state: Some(ConsoleExecutionState::Queued),
            ..Default::default()
        },
        Default::default(),
    )
    .await;
    assert_eq!(children.items.len(), 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn execution_discovery_preserves_scope_retention_and_same_raw_identity() {
    let db = TestDb::new().await;
    let w = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    // Task and workflow namespaces are distinct. Exercise the valid identical-ID
    // case directly to prove that the union/cursor never conflates their rows.
    let mut submit = command();
    submit.idempotency_key = "standalone".into();
    let task = db
        .store
        .accept_resolved_submission(&submit, &descriptor())
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,attempt_count) SELECT $1,'run_collision',tenant_id,namespace,queue,'collision',input_bytes,descriptor_bytes,'queued',10,10,0 FROM tasks WHERE task_id=$2").bind(&w.workflow_id).bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=10 WHERE workflow_id=$1")
        .bind(&w.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let filters = ConsoleExecutionFilters {
        execution_id: Some(w.workflow_id.clone()),
        ..Default::default()
    };
    let first = executions(&db, filters.clone(), pagination()).await;
    assert_eq!(first.items[0].kind, ConsoleExecutionKind::Workflow);
    let second = executions(
        &db,
        filters.clone(),
        ConsolePagination {
            cursor: first.next_cursor,
            limit: 1,
        },
    )
    .await;
    assert_eq!(second.items[0].kind, ConsoleExecutionKind::Task);
    assert_eq!(second.items[0].id, w.workflow_id);
    assert!(second.next_cursor.is_none());
    sqlx::query(
        "UPDATE tasks SET state='cancelled',terminal_at_ms=20,retiring_at_ms=20 WHERE task_id=$1",
    )
    .bind(&w.workflow_id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(
        executions(&db, filters.clone(), Default::default())
            .await
            .items
            .len(),
        1
    );
    let other = Scope {
        tenant_id: "other".into(),
        namespace: scope().namespace,
    };
    let ConsoleQueryReply::Executions(result) = db
        .store
        .query_console(
            &other,
            &ConsoleQuery::Executions {
                filters,
                page: Default::default(),
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(result.items.is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn execution_discovery_upgrade_backfills_identity_without_rewriting_user_bytes() {
    let db = TestDb::without_migrations().await;
    let previous = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20260928010000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    let mut submit = command();
    submit.input.data =
        json!({"program": {"id": "spoof"}, "nul": "\u{0}", "integer": 9007199254740993_u64});
    let payload = codec::encode(&submit).unwrap();
    let task_payload = codec::encode(&submit.input).unwrap();
    let descriptor = codec::encode(&descriptor()).unwrap();
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,attempt_count) VALUES('upgrade_task','upgrade_run','acme','billing','python','upgrade_task',$1,$2,'queued',10,10,0)")
        .bind(&task_payload).bind(&descriptor).execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,queue,idempotency_key,submission_bytes,controller_bytes,state,continuation,checkpoint_bytes,submitted_at_ms) VALUES('upgrade_workflow','acme','billing','python','upgrade_workflow',$1,$2,'running','start',$3,10)")
        .bind(&payload).bind(&descriptor).bind(b"null".as_slice()).execute(&db.store.pool).await.unwrap();
    let metadata = codec::encode(&ProgramDisplayMetadata {
        kind: ConsoleProgramKind::Task,
        ..Default::default()
    })
    .unwrap();
    sqlx::query("INSERT INTO console_programs(tenant_id,namespace,program_id,metadata_bytes,registered_versions,last_registered_at_ms) VALUES('acme','billing','mixed',$1,2,10)")
        .bind(&metadata).execute(&db.store.pool).await.unwrap();
    for (version, kind) in [
        ("1", ConsoleProgramKind::Task),
        ("2", ConsoleProgramKind::Workflow),
    ] {
        let mut registered = crate::tests::descriptor();
        registered.program = ledgence_worker_api::ProgramRef {
            id: "mixed".into(),
            version: version.into(),
        };
        let manifest = ledgence_worker_api::ProgramManifest {
            schema_version: 1,
            program: registered.program.clone(),
            runtime: ledgence_worker_api::PythonRuntime {
                kind: "python".into(),
                python: "3.12".into(),
                protocol: 1,
            },
            handler: "app:handle".into(),
            platform: ledgence_worker_api::Platform {
                os: "linux".into(),
                arch: "x86_64".into(),
            },
        };
        let metadata = codec::encode(&ProgramDisplayMetadata {
            kind,
            ..Default::default()
        })
        .unwrap();
        sqlx::query("INSERT INTO console_program_versions(tenant_id,namespace,program_id,version,descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms) VALUES('acme','billing','mixed',$1,$2,$3,$4,10)")
            .bind(version).bind(codec::encode(&registered).unwrap()).bind(codec::encode(&manifest).unwrap()).bind(&metadata).execute(&db.store.pool).await.unwrap();
    }
    let current = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version <= 20260928010000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &current)
        .await
        .unwrap();
    let items = executions(&db, Default::default(), Default::default())
        .await
        .items;
    assert_eq!(items.len(), 2);
    assert!(
        items
            .iter()
            .all(|item| item.descriptor.program == submit.input.program)
    );
    for (table, id, payload_column, descriptor_column) in [
        ("tasks", "upgrade_task", "input_bytes", "descriptor_bytes"),
        (
            "workflow_runs",
            "upgrade_workflow",
            "submission_bytes",
            "controller_bytes",
        ),
    ] {
        let id_column = if table == "tasks" {
            "task_id"
        } else {
            "workflow_id"
        };
        let sql = AssertSqlSafe(format!(
            "SELECT {payload_column},{descriptor_column} FROM {table} WHERE {id_column}=$1"
        ));
        let bytes: (Vec<u8>, Vec<u8>) = sqlx::query_as(sql)
            .bind(id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        let expected_payload = if table == "tasks" {
            &task_payload
        } else {
            &payload
        };
        assert_eq!(bytes, (expected_payload.clone(), descriptor.clone()));
    }
    let ProgramCatalogReply::Catalog(catalog) = db
        .store
        .query_programs(
            &scope(),
            &ProgramCatalogQuery::Catalog {
                kind: Some(ConsoleProgramKind::Workflow),
                page: Default::default(),
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(catalog.items.len(), 1);
    assert_eq!(
        catalog.items[0].kinds,
        vec![ConsoleProgramKind::Task, ConsoleProgramKind::Workflow]
    );
    assert_eq!(
        catalog.items[0].program.metadata.kind,
        ConsoleProgramKind::Task
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn execution_discovery_deep_seek_limits_each_kind_before_descriptor_hydration() {
    let db = TestDb::new().await;
    let mut connection = db.store.pool.acquire().await.unwrap();
    // Temporary metadata tables isolate access-path qualification from payload
    // size and workflow execution. 100,000 rows include equal timestamp ties.
    for sql in [
        "CREATE TEMP TABLE tasks(task_id text COLLATE \"C\" PRIMARY KEY,tenant_id text COLLATE \"C\",namespace text COLLATE \"C\",queue text,state text,submitted_at_ms bigint,terminal_at_ms bigint,correlation_key text,workflow_id text,root_workflow_id text,workflow_activation_id text,retiring_at_ms bigint,descriptor_bytes bytea,program_id text,program_version text)",
        "CREATE TEMP TABLE workflow_runs(workflow_id text COLLATE \"C\" PRIMARY KEY,tenant_id text COLLATE \"C\",namespace text COLLATE \"C\",queue text,state text,submitted_at_ms bigint,terminal_at_ms bigint,correlation_key text,parent_workflow_id text,root_workflow_id text,retiring_at_ms bigint,controller_bytes bytea,program_id text,program_version text)",
        "CREATE INDEX fixture_execution_tasks ON tasks(tenant_id,namespace,submitted_at_ms DESC,task_id DESC) WHERE retiring_at_ms IS NULL AND workflow_activation_id IS NULL AND workflow_id IS NULL",
        "CREATE INDEX fixture_execution_workflows ON workflow_runs(tenant_id,namespace,submitted_at_ms DESC,workflow_id DESC) WHERE retiring_at_ms IS NULL AND parent_workflow_id IS NULL",
        "INSERT INTO tasks(task_id,tenant_id,namespace,queue,state,submitted_at_ms,descriptor_bytes) SELECT 'task_'||lpad(n::text,8,'0'),'acme','billing','python','queued',n/10,decode('00','hex') FROM generate_series(1,50000)n",
        "INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,queue,state,submitted_at_ms,controller_bytes) SELECT 'workflow_'||lpad(n::text,8,'0'),'acme','billing','python','running',n/10,decode('00','hex') FROM generate_series(1,50000)n",
        "ANALYZE tasks",
        "ANALYZE workflow_runs",
    ] {
        sqlx::query(sql).execute(&mut *connection).await.unwrap();
    }
    for kind in ["task", "workflow"] {
        let position = vec![
            ConsoleKey::Number(ConsoleU64(100)),
            ConsoleKey::Text(kind.into()),
            ConsoleKey::Text(format!("{kind}_00001000")),
        ];
        let mut builder = super::super::executions::discovery_query(
            &scope(),
            &Default::default(),
            50,
            Some(&position),
        )
        .unwrap();
        let mut query = builder.build();
        let args = query.take_arguments().unwrap().unwrap();
        let sql = AssertSqlSafe(format!(
            "EXPLAIN (ANALYZE,BUFFERS) {}",
            query.sql().as_str()
        ));
        let plan = sqlx::query_scalar_with::<_, String, _>(sql, args)
            .fetch_all(&mut *connection)
            .await
            .unwrap()
            .join("\n");
        println!("Unified execution deep {kind} seek:\n{plan}");
        assert!(plan.contains("fixture_execution_tasks"));
        assert!(plan.contains("fixture_execution_workflows"));
        assert!(!plan.contains("Seq Scan"), "{plan}");
        assert!(!plan.contains("Rows Removed by Filter: 49000"), "{plan}");
    }
    drop(connection);
    db.finish().await;
}
