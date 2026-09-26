//! Real isolated PostgreSQL coverage of the persistent installation boundary.
use crate::{
    tests::{TestDb, acquire_command, command, descriptor, scope},
    *,
};
use serde_json::json;

fn config() -> SelfHostedInstanceConfig {
    SelfHostedInstanceConfig {
        instance_id: "local".into(),
        name: "Local instance".into(),
        scope: scope(),
        suggested_queues: vec!["python".into()],
        allowed_origins: vec![],
    }
}
fn foreign() -> Scope {
    Scope {
        tenant_id: "other".into(),
        namespace: "other".into(),
    }
}
fn foreign_command() -> SubmitCommand {
    let mut command = command();
    command.input.tenant_id = foreign().tenant_id;
    command.input.namespace = foreign().namespace;
    command
}
async fn bound(db: &TestDb) {
    db.store.initialize_instance(Some(&config())).await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_binding_is_persistent_and_omission_never_restores_legacy_mode() {
    let db = TestDb::new().await;
    assert!(db.store.initialize_instance(None).await.unwrap().is_none());
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let before = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    bound(&db).await;
    assert_eq!(
        db.store
            .inspect(&scope(), &task.task_id)
            .await
            .unwrap()
            .task_id,
        before.task_id
    );
    assert!(db.store.initialize_instance(None).await.is_err());
    let mut renamed = config();
    renamed.name = "Renamed instance".into();
    assert_eq!(
        db.store
            .initialize_instance(Some(&renamed))
            .await
            .unwrap()
            .unwrap()
            .name,
        renamed.name
    );
    let mut different = config();
    different.scope = foreign();
    assert!(
        db.store
            .initialize_instance(Some(&different))
            .await
            .is_err()
    );
    different = config();
    different.instance_id = "another".into();
    assert!(
        db.store
            .initialize_instance(Some(&different))
            .await
            .is_err()
    );
    let reconnect = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    assert!(reconnect.initialize_instance(None).await.is_err());
    assert_eq!(
        reconnect
            .open_session(&foreign(), "python", 1)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    reconnect.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_initialization_serializes_competing_first_bindings() {
    let db = TestDb::new().await;
    let first = config();
    let mut second = config();
    second.scope = foreign();
    second.instance_id = "other".into();
    let (a, b) = tokio::join!(
        db.store.initialize_instance(Some(&first)),
        db.store.initialize_instance(Some(&second))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM self_hosted_instance")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(db.store.initialize_instance(None).await.is_err());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_binding_rejects_configuration_only_and_expired_foreign_rows() {
    for statement in [
        "INSERT INTO dispatch_routes(tenant_id,namespace,queue) VALUES('other','other','q')",
        "INSERT INTO completion_destinations(tenant_id,namespace,destination,binding) VALUES('other','other','d','local')",
        "INSERT INTO retention_turn(tenant_id,namespace,lane) VALUES('other','other',0)",
        "INSERT INTO retention_scans(tenant_id,namespace,lane) VALUES('other','other','task')",
        "INSERT INTO worker_sessions(session_id,tenant_id,namespace,queue,concurrency,expires_at_ms) VALUES('foreign','other','other','q',1,1)",
    ] {
        let db = TestDb::new().await;
        sqlx::query(statement)
            .execute(&db.store.pool)
            .await
            .unwrap();
        assert!(
            db.store.initialize_instance(Some(&config())).await.is_err(),
            "{statement}"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM self_hosted_instance")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "rejected initialization must roll back its binding"
        );
        db.finish().await;
    }
    let db = TestDb::new().await;
    db.store
        .accept_resolved_submission(&foreign_command(), &descriptor())
        .await
        .unwrap();
    assert!(db.store.initialize_instance(Some(&config())).await.is_err());
    db.finish().await;
    let db = TestDb::new().await;
    db.store
        .accept_resolved_workflow(&foreign_command(), &descriptor())
        .await
        .unwrap();
    assert!(db.store.initialize_instance(Some(&config())).await.is_err());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_session_extension_checks_membership_before_changing_expiry() {
    let db = TestDb::new().await;
    bound(&db).await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    // Deliberately inject an incompatible stored row to prove transaction-side
    // defense independently from the startup audit and HTTP scope validation.
    sqlx::query(
        "UPDATE worker_sessions SET tenant_id='other',namespace='other' WHERE session_id=$1",
    )
    .bind(&session.id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(
        db.store.extend_session(&session.id).await.unwrap_err(),
        ContractError::UnknownSession
    );
    let at: i64 =
        sqlx::query_scalar("SELECT expires_at_ms FROM worker_sessions WHERE session_id=$1")
            .bind(&session.id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(at, session.expires_at as i64);
    assert_eq!(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap_err(),
        ContractError::UnknownSession
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM consumer_cursors")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_bound_store_rejects_foreign_reads_and_commands_without_writes() {
    let db = TestDb::new().await;
    bound(&db).await;
    let other = foreign();
    let command = foreign_command();
    assert_eq!(
        db.store
            .accept_resolved_submission(&command, &descriptor())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store.lookup_submission(&other, "key").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .open_session(&other, "python", 1)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .list_tasks(&other, &TaskListQuery::default())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store.cancel(&other, "task").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .accept_resolved_workflow(&command, &descriptor())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .cancel_workflow(&other, "workflow")
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .configure_route(&DispatchRoute {
                scope: other.clone(),
                queue: "q".into(),
                destination: "d".into()
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .configure_completion_destination(&CompletionDestination {
                scope: other.clone(),
                destination: "d".into(),
                binding: "local".into()
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store
            .retain_batch(
                &other,
                &RetentionPolicy::default(),
                Instant::now() + Duration::from_secs(5)
            )
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    let counts:(i64,i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM tasks),(SELECT count(*) FROM worker_sessions),(SELECT count(*) FROM workflow_runs),(SELECT count(*) FROM dispatch_routes),(SELECT count(*) FROM retention_turn)").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0, 0, 0, 0));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_dispatch_receipt_replays_after_expiry_and_foreign_scope_cannot_replay_it() {
    let db = TestDb::new().await;
    bound(&db).await;
    db.store
        .configure_route(&DispatchRoute {
            scope: scope(),
            queue: "python".into(),
            destination: "d".into(),
        })
        .await
        .unwrap();
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let command = ClaimCommand {
        acquisition: acquire_command(&session, 0, 1),
        dispatch: DispatchRef {
            scope: scope(),
            queue: "python".into(),
            task_id: task.task_id,
            generation: 1,
        },
    };
    let claimed = db.store.claim_dispatch(&command).await.unwrap();
    assert!(matches!(
        claimed.disposition,
        ClaimDisposition::Claimed { .. }
    ));
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=1 WHERE session_id=$1")
        .bind(&session.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let replay = db.store.claim_dispatch(&command).await.unwrap();
    assert!(matches!(
        replay.disposition,
        ClaimDisposition::Claimed {
            reply: AcquireReply::OwnershipLost { .. }
        }
    ));
    let mut changed = command.clone();
    changed.acquisition.scope = foreign();
    changed.dispatch.scope = foreign();
    assert_eq!(
        db.store.claim_dispatch(&changed).await.unwrap_err(),
        ContractError::NotFound
    );
    sqlx::query(
        "UPDATE worker_sessions SET tenant_id='other',namespace='other' WHERE session_id=$1",
    )
    .bind(&session.id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(
        db.store.claim_dispatch(&command).await.unwrap_err(),
        ContractError::UnknownSession
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_claim_receipts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_activation_local_receipt_keeps_exact_replay_semantics() {
    let db = TestDb::new().await;
    bound(&db).await;
    let workflow = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assignment = crate::tests::assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    assert_eq!(
        Some(assignment.lease.owner.task_id.clone()),
        workflow.activation_id
    );
    let command = LocalResultCommand {
        owner: assignment.lease.owner.clone(),
        record: LocalStepRecord {
            key: "local".into(),
            callable: "read".into(),
            input: json!(null),
            output: json!(null),
        },
    };
    let dispatched = RenewCommand {
        owner: command.owner.clone(),
        sequence: 1,
        intent: RenewIntent::Dispatch,
    };
    db.store.renew(&dispatched).await.unwrap();
    assert!(
        !db.store
            .record_local_result(&command)
            .await
            .unwrap()
            .already_accepted
    );
    sqlx::query("UPDATE attempts SET expires_at_ms=1 WHERE attempt_id=$1")
        .bind(&command.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(
        db.store
            .record_local_result(&command)
            .await
            .unwrap()
            .already_accepted
    );
    let mut other = command.clone();
    other.owner.scope = foreign();
    assert_eq!(
        db.store.record_local_result(&other).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        db.store.activation_context(&other.owner).await.unwrap_err(),
        ContractError::NotFound
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn instance_scope_audit_uses_indexed_extremes_and_detects_both_sides() {
    use sqlx::AssertSqlSafe;
    let db = TestDb::new().await;
    let mut connection = db.store.pool.acquire().await.unwrap();
    for table in [
        "tasks",
        "workflow_runs",
        "worker_sessions",
        "dispatch_routes",
        "completion_destinations",
        "completion_subscriptions",
        "retention_scans",
        "retention_turn",
        "console_programs",
        "console_program_versions",
    ] {
        sqlx::query(AssertSqlSafe(format!("CREATE TEMP TABLE {table}(tenant_id text COLLATE \"C\" NOT NULL,namespace text COLLATE \"C\" NOT NULL)")))
            .execute(&mut *connection).await.unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "CREATE INDEX {table}_fixture_scope ON {table}(tenant_id,namespace)"
        )))
        .execute(&mut *connection)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} SELECT 'acme','billing' FROM generate_series(1,100000)"
        )))
        .execute(&mut *connection)
        .await
        .unwrap();
        sqlx::query(AssertSqlSafe(format!("ANALYZE {table}")))
            .execute(&mut *connection)
            .await
            .unwrap();
    }
    let query = crate::instance::scope_audit_query();
    let other: bool = sqlx::query_scalar(AssertSqlSafe(query.clone()))
        .bind("acme")
        .bind("billing")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert!(!other);
    let plan = sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE,BUFFERS) {query}"
    )))
    .bind("acme")
    .bind("billing")
    .fetch_all(&mut *connection)
    .await
    .unwrap()
    .join("\n");
    println!("Instance indexed scope audit, 100k rows in each of ten tables:\n{plan}");
    assert!(!plan.contains("Seq Scan"));
    assert_eq!(plan.matches("Index Only Scan").count(), 20);
    for table in [
        "tasks",
        "workflow_runs",
        "worker_sessions",
        "dispatch_routes",
        "completion_destinations",
        "completion_subscriptions",
        "retention_scans",
        "retention_turn",
        "console_programs",
        "console_program_versions",
    ] {
        for (tenant, namespace) in [
            ("a", "billing"),
            ("z", "billing"),
            ("acme", "a"),
            ("acme", "z"),
        ] {
            sqlx::query(AssertSqlSafe(format!("INSERT INTO {table} VALUES($1,$2)")))
                .bind(tenant)
                .bind(namespace)
                .execute(&mut *connection)
                .await
                .unwrap();
            let other: bool = sqlx::query_scalar(AssertSqlSafe(query.clone()))
                .bind("acme")
                .bind("billing")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            assert!(other, "{table}:{tenant}/{namespace}");
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE tenant_id=$1 AND namespace=$2"
            )))
            .bind(tenant)
            .bind(namespace)
            .execute(&mut *connection)
            .await
            .unwrap();
        }
    }
    drop(connection);
    db.finish().await;
}
