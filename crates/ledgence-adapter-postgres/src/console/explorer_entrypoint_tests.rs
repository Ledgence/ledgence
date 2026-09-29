//! Upgrade tests seed Console 2 bytes without invoking Console 3 writers.
use super::*;

const MIGRATION_NAME: &str = "20260929000000_console_entrypoints.sql";
const MIGRATION: &str = include_str!("../../migrations/20260929000000_console_entrypoints.sql");

async fn historical_database() -> TestDb {
    let db = TestDb::without_migrations().await;
    let directory = tempfile::tempdir().unwrap();
    for file in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).unwrap() {
        let file = file.unwrap();
        if file.file_name() != MIGRATION_NAME {
            std::fs::copy(file.path(), directory.path().join(file.file_name())).unwrap();
        }
    }
    sqlx::migrate::Migrator::new(directory.path())
        .await
        .unwrap()
        .run(&db.store.pool)
        .await
        .unwrap();
    db
}

async fn seed_root(db: &TestDb) {
    // These authority bytes include legal NUL, large integers, exponent spelling
    // and negative zero. The Console migration must never rewrite any of them.
    sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,revision,continuation,checkpoint_bytes,outcome_bytes,submitted_at_ms,terminal_at_ms,queue) VALUES('historical','acme','billing','historical',$1,$2,'succeeded',18446744073709551615,'review',$3,$4,1,50,'python')")
        .bind(codec::encode(&command()).unwrap())
        .bind(codec::encode(&descriptor()).unwrap())
        .bind(br#"{"nul":"\u0000","literal":"\\u0000","n":18446744073709551615,"f":1e+0,"z":-0.0}"#.as_slice())
        .bind(br#"{"kind":"succeeded","output":{"n":9007199254740993,"f":1.0,"z":-0.0}}"#.as_slice())
        .execute(&db.store.pool).await.unwrap();
}

async fn authority_bytes(db: &TestDb) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
    sqlx::query_as("SELECT submission_bytes,controller_bytes,checkpoint_bytes,outcome_bytes FROM workflow_runs WHERE workflow_id='historical'")
        .fetch_one(&db.store.pool).await.unwrap()
}

async fn insert_phase(db: &TestDb, revision: u64, bytes: &[u8]) {
    sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) VALUES('historical',($1::text)::ldg_u64,'phase','',$2,'review',$3)")
        .bind(revision.to_string()).bind(format!("activation_{revision}")).bind(bytes)
        .execute(&db.store.pool).await.unwrap();
}

fn old_metadata(message: &str) -> Vec<u8> {
    let current = ConsoleExplorerData::Entrypoint {
        state: None,
        availability: ConsoleEvidenceAvailability::Unavailable,
        submitted_at: 1,
        terminal_at: Some(50),
        applied_at: Some(2),
        decision_kind: None,
        error: Some(ApplicationError {
            kind: "rejected".into(),
            message: message.into(),
        }),
        resumed_activation_id: None,
    };
    String::from_utf8(codec::encode(&current).unwrap())
        .unwrap()
        .replacen("\"kind\":\"entrypoint\"", "\"kind\":\"phase\"", 1)
        .into_bytes()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn entrypoint_upgrade_preserves_retained_evidence_and_authority_bytes() {
    let db = historical_database().await;
    seed_root(&db).await;
    let message = "NUL:\0 literal:\\u0000 newline:\n Unicode:á東京 kind:\"phase\"";
    let canonical = old_metadata(message);
    let spaced = String::from_utf8(canonical.clone())
        .unwrap()
        .replacen("\"kind\":\"phase\"", "\"kind\" : \"phase\"", 1)
        .into_bytes();
    for (revision, bytes) in [(0, &canonical), (u64::MAX, &spaced)] {
        insert_phase(&db, revision, bytes).await;
    }
    let local = codec::encode(&ConsoleExplorerData::Local {
        key: "local:0".into(),
        callable: "program:tests".into(),
        accepted_at: Some(2),
        accepting_attempt_id: Some("attempt_old".into()),
        observation: None,
    })
    .unwrap();
    sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) VALUES('historical',0,'local','local:0','activation_0','review',$1)")
        .bind(&local).execute(&db.store.pool).await.unwrap();
    let before = authority_bytes(&db).await;
    let checksums: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&db.store.pool)
            .await
            .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    assert_eq!(authority_bytes(&db).await, before);
    let after_checksums: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version,checksum FROM _sqlx_migrations WHERE version<20260929000000 ORDER BY version",
    )
    .fetch_all(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(after_checksums, checksums);
    let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT kind,metadata_bytes FROM workflow_explorer_records ORDER BY revision,kind,record_key",
    )
    .fetch_all(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        (
            "entrypoint".into(),
            String::from_utf8(canonical)
                .unwrap()
                .replacen("\"phase\"", "\"entrypoint\"", 1)
                .into_bytes()
        )
    );
    assert_eq!(rows[1], ("local".into(), local));
    assert_eq!(
        rows[2],
        (
            "entrypoint".into(),
            String::from_utf8(spaced)
                .unwrap()
                .replacen("\"phase\"", "\"entrypoint\"", 1)
                .into_bytes()
        )
    );
    let nodes = all(&db, "historical", 1).await;
    assert_eq!(nodes.len(), 3);
    assert_eq!(
        serde_json::to_value(&nodes).unwrap(),
        serde_json::to_value(all(&db, "historical", 100).await).unwrap()
    );
    let entrypoints: Vec<_> = nodes
        .iter()
        .filter(|node| matches!(node.data, ConsoleExplorerData::Entrypoint { .. }))
        .collect();
    assert_eq!(
        entrypoints.len(),
        2,
        "reentry uses distinct activation identities"
    );
    assert_ne!(entrypoints[0].id, entrypoints[1].id);
    assert_eq!(entrypoints[1].revision, ConsoleU64(u64::MAX));
    for node in entrypoints {
        assert_eq!(node.entrypoint, "review");
        assert!(
            matches!(&node.data, ConsoleExplorerData::Entrypoint { error: Some(error), availability: ConsoleEvidenceAvailability::Unavailable, .. } if error.message == message)
        );
    }
    assert!(
        sqlx::query("UPDATE workflow_explorer_records SET kind='phase' WHERE kind='entrypoint'")
            .execute(&db.store.pool)
            .await
            .is_err()
    );
    let policy = RetentionPolicy {
        batch_size: 1,
        ..Default::default()
    };
    let mut remaining = 3_i64;
    // Retention alternates task/workflow/session lanes, discovery/collection,
    // and cursor wraparound; allow those empty turns while bounding each delete.
    for _ in 0..128 {
        db.store
            .retain_batch(
                &scope(),
                &policy,
                std::time::Instant::now() + std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
        let after: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM workflow_explorer_records WHERE workflow_id='historical'",
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert!(
            remaining - after <= 1,
            "projection cleanup obeys the row budget"
        );
        remaining = after;
        let root_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id='historical')",
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        if !root_exists {
            break;
        }
    }
    assert_eq!(remaining, 0);
    let roots: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_runs")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        roots, 0,
        "the upgraded projection must not prevent parent retention"
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn entrypoint_conversion_preserves_every_non_discriminator_byte() {
    let db = historical_database().await;
    // Keep the exact migration helper alive only in this rolled-back transaction
    // to exercise arbitrary JSON number tokens outside the typed graph schema.
    let helper_drop = "DROP FUNCTION pg_temp.ldg_console_entrypoint_metadata(bytea);";
    assert_eq!(MIGRATION.matches(helper_drop).count(), 1);
    let mut tx = db.store.pool.begin().await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(MIGRATION.replace(helper_drop, "")))
        .execute(&mut *tx)
        .await
        .unwrap();
    let original = br#" 
{ "error":{"kind":"phase","message":"\u0000 \\u0000 \\\u0000 \n \"kind\":\"phase\""}, "numbers":[18446744073709551615,9007199254740993,1.0,1e+12,-0.0,-2.5E-3], "kind" : "phase" }
"#;
    let converted: Vec<u8> =
        sqlx::query_scalar("SELECT pg_temp.ldg_console_entrypoint_metadata($1)")
            .bind(original.as_slice())
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        converted,
        String::from_utf8(original.to_vec())
            .unwrap()
            .replacen("\"kind\" : \"phase\"", "\"kind\" : \"entrypoint\"", 1)
            .into_bytes()
    );
    tx.rollback().await.unwrap();
    db.finish().await;
}

async fn assert_migration_rollback_and_retry(unexpected: &[u8]) {
    let db = historical_database().await;
    seed_root(&db).await;
    let valid = old_metadata("preserve me");
    insert_phase(&db, 0, &valid).await;
    insert_phase(&db, 1, unexpected).await;
    let before: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT kind,metadata_bytes FROM workflow_explorer_records ORDER BY revision",
    )
    .fetch_all(&db.store.pool)
    .await
    .unwrap();
    assert!(db.store.migrate().await.is_err());
    let after: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT kind,metadata_bytes FROM workflow_explorer_records ORDER BY revision",
    )
    .fetch_all(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "no partial tag or metadata conversion survives"
    );
    let installed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=20260929000000)",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert!(!installed);
    assert!(
        sqlx::query("UPDATE workflow_explorer_records SET kind='entrypoint'")
            .execute(&db.store.pool)
            .await
            .is_err(),
        "the old kind constraint must also roll back"
    );
    sqlx::query("UPDATE workflow_explorer_records SET metadata_bytes=$1 WHERE revision=1")
        .bind(&valid)
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    assert_eq!(all(&db, "historical", 1).await.len(), 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn entrypoint_migration_rolls_back_unexpected_encoding_and_can_retry() {
    assert_migration_rollback_and_retry(br#"{"kind":"entrypoint","error":null}"#).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn entrypoint_migration_rejects_duplicate_top_level_discriminators_atomically() {
    assert_migration_rollback_and_retry(br#"{"kind":"phase","error":null,"kind":"phase"}"#).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn entrypoint_migration_rejects_missing_top_level_discriminator_atomically() {
    // A nested kind is unrelated metadata and cannot stand in for the absent
    // top-level discriminator, even when its string value is exactly phase.
    assert_migration_rollback_and_retry(
        br#"{"error":{"kind":"phase"},"availability":"available"}"#,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn entrypoint_explorer_rejects_every_previous_console_cursor_kind() {
    let db = TestDb::new().await;
    let (root, _) = start(&db, "cursor-upgrade").await;
    let request = ConsoleQuery::Explorer {
        workflow_id: root.workflow_id.clone(),
        page: ConsolePagination::default(),
    };
    let mut old_binding = request.binding(&scope()).unwrap();
    assert_eq!(old_binding.endpoint, "workflows/explorer/v4");
    for endpoint in ["workflows/explorer", "workflows/explorer/v3"] {
        old_binding.endpoint = endpoint;
        for kind in [
            "entrypoint",
            "phase",
            "child",
            "fork",
            "local",
            "child_wait",
            "external_wait",
        ] {
            let position = vec![
                ConsoleKey::Number(ConsoleU64(0)),
                ConsoleKey::Text(kind.into()),
                ConsoleKey::Text(if matches!(kind, "phase" | "child_wait") {
                    kind.into()
                } else {
                    "key:0".into()
                }),
            ];
            let cursor = ConsolePagination::default()
                .next_cursor(&old_binding, &position)
                .unwrap();
            let query = ConsoleQuery::Explorer {
                workflow_id: root.workflow_id.clone(),
                page: ConsolePagination {
                    limit: 1,
                    cursor: Some(cursor),
                },
            };
            assert!(
                matches!(
                    db.store.query_console(&scope(), &query).await,
                    Err(ContractError::InvalidInput(_))
                ),
                "{endpoint} {kind} cursor must not survive the new relation contract"
            );
        }
    }
    assert_eq!(all(&db, &root.workflow_id, 1).await.len(), 1);
    db.finish().await;
}
