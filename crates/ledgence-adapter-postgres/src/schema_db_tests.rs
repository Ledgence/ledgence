use crate::tests::TestDb;

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn schema_verification_is_read_only_and_rejects_incompatible_versions() {
    let db = TestDb::new().await;
    db.store.verify_schema().await.unwrap();
    db.store.check_connection().await.unwrap();
    let original: Vec<u8> = sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();

    sqlx::query("UPDATE _sqlx_migrations SET success = false")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    sqlx::query("UPDATE _sqlx_migrations SET success = true, checksum = ''::bytea")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1")
        .bind(original)
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.verify_schema().await.unwrap();

    sqlx::query("INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (9223372036854775807, 'future', true, ''::bytea, 0)")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    sqlx::query("DELETE FROM _sqlx_migrations")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0, "verification must not run pending migrations");
    sqlx::query("DROP TABLE _sqlx_migrations")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    db.finish().await;
}
