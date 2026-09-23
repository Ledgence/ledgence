//! Deployment budgets use real PostgreSQL waits and transactional migrations.

use crate::{tests::TestDb, *};
use sqlx::{
    SqlSafeStr,
    migrate::{Migrate, Migration, MigrationType, Migrator},
};

const PROBE_VERSION: i64 = 20260914000000;

fn migration(version: i64, sql: &'static str) -> Migration {
    Migration::new(
        version,
        "budget probe".into(),
        MigrationType::Simple,
        sql.into_sql_str(),
        false,
    )
}

fn probe(sql: &'static str) -> Migrator {
    let mut migrations: Vec<_> = MIGRATOR.iter().cloned().collect();
    migrations.push(migration(PROBE_VERSION, sql));
    Migrator::with_migrations(migrations)
}

fn budget(milliseconds: u64) -> MigrationOptions {
    MigrationOptions {
        timeout: Duration::from_millis(milliseconds),
    }
}

async fn short_request_store(db: &TestDb) -> PostgresStore {
    PostgresStore::connect(
        &db.url,
        PostgresOptions {
            max_connections: 1,
            acquire_timeout: Duration::from_secs(2),
            statement_timeout: Duration::from_millis(100),
            operation_timeout: Duration::from_millis(200),
        },
    )
    .await
    .unwrap()
}

async fn runtime_limits_unchanged(store: &PostgresStore) {
    let limits: (String, String) = sqlx::query_as(
        "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    assert_eq!(limits, ("100ms".into(), "100ms".into()));
    let error = sqlx::query("SELECT pg_sleep(0.3)")
        .execute(&store.pool)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("57014"),
        "ordinary requests must retain the short statement limit"
    );
}

async fn assert_rolled_back_and_unlocked(db: &TestDb, table: &str) {
    // Closing a cancelled connection is asynchronous. Observe eventual cleanup
    // rather than claim that the server has rolled back when the caller returns.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let locks: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database())",
            )
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
            if locks == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("migration session retained an advisory lock");
    let relation: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(table)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert!(relation.is_none(), "incomplete migration persisted {table}");
    let applied: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=$1)")
            .bind(PROBE_VERSION)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(!applied, "incomplete migration was recorded as applied");
}

async fn wait_for_query(db: &TestDb, marker: &str, expected_wait: Option<&str>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active' AND query LIKE $1 AND ($2::text IS NULL OR wait_event=$2))",
            )
            .bind(format!("%{marker}%"))
            .bind(expected_wait)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
            if active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("migration never reached its expected database wait");
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn migration_budget_exceeds_request_limits_without_changing_them() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let migrator =
        probe("SELECT pg_sleep(0.3); CREATE TABLE migration_budget_success(id integer);");
    let started = Instant::now();
    store
        .migrate_with_migrator(MigrationOptions::default(), &migrator)
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(300));
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('migration_budget_success') IS NOT NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(exists);
    runtime_limits_unchanged(&store).await;
    // A normal availability check still has its 200 ms operation budget, even
    // when its pool could wait two seconds for the only checked-out connection.
    let held = store.pool.acquire().await.unwrap();
    let started = Instant::now();
    assert!(matches!(
        store.check_connection().await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(held);
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn migration_timeout_rolls_back_and_can_retry_with_a_larger_budget() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let migrator = probe("CREATE TABLE migration_timeout_probe(id integer); SELECT pg_sleep(0.3);");
    assert!(matches!(
        store.migrate_with_migrator(budget(150), &migrator).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_rolled_back_and_unlocked(&db, "migration_timeout_probe").await;
    runtime_limits_unchanged(&store).await;
    store
        .migrate_with_migrator(budget(2_000), &migrator)
        .await
        .unwrap();
    runtime_limits_unchanged(&store).await;
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn failed_migration_rolls_back_and_cannot_leak_its_extended_session_limits() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let migrator = probe("CREATE TABLE migration_failure_probe(id integer); SELECT 1/0;");
    assert!(matches!(
        store.migrate_with_migrator(budget(2_000), &migrator).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_rolled_back_and_unlocked(&db, "migration_failure_probe").await;
    runtime_limits_unchanged(&store).await;
    store.migrate().await.unwrap();
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancelled_migration_rolls_back_and_releases_its_lock_and_pool_capacity() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let running_store = store.clone();
    let running = tokio::spawn(async move {
        let migrator =
            probe("CREATE TABLE migration_cancel_probe(id integer); SELECT pg_sleep(0.5);");
        running_store
            .migrate_with_migrator(budget(2_000), &migrator)
            .await
    });
    wait_for_query(&db, "migration_cancel_probe", Some("PgSleep")).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    assert_rolled_back_and_unlocked(&db, "migration_cancel_probe").await;
    runtime_limits_unchanged(&store).await;
    store.migrate().await.unwrap();
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn migration_lock_wait_uses_deployment_budget_and_retains_sqlx_serialization() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let mut blocker = db.store.pool.acquire().await.unwrap();
    blocker.close_on_drop();
    blocker.lock().await.unwrap();
    let running_store = store.clone();
    let running = tokio::spawn(async move {
        let migrator = probe("CREATE TABLE migration_lock_probe(id integer);");
        running_store
            .migrate_with_migrator(budget(2_000), &migrator)
            .await
    });
    wait_for_query(&db, "SELECT pg_advisory_lock", None).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !running.is_finished(),
        "migration did not honor its advisory lock"
    );
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('migration_lock_probe') IS NOT NULL")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert!(
        !exists,
        "migration ran while another session held the SQLx lock"
    );
    blocker.unlock().await.unwrap();
    blocker.close().await.unwrap();
    running.await.unwrap().unwrap();
    runtime_limits_unchanged(&store).await;
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn migration_budget_bounds_lock_wait_and_does_not_poison_retry() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let mut blocker = db.store.pool.acquire().await.unwrap();
    blocker.close_on_drop();
    blocker.lock().await.unwrap();
    let migrator = probe("CREATE TABLE migration_lock_timeout_probe(id integer);");
    let started = Instant::now();
    assert!(matches!(
        store.migrate_with_migrator(budget(150), &migrator).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    blocker.unlock().await.unwrap();
    blocker.close().await.unwrap();
    assert_rolled_back_and_unlocked(&db, "migration_lock_timeout_probe").await;
    runtime_limits_unchanged(&store).await;
    store
        .migrate_with_migrator(budget(2_000), &migrator)
        .await
        .unwrap();
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn migration_budget_includes_pool_acquisition_and_rejects_invalid_limits_first() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let held = store.pool.acquire().await.unwrap();
    let started = Instant::now();
    assert!(matches!(
        store.migrate_with_options(budget(100)).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
    drop(held);
    store.migrate_with_options(budget(2_000)).await.unwrap();
    runtime_limits_unchanged(&store).await;
    store.close().await;
    assert!(matches!(
        store.migrate_with_options(budget(0)).await,
        Err(ContractError::InvalidInput(_))
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn total_budget_preserves_prior_migration_but_rolls_back_the_incomplete_one() {
    let db = TestDb::new().await;
    let store = short_request_store(&db).await;
    let mut migrations: Vec<_> = MIGRATOR.iter().cloned().collect();
    migrations.push(migration(
        PROBE_VERSION - 1,
        "CREATE TABLE migration_first_probe(id integer); SELECT pg_sleep(0.2);",
    ));
    migrations.push(migration(
        PROBE_VERSION,
        "CREATE TABLE migration_second_probe(id integer); SELECT pg_sleep(0.3);",
    ));
    let migrator = Migrator::with_migrations(migrations);
    assert!(matches!(
        store.migrate_with_migrator(budget(400), &migrator).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_rolled_back_and_unlocked(&db, "migration_second_probe").await;
    let first_applied: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=$1)")
            .bind(PROBE_VERSION - 1)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(
        first_applied,
        "a completed earlier migration must remain committed"
    );
    let first_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('migration_first_probe') IS NOT NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(first_exists);
    runtime_limits_unchanged(&store).await;
    store
        .migrate_with_migrator(budget(2_000), &migrator)
        .await
        .unwrap();
    store.close().await;
    db.finish().await;
}
