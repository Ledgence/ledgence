//! Destructive recovery test restricted to an explicitly named disposable container.
use super::*;
use crate::tests::{
    TestDb, acquire_command, assignment, claimed, command, completed, descriptor, scope,
};
use serde_json::json;

async fn docker(arguments: &[&str]) {
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("docker")
            .args(arguments)
            .output(),
    )
    .await
    .expect("Docker test operation timed out")
    .expect("Docker is required for crash recovery testing");
    assert!(
        output.status.success(),
        "Docker test operation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 and its disposable LEDGENCE_POSTGRES_CONTAINER"]
async fn database_crash_preserves_commits_and_discards_uncommitted_writes() {
    let container = std::env::var("LEDGENCE_POSTGRES_CONTAINER")
        .expect("set LEDGENCE_POSTGRES_CONTAINER to the disposable PostgreSQL test container");
    assert!(
        !container.is_empty()
            && container
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    );
    let mut db = TestDb::new().await;
    let (accepted_task, session, first) = claimed(&db.store).await;
    let report = completed(
        &first,
        Quiescence::Unconfirmed,
        json!({"answer":9007199254740993u64,"zero":-0.0,"text":"before\u{0}after"}),
    );
    let receipt = db.store.settle(&report).await.unwrap().receipt;
    let mut second = command();
    second.idempotency_key = "live-during-restart".into();
    let live_task = db
        .store
        .accept_resolved_submission(&second, &descriptor())
        .await
        .unwrap();
    let live_command = acquire_command(&session, 1, 1);
    let live = assignment(db.store.acquire(&live_command).await.unwrap());
    let mut third = command();
    third.idempotency_key = "queued-during-restart".into();
    let queued = db
        .store
        .accept_resolved_submission(&third, &descriptor())
        .await
        .unwrap();
    let mut uncommitted = db.store.pool.begin().await.unwrap();
    sqlx::query("UPDATE tasks SET available_at_ms=available_at_ms+10000 WHERE task_id=$1")
        .bind(&queued.task_id)
        .execute(&mut *uncommitted)
        .await
        .unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()::text")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert!(database.starts_with("ldg_test_"));
    // Prove the explicitly named container owns this unique fixture database
    // before stopping it; a wrong container fails without receiving a signal.
    docker(&[
        "exec", &container, "psql", "-U", "postgres", "-d", &database, "-tAc", "SELECT 1",
    ])
    .await;
    docker(&["kill", "--signal", "KILL", &container]).await;
    docker(&["start", &container]).await;
    drop(uncommitted);
    db.store.close().await;
    db.store = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(store) = PostgresStore::connect(&db.url, PostgresOptions::default()).await {
                break store;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("PostgreSQL did not recover");
    db.store.migrate().await.unwrap();
    let replay = db.store.settle(&report).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.receipt.accepted_at, receipt.accepted_at);
    assert_eq!(replay.task_state, TaskState::Active);
    let live_replay = assignment(db.store.acquire(&live_command).await.unwrap());
    assert_eq!(live_replay.event.id(), live.event.id());
    assert_eq!(live_replay.lease.owner, live.lease.owner);
    assert_eq!(
        db.store
            .inspect(&scope(), &live_task.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Active
    );
    let after = db.store.inspect(&scope(), &queued.task_id).await.unwrap();
    assert_eq!(after.state, TaskState::Queued);
    assert_eq!(after.available_at, queued.available_at);
    assert_eq!(
        db.store
            .history(&scope(), &queued.task_id, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.store
            .confirm_quiescence(&first.lease.owner)
            .await
            .unwrap(),
        TaskState::Succeeded
    );
    assert_eq!(
        db.store
            .inspect(&scope(), &accepted_task.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Succeeded
    );
    db.finish().await;
}
