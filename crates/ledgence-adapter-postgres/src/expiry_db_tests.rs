//! Recovery shortlist planning and bounded lifecycle progress on PostgreSQL.

use crate::{tests::*, *};
use sqlx::AssertSqlSafe;

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn expiry_shortlist_uses_an_index_range_when_no_active_task_is_due() {
    let db = TestDb::new().await;
    // This population exercises the real schema and query planner, without
    // decoding synthetic payloads. Every deadline is safely in the future.
    sqlx::query(
        "INSERT INTO tasks (task_id,run_id,tenant_id,namespace,queue,idempotency_key,\
         input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,attempt_count) \
         SELECT 'task_' || n,'run_' || n,'tenant','namespace','queue','key_' || n,\
         '{}','{}','queued',1,1,0 FROM generate_series(1,20000) n",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO attempts (attempt_id,task_id,generation,lease_id,worker_session_id,\
         consumer_id,event_source,event_id,event_bytes,expires_at_ms,deadline_ms,\
         authority_deadline_ms,state,execution_may_have_started,quiescence) \
         SELECT 'att_' || n,'task_' || n,1,'lease_' || n,'session',0,'urn:test','event_' || n,\
         '{}',253402300799000,253402300799000,253402300799000,'active',false,'unconfirmed' \
         FROM generate_series(1,20000) n",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE tasks SET state='active',current_attempt_id=replace(task_id,'task_','att_'),\
         attempt_count=1,next_expiry_ms=253402300799000",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("VACUUM ANALYZE tasks")
        .execute(&db.store.pool)
        .await
        .unwrap();

    // Explain exactly the production query; a copied predicate could leave a
    // broken production statement undetected. No planner settings are forced.
    let explain = format!(
        "EXPLAIN (ANALYZE, BUFFERS) {}",
        include_str!("../queries/expiry_candidates.sql")
    );
    let plan: Vec<String> = sqlx::query_scalar(AssertSqlSafe(explain))
        .bind(i64::from(MAX_RECOVERY_BATCH))
        .fetch_all(&db.store.pool)
        .await
        .unwrap();
    let plan = plan.join("\n");
    println!("Recovery shortlist plan with 20,000 future active tasks:\n{plan}");
    assert!(
        plan.lines()
            .any(|line| line.contains("Index Cond:") && line.contains("next_expiry_ms")),
        "expiry must be an index range boundary, not a row filter:\n{plan}"
    );
    assert!(
        !plan.contains("Rows Removed by Filter:"),
        "an empty shortlist must not visit and filter the active population:\n{plan}"
    );
    let progress = db.store.expire_batch(MAX_RECOVERY_BATCH).await.unwrap();
    assert_eq!(progress.examined, 0);
    assert_eq!(progress.expired, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn expiry_batch_respects_limit_and_leaves_future_attempts_active() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 4).await.unwrap();
    let mut assignments = Vec::new();
    for consumer in 0..4 {
        let mut command = command();
        command.idempotency_key = format!("expiry-limit-{consumer}");
        db.store
            .accept_resolved_submission(&command, &descriptor())
            .await
            .unwrap();
        assignments.push(assignment(
            db.store
                .acquire(&acquire_command(&session, consumer, 1))
                .await
                .unwrap(),
        ));
    }
    for assigned in &assignments[..3] {
        let mut tx = db.store.pool.begin().await.unwrap();
        let expiry: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint - 1",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        sqlx::query("UPDATE attempts SET expires_at_ms=$2 WHERE attempt_id=$1")
            .bind(&assigned.lease.owner.attempt_id)
            .bind(expiry)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE tasks SET next_expiry_ms=$2 WHERE task_id=$1")
            .bind(&assigned.lease.owner.task_id)
            .bind(expiry)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let first = db.store.expire_batch(2).await.unwrap();
    assert_eq!((first.examined, first.expired), (2, 2));
    let second = db.store.expire_batch(2).await.unwrap();
    assert_eq!((second.examined, second.expired), (1, 1));
    let empty = db.store.expire_batch(2).await.unwrap();
    assert_eq!((empty.examined, empty.expired), (0, 0));
    for (index, assigned) in assignments.iter().enumerate() {
        let owner = &assigned.lease.owner;
        let attempt = db
            .store
            .inspect_attempt(&scope(), &owner.task_id, &owner.attempt_id)
            .await
            .unwrap();
        assert_eq!(attempt.state == AttemptState::Active, index == 3);
    }
    db.finish().await;
}
