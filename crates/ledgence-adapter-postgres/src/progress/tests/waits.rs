//! Real SQL/HTTP qualification of observer coalescing and bounded ownership.
use super::*;
use ledgence_orchestration_service::ProgressStatistics;
use tokio::time::Instant;

async fn statistics_until(server: &ProgressServer, ready: impl Fn(ProgressStatistics) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ready(server.service.progress_statistics()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("progress coordinator did not reach the expected state");
}

async fn observers(
    server: &ProgressServer,
    query: &ExecutionProgressQuery,
    count: usize,
) -> Vec<tokio::task::JoinHandle<Result<ExecutionProgressPage>>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(count + 1));
    let mut handles = Vec::new();
    for _ in 0..count {
        let client = server.client.clone();
        let query = query.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            client.execution_progress(&query).await
        }));
    }
    barrier.wait().await;
    handles
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn http_waiters_share_reads_and_observe_an_independent_committed_transition() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let mut query = task_query(&task.task_id);
    let initial = page(&db, &query).await;
    query.cursor = Some(initial.next_cursor);
    query.wait_ms = 5000;
    let server = ProgressServer::new(db.store.clone()).await;
    let started = Instant::now();
    let readers = observers(&server, &query, 64).await;
    statistics_until(&server, |s| s.waiters == 64 && s.probes == 0).await;
    let idle_transactions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'")
        .fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(
        idle_transactions, 0,
        "observers retained idle database transactions"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(server.service.progress_statistics().total_probes <= started.elapsed().as_secs() + 2);
    // This writer is a separately connected storage adapter, not an in-memory wake.
    let writer = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    writer.cancel(&scope(), &task.task_id).await.unwrap();
    for reader in readers {
        let observed = reader.await.unwrap().unwrap();
        observed.validate_for(&query).unwrap();
        assert!(observed.terminal);
        assert_eq!(
            observed
                .events
                .iter()
                .map(|e| e.reason.as_str())
                .collect::<Vec<_>>(),
            ["cancel_requested", "cancelled"]
        );
    }
    statistics_until(&server, |s| s.waiters == 0 && s.keys == 0 && s.probes == 0).await;
    let stats = server.service.progress_statistics();
    assert_eq!(stats.peak_keys, 1);
    assert_eq!(stats.peak_probes, 1);
    assert!(
        stats.total_probes <= started.elapsed().as_secs() + 2,
        "idle fanout multiplied database reads: {stats:?}"
    );
    writer.close().await;
    server.finish().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn http_observer_deadlines_are_independent_and_disconnections_release_bounded_state() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let mut query = task_query(&task.task_id);
    query.cursor = Some(page(&db, &query).await.next_cursor);
    query.wait_ms = 3000;
    let server = ProgressServer::new(db.store.clone()).await;
    let mut long = observers(&server, &query, 1).await;
    statistics_until(&server, |s| s.waiters == 1 && s.probes == 0).await;
    let mut short = query.clone();
    short.wait_ms = 1100;
    let observed = server.client.execution_progress(&short).await.unwrap();
    assert!(observed.events.is_empty() && !observed.terminal);
    assert!(
        !long[0].is_finished(),
        "short observer deadline terminated its peer"
    );
    long.pop().unwrap().abort();
    // Some HTTP transports retain the request future after a peer disconnect.
    // The per-request maximum still bounds server-side registrations.
    statistics_until(&server, |s| s.waiters == 0 && s.keys == 0 && s.probes == 0).await;
    db.store.cancel(&scope(), &task.task_id).await.unwrap();
    let remaining = server.client.execution_progress(&query).await.unwrap();
    assert!(remaining.terminal && !remaining.events.is_empty());
    server.finish().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn http_idle_observer_read_comparison_records_matched_polling_and_waiting_workloads() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let mut query = task_query(&task.task_id);
    query.cursor = Some(page(&db, &query).await.next_cursor);
    let server = ProgressServer::new(db.store.clone()).await;
    const OBSERVERS: usize = 32;
    const POLLS: usize = 3;
    let before = Instant::now();
    let mut baseline = Vec::new();
    for _ in 0..OBSERVERS {
        let client = server.client.clone();
        let query = query.clone();
        baseline.push(tokio::spawn(async move {
            for _ in 0..POLLS {
                let start = Instant::now();
                let page = client.execution_progress(&query).await.unwrap();
                assert!(page.events.is_empty() && !page.terminal);
                tokio::time::sleep_until(start + Duration::from_millis(1000)).await;
            }
        }));
    }
    for task in baseline {
        task.await.unwrap();
    }
    let baseline_ms = before.elapsed().as_millis();
    query.wait_ms = 3000;
    let before = Instant::now();
    for task in observers(&server, &query, OBSERVERS).await {
        let page = task.await.unwrap().unwrap();
        assert!(page.events.is_empty() && !page.terminal);
    }
    let waiting_ms = before.elapsed().as_millis();
    let stats = server.service.progress_statistics();
    assert!(
        u128::from(stats.total_probes) <= waiting_ms / 1000 + 2,
        "coalescing did not bound idle probes: {stats:?}"
    );
    assert_eq!(stats.waiters, 0);
    assert_eq!(stats.keys, 0);
    println!(
        "PROGRESS_OBSERVER_COMPARISON {}",
        json!({
            "observers": OBSERVERS, "target_count": 1, "idle": true,
            "poll_interval_ms": 1000, "coordinator_probe_interval_ms": 1000, "polling_http_requests": OBSERVERS * POLLS,
            "polling_elapsed_ms": baseline_ms, "wait_http_requests": OBSERVERS,
            "wait_ms": 3000, "wait_elapsed_ms": waiting_ms,
            "wait_backend_probes": stats.total_probes, "peak_probes": stats.peak_probes,
            "scope": "one local HTTP server and PostgreSQL; request/read reduction, not fleet capacity"
        })
    );
    server.finish().await;
    db.finish().await;
}
