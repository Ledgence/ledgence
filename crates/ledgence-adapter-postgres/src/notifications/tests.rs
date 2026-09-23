use super::*;

fn queue(number: usize) -> AcquisitionHint {
    AcquisitionHint::QueueChanged(AcquisitionQueue {
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        },
        queue: format!("queue-{number}"),
    })
}

#[test]
fn strict_bounded_notification_schema_preserves_platform_keys() {
    let hint = AcquisitionHint::AcquisitionCompleted(AcquisitionKey {
        queue: AcquisitionQueue {
            scope: Scope {
                tenant_id: "t".into(),
                namespace: "n".into(),
            },
            queue: "q".into(),
        },
        worker_session_id: "worker-session".into(),
        consumer_id: u32::MAX,
        sequence: u64::MAX,
    });
    let payload = encode(hint.clone()).unwrap();
    assert_eq!(decode(&payload), Some(hint));
    assert!(decode(&payload.replace("\"version\":1", "\"version\":2")).is_none());
    assert!(decode(&payload.replacen('{', "{\"unknown\":0,", 1)).is_none());
    assert!(decode(&payload.replace("\"version\":1", "\"version\":1,\"version\":1")).is_none());
    assert!(decode(&"x".repeat(MAX_PAYLOAD_BYTES + 1)).is_none());
    assert!(encode(AcquisitionHint::Rescan).is_none());
    assert!(
        encode(AcquisitionHint::QueueChanged(AcquisitionQueue {
            scope: Scope {
                tenant_id: "t".into(),
                namespace: "n".into()
            },
            queue: "".into(),
        }))
        .is_none()
    );
}

#[test]
fn publisher_coalesces_and_bounds_unique_hints_and_releases_closed_backlog() {
    let publisher = Publisher::default();
    for number in 0..MAX_HINTS {
        publisher.enqueue(queue(number));
        publisher.enqueue(queue(number));
    }
    publisher.enqueue(queue(MAX_HINTS));
    assert_eq!(publisher.statistics().queued, MAX_HINTS);
    assert_eq!(publisher.statistics().dropped, 1);
    for number in 0..MAX_HINTS {
        let payload = publisher.pop().unwrap();
        assert_eq!(decode(&payload), Some(queue(number)));
        publisher.finished(&payload);
    }
    assert!(publisher.pop().is_none());
    publisher.enqueue(queue(7));
    publisher.close();
    publisher.enqueue(queue(8));
    assert_eq!(publisher.statistics().queued, 0);
    assert_eq!(publisher.statistics().dropped, 2);
}

#[test]
fn faulty_optional_local_adapter_cannot_escape_into_committed_mutation() {
    struct Panics;
    impl AcquisitionWake for Panics {
        fn wake(&self, _: AcquisitionHint) {
            panic!("controlled optional sink panic");
        }
    }
    let dispatch = WakeDispatch::default();
    dispatch.set_local(Arc::new(Panics));
    dispatch.publish(queue(0));
}

#[derive(Default)]
struct Recorder(Mutex<Vec<AcquisitionHint>>);
impl AcquisitionWake for Recorder {
    fn wake(&self, hint: AcquisitionHint) {
        self.0.lock().unwrap().push(hint);
    }
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("notification state did not converge");
}

// Closing the client pool completes local ownership cleanup. PostgreSQL's
// Terminate message has no acknowledgement, so its backend can remain visible
// briefly after the client socket has closed. Observe that independent boundary
// without relaxing the manager's shutdown deadline or accepting a persistent leak.
async fn wait_for_notification_backends_to_close(
    pool: &sqlx::PgPool,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let started = Instant::now();
    let mut backends = Vec::new();
    let result = tokio::time::timeout(timeout, async {
        loop {
            backends = sqlx::query_as::<_, (i32, Option<String>, Option<String>, Option<String>)>(
                "SELECT pid, state, wait_event_type, wait_event FROM pg_stat_activity \
                 WHERE datname=current_database() AND application_name='ledgence-wake' \
                 ORDER BY pid",
            )
            .fetch_all(pool)
            .await
            .map_err(|error| format!("could not inspect notification backends: {error}"))?;
            if backends.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        Err(format!(
            "notification backends did not close within {timeout:?} (elapsed {:?}); \
             last (pid, state, wait_event_type, wait_event): {backends:?}",
            started.elapsed(),
        ))
    })
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn backend_shutdown_observation_rejects_a_connection_that_remains_open() {
    use sqlx::{ConnectOptions, Connection};
    let db = crate::tests::TestDb::new().await;
    let options: sqlx::postgres::PgConnectOptions = db.url.parse().unwrap();
    let mut held = options
        .application_name("ledgence-wake")
        .connect()
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut held)
        .await
        .unwrap();
    let error = wait_for_notification_backends_to_close(&db.store.pool, Duration::from_secs(2))
        .await
        .expect_err("an open notification backend must not pass shutdown observation");
    assert!(error.contains(&pid.to_string()), "{error}");
    assert!(error.contains("idle"), "{error}");
    held.close().await.unwrap();
    wait_for_notification_backends_to_close(&db.store.pool, Duration::from_secs(8))
        .await
        .unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cross_replica_hints_reconnect_and_close_without_using_the_query_pool() {
    use crate::tests::{TestDb, acquire_command, command, descriptor, scope};
    let db = TestDb::new().await;
    let other = PostgresStore::connect(
        &db.url,
        PostgresOptions {
            max_connections: 1,
            ..PostgresOptions::default()
        },
    )
    .await
    .unwrap();
    let seen = Arc::new(Recorder::default());
    other.set_acquisition_wake(seen.clone());
    let first = db.store.start_acquisition_notifications(&db.url).unwrap();
    let second = other.start_acquisition_notifications(&db.url).unwrap();
    until(|| first.statistics().listener_connected && second.statistics().listener_connected).await;
    assert!(db.store.start_acquisition_notifications(&db.url).is_err());
    other.check_connection().await.unwrap();
    seen.0.lock().unwrap().clear();
    db.store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    until(|| {
        seen.0
            .lock()
            .unwrap()
            .iter()
            .any(|hint| matches!(hint, AcquisitionHint::QueueChanged(_)))
    })
    .await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let request = acquire_command(&session, 0, 1);
    db.store.acquire(&request).await.unwrap();
    until(|| {
        seen.0
            .lock()
            .unwrap()
            .contains(&AcquisitionHint::AcquisitionCompleted((&request).into()))
    })
    .await;
    sqlx::query("SELECT pg_notify($1,'{invalid}')")
        .bind(CHANNEL)
        .execute(&db.store.pool)
        .await
        .unwrap();
    until(|| second.statistics().malformed > 0).await;
    let admin_url = std::env::var("LEDGENCE_POSTGRES_URL").unwrap();
    let mut control = <sqlx::PgConnection as sqlx::Connection>::connect(&admin_url)
        .await
        .unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert!(
        database.starts_with("ldg_test_")
            && database
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    );
    for round in 0..2 {
        let before = second.statistics().subscriptions;
        // Existing lifecycle connections can commit while reconnecting listeners
        // cannot open a replacement socket. Terminate the publisher too so the
        // listener cannot reuse its existing connection. Only this test's DB is affected.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER DATABASE {database} ALLOW_CONNECTIONS false"
        )))
        .execute(&mut control)
        .await
        .unwrap();
        let terminated: Vec<bool> = sqlx::query_scalar("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND application_name='ledgence-wake' AND pid<>pg_backend_pid()")
            .bind(&database).fetch_all(&mut control).await.unwrap();
        assert!(!terminated.is_empty());
        assert!(terminated.into_iter().all(|value| value));
        until(|| !first.statistics().listener_connected && !second.statistics().listener_connected)
            .await;
        seen.0.lock().unwrap().clear();
        let mut input = command();
        input.idempotency_key = format!("disconnected-{round}");
        input.input.queue = format!("reconnected-{round}");
        let accepted = db
            .store
            .accept_resolved_submission(&input, &descriptor())
            .await
            .unwrap();
        assert!(!second.statistics().listener_connected);
        assert_eq!(second.statistics().subscriptions, before);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER DATABASE {database} ALLOW_CONNECTIONS true"
        )))
        .execute(&mut control)
        .await
        .unwrap();
        until(|| {
            second.statistics().subscriptions > before
                && second.statistics().listener_connected
                && seen.0.lock().unwrap().contains(&AcquisitionHint::Rescan)
        })
        .await;
        assert!(seen.0.lock().unwrap().contains(&AcquisitionHint::Rescan));
        let session = other
            .open_session(&scope(), &input.input.queue, 1)
            .await
            .unwrap();
        let request = acquire_command(&session, 0, 1);
        let assigned = crate::tests::assignment(other.acquire(&request).await.unwrap());
        assert_eq!(assigned.lease.owner.task_id, accepted.task_id);
        let replay = crate::tests::assignment(other.acquire(&request).await.unwrap());
        assert_eq!(assigned.event.value(), replay.event.value());
        assert_eq!(
            other
                .inspect(&scope(), &accepted.task_id)
                .await
                .unwrap()
                .attempt_count,
            1
        );
        until(|| first.statistics().listener_connected).await;
    }
    drop(control);
    let started = Instant::now();
    let (left, right) = tokio::join!(first.shutdown(), second.shutdown());
    assert!(!left.listener_connected && !right.listener_connected);
    assert!(started.elapsed() < Duration::from_secs(4));
    wait_for_notification_backends_to_close(&db.store.pool, Duration::from_secs(8))
        .await
        .unwrap();
    other.close().await;
    db.finish().await;
}

#[tokio::test]
async fn unavailable_notification_setup_is_nonblocking_and_shutdown_interrupts_handshake() {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "postgres://postgres:test@{}/postgres?sslmode=disable",
        socket.local_addr().unwrap()
    );
    let dispatch = Arc::new(WakeDispatch::default());
    let started = Instant::now();
    let manager = start(&url, dispatch.clone()).unwrap();
    assert!(started.elapsed() < Duration::from_millis(100));
    let (connection, _) = tokio::time::timeout(Duration::from_secs(2), socket.accept())
        .await
        .unwrap()
        .unwrap();
    dispatch.publish(queue(0));
    let stopped = Instant::now();
    let stats = manager.shutdown().await;
    assert!(!stats.listener_connected);
    assert!(stopped.elapsed() < Duration::from_secs(4));
    drop(connection);
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn shutdown_retains_auxiliary_pool_ownership_beyond_observation_deadline() {
    let db = crate::tests::TestDb::new().await;
    let manager = db.store.start_acquisition_notifications(&db.url).unwrap();
    until(|| manager.statistics().listener_connected).await;
    let held = manager.pool.acquire().await.unwrap();
    let mut shutdown = tokio::spawn(manager.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(3_100), &mut shutdown)
            .await
            .is_err(),
        "an owned auxiliary connection was abandoned at the observation deadline"
    );
    drop(held);
    let statistics = tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert!(!statistics.listener_connected);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn failed_optional_notifier_does_not_change_committed_submission_or_local_wake() {
    use crate::tests::{TestDb, acquire_command, command, descriptor, scope};
    let db = TestDb::new().await;
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "postgres://postgres:test@{}/postgres?sslmode=disable",
        socket.local_addr().unwrap()
    );
    let seen = Arc::new(Recorder::default());
    db.store.set_acquisition_wake(seen.clone());
    let manager = db.store.start_acquisition_notifications(&url).unwrap();
    let (connection, _) = tokio::time::timeout(Duration::from_secs(2), socket.accept())
        .await
        .unwrap()
        .unwrap();
    let accepted = tokio::time::timeout(
        Duration::from_secs(2),
        db.store
            .accept_resolved_submission(&command(), &descriptor()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        seen.0
            .lock()
            .unwrap()
            .iter()
            .any(|hint| matches!(hint, AcquisitionHint::QueueChanged(_)))
    );
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = crate::tests::assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    assert_eq!(assigned.lease.owner.task_id, accepted.task_id);
    manager.shutdown().await;
    drop(connection);
    db.finish().await;
}
