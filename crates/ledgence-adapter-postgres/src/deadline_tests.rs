//! Deadline observations must include synchronous work in the final poll.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

fn store() -> PostgresStore {
    // These tests exercise the operation wrapper without contacting a database.
    PostgresStore {
        pool: PgPoolOptions::new()
            .connect_lazy("postgres://postgres@127.0.0.1:1/deadline_test")
            .unwrap(),
        instance_scope: Arc::new(std::sync::OnceLock::new()),
        operation_timeout: Duration::from_secs(1),
        trace_bridge: Arc::new(NoopTraceBridge),
        acquisition_wake: Arc::new(notifications::WakeDispatch::default()),
    }
}

#[tokio::test]
async fn late_synchronous_completion_is_uncertain_even_when_the_operation_succeeded() {
    let store = store();
    let completed = AtomicBool::new(false);
    let deadline = Instant::now() + Duration::from_millis(100);
    let result = store
        .run_until(deadline, || async {
            // Like decoding a large ready response, this poll cannot be preempted
            // by Tokio. The effect happened, but it cannot receive a timely ACK.
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
            );
            completed.store(true, Ordering::SeqCst);
            Ok(42)
        })
        .await;
    assert!(completed.load(Ordering::SeqCst));
    assert!(
        matches!(result, Err(ContractError::Unavailable(message)) if message.contains("reconcile"))
    );
}

#[tokio::test]
async fn expired_deadline_never_starts_an_operation() {
    let store = store();
    let invoked = AtomicBool::new(false);
    let result = store
        .run_until(Instant::now(), || async {
            invoked.store(true, Ordering::SeqCst);
            Ok(42)
        })
        .await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert!(!invoked.load(Ordering::SeqCst));
}

#[tokio::test]
async fn on_time_results_keep_their_value_and_definitive_error() {
    let store = store();
    assert_eq!(store.run(|| async { Ok(42) }).await.unwrap(), 42);
    let result: Result<()> = store
        .run(|| async { Err(ContractError::Conflict.into()) })
        .await;
    assert!(matches!(result, Err(ContractError::Conflict)));
}
