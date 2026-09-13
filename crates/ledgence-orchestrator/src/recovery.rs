use crate::health::Health;
use ledgence_orchestration_api::{
    CONTROL_REQUEST_TIMEOUT_MS, ContractError, MAX_RECOVERY_BATCH, RecoveryStore, Result,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Copy)]
pub struct RecoveryConfig {
    pub interval: Duration,
    pub max_backoff: Duration,
    pub operation_timeout: Duration,
    /// Extra batches after the first full shortlist, before yielding to cadence.
    pub max_catchup_batches: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            operation_timeout: Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS),
            max_catchup_batches: 10,
        }
    }
}

impl RecoveryConfig {
    pub fn freshness(self) -> Duration {
        self.operation_timeout + self.interval.max(self.max_backoff) + Duration::from_secs(5)
    }
}

/// Stop between bounded operations, retaining a current scan until it completes
/// or reaches its operation budget. A failed batch may already have committed
/// earlier transitions; the store owns safe repeat/concurrent scan semantics.
pub async fn run(
    store: Arc<dyn RecoveryStore>,
    health: Health,
    mut stop: watch::Receiver<bool>,
    config: RecoveryConfig,
) -> Result<()> {
    let _supervision = RecoveryLifetime(health.clone());
    let mut backoff = config.interval;
    loop {
        let mut delay = config.interval;
        for batch in 0..=config.max_catchup_batches {
            if *stop.borrow() {
                return Ok(());
            }
            let scan = tokio::time::timeout(
                config.operation_timeout,
                store.expire_batch(MAX_RECOVERY_BATCH),
            )
            .await;
            match scan {
                Ok(Ok(progress)) => {
                    health.recovery_success();
                    backoff = config.interval;
                    if progress.expired > 0 {
                        tracing::info!(
                            examined = progress.examined,
                            expired = progress.expired,
                            "expired attempts recovered"
                        );
                    }
                    // A full shortlist still merits catch-up when every candidate
                    // was skipped due to locks; expired == 0 is not exhaustion.
                    if progress.examined < MAX_RECOVERY_BATCH || batch == config.max_catchup_batches
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok(Err(ContractError::Unavailable(_))) | Err(_) => {
                    health.recovery_failure(if scan.is_err() {
                        "recovery_timeout"
                    } else {
                        "recovery_unavailable"
                    });
                    delay = backoff;
                    backoff = backoff.saturating_mul(2).min(config.max_backoff);
                    tracing::warn!(
                        retry_ms = delay.as_millis() as u64,
                        "recovery unavailable; retaining durable progress and retrying"
                    );
                    break;
                }
                Ok(Err(error)) => {
                    health.recovery_failure("recovery_failed");
                    return Err(error);
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {},
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

/// Even a panic or unexpected task cancellation immediately degrades health.
struct RecoveryLifetime(Health);
impl Drop for RecoveryLifetime {
    fn drop(&mut self) {
        self.0.recovery_failure("recovery_stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_orchestration_api::{ContractFuture, RecoveryProgress};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio::{
        sync::{Notify, mpsc},
        time::Instant,
    };

    enum Step {
        Reply(Result<RecoveryProgress>),
        Wait(Arc<Notify>),
        Stall,
        Panic,
    }

    struct Store {
        steps: Mutex<VecDeque<Step>>,
        calls: mpsc::UnboundedSender<Instant>,
        default_examined: u32,
    }

    impl RecoveryStore for Store {
        fn expire_batch(&self, limit: u32) -> ContractFuture<'_, RecoveryProgress> {
            assert_eq!(limit, MAX_RECOVERY_BATCH);
            let step = self.steps.lock().unwrap().pop_front();
            Box::pin(async move {
                self.calls.send(Instant::now()).unwrap();
                match step {
                    Some(Step::Reply(reply)) => reply,
                    Some(Step::Wait(release)) => {
                        release.notified().await;
                        Ok(RecoveryProgress {
                            examined: 0,
                            expired: 0,
                        })
                    }
                    Some(Step::Stall) => std::future::pending().await,
                    Some(Step::Panic) => panic!("controlled scanner panic"),
                    None => Ok(RecoveryProgress {
                        examined: self.default_examined,
                        expired: 0,
                    }),
                }
            })
        }
    }

    fn store(steps: Vec<Step>, examined: u32) -> (Arc<Store>, mpsc::UnboundedReceiver<Instant>) {
        let (calls, receive) = mpsc::unbounded_channel();
        (
            Arc::new(Store {
                steps: Mutex::new(steps.into()),
                calls,
                default_examined: examined,
            }),
            receive,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn full_shortlists_with_zero_expirations_catch_up_but_yield_to_cadence() {
        let config = RecoveryConfig::default();
        let (store, mut calls) = store(vec![], MAX_RECOVERY_BATCH);
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(store, Health::new(config.freshness()), stopped, config));
        let first = calls.recv().await.unwrap();
        for _ in 0..10 {
            assert_eq!(calls.recv().await.unwrap(), first);
        }
        assert!(calls.try_recv().is_err());
        assert_eq!(calls.recv().await.unwrap() - first, Duration::from_secs(1));
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_backoff_is_capped_and_success_resets_it() {
        let config = RecoveryConfig::default();
        let unavailable =
            || Step::Reply(Err(ContractError::Unavailable("controlled outage".into())));
        let (store, mut calls) = store(
            vec![
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                Step::Reply(Ok(RecoveryProgress {
                    examined: 0,
                    expired: 0,
                })),
                unavailable(),
            ],
            0,
        );
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(store, Health::new(config.freshness()), stopped, config));
        let mut previous = calls.recv().await.unwrap();
        for seconds in [1, 2, 4, 5, 1, 1] {
            let next = calls.recv().await.unwrap();
            assert_eq!(next - previous, Duration::from_secs(seconds));
            previous = next;
        }
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_operation_is_bounded_then_retried() {
        let config = RecoveryConfig::default();
        let (store, mut calls) = store(vec![Step::Stall], 0);
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(store, Health::new(config.freshness()), stopped, config));
        let first = calls.recv().await.unwrap();
        let second = calls.recv().await.unwrap();
        assert_eq!(second - first, Duration::from_secs(31));
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_finishes_current_scan_before_stopping() {
        let config = RecoveryConfig::default();
        let release = Arc::new(Notify::new());
        let (store, mut calls) = store(vec![Step::Wait(release.clone())], 0);
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(store, Health::new(config.freshness()), stopped, config));
        calls.recv().await.unwrap();
        stop.send(true).unwrap();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        release.notify_one();
        task.await.unwrap().unwrap();
        assert!(calls.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn unexpected_error_and_panic_finish_the_supervised_task() {
        let config = RecoveryConfig::default();
        let (store, _calls) = store(
            vec![Step::Reply(Err(ContractError::InvalidInput("bug".into())))],
            0,
        );
        // Keep receivers alive; the controlled store records every operation.
        let (sender, receiver) = watch::channel(false);
        let result = run(store, Health::new(config.freshness()), receiver, config).await;
        assert!(matches!(result, Err(ContractError::InvalidInput(_))));
        drop(sender);
        let (store, _calls) = self::store(vec![Step::Panic], 0);
        let (_sender, receiver) = watch::channel(false);
        let result = tokio::spawn(run(
            store,
            Health::new(config.freshness()),
            receiver,
            config,
        ))
        .await;
        assert!(result.unwrap_err().is_panic());
    }
}
