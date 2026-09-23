//! Bounded recovery of durable workflow obligations; no task waits live here.
use crate::health::Health;
use ledgence_orchestration_api::{ContractError, Result, WORKFLOW_MAX_WORK_BATCH};
use ledgence_orchestration_service::ApplicationService;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

const IDLE: Duration = Duration::from_millis(50);
const MAX_CATCHUP: usize = 64;

pub async fn run(
    service: Arc<ApplicationService>,
    health: Health,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let _lifetime = Lifetime(health.clone());
    let mut backoff = IDLE;
    loop {
        let mut delay = IDLE;
        for _ in 0..MAX_CATCHUP {
            if *stop.borrow() {
                return Ok(());
            }
            match tokio::time::timeout(
                Duration::from_secs(30),
                service.advance_workflows(WORKFLOW_MAX_WORK_BATCH),
            )
            .await
            {
                Ok(Ok(progress)) => {
                    health.workflow_success();
                    backoff = IDLE;
                    if progress.processed > 0 {
                        tracing::debug!(
                            processed = progress.processed,
                            activations = progress.activations_scheduled,
                            children = progress.children_scheduled,
                            "workflow obligations applied"
                        );
                    }
                    if progress.processed < WORKFLOW_MAX_WORK_BATCH {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok(Err(ContractError::Unavailable(_) | ContractError::OwnershipLost)) | Err(_) => {
                    health.workflow_failure("workflow_unavailable");
                    delay = backoff;
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
                    tracing::warn!(
                        retry_ms = delay.as_millis() as u64,
                        "workflow recovery unavailable; durable work retained"
                    );
                    break;
                }
                Ok(Err(error)) => {
                    health.workflow_failure("workflow_failed");
                    return Err(error);
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {},
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { return Ok(()); },
        }
    }
}
struct Lifetime(Health);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.workflow_failure("workflow_stopped");
    }
}
