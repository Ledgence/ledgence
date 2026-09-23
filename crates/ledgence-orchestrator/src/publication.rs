//! Bounded durable intent publication; broker I/O never holds a state transaction.

use crate::health::Health;
use ledgence_orchestration_api::*;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const BATCH_BUDGET: Duration = Duration::from_secs(25);
const IDLE_INTERVAL: Duration = Duration::from_millis(100);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// The composition supplies an actual provider configuration check. There is no
/// default success probe and no implication that an empty DB poll reached SQS.
pub type ConfigurationProbe = Arc<dyn Fn() -> ContractFuture<'static, ()> + Send + Sync>;

pub async fn run(
    store: Arc<dyn DispatchIntentStore>,
    publisher: Arc<dyn DispatchPublisher>,
    route: DispatchRoute,
    probe: ConfigurationProbe,
    health: Health,
    mut stopped: watch::Receiver<bool>,
) -> Result<()> {
    route.validate()?;
    let limits = publisher.limits();
    limits.validate()?;
    if limits.max_message_bytes < DISPATCH_MAX_BYTES {
        return Err(ContractError::InvalidInput(
            "publisher cannot carry dispatch envelopes".into(),
        ));
    }
    let _lifetime = PublicationLifetime(health.clone());
    let mut backoff = INITIAL_BACKOFF;
    let mut broker_unavailable = false;
    let mut last_probe = Instant::now();
    loop {
        if *stopped.borrow() || stopped.has_changed().is_err() {
            return Ok(());
        }
        let deadline = Instant::now() + BATCH_BUDGET;
        let operation = batch(
            store.as_ref(),
            publisher.as_ref(),
            &route,
            limits.max_publish_batch,
            deadline,
        );
        let result = tokio::time::timeout_at(deadline, operation)
            .await
            .unwrap_or_else(|_| Err(unavailable("publication batch deadline elapsed")));
        let result = if Instant::now() >= deadline {
            Err(unavailable(
                "publication batch deadline elapsed; outcome is uncertain",
            ))
        } else {
            result
        };
        let mut delay = IDLE_INTERVAL;
        match result {
            Ok(progress) if progress.failed => {
                broker_unavailable = true;
                health.publication_failure("publication_unavailable");
                delay = backoff;
                backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                tracing::warn!(
                    retry_ms = delay.as_millis() as u64,
                    "publication unavailable; retaining retryable durable intents"
                );
            }
            Ok(progress) => {
                if progress.leased > 0 {
                    // Every result in this batch was positively confirmed.
                    broker_unavailable = false;
                }
                if broker_unavailable
                    && last_probe.elapsed() >= PROBE_INTERVAL
                    && !*stopped.borrow()
                {
                    last_probe = Instant::now();
                    let deadline = Instant::now() + BATCH_BUDGET;
                    let checked = tokio::time::timeout_at(deadline, probe()).await;
                    if matches!(checked, Ok(Ok(()))) && Instant::now() < deadline {
                        broker_unavailable = false;
                    }
                }
                if !broker_unavailable {
                    health.publication_success();
                    backoff = INITIAL_BACKOFF;
                } else {
                    // Empty due batches may hide delayed retries or retained
                    // uncertain leases. They cannot clear a broker outage.
                    health.publication_failure("publication_unavailable");
                    delay = MAX_BACKOFF;
                }
                if progress.leased == limits.max_publish_batch as usize {
                    // Drain full batches without a fixed per-batch pause. Keep
                    // scheduler fairness and check shutdown before the next lease.
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            Err(ContractError::Unavailable(_)) => {
                health.publication_failure("publication_unavailable");
                // A batch error can follow an uncertain send/commit, so the next
                // empty poll also needs positive provider evidence to recover.
                broker_unavailable = true;
                delay = backoff;
                backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                tracing::warn!(
                    retry_ms = delay.as_millis() as u64,
                    "publication state unavailable; leased intents remain recoverable"
                );
            }
            Err(error) => {
                health.publication_failure("publication_failed");
                return Err(error);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {},
            changed = stopped.changed() => {
                if changed.is_err() || *stopped.borrow() { return Ok(()); }
            }
        }
    }
}

struct Progress {
    leased: usize,
    failed: bool,
}

async fn batch(
    store: &dyn DispatchIntentStore,
    publisher: &dyn DispatchPublisher,
    route: &DispatchRoute,
    limit: u32,
    deadline: Instant,
) -> Result<Progress> {
    let leases = store
        .lease_publications(&route.destination, limit, deadline.into_std())
        .await?;
    if leases.len() > limit as usize {
        return Err(unavailable("publication store exceeded batch limit"));
    }
    let mut identities = HashSet::new();
    let mut dispatches = HashSet::new();
    for lease in &leases {
        lease
            .validate()
            .map_err(|_| unavailable("publication store returned invalid lease"))?;
        if lease.destination != route.destination
            || lease.record.dispatch.scope != route.scope
            || lease.record.dispatch.queue != route.queue
            || !identities.insert(lease.record.publication_id.clone())
            || !dispatches.insert(lease.record.dispatch.clone())
        {
            return Err(unavailable(
                "publication store returned unexpected or duplicate identity",
            ));
        }
    }
    if leases.is_empty() {
        return Ok(Progress {
            leased: 0,
            failed: false,
        });
    }
    if Instant::now() >= deadline {
        return Err(unavailable(
            "publication deadline elapsed before broker send",
        ));
    }
    let records: Vec<_> = leases.iter().map(|lease| lease.record.clone()).collect();
    let result = publisher.publish(&records, deadline.into_std()).await;
    let outcomes = match result {
        Ok(results) => validated_outcomes(&identities, results),
        Err(_) => None,
    };
    let completions: Vec<_> = leases
        .iter()
        .map(|lease| PublicationCompletion {
            dispatch: lease.record.dispatch.clone(),
            publication_id: lease.record.publication_id.clone(),
            lease_token: lease.lease_token.clone(),
            outcome: outcomes
                .as_ref()
                .and_then(|outcomes| outcomes.get(&lease.record.publication_id))
                .copied()
                .unwrap_or(PublicationOutcome::Retry),
        })
        .collect();
    let failed = completions
        .iter()
        .any(|completion| completion.outcome != PublicationOutcome::Confirmed);
    if Instant::now() >= deadline {
        return Err(unavailable(
            "publication deadline elapsed before completion",
        ));
    }
    store
        .complete_publications(&completions, deadline.into_std())
        .await?;
    Ok(Progress {
        leased: leases.len(),
        failed,
    })
}

fn validated_outcomes(
    expected: &HashSet<String>,
    results: Vec<PublishResult>,
) -> Option<HashMap<String, PublicationOutcome>> {
    if results.len() != expected.len() {
        return None;
    }
    let mut outcomes = HashMap::new();
    for result in results {
        if !expected.contains(&result.publication_id)
            || outcomes
                .insert(result.publication_id, result.outcome)
                .is_some()
        {
            return None;
        }
    }
    Some(outcomes)
}

fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}
struct PublicationLifetime(Health);
impl Drop for PublicationLifetime {
    fn drop(&mut self) {
        self.0.publication_failure("publication_stopped");
    }
}

#[cfg(test)]
mod tests;
