//! Bounded, fair delivery of persisted completion obligations.

use crate::health::Health;
use ledgence_orchestration_api::*;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet, time::Instant};
use tracing::instrument::WithSubscriber;

const MAX_DESTINATIONS: usize = 16;
const GLOBAL_IN_FLIGHT: usize = 16;
const PER_DESTINATION: u32 = 2;
const BATCH_BUDGET: Duration = Duration::from_secs(25);
const IDLE: Duration = Duration::from_secs(1);
const FAILURE_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

pub struct Destination {
    pub binding: CompletionDestination,
    pub sender: Arc<dyn CompletionSender>,
}

struct Schedule {
    destination: Arc<Destination>,
    active: bool,
    next: Instant,
    unavailable: bool,
    backoff: Duration,
}

/// Only active batches own memory and requests. Waiting subscriptions stay in
/// storage. A destination has at most two sends; all destinations share sixteen.
/// Expired/uncertain leases remain recoverable in storage when a batch fails.
pub async fn run(
    store: Arc<dyn CompletionStore>,
    destinations: Vec<Destination>,
    health: Health,
    mut stopped: watch::Receiver<bool>,
) -> Result<()> {
    if destinations.is_empty() || destinations.len() > MAX_DESTINATIONS {
        return Err(ContractError::InvalidInput(
            "completion dispatcher requires 1..16 destinations".into(),
        ));
    }
    let mut identities = HashSet::new();
    for destination in &destinations {
        destination.binding.validate()?;
        if !identities.insert((
            destination.binding.scope.clone(),
            destination.binding.destination.clone(),
        )) {
            return Err(ContractError::InvalidInput(
                "duplicate completion dispatcher destination".into(),
            ));
        }
    }
    let _lifetime = Lifetime(health.clone());
    let mut schedules: Vec<_> = destinations
        .into_iter()
        .map(|destination| Schedule {
            destination: Arc::new(destination),
            active: false,
            next: Instant::now(),
            unavailable: false,
            backoff: FAILURE_BACKOFF,
        })
        .collect();
    let mut tasks = JoinSet::new();
    let mut cursor = 0;
    let mut draining = false;
    let mut failure = None;
    loop {
        draining |= *stopped.borrow() || stopped.has_changed().is_err();
        if !draining {
            // Start each ready destination once in rotating order. Reservations
            // include database leasing and settlement, so requests never exceed
            // the global bound even when every leased batch is full.
            for _ in 0..schedules.len() {
                if tasks.len() >= GLOBAL_IN_FLIGHT / PER_DESTINATION as usize {
                    break;
                }
                let index = cursor;
                cursor = (cursor + 1) % schedules.len();
                let schedule = &mut schedules[index];
                if schedule.active || schedule.next > Instant::now() {
                    continue;
                }
                schedule.active = true;
                let destination = schedule.destination.clone();
                let store = store.clone();
                tasks.spawn(
                    async move {
                        let deadline = Instant::now() + BATCH_BUDGET;
                        let result = tokio::time::timeout_at(
                            deadline,
                            batch(store.as_ref(), &destination, deadline),
                        )
                        .await
                        .unwrap_or_else(|_| Err(unavailable("completion batch deadline elapsed")));
                        (index, result)
                    }
                    .with_current_subscriber(),
                );
            }
        }
        if draining && tasks.is_empty() {
            return failure.map_or(Ok(()), Err);
        }
        let wake = if draining || tasks.len() >= GLOBAL_IN_FLIGHT / PER_DESTINATION as usize {
            Instant::now() + Duration::from_secs(1)
        } else {
            schedules
                .iter()
                .filter(|schedule| !schedule.active)
                .map(|schedule| schedule.next)
                .min()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(1))
        };
        tokio::select! {
            changed = stopped.changed(), if !draining => {
                draining = changed.is_err() || *stopped.borrow();
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined.expect("a nonempty JoinSet returns a task") {
                    Ok((index, Ok(leased))) => {
                        let schedule = &mut schedules[index];
                        schedule.active = false;
                        schedule.unavailable = false;
                        schedule.backoff = FAILURE_BACKOFF;
                        schedule.next = Instant::now() + if leased == PER_DESTINATION as usize { Duration::ZERO } else { IDLE };
                        if failure.is_none() && schedules.iter().all(|schedule| !schedule.unavailable) {
                            // Receiver failures are successfully persisted retry
                            // decisions, not orchestration availability failures.
                            // A fatal batch keeps readiness failed while accepted
                            // sibling batches drain, even if they finish normally.
                            health.completion_success();
                        }
                    }
                    Ok((index, Err(ContractError::Unavailable(_)))) => {
                        let schedule = &mut schedules[index];
                        schedule.active = false;
                        schedule.unavailable = true;
                        schedule.next = Instant::now() + schedule.backoff;
                        schedule.backoff = schedule.backoff.saturating_mul(2).min(MAX_BACKOFF);
                        health.completion_failure("completion_unavailable");
                        tracing::warn!(destination = %schedule.destination.binding.destination,
                            "completion storage unavailable; uncertain leases retained for recovery");
                    }
                    Ok((_, Err(error))) => {
                        health.completion_failure("completion_failed");
                        failure.get_or_insert(error);
                        draining = true;
                    }
                    Err(_) => {
                        health.completion_failure("completion_failed");
                        failure.get_or_insert_with(|| unavailable("completion dispatcher task failed"));
                        draining = true;
                    }
                }
                // Full batches catch up without a fixed sleep, with a rotating
                // next admission point and cooperative scheduler fairness.
                tokio::task::yield_now().await;
            }
            _ = tokio::time::sleep_until(wake) => {}
        }
    }
}

async fn batch(
    store: &dyn CompletionStore,
    destination: &Destination,
    deadline: Instant,
) -> Result<usize> {
    let leases = store
        .lease_completions(&destination.binding, PER_DESTINATION, deadline.into_std())
        .await?;
    if leases.len() > PER_DESTINATION as usize {
        return Err(unavailable("completion store exceeded batch limit"));
    }
    let mut identities = HashSet::new();
    for lease in &leases {
        lease
            .validate()
            .map_err(|_| unavailable("completion store returned an invalid lease"))?;
        if lease.subscription.command.scope != destination.binding.scope
            || lease.subscription.command.destination != destination.binding.destination
            || !identities.insert(lease.subscription.subscription_id.clone())
        {
            return Err(unavailable(
                "completion store returned an unexpected or duplicate subscription",
            ));
        }
    }
    if leases.is_empty() {
        return Ok(0);
    }
    if Instant::now() >= deadline {
        return Err(unavailable("completion deadline elapsed before sending"));
    }
    let deliver = |lease| deliver_one(destination.sender.as_ref(), lease, deadline);
    let mut results = Vec::with_capacity(leases.len());
    if leases.len() == 2 {
        let (first, second) = tokio::join!(deliver(&leases[0]), deliver(&leases[1]));
        results.push(first);
        results.push(second);
    } else {
        results.push(deliver(&leases[0]).await);
    }
    if Instant::now() >= deadline {
        return Err(unavailable("completion deadline elapsed before settlement"));
    }
    store
        .complete_deliveries(&results, deadline.into_std())
        .await?;
    Ok(leases.len())
}

async fn deliver_one(
    sender: &dyn CompletionSender,
    lease: &CompletionLease,
    deadline: Instant,
) -> CompletionDeliveryResult {
    let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
    let result =
        tokio::time::timeout_at(deadline, sender.deliver(lease, deadline.into_std())).await;
    let outcome = match result {
        Ok(Ok(outcome)) if Instant::now() < deadline => outcome,
        _ => CompletionDeliveryOutcome::Retry {
            reason: "delivery_outcome_uncertain".into(),
            retry_after_ms: None,
        },
    };
    let mut result = CompletionDeliveryResult {
        subscription_id: lease.subscription.subscription_id.clone(),
        generation: lease.subscription.generation,
        lease_token: lease.lease_token.clone(),
        outcome,
    };
    if result.validate().is_err() {
        result.outcome = CompletionDeliveryOutcome::Retry {
            reason: "invalid_delivery_reply".into(),
            retry_after_ms: None,
        };
    }
    result
}

fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}
struct Lifetime(Health);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.completion_failure("completion_stopped");
    }
}

#[cfg(test)]
mod tests;
