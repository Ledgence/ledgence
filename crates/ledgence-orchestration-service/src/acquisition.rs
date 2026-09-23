//! Request-owned long polling. Notifications schedule authoritative probes;
//! they never carry assignments or keep a database connection while idle.

mod state;
use ledgence_orchestration_api::*;
use state::{Registration, State};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const FINALIZATION_ALLOWANCE: Duration = Duration::from_secs(10);
const FALLBACK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_WAITERS: usize = 4096;
const MAX_SUBSCRIBERS: usize = 2;
const MAX_PROBES: usize = 4;

/// Current bounded coordinator resources and their observed high-water marks.
#[derive(Debug, Clone, Copy, Default)]
pub struct AcquisitionStatistics {
    pub waiters: usize,
    pub keys: usize,
    pub queues: usize,
    pub nominated: usize,
    pub probes: usize,
    pub peak_waiters: usize,
    pub peak_keys: usize,
    pub peak_queues: usize,
    pub peak_nominated: usize,
    pub peak_probes: usize,
}

pub(crate) struct Coordinator {
    state: Mutex<State>,
    permits: Semaphore,
}
impl Coordinator {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            permits: Semaphore::new(MAX_PROBES),
        })
    }

    pub(crate) fn stop(&self) {
        let mut state = self.state.lock().expect("acquisition state poisoned");
        state.stopping = true;
        state.wake_all();
    }

    pub(crate) fn statistics(&self) -> AcquisitionStatistics {
        self.state
            .lock()
            .expect("acquisition state poisoned")
            .statistics()
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        store: &dyn TaskStore,
        command: &AcquireCommand,
        options: AcquireOptions,
    ) -> Result<AcquireReply> {
        options.validate()?;
        command.scope.validate()?;
        validate_text(&command.queue, 128)?;
        validate_text(&command.worker_session_id, 128)?;
        let started = now();
        let deadline = options
            .deadline
            .min(started + Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS));
        let wait_until = (started + options.max_wait).min(
            deadline
                .checked_sub(FINALIZATION_ALLOWANCE)
                .unwrap_or(started),
        );
        if deadline <= started {
            return Err(unavailable());
        }
        // Registration precedes the initial authoritative probe. Each physical
        // subscriber validates independently, even for an identical command.
        let registration = Registration::new(self.clone(), command)?;
        let mut epoch = registration.epoch();
        let mut wait_observed = false;
        loop {
            let finish_empty = now() >= wait_until || registration.stopping();
            let result = self.probe(store, command, finish_empty, deadline).await?;
            match result {
                AcquisitionProbe::Completed { reply, kind } => {
                    registration.completed(epoch, kind);
                    if kind != AcquisitionCompletion::Replayed {
                        self.wake(AcquisitionHint::AcquisitionCompleted(command.into()));
                    }
                    return Ok(reply);
                }
                AcquisitionProbe::Pending {
                    session_remaining_ms,
                } => {
                    if finish_empty {
                        return Err(ContractError::Unavailable(
                            "store returned Pending from final acquisition probe".into(),
                        ));
                    }
                    if !wait_observed {
                        tracing::debug!(
                            tenant_id = command.scope.tenant_id,
                            namespace = command.scope.namespace,
                            queue = command.queue,
                            worker_session_id = command.worker_session_id,
                            consumer_id = command.consumer_id,
                            sequence = command.sequence,
                            "acquisition entered wait"
                        );
                        wait_observed = true;
                    }
                    registration.pending(epoch);
                    let session_check = now()
                        .checked_add(Duration::from_millis(session_remaining_ms))
                        .unwrap_or(wait_until);
                    epoch = registration.wait(wait_until.min(session_check)).await;
                }
            }
        }
    }

    async fn probe(
        &self,
        store: &dyn TaskStore,
        command: &AcquireCommand,
        finish_empty: bool,
        deadline: Instant,
    ) -> Result<AcquisitionProbe> {
        // Both initial and final probes join the same FIFO permit queue as
        // nominated wake probes. Permit waiting consumes the exchange budget.
        let operation = async {
            let _permit = self.permits.acquire().await.map_err(|_| unavailable())?;
            // A permit can become ready in the same poll as the deadline. Do
            // not start another mutation once the enclosing budget is spent.
            if now() >= deadline {
                return Err(unavailable());
            }
            let _count = ProbeCount::new(self);
            store
                .probe_acquisition(command, finish_empty, deadline)
                .await
        };
        let result = tokio::time::timeout_at(deadline.into(), operation)
            .await
            .map_err(|_| unavailable())?;
        // Tokio cannot interrupt a synchronous adapter poll. A late completed
        // mutation remains uncertain and must be reconciled, never acknowledged.
        if now() >= deadline {
            Err(unavailable())
        } else {
            result
        }
    }
}
impl AcquisitionWake for Coordinator {
    fn wake(&self, hint: AcquisitionHint) {
        self.state
            .lock()
            .expect("acquisition state poisoned")
            .hint(hint);
    }
}

struct ProbeCount<'a>(&'a Coordinator);
impl<'a> ProbeCount<'a> {
    fn new(coordinator: &'a Coordinator) -> Self {
        let mut state = coordinator
            .state
            .lock()
            .expect("acquisition state poisoned");
        state.probes += 1;
        state.peaks.peak_probes = state.peaks.peak_probes.max(state.probes);
        Self(coordinator)
    }
}
impl Drop for ProbeCount<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("acquisition state poisoned")
            .probes -= 1;
    }
}
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}
fn unavailable() -> ContractError {
    ContractError::Unavailable("acquisition exchange deadline exceeded".into())
}

#[cfg(test)]
mod tests;
