use super::*;
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};

const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_KEYS: usize = 256;
const MAX_WAITERS: usize = 4096;
const MAX_WAITERS_PER_KEY: usize = 256;
const MAX_PROBES: usize = 4;
const MAX_ERROR_BYTES: usize = 4096;

/// Resources held by positive progress waits in this application-service
/// instance. Immediate reads and other service instances are not counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProgressStatistics {
    pub keys: usize,
    pub waiters: usize,
    pub probes: usize,
    pub peak_keys: usize,
    pub peak_waiters: usize,
    pub peak_probes: usize,
    pub total_waits: u64,
    pub total_probes: u64,
}

pub(crate) struct Coordinator {
    state: Mutex<State>,
    permits: Semaphore,
}
#[derive(Default)]
struct State {
    stopping: bool,
    entries: HashMap<Key, Entry>,
    last_probe_generation: u64,
    statistics: ProgressStatistics,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    scope: Scope,
    kind: &'static str,
    id: String,
    after: u64,
    limit: u32,
}
impl Key {
    fn new(query: &ExecutionProgressQuery) -> Result<Self> {
        Ok(Self {
            scope: query.scope.clone(),
            kind: match query.target {
                ExecutionProgressTarget::Task { .. } => "task",
                ExecutionProgressTarget::Workflow { .. } => "workflow",
            },
            id: query.target.id().to_owned(),
            after: query.validate()?,
            limit: query.limit,
        })
    }
}
struct Entry {
    notify: Arc<Notify>,
    subscribers: usize,
    in_flight: bool,
    next_probe: Instant,
    // One validated, bounded page/error per key, shared without copying by idle
    // waiters. The final registration removes it; it is not a response cache.
    latest: Option<Snapshot>,
}
struct Snapshot {
    /// Assigned at authoritative read start, after acquiring the probe permit.
    generation: u64,
    result: Arc<Result<ExecutionProgressPage>>,
}
impl Coordinator {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            permits: Semaphore::new(MAX_PROBES),
        })
    }
    pub(crate) fn statistics(&self) -> ProgressStatistics {
        self.state
            .lock()
            .expect("progress state poisoned")
            .statistics
    }
    pub(crate) fn stop(&self) {
        let mut state = self.state.lock().expect("progress state poisoned");
        state.stopping = true;
        for entry in state.entries.values() {
            entry.notify.notify_waiters();
        }
    }
    pub(crate) async fn read(
        self: &Arc<Self>,
        reader: &dyn ExecutionProgressReader,
        query: &ExecutionProgressQuery,
    ) -> Result<ExecutionProgressPage> {
        let started = Instant::now();
        let key = Key::new(query)?;
        let deadline = started + Duration::from_millis(query.wait_ms);
        let registration = Registration::new(self.clone(), key)?;
        let mut probe_query = query.clone();
        probe_query.wait_ms = 0;
        loop {
            // Register before inspecting the shared state: a completed probe or
            // shutdown between inspection and await cannot lose its wake-up.
            let notified = registration.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match registration.decide(deadline)? {
                Decision::Return(result) => return result.as_ref().clone(),
                Decision::Wait(until) => {
                    tokio::select! {
                        _ = notified => {},
                        _ = tokio::time::sleep_until(until) => {},
                    }
                }
                Decision::Probe(guard) => {
                    let operation = async {
                        let _permit = self
                            .permits
                            .acquire()
                            .await
                            .map_err(|_| unavailable("progress probe admission stopped"))?;
                        if Instant::now() >= deadline {
                            return Err(unavailable("progress observation deadline exceeded"));
                        }
                        let count = ProbeCount::new(self)?;
                        let result =
                            reader
                                .execution_progress(&probe_query)
                                .await
                                .and_then(|page| {
                                    validate_page(&page, &probe_query)?;
                                    Ok(page)
                                });
                        Ok((count.generation, result))
                    };
                    tokio::select! {
                        biased;
                        _ = notified => {
                            // Shutdown wakes the elected reader too. Dropping
                            // the operation releases its probe and DB resources.
                            drop(guard);
                        },
                        result = tokio::time::timeout_at(deadline, operation) => {
                            match result {
                                Ok(Ok((generation, result))) if Instant::now() < deadline => guard.publish(generation, result),
                                Ok(Err(error)) => return Err(error),
                                _ => drop(guard),
                            }
                        },
                    }
                }
            }
        }
    }
}

struct Registration {
    coordinator: Arc<Coordinator>,
    key: Key,
    notify: Arc<Notify>,
    /// A snapshot whose read began before admission cannot satisfy this request.
    after_generation: u64,
}
impl Registration {
    fn new(coordinator: Arc<Coordinator>, key: Key) -> Result<Self> {
        let (notify, after_generation) = {
            let mut state = coordinator.state.lock().expect("progress state poisoned");
            if state.stopping {
                return Err(unavailable("progress observation service is draining"));
            }
            if state.statistics.waiters >= MAX_WAITERS
                || state
                    .entries
                    .get(&key)
                    .is_some_and(|entry| entry.subscribers >= MAX_WAITERS_PER_KEY)
                || (!state.entries.contains_key(&key) && state.entries.len() >= MAX_KEYS)
            {
                return Err(unavailable("progress observer capacity exhausted"));
            }
            let after_generation = state.last_probe_generation;
            let entry = state.entries.entry(key.clone()).or_insert_with(|| Entry {
                notify: Arc::new(Notify::new()),
                subscribers: 0,
                in_flight: false,
                next_probe: Instant::now(),
                latest: None,
            });
            entry.subscribers += 1;
            let notify = entry.notify.clone();
            state.statistics.waiters += 1;
            state.statistics.keys = state.entries.len();
            state.statistics.peak_waiters =
                state.statistics.peak_waiters.max(state.statistics.waiters);
            state.statistics.peak_keys = state.statistics.peak_keys.max(state.statistics.keys);
            state.statistics.total_waits = state.statistics.total_waits.saturating_add(1);
            (notify, after_generation)
        };
        Ok(Self {
            coordinator,
            key,
            notify,
            after_generation,
        })
    }
    fn decide(&self, deadline: Instant) -> Result<Decision> {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("progress state poisoned");
        if state.stopping {
            return Err(unavailable("progress observation service is draining"));
        }
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("registered progress key");
        if let Some(snapshot) = &entry.latest
            && snapshot.generation > self.after_generation
        {
            let ready = match snapshot.result.as_ref() {
                Err(_) => true,
                Ok(page) => !page.events.is_empty() || page.terminal,
            };
            if ready || Instant::now() >= deadline {
                return Ok(Decision::Return(snapshot.result.clone()));
            }
        } else if Instant::now() >= deadline {
            // Another observer may keep an older page alive. Returning it would
            // turn a post-retirement request into a stale successful response.
            // Keep the shared cadence; a short budget can instead be unavailable.
            return Err(unavailable(
                "progress observation deadline exceeded before a fresh snapshot",
            ));
        }
        if !entry.in_flight && Instant::now() >= entry.next_probe {
            entry.in_flight = true;
            return Ok(Decision::Probe(ProbeGuard {
                coordinator: self.coordinator.clone(),
                key: self.key.clone(),
                active: true,
            }));
        }
        Ok(Decision::Wait(if entry.in_flight {
            deadline
        } else {
            deadline.min(entry.next_probe)
        }))
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("progress state poisoned");
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("registered progress key");
        entry.subscribers -= 1;
        if entry.subscribers == 0 {
            state.entries.remove(&self.key);
        }
        state.statistics.waiters -= 1;
        state.statistics.keys = state.entries.len();
    }
}
enum Decision {
    Return(Arc<Result<ExecutionProgressPage>>),
    Wait(Instant),
    Probe(ProbeGuard),
}
struct ProbeGuard {
    coordinator: Arc<Coordinator>,
    key: Key,
    active: bool,
}
impl ProbeGuard {
    fn publish(mut self, generation: u64, result: Result<ExecutionProgressPage>) {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("progress state poisoned");
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("registered progress probe");
        // Errors can contain adapter-owned strings too. Do not retain an
        // arbitrarily large diagnostic while sharing an otherwise bounded page.
        let result = result.map_err(|error| match &error {
            ContractError::InvalidInput(message)
            | ContractError::InvalidQueueDelivery(message)
            | ContractError::Unavailable(message)
                if message.len() > MAX_ERROR_BYTES =>
            {
                unavailable("execution progress adapter error exceeds limit")
            }
            _ => error,
        });
        entry.latest = Some(Snapshot {
            generation,
            result: Arc::new(result),
        });
        entry.in_flight = false;
        entry.next_probe = Instant::now() + PROBE_INTERVAL;
        entry.notify.notify_waiters();
        self.active = false;
    }
}
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if self.active {
            let mut state = self
                .coordinator
                .state
                .lock()
                .expect("progress state poisoned");
            let entry = state
                .entries
                .get_mut(&self.key)
                .expect("registered progress probe");
            entry.in_flight = false;
            // The elected caller was cancelled or exhausted its own budget.
            // Surviving subscribers must be able to take over immediately.
            entry.next_probe = Instant::now();
            entry.notify.notify_waiters();
        }
    }
}
struct ProbeCount<'a> {
    coordinator: &'a Coordinator,
    generation: u64,
}
impl<'a> ProbeCount<'a> {
    fn new(coordinator: &'a Coordinator) -> Result<Self> {
        let mut state = coordinator.state.lock().expect("progress state poisoned");
        if state.stopping {
            return Err(unavailable("progress observation service is draining"));
        }
        let generation = state
            .last_probe_generation
            .checked_add(1)
            .ok_or_else(|| unavailable("progress probe generations exhausted"))?;
        // This lock also serializes observer admission. Cancelled probes consume
        // their generation permanently; neither cancellation nor key removal may
        // make an old read look newer than a later registration.
        state.last_probe_generation = generation;
        state.statistics.probes += 1;
        state.statistics.peak_probes = state.statistics.peak_probes.max(state.statistics.probes);
        state.statistics.total_probes = state.statistics.total_probes.saturating_add(1);
        Ok(Self {
            coordinator,
            generation,
        })
    }
}
impl Drop for ProbeCount<'_> {
    fn drop(&mut self) {
        self.coordinator
            .state
            .lock()
            .expect("progress state poisoned")
            .statistics
            .probes -= 1;
    }
}
fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}

#[cfg(test)]
mod tests;
