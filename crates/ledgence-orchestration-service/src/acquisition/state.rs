use super::*;
use std::collections::{HashMap, VecDeque};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct State {
    pub stopping: bool,
    next_id: u64,
    entries: HashMap<u64, Entry>,
    keys: HashMap<AcquisitionKey, usize>,
    queues: HashMap<AcquisitionQueue, Queue>,
    order: VecDeque<AcquisitionQueue>,
    nominated: usize,
    #[cfg(test)]
    pub queue_visits: usize,
    pub probes: usize,
    pub peaks: AcquisitionStatistics,
}
struct Entry {
    key: AcquisitionKey,
    notify: Arc<Notify>,
    waiting: bool,
    receipt: bool,
    // A scheduled turn stays counted while awaiting a permit and while probing.
    turn: Option<u64>,
}
struct Queue {
    epoch: u64,
    open: bool,
    width: usize,
    active: usize,
    next_tick: Instant,
    order: VecDeque<u64>,
}

pub(super) struct Registration {
    coordinator: Arc<Coordinator>,
    id: u64,
    notify: Arc<Notify>,
}
impl Registration {
    pub fn new(coordinator: Arc<Coordinator>, command: &AcquireCommand) -> Result<Self> {
        let key = AcquisitionKey::from(command);
        let notify = Arc::new(Notify::new());
        let id = {
            let mut state = coordinator
                .state
                .lock()
                .expect("acquisition state poisoned");
            if state.stopping {
                return Err(ContractError::Unavailable(
                    "acquisition service is draining".into(),
                ));
            }
            if state.entries.len() >= MAX_WAITERS
                || state.keys.get(&key).copied().unwrap_or(0) >= MAX_SUBSCRIBERS
            {
                return Err(ContractError::Unavailable(
                    "acquisition waiter capacity exhausted".into(),
                ));
            }
            let id = state.next_id;
            state.next_id = id.checked_add(1).ok_or_else(|| {
                ContractError::Unavailable("acquisition registration IDs exhausted".into())
            })?;
            *state.keys.entry(key.clone()).or_default() += 1;
            if !state.queues.contains_key(&key.queue) {
                state.order.push_back(key.queue.clone());
                state.queues.insert(
                    key.queue.clone(),
                    Queue {
                        epoch: 0,
                        open: true,
                        width: 1,
                        active: 0,
                        next_tick: now() + FALLBACK_INTERVAL,
                        order: VecDeque::new(),
                    },
                );
            }
            state
                .queues
                .get_mut(&key.queue)
                .expect("queue inserted")
                .order
                .push_back(id);
            state.entries.insert(
                id,
                Entry {
                    key,
                    notify: notify.clone(),
                    waiting: false,
                    receipt: false,
                    turn: None,
                },
            );
            state.peaks.peak_waiters = state.peaks.peak_waiters.max(state.entries.len());
            state.peaks.peak_keys = state.peaks.peak_keys.max(state.keys.len());
            state.peaks.peak_queues = state.peaks.peak_queues.max(state.queues.len());
            id
        };
        Ok(Self {
            coordinator,
            id,
            notify,
        })
    }

    pub fn epoch(&self) -> u64 {
        let state = self
            .coordinator
            .state
            .lock()
            .expect("acquisition state poisoned");
        let entry = &state.entries[&self.id];
        state.queues[&entry.key.queue].epoch
    }
    pub fn stopping(&self) -> bool {
        self.coordinator
            .state
            .lock()
            .expect("acquisition state poisoned")
            .stopping
    }
    pub fn pending(&self, epoch: u64) {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("acquisition state poisoned");
        let key = state.entries[&self.id].key.queue.clone();
        state.release_turn(self.id);
        state.entries.get_mut(&self.id).expect("registered").waiting = true;
        let queue = state.queues.get_mut(&key).expect("registered queue");
        if epoch == queue.epoch {
            queue.open = false;
        }
        state.revoke_parked_turns(&key);
        state.pump();
    }
    pub fn completed(&self, epoch: u64, kind: AcquisitionCompletion) {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("acquisition state poisoned");
        let key = state.entries[&self.id].key.queue.clone();
        state.release_turn(self.id);
        if kind == AcquisitionCompletion::Claimed {
            let queue = state.queues.get_mut(&key).expect("registered queue");
            // A Pending from this epoch closes it permanently. A late successful
            // probe cannot restart fanout after the scan already found no work.
            if epoch == queue.epoch && queue.open {
                queue.width = MAX_PROBES;
            }
        }
        state.pump();
    }

    pub async fn wait(&self, until: Instant) -> u64 {
        loop {
            // Notify stores a permit for a wake between this check and polling.
            let notified = self.notify.notified();
            let next_tick = {
                let mut state = self
                    .coordinator
                    .state
                    .lock()
                    .expect("acquisition state poisoned");
                let queue_key = state.entries[&self.id].key.queue.clone();
                if state.stopping || now() >= until {
                    state.release_turn(self.id);
                    state.entries.get_mut(&self.id).expect("registered").waiting = false;
                    state.pump();
                    return state.queues[&queue_key].epoch;
                }
                if now() >= state.queues[&queue_key].next_tick {
                    let queue = state.queues.get_mut(&queue_key).expect("registered queue");
                    queue.next_tick = now() + FALLBACK_INTERVAL;
                    queue.epoch += 1;
                    queue.open = true;
                    queue.width = 1;
                    state.pump();
                }
                let entry = state.entries.get_mut(&self.id).expect("registered");
                if entry.receipt {
                    entry.receipt = false;
                    entry.waiting = false;
                    state.release_turn(self.id);
                    state.pump();
                    return state.queues[&queue_key].epoch;
                }
                if let Some(epoch) = entry.turn {
                    entry.waiting = false;
                    return epoch;
                }
                state.queues[&queue_key].next_tick
            };
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(until.min(next_tick).into()) => {},
            }
        }
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("acquisition state poisoned");
        state.release_turn(self.id);
        let entry = state.entries.remove(&self.id).expect("registered");
        let subscribers = state.keys.get_mut(&entry.key).expect("registered key");
        *subscribers -= 1;
        if *subscribers == 0 {
            state.keys.remove(&entry.key);
        }
        let queue = state
            .queues
            .get_mut(&entry.key.queue)
            .expect("registered queue");
        queue.order.retain(|id| *id != self.id);
        if queue.order.is_empty() {
            state.queues.remove(&entry.key.queue);
            state.order.retain(|key| key != &entry.key.queue);
        }
        state.pump();
    }
}

impl State {
    pub fn statistics(&self) -> AcquisitionStatistics {
        AcquisitionStatistics {
            waiters: self.entries.len(),
            keys: self.keys.len(),
            queues: self.queues.len(),
            nominated: self.nominated,
            probes: self.probes,
            ..self.peaks
        }
    }
    pub fn wake_all(&self) {
        for entry in self.entries.values() {
            entry.notify.notify_one();
        }
    }
    pub fn hint(&mut self, hint: AcquisitionHint) {
        match hint {
            AcquisitionHint::QueueChanged(key) => {
                let Some(queue) = self.queues.get_mut(&key) else {
                    return;
                };
                queue.epoch += 1;
                queue.open = true;
                queue.width = 1;
            }
            AcquisitionHint::AcquisitionCompleted(key) => {
                if !self.keys.contains_key(&key) {
                    return;
                }
                for entry in self.entries.values_mut().filter(|entry| entry.key == key) {
                    entry.receipt = true;
                    entry.notify.notify_one();
                }
            }
            AcquisitionHint::Rescan => {
                for queue in self.queues.values_mut() {
                    queue.epoch += 1;
                    queue.open = true;
                    queue.width = 1;
                }
            }
        }
        self.pump();
    }
    fn release_turn(&mut self, id: u64) {
        let entry = self.entries.get_mut(&id).expect("registered");
        if entry.turn.take().is_some() {
            self.nominated -= 1;
            self.queues
                .get_mut(&entry.key.queue)
                .expect("registered queue")
                .active -= 1;
        }
    }
    fn revoke_parked_turns(&mut self, key: &AcquisitionQueue) {
        let queue = &self.queues[key];
        if queue.open {
            return;
        }
        let ids: Vec<_> = queue
            .order
            .iter()
            .copied()
            .filter(|id| {
                let entry = &self.entries[id];
                entry.waiting && entry.turn.is_some()
            })
            .collect();
        for id in ids {
            self.release_turn(id);
        }
    }
    fn pump(&mut self) {
        if self.stopping {
            return;
        }
        let mut remaining = self.order.len();
        while self.nominated < MAX_PROBES && remaining > 0 {
            #[cfg(test)]
            {
                self.queue_visits += 1;
            }
            let key = self.order.pop_front().expect("nonempty queue rotation");
            let queue = self.queues.get_mut(&key).expect("queue in rotation");
            let mut nominated = false;
            if queue.open && queue.active < queue.width {
                for _ in 0..queue.order.len() {
                    let id = queue.order.pop_front().expect("registered queue member");
                    queue.order.push_back(id);
                    let entry = self.entries.get_mut(&id).expect("registered");
                    if entry.waiting && entry.turn.is_none() {
                        entry.turn = Some(queue.epoch);
                        queue.active += 1;
                        self.nominated += 1;
                        entry.notify.notify_one();
                        nominated = true;
                        break;
                    }
                }
            }
            self.order.push_back(key);
            remaining = if nominated {
                self.order.len()
            } else {
                remaining - 1
            };
        }
        self.peaks.peak_nominated = self.peaks.peak_nominated.max(self.nominated);
    }
}
