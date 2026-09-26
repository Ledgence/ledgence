//! Slot identities travel with the ownership that retains global capacity.
use super::*;

/// Unexpected destruction retains capacity and marks the observation uncertain.
/// Unwinding does not acquire the asynchronous pool lock.
pub(super) struct SlotToken {
    id: usize,
    abandoned: Arc<AtomicBool>,
}
impl Drop for SlotToken {
    fn drop(&mut self) {
        self.abandoned.store(true, Ordering::Release);
    }
}
pub(super) struct SlotEntry {
    observation: SlotObservation,
    abandoned: Arc<AtomicBool>,
}
impl SlotEntry {
    fn snapshot(&self) -> SlotObservation {
        let mut value = self.observation.clone();
        if self.abandoned.load(Ordering::Acquire) {
            value.state = ProcessSlotState::Unknown;
            value.invocation = None;
        }
        value
    }
}
impl Pool {
    pub(super) fn reserve_slot(&mut self, capacity: usize) -> Result<SlotToken> {
        let mut id = 0;
        for occupied in self.slots.keys() {
            if *occupied != id {
                break;
            }
            id += 1;
        }
        if id >= capacity {
            return Err(Error::new(
                ErrorKind::Capacity,
                "process slots are occupied",
            ));
        }
        let abandoned = Arc::new(AtomicBool::new(false));
        let mut observation = empty_slot(id);
        observation.state = ProcessSlotState::Unknown;
        self.slots.insert(
            id,
            SlotEntry {
                observation,
                abandoned: abandoned.clone(),
            },
        );
        Ok(SlotToken { id, abandoned })
    }
    pub(super) fn starting(
        &mut self,
        slot: &SlotToken,
        key: &SessionKey,
        artifact: &PreparedArtifact,
        identity: &InvocationIdentity,
    ) -> Result<()> {
        self.next_process_instance =
            self.next_process_instance.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorKind::Capacity,
                    "process observation identity exhausted",
                )
            })?;
        self.slots
            .get_mut(&slot.id)
            .expect("owned slot")
            .observation = SlotObservation {
            slot_id: slot.id,
            state: ProcessSlotState::Starting,
            process_instance_id: Some(format!("proc_{}", self.next_process_instance)),
            process_id: None,
            program: Some(artifact.manifest().program.clone()),
            digest: Some(key.digest.clone()),
            scope: Some(WorkerObservationScope {
                tenant_id: key.tenant.clone(),
                namespace: key.namespace.clone(),
            }),
            invocation: Some(invocation(identity)),
        };
        Ok(())
    }
    pub(super) fn state(&mut self, slot: &SlotToken, state: ProcessSlotState) {
        self.slots
            .get_mut(&slot.id)
            .expect("owned slot")
            .observation
            .state = state;
    }
    pub(super) fn executing(&mut self, slot: &SlotToken, pid: u32, identity: &InvocationIdentity) {
        let observation = &mut self
            .slots
            .get_mut(&slot.id)
            .expect("owned slot")
            .observation;
        observation.state = ProcessSlotState::Executing;
        observation.process_id = Some(pid);
        observation.invocation = Some(invocation(identity));
    }
    pub(super) fn warm(&mut self, slot: &SlotToken) {
        let observation = &mut self
            .slots
            .get_mut(&slot.id)
            .expect("owned slot")
            .observation;
        observation.state = ProcessSlotState::Warm;
        observation.invocation = None;
    }
    pub(super) fn release(&mut self, slot: &SlotToken) {
        self.slots.remove(&slot.id).expect("owned slot");
    }
}
fn invocation(identity: &InvocationIdentity) -> InvocationObservation {
    InvocationObservation {
        task_id: identity.task_id.clone(),
        attempt_id: identity.attempt_id.clone(),
    }
}
fn empty_slot(slot_id: usize) -> SlotObservation {
    SlotObservation {
        slot_id,
        state: ProcessSlotState::Empty,
        process_instance_id: None,
        process_id: None,
        program: None,
        digest: None,
        scope: None,
        invocation: None,
    }
}
impl Worker {
    /// Copy process ownership without adapter calls, external I/O or serialization.
    /// Empty slots are generated outside the locks and only for N<=1024.
    /// Uncertain cleanup and abandoned ownership always remain occupied.
    pub async fn observation(&self) -> WorkerObservationSnapshot {
        let capacity = self.inner.config.concurrency;
        let (occupied_process_slots, accepting, active_consumers, mut observed) = {
            let pool = self.inner.pool.lock().await;
            let registry = self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let observed = if capacity <= WORKER_OBSERVATION_MAX_SLOTS {
                pool.slots
                    .iter()
                    .map(|(id, entry)| (*id, entry.snapshot()))
                    .collect::<BTreeMap<_, _>>()
            } else {
                BTreeMap::new()
            };
            (
                pool.slots.len(),
                registry.accepting,
                capacity - self.inner.consumers.available_permits(),
                observed,
            )
        };
        let (detail_state, slots) = if capacity <= WORKER_OBSERVATION_MAX_SLOTS {
            (
                WorkerObservationDetailState::Available,
                (0..capacity)
                    .map(|id| observed.remove(&id).unwrap_or_else(|| empty_slot(id)))
                    .collect(),
            )
        } else {
            (
                WorkerObservationDetailState::UnsupportedCapacity,
                Vec::new(),
            )
        };
        WorkerObservationSnapshot {
            configured_concurrency: capacity,
            accepting,
            active_consumers,
            occupied_process_slots,
            detail_state,
            slots,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abandoned_token_does_not_release_capacity_or_claim_warm_reuse() {
        let mut pool = Pool::default();
        let token = pool.reserve_slot(1).unwrap();
        pool.state(&token, ProcessSlotState::Warm);
        drop(token);
        assert_eq!(pool.slots.len(), 1);
        assert_eq!(pool.slots[&0].snapshot().state, ProcessSlotState::Unknown);
        assert!(pool.reserve_slot(1).is_err());
    }

    #[test]
    fn old_token_destruction_cannot_mark_a_reallocated_slot_abandoned() {
        let mut pool = Pool::default();
        let first = pool.reserve_slot(1).unwrap();
        pool.release(&first);
        let second = pool.reserve_slot(1).unwrap();
        pool.state(&second, ProcessSlotState::Starting);
        drop(first);
        assert_eq!(pool.slots[&0].snapshot().state, ProcessSlotState::Starting);
        pool.release(&second);
    }
}
