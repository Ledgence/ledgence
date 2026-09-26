use super::*;

fn assert_complete(snapshot: &WorkerObservationSnapshot) {
    assert_eq!(
        snapshot.detail_state,
        WorkerObservationDetailState::Available
    );
    assert_eq!(snapshot.slots.len(), snapshot.configured_concurrency);
    assert!(snapshot.active_consumers <= snapshot.configured_concurrency);
    let occupied = snapshot
        .slots
        .iter()
        .filter(|slot| slot.state != ProcessSlotState::Empty)
        .count();
    assert_eq!(occupied, snapshot.occupied_process_slots);
    for (id, slot) in snapshot.slots.iter().enumerate() {
        assert_eq!(slot.slot_id, id);
        if slot.state == ProcessSlotState::Empty {
            assert!(slot.process_instance_id.is_none());
            assert!(slot.process_id.is_none());
            assert!(slot.program.is_none());
            assert!(slot.digest.is_none());
            assert!(slot.scope.is_none());
            assert!(slot.invocation.is_none());
        }
    }
}

#[tokio::test]
async fn consumer_reservation_is_not_a_process_or_an_empty_capacity_promise() {
    let (worker, _) = setup(2);
    let reserved = worker.reserve_consumer(control()).await.unwrap();
    let snapshot = worker.observation().await;
    assert_complete(&snapshot);
    assert_eq!(snapshot.active_consumers, 1);
    assert_eq!(snapshot.occupied_process_slots, 0);
    assert!(snapshot.accepting);
    reserved.release();
    assert_eq!(worker.observation().await.active_consumers, 0);
}

struct FixedPidRuntime(Runtime);
impl ExecutionRuntime for FixedPidRuntime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            match self.0.start(artifact, control).await? {
                StartOutcome::Ready(session) => {
                    Ok(StartOutcome::Ready(Box::new(FixedPidSession(session))))
                }
                other => Ok(other),
            }
        })
    }
}
struct FixedPidSession(Box<dyn ExecutionSession>);
impl ExecutionSession for FixedPidSession {
    fn pid(&self) -> u32 {
        42
    }
    fn execute<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
    ) -> PortFuture<'a, ProgramOutcome> {
        self.0.execute(invocation, control)
    }
    fn close(&mut self) -> PortFuture<'_, ()> {
        self.0.close()
    }
}

#[tokio::test]
async fn warm_reuse_retains_identity_but_replacement_with_same_pid_does_not() {
    let counts = Arc::new(Counts::default());
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            fetch_timeout: Duration::from_secs(2),
        },
        Arc::new(Store(counts.clone())),
        Arc::new(Cache::default()),
        Arc::new(FixedPidRuntime(Runtime(counts.clone()))),
    )
    .unwrap();
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let first = worker.observation().await;
    assert_complete(&first);
    assert_eq!(first.slots[0].state, ProcessSlotState::Warm);
    assert!(first.slots[0].invocation.is_none());
    assert_eq!(first.slots[0].process_id, Some(42));
    assert!(first.slots[0].process_instance_id.is_some());
    worker.execute(request(2, 1, 0), control()).await.unwrap();
    let reused = worker.observation().await;
    assert_eq!(reused.slots, first.slots);
    worker.execute(request(3, 2, 0), control()).await.unwrap();
    let replaced = worker.observation().await;
    assert_complete(&replaced);
    assert_eq!(replaced.slots[0].slot_id, first.slots[0].slot_id);
    assert_eq!(replaced.slots[0].process_id, first.slots[0].process_id);
    assert_ne!(
        replaced.slots[0].process_instance_id,
        first.slots[0].process_instance_id
    );
    assert_eq!(replaced.slots[0].program.as_ref().unwrap().version, "2");
    assert!(replaced.slots[0].invocation.is_none());
    worker.shutdown_until_quiescent().await.unwrap();
    assert_eq!(
        worker.observation().await.slots[0].state,
        ProcessSlotState::Empty
    );
}

struct GatedStartRuntime {
    runtime: Runtime,
    gate: Arc<CloseGate>,
}
impl ExecutionRuntime for GatedStartRuntime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            self.gate.entered.add_permits(1);
            self.gate.release.acquire().await.unwrap().forget();
            self.runtime.start(artifact, control).await
        })
    }
}

#[tokio::test]
async fn pending_startup_owns_a_slot_before_any_pid_exists() {
    let counts = Arc::new(Counts::default());
    let gate = Arc::new(CloseGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            fetch_timeout: Duration::from_secs(2),
        },
        Arc::new(Store(counts.clone())),
        Arc::new(Cache::default()),
        Arc::new(GatedStartRuntime {
            runtime: Runtime(counts.clone()),
            gate: gate.clone(),
        }),
    )
    .unwrap();
    let caller = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(1, 1, 0), control()).await })
    };
    gate.wait_until_entered().await;
    let starting = worker.observation().await;
    assert_complete(&starting);
    assert_eq!(starting.occupied_process_slots, 1);
    assert_eq!(starting.slots[0].state, ProcessSlotState::Starting);
    assert!(starting.slots[0].process_id.is_none());
    assert_eq!(
        starting.slots[0].invocation.as_ref().unwrap().attempt_id,
        "att_1"
    );
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
    caller.abort();
    let _ = caller.await;
    // Dropping observation/caller does not prove that the still-pending startup stopped.
    assert_eq!(worker.observation().await.occupied_process_slots, 1);
    gate.release.add_permits(1);
    worker.shutdown_until_quiescent().await.unwrap();
    assert_eq!(
        worker.observation().await.slots[0].state,
        ProcessSlotState::Empty
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_processes_have_unique_slots_and_only_current_invocations() {
    let (worker, counts) = setup(3);
    let gate = Arc::new(CloseGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    *counts.execute_gate.lock().unwrap() = Some(gate.clone());
    let mut callers = Vec::new();
    for id in 1..=3 {
        let worker = worker.clone();
        let mut invocation = request(id, id, 0);
        let mut event = invocation.event.into_value();
        event["data"]["hold_execution"] = true.into();
        invocation.event = CloudEvent::new(event).unwrap();
        callers.push(tokio::spawn(async move {
            worker.execute(invocation, control()).await
        }));
    }
    for _ in 0..3 {
        gate.wait_until_entered().await;
    }
    for _ in 0..100 {
        let snapshot = worker.observation().await;
        assert_complete(&snapshot);
        assert_eq!(snapshot.occupied_process_slots, 3);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|s| s.state == ProcessSlotState::Executing)
        );
        let mut attempts = snapshot
            .slots
            .iter()
            .map(|s| s.invocation.as_ref().unwrap().attempt_id.clone())
            .collect::<Vec<_>>();
        attempts.sort();
        assert_eq!(attempts, ["att_1", "att_2", "att_3"]);
        let identities = snapshot
            .slots
            .iter()
            .map(|s| s.process_instance_id.as_ref().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(identities.len(), 3);
    }
    gate.release.add_permits(3);
    for caller in callers {
        caller.await.unwrap().unwrap();
    }
    let warm = worker.observation().await;
    assert_complete(&warm);
    assert!(
        warm.slots
            .iter()
            .all(|s| s.state == ProcessSlotState::Warm && s.invocation.is_none())
    );
    worker.shutdown_until_quiescent().await.unwrap();
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retirement_and_failed_cleanup_keep_identity_and_occupied_capacity() {
    let (worker, counts) = setup(1);
    let gate = CloseGate::install(&counts);
    counts.close_failures.store(1, Ordering::SeqCst);
    let mut invocation = request(1, 1, 0);
    let mut event = invocation.event.into_value();
    event["data"]["crash"] = true.into();
    invocation.event = CloudEvent::new(event).unwrap();
    let caller = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(invocation, control()).await })
    };
    gate.wait_until_entered().await;
    let retiring = worker.observation().await;
    assert_complete(&retiring);
    assert_eq!(retiring.slots[0].state, ProcessSlotState::Retiring);
    assert_eq!(retiring.occupied_process_slots, 1);
    gate.finish(&counts);
    assert!(caller.await.unwrap().is_err());
    let pending = worker.observation().await;
    assert_complete(&pending);
    assert_eq!(pending.slots[0].state, ProcessSlotState::CleanupPending);
    assert_eq!(
        pending.slots[0].process_instance_id,
        retiring.slots[0].process_instance_id
    );
    assert_eq!(pending.slots[0].invocation, retiring.slots[0].invocation);
    worker.shutdown_until_quiescent().await.unwrap();
    assert_eq!(worker.observation().await.occupied_process_slots, 0);
}

#[tokio::test]
async fn startup_and_cleanup_panics_never_make_a_slot_appear_free() {
    for during_start in [true, false] {
        let (worker, counts) = setup(1);
        let mut invocation = request(1, 1, 0);
        if during_start {
            counts.start_panics.store(1, Ordering::SeqCst);
        } else {
            counts.close_panics.store(1, Ordering::SeqCst);
            let mut event = invocation.event.into_value();
            event["data"]["crash"] = true.into();
            invocation.event = CloudEvent::new(event).unwrap();
        }
        assert!(worker.execute(invocation, control()).await.is_err());
        let snapshot = worker.observation().await;
        assert_complete(&snapshot);
        assert!(!snapshot.accepting);
        assert_eq!(snapshot.occupied_process_slots, 1);
        assert_eq!(
            snapshot.slots[0].state,
            if during_start {
                ProcessSlotState::Unknown
            } else {
                ProcessSlotState::CleanupPending
            }
        );
        assert!(snapshot.slots[0].process_instance_id.is_some());
        assert_eq!(
            worker.shutdown_until_quiescent().await.is_err(),
            during_start
        );
    }
}

#[tokio::test]
async fn large_capacity_is_preserved_without_allocating_detailed_slots() {
    let (worker, _) = setup(Semaphore::MAX_PERMITS);
    let initial = worker.observation().await;
    assert_eq!(initial.configured_concurrency, Semaphore::MAX_PERMITS);
    assert_eq!(
        initial.detail_state,
        WorkerObservationDetailState::UnsupportedCapacity
    );
    assert_eq!(initial.occupied_process_slots, 0);
    assert!(initial.slots.is_empty());
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let warm = worker.observation().await;
    assert_eq!(warm.configured_concurrency, Semaphore::MAX_PERMITS);
    assert_eq!(warm.occupied_process_slots, 1);
    assert!(warm.slots.is_empty());
    worker.shutdown_until_quiescent().await.unwrap();
}
