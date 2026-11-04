use super::*;
use ledgence_orchestration_api::console::{
    WorkerObservationCommand, WorkerObservationPublisher, WorkerObservationReceipt,
};

#[derive(Default)]
struct Publisher {
    commands: Mutex<Vec<WorkerObservationCommand>>,
    hold: AtomicBool,
    fail: AtomicBool,
    panic: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct Publishing<'a>(&'a Publisher);
impl Drop for Publishing<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
impl WorkerObservationPublisher for Publisher {
    fn publish_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt> {
        assert!(
            !self.panic.load(Ordering::SeqCst),
            "publisher construction panic"
        );
        Box::pin(async move {
            command.validate()?;
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _active = Publishing(self);
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.commands.lock().unwrap().push(command.clone());
            while self.hold.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(ContractError::Unavailable("observation outage".into()));
            }
            Ok(WorkerObservationReceipt {
                worker_session_id: command.worker_session_id.clone(),
                sequence: command.sequence,
                received_at: 1,
                already_received: false,
            })
        })
    }
}

#[tokio::test]
async fn slow_observation_does_not_delay_execution_settlement_or_shutdown() {
    let (worker, counts) = setup(2);
    let service = Service::new(4);
    let publisher = Arc::new(Publisher::default());
    publisher.hold.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .with_observation_publisher(publisher.clone(), Some("Billing worker".into()))
        .unwrap()
        .start();
    wait_for(|| publisher.commands.lock().unwrap().len() == 1).await;
    wait_for(|| handle.status().settled_attempts == 4).await;
    assert_eq!(counts.executions.load(Ordering::SeqCst), 4);
    assert_eq!(publisher.active.load(Ordering::SeqCst), 1);
    let status = handle.shutdown(Duration::from_millis(500)).await.unwrap();
    assert!(status.finished);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    wait_for(|| publisher.commands.lock().unwrap().len() == 2).await;
    let commands = publisher.commands.lock().unwrap().clone();
    assert_eq!(commands[0].sequence.0, 1);
    assert_eq!(
        commands[1].sequence.0, 2,
        "uncertain publication identity cannot be reused"
    );
    assert_eq!(commands[1].worker_session_id, status.session_id.unwrap());
    assert_eq!(commands[1].scope, scope());
    assert_eq!(commands[1].display_name.as_deref(), Some("Billing worker"));
    assert!(!commands[1].snapshot.accepting);
    assert_eq!(commands[1].snapshot.active_consumers, 0);
    assert_eq!(commands[1].snapshot.occupied_process_slots, 0);
    assert!(
        commands[1]
            .snapshot
            .slots
            .iter()
            .all(|slot| slot.state == ProcessSlotState::Empty)
    );
    assert_eq!(publisher.peak.load(Ordering::SeqCst), 1);
    publisher.hold.store(false, Ordering::SeqCst);
    wait_for(|| publisher.active.load(Ordering::SeqCst) == 0).await;
}

#[tokio::test]
async fn unavailable_or_panicking_observer_does_not_change_driver_authority() {
    for panic in [false, true] {
        let (worker, counts) = setup(1);
        let service = Service::new(2);
        let publisher = Arc::new(Publisher::default());
        publisher.fail.store(!panic, Ordering::SeqCst);
        publisher.panic.store(panic, Ordering::SeqCst);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .with_observation_publisher(publisher, None)
            .unwrap()
            .start();
        wait_for(|| handle.status().settled_attempts == 2).await;
        let status = handle.shutdown(WAIT).await.unwrap();
        assert_eq!(counts.executions.load(Ordering::SeqCst), 2);
        assert_eq!(status.lost_attempts, 0);
        assert!(status.last_error.is_none());
        assert_eq!(service.state.lock().unwrap().accepted.len(), 2);
    }
}

#[tokio::test]
async fn periodic_report_coalesces_changes_and_uses_current_slot_ownership() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let publisher = Arc::new(Publisher::default());
    let mut handle = DeliveryDriver::new(worker, Service::new(1), config())
        .unwrap()
        .with_observation_publisher(publisher.clone(), None)
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    tokio::time::timeout(Duration::from_secs(7), async {
        while publisher.commands.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let commands = publisher.commands.lock().unwrap().clone();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[1].sequence.0, 2);
    assert_eq!(commands[1].snapshot.occupied_process_slots, 1);
    assert_eq!(
        commands[1].snapshot.slots[0].state,
        ProcessSlotState::Executing
    );
    assert_eq!(
        commands[1].snapshot.slots[0]
            .invocation
            .as_ref()
            .unwrap()
            .attempt_id,
        "att_1"
    );
    assert_eq!(publisher.peak.load(Ordering::SeqCst), 1);
    counts.hold_execution.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
}

#[tokio::test]
async fn idle_planned_worker_reports_its_physical_pool_on_the_primary_session_only() {
    use ledgence_worker_delivery::poll_plan::{PollPlan, PollPlanner, PollStrategy};
    let (worker, _) = setup(3);
    let service = Service::new(0);
    let publisher = Arc::new(Publisher::default());
    let plan = PollPlan::new(1, vec![("a".into(), 0), ("invoices".into(), 0)]).unwrap();
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_poll_planner(PollPlanner::new(plan, PollStrategy::Stride))
        .unwrap()
        .with_observation_publisher(publisher.clone(), Some("shared pool".into()))
        .unwrap()
        .start();
    wait_for(|| !publisher.commands.lock().unwrap().is_empty()).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    // The reporter sends a final bounded snapshot after driver shutdown.
    wait_for(|| publisher.commands.lock().unwrap().len() >= 2).await;
    let primary = &status.route_sessions["invoices"];
    assert_ne!(*primary, status.route_sessions["a"]);
    for command in publisher.commands.lock().unwrap().iter() {
        assert_eq!(&command.worker_session_id, primary);
        assert_eq!(command.snapshot.configured_concurrency, 3);
    }
    assert!(service.state.lock().unwrap().acquisitions.is_empty());
}

#[tokio::test]
async fn a_physical_snapshot_keeps_primary_links_and_suppresses_secondary_links() {
    use ledgence_worker_delivery::poll_plan::{PollPlan, PollPlanner, PollStrategy};
    let (worker, counts) = setup(2);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(2);
    let publisher = Arc::new(Publisher::default());
    let plan = PollPlan::new(1, vec![("invoices".into(), 1), ("reports".into(), 1)]).unwrap();
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_poll_planner(PollPlanner::new(plan, PollStrategy::Stride))
        .unwrap()
        .with_observation_publisher(publisher.clone(), None)
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 2).await;
    let primary = handle.status().session_id.unwrap();
    let primary_attempt = {
        let state = service.state.lock().unwrap();
        state
            .replies
            .values()
            .find_map(|reply| {
                if let AcquireReply::Assigned { assignment, .. } = reply
                    && assignment.lease.owner.worker_session_id == primary
                {
                    return Some(assignment.lease.owner.attempt_id.clone());
                }
                None
            })
            .expect("a primary task was acquired")
    };
    // Reports are periodic with a maximum 5.5s interval. Inspect an actual
    // sample taken while both the primary and secondary handlers are running.
    let observed = tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            if let Some(report) = publisher
                .commands
                .lock()
                .unwrap()
                .iter()
                .find(|r| {
                    r.snapshot.occupied_process_slots == 2
                        && r.snapshot.slots.iter().all(|slot| {
                            matches!(
                                slot.state,
                                ProcessSlotState::Executing | ProcessSlotState::Unknown
                            )
                        })
                })
                .cloned()
            {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observed.worker_session_id, primary);
    assert_eq!(observed.snapshot.active_consumers, 2);
    let primary_slot = observed
        .snapshot
        .slots
        .iter()
        .find(|slot| slot.state == ProcessSlotState::Executing)
        .unwrap();
    assert_eq!(
        primary_slot.invocation.as_ref().unwrap().attempt_id,
        primary_attempt
    );
    assert_eq!(
        observed
            .snapshot
            .slots
            .iter()
            .filter(|slot| slot.invocation.is_some())
            .count(),
        1
    );
    let secondary = observed
        .snapshot
        .slots
        .iter()
        .find(|slot| slot.state == ProcessSlotState::Unknown)
        .unwrap();
    assert!(secondary.invocation.is_none());
    assert!(secondary.process_instance_id.is_some());
    counts.hold_execution.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 2).await;
    handle.shutdown(WAIT).await.unwrap();
}
