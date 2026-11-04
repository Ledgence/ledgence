use super::*;
use ledgence_worker_delivery::poll_plan::{PollPlan, PollPlanner, PollStrategy};

const PRIMARY: &str = "invoices";
const OTHER: &str = "reports";

fn weights(revision: u64, a: u32, b: u32) -> PollPlan {
    PollPlan::new(revision, vec![(PRIMARY.into(), a), (OTHER.into(), b)]).unwrap()
}

#[tokio::test]
async fn active_routes_share_one_physical_pool_and_pause_without_preempting_work() {
    let (worker, counts) = setup(2);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(4);
    let planner = PollPlanner::new(weights(1, 1, 1), PollStrategy::Stride);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .with_poll_planner(planner.clone())
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 2).await;
    {
        let state = service.state.lock().unwrap();
        let commands = &state.acquisitions;
        assert_eq!(commands.len(), 2);
        assert_ne!(commands[0].queue, commands[1].queue);
        assert_ne!(commands[0].consumer_id, commands[1].consumer_id);
    }
    planner.install(weights(2, 0, 0)).unwrap();
    counts.hold_execution.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 2).await;
    // Take both physical slots ourselves while the idle plan is installed:
    // neither consumer parks holding its reservation or starts a probe.
    let first = worker
        .reserve_consumer(RunControl::new(WAIT))
        .await
        .unwrap();
    let second = worker
        .reserve_consumer(RunControl::new(WAIT))
        .await
        .unwrap();
    assert_eq!(service.state.lock().unwrap().acquisitions.len(), 2);
    planner.install(weights(3, 0, 1)).unwrap();
    drop((first, second));
    wait_for(|| handle.status().settled_attempts == 4).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    assert_eq!(status.route_sessions.len(), 2);
    assert_eq!(
        status.session_id.as_ref(),
        status.route_sessions.get(PRIMARY)
    );
    let state = service.state.lock().unwrap();
    assert!(state.acquisitions[2..].iter().all(|cmd| cmd.queue == OTHER));
    let mut next = HashMap::new();
    for cmd in &state.acquisitions {
        let seq = next
            .entry((cmd.worker_session_id.as_str(), cmd.consumer_id))
            .or_insert(1);
        assert_eq!(cmd.sequence, *seq);
        *seq += 1;
        assert_eq!(status.route_sessions[&cmd.queue], cmd.worker_session_id);
    }
    assert_eq!(state.capacity_violations, 0);
    assert!(counts.peak.load(Ordering::SeqCst) <= 2);
    assert!(status.last_error.is_none(), "{status:?}");
}

#[tokio::test]
async fn committed_uncertain_poll_reuses_its_route_after_the_plan_changes() {
    for task_count in [0, 1] {
        let (worker, counts) = setup(1);
        let service = Service::new(task_count);
        service.first_acquire_delay_ms.store(500, Ordering::SeqCst);
        let planner = PollPlanner::new(weights(1, 1, 0), PollStrategy::Stride);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .with_poll_planner(planner.clone())
            .unwrap()
            .start();
        wait_for(|| !service.state.lock().unwrap().replies.is_empty()).await;
        planner.install(weights(2, 0, 1)).unwrap();
        wait_for(|| {
            service
                .state
                .lock()
                .unwrap()
                .acquisitions
                .iter()
                .any(|c| c.queue == OTHER)
        })
        .await;
        let status = handle.shutdown(WAIT).await.unwrap();
        let state = service.state.lock().unwrap();
        assert_eq!(state.acquisitions[0], state.acquisitions[1]);
        assert_eq!(state.acquisitions[0].queue, PRIMARY);
        assert_eq!(state.acquisitions[2].sequence, 1);
        assert_eq!(state.acquisitions[2].queue, OTHER);
        assert_eq!(status.settled_attempts, task_count as u64);
        assert_eq!(counts.executions.load(Ordering::SeqCst), task_count);
    }
}

#[tokio::test]
async fn admission_session_and_handler_start_authority_use_the_selected_route() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    let planner = PollPlanner::new(weights(1, 0, 1), PollStrategy::Stride);
    let mut handle = task_admission::driver(worker, service.clone())
        .with_poll_planner(planner)
        .unwrap()
        .start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    let owner = &service.task_admission.starts.lock().unwrap()[0].owner;
    assert_eq!(owner.worker_session_id, status.route_sessions[OTHER]);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert!(service.state.lock().unwrap().sessions.values().all(|s| {
        s.features
            .iter()
            .any(|f| f == worker_api::TASK_ADMISSION_FEATURE)
    }));
}

#[tokio::test]
async fn a_denied_second_registration_never_dispatches_on_a_partial_route_set() {
    let (worker, counts) = setup(2);
    let service = Service::new(1);
    service.state.lock().unwrap().deny_registration = Some(OTHER.into());
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .with_poll_planner(PollPlanner::new(weights(1, 1, 1), PollStrategy::Stride))
        .unwrap()
        .start();
    let status = tokio::time::timeout(WAIT, handle.wait()).await.unwrap();
    assert!(matches!(status.last_error, Some(ContractError::Forbidden)));
    assert!(status.route_sessions.is_empty());
    {
        let state = service.state.lock().unwrap();
        assert_eq!(state.sessions.len(), 1);
        assert!(state.acquisitions.is_empty());
    }
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert!(!worker.stats().await.accepting);
}

#[tokio::test]
async fn a_custom_source_and_a_planner_are_rejected_in_either_builder_order() {
    let service = Service::new(0);
    let source = Arc::new(UnexpectedSource);
    let planner = PollPlanner::new(weights(1, 1, 1), PollStrategy::Stride);
    let (first, _) = setup(1);
    assert!(
        DeliveryDriver::new(first, service.clone(), config())
            .unwrap()
            .with_acquisition_source(source.clone())
            .with_poll_planner(planner.clone())
            .is_err()
    );
    let (second, _) = setup(1);
    let mut handle = DeliveryDriver::new(second, service.clone(), config())
        .unwrap()
        .with_poll_planner(planner)
        .unwrap()
        .with_acquisition_source(source)
        .start();
    let status = tokio::time::timeout(WAIT, handle.wait()).await.unwrap();
    assert!(matches!(
        status.last_error,
        Some(ContractError::InvalidInput(_))
    ));
    assert!(service.state.lock().unwrap().sessions.is_empty());
}

struct UnexpectedSource;
impl ledgence_worker_delivery::AcquisitionSource for UnexpectedSource {
    fn acquire<'a>(
        &'a self,
        _: &'a AcquireCommand,
        _: AcquireOptions,
    ) -> ContractFuture<'a, ledgence_worker_delivery::SourceReply> {
        panic!("planned polling must reject the source before any acquisition")
    }
}
