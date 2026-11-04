use super::*;
use ledgence_worker_delivery::poll_plan::{PollPlan, PollPlanner, PollStrategy};
use std::collections::BTreeMap;

const PRIMARY: &str = "invoices";
const SECONDARY: &str = "reports";

fn plan(revision: u64, a: u32, b: u32) -> PollPlan {
    PollPlan::new(revision, vec![(PRIMARY.into(), a), (SECONDARY.into(), b)]).unwrap()
}

fn record_on(queue: &str, task: usize) -> QueueDelivery {
    let mut delivery = record(task);
    let mut body = PublishedDispatch::decode(&delivery.body).unwrap();
    body.dispatch.queue = queue.into();
    delivery.body = serde_json::to_vec(&body).unwrap();
    delivery.receipt = format!("{queue}/{}", delivery.receipt);
    delivery
}

fn sources(
    service: &Arc<Service>,
    a: &Arc<Queue>,
    b: &Arc<Queue>,
) -> BTreeMap<String, Arc<dyn AcquisitionSource>> {
    [(PRIMARY, a), (SECONDARY, b)]
        .into_iter()
        .map(|(queue, broker)| {
            let source = BrokerAcquisitionSource::new(broker.clone(), service.clone()).unwrap();
            (queue.into(), Arc::new(source) as Arc<dyn AcquisitionSource>)
        })
        .collect()
}

#[tokio::test]
async fn confirmed_empty_counts_as_a_fresh_poll_but_preserves_its_own_claim_sequence() {
    let service = Service::new(0);
    *service.broker_disposition.lock().unwrap() = Some(ClaimDisposition::TerminalOrSuperseded);
    let a = Queue::with_records([vec![], vec![record_on(PRIMARY, 1)]]);
    let b = Queue::with_records([vec![record_on(SECONDARY, 2)], vec![record_on(SECONDARY, 3)]]);
    let (worker, counts) = setup(1);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_planned_sources(
            PollPlanner::new(plan(1, 1, 1), PollStrategy::Stride),
            sources(&service, &a, &b),
        )
        .unwrap()
        .start();
    wait_for(|| b.acknowledgments.lock().unwrap().len() == 2).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    let claims = service.broker_commands.lock().unwrap();
    let observed: Vec<_> = claims
        .iter()
        .map(|c| (c.acquisition.queue.as_str(), c.acquisition.sequence))
        .collect();
    // A/empty, B/task, A/task, B/task. Neither a hidden retry of A/empty nor
    // advancing A's durable sequence on an empty receive produces this trace.
    assert_eq!(observed, [(SECONDARY, 1), (PRIMARY, 1), (SECONDARY, 2)]);
    for cmd in claims.iter() {
        assert_eq!(
            cmd.acquisition.worker_session_id,
            status.route_sessions[&cmd.dispatch.queue]
        );
        assert_eq!(cmd.acquisition.consumer_id, 0);
    }
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert!(status.last_error.is_none(), "{status:?}");
}

#[tokio::test]
async fn lost_claim_and_lost_delete_reconcile_original_route_after_weights_change() {
    let service = Service::new(2);
    service
        .first_acquire_delay_ms
        .store(1_000, Ordering::SeqCst);
    let a = Queue::with_records([vec![record_on(PRIMARY, 1)]]);
    a.ack_behavior
        .lock()
        .unwrap()
        .push_back(AckBehavior::Pending);
    let b = Queue::with_records([vec![record_on(SECONDARY, 2)]]);
    let planner = PollPlanner::new(plan(1, 1, 0), PollStrategy::Stride);
    let (worker, counts) = setup(1);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_planned_sources(planner.clone(), sources(&service, &a, &b))
        .unwrap()
        .start();
    wait_for(|| service.state.lock().unwrap().replies.len() == 1).await;
    planner.install(plan(2, 0, 1)).unwrap();
    wait_for(|| handle.status().settled_attempts == 2).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    let claims = service.broker_commands.lock().unwrap();
    let (last, retries) = claims.split_last().unwrap();
    assert!(retries.len() >= 3); // initial claim, lost response, lost delete
    assert!(retries.iter().all(|r| *r == retries[0]));
    assert_eq!(retries[0].dispatch.queue, PRIMARY);
    for (claim, queue) in [(last, SECONDARY), (&retries[0], PRIMARY)] {
        assert_eq!(
            claim.acquisition.worker_session_id,
            status.route_sessions[queue]
        );
        assert_eq!(claim.dispatch.queue, queue);
        assert_eq!(claim.acquisition.sequence, 1);
        assert_eq!(claim.acquisition.consumer_id, 0);
    }
    assert_eq!(a.polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        a.acknowledgments.lock().unwrap().as_slice(),
        &[vec![String::from("invoices/receipt_1")]]
    );
    assert_eq!(
        b.acknowledgments.lock().unwrap().as_slice(),
        &[vec![String::from("reports/receipt_2")]]
    );
    assert_eq!(counts.executions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn planned_brokers_share_physical_capacity_and_pause_without_interrupting_active_work() {
    let service = Service::new(2);
    let a = Queue::with_records([vec![record_on(PRIMARY, 1)]]);
    let b = Queue::with_records([vec![record_on(SECONDARY, 2)]]);
    b.receive_pending.store(true, Ordering::SeqCst);
    let (worker, counts) = setup(2);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let planner = PollPlanner::new(plan(1, 1, 1), PollStrategy::Stride);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .with_planned_sources(planner.clone(), sources(&service, &a, &b))
        .unwrap()
        .start();
    wait_for(|| {
        counts.executions.load(Ordering::SeqCst) == 1 && b.polls.load(Ordering::SeqCst) == 1
    })
    .await;
    b.receive_release.notify_one();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 2).await;
    planner.install(plan(2, 0, 0)).unwrap();
    counts.hold_execution.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 2).await;
    let slot_a = worker
        .reserve_consumer(RunControl::new(WAIT))
        .await
        .unwrap();
    let slot_b = worker
        .reserve_consumer(RunControl::new(WAIT))
        .await
        .unwrap();
    assert_eq!(a.polls.load(Ordering::SeqCst), 1);
    assert_eq!(b.polls.load(Ordering::SeqCst), 1);
    drop((slot_a, slot_b));
    let status = handle.shutdown(WAIT).await.unwrap();
    assert_eq!(status.route_sessions.len(), 2);
    assert_eq!(service.state.lock().unwrap().capacity_violations, 0);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 2);
    assert!(status.last_error.is_none(), "{status:?}");
}

#[tokio::test]
async fn stop_drains_only_the_bound_receive_even_when_its_route_loses_all_weight() {
    for simultaneous in [false, true] {
        let service = Service::new(2);
        let a = Queue::with_records([vec![record_on(PRIMARY, 1)]]);
        let b = Queue::with_records([vec![record_on(SECONDARY, 2)]]);
        a.receive_pending.store(true, Ordering::SeqCst);
        let (worker, counts) = setup(1);
        let planner = PollPlanner::new(plan(1, 1, 0), PollStrategy::Stride);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .with_planned_sources(planner.clone(), sources(&service, &a, &b))
            .unwrap()
            .start();
        tokio::time::timeout(WAIT, a.receive_entered.notified())
            .await
            .unwrap();
        planner.install(plan(2, 0, 1)).unwrap();
        handle.stop();
        if !simultaneous {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), handle.wait())
                    .await
                    .is_err()
            );
        }
        a.receive_release.notify_one();
        assert!(handle.shutdown(WAIT).await.unwrap().finished);
        assert_eq!(a.polls.load(Ordering::SeqCst), 1);
        assert_eq!(b.polls.load(Ordering::SeqCst), 0);
        assert!(service.broker_commands.lock().unwrap().is_empty());
        assert!(a.acknowledgments.lock().unwrap().is_empty());
        assert!(b.acknowledgments.lock().unwrap().is_empty());
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    }
}

#[derive(Default)]
struct Lifecycle {
    reject: bool,
    starts: Mutex<Vec<WorkerSession>>,
    finishes: Mutex<Vec<String>>,
}
impl AcquisitionSource for Lifecycle {
    fn start_session(&self, session: &WorkerSession) -> ledgence_orchestration_api::Result<()> {
        self.starts.lock().unwrap().push(session.clone());
        if self.reject {
            Err(ContractError::Forbidden)
        } else {
            Ok(())
        }
    }
    fn acquire<'a>(
        &'a self,
        _: &'a AcquireCommand,
        _: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        panic!("cannot poll a partially initialized route set")
    }
    fn finish_session(&self, session: &str) {
        self.finishes.lock().unwrap().push(session.into());
    }
}

#[tokio::test]
async fn startup_failure_unwinds_each_exact_session_and_never_advertises_partial_capacity() {
    let a = Arc::new(Lifecycle::default());
    let b = Arc::new(Lifecycle {
        reject: true,
        ..Default::default()
    });
    let sources = BTreeMap::from([
        (PRIMARY.into(), a.clone() as Arc<dyn AcquisitionSource>),
        (SECONDARY.into(), b.clone()),
    ]);
    let service = Service::new(0);
    let (worker, counts) = setup(2);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .with_planned_sources(
            PollPlanner::new(plan(1, 1, 1), PollStrategy::Stride),
            sources,
        )
        .unwrap()
        .start();
    let status = tokio::time::timeout(WAIT, handle.wait()).await.unwrap();
    assert!(matches!(status.last_error, Some(ContractError::Forbidden)));
    assert!(status.route_sessions.is_empty());
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert!(!worker.stats().await.accepting);
    for (source, queue) in [(a, PRIMARY), (b, SECONDARY)] {
        let starts = source.starts.lock().unwrap();
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].queue, queue);
        assert_eq!(*source.finishes.lock().unwrap(), [starts[0].id.clone()]);
    }
}

#[tokio::test]
async fn incomplete_extra_or_aliased_source_maps_fail_before_registration() {
    let service = Service::new(0);
    let a: Arc<dyn AcquisitionSource> = Arc::new(Lifecycle::default());
    let b: Arc<dyn AcquisitionSource> = Arc::new(Lifecycle::default());
    let planner = PollPlanner::new(plan(1, 1, 0), PollStrategy::Stride);
    for map in [
        BTreeMap::from([(PRIMARY.into(), a.clone())]),
        BTreeMap::from([(PRIMARY.into(), a.clone()), ("unknown".into(), b.clone())]),
        BTreeMap::from([(PRIMARY.into(), a.clone()), (SECONDARY.into(), a.clone())]),
    ] {
        let (worker, _) = setup(1);
        assert!(
            DeliveryDriver::new(worker, service.clone(), config())
                .unwrap()
                .with_planned_sources(planner.clone(), map)
                .is_err()
        );
    }
    assert!(service.state.lock().unwrap().sessions.is_empty());
}
