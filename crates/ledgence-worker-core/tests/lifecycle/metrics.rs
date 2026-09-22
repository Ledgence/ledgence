use super::*;
use ledgence_worker_api::metrics::{METRIC_TARGET, Metric, MetricOutcome};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

#[derive(Clone, Default)]
struct Capture(Arc<std::sync::Mutex<Vec<Observation>>>);
#[derive(Default)]
struct Observation {
    metric: u64,
    outcome: u64,
    value: f64,
}
impl Visit for Observation {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "metric" => self.metric = value,
            "outcome" => self.outcome = value,
            _ => {}
        }
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "value" {
            self.value = value;
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() == METRIC_TARGET {
            let mut observation = Observation::default();
            event.record(&mut observation);
            self.0.lock().unwrap().push(observation);
        }
    }
}
impl Capture {
    fn sum(&self, metric: Metric, outcome: MetricOutcome) -> f64 {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.metric == metric as u64 && o.outcome == outcome as u64)
            .map(|o| o.value)
            .sum()
    }
    fn count(&self, metric: Metric, outcome: MetricOutcome) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.metric == metric as u64 && o.outcome == outcome as u64)
            .count()
    }
}

#[tokio::test]
async fn measurements_follow_actual_reuse_and_reserved_consumer_ownership() {
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let (worker, _) = setup(1);
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    reservation
        .execute(request(2, 1, 0), control())
        .await
        .unwrap();
    assert_eq!(
        capture.sum(Metric::ConsumerSlots, MetricOutcome::None),
        1.0,
        "execution result does not release external ownership"
    );
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(capture.sum(Metric::ConsumerSlots, MetricOutcome::None), 0.0);
    assert_eq!(capture.sum(Metric::CacheLookup, MetricOutcome::Miss), 1.0);
    assert_eq!(capture.sum(Metric::CacheLookup, MetricOutcome::Hit), 1.0);
    assert_eq!(
        capture.sum(Metric::ProcessSelection, MetricOutcome::Started),
        1.0
    );
    assert_eq!(
        capture.sum(Metric::ProcessSelection, MetricOutcome::Reused),
        1.0
    );
    assert_eq!(
        capture.count(Metric::ExecutionDuration, MetricOutcome::Ok),
        2
    );
    assert_eq!(
        capture.count(Metric::PreparationDuration, MetricOutcome::Ok),
        2
    );
}

#[tokio::test]
async fn occupancy_includes_unconfirmed_cleanup_after_the_caller_releases() {
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let (worker, counts) = setup(1);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    counts.close_failures.store(1, Ordering::SeqCst);
    let mut invocation = request(1, 1, 0);
    let mut event = invocation.event.into_value();
    event["data"]["crash"] = true.into();
    invocation.event = CloudEvent::new(event).unwrap();
    reservation
        .execute(invocation, control())
        .await
        .unwrap_err();
    reservation.release();
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(capture.sum(Metric::ConsumerSlots, MetricOutcome::None), 1.0);
    assert_eq!(
        capture.count(Metric::ExecutionDuration, MetricOutcome::RuntimeError),
        1
    );
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(capture.sum(Metric::ConsumerSlots, MetricOutcome::None), 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reacquisition_never_reports_more_occupied_slots_than_capacity() {
    use tracing::instrument::WithSubscriber;
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let (worker, _) = setup(1);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let worker = worker.clone();
        tasks.spawn(
            async move {
                for _ in 0..100 {
                    let reservation = worker.reserve_consumer(control()).await.unwrap();
                    tokio::task::yield_now().await;
                    reservation.release();
                }
            }
            .with_current_subscriber(),
        );
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let observations = capture.0.lock().unwrap();
    let mut occupied = 0.0;
    let mut acquisitions = 0;
    for observation in observations
        .iter()
        .filter(|o| o.metric == Metric::ConsumerSlots as u64)
    {
        occupied += observation.value;
        if observation.value == 1.0 {
            acquisitions += 1;
        }
        assert!(
            (0.0..=1.0).contains(&occupied),
            "observed impossible occupancy: {occupied}"
        );
    }
    assert_eq!(occupied, 0.0);
    assert_eq!(acquisitions, 3200);
}
