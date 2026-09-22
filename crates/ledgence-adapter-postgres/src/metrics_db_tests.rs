use super::*;
use crate::tests::{TestDb, acquire_command, command, descriptor, scope};
use std::sync::Mutex;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

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
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Observation>>>);
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() == ledgence_worker_api::metrics::METRIC_TARGET {
            let mut value = Observation::default();
            event.record(&mut value);
            self.0.lock().unwrap().push(value);
        }
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn claim_age_is_observed_after_commit_once_and_never_for_replay_or_empty_poll() {
    let db = TestDb::new().await;
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    db.store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 2).await.unwrap();
    let command = acquire_command(&session, 0, 1);
    assert!(matches!(
        db.store.acquire(&command).await.unwrap(),
        AcquireReply::Assigned { .. }
    ));
    assert!(matches!(
        db.store.acquire(&command).await.unwrap(),
        AcquireReply::Assigned { .. }
    ));
    assert!(matches!(
        db.store
            .acquire(&acquire_command(&session, 1, 1))
            .await
            .unwrap(),
        AcquireReply::Empty { .. }
    ));
    {
        let observations = capture.0.lock().unwrap();
        let ages: Vec<_> = observations
            .iter()
            .filter(|o| o.metric == Metric::QueueAge as u64)
            .collect();
        assert_eq!(ages.len(), 1);
        assert_eq!(ages[0].outcome, MetricOutcome::Integrated as u64);
        assert!(ages[0].value >= 0.0);
        // Submission, session creation and all three acquisition requests are
        // operational calls even though only one new task attempt was claimed.
        assert!(
            observations
                .iter()
                .filter(|o| o.metric == Metric::DatabaseDuration as u64)
                .count()
                >= 5
        );
    }
    db.finish().await;
}
