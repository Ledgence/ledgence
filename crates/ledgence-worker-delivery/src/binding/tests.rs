use super::*;

#[derive(Default)]
struct PendingSource(Mutex<Vec<&'static str>>);

impl AcquisitionSource for PendingSource {
    fn acquire<'a>(
        &'a self,
        _: &'a AcquireCommand,
        _: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        Box::pin(async {
            self.0.lock().unwrap().push("acquire");
            std::future::pending().await
        })
    }
    fn drain<'a>(
        &'a self,
        _: &'a AcquireCommand,
        _: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        Box::pin(async {
            self.0.lock().unwrap().push("drain");
            Ok(SourceReply::Idle)
        })
    }
}

struct IssuedEvents {
    count: Arc<std::sync::atomic::AtomicUsize>,
    stop: Arc<Shared>,
}
impl tracing_subscriber::Layer<tracing_subscriber::Registry> for IssuedEvents {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, tracing_subscriber::Registry>,
    ) {
        if event.metadata().target() != "ledgence_worker_delivery::polls" {
            return;
        }
        #[derive(Default)]
        struct Issued(bool);
        impl tracing::field::Visit for Issued {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "event" {
                    self.0 = value == "worker.poll.issued";
                }
            }
        }
        let mut issued = Issued::default();
        event.record(&mut issued);
        if issued.0 {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.stop.stop();
        }
    }
}

#[tokio::test]
async fn fresh_handoff_charges_and_traces_only_issued_source_calls() {
    use poll_plan::{PollPlan, PollPlanner, PollStrategy};
    use tracing_subscriber::prelude::*;

    for (stopping, paused) in [(true, false), (false, true), (false, false)] {
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(stopping),
            stop_changed: tokio::sync::Notify::new(),
            status: Mutex::default(),
        });
        let issued = Arc::new(std::sync::atomic::AtomicUsize::default());
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(IssuedEvents {
                count: issued.clone(),
                stop: shared.clone(),
            }));
        let scope = Scope {
            tenant_id: "t".into(),
            namespace: "n".into(),
        };
        let config = DeliveryConfig::new(scope.clone(), "a");
        let weights =
            |revision, w| PollPlan::new(revision, vec![("a".into(), w), ("b".into(), w)]).unwrap();
        let planner = PollPlanner::new(weights(1, 1), PollStrategy::Stride);
        if paused {
            planner.install(weights(2, 0)).unwrap();
        }
        let sources = [
            Arc::new(PendingSource::default()),
            Arc::new(PendingSource::default()),
        ];
        let (routes, commands) = fixture(
            &scope,
            &["a", "b"],
            &[sources[0].clone(), sources[1].clone()],
        );
        let reply = tokio::time::timeout(
            Duration::from_secs(2),
            acquire(&routes, &commands, Some(&planner), &config, &shared),
        )
        .await
        .unwrap();
        if stopping || paused {
            assert!(reply.is_none());
            assert_eq!(issued.load(Ordering::SeqCst), 0);
            assert!(sources[0].0.lock().unwrap().is_empty());
            if paused {
                planner.install(weights(3, 1)).unwrap();
            }
            assert_eq!(
                planner.issue(),
                Some(0),
                "skipped handoff cannot alter stride debt"
            );
        } else {
            // The subscriber requested stop synchronously at the issue trace.
            // The originally selected source enters acquire exactly once
            // and then drains, with no second fresh receive or plan charge.
            assert!(matches!(reply, Some((0, SourceReply::Idle))));
            assert_eq!(issued.load(Ordering::SeqCst), 1);
            assert_eq!(*sources[0].0.lock().unwrap(), ["acquire", "drain"]);
            assert_eq!(
                planner.issue(),
                Some(1),
                "issued handoff costs exactly one turn"
            );
        }
        assert!(sources[1].0.lock().unwrap().is_empty());
    }
}

fn fixture(
    scope: &Scope,
    queues: &[&str],
    sources: &[Arc<dyn AcquisitionSource>],
) -> (Vec<RegisteredRoute>, Vec<AcquireCommand>) {
    let routes: Vec<_> = queues
        .iter()
        .zip(sources)
        .map(|(queue, source)| RegisteredRoute {
            session: WorkerSession {
                scope: scope.clone(),
                queue: (*queue).into(),
                id: format!("session-{queue}"),
                expires_at: 10_000,
                concurrency: 1,
                features: vec![],
            },
            source: source.clone(),
        })
        .collect();
    let commands = routes
        .iter()
        .map(|r| AcquireCommand {
            scope: r.session.scope.clone(),
            queue: r.session.queue.clone(),
            worker_session_id: r.session.id.clone(),
            consumer_id: 0,
            sequence: 1,
        })
        .collect();
    (routes, commands)
}

#[derive(Default)]
struct SlowFirstFactory {
    constructed: std::sync::atomic::AtomicUsize,
    polled: Mutex<Vec<usize>>,
}
impl AcquisitionSource for SlowFirstFactory {
    fn acquire<'a>(
        &'a self,
        _: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        let index = self.constructed.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            std::thread::sleep(
                options.deadline.saturating_duration_since(Instant::now())
                    + Duration::from_millis(10),
            );
        }
        Box::pin(async move {
            self.polled.lock().unwrap().push(index);
            Ok(SourceReply::Idle)
        })
    }
}

#[tokio::test]
async fn slow_first_source_constructor_cannot_poll_its_expired_future_or_redraw() {
    use poll_plan::{PollPlan, PollPlanner, PollStrategy};
    let slow = Arc::new(SlowFirstFactory::default());
    let other = Arc::new(PendingSource::default());
    let scope = Scope {
        tenant_id: "t".into(),
        namespace: "n".into(),
    };
    let mut config = DeliveryConfig::new(scope.clone(), "a");
    config.request_timeout = Duration::from_millis(25);
    config.retry_delay = Duration::from_millis(1);
    let shared = Shared {
        stop: AtomicBool::new(false),
        stop_changed: tokio::sync::Notify::new(),
        status: Mutex::default(),
    };
    let planner = PollPlanner::new(
        PollPlan::new(1, vec![("a".into(), 1), ("b".into(), 1)]).unwrap(),
        PollStrategy::Stride,
    );
    let (routes, commands) = fixture(&scope, &["a", "b"], &[slow.clone(), other.clone()]);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        acquire(&routes, &commands, Some(&planner), &config, &shared),
    )
    .await
    .unwrap();
    assert!(matches!(result, Some((0, SourceReply::Idle))));
    assert_eq!(slow.constructed.load(Ordering::SeqCst), 2);
    assert_eq!(*slow.polled.lock().unwrap(), [1]);
    assert!(other.0.lock().unwrap().is_empty());
    assert_eq!(
        planner.issue(),
        Some(1),
        "retry cannot charge another route"
    );
}
