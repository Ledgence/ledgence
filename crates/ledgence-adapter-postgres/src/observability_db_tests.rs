//! Durable producer identity and publication outcomes against PostgreSQL.

use crate::{tests::*, *};
use ledgence_worker_api::TraceBridge;
use serde_json::json;
use std::{collections::BTreeMap, sync::Mutex};
use tracing::{Instrument, instrument::WithSubscriber};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

#[derive(Default)]
struct TraceProbe {
    parents: Mutex<Vec<Option<TraceContext>>>,
    links: Mutex<Vec<TraceContext>>,
    produced: Mutex<Vec<TraceContext>>,
}
fn transport() -> TraceContext {
    TraceContext {
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-2222222222222222-01".into(),
        tracestate: Some("transport=independent".into()),
    }
}
impl TraceBridge for TraceProbe {
    fn set_parent(&self, _: &tracing::Span, parent: Option<&TraceContext>) {
        self.parents.lock().unwrap().push(parent.cloned());
    }
    fn add_link(&self, _: &tracing::Span, context: &TraceContext) {
        self.links.lock().unwrap().push(context.clone());
    }
    fn context(&self, span: &tracing::Span) -> Option<TraceContext> {
        if span
            .metadata()
            .is_some_and(|meta| meta.name() == "ledgence.invocation.create")
        {
            let parents = self.parents.lock().unwrap();
            let mut produced = self.produced.lock().unwrap();
            // This asserts parent and link are available when a real bridge
            // would materialize the span and invoke its sampler.
            assert_eq!(parents.len(), produced.len() + 1);
            assert_eq!(self.links.lock().unwrap().len(), parents.len());
            let index = produced.len() + 1;
            let (trace_id, flags, state) = match parents.last().unwrap() {
                Some(parent) => (
                    parent.traceparent[3..35].to_owned(),
                    &parent.traceparent[53..55],
                    parent.tracestate.clone(),
                ),
                None => (format!("{index:032x}"), "01", None),
            };
            let created = TraceContext {
                traceparent: format!("00-{trace_id}-{index:016x}-{flags}"),
                tracestate: state,
            };
            produced.push(created.clone());
            Some(created)
        } else {
            Some(transport())
        }
    }
}

#[derive(Default)]
struct ProducerSpans {
    active: Mutex<BTreeMap<u64, BTreeMap<String, String>>>,
    ended: Mutex<Vec<BTreeMap<String, String>>>,
}
struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.to_owned());
    }
}
#[derive(Clone)]
struct Capture(Arc<ProducerSpans>);
impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        _: Context<'_, S>,
    ) {
        if attributes.metadata().name() == "ledgence.invocation.create" {
            let mut fields = BTreeMap::new();
            attributes.record(&mut Fields(&mut fields));
            self.0.active.lock().unwrap().insert(id.into_u64(), fields);
        }
    }
    fn on_record(&self, id: &tracing::Id, values: &tracing::span::Record<'_>, _: Context<'_, S>) {
        if let Some(fields) = self.0.active.lock().unwrap().get_mut(&id.into_u64()) {
            values.record(&mut Fields(fields));
        }
    }
    fn on_close(&self, id: tracing::Id, _: Context<'_, S>) {
        if let Some(fields) = self.0.active.lock().unwrap().remove(&id.into_u64()) {
            self.0.ended.lock().unwrap().push(fields);
        }
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn durable_producer_context_survives_replay_and_retries_use_the_accepted_origin() {
    let db = TestDb::new().await;
    let bridge = Arc::new(TraceProbe::default());
    let spans = Arc::new(ProducerSpans::default());
    let store = db.store.clone().with_trace_bridge(bridge.clone());
    async {
        for (index, flags) in [Some("01"), Some("00"), None].into_iter().enumerate() {
            let mut submission = command();
            submission.idempotency_key = format!("trace_{index}");
            submission.origin_trace = flags.map(|flags| TraceContext {
                traceparent: format!(
                    "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-{flags}"
                ),
                tracestate: Some("origin=accepted".into()),
            });
            let task = store
                .accept_resolved_submission(&submission, &descriptor())
                .await
                .unwrap();
            let session = store.open_session(&scope(), "python", 1).await.unwrap();
            let acquisition = acquire_command(&session, 0, 1);
            let first = assignment(store.acquire(&acquisition).await.unwrap());
            let first_context = TraceContext::from_event(&first.event).unwrap();
            assert_ne!(Some(&first_context), submission.origin_trace.as_ref());
            let count = bridge.produced.lock().unwrap().len();
            let replay = assignment(store.acquire(&acquisition).await.unwrap());
            assert_eq!(replay.event.value(), first.event.value());
            assert_eq!(
                bridge.produced.lock().unwrap().len(),
                count,
                "replay must not recreate a producer"
            );
            assert_eq!(
                store
                    .inspect(&scope(), &task.task_id)
                    .await
                    .unwrap()
                    .origin_trace,
                submission.origin_trace
            );
            store
                .renew(&RenewCommand {
                    owner: first.lease.owner.clone(),
                    sequence: 1,
                    intent: RenewIntent::Dispatch,
                })
                .await
                .unwrap();
            let mut failed = completed(&first, Quiescence::Confirmed, json!(null));
            let AttemptReport::Completed(report) = &mut failed.report else {
                unreachable!()
            };
            failed.report = AttemptReport::Failed(ledgence_worker_api::ExecutionFailure {
                context: report.context.clone(),
                error: ledgence_worker_api::Error::new(
                    ledgence_worker_api::ErrorKind::Runtime,
                    "first attempt process failed",
                ),
                cleanup_error: None,
                phase: ledgence_worker_api::Phase::Execution,
                execution_may_have_started: true,
            });
            assert_eq!(
                store.settle(&failed).await.unwrap().task_state,
                TaskState::Queued
            );
            let second = assignment(
                store
                    .acquire(&acquire_command(&session, 0, 2))
                    .await
                    .unwrap(),
            );
            let second_context = TraceContext::from_event(&second.event).unwrap();
            assert_ne!(first_context, second_context);
            if let Some(flags) = flags {
                assert_eq!(
                    &first_context.traceparent[3..35],
                    &second_context.traceparent[3..35]
                );
                assert!(first_context.traceparent.ends_with(flags));
                assert!(second_context.traceparent.ends_with(flags));
            } else {
                assert_ne!(
                    &first_context.traceparent[3..35],
                    &second_context.traceparent[3..35]
                );
            }
            {
                let parents = bridge.parents.lock().unwrap();
                assert_eq!(parents[2 * index], submission.origin_trace);
                assert_eq!(parents[2 * index + 1], submission.origin_trace);
            }
            store
                .settle(&completed(&second, Quiescence::Confirmed, json!("done")))
                .await
                .unwrap();
        }
    }
    .instrument(tracing::Span::none())
    .with_subscriber(tracing_subscriber::registry().with(Capture(spans.clone())))
    .await;
    assert!(
        bridge
            .links
            .lock()
            .unwrap()
            .iter()
            .all(|link| *link == transport())
    );
    {
        let ended = spans.ended.lock().unwrap();
        assert_eq!(ended.len(), 6);
        assert!(ended.iter().all(
            |fields| fields["ledgence.publication.outcome"] == "committed"
                && fields["otel.status_code"] == "OK"
        ));
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn failed_allocation_does_not_report_publication_or_reuse_its_context() {
    let db = TestDb::new().await;
    let bridge = Arc::new(TraceProbe::default());
    let spans = Arc::new(ProducerSpans::default());
    let store = db.store.clone().with_trace_bridge(bridge.clone());
    let task = store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = store.open_session(&scope(), "python", 1).await.unwrap();
    let acquisition = acquire_command(&session, 0, 1);
    sqlx::raw_sql("CREATE FUNCTION reject_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test allocation failure'; END $$; CREATE TRIGGER reject_attempt BEFORE INSERT ON attempts FOR EACH ROW EXECUTE FUNCTION reject_attempt()")
        .execute(&store.pool).await.unwrap();
    async {
        assert!(matches!(
            store.acquire(&acquisition).await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(
            store.inspect(&scope(), &task.task_id).await.unwrap().state,
            TaskState::Queued
        );
        sqlx::raw_sql("DROP TRIGGER reject_attempt ON attempts; DROP FUNCTION reject_attempt()")
            .execute(&store.pool)
            .await
            .unwrap();
        let accepted = assignment(store.acquire(&acquisition).await.unwrap());
        let produced = bridge.produced.lock().unwrap();
        assert_eq!(produced.len(), 2);
        assert_eq!(
            TraceContext::from_event(&accepted.event).as_ref(),
            Some(&produced[1])
        );
        assert_ne!(produced[0], produced[1]);
    }
    .with_subscriber(tracing_subscriber::registry().with(Capture(spans.clone())))
    .await;
    {
        let ended = spans.ended.lock().unwrap();
        assert_eq!(ended.len(), 2);
        assert_eq!(ended[0]["ledgence.publication.outcome"], "not_committed");
        assert_eq!(ended[0]["otel.status_code"], "ERROR");
        assert_eq!(ended[1]["ledgence.publication.outcome"], "committed");
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn lost_commit_acknowledgment_keeps_uncertainty_and_replay_preserves_the_creation_context() {
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgSslMode},
    };
    use std::{str::FromStr, sync::atomic::Ordering};

    let db = TestDb::new().await;
    let bridge = Arc::new(TraceProbe::default());
    let spans = Arc::new(ProducerSpans::default());
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let acquisition = acquire_command(&session, 0, 1);
    let direct = PgConnectOptions::from_str(&db.url).unwrap();
    let mut gate = crate::transaction_db_tests::BeginReplyGate::at_commit(
        direct.get_host().into(),
        direct.get_port(),
    )
    .await;
    let proxy_url = direct
        .host("127.0.0.1")
        .port(gate.address.port())
        .ssl_mode(PgSslMode::Disable)
        .to_url_lossy()
        .to_string();
    let store = PostgresStore::connect(&proxy_url, PostgresOptions::default())
        .await
        .unwrap()
        .with_trace_bridge(bridge.clone());
    gate.armed.store(true, Ordering::SeqCst);
    let pending_store = store.clone();
    let pending_command = acquisition.clone();
    let pending = tokio::spawn(
        async move { pending_store.acquire(&pending_command).await }
            .with_subscriber(tracing_subscriber::registry().with(Capture(spans.clone()))),
    );
    tokio::time::timeout(Duration::from_secs(10), &mut gate.reached)
        .await
        .unwrap()
        .unwrap();
    let persisted = db.store.inspect(&scope(), &task.task_id).await.unwrap();
    assert_eq!(
        persisted.state,
        TaskState::Active,
        "the database has committed before the reply is released"
    );
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    gate.release.notify_one();
    {
        let ended = spans.ended.lock().unwrap();
        assert_eq!(ended.len(), 1);
        assert_eq!(
            ended[0]["ledgence.publication.outcome"],
            "commit_unconfirmed"
        );
        assert_eq!(ended[0]["otel.status_code"], "ERROR");
    }
    let replay_store = db.store.clone().with_trace_bridge(bridge.clone());
    let replay = replay_store
        .acquire(&acquisition)
        .with_subscriber(tracing_subscriber::registry().with(Capture(spans.clone())))
        .await
        .unwrap();
    assert_eq!(
        TraceContext::from_event(&assignment(replay).event).as_ref(),
        bridge.produced.lock().unwrap().first()
    );
    assert_eq!(bridge.produced.lock().unwrap().len(), 1);
    assert_eq!(
        spans.ended.lock().unwrap().len(),
        1,
        "replay does not reconstruct historical producer spans"
    );
    drop(gate);
    store.close().await;
    db.finish().await;
}
