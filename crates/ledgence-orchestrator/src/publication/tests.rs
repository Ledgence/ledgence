use super::*;
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, mpsc};

struct Store {
    batches: Mutex<VecDeque<Result<Vec<PublicationLease>>>>,
    completed: Mutex<Vec<Vec<PublicationCompletion>>>,
    calls: mpsc::UnboundedSender<Instant>,
}
impl DispatchIntentStore for Store {
    fn configure_route<'a>(&'a self, _: &'a DispatchRoute) -> ContractFuture<'a, ()> {
        panic!("not used")
    }
    fn lease_publications<'a>(
        &'a self,
        destination: &'a str,
        limit: u32,
        _: std::time::Instant,
    ) -> ContractFuture<'a, Vec<PublicationLease>> {
        assert_eq!(destination, "primary");
        assert_eq!(limit, 10);
        Box::pin(async move {
            self.calls.send(Instant::now()).unwrap();
            self.batches
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(vec![]))
        })
    }
    fn complete_publications<'a>(
        &'a self,
        completions: &'a [PublicationCompletion],
        _: std::time::Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(async move {
            self.completed.lock().unwrap().push(completions.to_vec());
            Ok(())
        })
    }
}
struct Publisher {
    replies: Mutex<VecDeque<Result<Vec<PublishResult>>>>,
    gate: Mutex<Option<Arc<Notify>>>,
    entered: Notify,
    calls: AtomicUsize,
}
impl DispatchPublisher for Publisher {
    fn limits(&self) -> QueueLimits {
        QueueLimits {
            max_publish_batch: 10,
            max_receive_batch: 10,
            max_ack_batch: 10,
            max_message_bytes: DISPATCH_MAX_BYTES,
        }
    }
    fn publish<'a>(
        &'a self,
        records: &'a [PublishedDispatch],
        _: std::time::Instant,
    ) -> ContractFuture<'a, Vec<PublishResult>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let gate = self.gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            self.replies.lock().unwrap().pop_front().unwrap_or_else(|| {
                Ok(records
                    .iter()
                    .map(|record| PublishResult {
                        publication_id: record.publication_id.clone(),
                        outcome: PublicationOutcome::Confirmed,
                    })
                    .collect())
            })
        })
    }
}
fn route() -> DispatchRoute {
    DispatchRoute {
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "billing".into(),
        },
        queue: "queue".into(),
        destination: "primary".into(),
    }
}
fn leases(count: usize) -> Vec<PublicationLease> {
    (0..count)
        .map(|index| PublicationLease {
            record: PublishedDispatch {
                dispatch: DispatchRef {
                    scope: route().scope,
                    queue: route().queue,
                    task_id: format!("task_{index}"),
                    generation: 1,
                },
                publication_id: format!("publication_{index}"),
            },
            destination: "primary".into(),
            lease_token: format!("lease_{index}"),
        })
        .collect()
}
fn publisher(replies: Vec<Result<Vec<PublishResult>>>) -> Arc<Publisher> {
    Arc::new(Publisher {
        replies: Mutex::new(replies.into()),
        gate: Mutex::new(None),
        entered: Notify::new(),
        calls: AtomicUsize::new(0),
    })
}
fn store(
    batches: Vec<Result<Vec<PublicationLease>>>,
) -> (Arc<Store>, mpsc::UnboundedReceiver<Instant>) {
    let (calls, receiver) = mpsc::unbounded_channel();
    (
        Arc::new(Store {
            batches: Mutex::new(batches.into()),
            completed: Mutex::new(vec![]),
            calls,
        }),
        receiver,
    )
}
fn health() -> Health {
    let health = Health::new(Duration::from_secs(40));
    health.require_publication();
    health.prerequisites_ready();
    health.recovery_success();
    health
}
fn successful_probe() -> ConfigurationProbe {
    Arc::new(|| Box::pin(async { Ok(()) }))
}

#[tokio::test]
async fn exact_publication_results_match_by_identity_and_complete_original_leases() {
    let (store, _calls) = store(vec![Ok(leases(2))]);
    let publisher = publisher(vec![Ok(vec![
        PublishResult {
            publication_id: "publication_1".into(),
            outcome: PublicationOutcome::Retry,
        },
        PublishResult {
            publication_id: "publication_0".into(),
            outcome: PublicationOutcome::Confirmed,
        },
    ])]);
    let result = batch(
        store.as_ref(),
        publisher.as_ref(),
        &route(),
        10,
        Instant::now() + BATCH_BUDGET,
    )
    .await
    .unwrap();
    assert!(result.failed);
    let completed = store.completed.lock().unwrap();
    assert_eq!(completed[0][0].outcome, PublicationOutcome::Confirmed);
    assert_eq!(completed[0][1].outcome, PublicationOutcome::Retry);
    for (completion, original) in completed[0].iter().zip(leases(2)) {
        assert_eq!(completion.dispatch, original.record.dispatch);
        assert_eq!(completion.publication_id, original.record.publication_id);
        assert_eq!(completion.lease_token, original.lease_token);
    }
}

#[tokio::test]
async fn malformed_or_uncertain_publish_never_becomes_confirmation() {
    for result in [
        Ok(vec![]),
        Ok(vec![PublishResult {
            publication_id: "publication_0".into(),
            outcome: PublicationOutcome::Confirmed,
        }]),
        Ok(vec![
            PublishResult {
                publication_id: "publication_0".into(),
                outcome: PublicationOutcome::Confirmed
            };
            2
        ]),
        Ok(vec![
            PublishResult {
                publication_id: "other".into(),
                outcome: PublicationOutcome::Confirmed
            };
            2
        ]),
        Err(unavailable("send may have succeeded")),
    ] {
        let (store, _calls) = store(vec![Ok(leases(2))]);
        let publisher = publisher(vec![result]);
        let result = batch(
            store.as_ref(),
            publisher.as_ref(),
            &route(),
            10,
            Instant::now() + BATCH_BUDGET,
        )
        .await
        .unwrap();
        assert!(result.failed);
        assert!(
            store.completed.lock().unwrap()[0]
                .iter()
                .all(|c| c.outcome == PublicationOutcome::Retry)
        );
    }
}

#[tokio::test]
async fn invalid_store_leases_are_not_published_or_completed() {
    let mut wrong_route = leases(1);
    wrong_route[0].record.dispatch.queue = "another".into();
    let mut duplicate = leases(1);
    duplicate.push(duplicate[0].clone());
    for leases in [leases(11), wrong_route, duplicate] {
        let (store, _calls) = store(vec![Ok(leases)]);
        let publisher = publisher(vec![]);
        assert!(matches!(
            batch(
                store.as_ref(),
                publisher.as_ref(),
                &route(),
                10,
                Instant::now() + BATCH_BUDGET
            )
            .await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(publisher.calls.load(Ordering::SeqCst), 0);
        assert!(store.completed.lock().unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn full_batches_drain_immediately_partial_batches_wait_and_shutdown_finishes_send() {
    let (store, mut calls) = store(vec![Ok(leases(10)), Ok(leases(10)), Ok(leases(1))]);
    let publisher = publisher(vec![]);
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store,
        publisher,
        route(),
        successful_probe(),
        health(),
        stopped,
    ));
    let first = calls.recv().await.unwrap();
    assert_eq!(calls.recv().await.unwrap(), first);
    assert_eq!(calls.recv().await.unwrap(), first);
    assert_eq!(calls.recv().await.unwrap() - first, IDLE_INTERVAL);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();

    let (store, mut calls) = self::store(vec![Ok(leases(1))]);
    let publisher = self::publisher(vec![]);
    let gate = Arc::new(Notify::new());
    *publisher.gate.lock().unwrap() = Some(gate.clone());
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store.clone(),
        publisher.clone(),
        route(),
        successful_probe(),
        health(),
        stopped,
    ));
    publisher.entered.notified().await;
    stop.send(true).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    gate.notify_one();
    task.await.unwrap().unwrap();
    assert_eq!(store.completed.lock().unwrap().len(), 1);
    calls.recv().await.unwrap();
    assert!(calls.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn empty_batches_do_not_clear_broker_failure_until_bounded_positive_probe() {
    let (store, mut calls) = store(vec![Ok(leases(1))]);
    let publisher = publisher(vec![Err(unavailable("broker offline"))]);
    let checked = Arc::new(AtomicUsize::new(0));
    let observed = checked.clone();
    let probe: ConfigurationProbe = Arc::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    });
    let observed_health = health();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store,
        publisher,
        route(),
        probe,
        observed_health.clone(),
        stopped,
    ));
    calls.recv().await.unwrap();
    calls.recv().await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(checked.load(Ordering::SeqCst), 0);
    assert!(
        observed_health.readiness().1["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "publication_unavailable")
    );
    calls.recv().await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(checked.load(Ordering::SeqCst), 1);
    assert_eq!(observed_health.readiness().0, axum::http::StatusCode::OK);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn send_timeout_retains_lease_and_closed_stop_channel_exits() {
    let (store, mut calls) = store(vec![Ok(leases(1))]);
    let publisher = publisher(vec![]);
    *publisher.gate.lock().unwrap() = Some(Arc::new(Notify::new()));
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store.clone(),
        publisher.clone(),
        route(),
        successful_probe(),
        health(),
        stopped,
    ));
    publisher.entered.notified().await;
    let first = calls.recv().await.unwrap();
    assert_eq!(
        calls.recv().await.unwrap() - first,
        BATCH_BUDGET + INITIAL_BACKOFF
    );
    assert!(store.completed.lock().unwrap().is_empty());
    drop(stop);
    task.await.unwrap().unwrap();
}
