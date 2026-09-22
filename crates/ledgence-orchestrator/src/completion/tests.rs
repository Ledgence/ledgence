use super::*;
use serde_json::json;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, mpsc};

fn lease(destination: &str, index: usize) -> CompletionLease {
    let scope = Scope {
        tenant_id: "tenant".into(),
        namespace: "namespace".into(),
    };
    let target = CompletionTarget::Task {
        id: format!("task-{destination}-{index}"),
    };
    let event=CompletionEvent::new(json!({"specversion":"1.0","id":completion_event_id(&target),
        "source":"urn:ledgence:orchestrator","type":"com.ledgence.task.completed.v1",
        "subject":format!("tasks/{}",target.id()),"time":"2026-09-22T00:00:00Z",
        "ldgtenantid":"tenant","ldgnamespace":"namespace","ldgstate":"succeeded",
        "ldgtaskid":target.id(),"ldgrunid":"run-id","ldgresultref":completion_result_ref(&scope,&target)})).unwrap();
    CompletionLease {
        event_bytes: serde_json::to_vec(&event).unwrap(),
        lease_token: format!("lease-{destination}-{index}"),
        subscription: CompletionSubscription {
            subscription_id: format!("subscription-{destination}-{index}"),
            command: CompletionSubscribeCommand {
                scope,
                target,
                destination: destination.into(),
                idempotency_key: format!("key-{index}"),
            },
            state: CompletionState::Delivering,
            generation: 2,
            attempts: 1,
            total_attempts: 9,
            created_at: 1,
            activated_at: Some(2),
            next_attempt_at: None,
            lease_expires_at: Some(30002),
            delivered_at: None,
            exhausted_at: None,
            last_failure: None,
            event: Some(event),
        },
    }
}
struct Store {
    pending: Mutex<HashMap<String, VecDeque<CompletionLease>>>,
    calls: mpsc::UnboundedSender<(String, Instant)>,
    completed: mpsc::UnboundedSender<Vec<CompletionDeliveryResult>>,
    unavailable: Arc<AtomicBool>,
    fatal: Mutex<Option<(Arc<Notify>, bool)>>,
}
impl CompletionStore for Store {
    fn configure_completion_destination<'a>(
        &'a self,
        _: &'a CompletionDestination,
    ) -> ContractFuture<'a, ()> {
        Box::pin(async { unreachable!() })
    }
    fn subscribe_completion<'a>(
        &'a self,
        _: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async { unreachable!() })
    }
    fn completion_status<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async { unreachable!() })
    }
    fn retry_completion<'a>(
        &'a self,
        _: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async { unreachable!() })
    }
    fn lease_completions<'a>(
        &'a self,
        destination: &'a CompletionDestination,
        limit: u32,
        deadline: std::time::Instant,
    ) -> ContractFuture<'a, Vec<CompletionLease>> {
        assert_eq!(limit, PER_DESTINATION);
        Box::pin(async move {
            assert!(std::time::Instant::now() < deadline);
            self.calls
                .send((destination.destination.clone(), Instant::now()))
                .unwrap();
            if destination.destination == "unavailable" && self.unavailable.load(Ordering::Acquire)
            {
                return Err(super::unavailable("controlled storage failure"));
            }
            let fatal = if destination.destination == "fatal" {
                self.fatal.lock().unwrap().take()
            } else {
                None
            };
            if let Some((release, panic)) = fatal {
                release.notified().await;
                assert!(!panic, "controlled completion batch panic");
                return Err(ContractError::InvalidInput(
                    "controlled fatal store rejection".into(),
                ));
            }
            let mut pending = self.pending.lock().unwrap();
            let queue = pending.entry(destination.destination.clone()).or_default();
            Ok((0..limit).filter_map(|_| queue.pop_front()).collect())
        })
    }
    fn complete_deliveries<'a>(
        &'a self,
        completions: &'a [CompletionDeliveryResult],
        _: std::time::Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(async move {
            for completion in completions {
                completion.validate().unwrap();
                assert_eq!(
                    completion.generation, 2,
                    "fencing generation must be forwarded"
                );
                assert_eq!(
                    completion.lease_token,
                    completion
                        .subscription_id
                        .replacen("subscription-", "lease-", 1),
                    "fencing token must be forwarded unchanged"
                );
            }
            self.completed.send(completions.to_vec()).unwrap();
            Ok(())
        })
    }
}
struct Channels {
    calls: mpsc::UnboundedReceiver<(String, Instant)>,
    completed: mpsc::UnboundedReceiver<Vec<CompletionDeliveryResult>>,
}
fn store(queues: &[(&str, usize)]) -> (Arc<Store>, Channels) {
    let (calls, called) = mpsc::unbounded_channel();
    let (completed, received) = mpsc::unbounded_channel();
    (
        Arc::new(Store {
            pending: Mutex::new(
                queues
                    .iter()
                    .map(|(name, count)| {
                        (
                            name.to_string(),
                            (0..*count).map(|index| lease(name, index)).collect(),
                        )
                    })
                    .collect(),
            ),
            calls,
            completed,
            unavailable: Arc::new(AtomicBool::new(false)),
            fatal: Mutex::new(None),
        }),
        Channels {
            calls: called,
            completed: received,
        },
    )
}
struct Sender {
    wait: Option<Arc<Notify>>,
    started: mpsc::UnboundedSender<String>,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    fail: bool,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
impl CompletionSender for Sender {
    fn deliver<'a>(
        &'a self,
        lease: &'a CompletionLease,
        _: std::time::Instant,
    ) -> ContractFuture<'a, CompletionDeliveryOutcome> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.maximum.fetch_max(active, Ordering::AcqRel);
            let _active = Active(self.active.clone());
            self.started
                .send(lease.subscription.command.destination.clone())
                .unwrap();
            if let Some(wait) = &self.wait {
                wait.notified().await;
            }
            Ok(if self.fail {
                CompletionDeliveryOutcome::Retry {
                    reason: "http_503".into(),
                    retry_after_ms: None,
                }
            } else {
                CompletionDeliveryOutcome::Confirmed
            })
        })
    }
}
fn destination(name: &str, sender: Arc<dyn CompletionSender>) -> Destination {
    Destination {
        binding: CompletionDestination {
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            destination: name.into(),
            binding: "opaque".into(),
        },
        sender,
    }
}
fn sender(wait: Option<Arc<Notify>>, fail: bool) -> (Arc<Sender>, mpsc::UnboundedReceiver<String>) {
    let (started, received) = mpsc::unbounded_channel();
    (
        Arc::new(Sender {
            wait,
            fail,
            started,
            active: Arc::new(AtomicUsize::new(0)),
            maximum: Arc::new(AtomicUsize::new(0)),
        }),
        received,
    )
}
fn health() -> Health {
    let health = Health::new(Duration::from_secs(40));
    health.prerequisites_ready();
    health.recovery_success();
    health.require_completions();
    health
}

#[tokio::test(start_paused = true)]
async fn a_stalled_destination_does_not_block_fast_delivery_and_shutdown_drains_leased_work() {
    let (store, mut channels) = store(&[("slow", 2), ("fast", 6)]);
    let release = Arc::new(Notify::new());
    let (slow, mut slow_started) = sender(Some(release.clone()), false);
    let (fast, _fast_started) = sender(None, false);
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store,
        vec![destination("slow", slow.clone()), destination("fast", fast)],
        health(),
        stopped,
    ));
    slow_started.recv().await.unwrap();
    slow_started.recv().await.unwrap();
    let mut completions = Vec::new();
    for _ in 0..3 {
        completions.extend(channels.completed.recv().await.unwrap());
    }
    assert_eq!(completions.len(), 6);
    assert!(
        completions
            .iter()
            .all(|done| done.subscription_id.contains("fast"))
    );
    assert_eq!(slow.active.load(Ordering::Acquire), 2);
    stop.send(true).unwrap();
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "normal shutdown must retain in-flight sends"
    );
    release.notify_waiters();
    task.await.unwrap().unwrap();
    assert_eq!(channels.completed.recv().await.unwrap().len(), 2);
    assert_eq!(slow.active.load(Ordering::Acquire), 0);
    let calls: Vec<_> = std::iter::from_fn(|| channels.calls.try_recv().ok()).collect();
    let fast_calls: Vec<_> = calls
        .iter()
        .filter(|(destination, _)| destination == "fast")
        .collect();
    assert!(fast_calls.len() >= 3);
    assert_eq!(
        fast_calls[0].1, fast_calls[2].1,
        "full batches must catch up without a polling sleep"
    );
}

#[tokio::test(start_paused = true)]
async fn all_destinations_share_sixteen_sends_and_rotation_admits_waiting_destinations() {
    let names: Vec<_> = (0..16)
        .map(|index| format!("destination-{index}"))
        .collect();
    let queues: Vec<_> = names.iter().map(|name| (name.as_str(), 2)).collect();
    let (store, mut channels) = store(&queues);
    let release = Arc::new(Notify::new());
    let (sender, mut starts) = sender(Some(release.clone()), false);
    let destinations = names
        .iter()
        .map(|name| destination(name, sender.clone()))
        .collect();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(store, destinations, health(), stopped));
    let mut first = HashMap::new();
    for _ in 0..16 {
        *first.entry(starts.recv().await.unwrap()).or_insert(0) += 1;
    }
    assert_eq!(first.len(), 8);
    assert!(first.values().all(|count| *count == 2));
    assert_eq!(sender.active.load(Ordering::Acquire), 16);
    assert!(starts.try_recv().is_err());
    release.notify_waiters();
    let mut second = HashMap::new();
    for _ in 0..16 {
        *second.entry(starts.recv().await.unwrap()).or_insert(0) += 1;
    }
    assert_eq!(second.len(), 8);
    assert!(second.keys().all(|name| !first.contains_key(name)));
    assert!(sender.maximum.load(Ordering::Acquire) <= 16);
    stop.send(true).unwrap();
    release.notify_waiters();
    task.await.unwrap().unwrap();
    let mut count = 0;
    while let Ok(results) = channels.completed.try_recv() {
        count += results.len();
    }
    assert_eq!(count, 32);
}

#[tokio::test(start_paused = true)]
async fn receiver_timeouts_are_persisted_retries_and_do_not_degrade_service_readiness() {
    let (store, mut channels) = store(&[("slow", 2)]);
    let (sender, mut starts) = sender(Some(Arc::new(Notify::new())), false);
    let observed = health();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store,
        vec![destination("slow", sender)],
        observed.clone(),
        stopped,
    ));
    starts.recv().await.unwrap();
    starts.recv().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    let completed = channels.completed.recv().await.unwrap();
    assert!(
        completed
            .iter()
            .all(|done| matches!(done.outcome, CompletionDeliveryOutcome::Retry { .. }))
    );
    tokio::task::yield_now().await;
    assert_eq!(observed.readiness().0, axum::http::StatusCode::OK);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn another_destination_success_does_not_clear_a_storage_failure() {
    let (store, mut channels) = store(&[("unavailable", 0), ("fast", 2)]);
    store.unavailable.store(true, Ordering::Release);
    let (receiver, _starts) = sender(None, false);
    let observed = health();
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(run(
        store.clone(),
        vec![
            destination("unavailable", receiver.clone()),
            destination("fast", receiver),
        ],
        observed.clone(),
        stopped,
    ));
    channels.completed.recv().await.unwrap();
    tokio::task::yield_now().await;
    assert!(
        observed.readiness().1["reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("completion_unavailable"))
    );
    store.unavailable.store(false, Ordering::Release);
    tokio::time::advance(Duration::from_secs(1)).await;
    loop {
        if channels.calls.recv().await.unwrap().0 == "unavailable"
            && observed.readiness().0 == axum::http::StatusCode::OK
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_or_duplicate_leases_do_not_reach_a_receiver() {
    let (store, _channels) = store(&[("target", 2)]);
    {
        let mut pending = store.pending.lock().unwrap();
        let queue = pending.get_mut("target").unwrap();
        queue[1] = queue[0].clone();
    }
    let (receiver, mut starts) = sender(None, false);
    let result = batch(
        store.as_ref(),
        &destination("target", receiver),
        Instant::now() + BATCH_BUDGET,
    )
    .await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert!(starts.try_recv().is_err());
}

#[tokio::test]
async fn spawned_batches_retain_the_executable_tracing_subscriber() {
    struct TraceProbe;
    impl CompletionSender for TraceProbe {
        fn deliver<'a>(
            &'a self,
            _: &'a CompletionLease,
            _: std::time::Instant,
        ) -> ContractFuture<'a, CompletionDeliveryOutcome> {
            Box::pin(async {
                let span = tracing::info_span!("completion.test.transport");
                assert!(
                    !span.is_disabled(),
                    "spawned delivery lost its tracing subscriber"
                );
                Ok(CompletionDeliveryOutcome::Confirmed)
            })
        }
    }
    let (store, mut channels) = store(&[("traced", 1)]);
    let (stop, stopped) = watch::channel(false);
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::sink)
        .finish();
    let task = tokio::spawn(
        run(
            store,
            vec![destination("traced", Arc::new(TraceProbe))],
            health(),
            stopped,
        )
        .with_subscriber(subscriber),
    );
    assert_eq!(channels.completed.recv().await.unwrap().len(), 1);
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn successful_sibling_cannot_clear_fatal_health_while_other_batches_drain() {
    for panic in [false, true] {
        let (store, mut channels) = store(&[("fatal", 0), ("success", 1), ("blocked", 1)]);
        let fatal_release = Arc::new(Notify::new());
        *store.fatal.lock().unwrap() = Some((fatal_release.clone(), panic));
        let success_release = Arc::new(Notify::new());
        let blocked_release = Arc::new(Notify::new());
        let (success, mut success_started) = sender(Some(success_release.clone()), false);
        let (blocked, mut blocked_started) = sender(Some(blocked_release.clone()), false);
        let observed = health();
        observed.completion_success();
        let (_stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(
            store,
            vec![
                destination("fatal", success.clone()),
                destination("success", success),
                destination("blocked", blocked),
            ],
            observed.clone(),
            stopped,
        ));
        success_started.recv().await.unwrap();
        blocked_started.recv().await.unwrap();
        fatal_release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if observed.readiness().1["reasons"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("completion_failed"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        success_release.notify_one();
        let completed = channels.completed.recv().await.unwrap();
        assert_eq!(completed.len(), 1);
        assert!(completed[0].subscription_id.contains("success"));
        // Let JoinSet consume the successful batch while the third destination
        // remains held. Its success must not clear the already-fatal health.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!task.is_finished());
        assert!(
            observed.readiness().1["reasons"]
                .as_array()
                .unwrap()
                .contains(&json!("completion_failed"))
        );
        assert_eq!(
            observed.readiness().0,
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        blocked_release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            observed.readiness().0,
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
