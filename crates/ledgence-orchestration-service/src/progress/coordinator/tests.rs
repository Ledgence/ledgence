use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
enum Reply {
    Empty,
    Event,
    Terminal,
    Cleanup,
    Failure(ContractError),
    Malformed,
}
struct Reader {
    calls: AtomicUsize,
    reply: Mutex<Reply>,
    hold: Mutex<Option<Arc<Semaphore>>>,
}
impl Reader {
    fn new(reply: Reply) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            reply: Mutex::new(reply),
            hold: Mutex::new(None),
        })
    }
    fn block(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.hold.lock().unwrap() = Some(gate.clone());
        gate
    }
    fn release(&self, gate: &Semaphore, count: usize) {
        *self.hold.lock().unwrap() = None;
        gate.add_permits(count);
    }
}
impl ExecutionProgressReader for Reader {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move {
            assert_eq!(
                query.wait_ms, 0,
                "a store probe must never recursively wait"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.hold.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.acquire().await.unwrap().forget();
            }
            let reply = self.reply.lock().unwrap().clone();
            if let Reply::Failure(error) = reply {
                return Err(error);
            }
            let after = query.validate().unwrap();
            let sequence = after + u64::from(matches!(reply, Reply::Event | Reply::Cleanup));
            let mut page = ExecutionProgressPage {
                scope: query.scope.clone(),
                target: query.target.clone(),
                state: if matches!(reply, Reply::Terminal | Reply::Cleanup) {
                    "failed"
                } else {
                    "active"
                }
                .into(),
                terminal: matches!(reply, Reply::Terminal | Reply::Cleanup),
                high_watermark: ProgressSequence(sequence),
                events: if matches!(reply, Reply::Event | Reply::Cleanup) {
                    vec![ExecutionProgressEvent {
                        sequence: ProgressSequence(sequence),
                        at: 1,
                        reason: if matches!(reply, Reply::Cleanup) {
                            "cleanup_confirmed"
                        } else {
                            "claimed"
                        }
                        .into(),
                        activation_id: None,
                        attempt_id: Some("attempt_1".into()),
                    }]
                } else {
                    Vec::new()
                },
                next_cursor: query.cursor_for(sequence).unwrap(),
                has_more: false,
            };
            if matches!(reply, Reply::Malformed) {
                page.next_cursor = query.cursor_for(sequence + 1).unwrap();
            }
            Ok(page)
        })
    }
}
fn query(id: &str, wait_ms: u64) -> ExecutionProgressQuery {
    let mut query = ExecutionProgressQuery {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "demo".into(),
        },
        target: ExecutionProgressTarget::Task { id: id.into() },
        cursor: None,
        limit: 100,
        wait_ms,
    };
    query.cursor = Some(query.cursor_for(1).unwrap());
    query
}
fn spawn(
    coordinator: &Arc<Coordinator>,
    reader: &Arc<Reader>,
    query: ExecutionProgressQuery,
) -> tokio::task::JoinHandle<Result<ExecutionProgressPage>> {
    let coordinator = coordinator.clone();
    let reader = reader.clone();
    tokio::spawn(async move { coordinator.read(reader.as_ref(), &query).await })
}
async fn until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..500 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        predicate(),
        "condition did not become true without advancing time"
    );
}
fn assert_released(coordinator: &Coordinator) {
    let stats = coordinator.statistics();
    assert_eq!((stats.keys, stats.waiters, stats.probes), (0, 0, 0));
    assert_eq!(coordinator.permits.available_permits(), MAX_PROBES);
}

#[tokio::test(start_paused = true)]
async fn identical_observers_share_initial_and_periodic_reads_and_receive_the_same_commit() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let gate = reader.block();
    let observers: Vec<_> = (0..64)
        .map(|_| spawn(&coordinator, &reader, query("task", 5000)))
        .collect();
    until(|| coordinator.statistics().waiters == 64).await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    reader.release(&gate, 1);
    until(|| coordinator.statistics().probes == 0).await;
    tokio::time::advance(Duration::from_millis(999)).await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    until(|| reader.calls.load(Ordering::SeqCst) == 2).await;
    *reader.reply.lock().unwrap() = Reply::Event;
    tokio::time::advance(PROBE_INTERVAL).await;
    for observer in observers {
        let page = observer.await.unwrap().unwrap();
        assert_eq!(page.events[0].sequence.0, 2);
    }
    let stats = coordinator.statistics();
    assert_eq!(
        (stats.total_waits, stats.total_probes, stats.peak_probes),
        (64, 3, 1)
    );
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn subscriber_deadlines_are_independent_and_expiry_does_not_invent_an_event() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let short = spawn(&coordinator, &reader, query("task", 300));
    let long = spawn(&coordinator, &reader, query("task", 5000));
    until(|| coordinator.statistics().waiters == 2).await;
    tokio::time::advance(Duration::from_millis(300)).await;
    assert!(short.await.unwrap().unwrap().events.is_empty());
    assert!(!long.is_finished());
    assert_eq!(coordinator.statistics().waiters, 1);
    *reader.reply.lock().unwrap() = Reply::Event;
    tokio::time::advance(Duration::from_millis(700)).await;
    assert_eq!(long.await.unwrap().unwrap().events.len(), 1);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn cancelling_elected_reader_hands_probe_ownership_to_a_surviving_observer() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Event);
    let gate = reader.block();
    let first = spawn(&coordinator, &reader, query("task", 5000));
    until(|| reader.calls.load(Ordering::SeqCst) == 1).await;
    let survivor = spawn(&coordinator, &reader, query("task", 5000));
    until(|| coordinator.statistics().waiters == 2).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    until(|| reader.calls.load(Ordering::SeqCst) == 2).await;
    assert_eq!(coordinator.statistics().probes, 1);
    reader.release(&gate, 1);
    assert_eq!(survivor.await.unwrap().unwrap().events.len(), 1);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn expired_probe_owner_does_not_expire_a_longer_subscriber() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Event);
    let gate = reader.block();
    let short = spawn(&coordinator, &reader, query("task", 100));
    until(|| reader.calls.load(Ordering::SeqCst) == 1).await;
    let long = spawn(&coordinator, &reader, query("task", 5000));
    until(|| coordinator.statistics().waiters == 2).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(matches!(
        short.await.unwrap(),
        Err(ContractError::Unavailable(_))
    ));
    until(|| reader.calls.load(Ordering::SeqCst) == 2).await;
    reader.release(&gate, 1);
    assert_eq!(long.await.unwrap().unwrap().events.len(), 1);
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn cancelling_every_observer_drops_probes_and_leaves_no_background_polling() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let _gate = reader.block();
    let tasks: Vec<_> = (0..32)
        .map(|_| spawn(&coordinator, &reader, query("task", 20_000)))
        .collect();
    until(|| coordinator.statistics().waiters == 32).await;
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_released(&coordinator);
    let reads = reader.calls.load(Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), reads);
}

#[tokio::test(start_paused = true)]
async fn distinct_keys_share_the_global_probe_limit_and_release_queued_permits_on_cancel() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let _gate = reader.block();
    let tasks: Vec<_> = (0..12)
        .map(|n| spawn(&coordinator, &reader, query(&format!("task_{n}"), 20_000)))
        .collect();
    until(|| {
        coordinator.statistics().waiters == 12 && coordinator.statistics().probes == MAX_PROBES
    })
    .await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), MAX_PROBES);
    assert_eq!(coordinator.statistics().peak_probes, MAX_PROBES);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn failures_and_invalid_snapshots_reach_every_subscriber_without_false_empty_success() {
    for reply in [
        Reply::Failure(ContractError::NotFound),
        Reply::Failure(ContractError::Unavailable("database unavailable".into())),
        Reply::Malformed,
    ] {
        let coordinator = Coordinator::new();
        let reader = Reader::new(reply.clone());
        let gate = reader.block();
        let tasks: Vec<_> = (0..8)
            .map(|_| spawn(&coordinator, &reader, query("task", 5000)))
            .collect();
        until(|| coordinator.statistics().waiters == 8).await;
        reader.release(&gate, 1);
        for task in tasks {
            let error = task.await.unwrap().unwrap_err();
            match &reply {
                Reply::Failure(expected) => assert_eq!(&error, expected),
                _ => assert!(matches!(error, ContractError::Unavailable(_))),
            }
        }
        assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
        assert_released(&coordinator);
    }
}

#[tokio::test(start_paused = true)]
async fn backlog_and_terminal_return_immediately_but_a_new_request_rechecks_retirement() {
    for reply in [Reply::Event, Reply::Terminal] {
        let coordinator = Coordinator::new();
        let reader = Reader::new(reply);
        let started = Instant::now();
        let page = coordinator
            .read(reader.as_ref(), &query("task", 20_000))
            .await
            .unwrap();
        assert!(page.terminal || !page.events.is_empty());
        assert_eq!(Instant::now(), started);
        assert_released(&coordinator);
        *reader.reply.lock().unwrap() = Reply::Failure(ContractError::NotFound);
        assert_eq!(
            coordinator
                .read(reader.as_ref(), &query("task", 20_000))
                .await
                .unwrap_err(),
            ContractError::NotFound
        );
        assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
        assert_released(&coordinator);
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_both_idle_waiters_and_an_inflight_authoritative_probe() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let idle = spawn(&coordinator, &reader, query("idle", 20_000));
    until(|| reader.calls.load(Ordering::SeqCst) == 1 && coordinator.statistics().probes == 0)
        .await;
    let _gate = reader.block();
    let probing = spawn(&coordinator, &reader, query("probing", 20_000));
    until(|| coordinator.statistics().probes == 1).await;
    coordinator.stop();
    for task in [idle, probing] {
        assert!(matches!(
            task.await.unwrap(),
            Err(ContractError::Unavailable(_))
        ));
    }
    assert_released(&coordinator);
    assert!(matches!(
        coordinator
            .read(reader.as_ref(), &query("new", 20_000))
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn key_and_subscriber_capacity_rejections_leave_no_registration_or_probe_leak() {
    let coordinator = Coordinator::new();
    let mut registrations = Vec::new();
    for n in 0..MAX_KEYS {
        registrations.push(
            Registration::new(
                coordinator.clone(),
                Key::new(&query(&format!("task_{n}"), 5000)).unwrap(),
            )
            .unwrap(),
        );
    }
    assert!(
        Registration::new(
            coordinator.clone(),
            Key::new(&query("overflow", 5000)).unwrap()
        )
        .is_err()
    );
    assert_eq!(coordinator.statistics().keys, MAX_KEYS);
    drop(registrations);
    assert_released(&coordinator);
    let key = Key::new(&query("same", 5000)).unwrap();
    let registrations: Vec<_> = (0..MAX_WAITERS_PER_KEY)
        .map(|_| Registration::new(coordinator.clone(), key.clone()).unwrap())
        .collect();
    assert!(Registration::new(coordinator.clone(), key).is_err());
    assert_eq!(coordinator.statistics().waiters, MAX_WAITERS_PER_KEY);
    drop(registrations);
    assert_released(&coordinator);
    let registrations: Vec<_> = (0..MAX_WAITERS)
        .map(|n| {
            Registration::new(
                coordinator.clone(),
                Key::new(&query(&format!("group_{}", n / MAX_WAITERS_PER_KEY), 5000)).unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert!(
        Registration::new(
            coordinator.clone(),
            Key::new(&query("overflow", 5000)).unwrap()
        )
        .is_err()
    );
    assert_eq!(coordinator.statistics().waiters, MAX_WAITERS);
    drop(registrations);
    assert_released(&coordinator);
}

#[test]
fn only_wait_duration_and_the_equivalent_zero_cursor_are_excluded_from_identity() {
    let mut query = query("task", 5000);
    let original = Key::new(&query).unwrap();
    query.wait_ms = 100;
    assert_eq!(Key::new(&query).unwrap(), original);
    query.limit = 50;
    assert_ne!(Key::new(&query).unwrap(), original);
    query.limit = 100;
    query.cursor = Some(query.cursor_for(2).unwrap());
    assert_ne!(Key::new(&query).unwrap(), original);
    query.cursor = None;
    let without_cursor = Key::new(&query).unwrap();
    query.cursor = Some(query.cursor_for(0).unwrap());
    assert_eq!(Key::new(&query).unwrap(), without_cursor);
    query.cursor = None;
    query.scope.namespace = "different".into();
    assert_ne!(Key::new(&query).unwrap(), without_cursor);
    query.scope.namespace = "demo".into();
    query.target = ExecutionProgressTarget::Workflow { id: "task".into() };
    assert_ne!(Key::new(&query).unwrap(), without_cursor);
}

#[tokio::test(start_paused = true)]
async fn a_retired_target_during_an_idle_wait_is_not_reported_as_an_empty_page() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let task = spawn(&coordinator, &reader, query("task", 5000));
    until(|| reader.calls.load(Ordering::SeqCst) == 1 && coordinator.statistics().probes == 0)
        .await;
    *reader.reply.lock().unwrap() = Reply::Failure(ContractError::NotFound);
    tokio::time::advance(PROBE_INTERVAL).await;
    assert_eq!(task.await.unwrap().unwrap_err(), ContractError::NotFound);
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn resuming_a_terminal_cursor_can_read_a_late_cleanup_record() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Terminal);
    let mut query = query("task", 20_000);
    let terminal = coordinator.read(reader.as_ref(), &query).await.unwrap();
    assert!(terminal.terminal);
    query.cursor = Some(terminal.next_cursor);
    *reader.reply.lock().unwrap() = Reply::Cleanup;
    let page = coordinator.read(reader.as_ref(), &query).await.unwrap();
    assert_eq!(page.events[0].sequence.0, 2);
    assert!(page.terminal);
    assert_eq!(page.events[0].reason, "cleanup_confirmed");
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn oversized_adapter_errors_are_not_retained_or_copied_to_subscribers() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Failure(ContractError::Unavailable(
        "x".repeat(MAX_ERROR_BYTES + 1),
    )));
    assert_eq!(
        coordinator
            .read(reader.as_ref(), &query("task", 1000))
            .await
            .unwrap_err(),
        ContractError::Unavailable("execution progress adapter error exceeds limit".into())
    );
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn a_late_registration_cannot_return_the_existing_observers_pre_retirement_snapshot() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let original = spawn(&coordinator, &reader, query("task", 5000));
    until(|| reader.calls.load(Ordering::SeqCst) == 1 && coordinator.statistics().probes == 0)
        .await;
    tokio::time::advance(Duration::from_millis(100)).await;
    *reader.reply.lock().unwrap() = Reply::Failure(ContractError::NotFound);
    tokio::time::advance(Duration::from_millis(100)).await;
    let short = spawn(&coordinator, &reader, query("task", 100));
    let long = spawn(&coordinator, &reader, query("task", 2000));
    until(|| coordinator.statistics().waiters == 3).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(matches!(
        short.await.unwrap(),
        Err(ContractError::Unavailable(_))
    ));
    assert!(!original.is_finished());
    assert!(!long.is_finished());
    assert_eq!(
        reader.calls.load(Ordering::SeqCst),
        1,
        "late arrivals must not force a probe storm"
    );
    tokio::time::advance(Duration::from_millis(700)).await;
    for task in [original, long] {
        assert_eq!(task.await.unwrap().unwrap_err(), ContractError::NotFound);
    }
    assert_eq!(reader.calls.load(Ordering::SeqCst), 2);
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn observers_admitted_while_a_probe_is_queued_share_its_actual_later_start() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Event);
    let occupied = coordinator
        .permits
        .acquire_many(MAX_PROBES as u32)
        .await
        .unwrap();
    let first = spawn(&coordinator, &reader, query("task", 5000));
    until(|| coordinator.statistics().waiters == 1).await;
    let second = spawn(&coordinator, &reader, query("task", 5000));
    until(|| coordinator.statistics().waiters == 2).await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    let before = Instant::now();
    drop(occupied);
    for task in [first, second] {
        assert_eq!(task.await.unwrap().unwrap().events.len(), 1);
    }
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        Instant::now(),
        before,
        "generation must start at the read, not probe selection"
    );
    assert_released(&coordinator);
}

#[tokio::test(start_paused = true)]
async fn short_late_registration_during_an_older_inflight_read_cannot_use_its_result() {
    let coordinator = Coordinator::new();
    let reader = Reader::new(Reply::Empty);
    let gate = reader.block();
    let original = spawn(&coordinator, &reader, query("task", 5000));
    until(|| reader.calls.load(Ordering::SeqCst) == 1).await;
    let later = spawn(&coordinator, &reader, query("task", 100));
    until(|| coordinator.statistics().waiters == 2).await;
    reader.release(&gate, 1);
    until(|| coordinator.statistics().probes == 0).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(matches!(
        later.await.unwrap(),
        Err(ContractError::Unavailable(_))
    ));
    assert!(!original.is_finished());
    original.abort();
    assert!(original.await.unwrap_err().is_cancelled());
    assert_released(&coordinator);
}
