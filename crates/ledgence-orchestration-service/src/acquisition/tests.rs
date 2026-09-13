use super::*;
use ledgence_worker_api::ProgramDescriptor;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct Store {
    calls: AtomicUsize,
    finalizations: AtomicUsize,
    wake_during_probe: Mutex<Option<Arc<Coordinator>>>,
    blocked: AtomicUsize,
    release: Notify,
    synchronous_delay: Mutex<Option<Duration>>,
}
impl TaskStore for Store {
    fn probe_acquisition<'a>(
        &'a self,
        command: &'a AcquireCommand,
        finish_empty: bool,
        _: Instant,
    ) -> ContractFuture<'a, AcquisitionProbe> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(delay) = *self.synchronous_delay.lock().unwrap() {
                std::thread::sleep(delay);
            }
            if let Some(wake) = self.wake_during_probe.lock().unwrap().take() {
                wake.wake(AcquisitionHint::QueueChanged(command.into()));
            }
            while self.blocked.load(Ordering::SeqCst) != 0 {
                self.release.notified().await;
            }
            if finish_empty {
                self.finalizations.fetch_add(1, Ordering::SeqCst);
                Ok(AcquisitionProbe::Completed {
                    reply: AcquireReply::Empty {
                        sequence: command.sequence,
                    },
                    kind: AcquisitionCompletion::FinalizedEmpty,
                })
            } else {
                Ok(AcquisitionProbe::Pending {
                    session_remaining_ms: 86_400_000,
                })
            }
        })
    }
    fn open_session<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        unused()
    }
    fn extend_session<'a>(&'a self, _: &'a str) -> ContractFuture<'a, WorkerSession> {
        unused()
    }
    fn lookup_submission<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, Option<TaskSnapshot>> {
        unused()
    }
    fn accept_resolved_submission<'a>(
        &'a self,
        _: &'a SubmitCommand,
        _: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, TaskSnapshot> {
        unused()
    }
    fn inspect<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        unused()
    }
    fn inspect_attempt<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        unused()
    }
    fn history<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        unused()
    }
    fn renew<'a>(&'a self, _: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        unused()
    }
    fn settle<'a>(&'a self, _: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        unused()
    }
    fn confirm_quiescence<'a>(&'a self, _: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        unused()
    }
    fn cancel<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskState> {
        unused()
    }
}
fn unused<T>() -> ContractFuture<'static, T> {
    Box::pin(async { panic!("unexpected non-acquisition call") })
}
fn command(consumer_id: u32) -> AcquireCommand {
    AcquireCommand {
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        },
        queue: "queue".into(),
        worker_session_id: "session".into(),
        consumer_id,
        sequence: 1,
    }
}
fn options(wait: u64) -> AcquireOptions {
    AcquireOptions::new(Duration::from_millis(wait), now() + Duration::from_secs(30)).unwrap()
}
fn spawn(
    coordinator: &Arc<Coordinator>,
    store: &Arc<Store>,
    command: AcquireCommand,
    options: AcquireOptions,
) -> tokio::task::JoinHandle<Result<AcquireReply>> {
    let coordinator = coordinator.clone();
    let store = store.clone();
    tokio::spawn(async move { coordinator.acquire(store.as_ref(), &command, options).await })
}
async fn settle() {
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
}
fn idle(coordinator: &Arc<Coordinator>, id: u32) -> Registration {
    let registration = Registration::new(coordinator.clone(), &command(id)).unwrap();
    registration.pending(registration.epoch());
    registration
}

#[tokio::test(start_paused = true)]
async fn idle_consumers_share_one_fallback_probe_and_release_all_resources() {
    let coordinator = Coordinator::new();
    let store = Arc::new(Store::default());
    let requests: Vec<_> = (0..32)
        .map(|id| spawn(&coordinator, &store, command(id), options(20_000)))
        .collect();
    settle().await;
    assert_eq!(store.calls.load(Ordering::SeqCst), 32);
    assert_eq!(coordinator.statistics().probes, 0);
    for second in 1..=3 {
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(store.calls.load(Ordering::SeqCst), 32 + second);
        assert_eq!(store.finalizations.load(Ordering::SeqCst), 0);
    }
    for request in requests {
        request.abort();
        let _ = request.await;
    }
    let statistics = coordinator.statistics();
    assert_eq!(
        (
            statistics.waiters,
            statistics.keys,
            statistics.queues,
            statistics.nominated,
            statistics.probes
        ),
        (0, 0, 0, 0, 0)
    );
}

#[tokio::test(start_paused = true)]
async fn hint_between_registration_probe_and_sleep_is_not_lost() {
    let coordinator = Coordinator::new();
    let store = Arc::new(Store::default());
    *store.wake_during_probe.lock().unwrap() = Some(coordinator.clone());
    let request = spawn(&coordinator, &store, command(0), options(20_000));
    settle().await;
    assert_eq!(store.calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.finalizations.load(Ordering::SeqCst), 0);
    request.abort();
    let _ = request.await;
}

#[tokio::test(start_paused = true)]
async fn deadline_always_finalizes_even_without_a_queue_hint() {
    let coordinator = Coordinator::new();
    let store = Arc::new(Store::default());
    let request = spawn(&coordinator, &store, command(0), options(500));
    settle().await;
    tokio::time::advance(Duration::from_millis(500)).await;
    assert!(matches!(
        request.await.unwrap().unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    assert_eq!(store.calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.finalizations.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn duplicate_completion_bypasses_parked_queue_and_keeps_physical_deadlines() {
    let coordinator = Coordinator::new();
    let store = Arc::new(Store::default());
    let waiting = spawn(&coordinator, &store, command(0), options(20_000));
    settle().await;
    let duplicate = spawn(&coordinator, &store, command(0), options(0));
    assert!(matches!(
        duplicate.await.unwrap().unwrap(),
        AcquireReply::Empty { .. }
    ));
    settle().await;
    // This scheduling fixture deliberately returns Pending on a nonfinal probe;
    // PostgreSQL tests verify the completed cursor replay itself.
    assert_eq!(store.calls.load(Ordering::SeqCst), 3);
    assert!(!waiting.is_finished());
    waiting.abort();
    let _ = waiting.await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_finalizes_accepted_waits_and_rejects_new_admission() {
    let coordinator = Coordinator::new();
    let store = Arc::new(Store::default());
    let requests: Vec<_> = (0..12)
        .map(|id| spawn(&coordinator, &store, command(id), options(20_000)))
        .collect();
    settle().await;
    coordinator.stop();
    for request in requests {
        assert!(matches!(
            request.await.unwrap().unwrap(),
            AcquireReply::Empty { .. }
        ));
    }
    assert_eq!(store.finalizations.load(Ordering::SeqCst), 12);
    assert!(matches!(
        coordinator
            .acquire(store.as_ref(), &command(90), options(0))
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(coordinator.statistics().waiters, 0);
}

#[tokio::test(start_paused = true)]
async fn stalled_probes_and_admission_consume_the_same_deadline_budget() {
    let coordinator = Coordinator::new();
    let store = Arc::new(Store::default());
    store.blocked.store(1, Ordering::SeqCst);
    let requests: Vec<_> = (0..16)
        .map(|id| spawn(&coordinator, &store, command(id), options(20_000)))
        .collect();
    settle().await;
    assert_eq!(store.calls.load(Ordering::SeqCst), MAX_PROBES);
    assert_eq!(coordinator.statistics().probes, MAX_PROBES);
    tokio::time::advance(Duration::from_secs(30)).await;
    for request in requests {
        assert!(matches!(
            request.await.unwrap(),
            Err(ContractError::Unavailable(_))
        ));
    }
    assert_eq!(store.finalizations.load(Ordering::SeqCst), 0);
    assert_eq!(coordinator.statistics().probes, 0);
    assert_eq!(coordinator.statistics().waiters, 0);
}

#[tokio::test(start_paused = true)]
async fn short_remaining_exchange_budget_skips_sleep_and_finalizes() {
    let coordinator = Coordinator::new();
    let store = Store::default();
    let options =
        AcquireOptions::new(Duration::from_secs(20), now() + Duration::from_secs(5)).unwrap();
    assert!(matches!(
        coordinator
            .acquire(&store, &command(0), options)
            .await
            .unwrap(),
        AcquireReply::Empty { .. }
    ));
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn duplicate_and_global_registration_caps_are_reclaimed_on_drop() {
    let coordinator = Coordinator::new();
    let first = idle(&coordinator, 0);
    let second = idle(&coordinator, 0);
    assert!(matches!(
        Registration::new(coordinator.clone(), &command(0)),
        Err(ContractError::Unavailable(_))
    ));
    drop(second);
    let replacement = idle(&coordinator, 0);
    let mut registrations: Vec<_> = (1..MAX_WAITERS - 1)
        .map(|id| idle(&coordinator, id as u32))
        .collect();
    assert_eq!(coordinator.statistics().waiters, MAX_WAITERS);
    assert!(matches!(
        Registration::new(coordinator.clone(), &command(99999)),
        Err(ContractError::Unavailable(_))
    ));
    registrations.clear();
    drop(first);
    drop(replacement);
    assert_eq!(
        (
            coordinator.statistics().waiters,
            coordinator.statistics().keys,
            coordinator.statistics().queues
        ),
        (0, 0, 0)
    );
}

#[tokio::test(start_paused = true)]
async fn pending_closes_epoch_and_older_success_cannot_reopen_burst() {
    let coordinator = Coordinator::new();
    let registrations: Vec<_> = (0..12).map(|id| idle(&coordinator, id)).collect();
    coordinator.wake(AcquisitionHint::QueueChanged((&command(0)).into()));
    let first_epoch = registrations[0].wait(now() + Duration::from_secs(20)).await;
    registrations[0].completed(first_epoch, AcquisitionCompletion::Claimed);
    assert_eq!(coordinator.statistics().nominated, 4);
    let second_epoch = registrations[1].wait(now() + Duration::from_secs(20)).await;
    let third_epoch = registrations[2].wait(now() + Duration::from_secs(20)).await;
    registrations[1].pending(second_epoch);
    assert_eq!(coordinator.statistics().nominated, 1);
    registrations[2].completed(third_epoch, AcquisitionCompletion::Claimed);
    assert_eq!(coordinator.statistics().nominated, 0);
    coordinator.wake(AcquisitionHint::QueueChanged((&command(0)).into()));
    assert_eq!(coordinator.statistics().nominated, 1);
}

#[tokio::test(start_paused = true)]
async fn queue_rotation_gives_cold_queue_a_turn_during_hot_backlog() {
    let coordinator = Coordinator::new();
    let hot: Vec<_> = (0..8).map(|id| idle(&coordinator, id)).collect();
    let mut cold_command = command(100);
    cold_command.queue = "cold".into();
    let cold = Registration::new(coordinator.clone(), &cold_command).unwrap();
    cold.pending(cold.epoch());
    coordinator.wake(AcquisitionHint::Rescan);
    assert_eq!(coordinator.statistics().nominated, 2);
    let epoch = cold.wait(now() + Duration::from_secs(20)).await;
    cold.pending(epoch);
    let epoch = hot[0].wait(now() + Duration::from_secs(20)).await;
    hot[0].completed(epoch, AcquisitionCompletion::Claimed);
    assert_eq!(coordinator.statistics().nominated, MAX_PROBES);
}

#[tokio::test]
async fn synchronous_adapter_completion_after_deadline_remains_uncertain() {
    let coordinator = Coordinator::new();
    let store = Store::default();
    *store.synchronous_delay.lock().unwrap() = Some(Duration::from_millis(50));
    let options = AcquireOptions::immediate(now() + Duration::from_millis(20));
    let result = coordinator.acquire(&store, &command(0), options).await;
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.finalizations.load(Ordering::SeqCst),
        1,
        "fixture completed a mutation; timeout must not invent its absence"
    );
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert_eq!(coordinator.statistics().probes, 0);
    assert_eq!(coordinator.statistics().waiters, 0);
}

#[tokio::test]
async fn expired_probe_budget_does_not_start_a_store_operation() {
    let coordinator = Coordinator::new();
    let store = Store::default();
    let result = coordinator.probe(&store, &command(0), true, now()).await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    assert_eq!(coordinator.statistics().probes, 0);
}

#[test]
fn unrelated_notification_flood_does_not_scan_registered_queue_rotation() {
    let coordinator = Coordinator::new();
    let registrations: Vec<_> = (0..128)
        .map(|id| {
            let mut input = command(id);
            input.queue = format!("registered-{id}");
            let entry = Registration::new(coordinator.clone(), &input).unwrap();
            entry.pending(entry.epoch());
            entry
        })
        .collect();
    let before = coordinator.state.lock().unwrap().queue_visits;
    for id in 0..10_000 {
        let mut foreign = command(id);
        foreign.queue = "another-replica-only".into();
        coordinator.wake(AcquisitionHint::QueueChanged((&foreign).into()));
        coordinator.wake(AcquisitionHint::AcquisitionCompleted((&foreign).into()));
    }
    let state = coordinator.state.lock().unwrap();
    assert_eq!(
        state.queue_visits, before,
        "unrelated hints must not amplify into scans of every local idle queue"
    );
    drop(state);
    assert_eq!(
        (
            coordinator.statistics().waiters,
            coordinator.statistics().keys,
            coordinator.statistics().queues
        ),
        (128, 128, 128)
    );
    assert_eq!(coordinator.statistics().nominated, 0);
    drop(registrations);
}
