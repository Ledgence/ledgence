use ledgence_worker_api::*;
use ledgence_worker_core::*;
use serde_json::json;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};

#[path = "lifecycle/interactive.rs"]
mod interactive;

#[derive(Default)]
struct Counts {
    fetches: AtomicUsize,
    starts: AtomicUsize,
    live: AtomicUsize,
    peak: AtomicUsize,
    executions: AtomicUsize,
    close_calls: AtomicUsize,
    close_completions: AtomicUsize,
    implicit_drops: AtomicUsize,
    close_failures: AtomicUsize,
    execute_panics: AtomicUsize,
    close_panics: AtomicUsize,
    start_panics: AtomicUsize,
    execute_gate: std::sync::Mutex<Option<Arc<CloseGate>>>,
    startup_cleanup_required: AtomicUsize,
    close_gate: std::sync::Mutex<Option<Arc<CloseGate>>>,
}
struct CloseGate {
    entered: Semaphore,
    release: Semaphore,
}
impl CloseGate {
    fn install(counts: &Counts) -> Arc<Self> {
        let gate = Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        *counts.close_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    async fn wait_until_entered(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    fn finish(&self, counts: &Counts) {
        *counts.close_gate.lock().unwrap() = None;
        self.release.add_permits(1);
    }
}
struct Store(Arc<Counts>);
impl ProgramStore for Store {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async { unreachable!("assignments are already bound") })
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async {
            self.0.fetches.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok(vec![1])
        })
    }
}
#[derive(Default)]
struct Cache {
    entries: Mutex<HashMap<Digest, PreparedArtifact>>,
    publish_failure: std::sync::Mutex<Option<Error>>,
}
impl ArtifactCache for Cache {
    fn lookup<'a>(&'a self, d: &'a ProgramDescriptor) -> PortFuture<'a, Option<PreparedArtifact>> {
        Box::pin(async { Ok(self.entries.lock().await.get(&d.digest).cloned()) })
    }
    fn publish<'a>(
        &'a self,
        d: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        Box::pin(async {
            if let Some(error) = self.publish_failure.lock().unwrap().clone() {
                return Err(error);
            }
            let artifact = PreparedArtifact::new(
                PathBuf::from("/unused-test-artifact"),
                manifest(d.program.clone()),
                d.digest.clone(),
                Arc::new(()),
            );
            self.entries
                .lock()
                .await
                .insert(d.digest.clone(), artifact.clone());
            Ok(artifact)
        })
    }
}
struct Runtime(Arc<Counts>);
impl ExecutionRuntime for Runtime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            if self
                .0
                .start_panics
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                panic!("injected startup panic before returning a cleanup handle");
            }
            control.check()?;
            let pid = self.0.starts.fetch_add(1, Ordering::SeqCst) + 1;
            let live = self.0.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak.fetch_max(live, Ordering::SeqCst);
            let session: Box<dyn ExecutionSession> = Box::new(Session {
                counts: self.0.clone(),
                pid: pid as u32,
                alive: true,
                _artifact: artifact,
            });
            if self
                .0
                .startup_cleanup_required
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Ok(StartOutcome::CleanupRequired {
                    error: Error::new(ErrorKind::Runtime, "startup failed; retirement unconfirmed"),
                    session,
                });
            }
            Ok(StartOutcome::Ready(session))
        })
    }
}
struct Session {
    counts: Arc<Counts>,
    pid: u32,
    alive: bool,
    _artifact: PreparedArtifact,
}
impl Session {
    fn stop(&mut self) {
        if self.alive {
            self.alive = false;
            self.counts.live.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if self.alive {
            self.counts.implicit_drops.fetch_add(1, Ordering::SeqCst);
        }
        self.stop();
    }
}
impl ExecutionSession for Session {
    fn pid(&self) -> u32 {
        self.pid
    }
    fn execute<'a>(
        &'a mut self,
        invocation: ledgence_worker_api::RuntimeInvocation,
        control: RunControl,
    ) -> PortFuture<'a, ProgramOutcome> {
        let event = invocation.event;
        Box::pin(async move {
            self.counts.executions.fetch_add(1, Ordering::SeqCst);
            if event.value()["data"]["hold_execution"] == true {
                let gate = self.counts.execute_gate.lock().unwrap().clone().unwrap();
                gate.entered.add_permits(1);
                loop {
                    control.check()?;
                    tokio::select! {
                        permit = gate.release.acquire() => {
                            permit.unwrap().forget();
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                    }
                }
            }
            if self
                .counts
                .execute_panics
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                let gate = self.counts.execute_gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    gate.entered.add_permits(1);
                    gate.release.acquire().await.unwrap().forget();
                }
                panic!("injected execution adapter panic after dispatch");
            }
            let delay = event.value()["data"]["delay_ms"].as_u64().unwrap_or(0);
            let until = std::time::Instant::now() + Duration::from_millis(delay);
            loop {
                control.check()?;
                if std::time::Instant::now() >= until {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            if event.value()["data"]["crash"] == true {
                return Err(Error::new(ErrorKind::Runtime, "lost response"));
            }
            Ok(ProgramOutcome::Success {
                output: event.into_value(),
            })
        })
    }
    fn close(&mut self) -> PortFuture<'_, ()> {
        Box::pin(async {
            self.counts.close_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .counts
                .close_panics
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                panic!("injected cleanup panic while session remains owned");
            }
            let gate = self.counts.close_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.entered.add_permits(1);
                gate.release.acquire().await.unwrap().forget();
            }
            if self
                .counts
                .close_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(Error::new(ErrorKind::Io, "retirement unconfirmed"));
            }
            self.stop();
            self.counts.close_completions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}
fn manifest(program: ProgramRef) -> ProgramManifest {
    ProgramManifest {
        schema_version: 1,
        program,
        runtime: PythonRuntime {
            kind: "python".into(),
            python: "3.12".into(),
            protocol: 1,
        },
        handler: "app:handle".into(),
        platform: Platform {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
        },
    }
}
fn request(id: usize, version: usize, delay: u64) -> ExecutionRequest {
    let event = CloudEvent::new(json!({
        "specversion":"1.0", "id":format!("evt_{id}"), "source":"urn:test", "type":"com.ledgence.task.invocation.requested.v1", "datacontenttype":"application/json",
        "ldgtenantid":"tenant_a", "ldgnamespace":"demo", "ldgrunid":"run_1", "ldgtaskid":format!("task_{id}"), "ldgattemptid":format!("att_{id}"), "ldgattemptno":1,
        "traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "data":{"delay_ms":delay, "business_id":"INV-7", "nested":[1,{"opaque":true}]}
    })).unwrap();
    ExecutionRequest {
        descriptor: ProgramDescriptor {
            program: ProgramRef {
                id: "test".into(),
                version: version.to_string(),
            },
            digest: Digest(format!("sha256:{version:064x}")),
            size: 1,
        },
        event,
    }
}
fn setup(n: usize) -> (Worker, Arc<Counts>) {
    setup_with_cache(n, Arc::new(Cache::default()))
}
fn setup_with_cache(n: usize, cache: Arc<Cache>) -> (Worker, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let worker = Worker::new(
        WorkerConfig {
            concurrency: n,
            fetch_timeout: Duration::from_secs(2),
        },
        Arc::new(Store(counts.clone())),
        cache,
        Arc::new(Runtime(counts.clone())),
    )
    .unwrap();
    (worker, counts)
}
fn control() -> RunControl {
    RunControl::new(Duration::from_secs(5))
}
async fn wait_for(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cached_artifact_and_healthy_process_are_reused_without_touching_user_data() {
    let (worker, counts) = setup(1);
    let first = worker.execute(request(1, 1, 0), control()).await.unwrap();
    let next = request(2, 1, 0);
    let original = next.event.value().clone();
    let second = worker.execute(next, control()).await.unwrap();
    assert_eq!(first.process_id, second.process_id);
    assert!(second.reused_process);
    assert_eq!(second.outcome, ProgramOutcome::Success { output: original });
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_programs_share_one_global_process_limit_and_coalesce_downloads() {
    let (worker, counts) = setup(3);
    let mut tasks = Vec::new();
    for id in 0..18 {
        let worker = worker.clone();
        tasks.push(tokio::spawn(async move {
            worker.execute(request(id, 1 + id % 2, 15), control()).await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 2);
    assert!(counts.peak.load(Ordering::SeqCst) <= 3);
    assert_eq!(worker.stats().await.active_consumers, 0);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_fetch_owner_does_not_cancel_other_waiter_or_dispatch_owner_later() {
    let (worker, counts) = setup(2);
    let cancelled = control();
    let first = {
        let worker = worker.clone();
        let c = cancelled.clone();
        tokio::spawn(async move { worker.execute(request(1, 1, 0), c).await })
    };
    wait_for(|| counts.fetches.load(Ordering::SeqCst) == 1).await;
    let second = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(2, 1, 0), control()).await })
    };
    cancelled.cancel();
    let failure = first.await.unwrap().unwrap_err();
    assert_eq!(failure.error.kind, ErrorKind::Cancelled);
    assert!(!failure.execution_may_have_started);
    second.await.unwrap().unwrap();
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_caller_keeps_supervision_until_cancelled_process_is_retired() {
    let (worker, counts) = setup(1);
    let task = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(1, 1, 5000), control()).await })
    };
    wait_for(|| counts.starts.load(Ordering::SeqCst) == 1).await;
    task.abort();
    let _ = task.await;
    wait_for(|| counts.live.load(Ordering::SeqCst) == 0).await;
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.process_slots, 0);
}

#[tokio::test]
async fn cancelled_start_reservation_is_released_and_process_can_be_replaced() {
    let (worker, counts) = setup(1);
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let cancelled = control();
    cancelled.cancel();
    assert!(worker.execute(request(2, 2, 0), cancelled).await.is_err());
    worker.execute(request(3, 2, 0), control()).await.unwrap();
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn retirement_failure_keeps_slot_occupied_until_shutdown_reconciles_it() {
    let (worker, counts) = setup(1);
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    counts.close_failures.store(1, Ordering::SeqCst);
    let error = worker
        .execute(request(2, 2, 0), control())
        .await
        .unwrap_err();
    assert_eq!(error.phase, Phase::Startup);
    assert!(!error.execution_may_have_started);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.process_slots, 1);
    assert_eq!(
        worker
            .execute(request(3, 2, 0), control())
            .await
            .unwrap_err()
            .error
            .kind,
        ErrorKind::Capacity
    );
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unknown_execution_is_reported_once_and_never_silently_retried() {
    let (worker, counts) = setup(1);
    let mut r = request(1, 1, 0);
    let mut event = r.event.into_value();
    event["data"]["crash"] = true.into();
    r.event = CloudEvent::new(event).unwrap();
    let error = worker.execute(r, control()).await.unwrap_err();
    assert!(error.execution_may_have_started);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_stops_admission_cancels_work_and_reaps_processes() {
    let (worker, counts) = setup(1);
    let task = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(1, 1, 5000), control()).await })
    };
    wait_for(|| counts.starts.load(Ordering::SeqCst) == 1).await;
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        task.await.unwrap().unwrap_err().error.kind,
        ErrorKind::Cancelled
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert!(!worker.stats().await.accepting);
    assert_eq!(
        worker
            .execute(request(2, 1, 0), control())
            .await
            .unwrap_err()
            .phase,
        Phase::Admission
    );
}

#[tokio::test]
async fn cancellation_during_replacement_retirement_releases_the_reserved_slot() {
    let (worker, counts) = setup(1);
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let gate = CloseGate::install(&counts);
    let cancelled = control();
    let replacement = {
        let worker = worker.clone();
        let control = cancelled.clone();
        tokio::spawn(async move { worker.execute(request(2, 2, 0), control).await })
    };
    gate.wait_until_entered().await;
    assert_eq!(worker.stats().await.process_slots, 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    cancelled.cancel();
    gate.finish(&counts);
    let failure = replacement.await.unwrap().unwrap_err();
    assert_eq!(failure.phase, Phase::Startup);
    assert_eq!(failure.error.kind, ErrorKind::Cancelled);
    assert!(!failure.execution_may_have_started);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    worker.execute(request(3, 2, 0), control()).await.unwrap();
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_active_attempt_is_rejected_before_any_second_dispatch() {
    let (worker, counts) = setup(2);
    let first_control = control();
    let original = request(1, 1, 5000);
    let first = {
        let worker = worker.clone();
        let control = first_control.clone();
        let request = original.clone();
        tokio::spawn(async move { worker.execute(request, control).await })
    };
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    let mut replay = original;
    let mut envelope = replay.event.into_value();
    envelope["id"] = "another-delivery-for-the-same-attempt".into();
    replay.event = CloudEvent::new(envelope).unwrap();
    let rejected = worker.execute(replay, control()).await.unwrap_err();
    assert_eq!(rejected.phase, Phase::Admission);
    assert_eq!(rejected.error.kind, ErrorKind::InvalidInput);
    assert!(!rejected.execution_may_have_started);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    first_control.cancel();
    assert_eq!(
        first.await.unwrap().unwrap_err().error.kind,
        ErrorKind::Cancelled
    );
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_shutdown_caller_preserves_the_in_progress_retirement() {
    let (worker, counts) = setup(1);
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let gate = CloseGate::install(&counts);
    let shutdown = {
        let worker = worker.clone();
        tokio::spawn(async move {
            worker
                .shutdown(Duration::ZERO, Duration::from_secs(2))
                .await
        })
    };
    gate.wait_until_entered().await;
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());
    assert_eq!(worker.stats().await.process_slots, 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    gate.finish(&counts);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(counts.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.process_slots, 0);
}

#[tokio::test]
async fn dropping_unbounded_shutdown_caller_preserves_the_same_retirement() {
    let (worker, counts) = setup(1);
    worker.execute(request(1, 1, 0), control()).await.unwrap();
    let gate = CloseGate::install(&counts);
    let shutdown = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.shutdown_until_quiescent().await })
    };
    gate.wait_until_entered().await;
    shutdown.abort();
    assert!(shutdown.await.unwrap_err().is_cancelled());

    // A second caller waits behind the retained cleanup supervisor. Its own
    // observation timeouts cannot restart or drop the session being retired.
    let observer = worker.shutdown_until_quiescent();
    tokio::pin!(observer);
    for _ in 0..2 {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut observer)
                .await
                .is_err()
        );
        let stats = worker.stats().await;
        assert!(!stats.accepting);
        assert_eq!(stats.process_slots, 1);
        assert_eq!(counts.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.close_completions.load(Ordering::SeqCst), 0);
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
        assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    }

    gate.finish(&counts);
    tokio::time::timeout(Duration::from_secs(2), &mut observer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.process_slots, 0);
}

#[tokio::test]
async fn invalid_artifact_is_not_treated_as_cache_pressure_and_keeps_warm_session() {
    let cache = Arc::new(Cache::default());
    let (worker, counts) = setup_with_cache(1, cache.clone());
    let first = worker.execute(request(1, 1, 0), control()).await.unwrap();
    *cache.publish_failure.lock().unwrap() = Some(Error::new(
        ErrorKind::InvalidInput,
        "archive digest mismatch",
    ));
    let failure = worker
        .execute(request(2, 2, 0), control())
        .await
        .unwrap_err();
    assert_eq!(failure.phase, Phase::Preparation);
    assert_eq!(failure.error.kind, ErrorKind::InvalidInput);
    assert!(!failure.execution_may_have_started);
    assert_eq!(counts.close_calls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.warm_processes, 1);
    let reused = worker.execute(request(3, 1, 0), control()).await.unwrap();
    assert_eq!(reused.process_id, first.process_id);
    assert!(reused.reused_process);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unconfirmed_startup_cleanup_retains_capacity_and_is_reconciled_by_shutdown() {
    let (worker, counts) = setup(1);
    counts.startup_cleanup_required.store(1, Ordering::SeqCst);
    let failed = worker
        .execute(request(1, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(failed.phase, Phase::Startup);
    assert!(!failed.execution_may_have_started);
    assert_eq!(worker.stats().await.process_slots, 1);
    assert_eq!(worker.stats().await.warm_processes, 0);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    let blocked = worker
        .execute(request(2, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(blocked.error.kind, ErrorKind::Capacity);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.process_slots, 0);
}

struct BlockingFetch {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}
impl ProgramStore for BlockingFetch {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async { unreachable!() })
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        let live = self.live.clone();
        let peak = self.peak.clone();
        let gate = self.gate.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let count = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                let (lock, condition) = &*gate;
                let released = lock.lock().unwrap();
                let _guard = condition
                    .wait_timeout_while(released, Duration::from_secs(3), |released| !*released)
                    .unwrap();
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(vec![1])
            })
            .await
            .unwrap()
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_blocking_fetch_retains_admission_and_shutdown_ownership_until_finished() {
    let counts = Arc::new(Counts::default());
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            fetch_timeout: Duration::from_millis(40),
        },
        Arc::new(BlockingFetch {
            live: live.clone(),
            peak: peak.clone(),
            gate: gate.clone(),
        }),
        Arc::new(Cache::default()),
        Arc::new(Runtime(counts.clone())),
    )
    .unwrap();
    let invocation = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(1, 1, 0), control()).await })
    };
    wait_for(|| live.load(Ordering::SeqCst) == 1).await;
    let timed_out = tokio::time::timeout(Duration::from_millis(500), invocation)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(timed_out.error.kind, ErrorKind::TimedOut);
    assert_eq!(timed_out.phase, Phase::Preparation);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(
        worker
            .execute(request(1, 1, 0), control())
            .await
            .unwrap_err()
            .error
            .kind,
        ErrorKind::InvalidInput
    );
    let waiting = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(2, 1, 0), control()).await })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(
        worker
            .shutdown(Duration::ZERO, Duration::from_millis(30))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TimedOut
    );
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(
        waiting.await.unwrap().unwrap_err().error.kind,
        ErrorKind::Cancelled
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(
        counts.starts.load(Ordering::SeqCst),
        0,
        "a reported fetch timeout must never dispatch later"
    );
}

#[tokio::test]
async fn detached_execution_panic_closes_admission_and_retires_the_owned_session() {
    let (worker, counts) = setup(2);
    let gate = Arc::new(CloseGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    *counts.execute_gate.lock().unwrap() = Some(gate.clone());
    counts.execute_panics.store(1, Ordering::SeqCst);
    let caller = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(1, 1, 0), control()).await })
    };
    gate.wait_until_entered().await;
    caller.abort();
    let _ = caller.await;
    gate.release.add_permits(1);
    wait_for(|| counts.close_completions.load(Ordering::SeqCst) == 1).await;
    assert!(!worker.stats().await.accepting);
    assert!(worker.execute(request(1, 1, 0), control()).await.is_err());
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn panicking_cleanup_keeps_original_failure_and_recoverable_session() {
    let (worker, counts) = setup(1);
    counts.close_panics.store(1, Ordering::SeqCst);
    let mut invocation = request(1, 1, 0);
    let mut event = invocation.event.into_value();
    event["data"]["crash"] = true.into();
    invocation.event = CloudEvent::new(event).unwrap();
    let failure = worker.execute(invocation, control()).await.unwrap_err();
    assert_eq!(failure.phase, Phase::Execution);
    assert_eq!(failure.error.message, "lost response");
    assert!(
        failure
            .cleanup_error
            .as_ref()
            .unwrap()
            .message
            .contains("cleanup panic")
    );
    assert!(!worker.stats().await.accepting);
    assert_eq!(worker.stats().await.process_slots, 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn panic_before_start_returns_a_handle_keeps_an_explicit_unresolved_reservation() {
    let (worker, counts) = setup(1);
    counts.start_panics.store(1, Ordering::SeqCst);
    let failure = worker
        .execute(request(1, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(failure.phase, Phase::Startup);
    assert_eq!(failure.error.kind, ErrorKind::Runtime);
    assert!(!worker.stats().await.accepting);
    assert_eq!(worker.stats().await.process_slots, 1);
    assert!(
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .is_err()
    );
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn free_slots_keep_mixed_programs_warm_for_later_reuse() {
    let (worker, counts) = setup(2);
    let first = worker.execute(request(1, 1, 0), control()).await.unwrap();
    worker.execute(request(2, 2, 0), control()).await.unwrap();
    let third = worker.execute(request(3, 1, 0), control()).await.unwrap();
    assert_eq!(counts.starts.load(Ordering::SeqCst), 2);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 2);
    assert_eq!(worker.stats().await.warm_processes, 2);
    assert_eq!(first.process_id, third.process_id);
    assert!(third.reused_process);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

fn assert_flat_context(value: &serde_json::Value, request: &ExecutionRequest) {
    let identity = serde_json::to_value(InvocationIdentity::from(&request.event)).unwrap();
    for (key, expected) in identity.as_object().unwrap() {
        assert_eq!(
            &value[key], expected,
            "missing or changed identity field {key}"
        );
    }
    assert_eq!(
        value["program"],
        serde_json::to_value(&request.descriptor.program).unwrap()
    );
    assert_eq!(
        value["digest"],
        serde_json::to_value(&request.descriptor.digest).unwrap()
    );
    assert!(value.get("context").is_none());
}
#[tokio::test]
async fn reports_and_failures_keep_complete_scoped_identity() {
    let (worker, counts) = setup(1);
    let invocation = request(1, 1, 0);
    let report = worker.execute(invocation.clone(), control()).await.unwrap();
    assert_flat_context(&serde_json::to_value(report).unwrap(), &invocation);
    let rejected = control();
    rejected.cancel();
    let admission = worker
        .execute(invocation.clone(), rejected)
        .await
        .unwrap_err();
    assert_flat_context(&serde_json::to_value(admission).unwrap(), &invocation);
    counts.close_failures.store(1, Ordering::SeqCst);
    let mut crashing = request(2, 1, 0);
    let mut value = crashing.event.into_value();
    value["source"] = "urn:other-source".into();
    value["ldgrunid"] = "different-run".into();
    value["data"]["crash"] = true.into();
    crashing.event = CloudEvent::new(value).unwrap();
    let failure = worker
        .execute(crashing.clone(), control())
        .await
        .unwrap_err();
    assert_eq!(failure.error.message, "lost response");
    assert_eq!(
        failure.cleanup_error.as_ref().unwrap().message,
        "retirement unconfirmed"
    );
    assert_flat_context(&serde_json::to_value(failure).unwrap(), &crashing);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn delivery_reservation_spans_acquisition_execution_and_pending_settlement() {
    let (worker, counts) = setup(1);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    assert!(reservation.is_quiescent());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            worker.reserve_consumer(control())
        )
        .await
        .is_err(),
        "acquisition owns the only consumer before any local execution"
    );
    let invocation = request(1, 1, 0);
    let first = reservation
        .execute(invocation.clone(), control())
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(worker.stats().await.warm_processes, 1);
    wait_for(|| reservation.is_quiescent()).await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    let reused_reservation = reservation
        .execute(request(2, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(reused_reservation.phase, Phase::Admission);
    assert_eq!(reused_reservation.error.kind, ErrorKind::InvalidInput);
    let duplicate = worker.execute(invocation, control()).await.unwrap_err();
    assert_eq!(duplicate.error.kind, ErrorKind::InvalidInput);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);

    let waiting = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.execute(request(3, 1, 0), control()).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !waiting.is_finished(),
        "a delivered result is not settlement"
    );
    reservation.release();
    let next = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next.process_id, first.process_id);
    assert!(next.reused_process);
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn reservation_shutdown_waits_for_external_owner_to_resolve_and_release() {
    let (worker, counts) = setup(1);
    let reservation = worker.reserve_consumer(control()).await.unwrap();
    assert_eq!(
        worker
            .shutdown(Duration::ZERO, Duration::from_millis(30))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TimedOut
    );
    assert!(reservation.is_cancellation_requested());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert!(matches!(
        worker.reserve_consumer(control()).await,
        Err(Error {
            kind: ErrorKind::Unavailable,
            ..
        })
    ));
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropped_reservation_caller_retains_the_consumer_until_process_cleanup_finishes() {
    let (worker, counts) = setup(1);
    let gate = CloseGate::install(&counts);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let caller =
        tokio::spawn(async move { reservation.execute(request(1, 1, 5000), control()).await });
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    gate.wait_until_entered().await;
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            worker.reserve_consumer(control())
        )
        .await
        .is_err()
    );
    assert_eq!(
        worker
            .shutdown(Duration::ZERO, Duration::from_millis(30))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TimedOut
    );
    gate.finish(&counts);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropped_execution_future_does_not_make_its_delivery_reservation_reusable() {
    let (worker, counts) = setup(1);
    let gate = CloseGate::install(&counts);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    {
        let execution = reservation.execute(request(1, 1, 5000), control());
        tokio::pin!(execution);
        tokio::select! {
            result = &mut execution => panic!("invocation finished before cancellation: {result:?}"),
            _ = wait_for(|| counts.executions.load(Ordering::SeqCst) == 1) => {},
        }
    }
    gate.wait_until_entered().await;
    assert!(!reservation.is_quiescent());
    let reused = reservation
        .execute(request(2, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(reused.phase, Phase::Admission);
    assert_eq!(reused.error.kind, ErrorKind::InvalidInput);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    gate.finish(&counts);
    wait_for(|| counts.close_completions.load(Ordering::SeqCst) == 1).await;
    wait_for(|| reservation.is_quiescent()).await;
    assert_eq!(worker.stats().await.active_consumers, 1);
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_delivery_reservation_still_owns_a_timed_out_blocking_preparation() {
    let counts = Arc::new(Counts::default());
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            fetch_timeout: Duration::from_millis(40),
        },
        Arc::new(BlockingFetch {
            live: live.clone(),
            peak: peak.clone(),
            gate: gate.clone(),
        }),
        Arc::new(Cache::default()),
        Arc::new(Runtime(counts.clone())),
    )
    .unwrap();
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let execution_control = control();
    let caller_control = execution_control.clone();
    let caller = tokio::spawn(async move {
        let result = reservation.execute(request(1, 1, 0), caller_control).await;
        (result, reservation)
    });
    wait_for(|| live.load(Ordering::SeqCst) == 1).await;
    let (result, reservation) = tokio::time::timeout(Duration::from_millis(500), caller)
        .await
        .unwrap()
        .unwrap();
    let failed = result.unwrap_err();
    assert_eq!(failed.phase, Phase::Preparation);
    assert_eq!(failed.error.kind, ErrorKind::TimedOut);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    assert!(!reservation.is_quiescent());
    assert!(!execution_control.is_cancelled());
    reservation.release();
    assert!(
        execution_control.is_cancelled(),
        "an early report does not detach unfinished work"
    );
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            worker.reserve_consumer(control())
        )
        .await
        .is_err()
    );
    assert_eq!(
        worker
            .shutdown(Duration::ZERO, Duration::from_millis(30))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::TimedOut
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unconfirmed_delivery_cleanup_retains_consumer_and_shutdown_can_reconcile_it() {
    let (worker, counts) = setup(1);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    counts.close_failures.store(1, Ordering::SeqCst);
    let mut invocation = request(1, 1, 0);
    let mut value = invocation.event.into_value();
    value["data"]["crash"] = true.into();
    invocation.event = CloudEvent::new(value).unwrap();
    let execution_control = control();
    let failure = reservation
        .execute(invocation, execution_control.clone())
        .await
        .unwrap_err();
    assert_eq!(failure.phase, Phase::Execution);
    assert!(failure.cleanup_error.is_some());
    assert!(!reservation.is_quiescent());
    assert!(!execution_control.is_cancelled());
    reservation.release();
    assert!(
        execution_control.is_cancelled(),
        "quarantined cleanup keeps its cancellation control"
    );
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(worker.stats().await.process_slots, 1);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            worker.reserve_consumer(control())
        )
        .await
        .is_err(),
        "unconfirmed retirement keeps the same consumer permit"
    );
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_reserved_start_retains_capacity_until_its_returned_handle_is_closed() {
    let (worker, counts) = setup(1);
    counts.startup_cleanup_required.store(1, Ordering::SeqCst);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let failure = reservation
        .execute(request(1, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(failure.phase, Phase::Startup);
    assert!(!failure.execution_may_have_started);
    assert!(!reservation.is_quiescent());
    reservation.release();
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_or_dropping_admission_waiters_does_not_lose_a_consumer_permit() {
    let (worker, counts) = setup(1);
    let reserved = worker.reserve_consumer(control()).await.unwrap();
    let cancellation = control();
    let waiting = worker.reserve_consumer(cancellation.clone());
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    cancellation.cancel();
    assert!(matches!(
        waiting.await,
        Err(Error {
            kind: ErrorKind::Cancelled,
            ..
        })
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            worker.reserve_consumer(control())
        )
        .await
        .is_err()
    );
    reserved.release();
    let recovered =
        tokio::time::timeout(Duration::from_secs(1), worker.reserve_consumer(control()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 1);
    recovered.release();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_reconciles_quarantine_before_delivery_owner_releases() {
    for failed_start in [false, true] {
        let (worker, counts) = setup(1);
        let mut reservation = worker.reserve_consumer(control()).await.unwrap();
        let mut invocation = request(1, 1, 0);
        if failed_start {
            counts.startup_cleanup_required.store(1, Ordering::SeqCst);
        } else {
            counts.close_failures.store(1, Ordering::SeqCst);
            let mut event = invocation.event.into_value();
            event["data"]["crash"] = true.into();
            invocation.event = CloudEvent::new(event).unwrap();
        }
        let failure = reservation
            .execute(invocation, control())
            .await
            .unwrap_err();
        assert_eq!(
            failure.phase,
            if failed_start {
                Phase::Startup
            } else {
                Phase::Execution
            }
        );
        assert!(!reservation.is_quiescent());
        let mut shutdown = {
            let worker = worker.clone();
            tokio::spawn(async move {
                worker
                    .shutdown(Duration::ZERO, Duration::from_secs(5))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !reservation.is_quiescent() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("cleanup must progress while its delivery owner observes it");
        assert!(reservation.is_cancellation_requested());
        assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
        assert_eq!(
            counts.close_calls.load(Ordering::SeqCst),
            if failed_start { 1 } else { 2 }
        );
        assert_eq!(worker.stats().await.process_slots, 0);
        assert_eq!(worker.stats().await.active_consumers, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
                .await
                .is_err(),
            "local cleanup does not resolve the delivery owner's external settlement"
        );
        reservation.release();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(worker.stats().await.active_consumers, 0);
        assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
        assert!(counts.peak.load(Ordering::SeqCst) <= 1);
    }
}

#[tokio::test]
async fn shutdown_reconciles_quarantine_published_after_its_first_pool_scan() {
    let (worker, counts) = setup(1);
    let gate = CloseGate::install(&counts);
    counts.close_failures.store(1, Ordering::SeqCst);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let caller = tokio::spawn(async move {
        let result = reservation.execute(request(1, 1, 5_000), control()).await;
        (result, reservation)
    });
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    let mut shutdown = {
        let worker = worker.clone();
        tokio::spawn(async move {
            worker
                .shutdown(Duration::ZERO, Duration::from_secs(5))
                .await
        })
    };
    gate.wait_until_entered().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(worker.stats().await.warm_processes, 0);
    assert_eq!(counts.close_calls.load(Ordering::SeqCst), 1);
    // Shutdown is already waiting while the still-supervised close is held.
    // Its first failure publishes quarantine only after this barrier opens.
    gate.finish(&counts);
    let (result, reservation) = caller.await.unwrap();
    let failure = result.unwrap_err();
    assert_eq!(failure.error.kind, ErrorKind::Cancelled);
    assert!(failure.cleanup_error.is_some());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !reservation.is_quiescent() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the same shutdown must drain newly published quarantine");
    assert_eq!(counts.close_calls.load(Ordering::SeqCst), 2);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    reservation.release();
    tokio::time::timeout(Duration::from_secs(1), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn completed_delivery_release_preserves_shared_control_for_running_peer() {
    for failed_execution in [false, true] {
        let (worker, counts) = setup(2);
        let shared = control();
        let mut completed = worker.reserve_consumer(control()).await.unwrap();
        let mut pending = worker.reserve_consumer(control()).await.unwrap();
        let gate = Arc::new(CloseGate {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        *counts.execute_gate.lock().unwrap() = Some(gate.clone());
        let mut invocation = request(2, 1, 0);
        let mut event = invocation.event.into_value();
        event["data"]["hold_execution"] = true.into();
        invocation.event = CloudEvent::new(event).unwrap();
        let peer = {
            let shared = shared.clone();
            tokio::spawn(async move {
                let result = pending.execute(invocation, shared).await;
                (result, pending)
            })
        };
        gate.wait_until_entered().await;
        let mut invocation = request(1, 1, 0);
        if failed_execution {
            let mut event = invocation.event.into_value();
            event["data"]["crash"] = true.into();
            invocation.event = CloudEvent::new(event).unwrap();
        }
        let result = completed.execute(invocation, shared.clone()).await;
        if failed_execution {
            let failure = result.unwrap_err();
            assert_eq!(failure.phase, Phase::Execution);
            assert!(failure.cleanup_error.is_none());
            assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
        } else {
            result.unwrap();
        }
        wait_for(|| completed.is_quiescent()).await;
        assert!(!shared.is_cancelled());
        completed.release();
        assert!(
            !shared.is_cancelled(),
            "releasing finished work must not cancel its shared control"
        );
        assert!(
            !peer.is_finished(),
            "the peer is still held at its execution barrier"
        );
        *counts.execute_gate.lock().unwrap() = None;
        gate.release.add_permits(1);
        let (result, pending) = peer.await.unwrap();
        result.unwrap();
        wait_for(|| pending.is_quiescent()).await;
        pending.release();
        assert!(!shared.is_cancelled());
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(worker.stats().await.active_consumers, 0);
        assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
        assert!(counts.peak.load(Ordering::SeqCst) <= 2);
    }
}

// The bridge remembers one portable context per live tracing span, so assertions
// compare span identity rather than depending on operation call order.
#[derive(Clone, Debug)]
struct RecordedTraceSpan {
    name: &'static str,
    fields: serde_json::Map<String, serde_json::Value>,
    parent: Option<Option<TraceContext>>,
    context: TraceContext,
}
#[derive(Default)]
struct TraceProbeState {
    next_span: u64,
    live: HashMap<tracing::Id, RecordedTraceSpan>,
    closed: Vec<RecordedTraceSpan>,
}
#[derive(Default)]
struct TraceProbe(std::sync::Mutex<TraceProbeState>);
impl TraceProbe {
    fn closed(&self, name: &str) -> Vec<RecordedTraceSpan> {
        self.0
            .lock()
            .unwrap()
            .closed
            .iter()
            .filter(|span| span.name == name)
            .cloned()
            .collect()
    }
    fn attempt(&self, attempt_id: &str) -> RecordedTraceSpan {
        self.closed("ledgence.attempt.process")
            .into_iter()
            .find(|span| span.fields["ledgence.attempt.id"] == attempt_id)
            .expect("the attempt span must close")
    }
}
impl TraceBridge for TraceProbe {
    fn set_parent(&self, span: &tracing::Span, parent: Option<&TraceContext>) {
        let Some(id) = span.id() else { return };
        let mut state = self.0.lock().unwrap();
        let recorded = state.live.get_mut(&id).expect("span must be registered");
        recorded.parent = Some(parent.cloned());
        if let Some(parent) = parent {
            parent.validate().unwrap();
            recorded.context.traceparent = format!(
                "00-{}-{}-{}",
                &parent.traceparent[3..35],
                &recorded.context.traceparent[36..52],
                &parent.traceparent[53..55],
            );
            recorded.context.tracestate = parent.tracestate.clone();
        }
    }
    fn add_link(&self, _: &tracing::Span, _: &TraceContext) {}
    fn context(&self, span: &tracing::Span) -> Option<TraceContext> {
        let id = span.id()?;
        self.0
            .lock()
            .unwrap()
            .live
            .get(&id)
            .map(|span| span.context.clone())
    }
}
struct TraceFields<'a>(&'a mut serde_json::Map<String, serde_json::Value>);
impl tracing::field::Visit for TraceFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
}
struct TraceCapture(Arc<TraceProbe>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for TraceCapture {
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut state = self.0.0.lock().unwrap();
        state.next_span += 1;
        let sequence = state.next_span;
        let mut fields = serde_json::Map::new();
        attributes.record(&mut TraceFields(&mut fields));
        state.live.insert(
            id.clone(),
            RecordedTraceSpan {
                name: attributes.metadata().name(),
                fields,
                parent: None,
                context: TraceContext {
                    traceparent: format!("00-{sequence:032x}-{sequence:016x}-01"),
                    tracestate: None,
                },
            },
        );
    }
    fn on_record(
        &self,
        id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if let Some(span) = self.0.0.lock().unwrap().live.get_mut(id) {
            values.record(&mut TraceFields(&mut span.fields));
        }
    }
    fn on_close(&self, id: tracing::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut state = self.0.0.lock().unwrap();
        let span = state.live.remove(&id).expect("span must be registered");
        state.closed.push(span);
    }
}
async fn with_trace_probe<F: std::future::Future<Output = ()>>(
    test: impl FnOnce(Arc<TraceProbe>) -> F,
) {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    // Keep tracing-core out of its one-subscriber fast path: other lifecycle
    // tests run concurrently without this scoped subscriber.
    let _second_dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
    let probe = Arc::new(TraceProbe::default());
    let subscriber = tracing_subscriber::registry().with(TraceCapture(probe.clone()));
    test(probe).with_subscriber(subscriber).await;
}
fn assert_failed_trace(span: &RecordedTraceSpan, kind: &str) {
    assert_eq!(span.fields["otel.status_code"], "ERROR", "{span:?}");
    assert_eq!(span.fields["error.type"], kind, "{span:?}");
    assert!(
        span.fields["ledgence.duration_ms"]
            .as_i64()
            .is_some_and(|duration| duration >= 0),
        "operation must retain a numeric duration when it fails: {span:?}"
    );
}

#[tokio::test]
async fn startup_cleanup_required_is_an_error_and_shutdown_retries_keep_its_attempt_parent() {
    with_trace_probe(|probe| async move {
        let (worker, counts) = setup(1);
        let worker = worker.with_trace_bridge(probe.clone());
        counts.startup_cleanup_required.store(1, Ordering::SeqCst);
        counts.close_failures.store(1, Ordering::SeqCst);
        let failure = worker
            .execute(request(1, 1, 0), control())
            .await
            .unwrap_err();
        assert_eq!(failure.phase, Phase::Startup);
        wait_for(|| probe.closed("ledgence.attempt.process").len() == 1).await;
        let attempt = probe.attempt("att_1");
        let starts = probe.closed("ledgence.runtime.start");
        assert_eq!(starts.len(), 1);
        assert_failed_trace(&starts[0], "Runtime");
        assert_eq!(starts[0].parent, Some(Some(attempt.context.clone())));
        assert!(probe.closed("ledgence.program.execute").is_empty());

        // Shutdown owns a separate task after the original W span has closed.
        assert_eq!(
            worker
                .shutdown(Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Io
        );
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap();
        let cleanup = probe.closed("ledgence.attempt.cleanup");
        assert_eq!(cleanup.len(), 2);
        for span in &cleanup {
            assert_eq!(span.parent, Some(Some(attempt.context.clone())));
        }
        assert_failed_trace(&cleanup[0], "Io");
        assert_ne!(
            cleanup[1].fields.get("otel.status_code"),
            Some(&json!("ERROR"))
        );
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
        assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[tokio::test]
async fn failed_and_panicking_cleanup_keep_the_attempt_parent_during_shutdown_retry() {
    for panic in [false, true] {
        with_trace_probe(|probe| async move {
            let (worker, counts) = setup(1);
            let worker = worker.with_trace_bridge(probe.clone());
            if panic {
                counts.close_panics.store(1, Ordering::SeqCst);
            } else {
                counts.close_failures.store(1, Ordering::SeqCst);
            }
            let mut invocation = request(1, 1, 0);
            let mut event = invocation.event.into_value();
            event["data"]["crash"] = true.into();
            invocation.event = CloudEvent::new(event).unwrap();
            let failure = worker.execute(invocation, control()).await.unwrap_err();
            assert_eq!(failure.phase, Phase::Execution);
            assert_eq!(failure.error.message, "lost response");
            assert!(failure.cleanup_error.is_some());
            wait_for(|| probe.closed("ledgence.attempt.process").len() == 1).await;
            let attempt = probe.attempt("att_1");
            worker
                .shutdown(Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap();
            let cleanup = probe.closed("ledgence.attempt.cleanup");
            assert_eq!(cleanup.len(), 2);
            for span in &cleanup {
                assert_eq!(span.parent, Some(Some(attempt.context.clone())));
            }
            assert_failed_trace(&cleanup[0], if panic { "Runtime" } else { "Io" });
            assert_ne!(
                cleanup[1].fields.get("otel.status_code"),
                Some(&json!("ERROR"))
            );
            assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
            assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
        })
        .await;
    }
}

#[tokio::test]
async fn warm_eviction_uses_the_new_attempt_and_healthy_shutdown_forgets_previous_attempts() {
    with_trace_probe(|probe| async move {
        let (worker, counts) = setup(1);
        let worker = worker.with_trace_bridge(probe.clone());
        let first = worker.execute(request(1, 1, 0), control()).await.unwrap();
        let second = worker.execute(request(2, 1, 0), control()).await.unwrap();
        assert_eq!(first.process_id, second.process_id);
        assert!(second.reused_process);
        worker.execute(request(3, 2, 0), control()).await.unwrap();
        wait_for(|| probe.closed("ledgence.attempt.process").len() == 3).await;
        let third = probe.attempt("att_3");
        let cleanup = probe.closed("ledgence.attempt.cleanup");
        assert_eq!(cleanup.len(), 1);
        assert_eq!(cleanup[0].parent, Some(Some(third.context)));
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap();
        let cleanup = probe.closed("ledgence.attempt.cleanup");
        assert_eq!(cleanup.len(), 2);
        assert_eq!(
            cleanup[1].parent,
            Some(None),
            "a healthy warm session must not retain an old W"
        );
        assert_eq!(counts.starts.load(Ordering::SeqCst), 2);
        assert_eq!(counts.close_completions.load(Ordering::SeqCst), 2);
        assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[tokio::test]
async fn startup_panic_closes_the_start_span_with_error_and_duration() {
    with_trace_probe(|probe| async move {
        let (worker, counts) = setup(1);
        let worker = worker.with_trace_bridge(probe.clone());
        counts.start_panics.store(1, Ordering::SeqCst);
        let failure = worker
            .execute(request(1, 1, 0), control())
            .await
            .unwrap_err();
        assert_eq!(failure.phase, Phase::Startup);
        wait_for(|| probe.closed("ledgence.attempt.process").len() == 1).await;
        let starts = probe.closed("ledgence.runtime.start");
        assert_eq!(starts.len(), 1);
        assert_failed_trace(&starts[0], "Runtime");
        assert_eq!(starts[0].parent, Some(Some(probe.attempt("att_1").context)));
        assert!(probe.closed("ledgence.program.execute").is_empty());
        assert!(
            worker
                .shutdown(Duration::ZERO, Duration::from_secs(1))
                .await
                .is_err()
        );
        assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
        assert!(probe.closed("ledgence.attempt.cleanup").is_empty());
    })
    .await;
}

#[path = "lifecycle/metrics.rs"]
mod metrics;
