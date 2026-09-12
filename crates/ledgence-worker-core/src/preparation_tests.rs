use super::*;
use serde_json::json;

struct GatedLookup {
    entered: Semaphore,
    release: Semaphore,
}
impl ArtifactCache for GatedLookup {
    fn lookup<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
    ) -> PortFuture<'a, Option<PreparedArtifact>> {
        if descriptor.program.version == "panic" {
            // Exercise a panic before the adapter even returns a future or a
            // recoverable artifact/session handle.
            panic!("injected initial cache lookup construction panic");
        }
        Box::pin(async {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            Err(Error::new(ErrorKind::Io, "blocked lookup released"))
        })
    }
    fn publish<'a>(
        &'a self,
        _: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        unreachable!("both invocations stop during initial lookup")
    }
}
struct NoExecution;
impl ProgramStore for NoExecution {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        unreachable!("assignments are already bound")
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        unreachable!("both invocations stop during initial lookup")
    }
}
impl ExecutionRuntime for NoExecution {
    fn start(&self, _: PreparedArtifact, _: RunControl) -> PortFuture<'_, StartOutcome> {
        unreachable!("preparation must not dispatch either invocation")
    }
}
fn invocation(id: &str, digest: char) -> ExecutionRequest {
    ExecutionRequest {
        descriptor: ProgramDescriptor {
            program: ProgramRef { id: "test".into(), version: id.into() },
            digest: Digest(format!("sha256:{}", digest.to_string().repeat(64))),
            size: 1,
        },
        event: CloudEvent::new(json!({
            "specversion": "1.0", "source": "urn:test:preparation", "id": id,
            "type": "com.ledgence.task.invocation.requested.v1", "datacontenttype": "application/json",
            "ldgtenantid": "tenant_a", "ldgnamespace": "test", "ldgrunid": "run_1",
            "ldgtaskid": id, "ldgattemptid": id, "ldgattemptno": 1, "data": {}
        })).unwrap(),
    }
}

#[tokio::test]
async fn initial_preparation_panic_cancels_peers_and_retains_unresolved_ownership() {
    let cache = Arc::new(GatedLookup {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 2,
            ..WorkerConfig::default()
        },
        Arc::new(NoExecution),
        cache.clone(),
        Arc::new(NoExecution),
    )
    .unwrap();
    let peer_control = RunControl::new(Duration::from_secs(5));
    let peer = {
        let worker = worker.clone();
        let control = peer_control.clone();
        tokio::spawn(async move { worker.execute(invocation("blocked", 'a'), control).await })
    };
    tokio::time::timeout(Duration::from_secs(1), cache.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let panicking = invocation("panic", 'b');
    let failure = worker
        .execute(panicking.clone(), RunControl::new(Duration::from_secs(5)))
        .await
        .unwrap_err();
    assert_eq!(failure.phase, Phase::Preparation);
    assert_eq!(failure.error.kind, ErrorKind::Runtime);
    assert!(
        failure
            .error
            .message
            .contains("initial cache lookup construction panic")
    );
    assert!(!failure.execution_may_have_started);
    assert_eq!(peer_control.check().unwrap_err().kind, ErrorKind::Cancelled);
    assert!(!worker.stats().await.accepting);
    let key = attempt_key(&InvocationIdentity::from(&panicking.event));
    {
        let registry = worker.inner.registry.lock().unwrap();
        assert!(registry.unresolved.contains(&key));
        assert_eq!(registry.unresolved_operations, 1);
        assert_eq!(
            registry.active.len(),
            1,
            "the blocked peer still owns its operation"
        );
    }

    // Resolve the other operation completely. Shutdown must still refuse to
    // certify cleanup of the adapter that never returned a recoverable handle.
    cache.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), peer)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(worker.stats().await.process_slots, 0);
    for _ in 0..2 {
        let error = worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Runtime);
        assert!(
            error
                .message
                .contains("unresolved process or adapter operations")
        );
    }
    let rejected = worker
        .execute(panicking, RunControl::new(Duration::from_secs(5)))
        .await
        .unwrap_err();
    assert_eq!(rejected.phase, Phase::Admission);
    assert_eq!(rejected.error.kind, ErrorKind::Unavailable);
    assert!(
        worker
            .inner
            .registry
            .lock()
            .unwrap()
            .unresolved
            .contains(&key)
    );
}

#[tokio::test]
async fn reserved_preparation_panic_retains_its_consumer_after_external_owners_release() {
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 2,
            ..WorkerConfig::default()
        },
        Arc::new(NoExecution),
        Arc::new(GatedLookup {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }),
        Arc::new(NoExecution),
    )
    .unwrap();
    let mut panicking = worker
        .reserve_consumer(RunControl::new(Duration::from_secs(1)))
        .await
        .unwrap();
    let peer = worker
        .reserve_consumer(RunControl::new(Duration::from_secs(1)))
        .await
        .unwrap();
    let failure = panicking
        .execute(
            invocation("panic", 'a'),
            RunControl::new(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert_eq!(failure.phase, Phase::Preparation);
    assert_eq!(failure.error.kind, ErrorKind::Runtime);
    assert!(panicking.is_cancellation_requested());
    assert!(peer.is_cancellation_requested());
    assert!(!panicking.is_quiescent());
    assert!(peer.is_quiescent());
    panicking.release();
    peer.release();
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Runtime,
        "an adapter without a recoverable operation cannot certify cleanup"
    );
    assert_eq!(worker.stats().await.active_consumers, 1);
}

struct GatedMiss {
    entered: Semaphore,
    release: Semaphore,
}
impl ArtifactCache for GatedMiss {
    fn lookup<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Option<PreparedArtifact>> {
        Box::pin(async {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            Ok(None)
        })
    }
    fn publish<'a>(
        &'a self,
        _: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        unreachable!("stopped lookup owners must not publish")
    }
}

#[derive(Default)]
struct CountNewFetches(std::sync::atomic::AtomicUsize);
impl ProgramStore for CountNewFetches {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        unreachable!("assignments are already bound")
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Err(Error::new(ErrorKind::Io, "unexpected new download")) })
    }
}

async fn stopped_cache_miss_does_not_start_a_download(expire: bool) {
    let cache = Arc::new(GatedMiss {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let store = Arc::new(CountNewFetches::default());
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            ..WorkerConfig::default()
        },
        store.clone(),
        cache.clone(),
        Arc::new(NoExecution),
    )
    .unwrap();
    let control = RunControl::new(Duration::from_secs(2));
    let invocation = {
        let worker = worker.clone();
        let control = control.clone();
        tokio::spawn(async move {
            worker
                .execute(invocation("stopped-miss", 'c'), control)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), cache.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let expected = if expire {
        // The lookup has acknowledged ownership before the real monotonic
        // deadline is allowed to expire. The gate controls when its miss returns.
        tokio::time::sleep_until(tokio::time::Instant::from_std(control.deadline())).await;
        ErrorKind::TimedOut
    } else {
        control.cancel();
        ErrorKind::Cancelled
    };
    assert_eq!(control.check().unwrap_err().kind, expected);
    assert!(
        !invocation.is_finished(),
        "the existing lookup remains owned"
    );
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(store.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    cache.release.add_permits(1);
    let failure = tokio::time::timeout(Duration::from_secs(1), invocation)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(
        store.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a cache miss must not start a new download after its request stopped"
    );
    assert_eq!(failure.error.kind, expected);
    assert_eq!(failure.phase, Phase::Preparation);
    assert!(!failure.execution_may_have_started);
    assert!(failure.cleanup_error.is_none());
    let stats = worker.stats().await;
    assert_eq!(stats.active_consumers, 0);
    assert_eq!(stats.process_slots, 0);
    assert!(stats.accepting);
    tokio::time::timeout(
        Duration::from_secs(1),
        worker.shutdown(Duration::ZERO, Duration::from_millis(100)),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn cancelled_cache_miss_does_not_start_a_new_fetch() {
    stopped_cache_miss_does_not_start_a_download(false).await;
}

#[tokio::test]
async fn expired_cache_miss_does_not_start_a_new_fetch() {
    stopped_cache_miss_does_not_start_a_download(true).await;
}
