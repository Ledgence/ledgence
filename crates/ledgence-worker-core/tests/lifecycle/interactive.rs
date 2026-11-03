use super::*;

struct InteractiveRuntime(Runtime);
impl ExecutionRuntime for InteractiveRuntime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            match self.0.start(artifact, control).await? {
                StartOutcome::Ready(session) => {
                    Ok(StartOutcome::Ready(Box::new(InteractiveSession(session))))
                }
                cleanup => Ok(cleanup),
            }
        })
    }
}
struct InteractiveSession(Box<dyn ExecutionSession>);
impl ExecutionSession for InteractiveSession {
    fn pid(&self) -> u32 {
        self.0.pid()
    }
    fn execute<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
    ) -> PortFuture<'a, ProgramOutcome> {
        self.0.execute(invocation, control)
    }
    fn execute_with_requests<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
        handler: Arc<dyn RuntimeRequestHandler>,
    ) -> PortFuture<'a, ProgramOutcome> {
        Box::pin(async move {
            let request = RuntimeRequest {
                id: 1,
                operation: "test.checkpoint".into(),
                payload: json!({"event": invocation.event.value(), "extension": invocation.extension}),
            };
            handler.handle(request, control.clone()).await?;
            self.0.execute(invocation, control).await
        })
    }
    fn close(&mut self) -> PortFuture<'_, ()> {
        self.0.close()
    }
}
struct Handler {
    entered: Semaphore,
    release: Semaphore,
    observations: Mutex<Vec<serde_json::Value>>,
}
impl Handler {
    fn new(released: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(usize::from(released)),
            observations: Mutex::new(Vec::new()),
        })
    }
}
impl RuntimeRequestHandler for Handler {
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async move {
            self.observations.lock().await.push(request.payload);
            self.entered.add_permits(1);
            loop {
                control.check()?;
                tokio::select! {
                    permit = self.release.acquire() => { permit.unwrap().forget(); break; }
                    _ = tokio::time::sleep(Duration::from_millis(2)) => {}
                }
            }
            Ok(RuntimeReply {
                id: request.id,
                result: json!({"committed": true}),
            })
        })
    }
}
fn extension() -> RuntimeExtension {
    RuntimeExtension {
        schema: "test.activation.v1".into(),
        payload: json!({"checkpoint": 7}),
    }
}

struct BudgetedHandler {
    inner: Arc<Handler>,
    timeout: Duration,
    reserve: Duration,
}
impl RuntimeRequestHandler for BudgetedHandler {
    fn invocation_timeout(&self) -> Option<Duration> {
        Some(self.timeout)
    }
    fn completion_reserve(&self) -> Duration {
        self.reserve
    }
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        self.inner.handle(request, control)
    }
}

#[tokio::test]
async fn shorter_invocation_deadline_retires_process_and_preserves_parent_report_budget() {
    let (worker, counts) = configured(true).await;
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let parent = RunControl::new(Duration::from_secs(5));
    let handler = Handler::new(false);
    let report = tokio::time::timeout(
        Duration::from_secs(3),
        reservation.execute_interactive(
            request(1, 1, 0),
            parent.clone(),
            extension(),
            Arc::new(BudgetedHandler {
                inner: handler.clone(),
                timeout: Duration::from_millis(50),
                reserve: Duration::from_secs(1),
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(report.error.kind, ErrorKind::TimedOut);
    assert_eq!(report.phase, Phase::Execution);
    assert!(parent.check().is_ok());
    assert_eq!(handler.observations.lock().await.len(), 1);
    assert!(reservation.is_quiescent());
    assert_eq!(worker.stats().await.warm_processes, 0);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn inadequate_completion_reserve_retires_before_invocation() {
    let (worker, _) = configured(true).await;
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let handler = Handler::new(true);
    let report = reservation
        .execute_interactive(
            request(1, 1, 0),
            RunControl::new(Duration::from_secs(2)),
            extension(),
            Arc::new(BudgetedHandler {
                inner: handler.clone(),
                timeout: Duration::from_secs(1),
                reserve: Duration::from_secs(3),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(report.error.kind, ErrorKind::TimedOut);
    assert_eq!(report.phase, Phase::Startup);
    assert!(!report.execution_may_have_started);
    assert!(handler.observations.lock().await.is_empty());
    assert!(reservation.is_quiescent());
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn restricted_invocation_observes_parent_cancellation() {
    let (worker, _) = configured(true).await;
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let parent = RunControl::new(Duration::from_secs(10));
    let handler = Handler::new(false);
    let run_control = parent.clone();
    let run_handler = handler.clone();
    let run = tokio::spawn(async move {
        let report = reservation
            .execute_interactive(
                request(1, 1, 0),
                run_control,
                extension(),
                Arc::new(BudgetedHandler {
                    inner: run_handler,
                    timeout: Duration::from_secs(5),
                    reserve: Duration::from_secs(1),
                }),
            )
            .await;
        (reservation, report)
    });
    tokio::time::timeout(Duration::from_secs(2), handler.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    parent.cancel();
    let (reservation, report) = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.unwrap_err().error.kind, ErrorKind::Cancelled);
    assert!(reservation.is_quiescent());
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}
async fn configured(interactive_runtime: bool) -> (Worker, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let cache = Arc::new(Cache::default());
    let request = request(1, 1, 0);
    let mut manifest = manifest(request.descriptor.program.clone());
    manifest.runtime.protocol = 3;
    cache.entries.lock().await.insert(
        request.descriptor.digest.clone(),
        PreparedArtifact::new(
            PathBuf::from("/unused-test-artifact"),
            manifest,
            request.descriptor.digest,
            Arc::new(()),
        ),
    );
    let runtime: Arc<dyn ExecutionRuntime> = if interactive_runtime {
        Arc::new(InteractiveRuntime(Runtime(counts.clone())))
    } else {
        Arc::new(Runtime(counts.clone()))
    };
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            ..WorkerConfig::default()
        },
        Arc::new(Store(counts.clone())),
        cache,
        runtime,
    )
    .unwrap();
    (worker, counts)
}

#[tokio::test]
async fn interactive_reservation_retains_one_slot_and_reuses_after_settlement() {
    let (worker, counts) = configured(true).await;
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let handler = Handler::new(false);
    let owned_handler = handler.clone();
    let original = request(1, 1, 0).event.value().clone();
    let run = tokio::spawn(async move {
        let report = reservation
            .execute_interactive(request(1, 1, 0), control(), extension(), owned_handler)
            .await;
        (reservation, report)
    });
    handler.entered.acquire().await.unwrap().forget();
    let stats = worker.stats().await;
    assert_eq!(stats.active_consumers, 1);
    assert_eq!(stats.process_slots, 1);
    assert_eq!(stats.warm_processes, 0);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            worker.reserve_consumer(control())
        )
        .await
        .is_err()
    );
    handler.release.add_permits(1);
    let (mut reservation, report) = run.await.unwrap();
    let first = report.unwrap();
    assert_eq!(
        first.outcome,
        ProgramOutcome::Success {
            output: original.clone()
        }
    );
    assert!(reservation.is_quiescent());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(worker.stats().await.warm_processes, 1);
    let observed = handler.observations.lock().await;
    assert_eq!(observed[0]["event"], original);
    assert_eq!(
        observed[0]["extension"],
        serde_json::to_value(extension()).unwrap()
    );
    drop(observed);
    let reused_reservation = reservation
        .execute(request(2, 1, 0), control())
        .await
        .unwrap_err();
    assert_eq!(reused_reservation.error.kind, ErrorKind::InvalidInput);
    reservation.release();
    let plain = worker.execute(request(2, 1, 0), control()).await.unwrap();
    assert!(plain.reused_process);
    assert_eq!(plain.process_id, first.process_id);
    let interactive = worker
        .execute_interactive(request(3, 1, 0), control(), extension(), Handler::new(true))
        .await
        .unwrap();
    assert!(interactive.reused_process);
    assert_eq!(interactive.process_id, first.process_id);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn interactive_rejects_old_protocol_before_start_and_default_session_without_execution() {
    let (worker, counts) = setup(1);
    let error = worker
        .execute_interactive(request(1, 1, 0), control(), extension(), Handler::new(true))
        .await
        .unwrap_err();
    assert_eq!(error.error.kind, ErrorKind::Incompatible);
    assert!(!error.execution_may_have_started);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();

    let (worker, counts) = configured(false).await;
    let error = worker
        .execute_interactive(request(1, 1, 0), control(), extension(), Handler::new(true))
        .await
        .unwrap_err();
    assert_eq!(error.error.kind, ErrorKind::Incompatible);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.process_slots, 0);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn interactive_callback_deadline_retires_before_returning_capacity() {
    let (worker, counts) = configured(true).await;
    let handler = Handler::new(false);
    let error = worker
        .execute_interactive(
            request(1, 1, 0),
            RunControl::new(Duration::from_millis(80)),
            extension(),
            handler.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.error.kind, ErrorKind::TimedOut);
    assert!(error.execution_may_have_started);
    assert_eq!(handler.observations.lock().await.len(), 1);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    let later = worker.execute(request(2, 1, 0), control()).await.unwrap();
    assert!(!later.reused_process);
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}
