use super::*;
use std::time::Instant;

struct AdmittedRuntime(Runtime);
impl ExecutionRuntime for AdmittedRuntime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            match self.0.start(artifact, control).await? {
                StartOutcome::Ready(session) => {
                    Ok(StartOutcome::Ready(Box::new(AdmittedSession(session))))
                }
                other => Ok(other),
            }
        })
    }
}
struct AdmittedSession(Box<dyn ExecutionSession>);
impl ExecutionSession for AdmittedSession {
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
    fn supports_admitted_invocation(&self) -> bool {
        true
    }
    fn execute_admitted<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
        permit: InvocationPermit,
    ) -> PortFuture<'a, ProgramOutcome> {
        Box::pin(async move {
            permit.check()?;
            self.0.execute(invocation, control).await
        })
    }
    fn close(&mut self) -> PortFuture<'_, ()> {
        self.0.close()
    }
}
#[derive(Clone, Copy)]
enum Decision {
    Grant,
    Defer,
    Expired,
    Lost,
    Pending,
    Panic,
}
struct Gate {
    decision: Decision,
    calls: Mutex<Vec<InvocationReady>>,
    entered: Semaphore,
}
impl Gate {
    fn new(decision: Decision) -> Arc<Self> {
        Arc::new(Self {
            decision,
            calls: Mutex::new(Vec::new()),
            entered: Semaphore::new(0),
        })
    }
}
impl InvocationAdmission for Gate {
    fn ready<'a>(
        &'a self,
        ready: &'a InvocationReady,
        _control: RunControl,
    ) -> PortFuture<'a, InvocationAdmissionDecision> {
        Box::pin(async move {
            self.calls.lock().await.push(ready.clone());
            self.entered.add_permits(1);
            match self.decision {
                Decision::Defer => Ok(InvocationAdmissionDecision::DoNotInvoke),
                Decision::Grant => Ok(InvocationAdmissionDecision::Invoke(InvocationPermit {
                    start_before: Instant::now() + Duration::from_secs(1),
                })),
                Decision::Expired => Ok(InvocationAdmissionDecision::Invoke(InvocationPermit {
                    start_before: Instant::now(),
                })),
                Decision::Lost => Err(Error::new(ErrorKind::Unavailable, "uncertain Start reply")),
                Decision::Pending => std::future::pending().await,
                Decision::Panic => panic!("injected admission gate panic"),
            }
        })
    }
}
fn configured() -> (Worker, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            ..WorkerConfig::default()
        },
        Arc::new(Store(counts.clone())),
        Arc::new(Cache::default()),
        Arc::new(AdmittedRuntime(Runtime(counts.clone()))),
    )
    .unwrap();
    (worker, counts)
}

#[tokio::test]
async fn permission_is_after_preparation_and_deferral_reuses_one_unbound_warm_session() {
    let (worker, counts) = configured();
    let gate = Gate::new(Decision::Defer);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let outcome = reservation
        .execute_admitted(request(1, 1, 0), control(), gate.clone())
        .await;
    assert!(matches!(
        outcome,
        AdmittedExecutionResult::NotInvoked { .. }
    ));
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert!(reservation.is_quiescent());
    assert_eq!(gate.calls.lock().await[0].process_id, 1);
    assert!(!gate.calls.lock().await[0].reused_process);
    assert_eq!(worker.stats().await.warm_processes, 1);
    reservation.release();
    let granted = Gate::new(Decision::Grant);
    let mut next = worker.reserve_consumer(control()).await.unwrap();
    let outcome = next
        .execute_admitted(request(2, 1, 0), control(), granted.clone())
        .await;
    assert!(matches!(outcome, AdmittedExecutionResult::Executed(Ok(_))));
    assert!(granted.calls.lock().await[0].reused_process);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    next.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn old_runtime_never_requests_permission_and_uncertain_or_expired_grant_never_invokes() {
    let (worker, counts) = setup(1);
    let gate = Gate::new(Decision::Grant);
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let outcome = reservation
        .execute_admitted(request(1, 1, 0), control(), gate.clone())
        .await;
    assert!(
        matches!(outcome,AdmittedExecutionResult::Executed(Err(ref e)) if e.error.kind==ErrorKind::Incompatible && !e.execution_may_have_started)
    );
    assert!(gate.calls.lock().await.is_empty());
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    for decision in [Decision::Expired, Decision::Lost, Decision::Panic] {
        let (worker, counts) = configured();
        let gate = Gate::new(decision);
        let mut reservation = worker.reserve_consumer(control()).await.unwrap();
        let outcome = reservation
            .execute_admitted(request(1, 1, 0), control(), gate.clone())
            .await;
        assert!(
            matches!(outcome,AdmittedExecutionResult::Executed(Err(ref e)) if !e.execution_may_have_started)
        );
        assert_eq!(gate.calls.lock().await.len(), 1);
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        assert!(reservation.is_quiescent());
        if matches!(decision, Decision::Panic) {
            assert!(!worker.stats().await.accepting);
        }
        reservation.release();
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn cancellation_bounds_a_noncooperative_gate_and_confirms_owned_cleanup() {
    let (worker, counts) = configured();
    let gate = Gate::new(Decision::Pending);
    let outer = control();
    let mut reservation = worker.reserve_consumer(control()).await.unwrap();
    let owner = outer.clone();
    let gate_copy = gate.clone();
    let execution = tokio::spawn(async move {
        let result = reservation
            .execute_admitted(request(1, 1, 0), owner, gate_copy)
            .await;
        (result, reservation)
    });
    tokio::time::timeout(Duration::from_secs(1), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    outer.cancel();
    let (result, reservation) = tokio::time::timeout(Duration::from_secs(1), execution)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result,AdmittedExecutionResult::Executed(Err(ref e)) if e.error.kind==ErrorKind::Cancelled && !e.execution_may_have_started)
    );
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(counts.close_completions.load(Ordering::SeqCst), 1);
    assert!(reservation.is_quiescent());
    reservation.release();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
}

struct RequestsRuntime {
    runtime: Runtime,
    supported: bool,
}
struct RequestsSession {
    session: AdmittedSession,
    supported: bool,
}
impl ExecutionRuntime for RequestsRuntime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            match self.runtime.start(artifact, control).await? {
                StartOutcome::Ready(session) => {
                    Ok(StartOutcome::Ready(Box::new(RequestsSession {
                        session: AdmittedSession(session),
                        supported: self.supported,
                    })))
                }
                other => Ok(other),
            }
        })
    }
}
impl ExecutionSession for RequestsSession {
    fn pid(&self) -> u32 {
        self.session.pid()
    }
    fn execute<'a>(
        &'a mut self,
        _: RuntimeInvocation,
        _: RunControl,
    ) -> PortFuture<'a, ProgramOutcome> {
        Box::pin(async { panic!("must never fall back to ordinary execution") })
    }
    fn supports_admitted_invocation(&self) -> bool {
        true
    }
    fn supports_admitted_requests(&self) -> bool {
        self.supported
    }
    fn execute_admitted_with_requests<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
        permit: InvocationPermit,
        handler: Arc<dyn RuntimeRequestHandler>,
    ) -> PortFuture<'a, ProgramOutcome> {
        Box::pin(async move {
            permit.check()?;
            assert!(
                invocation.extension.is_none(),
                "plain task must not gain workflow context"
            );
            handler
                .handle(
                    RuntimeRequest {
                        id: 1,
                        operation: "artifacts.put_file".into(),
                        payload: json!({}),
                    },
                    control.clone(),
                )
                .await?;
            self.session
                .execute_admitted(invocation, control, permit)
                .await
        })
    }
    fn close(&mut self) -> PortFuture<'_, ()> {
        self.session.close()
    }
}
struct RequestsHandler(AtomicUsize);
impl RuntimeRequestHandler for RequestsHandler {
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        _: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(RuntimeReply {
                id: request.id,
                result: json!({}),
            })
        })
    }
}
#[tokio::test]
async fn admitted_requests_require_explicit_support_and_never_bypass_the_grant() {
    for (supported, decision, expected_grants, expected_invocations) in [
        (false, Decision::Grant, 0, 0),
        (true, Decision::Defer, 1, 0),
        (true, Decision::Expired, 1, 0),
        (true, Decision::Grant, 1, 1),
    ] {
        let counts = Arc::new(Counts::default());
        let cache = Arc::new(Cache::default());
        let task = request(1, 1, 0);
        let mut manifest = manifest(task.descriptor.program.clone());
        manifest.runtime.protocol = 3;
        cache.entries.lock().await.insert(
            task.descriptor.digest.clone(),
            PreparedArtifact::new(
                PathBuf::from("/unused-test-artifact"),
                manifest,
                task.descriptor.digest.clone(),
                Arc::new(()),
            ),
        );
        let worker = Worker::new(
            WorkerConfig {
                concurrency: 1,
                ..WorkerConfig::default()
            },
            Arc::new(Store(counts.clone())),
            cache,
            Arc::new(RequestsRuntime {
                runtime: Runtime(counts.clone()),
                supported,
            }),
        )
        .unwrap();
        let gate = Gate::new(decision);
        let handler = Arc::new(RequestsHandler(AtomicUsize::new(0)));
        let mut reservation = worker.reserve_consumer(control()).await.unwrap();
        let result = reservation
            .execute_admitted_with_requests(task, control(), gate.clone(), handler.clone())
            .await;
        if expected_invocations == 1 {
            assert!(matches!(result, AdmittedExecutionResult::Executed(Ok(_))));
        }
        if !supported {
            assert!(
                matches!(result, AdmittedExecutionResult::Executed(Err(ref e)) if e.error.kind == ErrorKind::Incompatible && !e.execution_may_have_started)
            );
        }
        assert_eq!(gate.calls.lock().await.len(), expected_grants);
        assert_eq!(handler.0.load(Ordering::SeqCst), expected_invocations);
        assert_eq!(
            counts.executions.load(Ordering::SeqCst),
            expected_invocations
        );
        assert!(reservation.is_quiescent());
        reservation.release();
        worker
            .shutdown(Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap();
    }
}

struct OptionalHandler;
impl RuntimeRequestHandler for OptionalHandler {
    fn optional_for_legacy_protocol(&self) -> bool {
        true
    }
    fn features(&self) -> RuntimeFeatures {
        panic!("legacy protocol must not advertise artifact feature")
    }
    fn handle<'a>(&'a self, _: RuntimeRequest, _: RunControl) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async { panic!("legacy protocol must not issue artifact requests") })
    }
}
#[tokio::test]
async fn optional_artifacts_preserve_legacy_programs_and_their_admission_fence() {
    for protocol in [1, 2] {
        for decision in [Decision::Grant, Decision::Expired] {
            let counts = Arc::new(Counts::default());
            let cache = Arc::new(Cache::default());
            let task = request(1, 1, 0);
            let mut manifest = manifest(task.descriptor.program.clone());
            manifest.runtime.protocol = protocol;
            cache.entries.lock().await.insert(
                task.descriptor.digest.clone(),
                PreparedArtifact::new(
                    PathBuf::from("/unused-test-artifact"),
                    manifest,
                    task.descriptor.digest.clone(),
                    Arc::new(()),
                ),
            );
            let worker = Worker::new(
                WorkerConfig {
                    concurrency: 1,
                    ..WorkerConfig::default()
                },
                Arc::new(Store(counts.clone())),
                cache,
                Arc::new(AdmittedRuntime(Runtime(counts.clone()))),
            )
            .unwrap();
            let gate = Gate::new(decision);
            let mut reservation = worker.reserve_consumer(control()).await.unwrap();
            let result = reservation
                .execute_admitted_with_requests(
                    task,
                    control(),
                    gate.clone(),
                    Arc::new(OptionalHandler),
                )
                .await;
            let expected = usize::from(matches!(decision, Decision::Grant));
            assert_eq!(gate.calls.lock().await.len(), 1);
            assert_eq!(counts.executions.load(Ordering::SeqCst), expected);
            assert_eq!(
                matches!(result, AdmittedExecutionResult::Executed(Ok(_))),
                expected == 1
            );
            reservation.release();
            worker
                .shutdown(Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap();
        }
    }
}
