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
