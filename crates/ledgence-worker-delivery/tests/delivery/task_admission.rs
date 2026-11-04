use super::*;

#[derive(Default)]
pub(super) struct Fixture {
    pub(super) enabled: AtomicBool,
    lost_start: AtomicBool,
    lost_close: AtomicUsize,
    close_unavailable: AtomicBool,
    close_forbidden: AtomicBool,
    close_requests: AtomicUsize,
    pub(super) starts: Mutex<Vec<TaskAdmissionStartCommand>>,
    closes: Mutex<Vec<TaskAdmissionCloseCommand>>,
    // Persist before injecting a lost reply.
    source: Mutex<Option<TaskAdmissionSource>>,
}
fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "installation".into(),
        instance_id: "instance".into(),
        scope: scope(),
    }
}
fn identity() -> PrincipalIdentity {
    PrincipalIdentity {
        issuer: audience().issuer(),
        subject: "worker-one".into(),
    }
}

impl TaskAdmissionClient for Service {
    fn open_task_admission_session<'a>(
        &'a self,
        c: &'a TaskAdmissionSessionCommand,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            let mut session = self.open_session(&c.scope, &c.queue, c.concurrency).await?;
            session
                .features
                .push(worker_api::TASK_ADMISSION_FEATURE.into());
            let mut state = self.state.lock().unwrap();
            state.sessions.insert(session.id.clone(), session.clone());
            state.session = Some(session.clone());
            Ok(session)
        })
    }
    fn start_task_admission<'a>(
        &'a self,
        c: &'a TaskAdmissionStartCommand,
    ) -> ContractFuture<'a, TaskAdmissionStartReceipt> {
        Box::pin(async move {
            let fixture = &self.task_admission;
            fixture.starts.lock().unwrap().push(c.clone());
            let authority = self.authority(&c.owner, 1, true);
            let accepted_at = authority.expires_at - authority.remaining_ms;
            let source = TaskAdmissionSource {
                owner: c.owner.clone(),
                offer_generation: c.offer_generation,
                resource: "invoice".into(),
                binding_digest: TaskAdmissionBinding {
                    occurrence: TaskAdmissionOccurrence::from(&c.owner),
                    resource: "invoice".into(),
                    program: descriptor(),
                }
                .digest()?,
                start_id: c.start_id.clone(),
                source_id: "source-original".into(),
                worker: AdmissionWorkerIdentity {
                    audience: audience(),
                    issuer: identity().issuer,
                    subject: identity().subject,
                    credential_id: "cred-one".into(),
                },
                accepted_at,
                start_deadline_at: accepted_at + 1000,
            };
            let previous = fixture.source.lock().unwrap().replace(source.clone());
            assert!(previous.is_none(), "grant must never be requested again");
            if fixture.lost_start.load(Ordering::SeqCst) {
                return Err(unavailable());
            }
            Ok(TaskAdmissionStartReceipt::Granted {
                source: Box::new(source),
                authority,
            })
        })
    }
    fn close_task_admission<'a>(
        &'a self,
        c: &'a TaskAdmissionCloseCommand,
    ) -> ContractFuture<'a, TaskAdmissionCloseReceipt> {
        Box::pin(async move {
            let f = &self.task_admission;
            f.close_requests.fetch_add(1, Ordering::SeqCst);
            if f.close_unavailable.load(Ordering::SeqCst) {
                return Err(unavailable());
            }
            if f.close_forbidden.load(Ordering::SeqCst) {
                return Err(ContractError::Forbidden);
            }
            let source = f.source.lock().unwrap().clone();
            let mut closes = f.closes.lock().unwrap();
            let already_accepted = !closes.is_empty();
            if let Some(original) = closes.first() {
                assert_eq!(original, c, "Close retries must keep the same facts");
            }
            closes.push(c.clone());
            let disposition = match (c.outcome, source.is_some()) {
                (TaskAdmissionCloseOutcome::NotInvoked, true) => {
                    TaskAdmissionCloseDisposition::ChargedNotInvoked
                }
                (TaskAdmissionCloseOutcome::NotInvoked, false) => {
                    TaskAdmissionCloseDisposition::Deferred
                }
                (TaskAdmissionCloseOutcome::Quiescent, true) => {
                    TaskAdmissionCloseDisposition::Released
                }
                (TaskAdmissionCloseOutcome::Quiescent, false) => {
                    TaskAdmissionCloseDisposition::Closed
                }
            };
            if c.outcome == TaskAdmissionCloseOutcome::NotInvoked {
                self.state
                    .lock()
                    .unwrap()
                    .active
                    .remove(&c.owner.consumer_id);
            }
            if lose(&f.lost_close) {
                return Err(unavailable());
            }
            Ok(TaskAdmissionCloseReceipt {
                owner: c.owner.clone(),
                offer_generation: c.offer_generation,
                start_id: c.start_id.clone(),
                operation_id: c.operation_id.clone(),
                outcome: c.outcome,
                source,
                disposition,
                accepted_at: 1_000_000,
                already_accepted,
            })
        })
    }
    fn inspect_task_admission<'a>(
        &'a self,
        _: &'a TaskAdmissionQuery,
    ) -> ContractFuture<'a, TaskAdmissionSnapshot> {
        Box::pin(async { panic!("worker may not inspect as operator") })
    }
    fn release_task_admission<'a>(
        &'a self,
        _: &'a TaskAdmissionReleaseCommand,
    ) -> ContractFuture<'a, TaskAdmissionReleaseReceipt> {
        Box::pin(async { panic!("worker may not release as operator") })
    }
}

pub(super) fn driver(worker: Worker, service: Arc<Service>) -> DeliveryDriver {
    service.task_admission.enabled.store(true, Ordering::SeqCst);
    DeliveryDriver::new(worker, service, config())
        .unwrap()
        .with_runtime_binding(audience(), identity())
        .unwrap()
        .with_runtime_features(RuntimeFeatures {
            v: 1,
            supported: vec![worker_api::TASK_ADMISSION_FEATURE.into()],
        })
        .unwrap()
}

#[tokio::test]
async fn ready_grant_waits_for_preparation_and_both_settlement_and_close_retry() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    counts.hold_startup.store(true, Ordering::SeqCst);
    service.task_admission.lost_close.store(1, Ordering::SeqCst);
    service.lost_settlement_replies.store(1, Ordering::SeqCst);
    let mut handle = driver(worker, service.clone()).start();
    wait_for(|| counts.startup_entries.load(Ordering::SeqCst) > 0).await;
    assert!(service.task_admission.starts.lock().unwrap().is_empty());
    counts.hold_startup.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(service.task_admission.starts.lock().unwrap().len(), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    let closes = service.task_admission.closes.lock().unwrap();
    assert_eq!(closes.len(), 2);
    assert_eq!(closes[0].outcome, TaskAdmissionCloseOutcome::Quiescent);
    let state = service.state.lock().unwrap();
    assert_eq!(state.settlements.len(), 2);
    assert_eq!(state.renewals[0].intent, RenewIntent::Prepare);
    assert!(
        state
            .renewals
            .iter()
            .all(|c| c.intent != RenewIntent::Dispatch)
    );
}

#[tokio::test]
async fn lost_committed_start_does_not_invoke_or_settle_an_invented_report() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service
        .task_admission
        .lost_start
        .store(true, Ordering::SeqCst);
    service.task_admission.lost_close.store(1, Ordering::SeqCst);
    let mut handle = driver(worker, service.clone()).start();
    wait_for(|| service.task_admission.closes.lock().unwrap().len() >= 2).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(service.task_admission.starts.lock().unwrap().len(), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    let closes = service.task_admission.closes.lock().unwrap();
    assert_eq!(closes[0].outcome, TaskAdmissionCloseOutcome::NotInvoked);
    assert!(service.task_admission.source.lock().unwrap().is_some());
    let state = service.state.lock().unwrap();
    assert!(state.settlements.is_empty());
    assert_eq!(state.capacity_violations, 0);
}

#[tokio::test]
async fn a_settled_failure_never_closes_occupancy_while_cleanup_is_running() {
    let (worker, counts) = setup(1);
    counts.execution_error.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    service.settle_unavailable.store(true, Ordering::SeqCst);
    let mut handle = driver(worker.clone(), service.clone()).start();
    wait_for(|| service.accepted_count() == 1).await;
    assert_eq!(
        service.state.lock().unwrap().accepted["att_1"].quiescence,
        Quiescence::Unconfirmed
    );
    assert!(service.task_admission.closes.lock().unwrap().is_empty());
    assert_eq!(worker.stats().await.process_slots, 1);
    counts.hold_cleanup.store(false, Ordering::SeqCst);
    wait_for(|| !service.task_admission.closes.lock().unwrap().is_empty()).await;
    assert_eq!(
        handle.status().settled_attempts,
        0,
        "late Close must not wait for a lost settlement reply"
    );
    service.settle_unavailable.store(false, Ordering::SeqCst);
    handle.shutdown(WAIT).await.unwrap();
    let closes = service.task_admission.closes.lock().unwrap();
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].outcome, TaskAdmissionCloseOutcome::Quiescent);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn preparation_adapter_error_is_charged_even_if_its_error_kind_is_cancelled() {
    let (worker, counts) = setup(1);
    *counts.injected_startup_error.lock().unwrap() = Some(worker_api::Error::new(
        ErrorKind::Cancelled,
        "startup adapter failed with live control",
    ));
    let service = Service::new(1);
    let mut handle = driver(worker, service.clone()).start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert!(service.task_admission.starts.lock().unwrap().is_empty());
    let closes = service.task_admission.closes.lock().unwrap();
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].outcome, TaskAdmissionCloseOutcome::Quiescent);
    let state = service.state.lock().unwrap();
    assert!(
        matches!(&state.accepted["att_1"].report, AttemptReport::Failed(failure)
        if failure.phase==worker_api::Phase::Startup && !failure.execution_may_have_started)
    );
}

#[tokio::test]
async fn remote_capability_does_not_register_legacy_runtime_for_governed_tasks() {
    let (worker, counts) = setup(1);
    counts.legacy_runtime.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    let mut handle = driver(worker, service.clone()).start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    let state = service.state.lock().unwrap();
    assert!(state.session.as_ref().unwrap().features.is_empty());
    assert_eq!(
        counts.executions.load(Ordering::SeqCst),
        1,
        "ordinary task remains usable"
    );
    assert!(
        state
            .renewals
            .iter()
            .any(|c| c.intent == RenewIntent::Dispatch)
    );
    assert!(service.task_admission.starts.lock().unwrap().is_empty());
    assert!(service.task_admission.closes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn confirmed_result_survives_close_outage_without_repeated_execution() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service
        .task_admission
        .close_unavailable
        .store(true, Ordering::SeqCst);
    let mut handle = driver(worker, service.clone()).start();
    wait_for(|| {
        handle.status().settled_attempts == 1
            && service.task_admission.close_requests.load(Ordering::SeqCst) > 1
    })
    .await;
    assert_eq!(
        service.state.lock().unwrap().accepted["att_1"].quiescence,
        Quiescence::Confirmed
    );
    assert!(service.task_admission.closes.lock().unwrap().is_empty());
    service
        .task_admission
        .close_unavailable
        .store(false, Ordering::SeqCst);
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(service.task_admission.closes.lock().unwrap().len(), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(service.task_admission.starts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn definitive_close_rejection_drains_worker_without_losing_confirmed_result() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service
        .task_admission
        .close_forbidden
        .store(true, Ordering::SeqCst);
    let mut handle = driver(worker, service.clone()).start();
    wait_for(|| handle.status().stopping && handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(handle.status().last_error, Some(ContractError::Forbidden));
    assert_eq!(
        service.task_admission.close_requests.load(Ordering::SeqCst),
        1
    );
    assert!(service.task_admission.closes.lock().unwrap().is_empty());
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
}
