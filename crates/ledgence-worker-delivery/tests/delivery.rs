//! Driver behavior under uncertain replies and delayed local cleanup.
//!
//! The service below records mutations before injecting reply failures. Tests
//! inspect those records, worker capacity, and actual runtime invocations rather
//! than equating a transport response with a successful execution.

use ledgence_orchestration_api::*;
use ledgence_worker_api::{self as worker_api, *};
use ledgence_worker_core::{Worker, WorkerConfig};
use ledgence_worker_delivery::{DeliveryConfig, DeliveryDriver};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(5);

fn scope() -> Scope {
    Scope {
        tenant_id: "tenant_test".into(),
        namespace: "billing".into(),
    }
}

fn descriptor() -> ProgramDescriptor {
    ProgramDescriptor {
        program: ProgramRef {
            id: "invoice".into(),
            version: "1.0.0".into(),
        },
        digest: Digest(format!("sha256:{:064x}", 1)),
        size: 1,
    }
}

#[derive(Default)]
struct Counts {
    fetches: AtomicUsize,
    fetch_completions: AtomicUsize,
    startup_entries: AtomicUsize,
    starts: AtomicUsize,
    live: AtomicUsize,
    peak: AtomicUsize,
    executions: AtomicUsize,
    cancelled: AtomicUsize,
    closes: AtomicUsize,
    slow_closes: AtomicUsize,
    close_delay_ms: AtomicU64,
    hold_slow_cleanup: AtomicBool,
    implicit_drops: AtomicUsize,
    hold_execution: AtomicBool,
    hold_fetch: AtomicBool,
    hold_startup: AtomicBool,
    execution_error: AtomicBool,
    hold_cleanup: AtomicBool,
    injected_outcome: Mutex<Option<ProgramOutcome>>,
    injected_error: Mutex<Option<worker_api::Error>>,
    observed_events: Mutex<Vec<Value>>,
    observed_processing: Mutex<Vec<Option<TraceContext>>>,
}

struct Store(Arc<Counts>);
impl ProgramStore for Store {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async { panic!("delivery must use the assignment's immutable binding") })
    }

    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async {
            self.0.fetches.fetch_add(1, Ordering::SeqCst);
            while self.0.hold_fetch.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            self.0.fetch_completions.fetch_add(1, Ordering::SeqCst);
            Ok(vec![1])
        })
    }
}

#[derive(Default)]
struct Cache(Mutex<HashMap<Digest, PreparedArtifact>>);
impl ArtifactCache for Cache {
    fn lookup<'a>(&'a self, d: &'a ProgramDescriptor) -> PortFuture<'a, Option<PreparedArtifact>> {
        Box::pin(async { Ok(self.0.lock().unwrap().get(&d.digest).cloned()) })
    }

    fn publish<'a>(
        &'a self,
        d: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        Box::pin(async {
            let artifact = PreparedArtifact::new(
                PathBuf::from("/unused-delivery-test-artifact"),
                ProgramManifest {
                    schema_version: 1,
                    program: d.program.clone(),
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
                },
                d.digest.clone(),
                Arc::new(()),
            );
            self.0
                .lock()
                .unwrap()
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
            self.0.startup_entries.fetch_add(1, Ordering::SeqCst);
            while self.0.hold_startup.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            control.check()?;
            let pid = self.0.starts.fetch_add(1, Ordering::SeqCst) + 1;
            let live = self.0.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak.fetch_max(live, Ordering::SeqCst);
            Ok(StartOutcome::Ready(Box::new(Session {
                counts: self.0.clone(),
                pid: pid as u32,
                alive: true,
                _artifact: artifact,
            })))
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
        self.counts
            .observed_processing
            .lock()
            .unwrap()
            .push(invocation.processing_context);
        let event = invocation.event;
        Box::pin(async move {
            self.counts.executions.fetch_add(1, Ordering::SeqCst);
            self.counts
                .observed_events
                .lock()
                .unwrap()
                .push(event.value().clone());
            loop {
                if let Err(error) = control.check() {
                    self.counts.cancelled.fetch_add(1, Ordering::SeqCst);
                    return Err(error);
                }
                if !self.counts.hold_execution.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            if let Some(error) = self.counts.injected_error.lock().unwrap().clone() {
                return Err(error);
            }
            if let Some(outcome) = self.counts.injected_outcome.lock().unwrap().clone() {
                return Ok(outcome);
            }
            if self.counts.execution_error.load(Ordering::SeqCst) {
                return Err(worker_api::Error::new(
                    ErrorKind::Runtime,
                    "injected execution failure",
                ));
            }
            Ok(ProgramOutcome::Success {
                output: event.into_value(),
            })
        })
    }

    fn close(&mut self) -> PortFuture<'_, ()> {
        Box::pin(async {
            self.counts.closes.fetch_add(1, Ordering::SeqCst);
            if self.counts.hold_cleanup.load(Ordering::SeqCst) {
                return Err(worker_api::Error::new(
                    ErrorKind::Io,
                    "injected cleanup remains unconfirmed",
                ));
            }
            let delay = self.counts.close_delay_ms.load(Ordering::SeqCst);
            if delay != 0 {
                self.counts.slow_closes.fetch_add(1, Ordering::SeqCst);
                // This step makes no progress until its delay completes. Dropping
                // the future retains process ownership and restarts the delay.
                tokio::time::sleep(Duration::from_millis(delay)).await;
                while self.counts.hold_slow_cleanup.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
            self.stop();
            Ok(())
        })
    }
}

#[derive(Default)]
struct ServiceState {
    session: Option<WorkerSession>,
    next_task: usize,
    acquisitions: Vec<AcquireCommand>,
    acquisition_waits: Vec<Duration>,
    replies: HashMap<(u32, u64), AcquireReply>,
    active: HashMap<u32, String>,
    renewals: Vec<RenewCommand>,
    accepted_renewals: HashMap<String, RenewCommand>,
    settlements: Vec<SettleCommand>,
    accepted: HashMap<String, SettleCommand>,
    confirmations: Vec<LeaseOwner>,
    capacity_violations: usize,
}

struct Service {
    broker_commands: Mutex<Vec<ClaimCommand>>,
    broker_disposition: Mutex<Option<ClaimDisposition>>,
    malformed_claim: AtomicBool,
    state: Mutex<ServiceState>,
    task_count: usize,
    acquire_unavailable: AtomicBool,
    settle_unavailable: AtomicBool,
    renew_unavailable: AtomicBool,
    confirm_unavailable: AtomicBool,
    malformed_acquire: AtomicBool,
    malformed_dispatch_once: AtomicUsize,
    acquire_error: Mutex<Option<ContractError>>,
    acquire_panics: AtomicUsize,
    settle_panics: AtomicUsize,
    session_expired: AtomicBool,
    lost_acquire_replies: AtomicUsize,
    lost_dispatch_replies: AtomicUsize,
    lost_settlement_replies: AtomicUsize,
    keepalive_delay_ms: AtomicU64,
    first_acquire_delay_ms: AtomicU64,
    first_dispatch_delay_ms: AtomicU64,
    first_settlement_delay_ms: AtomicU64,
    stale_keepalive: AtomicBool,
    lease_ms: AtomicU64,
    extensions: AtomicUsize,
}

impl Service {
    fn new(task_count: usize) -> Arc<Self> {
        Arc::new(Self {
            broker_commands: Mutex::new(Vec::new()),
            broker_disposition: Mutex::new(None),
            malformed_claim: AtomicBool::new(false),
            state: Mutex::new(ServiceState::default()),
            task_count,
            acquire_unavailable: AtomicBool::new(false),
            settle_unavailable: AtomicBool::new(false),
            renew_unavailable: AtomicBool::new(false),
            confirm_unavailable: AtomicBool::new(false),
            malformed_acquire: AtomicBool::new(false),
            malformed_dispatch_once: AtomicUsize::new(0),
            acquire_error: Mutex::new(None),
            acquire_panics: AtomicUsize::new(0),
            settle_panics: AtomicUsize::new(0),
            session_expired: AtomicBool::new(false),
            lost_acquire_replies: AtomicUsize::new(0),
            lost_dispatch_replies: AtomicUsize::new(0),
            lost_settlement_replies: AtomicUsize::new(0),
            keepalive_delay_ms: AtomicU64::new(0),
            first_acquire_delay_ms: AtomicU64::new(0),
            first_dispatch_delay_ms: AtomicU64::new(0),
            first_settlement_delay_ms: AtomicU64::new(0),
            stale_keepalive: AtomicBool::new(false),
            lease_ms: AtomicU64::new(60_000),
            extensions: AtomicUsize::new(0),
        })
    }

    fn authority(&self, owner: &LeaseOwner, sequence: u64, dispatch: bool) -> Authority {
        Authority {
            owner: owner.clone(),
            expires_at: 1_000_000,
            remaining_ms: self.lease_ms.load(Ordering::SeqCst),
            execution_remaining_ms: 300_000,
            renew_sequence: sequence,
            cancel_requested: false,
            dispatch_allowed: dispatch,
        }
    }

    fn accepted_count(&self) -> usize {
        self.state.lock().unwrap().accepted.len()
    }
}

fn lose(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

fn unavailable() -> ContractError {
    ContractError::Unavailable("injected reply unavailable after mutation".into())
}

impl TaskService for Service {
    fn claim_dispatch<'a>(&'a self, command: &'a ClaimCommand) -> ContractFuture<'a, ClaimReply> {
        Box::pin(async move {
            self.broker_commands.lock().unwrap().push(command.clone());
            let injected = self.broker_disposition.lock().unwrap().clone();
            let disposition = if let Some(disposition) = injected {
                disposition
            } else {
                ClaimDisposition::Claimed {
                    reply: self
                        .acquire(
                            &command.acquisition,
                            AcquireOptions::immediate(Instant::now() + WAIT),
                        )
                        .await?,
                }
            };
            let mut echo = command.clone();
            if self.malformed_claim.load(Ordering::SeqCst) {
                echo.dispatch.task_id = "wrong-task".into();
            }
            Ok(ClaimReply {
                command: echo,
                disposition,
            })
        })
    }
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            let session = WorkerSession {
                id: "worker_session_test".into(),
                scope: scope.clone(),
                queue: queue.into(),
                concurrency,
                expires_at: u64::MAX,
            };
            self.state.lock().unwrap().session = Some(session.clone());
            Ok(session)
        })
    }

    fn extend_session<'a>(&'a self, id: &'a str) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            self.extensions.fetch_add(1, Ordering::SeqCst);
            if self.session_expired.load(Ordering::SeqCst) {
                return Err(ContractError::SessionExpired);
            }
            let session = self.state.lock().unwrap().session.clone().unwrap();
            assert_eq!(id, session.id);
            Ok(session)
        })
    }

    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, AcquireReply> {
        Box::pin(async move {
            let mut reply = {
                let mut state = self.state.lock().unwrap();
                state.acquisitions.push(command.clone());
                state.acquisition_waits.push(options.max_wait);
                let key = (command.consumer_id, command.sequence);
                if let Some(reply) = state.replies.get(&key) {
                    reply.clone()
                } else {
                    let session = state.session.as_ref().unwrap();
                    assert_eq!(command.scope, session.scope);
                    assert_eq!(command.queue, session.queue);
                    assert_eq!(command.worker_session_id, session.id);
                    assert!(command.consumer_id < session.concurrency);
                    if state.active.contains_key(&command.consumer_id) {
                        state.capacity_violations += 1;
                    }
                    let reply = if state.next_task == self.task_count {
                        AcquireReply::Empty {
                            sequence: command.sequence,
                        }
                    } else {
                        state.next_task += 1;
                        let number = state.next_task;
                        let owner = LeaseOwner {
                            scope: command.scope.clone(),
                            task_id: format!("task_{number}"),
                            attempt_id: format!("att_{number}"),
                            lease_id: format!("lease_{number}"),
                            generation: 1,
                            worker_session_id: command.worker_session_id.clone(),
                            consumer_id: command.consumer_id,
                        };
                        state
                            .active
                            .insert(command.consumer_id, owner.attempt_id.clone());
                        let event = CloudEvent::new(json!({
                            "specversion":"1.0", "id":format!("evt_{number}"),
                            "source":"urn:ledgence:orchestrator",
                            "type":"com.ledgence.task.invocation.requested.v1",
                            "datacontenttype":"application/json",
                            "ldgtenantid": command.scope.tenant_id,
                            "ldgnamespace": command.scope.namespace,
                            "ldgrunid":"run_invoice", "ldgtaskid":owner.task_id,
                            "ldgattemptid":owner.attempt_id, "ldgattemptno":1,
                            "traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                            "data":{"invoice":"INV-1042","nested":[1,{"opaque":true}]}
                        }))
                        .unwrap();
                        AcquireReply::Assigned {
                            sequence: command.sequence,
                            assignment: Box::new(Assignment {
                                workflow_activation_id: None,
                                descriptor: descriptor(),
                                event,
                                lease: Lease {
                                    owner: owner.clone(),
                                    expires_at: 1_000_000,
                                },
                                authority: self.authority(&owner, 0, false),
                                attempt_deadline: 2_000_000,
                            }),
                        }
                    };
                    state.replies.insert(key, reply.clone());
                    reply
                }
            };
            if lose(&self.acquire_panics) {
                panic!("injected acquire adapter panic after committed mutation");
            }
            if let Some(error) = self.acquire_error.lock().unwrap().clone() {
                return Err(error);
            }
            if self.malformed_acquire.load(Ordering::SeqCst)
                && let AcquireReply::Assigned { assignment, .. } = &mut reply
            {
                assignment.lease.owner.scope.namespace = "wrong_namespace".into();
            }
            let delay = self.first_acquire_delay_ms.swap(0, Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if self.acquire_unavailable.load(Ordering::SeqCst) || lose(&self.lost_acquire_replies) {
                Err(unavailable())
            } else {
                Ok(reply)
            }
        })
    }

    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        Box::pin(async move {
            if self.session_expired.load(Ordering::SeqCst) {
                return Err(ContractError::SessionExpired);
            }
            {
                let mut state = self.state.lock().unwrap();
                state.renewals.push(command.clone());
                if let Some(previous) = state.accepted_renewals.get(&command.owner.attempt_id) {
                    if command.sequence == previous.sequence {
                        assert_eq!(command, previous, "renew retry changed its operation");
                    } else {
                        assert_eq!(command.sequence, previous.sequence + 1);
                    }
                } else {
                    assert_eq!(command.sequence, 1);
                }
                state
                    .accepted_renewals
                    .insert(command.owner.attempt_id.clone(), command.clone());
            }
            if command.intent == RenewIntent::KeepAlive {
                tokio::time::sleep(Duration::from_millis(
                    self.keepalive_delay_ms.load(Ordering::SeqCst),
                ))
                .await;
                if self.renew_unavailable.load(Ordering::SeqCst) {
                    return Err(unavailable());
                }
                if self.stale_keepalive.load(Ordering::SeqCst) {
                    return Ok(self.authority(&command.owner, command.sequence - 1, true));
                }
            } else {
                let delay = self.first_dispatch_delay_ms.swap(0, Ordering::SeqCst);
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if lose(&self.lost_dispatch_replies) {
                    return Err(unavailable());
                }
            }
            let mut authority = self.authority(
                &command.owner,
                command.sequence,
                command.intent == RenewIntent::Dispatch,
            );
            if command.intent == RenewIntent::Dispatch {
                match self.malformed_dispatch_once.swap(0, Ordering::SeqCst) {
                    0 => {}
                    1 => authority.owner.task_id = "wrong-task".into(),
                    2 => authority.renew_sequence += 1,
                    _ => panic!("unknown malformed Dispatch fixture"),
                }
            }
            Ok(authority)
        })
    }

    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        Box::pin(async move {
            let already_accepted = {
                let mut state = self.state.lock().unwrap();
                state.settlements.push(command.clone());
                if let Some(previous) = state.accepted.get(&command.owner.attempt_id) {
                    assert_eq!(
                        serde_json::to_value(command).unwrap(),
                        serde_json::to_value(previous).unwrap(),
                        "settlement retry changed immutable report or identity"
                    );
                    true
                } else {
                    state
                        .accepted
                        .insert(command.owner.attempt_id.clone(), command.clone());
                    if command.quiescence == Quiescence::Confirmed {
                        state.active.remove(&command.owner.consumer_id);
                    }
                    false
                }
            };
            if lose(&self.settle_panics) {
                panic!("injected settle adapter panic after committed mutation");
            }
            let delay = self.first_settlement_delay_ms.swap(0, Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if self.settle_unavailable.load(Ordering::SeqCst) || lose(&self.lost_settlement_replies)
            {
                return Err(unavailable());
            }
            Ok(SettleReply {
                receipt: SettlementReceipt {
                    operation_id: command.operation_id.clone(),
                    task_id: command.owner.task_id.clone(),
                    attempt_id: command.owner.attempt_id.clone(),
                    accepted_at: 1_000_000,
                },
                already_accepted,
                task_state: if command.quiescence == Quiescence::Confirmed {
                    TaskState::Succeeded
                } else {
                    TaskState::Active
                },
            })
        })
    }

    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            state.confirmations.push(owner.clone());
            state.active.remove(&owner.consumer_id);
            if self.confirm_unavailable.load(Ordering::SeqCst) {
                Err(unavailable())
            } else {
                Ok(TaskState::Failed)
            }
        })
    }

    fn submit<'a>(&'a self, _: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async { Err(ContractError::NotFound) })
    }
    fn inspect<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async { Err(ContractError::NotFound) })
    }
    fn list_tasks<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a TaskListQuery,
    ) -> ContractFuture<'a, TaskPage> {
        Box::pin(async {
            Err(ContractError::Unavailable(
                "list unused in this fixture".into(),
            ))
        })
    }
    fn status<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskStatus> {
        Box::pin(async { Err(ContractError::Unavailable("unused test observation".into())) })
    }
    fn result<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskResult> {
        Box::pin(async { Err(ContractError::Unavailable("unused test observation".into())) })
    }
    fn inspect_attempt<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        Box::pin(async { Err(ContractError::NotFound) })
    }
    fn history<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        Box::pin(async { Err(ContractError::NotFound) })
    }
    fn cancel<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskState> {
        Box::pin(async { Err(ContractError::NotFound) })
    }
}

fn setup(concurrency: usize) -> (Worker, Arc<Counts>) {
    setup_with_fetch_timeout(concurrency, Duration::from_secs(2))
}

fn setup_with_fetch_timeout(concurrency: usize, fetch_timeout: Duration) -> (Worker, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let worker = Worker::new(
        WorkerConfig {
            concurrency,
            fetch_timeout,
        },
        Arc::new(Store(counts.clone())),
        Arc::new(Cache::default()),
        Arc::new(Runtime(counts.clone())),
    )
    .unwrap();
    (worker, counts)
}

fn config() -> DeliveryConfig {
    let mut config = DeliveryConfig::new(scope(), "invoices");
    config.idle_delay = Duration::from_millis(5);
    config.retry_delay = Duration::from_millis(5);
    config.request_timeout = Duration::from_millis(100);
    config.renew_interval = Duration::from_millis(20);
    config.session_extend_interval = Duration::from_millis(50);
    config
}

async fn wait_for(predicate: impl Fn() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("condition did not become true before timeout");
}

#[tokio::test]
async fn unknown_replies_preserve_assignment_dispatch_and_report_without_rerunning() {
    let (worker, counts) = setup(1);
    let service = Service::new(2);
    service.lost_acquire_replies.store(1, Ordering::SeqCst);
    service.lost_dispatch_replies.store(1, Ordering::SeqCst);
    service.lost_settlement_replies.store(1, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| handle.status().settled_attempts == 2).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    assert!(status.finished);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 2);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.session.as_ref().unwrap().concurrency, 1);
    assert_eq!(state.acquisitions[0], state.acquisitions[1]);
    let dispatch: Vec<_> = state
        .renewals
        .iter()
        .filter(|command| {
            command.intent == RenewIntent::Dispatch
                && command.owner.attempt_id == "att_1"
                && command.sequence == 1
        })
        .collect();
    assert_eq!(dispatch.len(), 2, "{dispatch:?}");
    assert_eq!(dispatch[0], dispatch[1]);
    assert_eq!(
        state
            .settlements
            .iter()
            .filter(|command| command.owner.attempt_id == "att_1")
            .count(),
        2
    );
    assert_eq!(state.accepted.len(), 2);
    assert_eq!(state.capacity_violations, 0);
    let events = counts.observed_events.lock().unwrap();
    for event in events.iter() {
        let command = &state.accepted[event["ldgattemptid"].as_str().unwrap()];
        let AttemptReport::Completed(report) = &command.report else {
            panic!("expected completed program");
        };
        let ProgramOutcome::Success { output } = &report.outcome else {
            panic!("expected success");
        };
        assert_eq!(output, event);
        assert_eq!(
            event["data"],
            json!({"invoice":"INV-1042","nested":[1,{"opaque":true}]})
        );
    }
}

#[tokio::test]
async fn worker_capacity_is_reserved_before_polling_and_is_the_only_concurrency_limit() {
    for concurrency in [1, 2] {
        let (worker, counts) = setup(concurrency);
        counts.hold_execution.store(true, Ordering::SeqCst);
        let reserved = worker
            .reserve_consumer(RunControl::new(WAIT))
            .await
            .unwrap();
        let service = Service::new(6);
        let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
            .unwrap()
            .start();
        wait_for(|| service.state.lock().unwrap().session.is_some()).await;
        if concurrency == 2 {
            wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(service.state.lock().unwrap().next_task, concurrency - 1);
        assert_eq!(counts.executions.load(Ordering::SeqCst), concurrency - 1);
        assert_eq!(worker.stats().await.active_consumers, concurrency);
        reserved.release();
        wait_for(|| counts.executions.load(Ordering::SeqCst) == concurrency).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(service.state.lock().unwrap().next_task, concurrency);
        assert_eq!(counts.peak.load(Ordering::SeqCst), concurrency);
        counts.hold_execution.store(false, Ordering::SeqCst);
        wait_for(|| handle.status().settled_attempts == 6).await;
        handle.shutdown(WAIT).await.unwrap();
        assert_eq!(counts.executions.load(Ordering::SeqCst), 6);
        assert_eq!(counts.peak.load(Ordering::SeqCst), concurrency);
        assert_eq!(service.state.lock().unwrap().capacity_violations, 0);
    }
}

#[tokio::test]
async fn unavailable_renewals_stop_execution_at_the_last_conservative_lease_deadline() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    service.lease_ms.store(5_180, Ordering::SeqCst);
    service.renew_unavailable.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    wait_for(|| service.accepted_count() == 1).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(counts.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    let report = service.state.lock().unwrap().accepted["att_1"]
        .report
        .clone();
    let AttemptReport::Failed(failure) = report else {
        panic!("expired authority must stop work");
    };
    assert!(matches!(
        failure.error.kind,
        ErrorKind::Cancelled | ErrorKind::TimedOut
    ));
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(worker.stats().await.process_slots, 0);
}

#[tokio::test]
async fn a_slow_renewal_cannot_keep_work_running_past_authority_or_revive_it() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    service.lease_ms.store(5_180, Ordering::SeqCst);
    service.keepalive_delay_ms.store(350, Ordering::SeqCst);
    let mut settings = config();
    settings.request_timeout = Duration::from_secs(1);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), settings)
        .unwrap()
        .start();
    wait_for(|| {
        service
            .state
            .lock()
            .unwrap()
            .renewals
            .iter()
            .any(|r| r.intent == RenewIntent::KeepAlive)
    })
    .await;
    service.lease_ms.store(60_000, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_millis(280), async {
        wait_for(|| counts.cancelled.load(Ordering::SeqCst) == 1).await;
    })
    .await
    .expect("lease watchdog must run while the renewal RPC is pending");
    wait_for(|| service.accepted_count() == 1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    handle.shutdown(WAIT).await.unwrap();
}

#[tokio::test]
async fn stale_renewal_replies_do_not_refresh_the_confirmed_lease() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    service.lease_ms.store(5_180, Ordering::SeqCst);
    service.stale_keepalive.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    tokio::time::timeout(Duration::from_millis(500), async {
        wait_for(|| service.accepted_count() == 1).await;
    })
    .await
    .expect("stale replies must not keep extending authority");
    assert_eq!(counts.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    handle.shutdown(WAIT).await.unwrap();
}

#[tokio::test]
async fn shutdown_retains_unknown_acquisition_until_the_original_assignment_is_reconciled() {
    let (worker, counts) = setup(1);
    let service = Service::new(2);
    service.acquire_unavailable.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.state.lock().unwrap().next_task == 1).await;
    assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
    assert!(!handle.status().finished);
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    service.acquire_unavailable.store(false, Ordering::SeqCst);
    let status = handle.shutdown(WAIT).await.unwrap();
    assert!(status.finished);
    assert_eq!(service.accepted_count(), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    let state = service.state.lock().unwrap();
    assert!(
        state
            .acquisitions
            .iter()
            .all(|c| c == &state.acquisitions[0])
    );
    let AttemptReport::Failed(failure) = &state.accepted["att_1"].report else {
        panic!("stopping driver must not dispatch recovered work");
    };
    assert!(!failure.execution_may_have_started);
}

#[tokio::test]
async fn shutdown_retains_unknown_settlement_and_replays_the_report_without_new_work() {
    let (worker, counts) = setup(1);
    let service = Service::new(2);
    service.settle_unavailable.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.accepted_count() == 1).await;
    let accepted = serde_json::to_value(&service.state.lock().unwrap().accepted["att_1"]).unwrap();
    assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    service.settle_unavailable.store(false, Ordering::SeqCst);
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.active_consumers, 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.next_task, 1);
    assert!(state.settlements.len() >= 2);
    assert!(
        state
            .settlements
            .iter()
            .all(|command| serde_json::to_value(command).unwrap() == accepted)
    );
}

#[tokio::test]
async fn unconfirmed_cleanup_retains_capacity_until_separate_confirmation() {
    let (worker, counts) = setup(1);
    counts.execution_error.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(true, Ordering::SeqCst);
    let service = Service::new(2);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.accepted_count() == 1).await;
    let accepted = {
        let state = service.state.lock().unwrap();
        assert_eq!(state.accepted["att_1"].quiescence, Quiescence::Unconfirmed);
        assert!(state.confirmations.is_empty());
        serde_json::to_value(&state.accepted["att_1"]).unwrap()
    };
    assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(worker.stats().await.process_slots, 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    service.confirm_unavailable.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(false, Ordering::SeqCst);
    wait_for(|| !service.state.lock().unwrap().confirmations.is_empty()).await;
    assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(worker.stats().await.process_slots, 0);
    service.confirm_unavailable.store(false, Ordering::SeqCst);
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(worker.stats().await.active_consumers, 0);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.next_task, 1);
    assert_eq!(state.capacity_violations, 0);
    assert!(state.confirmations.len() >= 2);
    assert!(
        state
            .confirmations
            .iter()
            .all(|owner| owner == &state.accepted["att_1"].owner)
    );
    assert_eq!(
        serde_json::to_value(&state.accepted["att_1"]).unwrap(),
        accepted
    );
}

#[tokio::test]
async fn shutdown_cancels_running_user_work_and_settles_its_failure() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(2);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    let status = handle.shutdown(WAIT).await.unwrap();
    assert!(status.finished);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.next_task, 1);
    let AttemptReport::Failed(failure) = &state.accepted["att_1"].report else {
        panic!("expected cancellation failure");
    };
    assert_eq!(failure.error.kind, ErrorKind::Cancelled);
    assert!(failure.execution_may_have_started);
}

#[tokio::test]
async fn idle_consumers_advance_completed_empty_polls_and_extend_the_session() {
    let (worker, counts) = setup(1);
    let service = Service::new(0);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.extensions.load(Ordering::SeqCst) >= 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    let state = service.state.lock().unwrap();
    assert!(state.acquisitions.len() > 1);
    assert!(
        state
            .acquisitions
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1)
    );
    assert_eq!(state.capacity_violations, 0);
}

#[tokio::test]
async fn request_timeouts_replay_committed_operations_without_duplicate_execution() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service.first_acquire_delay_ms.store(150, Ordering::SeqCst);
    service.first_dispatch_delay_ms.store(150, Ordering::SeqCst);
    service
        .first_settlement_delay_ms
        .store(150, Ordering::SeqCst);
    let mut settings = config();
    settings.request_timeout = Duration::from_millis(50);
    let mut handle = DeliveryDriver::new(worker, service.clone(), settings)
        .unwrap()
        .start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    let state = service.state.lock().unwrap();
    assert_eq!(state.acquisitions[0], state.acquisitions[1]);
    let dispatch: Vec<_> = state
        .renewals
        .iter()
        .filter(|command| command.intent == RenewIntent::Dispatch && command.sequence == 1)
        .collect();
    assert_eq!(dispatch.len(), 2, "{dispatch:?}");
    assert_eq!(dispatch[0], dispatch[1]);
    assert_eq!(state.settlements.len(), 2);
    assert_eq!(
        serde_json::to_value(&state.settlements[0]).unwrap(),
        serde_json::to_value(&state.settlements[1]).unwrap()
    );
    assert_eq!(state.accepted.len(), 1);
    assert_eq!(state.capacity_violations, 0);
}

#[tokio::test]
async fn cancelling_wait_does_not_cancel_the_retained_driver_or_user_work() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    let mut handle = DeliveryDriver::new(worker.clone(), service, config())
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), handle.wait())
            .await
            .is_err()
    );
    assert!(!handle.status().stopping);
    assert_eq!(counts.cancelled.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.active_consumers, 1);
    counts.hold_execution.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_handle_keeps_unknown_settlement_owned_until_reconciled() {
    let (worker, counts) = setup(1);
    let service = Service::new(2);
    service.settle_unavailable.store(true, Ordering::SeqCst);
    let handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.accepted_count() == 1).await;
    drop(handle);
    wait_for(|| service.state.lock().unwrap().settlements.len() >= 3).await;
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    service.settle_unavailable.store(false, Ordering::SeqCst);
    tokio::time::timeout(WAIT, async {
        loop {
            let stats = worker.stats().await;
            if stats.active_consumers == 0 && stats.process_slots == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("retained supervisor failed to complete after handle drop");
    assert_eq!(service.state.lock().unwrap().next_task, 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn malformed_or_conflicting_acquisition_keeps_the_original_cursor_and_capacity() {
    for malformed in [true, false] {
        let (worker, counts) = setup(1);
        let service = Service::new(2);
        if malformed {
            service.malformed_acquire.store(true, Ordering::SeqCst);
        } else {
            *service.acquire_error.lock().unwrap() = Some(ContractError::Conflict);
        }
        let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
            .unwrap()
            .start();
        wait_for(|| service.state.lock().unwrap().next_task == 1).await;
        assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
        assert_eq!(worker.stats().await.active_consumers, 1);
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        service.malformed_acquire.store(false, Ordering::SeqCst);
        *service.acquire_error.lock().unwrap() = None;
        handle.shutdown(WAIT).await.unwrap();
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        assert_eq!(worker.stats().await.active_consumers, 0);
        let state = service.state.lock().unwrap();
        assert_eq!(state.next_task, 1);
        assert_eq!(state.accepted.len(), 1);
        assert!(state.acquisitions.len() >= 2);
        assert!(
            state
                .acquisitions
                .iter()
                .all(|command| command == &state.acquisitions[0])
        );
    }
}

#[tokio::test]
async fn adapter_panics_after_mutations_replay_the_original_operations() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service.acquire_panics.store(1, Ordering::SeqCst);
    service.settle_panics.store(1, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.acquisitions[0], state.acquisitions[1]);
    assert_eq!(state.settlements.len(), 2);
    assert_eq!(
        serde_json::to_value(&state.settlements[0]).unwrap(),
        serde_json::to_value(&state.settlements[1]).unwrap()
    );
    assert_eq!(state.accepted.len(), 1);
}

#[tokio::test]
async fn oversized_success_business_failure_and_runtime_errors_are_bounded_before_settlement() {
    for kind in ["success", "business_failure", "runtime_error"] {
        let (worker, counts) = setup(1);
        let oversized = "x".repeat(SETTLEMENT_MAX_BYTES);
        match kind {
            "success" => {
                *counts.injected_outcome.lock().unwrap() = Some(ProgramOutcome::Success {
                    output: json!({"oversized": oversized}),
                })
            }
            "business_failure" => {
                *counts.injected_outcome.lock().unwrap() = Some(ProgramOutcome::Failure {
                    kind: "business".into(),
                    message: oversized,
                })
            }
            _ => {
                *counts.injected_error.lock().unwrap() =
                    Some(worker_api::Error::new(ErrorKind::Runtime, oversized))
            }
        }
        let service = Service::new(1);
        service.lost_settlement_replies.store(1, Ordering::SeqCst);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .start();
        wait_for(|| handle.status().settled_attempts == 1).await;
        handle.shutdown(WAIT).await.unwrap();
        assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
        let state = service.state.lock().unwrap();
        assert_eq!(state.settlements.len(), 2);
        let first = serde_json::to_vec(&state.settlements[0]).unwrap();
        assert!(first.len() < SETTLEMENT_MAX_BYTES);
        assert!(SettleCommand::decode(&first).is_ok());
        assert_eq!(first, serde_json::to_vec(&state.settlements[1]).unwrap());
        let AttemptReport::Failed(failure) = &state.accepted["att_1"].report else {
            panic!("oversized {kind} must become a bounded failure before the first report");
        };
        assert_eq!(failure.error.kind, ErrorKind::Protocol);
        assert!(failure.execution_may_have_started);
    }
}

#[tokio::test]
async fn session_expiry_does_not_discard_an_accepted_report_with_an_unknown_reply() {
    let (worker, counts) = setup(1);
    let service = Service::new(2);
    service.settle_unavailable.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.accepted_count() == 1).await;
    let accepted = serde_json::to_vec(&service.state.lock().unwrap().accepted["att_1"]).unwrap();
    service.session_expired.store(true, Ordering::SeqCst);
    wait_for(|| handle.status().stopping).await;
    assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
    assert_eq!(worker.stats().await.active_consumers, 1);
    service.settle_unavailable.store(false, Ordering::SeqCst);
    let status = handle.shutdown(WAIT).await.unwrap();
    assert_eq!(status.settled_attempts, 1);
    assert_eq!(status.lost_attempts, 0);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    let state = service.state.lock().unwrap();
    assert_eq!(state.next_task, 1);
    assert!(
        state
            .settlements
            .iter()
            .all(|command| serde_json::to_vec(command).unwrap() == accepted)
    );
}

#[tokio::test]
async fn renewal_and_expiry_monitor_continue_during_fetch_and_startup() {
    for during_fetch in [true, false] {
        let (worker, counts) = setup(1);
        counts.hold_fetch.store(during_fetch, Ordering::SeqCst);
        counts.hold_startup.store(!during_fetch, Ordering::SeqCst);
        let service = Service::new(1);
        service.lease_ms.store(5_180, Ordering::SeqCst);
        let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
            .unwrap()
            .start();
        wait_for(|| {
            if during_fetch {
                counts.fetches.load(Ordering::SeqCst) == 1
            } else {
                counts.startup_entries.load(Ordering::SeqCst) == 1
            }
        })
        .await;
        wait_for(|| {
            service
                .state
                .lock()
                .unwrap()
                .renewals
                .iter()
                .any(|command| command.intent == RenewIntent::KeepAlive)
        })
        .await;
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        service.renew_unavailable.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Recovery of the service may extend reporting authority, but it cannot
        // revive user-work permission that expired while preparation was held.
        let before = service.state.lock().unwrap().renewals.len();
        service.lease_ms.store(60_000, Ordering::SeqCst);
        service.renew_unavailable.store(false, Ordering::SeqCst);
        wait_for(|| service.state.lock().unwrap().renewals.len() > before + 1).await;
        counts.hold_fetch.store(false, Ordering::SeqCst);
        counts.hold_startup.store(false, Ordering::SeqCst);
        wait_for(|| handle.status().settled_attempts == 1).await;
        handle.shutdown(WAIT).await.unwrap();
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        assert_eq!(worker.stats().await.active_consumers, 0);
        let state = service.state.lock().unwrap();
        let AttemptReport::Failed(failure) = &state.accepted["att_1"].report else {
            panic!("expired preparation must never reach execution");
        };
        assert_eq!(failure.error.kind, ErrorKind::Cancelled);
        assert_eq!(
            failure.phase,
            if during_fetch {
                Phase::Preparation
            } else {
                Phase::Startup
            }
        );
        assert!(!failure.execution_may_have_started);
    }
}

#[tokio::test]
async fn fetch_response_timeout_drains_until_owned_fetch_finishes_and_confirms_separately() {
    let (worker, counts) = setup_with_fetch_timeout(1, Duration::from_millis(50));
    counts.hold_fetch.store(true, Ordering::SeqCst);
    let service = Service::new(2);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.accepted_count() == 1).await;
    let accepted = {
        let state = service.state.lock().unwrap();
        let command = &state.accepted["att_1"];
        assert_eq!(command.quiescence, Quiescence::Unconfirmed);
        let AttemptReport::Failed(failure) = &command.report else {
            panic!("fetch timeout must report a preparation failure");
        };
        assert_eq!(failure.phase, Phase::Preparation);
        assert_eq!(failure.error.kind, ErrorKind::TimedOut);
        assert!(!failure.execution_may_have_started);
        assert!(state.confirmations.is_empty());
        serde_json::to_vec(command).unwrap()
    };
    assert!(handle.status().stopping);
    assert!(handle.shutdown(Duration::from_millis(60)).await.is_err());
    assert_eq!(worker.stats().await.active_consumers, 1);
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(counts.fetch_completions.load(Ordering::SeqCst), 0);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(service.state.lock().unwrap().next_task, 1);

    counts.hold_fetch.store(false, Ordering::SeqCst);
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.fetch_completions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.confirmations.len(), 1);
    assert_eq!(state.confirmations[0], state.accepted["att_1"].owner);
    assert_eq!(
        serde_json::to_vec(&state.accepted["att_1"]).unwrap(),
        accepted
    );
    assert_eq!(state.next_task, 1);
}

#[tokio::test]
async fn slow_warm_cleanup_survives_repeated_shutdown_wait_timeouts() {
    let (worker, counts) = setup(1);
    counts.close_delay_ms.store(250, Ordering::SeqCst);
    counts.hold_slow_cleanup.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    let mut handle = DeliveryDriver::new(worker.clone(), service, config())
        .unwrap()
        .start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    assert_eq!(worker.stats().await.warm_processes, 1);

    // A caller stops waiting while the retained driver still owns cleanup. The
    // completion gate makes these observations independent of scheduler pauses.
    for _ in 0..4 {
        assert!(handle.shutdown(Duration::from_millis(40)).await.is_err());
        assert!(!handle.status().finished);
        assert_eq!(worker.stats().await.process_slots, 1);
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    }
    assert_eq!(counts.slow_closes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.closes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);

    counts.hold_slow_cleanup.store(false, Ordering::SeqCst);
    let status = handle.shutdown(WAIT).await.unwrap();
    assert!(status.finished);
    assert_eq!(counts.slow_closes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.closes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    assert_eq!(worker.stats().await.process_slots, 0);
    assert_eq!(worker.stats().await.active_consumers, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repaired_slow_cleanup_confirms_quiescence_without_rewriting_the_report() {
    let (worker, counts) = setup(1);
    counts.execution_error.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(true, Ordering::SeqCst);
    let service = Service::new(2);
    service.lost_settlement_replies.store(1, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| handle.status().settled_attempts == 1).await;
    let accepted = {
        let state = service.state.lock().unwrap();
        assert_eq!(state.accepted["att_1"].quiescence, Quiescence::Unconfirmed);
        assert!(state.confirmations.is_empty());
        serde_json::to_vec(&state.accepted["att_1"]).unwrap()
    };

    // Repair the initial error, then keep the slow successful close observable
    // until all short caller waits have ended. No external cleanup is needed.
    counts.close_delay_ms.store(250, Ordering::SeqCst);
    counts.hold_slow_cleanup.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(false, Ordering::SeqCst);
    wait_for(|| counts.slow_closes.load(Ordering::SeqCst) == 1).await;
    for _ in 0..4 {
        assert!(handle.shutdown(Duration::from_millis(40)).await.is_err());
        assert!(!handle.status().finished);
        let stats = worker.stats().await;
        assert_eq!(stats.active_consumers, 1);
        assert_eq!(stats.process_slots, 1);
        assert_eq!(counts.live.load(Ordering::SeqCst), 1);
        assert!(service.state.lock().unwrap().confirmations.is_empty());
    }
    assert_eq!(counts.slow_closes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);

    counts.hold_slow_cleanup.store(false, Ordering::SeqCst);
    let status = handle.shutdown(WAIT).await.unwrap();
    assert!(status.finished);
    assert_eq!(status.settled_attempts, 1);
    assert_eq!(counts.slow_closes.load(Ordering::SeqCst), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    let stats = worker.stats().await;
    assert_eq!(stats.active_consumers, 0);
    assert_eq!(stats.process_slots, 0);
    let state = service.state.lock().unwrap();
    assert_eq!(state.next_task, 1);
    assert_eq!(state.capacity_violations, 0);
    assert_eq!(state.accepted.len(), 1);
    assert_eq!(
        state.confirmations,
        vec![state.accepted["att_1"].owner.clone()]
    );
    assert!(state.settlements.len() >= 2);
    assert!(state.settlements.iter().all(|command| {
        command.quiescence == Quiescence::Unconfirmed
            && serde_json::to_vec(command).unwrap() == accepted
    }));
    assert_eq!(
        serde_json::to_vec(&state.accepted["att_1"]).unwrap(),
        accepted
    );
}

#[derive(Default)]
struct RecordingTraceBridge {
    parents: Mutex<Vec<(String, Option<TraceContext>)>>,
}
impl worker_api::TraceBridge for RecordingTraceBridge {
    fn set_parent(&self, span: &tracing::Span, parent: Option<&TraceContext>) {
        if let Some(metadata) = span.metadata() {
            self.parents
                .lock()
                .unwrap()
                .push((metadata.name().into(), parent.cloned()));
        }
    }
    fn add_link(&self, _: &tracing::Span, _: &TraceContext) {}
    fn context(&self, span: &tracing::Span) -> Option<TraceContext> {
        span.id().map(|id| TraceContext {
            traceparent: format!(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-{:016x}-01",
                id.into_u64()
            ),
            tracestate: Some("test=value".into()),
        })
    }
}

#[tokio::test]
async fn processing_context_is_frozen_across_replies_and_normalization_and_separate_from_event() {
    // Exercise heterogeneous scoped dispatch: other parallel tests intentionally
    // have no subscriber. Avoid tracing-core's single-dispatch callsite shortcut.
    let _other_dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::sink)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    for normalize in [false, true] {
        let bridge = Arc::new(RecordingTraceBridge::default());
        let (worker, counts) = setup(1);
        let worker = worker.with_trace_bridge(bridge.clone());
        if normalize {
            *counts.injected_outcome.lock().unwrap() = Some(ProgramOutcome::Success {
                output: json!({"oversized": "x".repeat(SETTLEMENT_MAX_BYTES)}),
            });
        }
        let service = Service::new(1);
        service.lost_acquire_replies.store(1, Ordering::SeqCst);
        service.lost_settlement_replies.store(1, Ordering::SeqCst);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .start();
        wait_for(|| handle.status().settled_attempts == 1).await;
        handle.shutdown(WAIT).await.unwrap();
        let state = service.state.lock().unwrap();
        assert_eq!(state.settlements.len(), 2);
        let command = &state.settlements[0];
        let processing = command
            .processing_trace
            .as_ref()
            .expect("actual processing context");
        assert_eq!(
            serde_json::to_vec(command).unwrap(),
            serde_json::to_vec(&state.settlements[1]).unwrap()
        );
        assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
        let executions = counts.observed_processing.lock().unwrap();
        let execution = executions[0].as_ref().expect("separate execution context");
        assert_ne!(processing, execution);
        let events = counts.observed_events.lock().unwrap();
        assert_ne!(events[0]["traceparent"], processing.traceparent);
        let parents = bridge.parents.lock().unwrap();
        let (_, parent) = parents
            .iter()
            .find(|(name, _)| name == "ledgence.program.execute")
            .unwrap();
        assert_eq!(parent.as_ref(), Some(processing));
        let (_, origin) = parents
            .iter()
            .find(|(name, _)| name == "ledgence.attempt.process")
            .unwrap();
        assert_eq!(
            origin.as_ref().unwrap().traceparent,
            events[0]["traceparent"]
        );
        if normalize {
            assert!(matches!(command.report, AttemptReport::Failed(_)));
        }
    }
}

#[tokio::test]
async fn stopping_a_long_poll_reconciles_immediately_without_releasing_its_sequence() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service
        .first_acquire_delay_ms
        .store(20_000, Ordering::SeqCst);
    let mut settings = config();
    settings.request_timeout = Duration::from_secs(30);
    let mut handle = DeliveryDriver::new(worker, service.clone(), settings)
        .unwrap()
        .start();
    wait_for(|| service.state.lock().unwrap().acquisitions.len() == 1).await;
    handle.stop();
    let status = handle.shutdown(Duration::from_secs(2)).await.unwrap();
    assert!(status.finished);
    let state = service.state.lock().unwrap();
    assert_eq!(state.acquisitions.len(), 2);
    assert_eq!(state.acquisitions[0], state.acquisitions[1]);
    assert_eq!(
        state.acquisition_waits,
        [Duration::from_secs(20), Duration::ZERO]
    );
    assert!(
        state
            .renewals
            .iter()
            .all(|command| command.intent == RenewIntent::KeepAlive)
    );
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(state.accepted.len(), 1);
}

#[tokio::test]
async fn stopping_during_initial_confirmation_replays_sent_dispatch_without_running_user_code() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service
        .first_dispatch_delay_ms
        .store(5_000, Ordering::SeqCst);
    service.settle_unavailable.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.state.lock().unwrap().renewals.len() == 1).await;
    handle.stop();
    wait_for(|| {
        service
            .state
            .lock()
            .unwrap()
            .renewals
            .iter()
            .any(|command| command.intent == RenewIntent::KeepAlive)
    })
    .await;
    {
        let state = service.state.lock().unwrap();
        assert_eq!(state.renewals[0], state.renewals[1]);
        assert_eq!(state.renewals[0].sequence, 1);
        assert_eq!(state.renewals[0].intent, RenewIntent::Dispatch);
        assert!(state.accepted.len() == 1);
    }
    assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    service.settle_unavailable.store(false, Ordering::SeqCst);
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
}

#[tokio::test]
async fn processing_span_duration_excludes_acquisition_hold_and_empty_polls_create_no_span() {
    use tracing::{field::Visit, span};
    use tracing_subscriber::{Layer, Registry, layer::Context, prelude::*};

    struct Timing {
        id: span::Id,
        started: Instant,
        recorded: Option<(Duration, Instant)>,
    }
    struct Timings(Arc<Mutex<Vec<Timing>>>);
    #[derive(Default)]
    struct DurationField(Option<Duration>);
    impl Visit for DurationField {
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            if field.name() == "ledgence.duration_ms" {
                self.0 = Some(Duration::from_millis(value.try_into().unwrap()));
            }
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }
    impl Layer<Registry> for Timings {
        fn on_new_span(
            &self,
            attributes: &span::Attributes<'_>,
            id: &span::Id,
            _: Context<'_, Registry>,
        ) {
            if attributes.metadata().name() == "ledgence.attempt.process" {
                self.0.lock().unwrap().push(Timing {
                    id: id.clone(),
                    started: Instant::now(),
                    recorded: None,
                });
            }
        }
        fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _: Context<'_, Registry>) {
            let mut timings = self.0.lock().unwrap();
            if let Some(timing) = timings.iter_mut().find(|timing| &timing.id == id) {
                let mut duration = DurationField::default();
                values.record(&mut duration);
                if let Some(duration) = duration.0 {
                    assert!(
                        timing.recorded.is_none(),
                        "processing duration changed after recording"
                    );
                    timing.recorded = Some((duration, Instant::now()));
                }
            }
        }
    }

    // Keep callsite interest valid while parallel tests use other dispatchers.
    let _other_dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
    let timings = Arc::new(Mutex::new(Vec::new()));
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(Timings(timings.clone())),
    );
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    let hold = Duration::from_millis(500);
    service.first_acquire_delay_ms.store(500, Ordering::SeqCst);
    let mut settings = config();
    settings.request_timeout = WAIT;
    let started = Instant::now();
    let mut handle = DeliveryDriver::new(worker, service.clone(), settings)
        .unwrap()
        .start();
    // Execution cannot finish until released. Measure an interval wholly inside
    // processing so zero or underreported durations cannot satisfy the test.
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    let execution_held_since = Instant::now();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let execution_held = execution_held_since.elapsed();
    counts.hold_execution.store(false, Ordering::SeqCst);
    // Observe a subsequent Empty poll as well as the completed assignment.
    wait_for(|| service.state.lock().unwrap().acquisitions.len() >= 2).await;
    assert_eq!(handle.status().settled_attempts, 1);
    handle.shutdown(WAIT).await.unwrap();
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    let timings = timings.lock().unwrap();
    assert_eq!(
        timings.len(),
        1,
        "Empty acquisition created a processing span"
    );
    let timing = &timings[0];
    let (duration, recorded) = timing
        .recorded
        .expect("integer processing duration was recorded");
    assert!(timing.started.duration_since(started) >= hold - Duration::from_millis(5));
    // Compare to the observed span lifetime, not a machine-speed assumption:
    // slow execution or cleanup may increase both values and still passes.
    // The reported integer milliseconds may round down by less than 1 ms.
    assert!(
        duration + Duration::from_millis(1) >= execution_held,
        "processing duration {duration:?} omitted the controlled execution interval {execution_held:?}"
    );
    assert!(duration <= recorded.duration_since(timing.started) + Duration::from_millis(100));
    assert!(duration + hold / 2 < recorded.duration_since(started));
}

#[tokio::test]
async fn malformed_initial_dispatch_stops_execution_and_reconciles_exact_sent_command() {
    for malformed in [1, 2] {
        let (worker, counts) = setup(1);
        let service = Service::new(1);
        service
            .malformed_dispatch_once
            .store(malformed, Ordering::SeqCst);
        // Retain cleanup while the supervisor reconciles the malformed reply
        // with a subsequent valid authority and moves to KeepAlive.
        service.settle_unavailable.store(true, Ordering::SeqCst);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .start();
        wait_for(|| {
            let state = service.state.lock().unwrap();
            state.accepted.contains_key("att_1")
                && state
                    .renewals
                    .iter()
                    .any(|command| command.intent == RenewIntent::KeepAlive)
        })
        .await;
        assert!(handle.status().stopping);
        assert_eq!(service.malformed_dispatch_once.load(Ordering::SeqCst), 0);
        {
            let state = service.state.lock().unwrap();
            assert_eq!(state.renewals[0], state.renewals[1]);
            assert_eq!(state.renewals[0].sequence, 1);
            assert_eq!(state.renewals[0].intent, RenewIntent::Dispatch);
            assert_eq!(state.renewals[2].sequence, 2);
            assert_eq!(state.renewals[2].intent, RenewIntent::KeepAlive);
            let AttemptReport::Failed(failure) = &state.accepted["att_1"].report else {
                panic!("expected cancellation before preparation");
            };
            assert_eq!(failure.error.kind, ErrorKind::Cancelled);
            assert!(!failure.execution_may_have_started);
        }
        assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
        assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        service.settle_unavailable.store(false, Ordering::SeqCst);
        assert!(handle.shutdown(WAIT).await.unwrap().finished);
        // The valid retry reconciles authority but must never revive local work.
        assert_eq!(counts.fetches.load(Ordering::SeqCst), 0);
        assert_eq!(counts.starts.load(Ordering::SeqCst), 0);
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    }
}

#[path = "delivery/broker.rs"]
mod broker;

#[path = "delivery/sqs.rs"]
mod sqs;
