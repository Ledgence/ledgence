use super::*;
use ledgence_orchestration_core::{LeaseState, LeaseTracker};

pub(super) struct Permission {
    tracker: LeaseTracker,
    ready: bool,
    consumed: bool,
    stopped: bool,
}
pub(super) struct Monitor {
    permission: Mutex<Permission>,
    pub control: RunControl,
    pub done: AtomicBool,
}
impl Monitor {
    pub fn owner(&self) -> LeaseOwner {
        self.permission
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tracker
            .owner()
            .clone()
    }
    pub fn new(assignment: &Assignment, started: Instant) -> Self {
        let mut tracker = LeaseTracker::new(assignment.lease.owner.clone());
        let now = Instant::now();
        let stopped = tracker.apply(&assignment.authority, started, now).is_err();
        let control = RunControl::with_deadline(tracker.execution_deadline().unwrap_or(now));
        Self {
            permission: Mutex::new(Permission {
                tracker,
                ready: false,
                consumed: false,
                stopped,
            }),
            control,
            done: AtomicBool::new(false),
        }
    }
    fn check(&self, shared: &Shared) {
        let mut state = self.permission.lock().unwrap_or_else(|p| p.into_inner());
        if shared.stopping() {
            state.tracker.cancel();
        }
        if state.tracker.state(Instant::now()) != LeaseState::Active || self.control.is_cancelled()
        {
            state.stopped = true;
        }
        if state.stopped {
            self.control.cancel();
        }
    }
    pub async fn dispatch(&self, shared: &Shared) -> bool {
        loop {
            self.check(shared);
            {
                let mut state = self.permission.lock().unwrap_or_else(|p| p.into_inner());
                if state.stopped {
                    return false;
                }
                if state.ready {
                    let allowed = state.tracker.check_dispatch(Instant::now()).is_ok()
                        && self.control.check().is_ok();
                    state.consumed = allowed;
                    return allowed;
                }
            }
            tokio::time::sleep(TICK).await;
        }
    }
    fn stop(&self) {
        self.permission
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopped = true;
        self.control.cancel();
    }
    pub async fn run(self: Arc<Self>, context: Arc<Context>, assignment: Assignment) {
        // Unexpected termination of renewal must revoke local work immediately.
        let _cancel_on_exit = CancelOnExit(self.control.clone());
        let Some(sequence) = assignment.authority.renew_sequence.checked_add(1) else {
            context.shared.fatal(protocol("renewal sequence exhausted"));
            self.stop();
            return;
        };
        let mut command = RenewCommand {
            owner: assignment.lease.owner,
            sequence,
            intent: RenewIntent::Dispatch,
        };
        // Stopped work can still retain authority for cleanup/reporting. Do not
        // send a new dispatch intent if no request for it has gone out yet.
        self.check(&context.shared);
        if self.control.is_cancelled() {
            command.intent = RenewIntent::KeepAlive;
        }
        let mut next = Instant::now();
        loop {
            self.check(&context.shared);
            if self.done.load(Ordering::Acquire) {
                return;
            }
            if Instant::now() < next {
                tokio::time::sleep(TICK).await;
                continue;
            }
            let started = Instant::now();
            let result = {
                let request = exchange(context.config.request_timeout, || {
                    context.service.renew(&command)
                });
                tokio::pin!(request);
                loop {
                    tokio::select! {
                        result = &mut request => break result,
                        _ = tokio::time::sleep(TICK) => {
                            self.check(&context.shared);
                            if self.done.load(Ordering::Acquire) {return;}
                        }
                    }
                }
            };
            self.check(&context.shared);
            match result {
                Ok(authority) => {
                    if authority.owner != command.owner
                        || authority.renew_sequence != command.sequence
                    {
                        context
                            .shared
                            .fatal(protocol("renewal reply changed owner or revision"));
                        self.stop();
                        // A malformed response is not evidence of acceptance.
                        next = Instant::now() + context.config.retry_delay;
                        continue;
                    }
                    {
                        let mut state = self.permission.lock().unwrap_or_else(|p| p.into_inner());
                        if !state.stopped {
                            if state
                                .tracker
                                .apply(&authority, started, Instant::now())
                                .is_err()
                            {
                                state.stopped = true;
                                self.control.cancel();
                            } else if command.intent == RenewIntent::Dispatch {
                                state.ready = state.tracker.check_dispatch(Instant::now()).is_ok();
                                if !state.ready {
                                    state.stopped = true;
                                    self.control.cancel();
                                }
                            }
                        }
                    }
                    let Some(sequence) = command.sequence.checked_add(1) else {
                        context.shared.fatal(protocol("renewal sequence exhausted"));
                        self.stop();
                        return;
                    };
                    command.sequence = sequence;
                    let state = self.permission.lock().unwrap_or_else(|p| p.into_inner());
                    command.intent = if state.consumed || state.stopped {
                        RenewIntent::KeepAlive
                    } else {
                        // Preserve dispatch permission until the consumer has
                        // taken it. A faster heartbeat must not revoke an
                        // authorization that has not yet reached execution.
                        RenewIntent::Dispatch
                    };
                    next = Instant::now() + context.config.renew_interval;
                }
                Err(error) if retryable(&error) => {
                    context.shared.error(error);
                    next = Instant::now() + context.config.retry_delay;
                }
                Err(
                    error @ (ContractError::OwnershipLost
                    | ContractError::UnknownSession
                    | ContractError::SessionExpired
                    | ContractError::NotFound),
                ) => {
                    if matches!(
                        error,
                        ContractError::UnknownSession | ContractError::SessionExpired
                    ) {
                        context.shared.fatal(error);
                    } else {
                        context.shared.error(error);
                    }
                    self.stop();
                    return;
                }
                Err(error) => {
                    context.shared.fatal(error);
                    self.stop();
                    return;
                }
            }
        }
    }
}

struct CancelOnExit(RunControl);
impl Drop for CancelOnExit {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_worker_api::{
        ArtifactCache, CloudEvent, Digest, ExecutionRuntime, PortFuture, PreparedArtifact,
        ProgramDescriptor, ProgramRef, ProgramStore, StartOutcome,
    };
    use ledgence_worker_core::WorkerConfig;
    use serde_json::json;

    struct UnusedWorkerPorts;
    impl ProgramStore for UnusedWorkerPorts {
        fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
            unreachable!("lease monitoring must not resolve a program")
        }
        fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
            unreachable!("lease monitoring must not fetch a program")
        }
    }
    impl ArtifactCache for UnusedWorkerPorts {
        fn lookup<'a>(
            &'a self,
            _: &'a ProgramDescriptor,
        ) -> PortFuture<'a, Option<PreparedArtifact>> {
            unreachable!("lease monitoring must not read the cache")
        }
        fn publish<'a>(
            &'a self,
            _: &'a ProgramDescriptor,
            _: Vec<u8>,
        ) -> PortFuture<'a, PreparedArtifact> {
            unreachable!("lease monitoring must not write the cache")
        }
    }
    impl ExecutionRuntime for UnusedWorkerPorts {
        fn start<'a>(&'a self, _: PreparedArtifact, _: RunControl) -> PortFuture<'a, StartOutcome> {
            unreachable!("lease monitoring must not start a process")
        }
    }

    #[derive(Default)]
    struct Renewals(Mutex<Vec<RenewCommand>>);
    fn unsupported<'a, T>() -> ContractFuture<'a, T> {
        Box::pin(async { Err(ContractError::NotFound) })
    }
    impl TaskService for Renewals {
        fn open_session<'a>(
            &'a self,
            _: &'a Scope,
            _: &'a str,
            _: u32,
        ) -> ContractFuture<'a, WorkerSession> {
            unsupported()
        }
        fn extend_session<'a>(&'a self, _: &'a str) -> ContractFuture<'a, WorkerSession> {
            unsupported()
        }
        fn submit<'a>(&'a self, _: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot> {
            unsupported()
        }
        fn inspect<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskSnapshot> {
            unsupported()
        }
        fn inspect_attempt<'a>(
            &'a self,
            _: &'a Scope,
            _: &'a str,
            _: &'a str,
        ) -> ContractFuture<'a, AttemptSnapshot> {
            unsupported()
        }
        fn history<'a>(
            &'a self,
            _: &'a Scope,
            _: &'a str,
            _: u64,
        ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
            unsupported()
        }
        fn acquire<'a>(&'a self, _: &'a AcquireCommand) -> ContractFuture<'a, AcquireReply> {
            unsupported()
        }
        fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
            Box::pin(async move {
                let mut commands = self.0.lock().unwrap();
                assert_eq!(command.sequence, commands.len() as u64 + 1);
                commands.push(command.clone());
                Ok(authority(
                    &command.owner,
                    command.sequence,
                    command.intent == RenewIntent::Dispatch,
                ))
            })
        }
        fn settle<'a>(&'a self, _: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
            unsupported()
        }
        fn confirm_quiescence<'a>(&'a self, _: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
            unsupported()
        }
        fn cancel<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskState> {
            unsupported()
        }
    }

    fn authority(owner: &LeaseOwner, sequence: u64, dispatch: bool) -> Authority {
        Authority {
            owner: owner.clone(),
            expires_at: 60_000,
            remaining_ms: 60_000,
            execution_remaining_ms: 300_000,
            renew_sequence: sequence,
            cancel_requested: false,
            dispatch_allowed: dispatch,
        }
    }

    async fn observe(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("renewal actor did not reach the expected state");
    }

    #[tokio::test]
    async fn heartbeats_preserve_dispatch_until_the_consumer_takes_permission() {
        let scope = Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        };
        let owner = LeaseOwner {
            scope: scope.clone(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        };
        let assignment = Assignment {
            descriptor: ProgramDescriptor {
                program: ProgramRef {
                    id: "program".into(),
                    version: "1.0.0".into(),
                },
                digest: Digest(format!("sha256:{:064x}", 1)),
                size: 1,
            },
            event: CloudEvent::new(json!({
                "specversion": "1.0", "id": "event", "source": "urn:ledgence:orchestrator",
                "type": "com.ledgence.task.invocation.requested.v1",
                "datacontenttype": "application/json", "ldgtenantid": "tenant",
                "ldgnamespace": "namespace", "ldgrunid": "run", "ldgtaskid": "task",
                "ldgattemptid": "attempt", "ldgattemptno": 1, "data": null
            }))
            .unwrap(),
            lease: Lease {
                owner: owner.clone(),
                expires_at: 60_000,
            },
            authority: authority(&owner, 0, false),
            attempt_deadline: 300_000,
        };
        let ports = Arc::new(UnusedWorkerPorts);
        let worker = Worker::new(
            WorkerConfig {
                concurrency: 1,
                ..WorkerConfig::default()
            },
            ports.clone(),
            ports.clone(),
            ports,
        )
        .unwrap();
        let service = Arc::new(Renewals::default());
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            status: Mutex::new(DeliveryStatus::default()),
        });
        let mut config = DeliveryConfig::new(scope, "queue");
        config.renew_interval = Duration::from_millis(1);
        let context = Arc::new(Context {
            worker,
            service: service.clone(),
            config,
            shared: shared.clone(),
        });
        let monitor = Arc::new(Monitor::new(&assignment, Instant::now()));
        let running = monitor.clone();
        let actor = tokio::spawn(async move { running.run(context, assignment).await });

        // Withhold consumer scheduling across multiple confirmed renewals. This
        // exercises the handoff race directly, without relying on scheduler luck.
        observe(|| service.0.lock().unwrap().len() >= 3).await;
        assert!(
            service
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|command| command.intent == RenewIntent::Dispatch)
        );
        assert!(monitor.dispatch(&shared).await);
        observe(|| {
            service
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|command| command.intent == RenewIntent::KeepAlive)
        })
        .await;
        assert!(monitor.control.check().is_ok());
        monitor.done.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), actor)
            .await
            .unwrap()
            .unwrap();
    }
}
