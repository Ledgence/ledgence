use super::*;
use ledgence_orchestration_core::{LeaseState, LeaseTracker};

/// Acquiring an identity does not establish a local execution deadline: the
/// acquisition may have spent most of its exchange waiting before the claim.
enum Phase {
    AwaitingAuthority { deadline: Instant },
    Confirmed(RunControl),
    Stopped,
}

struct Permission {
    tracker: LeaseTracker,
    phase: Phase,
    ready: bool,
    consumed: bool,
}
impl Permission {
    fn stop(&mut self) {
        if let Phase::Confirmed(control) = &self.phase {
            control.cancel();
        }
        self.phase = Phase::Stopped;
        self.ready = false;
    }

    fn check(&mut self, stopping: bool, now: Instant) {
        let expired = match &self.phase {
            Phase::AwaitingAuthority { deadline } => now >= *deadline,
            Phase::Confirmed(control) => {
                self.tracker.state(now) != LeaseState::Active || control.check().is_err()
            }
            Phase::Stopped => true,
        };
        if stopping || expired {
            self.stop();
        }
    }

    fn stopped(&self) -> bool {
        matches!(self.phase, Phase::Stopped)
    }

    fn accept(
        &mut self,
        authority: &Authority,
        command: &RenewCommand,
        started: Instant,
        now: Instant,
    ) {
        // Initial confirmation has one fixed deadline across retries. A late
        // positive response cannot restore permission after that deadline.
        self.check(false, now);
        if self.stopped() {
            return;
        }
        if self.tracker.apply(authority, started, now).is_err() {
            self.stop();
            return;
        }
        if command.intent == RenewIntent::Dispatch {
            self.ready = self.tracker.check_dispatch(now).is_ok();
            if !self.ready {
                self.stop();
                return;
            }
            if matches!(self.phase, Phase::AwaitingAuthority { .. }) {
                self.phase = Phase::Confirmed(RunControl::with_deadline(
                    self.tracker
                        .execution_deadline()
                        .expect("confirmed authority has a deadline"),
                ));
            }
        }
    }
}

pub(super) struct Monitor {
    permission: Mutex<Permission>,
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
    pub fn new(assignment: &Assignment) -> Self {
        let phase = if assignment.authority.cancel_requested
            || assignment.authority.remaining_ms == 0
            || assignment.authority.execution_remaining_ms == 0
        {
            Phase::Stopped
        } else {
            Phase::AwaitingAuthority {
                deadline: Instant::now() + Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS),
            }
        };
        Self {
            permission: Mutex::new(Permission {
                tracker: LeaseTracker::new(assignment.lease.owner.clone()),
                phase,
                ready: false,
                consumed: false,
            }),
            done: AtomicBool::new(false),
        }
    }
    fn check(&self, shared: &Shared) {
        self.permission
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .check(shared.stopping(), Instant::now());
    }
    pub async fn dispatch(&self, shared: &Shared) -> Option<RunControl> {
        loop {
            self.check(shared);
            {
                let mut state = self.permission.lock().unwrap_or_else(|p| p.into_inner());
                if state.stopped() {
                    return None;
                }
                if state.ready {
                    let Phase::Confirmed(control) = &state.phase else {
                        unreachable!("dispatch permission requires confirmed authority");
                    };
                    let control = control.clone();
                    if state.tracker.check_dispatch(Instant::now()).is_ok()
                        && control.check().is_ok()
                    {
                        state.consumed = true;
                        return Some(control);
                    }
                    state.stop();
                    return None;
                }
            }
            tokio::time::sleep(TICK).await;
        }
    }
    pub(super) fn stop(&self) {
        self.permission
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stop();
    }
    pub async fn run(self: Arc<Self>, context: Arc<Context>, assignment: Assignment) {
        // Unexpected termination of renewal must revoke local work immediately,
        // including when no execution control has been created yet.
        let _stop_on_exit = StopOnExit(self.clone());
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
        let mut sent = false;
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
            // Before first send, stopping changes the intent to KeepAlive.
            // Once sent, every uncertain retry retains the exact command.
            if !sent
                && self
                    .permission
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .stopped()
            {
                command.intent = RenewIntent::KeepAlive;
            }
            sent = true;
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
                        state.accept(&authority, &command, started, Instant::now());
                    }
                    let Some(sequence) = command.sequence.checked_add(1) else {
                        context.shared.fatal(protocol("renewal sequence exhausted"));
                        self.stop();
                        return;
                    };
                    command.sequence = sequence;
                    sent = false;
                    let state = self.permission.lock().unwrap_or_else(|p| p.into_inner());
                    command.intent = if state.consumed || state.stopped() {
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

struct StopOnExit(Arc<Monitor>);
impl Drop for StopOnExit {
    fn drop(&mut self) {
        self.0.stop();
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

    struct PendingRenewal {
        command: RenewCommand,
        response: tokio::sync::oneshot::Sender<Result<Authority>>,
    }

    #[derive(Default)]
    struct Renewals {
        commands: Mutex<Vec<RenewCommand>>,
        controlled: Option<tokio::sync::mpsc::UnboundedSender<PendingRenewal>>,
    }
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
        fn acquire<'a>(
            &'a self,
            _: &'a AcquireCommand,
            _: AcquireOptions,
        ) -> ContractFuture<'a, AcquireReply> {
            unsupported()
        }
        fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
            Box::pin(async move {
                {
                    let mut commands = self.commands.lock().unwrap();
                    if self.controlled.is_none() {
                        assert_eq!(command.sequence, commands.len() as u64 + 1);
                    }
                    commands.push(command.clone());
                }
                if let Some(controlled) = &self.controlled {
                    let (response, received) = tokio::sync::oneshot::channel();
                    controlled
                        .send(PendingRenewal {
                            command: command.clone(),
                            response,
                        })
                        .unwrap_or_else(|_| panic!("test must receive each renewal request"));
                    return received
                        .await
                        .expect("test must resolve each renewal reply");
                }
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

    fn monitor_fixture(service: Arc<dyn TaskService>) -> (Assignment, Arc<Context>) {
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
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            stop_changed: tokio::sync::Notify::new(),
            status: Mutex::new(DeliveryStatus::default()),
        });
        let mut config = DeliveryConfig::new(scope, "queue");
        config.renew_interval = Duration::from_millis(1);
        config.retry_delay = Duration::from_millis(1);
        let context = Arc::new(Context {
            worker,
            service,
            config,
            shared,
        });
        (assignment, context)
    }

    #[tokio::test]
    async fn heartbeats_preserve_dispatch_until_the_consumer_takes_permission() {
        let service = Arc::new(Renewals::default());
        let (assignment, context) = monitor_fixture(service.clone());
        let shared = context.shared.clone();
        let monitor = Arc::new(Monitor::new(&assignment));
        let running = monitor.clone();
        let actor = tokio::spawn(async move { running.run(context, assignment).await });

        // Withhold consumer scheduling across multiple confirmed renewals. This
        // exercises the handoff race directly, without relying on scheduler luck.
        observe(|| service.commands.lock().unwrap().len() >= 3).await;
        assert!(
            service
                .commands
                .lock()
                .unwrap()
                .iter()
                .all(|command| command.intent == RenewIntent::Dispatch)
        );
        let control = monitor
            .dispatch(&shared)
            .await
            .expect("fresh dispatch permission");
        observe(|| {
            service
                .commands
                .lock()
                .unwrap()
                .iter()
                .any(|command| command.intent == RenewIntent::KeepAlive)
        })
        .await;
        assert!(control.check().is_ok());
        monitor.done.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), actor)
            .await
            .unwrap()
            .unwrap();
    }

    fn pending_permission(now: Instant) -> Permission {
        let owner = LeaseOwner {
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        };
        Permission {
            tracker: LeaseTracker::new(owner),
            phase: Phase::AwaitingAuthority {
                deadline: now + Duration::from_secs(30),
            },
            ready: false,
            consumed: false,
        }
    }

    #[test]
    fn fresh_confirmation_establishes_the_first_execution_control() {
        let now = Instant::now();
        let mut state = pending_permission(now);
        let command = RenewCommand {
            owner: state.tracker.owner().clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        };
        let mut response = authority(&command.owner, 1, true);
        response.execution_remaining_ms = 10_000;
        let sent = now + Duration::from_secs(15);
        state.accept(&response, &command, sent, sent + Duration::from_millis(10));
        let Phase::Confirmed(control) = &state.phase else {
            panic!("fresh authority was not established")
        };
        assert_eq!(control.deadline(), sent + Duration::from_secs(5));
        assert!(state.ready);
    }

    async fn next_renewal(
        requests: &mut tokio::sync::mpsc::UnboundedReceiver<PendingRenewal>,
    ) -> PendingRenewal {
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .expect("renewal actor did not send its next request")
            .expect("renewal actor unexpectedly stopped")
    }

    #[tokio::test]
    async fn the_initial_confirmation_deadline_is_fixed_and_late_permission_cannot_revive_it() {
        let (sent, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let service = Arc::new(Renewals {
            controlled: Some(sent),
            ..Renewals::default()
        });
        let (assignment, context) = monitor_fixture(service);
        let shared = context.shared.clone();
        let before_construction = Instant::now();
        let monitor = Arc::new(Monitor::new(&assignment));
        let constructed = Instant::now();
        {
            let state = monitor.permission.lock().unwrap();
            let Phase::AwaitingAuthority { deadline } = state.phase else {
                panic!("a live assignment must await initial authority")
            };
            assert!(deadline >= before_construction + Duration::from_secs(30));
            assert!(deadline <= constructed + Duration::from_secs(30));
        }
        let running = monitor.clone();
        let actor = running.run(context, assignment);
        let scenario = async {
            let first = next_renewal(&mut requests).await;
            let command = first.command;
            assert_eq!(command.sequence, 1);
            assert_eq!(command.intent, RenewIntent::Dispatch);
            first
                .response
                .send(Err(ContractError::Unavailable(
                    "uncertain renewal reply".into(),
                )))
                .unwrap();
            let second = next_renewal(&mut requests).await;
            assert_eq!(second.command, command);

            // Exercise real actor retries on both sides of the confirmation window.
            // std::time::Instant is the production clock, so deliberately use its
            // actual 30-second contract instead of a synthetic Permission deadline.
            tokio::time::sleep_until((constructed + Duration::from_secs(15)).into()).await;
            second
                .response
                .send(Err(ContractError::Unavailable(
                    "uncertain renewal reply".into(),
                )))
                .unwrap();
            let third = next_renewal(&mut requests).await;
            assert_eq!(third.command, command);
            tokio::time::sleep_until(
                (constructed + Duration::from_secs(30) + Duration::from_millis(100)).into(),
            )
            .await;
            assert!(
                tokio::time::timeout(Duration::from_secs(1), monitor.dispatch(&shared))
                    .await
                    .expect("the original confirmation deadline must stop dispatch")
                    .is_none()
            );

            // The reply is still fresh relative to its own request, but too late
            // for initial confirmation. Observe the next request to establish that
            // the actor processed the positive reply before checking permission.
            third
                .response
                .send(Ok(authority(&command.owner, command.sequence, true)))
                .unwrap();
            let fourth = next_renewal(&mut requests).await;
            assert_eq!(fourth.command.owner, command.owner);
            assert_eq!(fourth.command.sequence, command.sequence + 1);
            assert_eq!(fourth.command.intent, RenewIntent::KeepAlive);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), monitor.dispatch(&shared))
                    .await
                    .expect("late authority must not revive dispatch permission")
                    .is_none()
            );
            {
                let state = monitor.permission.lock().unwrap();
                assert!(state.stopped());
                assert!(state.tracker.execution_deadline().is_none());
            }
            monitor.done.store(true, Ordering::Release);
            fourth
                .response
                .send(Ok(authority(
                    &fourth.command.owner,
                    fourth.command.sequence,
                    false,
                )))
                .unwrap();
        };
        // Keep the actor owned by this future so assertion failures also drop
        // renewal work. The timeout bounds both the scenario and actor shutdown.
        tokio::time::timeout(Duration::from_secs(35), async {
            tokio::join!(actor, scenario);
        })
        .await
        .expect("initial confirmation scenario did not finish");
    }

    #[test]
    fn confirmed_expiry_cancels_the_existing_control_and_late_renewal_cannot_revive_it() {
        let now = Instant::now();
        let mut state = pending_permission(now);
        let mut command = RenewCommand {
            owner: state.tracker.owner().clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        };
        let mut response = authority(&command.owner, 1, true);
        response.remaining_ms = 6_000;
        state.accept(&response, &command, now, now);
        let Phase::Confirmed(control) = &state.phase else {
            panic!("fresh authority was not established")
        };
        let control = control.clone();
        state.check(false, now + Duration::from_secs(2));
        assert!(control.is_cancelled());
        command.sequence = 2;
        response = authority(&command.owner, 2, true);
        state.accept(
            &response,
            &command,
            now + Duration::from_secs(2),
            now + Duration::from_secs(2),
        );
        assert!(state.stopped());
        assert!(control.is_cancelled());
    }

    #[test]
    fn cancellation_or_denied_dispatch_never_creates_execution_control() {
        for cancelled in [false, true] {
            let now = Instant::now();
            let mut state = pending_permission(now);
            let command = RenewCommand {
                owner: state.tracker.owner().clone(),
                sequence: 1,
                intent: RenewIntent::Dispatch,
            };
            let mut response = authority(&command.owner, 1, false);
            response.cancel_requested = cancelled;
            state.accept(&response, &command, now, now);
            assert!(state.stopped());
            assert!(!state.ready);
        }
    }
}
