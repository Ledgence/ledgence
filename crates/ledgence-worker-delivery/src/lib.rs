//! Supervised delivery over [`TaskService`], independent of its transport/store.
//!
//! Reserve capacity before acquisition; retain it through ambiguous replies and
//! required cleanup. Dropping a handle requests shutdown, not abandonment. The
//! Tokio runtime must remain alive until shutdown finishes. Process crashes rely
//! on service expiry/retry; this driver has no local durable recovery journal.

mod attempt;
mod binding;
mod observations;
pub mod poll_plan;
mod provider;
mod renewal;
mod source;
mod task_admission;
mod workflow;
pub use source::{AcquisitionSource, BrokerAcquisitionSource, SourceReply};

use ledgence_orchestration_api::*;
use ledgence_worker_api::RunControl;
use ledgence_worker_core::Worker;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tracing::{Instrument, instrument::WithSubscriber};

const TICK: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    pub scope: Scope,
    pub queue: String,
    /// Maximum time an acquisition may wait for work; zero requests an immediate poll.
    pub acquire_wait: Duration,
    /// Minimum interval between new acquisition sequences after a confirmed Empty.
    pub idle_delay: Duration,
    pub retry_delay: Duration,
    pub request_timeout: Duration,
    pub renew_interval: Duration,
    pub session_extend_interval: Duration,
}
impl DeliveryConfig {
    pub fn new(scope: Scope, queue: impl Into<String>) -> Self {
        Self {
            scope,
            queue: queue.into(),
            acquire_wait: Duration::from_millis(LONG_POLL_WAIT_MS),
            idle_delay: Duration::from_secs(1),
            retry_delay: Duration::from_millis(250),
            request_timeout: Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS),
            renew_interval: Duration::from_millis(RENEW_INTERVAL_MS),
            session_extend_interval: Duration::from_secs(60 * 60),
        }
    }
    fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        for duration in [
            self.idle_delay,
            self.retry_delay,
            self.request_timeout,
            self.renew_interval,
            self.session_extend_interval,
        ] {
            if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
                return Err(ContractError::InvalidInput(
                    "delivery durations must be positive and representable".into(),
                ));
            }
        }
        if self.acquire_wait > Duration::from_millis(LONG_POLL_WAIT_MS)
            || self.request_timeout > Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS)
            || self.renew_interval > Duration::from_millis(RENEW_INTERVAL_MS)
            || self.session_extend_interval > Duration::from_millis(SESSION_VALIDITY_MS / 2)
        {
            return Err(ContractError::InvalidInput(
                "delivery timing exceeds the service contract".into(),
            ));
        }
        Ok(())
    }
}

/// Bounded local diagnostics. A settled attempt has a validated durable receipt;
/// lost attempts have an authoritative rejection, not merely a request timeout.
#[derive(Debug, Clone, Default)]
pub struct DeliveryStatus {
    /// Primary queue registration, also used to publish physical observations once.
    pub session_id: Option<String>,
    /// Ready registrations for this worker. All share the same physical consumers.
    pub route_sessions: BTreeMap<String, String>,
    pub stopping: bool,
    pub finished: bool,
    pub settled_attempts: u64,
    pub lost_attempts: u64,
    pub last_error: Option<ContractError>,
}

struct Shared {
    stop: AtomicBool,
    stop_changed: tokio::sync::Notify,
    status: Mutex<DeliveryStatus>,
}
impl Shared {
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }
    fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.stop_changed.notify_waiters();
    }
    async fn stopped(&self) {
        let changed = self.stop_changed.notified();
        tokio::pin!(changed);
        // Register before checking the monotone flag, closing the check/sleep
        // race without waking every idle consumer on a periodic timer.
        changed.as_mut().enable();
        if !self.stopping() {
            changed.await;
        }
    }
    fn status(&self) -> DeliveryStatus {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        status.stopping = self.stopping();
        status
    }
    fn error(&self, error: ContractError) {
        tracing::warn!(error = %error, "worker delivery operation failed");
        self.status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_error = Some(error);
    }
    fn fatal(&self, error: ContractError) {
        self.error(error);
        self.stop();
    }
}

enum AcquisitionSources {
    Integrated,
    Single(Arc<dyn AcquisitionSource>),
    Routed(BTreeMap<String, Arc<dyn AcquisitionSource>>),
}

/// Pair the adapter with the exact session it registered, through final drain.
struct RegisteredRoute {
    session: WorkerSession,
    source: Arc<dyn AcquisitionSource>,
}

pub struct DeliveryDriver {
    worker: Worker,
    service: Arc<dyn TaskService>,
    workflows: Option<Arc<dyn WorkflowService>>,
    effect_access: Option<Arc<EffectAccess>>,
    runtime_binding: Option<Arc<EffectAccess>>,
    runtime_features: ledgence_worker_api::RuntimeFeatures,
    sources: AcquisitionSources,
    planner: Option<poll_plan::PollPlanner>,
    config: DeliveryConfig,
    observations: Option<observations::ObservationReporting>,
}
impl DeliveryDriver {
    pub fn new(
        worker: Worker,
        service: Arc<dyn TaskService>,
        config: DeliveryConfig,
    ) -> Result<Self> {
        config.validate()?;
        u32::try_from(worker.concurrency()).map_err(|_| {
            ContractError::InvalidInput("worker concurrency exceeds session capacity".into())
        })?;
        Ok(Self {
            worker,
            sources: AcquisitionSources::Integrated,
            planner: None,
            service,
            workflows: None,
            effect_access: None,
            runtime_binding: None,
            runtime_features: ledgence_worker_api::RuntimeFeatures::default(),
            config,
            observations: None,
        })
    }
    /// Attach optional best-effort reporting without changing execution authority.
    /// The publisher should have independent transport resource limits.
    pub fn with_observation_publisher(
        mut self,
        publisher: Arc<dyn ledgence_orchestration_api::console::WorkerObservationPublisher>,
        display_name: Option<String>,
    ) -> Result<Self> {
        if let Some(name) = &display_name {
            validate_text(name, 128)?;
        }
        self.observations = Some(observations::ObservationReporting {
            publisher,
            display_name,
        });
        Ok(self)
    }
    /// Supply the optional workflow control port used only by marked activations.
    pub fn with_workflows(mut self, service: Arc<dyn WorkflowService>) -> Self {
        self.workflows = Some(service);
        self
    }

    /// Enable v2 only after authenticating remote capability discovery. The
    /// cached audience and stable worker identity fence every returned receipt.
    pub fn with_effect_evidence(
        mut self,
        audience: AccessAudience,
        identity: PrincipalIdentity,
    ) -> Result<Self> {
        audience.validate()?;
        identity.validate()?;
        if audience.scope != self.config.scope {
            return Err(ContractError::Forbidden);
        }
        self.workflows
            .as_ref()
            .ok_or_else(|| {
                ContractError::Unavailable("effect evidence requires workflow delivery".into())
            })?
            .effect_evidence_client()?;
        self.effect_access = Some(Arc::new(EffectAccess { audience, identity }));
        Ok(self)
    }

    /// Attach a startup-authenticated audience independently of effect evidence.
    pub fn with_runtime_binding(
        mut self,
        audience: AccessAudience,
        identity: PrincipalIdentity,
    ) -> Result<Self> {
        audience.validate()?;
        identity.validate()?;
        if audience.scope != self.config.scope {
            return Err(ContractError::Forbidden);
        }
        self.runtime_binding = Some(Arc::new(EffectAccess { audience, identity }));
        Ok(self)
    }

    /// Use only the features authenticated by this worker connection's startup
    /// discovery. Merely implementing a client trait does not establish support.
    /// Generic features do not confer effect authority; known effect features
    /// require an effect binding attached before this call.
    pub fn with_runtime_features(
        mut self,
        mut features: ledgence_worker_api::RuntimeFeatures,
    ) -> Result<Self> {
        features
            .validate()
            .map_err(|_| ContractError::InvalidInput("invalid runtime features".into()))?;
        if features.native_effect_resolution() && self.effect_access.is_none() {
            return Err(ContractError::Unavailable(
                "native effect features require authenticated effect evidence".into(),
            ));
        }
        if features.provider_operation() {
            self.workflows
                .as_ref()
                .ok_or_else(|| {
                    ContractError::Unavailable(
                        "provider operations require workflow delivery".into(),
                    )
                })?
                .provider_operation_client()?;
        }
        if features.resource_admission() {
            if self.runtime_binding.is_none() {
                return Err(ContractError::Unavailable(
                    "resource admission requires authenticated runtime binding".into(),
                ));
            }
            self.workflows
                .as_ref()
                .ok_or_else(|| {
                    ContractError::Unavailable(
                        "resource admission requires workflow delivery".into(),
                    )
                })?
                .admission_client()?;
        }
        if features.provider_admission() {
            self.workflows
                .as_ref()
                .ok_or_else(|| {
                    ContractError::Unavailable(
                        "provider admission requires workflow delivery".into(),
                    )
                })?
                .provider_admission_client()?;
        }
        if features.task_admission()
            && !self
                .worker
                .supports_admitted_invocation()
                .map_err(|error| ContractError::Unavailable(error.to_string()))?
        {
            // Discovery says what the service supports. A worker with a legacy
            // runtime must open an ordinary session; otherwise it would claim
            // governed work and charge a retry without being able to execute.
            features
                .supported
                .retain(|name| name != TASK_ADMISSION_FEATURE);
        }
        if features.task_admission() {
            if self.runtime_binding.is_none() {
                return Err(ContractError::Unavailable(
                    "task admission requires authenticated runtime binding".into(),
                ));
            }
            self.service.task_admission_client()?;
        }
        self.runtime_features = features;
        Ok(self)
    }

    /// Use a pluggable source with the same N consumers and attempt lifecycle.
    pub fn with_acquisition_source(mut self, source: Arc<dyn AcquisitionSource>) -> Self {
        self.sources = AcquisitionSources::Single(source);
        self
    }

    /// Poll the planner's fixed integrated HTTP routes with one shared set of N
    /// consumers. Keep a clone to install revised weights on the live worker.
    pub fn with_poll_planner(mut self, planner: poll_plan::PollPlanner) -> Result<Self> {
        if !matches!(self.sources, AcquisitionSources::Integrated) {
            return Err(protocol(
                "use with_planned_sources to bind custom sources by queue",
            ));
        }
        self.validate_primary(&planner)?;
        self.planner = Some(planner);
        Ok(self)
    }

    /// Apply one poll planner across an explicit source per logical queue.
    /// Sources and their sessions stay fixed when weights change. Each worker
    /// consumer holds at most one bound receive, claim or active attempt.
    pub fn with_planned_sources(
        mut self,
        planner: poll_plan::PollPlanner,
        sources: BTreeMap<String, Arc<dyn AcquisitionSource>>,
    ) -> Result<Self> {
        self.validate_primary(&planner)?;
        if !matches!(self.sources, AcquisitionSources::Integrated) {
            return Err(protocol("acquisition sources have already been configured"));
        }
        if sources.len() != planner.queues().len()
            || planner.queues().iter().any(|q| !sources.contains_key(q))
        {
            return Err(protocol(
                "acquisition sources must match the poll plan queues exactly",
            ));
        }
        // A broker source owns a registry and can bind only one service session.
        // Reject accidental sharing before opening any remote registration.
        let mut distinct = std::collections::HashSet::new();
        if sources
            .values()
            .any(|s| !distinct.insert(Arc::as_ptr(s).cast::<()>()))
        {
            return Err(protocol(
                "each planned queue requires a distinct acquisition source",
            ));
        }
        self.sources = AcquisitionSources::Routed(sources);
        self.planner = Some(planner);
        Ok(self)
    }

    fn validate_primary(&self, planner: &poll_plan::PollPlanner) -> Result<()> {
        if !planner.queues().contains(&self.config.queue) {
            return Err(protocol(
                "the primary worker queue must be present in the poll plan",
            ));
        }
        Ok(())
    }

    /// Start one non-resumable service session per route and N supervised consumers total.
    /// Session expiry drains this driver; callers may start a new worker after
    /// shutdown. Never transplant old cursors into a new service session.
    pub fn start(self) -> DeliveryHandle {
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            stop_changed: tokio::sync::Notify::new(),
            status: Mutex::new(DeliveryStatus::default()),
        });
        let (done, receiver) = watch::channel(false);
        let handle = DeliveryHandle {
            shared: shared.clone(),
            done: receiver,
        };
        tokio::spawn(
            async move {
                self.run(shared.clone()).await;
                shared
                    .status
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .finished = true;
                done.send_replace(true);
            }
            .with_current_subscriber(),
        );
        handle
    }
    async fn run(self, shared: Arc<Shared>) {
        let queues = self.planner.as_ref().map_or_else(
            || std::slice::from_ref(&self.config.queue),
            |plan| plan.queues(),
        );
        // The legacy single-source builder cannot return an error. Reject its
        // combination with a planner before opening any remote session.
        if self.planner.is_some() && matches!(self.sources, AcquisitionSources::Single(_)) {
            shared.fatal(protocol(
                "use with_planned_sources to bind custom sources by queue",
            ));
        }
        let integrated: Arc<dyn AcquisitionSource> = Arc::new(source::ServiceAcquisitionSource {
            service: self.service.clone(),
        });
        let context = Arc::new(Context {
            worker: self.worker.clone(),
            service: self.service.clone(),
            workflows: self.workflows.clone(),
            effect_access: self.effect_access.clone(),
            runtime_binding: self.runtime_binding.clone(),
            runtime_features: self.runtime_features.clone(),
            config: self.config.clone(),
            shared: shared.clone(),
        });
        let mut routes = Vec::with_capacity(queues.len());
        let mut consumers = Vec::new();
        let (maintenance_done, done) = watch::channel(());
        let mut maintenance = Vec::new();
        for queue in queues {
            let Some(session) = self.open(queue, &shared).await else {
                break;
            };
            if routes
                .iter()
                .any(|other: &RegisteredRoute| other.session.id == session.id)
            {
                shared.fatal(protocol(
                    "registration reused a session identity across queues",
                ));
                break;
            }
            let source = match &self.sources {
                AcquisitionSources::Integrated => integrated.clone(),
                AcquisitionSources::Single(source) => source.clone(),
                AcquisitionSources::Routed(sources) => sources[queue].clone(),
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                source.start_session(&session)
            }))
            .unwrap_or_else(|_| {
                Err(ContractError::Unavailable(
                    "acquisition source panicked during session setup".into(),
                ))
            });
            // Also unwind a source whose initialization failed after allocating
            // local state. No consumer starts unless every route is ready.
            routes.push(RegisteredRoute {
                session: session.clone(),
                source,
            });
            if let Err(error) = result {
                shared.fatal(error);
                break;
            }
            // Earlier registrations stay alive even if a later route retries.
            let current = context.clone();
            let done = done.clone();
            maintenance.push(tokio::spawn(
                async move { current.maintain_session(session, done).await }
                    .with_current_subscriber(),
            ));
        }
        let routes: Arc<[RegisteredRoute]> = routes.into();
        let mut reporting = None;
        if !shared.stopping() && routes.len() == queues.len() {
            let primary = routes
                .iter()
                .map(|route| &route.session)
                .find(|s| s.queue == self.config.queue)
                .expect("the validated route set includes the primary queue");
            // The sessions represent logical routes of one process; publishing
            // the physical pool N times would falsely multiply observed capacity.
            let primary_links = (routes.len() > 1 && self.observations.is_some())
                .then(observations::PrimaryLinks::default);
            if let Some(observations) = self.observations {
                reporting = Some(observations.start(
                    self.worker.clone(),
                    primary.clone(),
                    primary_links.clone(),
                ));
            }
            {
                let mut status = shared.status.lock().unwrap_or_else(|p| p.into_inner());
                status.session_id = Some(primary.id.clone());
                status.route_sessions = routes
                    .iter()
                    .map(|route| (route.session.queue.clone(), route.session.id.clone()))
                    .collect();
            }
            for consumer in 0..primary.concurrency {
                let current = context.clone();
                let routes = routes.clone();
                let planner = self.planner.clone();
                let primary_links = primary_links.clone();
                consumers.push(tokio::spawn(
                    async move {
                        current
                            .consume(routes, consumer, planner, primary_links)
                            .await
                    }
                    .with_current_subscriber(),
                ));
            }
        }
        // Cleanup must run concurrently with reconciliation: both hold parts of
        // the same reservation. Waiting for consumers first would deadlock it.
        // Keep this future across observation ticks so a slow close is never
        // cancelled merely to inspect consumer progress or update status.
        let cleanup = cleanup_worker(&self.worker, &shared);
        tokio::pin!(cleanup);
        let mut cleanup_finished = false;
        while !consumers.is_empty() {
            if !self.worker.stats().await.accepting {
                shared.stop();
            }
            let mut index = 0;
            while index < consumers.len() {
                if consumers[index].is_finished() {
                    if let Err(error) = consumers.swap_remove(index).await {
                        shared.fatal(ContractError::Unavailable(format!(
                            "delivery supervisor failed: {error}"
                        )));
                    }
                } else {
                    index += 1;
                }
            }
            tokio::select! {
                _ = &mut cleanup, if !cleanup_finished => cleanup_finished = true,
                _ = tokio::time::sleep(TICK) => {},
            }
        }
        for route in routes.iter() {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                route.source.finish_session(&route.session.id);
            }))
            .is_err()
            {
                shared.fatal(ContractError::Unavailable(
                    "acquisition source panicked during session cleanup".into(),
                ));
            }
        }
        drop(maintenance_done);
        for handle in maintenance {
            if let Err(error) = handle.await {
                shared.error(ContractError::Unavailable(format!(
                    "session maintenance failed: {error}"
                )));
            }
        }
        shared.stop();
        if !cleanup_finished {
            cleanup.await;
        }
        // Signals a final attempt without waiting on observation network I/O.
        drop(reporting);
    }
    async fn open(&self, queue: &str, shared: &Shared) -> Option<WorkerSession> {
        while !shared.stopping() {
            if !self.worker.stats().await.accepting {
                shared.stop();
                return None;
            }
            let future = exchange(self.config.request_timeout, || async {
                if self.runtime_features.task_admission() {
                    self.service
                        .task_admission_client()?
                        .open_task_admission_session(&TaskAdmissionSessionCommand {
                            scope: self.config.scope.clone(),
                            queue: queue.to_owned(),
                            concurrency: self.worker.concurrency() as u32,
                        })
                        .await
                } else {
                    self.service
                        .open_session(&self.config.scope, queue, self.worker.concurrency() as u32)
                        .await
                }
            });
            tokio::pin!(future);
            let result = loop {
                tokio::select! {
                    result = &mut future => break result,
                    _ = tokio::time::sleep(TICK) => if shared.stopping() { return None; },
                }
            };
            match result {
                Ok(session)
                    if session.scope == self.config.scope
                        && session.queue == queue
                        && session.concurrency == self.worker.concurrency() as u32
                        && validate_text(&session.id, 128).is_ok()
                        && session
                            .features
                            .iter()
                            .any(|feature| feature == TASK_ADMISSION_FEATURE)
                            == self.runtime_features.task_admission() =>
                {
                    return Some(session);
                }
                Ok(_) => {
                    shared.fatal(protocol("session reply does not match worker registration"));
                    return None;
                }
                Err(error) if retryable(&error) => {
                    shared.error(error);
                    pause(self.config.retry_delay, shared).await;
                }
                Err(error) => {
                    shared.fatal(error);
                    return None;
                }
            }
        }
        None
    }
}

async fn cleanup_worker(worker: &Worker, shared: &Shared) {
    while !shared.stopping() {
        tokio::time::sleep(TICK).await;
    }
    loop {
        match worker.shutdown_until_quiescent().await {
            Ok(()) => return,
            Err(error) => {
                shared.error(ContractError::Unavailable(error.to_string()));
                // Retry only a completed error, preserving the owned cleanup
                // future for as long as the adapter still reports Pending.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Dropping this handle requests stop; the retained supervisor reconciles work.
/// Keep the runtime alive and use `wait`/`shutdown` to observe completion.
pub struct DeliveryHandle {
    shared: Arc<Shared>,
    done: watch::Receiver<bool>,
}
#[derive(Debug, Clone)]
pub struct ShutdownPending {
    pub status: DeliveryStatus,
}
impl std::fmt::Display for ShutdownPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "delivery shutdown still owns unresolved work")
    }
}
impl std::error::Error for ShutdownPending {}
impl DeliveryHandle {
    pub fn stop(&self) {
        self.shared.stop();
    }
    pub fn status(&self) -> DeliveryStatus {
        self.shared.status()
    }
    /// Cancellation-safe observation: dropping this future does not abort work.
    pub async fn wait(&mut self) -> DeliveryStatus {
        while !*self.done.borrow_and_update() {
            if self.done.changed().await.is_err() {
                self.shared.fatal(ContractError::Unavailable(
                    "delivery supervisor terminated unexpectedly".into(),
                ));
                break;
            }
        }
        self.status()
    }
    /// A timeout leaves the same handle and supervisor available for another wait.
    pub async fn shutdown(
        &mut self,
        timeout: Duration,
    ) -> std::result::Result<DeliveryStatus, ShutdownPending> {
        self.stop();
        match tokio::time::timeout(timeout, self.wait()).await {
            Ok(status) if status.finished => Ok(status),
            _ => Err(ShutdownPending {
                status: self.status(),
            }),
        }
    }
}
impl Drop for DeliveryHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone)]
struct EffectAccess {
    audience: AccessAudience,
    identity: PrincipalIdentity,
}
impl EffectAccess {
    fn matches(&self, worker: &EffectWorkerIdentity) -> bool {
        worker.audience == self.audience
            && worker.issuer == self.identity.issuer
            && worker.subject == self.identity.subject
    }
}
struct Context {
    worker: Worker,
    service: Arc<dyn TaskService>,
    workflows: Option<Arc<dyn WorkflowService>>,
    effect_access: Option<Arc<EffectAccess>>,
    runtime_binding: Option<Arc<EffectAccess>>,
    runtime_features: ledgence_worker_api::RuntimeFeatures,
    config: DeliveryConfig,
    shared: Arc<Shared>,
}
impl Context {
    async fn maintain_session(&self, session: WorkerSession, mut done: watch::Receiver<()>) {
        let mut delay = self.config.session_extend_interval;
        loop {
            tokio::select! {
                biased;
                _ = done.changed() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            let result = tokio::select! {
                biased;
                _ = done.changed() => return,
                result = exchange(self.config.request_timeout, || self.service.extend_session(&session.id)) => result,
            };
            match result {
                Ok(reply)
                    if reply.id == session.id
                        && reply.scope == session.scope
                        && reply.queue == session.queue
                        && reply.concurrency == session.concurrency
                        && reply.features == session.features =>
                {
                    delay = self.config.session_extend_interval
                }
                Ok(_) => {
                    self.shared
                        .fatal(protocol("extension changed session identity"));
                    return;
                }
                Err(error) if retryable(&error) => {
                    self.shared.error(error);
                    delay = self.config.retry_delay;
                }
                Err(error) => {
                    self.shared.fatal(error);
                    return;
                }
            }
        }
    }
    #[tracing::instrument(name = "delivery_consumer", skip_all, fields(
        tenant_id = %self.config.scope.tenant_id, namespace = %self.config.scope.namespace,
        queue = (routes.len() == 1).then(|| routes[0].session.queue.as_str()),
        worker_session_id = (routes.len() == 1).then(|| routes[0].session.id.as_str()),
        consumer_id = consumer
    ))]
    async fn consume(
        self: Arc<Self>,
        routes: Arc<[RegisteredRoute]>,
        consumer: u32,
        planner: Option<poll_plan::PollPlanner>,
        primary_links: Option<observations::PrimaryLinks>,
    ) {
        // Each durable acquisition cursor belongs to one (session, consumer),
        // even while the same physical consumer moves between queues.
        let mut commands: Vec<_> = routes
            .iter()
            .map(|route| AcquireCommand {
                scope: route.session.scope.clone(),
                queue: route.session.queue.clone(),
                worker_session_id: route.session.id.clone(),
                consumer_id: consumer,
                sequence: 1,
            })
            .collect();
        while !self.shared.stopping() {
            if let Some(planner) = &planner {
                tokio::select! {
                    biased;
                    _ = self.shared.stopped() => return,
                    _ = planner.wait_active() => {}
                }
            }
            let admission = RunControl::new(self.config.request_timeout);
            let reserve = self.worker.reserve_consumer(admission.clone());
            tokio::pin!(reserve);
            let reservation = loop {
                tokio::select! {
                    result = &mut reserve => break result,
                    _ = tokio::time::sleep(TICK) => if self.shared.stopping() {admission.cancel();},
                }
            };
            let reservation = match reservation {
                Ok(reservation) => reservation,
                Err(_) if self.shared.stopping() => return,
                Err(error) if error.kind == ledgence_worker_api::ErrorKind::TimedOut => continue,
                Err(error) => {
                    self.shared
                        .fatal(ContractError::Unavailable(error.to_string()));
                    return;
                }
            };
            let cycle_started = Instant::now();
            let Some((route, reply)) = binding::acquire(
                &routes,
                &commands,
                planner.as_ref(),
                &self.config,
                &self.shared,
            )
            .await
            else {
                // A stop or paused revision before handoff leaves no binding
                // to reconcile and releases this physical reservation.
                continue;
            };
            let command = &mut commands[route];
            let idle = matches!(reply, SourceReply::Idle);
            match reply {
                SourceReply::Completed(AcquireReply::Assigned { assignment, .. }) => {
                    let _link = primary_links
                        .as_ref()
                        .filter(|_| command.queue == self.config.queue)
                        .map(|links| links.track(&assignment.lease.owner));
                    self.attempt(reservation, *assignment, &command.queue).await
                }
                SourceReply::Idle | SourceReply::Completed(AcquireReply::Empty { .. }) => {
                    drop(reservation);
                    pause(
                        self.config
                            .idle_delay
                            .saturating_sub(cycle_started.elapsed()),
                        &self.shared,
                    )
                    .await;
                }
                SourceReply::Completed(AcquireReply::OwnershipLost { .. }) => {
                    self.shared
                        .status
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .lost_attempts += 1;
                }
                SourceReply::Discarded { .. } => drop(reservation),
                SourceReply::Stopped { error } => {
                    self.shared.fatal(error);
                    return;
                }
            }
            if idle {
                continue;
            }
            let Some(next) = command.sequence.checked_add(1) else {
                self.shared
                    .fatal(protocol("acquisition sequence exhausted"));
                return;
            };
            command.sequence = next;
        }
    }
}

async fn exchange<T, F: Future<Output = Result<T>>>(
    timeout: Duration,
    call: impl FnOnce() -> F,
) -> Result<T> {
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        ContractError::Unavailable("control request timed out; outcome is unknown".into())
    })?;
    exchange_until(deadline, call).await
}

async fn exchange_until<T, F: Future<Output = Result<T>>>(
    deadline: Instant,
    call: impl FnOnce() -> F,
) -> Result<T> {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let timed_out =
        || ContractError::Unavailable("control request timed out; outcome is unknown".into());
    if Instant::now() >= deadline {
        return Err(timed_out());
    }
    let future = catch_unwind(AssertUnwindSafe(call)).map_err(|_| {
        ContractError::Unavailable("control adapter panicked; outcome is unknown".into())
    })?;
    // Adapter construction can perform synchronous work before returning its
    // future. That time belongs to the same exchange budget.
    if Instant::now() >= deadline {
        return Err(timed_out());
    }
    tokio::pin!(future);
    let future = std::future::poll_fn(|cx| {
        catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))).unwrap_or_else(|_| {
            std::task::Poll::Ready(Err(ContractError::Unavailable(
                "control adapter panicked; outcome is unknown".into(),
            )))
        })
    });
    let result = tokio::time::timeout_at(deadline.into(), future)
        .await
        .unwrap_or_else(|_| Err(timed_out()));
    // A synchronous poll cannot be interrupted. Reject its late Ready reply
    // while retaining ordinary uncertain-outcome reconciliation semantics.
    if Instant::now() >= deadline {
        Err(timed_out())
    } else {
        result
    }
}
fn retryable(error: &ContractError) -> bool {
    matches!(error, ContractError::Unavailable(_) | ContractError::Busy)
}
fn protocol(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
async fn pause(duration: Duration, shared: &Shared) {
    let end = Instant::now() + duration;
    while !shared.stopping() && Instant::now() < end {
        tokio::time::sleep(TICK.min(end.saturating_duration_since(Instant::now()))).await;
    }
}

#[cfg(test)]
mod exchange_tests {
    use super::*;

    #[tokio::test]
    async fn synchronous_adapter_construction_consumes_the_exchange_budget() {
        let polled = AtomicBool::new(false);
        let result = exchange(Duration::from_millis(20), || {
            std::thread::sleep(Duration::from_millis(50));
            async {
                polled.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .await;
        assert!(matches!(result, Err(ContractError::Unavailable(_))));
        assert!(
            !polled.load(Ordering::SeqCst),
            "do not poll a returned future after construction exhausted the deadline"
        );
    }

    #[tokio::test]
    async fn synchronous_ready_after_deadline_is_an_uncertain_outcome() {
        let completed = AtomicBool::new(false);
        let result = exchange(Duration::from_millis(20), || {
            std::future::poll_fn(|_| {
                std::thread::sleep(Duration::from_millis(50));
                completed.store(true, Ordering::SeqCst);
                std::task::Poll::Ready(Ok(()))
            })
        })
        .await;
        assert!(completed.load(Ordering::SeqCst));
        assert!(
            matches!(result, Err(ContractError::Unavailable(_))),
            "Tokio timeout alone can accept a synchronous late Ready reply"
        );
    }
}
