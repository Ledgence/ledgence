//! Bounded local preparation and reusable execution.
//!
//! The caller supplies an immutable descriptor already bound to its logical task.
//! Distributed leases and settlement belong to the separate delivery driver.

use ledgence_worker_api::*;
// Keep existing worker-core imports source-compatible while adapters can depend
// on portable contracts without importing the execution implementation.
pub use ledgence_worker_api::{
    ExecutionContext, ExecutionFailure, ExecutionReport, ExecutionRequest, ExecutionResult, Phase,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::{Instrument, instrument::WithSubscriber};
mod reservation;
pub use reservation::ConsumerReservation;
use reservation::{ConsumerOwnership, ConsumerPermit};
mod supervision;
use supervision::{Completion, Registration, catch_call, catch_panic};

#[cfg(test)]
mod preparation_tests;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Also the maximum number of managed starting/warm/running/stopping processes.
    pub concurrency: usize,
    /// Result deadline for a shared store download. Non-abortable work retains
    /// its consumer reservation until completion, even after this deadline.
    pub fetch_timeout: Duration,
}
impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            fetch_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct WorkerStats {
    pub process_slots: usize,
    pub warm_processes: usize,
    pub active_consumers: usize,
    pub accepting: bool,
}

#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}
struct Inner {
    config: WorkerConfig,
    store: Arc<dyn ProgramStore>,
    cache: Arc<dyn ArtifactCache>,
    runtime: Arc<dyn ExecutionRuntime>,
    consumers: Arc<Semaphore>,
    pool: Mutex<Pool>,
    preparations: Mutex<HashMap<Digest, Weak<Mutex<()>>>>,
    registry: StdMutex<Registry>,
    shutdown_lock: Mutex<()>,
}
struct Registry {
    next_id: u64,
    accepting: bool,
    active: HashMap<u64, RunControl>,
    keys: HashSet<AttemptKey>,
    unresolved: HashSet<AttemptKey>,
    unresolved_operations: usize,
    reservations: Vec<Weak<ConsumerOwnership>>,
    unresolved_consumers: Vec<Arc<ConsumerOwnership>>,
    pending_settlement: HashMap<AttemptKey, Weak<ConsumerOwnership>>,
}
#[derive(Clone, PartialEq, Eq, Hash)]
struct AttemptKey {
    tenant: String,
    namespace: String,
    attempt: String,
}
#[derive(Default)]
struct Pool {
    occupied: usize,
    idle: Vec<Idle>,
    quarantined: Vec<Idle>,
    /// Start panicked before returning a cleanup handle. Capacity and the
    /// artifact remain reserved; shutdown cannot certify these starts stopped.
    unresolved_starts: Vec<(PreparedArtifact, Option<Arc<ConsumerOwnership>>)>,
}
struct Idle {
    key: SessionKey,
    session: Box<dyn ExecutionSession>,
    _artifact: PreparedArtifact,
    _consumer: Option<Arc<ConsumerOwnership>>,
}
/// Owned by the supervisor outside the invocation future being polled. Unwinding
/// keeps the admission permit and an acquired session available for cleanup.
struct InvocationOwnership {
    permit: Option<ConsumerPermit>,
    session: Option<Idle>,
    stage: InvocationStage,
}
#[derive(Clone, Copy)]
enum InvocationStage {
    Admission,
    Preparation,
    Startup,
    Execution,
    Settled,
}
impl InvocationStage {
    fn phase(self) -> Phase {
        match self {
            Self::Admission => Phase::Admission,
            Self::Preparation => Phase::Preparation,
            Self::Startup => Phase::Startup,
            Self::Execution | Self::Settled => Phase::Execution,
        }
    }
    fn may_have_started(self) -> bool {
        matches!(self, Self::Execution | Self::Settled)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SessionKey {
    digest: Digest,
    tenant: String,
    namespace: String,
}

impl Worker {
    /// The single capacity setting shared by admission and managed subprocesses.
    pub fn concurrency(&self) -> usize {
        self.inner.config.concurrency
    }

    pub fn new(
        config: WorkerConfig,
        store: Arc<dyn ProgramStore>,
        cache: Arc<dyn ArtifactCache>,
        runtime: Arc<dyn ExecutionRuntime>,
    ) -> Result<Self> {
        if config.concurrency == 0
            || config.concurrency > Semaphore::MAX_PERMITS
            || config.fetch_timeout.is_zero()
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "concurrency must fit semaphore capacity and both concurrency and fetch timeout must be positive",
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                consumers: Arc::new(Semaphore::new(config.concurrency)),
                config,
                store,
                cache,
                runtime,
                pool: Mutex::new(Pool::default()),
                preparations: Mutex::new(HashMap::new()),
                registry: StdMutex::new(Registry {
                    next_id: 0,
                    accepting: true,
                    active: HashMap::new(),
                    keys: HashSet::new(),
                    unresolved: HashSet::new(),
                    unresolved_operations: 0,
                    reservations: Vec::new(),
                    unresolved_consumers: Vec::new(),
                    pending_settlement: HashMap::new(),
                }),
                shutdown_lock: Mutex::new(()),
            }),
        })
    }

    /// Dropping the caller future requests cancellation. Results may precede
    /// operation completion; the supervisor retains its reservation until then.
    pub async fn execute(&self, request: ExecutionRequest, control: RunControl) -> ExecutionResult {
        self.execute_with_reservation(request, control, None).await
    }

    async fn execute_with_reservation(
        &self,
        request: ExecutionRequest,
        control: RunControl,
        reservation: Option<Arc<ConsumerOwnership>>,
    ) -> ExecutionResult {
        let context = Box::new(ExecutionContext::from(&request));
        request
            .descriptor
            .validate()
            .map_err(|error| logged_failure(error, Phase::Admission, false, &context))?;
        control
            .check()
            .map_err(|error| logged_failure(error, Phase::Admission, false, &context))?;
        let attempt_key = attempt_key(&context.identity);
        let id = {
            let mut registry = self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if !registry.accepting {
                return Err(logged_failure(
                    Error::new(ErrorKind::Unavailable, "worker is draining"),
                    Phase::Admission,
                    false,
                    &context,
                ));
            }
            registry
                .pending_settlement
                .retain(|_, owner| owner.strong_count() != 0);
            if registry.keys.contains(&attempt_key)
                || registry.unresolved.contains(&attempt_key)
                || registry.pending_settlement.contains_key(&attempt_key)
            {
                return Err(logged_failure(
                    Error::new(
                        ErrorKind::InvalidInput,
                        "attempt is already owned or its cleanup is unresolved",
                    ),
                    Phase::Admission,
                    false,
                    &context,
                ));
            }
            if let Some(owner) = &reservation {
                if owner.cancelled.load(Ordering::Acquire) {
                    return Err(logged_failure(
                        Error::new(ErrorKind::Cancelled, "consumer reservation cancelled"),
                        Phase::Admission,
                        false,
                        &context,
                    ));
                }
                owner.begin_execution(control.clone());
                registry
                    .pending_settlement
                    .insert(attempt_key.clone(), Arc::downgrade(owner));
            }
            registry.keys.insert(attempt_key.clone());
            registry.next_id += 1;
            let id = registry.next_id;
            registry.active.insert(id, control.clone());
            id
        };
        let (sender, receiver) = oneshot::channel();
        let worker = self.clone();
        let owned_context = context.clone();
        let mut cancellation = CancelOnDrop(Some(control.clone()));
        let span = invocation_span(&context);
        tokio::spawn(
            async move {
                let mut registration = Registration {
                    inner: worker.inner.clone(),
                    id,
                    key: attempt_key,
                    _consumer: reservation.clone(),
                    completed: false,
                };
                let mut completion = Completion::new(sender);
                let mut ownership = InvocationOwnership {
                    permit: reservation.map(ConsumerPermit::Reserved),
                    session: None,
                    stage: InvocationStage::Admission,
                };
                let result = match catch_panic(worker.execute_owned(
                    request,
                    control,
                    &owned_context,
                    &mut completion,
                    &mut ownership,
                ))
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        worker.fail_closed(&owned_context.identity);
                        let cleanup = if ownership.session.is_some() {
                            worker
                                .retire_owned(&mut ownership, &owned_context.identity)
                                .await
                                .err()
                        } else {
                            if matches!(
                                ownership.stage,
                                InvocationStage::Preparation | InvocationStage::Startup
                            ) {
                                worker.unresolved_preparation(&owned_context.identity);
                            }
                            None
                        };
                        Err(with_cleanup(
                            failure(
                                error,
                                ownership.stage.phase(),
                                ownership.stage.may_have_started(),
                                &owned_context,
                            ),
                            cleanup,
                        ))
                    }
                };
                if result.as_ref().is_err_and(|failure| {
                    failure.cleanup_error.is_some() && failure.execution_may_have_started
                }) {
                    worker.retain_attempt(&owned_context.identity);
                }
                drop(ownership);
                registration.completed = true;
                drop(registration);
                completion.send(result);
            }
            .instrument(span)
            .with_current_subscriber(),
        );
        let result = receiver.await.map_err(|_| {
            logged_failure(
                Error::new(
                    ErrorKind::Runtime,
                    "execution supervisor stopped before reporting a result",
                ),
                Phase::Execution,
                true,
                &context,
            )
        })?;
        cancellation.0 = None;
        result
    }

    async fn execute_owned(
        &self,
        request: ExecutionRequest,
        control: RunControl,
        context: &ExecutionContext,
        completion: &mut Completion,
        ownership: &mut InvocationOwnership,
    ) -> ExecutionResult {
        let started = Instant::now();
        if ownership.permit.is_none() {
            let acquire = self.inner.consumers.clone().acquire_owned();
            tokio::pin!(acquire);
            let permit = loop {
                control
                    .check()
                    .map_err(|error| failure(error, Phase::Admission, false, context))?;
                tokio::select! {
                    permit = &mut acquire => break permit.map_err(|error| failure(Error::new(ErrorKind::Unavailable, error.to_string()), Phase::Admission, false, context))?,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            };
            ownership.permit = Some(ConsumerPermit::Direct { _permit: permit });
        }
        ownership.stage = InvocationStage::Preparation;
        tracing::info!(phase = "preparation", "preparing invocation");
        let artifact = self
            .prepare(
                &request.descriptor,
                &control,
                context,
                completion,
                ownership
                    .permit
                    .as_ref()
                    .and_then(ConsumerPermit::reservation),
            )
            .await
            .map_err(|error| failure(error, Phase::Preparation, false, context))?;
        control
            .check()
            .map_err(|error| failure(error, Phase::Preparation, false, context))?;
        artifact
            .manifest()
            .validate_host()
            .map_err(|error| failure(error, Phase::Preparation, false, context))?;
        let key = SessionKey {
            digest: artifact.digest().clone(),
            tenant: request.event.tenant_id().into(),
            namespace: request.event.namespace().into(),
        };
        ownership.stage = InvocationStage::Startup;
        let (session, reused) = self
            .session(
                &key,
                artifact.clone(),
                control.clone(),
                &context.identity,
                ownership
                    .permit
                    .as_ref()
                    .and_then(ConsumerPermit::reservation),
            )
            .await
            .map_err(|error| failure(error, Phase::Startup, false, context))?;
        // Keep this owner outside every adapter future. A panicking poll only
        // unwinds a borrow of the session; retirement still has its real handle.
        ownership.session = Some(Idle {
            key,
            session,
            _artifact: artifact,
            _consumer: ownership
                .permit
                .as_ref()
                .and_then(ConsumerPermit::reservation),
        });
        if let Err(error) = control.check() {
            let cleanup = self.retire_owned(ownership, &context.identity).await.err();
            return Err(with_cleanup(
                failure(error, Phase::Startup, false, context),
                cleanup,
            ));
        }
        let pid = match catch_call(|| {
            ownership
                .session
                .as_ref()
                .expect("session acquired")
                .session
                .pid()
        }) {
            Ok(pid) => pid,
            Err(error) => {
                self.fail_closed(&context.identity);
                let cleanup = self.retire_owned(ownership, &context.identity).await.err();
                return Err(with_cleanup(
                    failure(error, Phase::Startup, false, context),
                    cleanup,
                ));
            }
        };
        ownership.stage = InvocationStage::Execution;
        tracing::info!(pid, reused, phase = "execution", "invoking program");
        let outcome = match catch_panic(async {
            ownership
                .session
                .as_mut()
                .expect("session acquired")
                .session
                .execute(request.event.clone(), control)
                .await
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                self.fail_closed(&context.identity);
                Err(error)
            }
        };
        match outcome {
            Ok(outcome) => {
                let mut idle = ownership.session.take().expect("session acquired");
                // A healthy reusable process is owned by the global process
                // pool, not by the previous delivery's settlement reservation.
                idle._consumer = None;
                self.inner.pool.lock().await.idle.push(idle);
                ownership.stage = InvocationStage::Settled;
                tracing::info!(pid, phase = "completed", "program returned");
                Ok(ExecutionReport {
                    context: Box::new(context.clone()),
                    process_id: pid,
                    reused_process: reused,
                    outcome,
                    elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                })
            }
            Err(error) => {
                let cleanup = self.retire_owned(ownership, &context.identity).await.err();
                Err(with_cleanup(
                    failure(error, Phase::Execution, true, context),
                    cleanup,
                ))
            }
        }
    }

    async fn prepare(
        &self,
        descriptor: &ProgramDescriptor,
        control: &RunControl,
        context: &ExecutionContext,
        completion: &mut Completion,
        reservation: Option<Arc<ConsumerOwnership>>,
    ) -> Result<PreparedArtifact> {
        let lock = {
            let mut entries = self.inner.preparations.lock().await;
            entries.retain(|_, value| value.strong_count() > 0);
            if let Some(lock) = entries.get(&descriptor.digest).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                entries.insert(descriptor.digest.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let acquire = lock.lock();
        tokio::pin!(acquire);
        let _preparation = loop {
            control.check()?;
            tokio::select! {
                guard = &mut acquire => break guard,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        };
        let cached = self.preparation_result(
            catch_panic(async { self.inner.cache.lookup(descriptor).await }).await,
            &context.identity,
        )?;
        if let Some(hit) = cached {
            tracing::debug!(phase = "preparation", cache_hit = true, "artifact ready");
            return Ok(hit);
        }
        // The lookup may outlive cancellation or the invocation deadline. Keep
        // that existing operation owned, but do not begin a new download afterward.
        control.check()?;
        // Pin once: timing out a borrowed future leaves the real operation alive.
        // In particular, a started spawn_blocking read must keep its admission
        // permit and registration until that exact operation completes.
        let fetch = catch_panic(async { self.inner.store.fetch(descriptor).await });
        tokio::pin!(fetch);
        let fetched = tokio::select! {
            result = &mut fetch => result,
            _ = tokio::time::sleep(self.inner.config.fetch_timeout) => {
                let error = Error::new(ErrorKind::TimedOut, "artifact fetch timed out; completion remains supervised");
                completion.send(Err(failure(error.clone(), Phase::Preparation, false, context)));
                let completed = fetch.await;
                if let Err(panic) = completed {
                    self.unresolved_preparation(&context.identity);
                    trace_failure(&with_cleanup(failure(error.clone(), Phase::Preparation, false, context), Some(panic)));
                }
                // Do not execute or publish a result after the fetch deadline.
                return Err(error);
            }
        };
        let bytes = self.preparation_result(fetched, &context.identity)?;
        loop {
            let published = self.preparation_result(
                catch_panic(async { self.inner.cache.publish(descriptor, bytes.clone()).await })
                    .await,
                &context.identity,
            );
            match published {
                Ok(artifact) => {
                    tracing::debug!(
                        phase = "preparation",
                        cache_hit = false,
                        "artifact prepared"
                    );
                    return Ok(artifact);
                }
                Err(error) if error.kind == ErrorKind::Capacity => {
                    if !self
                        .retire_idle(&context.identity, reservation.clone())
                        .await?
                    {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn preparation_result<T>(
        &self,
        result: Result<Result<T>>,
        identity: &InvocationIdentity,
    ) -> Result<T> {
        match result {
            Ok(result) => result,
            Err(error) => {
                self.unresolved_preparation(identity);
                Err(error)
            }
        }
    }

    async fn session(
        &self,
        key: &SessionKey,
        artifact: PreparedArtifact,
        control: RunControl,
        identity: &InvocationIdentity,
        reservation: Option<Arc<ConsumerOwnership>>,
    ) -> Result<(Box<dyn ExecutionSession>, bool)> {
        let retired = {
            let mut pool = self.inner.pool.lock().await;
            if let Some(index) = pool.idle.iter().position(|idle| idle.key == *key) {
                return Ok((pool.idle.swap_remove(index).session, true));
            }
            if pool.occupied < self.inner.config.concurrency {
                pool.occupied += 1;
                None
            } else if let Some(idle) = pool.idle.pop() {
                Some(idle)
            } else {
                return Err(Error::new(
                    ErrorKind::Capacity,
                    "process slots are awaiting retirement",
                ));
            }
        };
        if let Some(mut idle) = retired {
            idle._consumer = reservation.clone();
            if let Err(error) = self.close_session(&mut idle, Some(identity)).await {
                if let Some(owner) = &idle._consumer {
                    owner.retain_cleanup();
                }
                self.inner.pool.lock().await.quarantined.push(idle);
                return Err(error);
            }
        }
        // Keep this reservation until the replacement is started or fails.
        if let Err(error) = control.check() {
            self.inner.pool.lock().await.occupied -= 1;
            return Err(error);
        }
        match catch_panic(async { self.inner.runtime.start(artifact.clone(), control).await }).await
        {
            Ok(Ok(StartOutcome::Ready(session))) => Ok((session, false)),
            Ok(Ok(StartOutcome::CleanupRequired { error, session })) => {
                if let Some(owner) = &reservation {
                    owner.retain_cleanup();
                }
                self.inner.pool.lock().await.quarantined.push(Idle {
                    key: key.clone(),
                    session,
                    _artifact: artifact,
                    _consumer: reservation.clone(),
                });
                Err(error)
            }
            Ok(Err(error)) => {
                self.inner.pool.lock().await.occupied -= 1;
                Err(error)
            }
            Err(error) => {
                self.fail_closed(identity);
                if let Some(owner) = &reservation {
                    owner.retain_unresolved_operation();
                }
                self.inner
                    .pool
                    .lock()
                    .await
                    .unresolved_starts
                    .push((artifact, reservation));
                Err(error)
            }
        }
    }

    async fn close_session(
        &self,
        idle: &mut Idle,
        identity: Option<&InvocationIdentity>,
    ) -> Result<()> {
        match catch_panic(async { idle.session.close().await }).await {
            Ok(Ok(())) => {
                if let Some(owner) = &idle._consumer {
                    owner.confirm_cleanup();
                }
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(error) => {
                let mut registry = self
                    .inner
                    .registry
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                registry.accepting = false;
                registry.cancel_all();
                if let Some(identity) = identity {
                    registry.unresolved.insert(attempt_key(identity));
                }
                Err(error)
            }
        }
    }

    async fn retire_owned(
        &self,
        ownership: &mut InvocationOwnership,
        identity: &InvocationIdentity,
    ) -> Result<()> {
        let result = self
            .close_session(
                ownership.session.as_mut().expect("owned session to retire"),
                Some(identity),
            )
            .await;
        let owned = ownership.session.take().expect("owned session to retire");
        match result {
            Ok(()) => {
                self.inner.pool.lock().await.occupied -= 1;
                Ok(())
            }
            Err(error) => {
                if let Some(owner) = &owned._consumer {
                    owner.retain_cleanup();
                }
                self.inner.pool.lock().await.quarantined.push(owned);
                Err(error)
            }
        }
    }

    async fn retire(&self, mut idle: Idle, identity: Option<&InvocationIdentity>) -> Result<()> {
        match self.close_session(&mut idle, identity).await {
            Ok(()) => {
                self.inner.pool.lock().await.occupied -= 1;
                Ok(())
            }
            Err(error) => {
                if let Some(owner) = &idle._consumer {
                    owner.retain_cleanup();
                }
                self.inner.pool.lock().await.quarantined.push(idle);
                Err(error)
            }
        }
    }

    async fn retire_idle(
        &self,
        identity: &InvocationIdentity,
        reservation: Option<Arc<ConsumerOwnership>>,
    ) -> Result<bool> {
        let idle = self.inner.pool.lock().await.idle.pop();
        if let Some(mut idle) = idle {
            idle._consumer = reservation;
            self.retire(idle, Some(identity)).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn stats(&self) -> WorkerStats {
        let pool = self.inner.pool.lock().await;
        let registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        WorkerStats {
            process_slots: pool.occupied,
            warm_processes: pool.idle.len(),
            active_consumers: self.inner.config.concurrency
                - self.inner.consumers.available_permits(),
            accepting: registry.accepting,
        }
    }

    /// Stop admission, drain, cancel if necessary, then retire processes. If cleanup
    /// exceeds its separate budget, report incomplete shutdown and retain ownership.
    pub async fn shutdown(&self, grace: Duration, cleanup: Duration) -> Result<()> {
        let worker = self.clone();
        tokio::spawn(async move {
            let _shutdown = worker.inner.shutdown_lock.lock().await;
            worker.shutdown_owned(grace, cleanup).await
        })
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Runtime,
                format!("shutdown supervisor failed: {e}"),
            )
        })?
    }

    async fn shutdown_owned(&self, grace: Duration, cleanup: Duration) -> Result<()> {
        self.inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accepting = false;
        let drain_until = Instant::now()
            .checked_add(grace)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "grace period is too large"))?;
        while self.active_count() != 0 && Instant::now() < drain_until {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        {
            let registry = self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            registry.cancel_all();
        }
        let cleanup_until = Instant::now()
            .checked_add(cleanup)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "cleanup period is too large"))?;
        loop {
            // External delivery owners may need to observe this cleanup before
            // releasing their reservation. Retire available sessions while they
            // wait. Sample owners before the pool: a finishing supervisor must
            // publish its session before removing its active registration.
            let active = self.active_count() != 0;
            let session = {
                let mut pool = self.inner.pool.lock().await;
                pool.quarantined.pop().or_else(|| pool.idle.pop())
            };
            let Some(mut idle) = session else {
                if !active {
                    break;
                }
                if Instant::now() >= cleanup_until {
                    return Err(Error::new(
                        ErrorKind::TimedOut,
                        "shutdown incomplete: active consumers still own work",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let remaining = cleanup_until.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.close_session(&mut idle, None)).await {
                Ok(Ok(())) => self.inner.pool.lock().await.occupied -= 1,
                result => {
                    if let Some(owner) = &idle._consumer {
                        owner.retain_cleanup();
                    }
                    self.inner.pool.lock().await.quarantined.push(idle);
                    return Err(match result {
                        Ok(Err(error)) => error,
                        _ => Error::new(
                            ErrorKind::TimedOut,
                            "shutdown incomplete: process retirement pending",
                        ),
                    });
                }
            }
        }
        if self.inner.pool.lock().await.occupied != 0
            || self
                .inner
                .registry
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .unresolved_operations
                != 0
        {
            return Err(Error::new(
                ErrorKind::Runtime,
                "shutdown incomplete: unresolved process or adapter operations",
            ));
        }
        Ok(())
    }
    fn retain_attempt(&self, identity: &InvocationIdentity) {
        self.inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .unresolved
            .insert(attempt_key(identity));
    }
    fn fail_closed(&self, identity: &InvocationIdentity) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        registry.accepting = false;
        registry.cancel_all();
        registry.unresolved.insert(attempt_key(identity));
    }
    fn unresolved_preparation(&self, identity: &InvocationIdentity) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        registry.accepting = false;
        registry.cancel_all();
        registry.unresolved.insert(attempt_key(identity));
        registry.unresolved_operations += 1;
        registry.retain_unresolved_consumer(&attempt_key(identity));
    }
    fn active_count(&self) -> usize {
        let registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        registry.active.len()
            + registry
                .reservations
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|owner| owner.external.load(Ordering::Acquire))
                .count()
    }
}

struct CancelOnDrop(Option<RunControl>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(control) = &self.0 {
            control.cancel();
        }
    }
}
fn attempt_key(identity: &InvocationIdentity) -> AttemptKey {
    AttemptKey {
        tenant: identity.tenant_id.clone(),
        namespace: identity.namespace.clone(),
        attempt: identity.attempt_id.clone(),
    }
}
fn failure(
    error: Error,
    phase: Phase,
    execution_may_have_started: bool,
    context: &ExecutionContext,
) -> ExecutionFailure {
    ExecutionFailure {
        context: Box::new(context.clone()),
        error,
        cleanup_error: None,
        phase,
        execution_may_have_started,
    }
}
fn with_cleanup(mut failure: ExecutionFailure, cleanup: Option<Error>) -> ExecutionFailure {
    failure.cleanup_error = cleanup;
    failure
}
fn logged_failure(
    error: Error,
    phase: Phase,
    execution_may_have_started: bool,
    context: &ExecutionContext,
) -> ExecutionFailure {
    let failure = failure(error, phase, execution_may_have_started, context);
    trace_failure(&failure);
    failure
}
fn invocation_span(context: &ExecutionContext) -> tracing::Span {
    let identity = &context.identity;
    tracing::info_span!("invocation", source = %identity.source, event_id = %identity.event_id, tenant_id = %identity.tenant_id, namespace = %identity.namespace, run_id = %identity.run_id, task_id = %identity.task_id, attempt_id = %identity.attempt_id, attempt_no = identity.attempt_no, traceparent = identity.traceparent.as_deref().unwrap_or(""), tracestate = identity.tracestate.as_deref().unwrap_or(""), program_id = %context.program.id, program_version = %context.program.version, digest = %context.digest.0)
}

fn trace_failure(failure: &ExecutionFailure) {
    let context = &failure.context;
    let identity = &context.identity;
    tracing::warn!(source = %identity.source, event_id = %identity.event_id, tenant_id = %identity.tenant_id, namespace = %identity.namespace, run_id = %identity.run_id, task_id = %identity.task_id, attempt_id = %identity.attempt_id, attempt_no = identity.attempt_no, traceparent = identity.traceparent.as_deref().unwrap_or(""), tracestate = identity.tracestate.as_deref().unwrap_or(""), program_id = %context.program.id, program_version = %context.program.version, digest = %context.digest.0, phase = ?failure.phase, error = %failure.error, cleanup_error = ?failure.cleanup_error, execution_may_have_started = failure.execution_may_have_started, "invocation failed");
}
