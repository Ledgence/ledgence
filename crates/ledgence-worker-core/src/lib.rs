//! Bounded local preparation and reusable execution.
//!
//! The caller supplies an immutable descriptor already bound to its logical task.
//! Distributed leases/settlement belong to a later orchestration adapter.

use ledgence_worker_api::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Also the maximum number of managed starting/warm/running/stopping processes.
    pub concurrency: usize,
    /// Bound a shared store download even when its original caller cancels.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub descriptor: ProgramDescriptor,
    pub event: CloudEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub event_id: String,
    pub attempt_id: String,
    pub task_id: String,
    pub digest: Digest,
    pub process_id: u32,
    pub reused_process: bool,
    pub outcome: ProgramOutcome,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Admission,
    Preparation,
    Startup,
    Execution,
    Cleanup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionFailure {
    pub error: Error,
    pub phase: Phase,
    /// Conservative: a lost response must not be retried inside the runtime.
    pub execution_may_have_started: bool,
}
impl std::fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.phase, self.error)
    }
}
impl std::error::Error for ExecutionFailure {}
pub type ExecutionResult = std::result::Result<ExecutionReport, ExecutionFailure>;

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
}
struct Idle {
    key: SessionKey,
    session: Box<dyn ExecutionSession>,
    _artifact: PreparedArtifact,
}
#[derive(Clone, PartialEq, Eq)]
struct SessionKey {
    digest: Digest,
    tenant: String,
    namespace: String,
}

impl Worker {
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
                }),
                shutdown_lock: Mutex::new(()),
            }),
        })
    }

    /// Dropping the caller future requests cancellation; a supervisor retains the
    /// consumer reservation until preparation/execution/retirement has completed.
    pub async fn execute(&self, request: ExecutionRequest, control: RunControl) -> ExecutionResult {
        request
            .descriptor
            .validate()
            .map_err(|error| failure(error, Phase::Admission, false))?;
        control
            .check()
            .map_err(|error| failure(error, Phase::Admission, false))?;
        let attempt_key = AttemptKey {
            tenant: request.event.tenant_id().into(),
            namespace: request.event.namespace().into(),
            attempt: request.event.attempt_id().into(),
        };
        let id = {
            let mut registry = self.inner.registry.lock().expect("registry lock poisoned");
            if !registry.accepting {
                return Err(failure(
                    Error::new(ErrorKind::Unavailable, "worker is draining"),
                    Phase::Admission,
                    false,
                ));
            }
            if registry.keys.contains(&attempt_key) || registry.unresolved.contains(&attempt_key) {
                return Err(failure(
                    Error::new(
                        ErrorKind::InvalidInput,
                        "attempt is already owned or its cleanup is unresolved",
                    ),
                    Phase::Admission,
                    false,
                ));
            }
            registry.keys.insert(attempt_key.clone());
            registry.next_id += 1;
            let id = registry.next_id;
            registry.active.insert(id, control.clone());
            id
        };
        let worker = self.clone();
        let mut cancellation = CancelOnDrop(Some(control.clone()));
        let task = tokio::spawn(async move {
            let _registration = Registration {
                inner: worker.inner.clone(),
                id,
                key: attempt_key.clone(),
            };
            let result = worker.execute_owned(request, control).await;
            if result
                .as_ref()
                .is_err_and(|e| e.phase == Phase::Cleanup && e.execution_may_have_started)
            {
                worker
                    .inner
                    .registry
                    .lock()
                    .expect("registry lock poisoned")
                    .unresolved
                    .insert(attempt_key);
            }
            result
        });
        let result = task.await.map_err(|e| {
            self.inner
                .registry
                .lock()
                .expect("registry lock poisoned")
                .accepting = false;
            failure(
                Error::new(
                    ErrorKind::Runtime,
                    format!("execution supervisor failed: {e}"),
                ),
                Phase::Execution,
                true,
            )
        })?;
        cancellation.0 = None;
        result
    }

    async fn execute_owned(
        &self,
        request: ExecutionRequest,
        control: RunControl,
    ) -> ExecutionResult {
        let started = Instant::now();
        let _permit = loop {
            control
                .check()
                .map_err(|error| failure(error, Phase::Admission, false))?;
            tokio::select! {
                permit = self.inner.consumers.clone().acquire_owned() => {
                    break permit.map_err(|e| failure(Error::new(ErrorKind::Unavailable, e.to_string()), Phase::Admission, false))?;
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        };
        tracing::info!(event_id = request.event.id(), attempt_id = request.event.attempt_id(), task_id = request.event.task_id(), digest = %request.descriptor.digest.0, phase = "preparation", "preparing invocation");
        let artifact = self
            .prepare(&request.descriptor, &control)
            .await
            .map_err(|error| failure(error, Phase::Preparation, false))?;
        control
            .check()
            .map_err(|error| failure(error, Phase::Preparation, false))?;
        artifact
            .manifest()
            .validate_host()
            .map_err(|error| failure(error, Phase::Preparation, false))?;
        let key = SessionKey {
            digest: artifact.digest().clone(),
            tenant: request.event.tenant_id().into(),
            namespace: request.event.namespace().into(),
        };
        let (mut session, reused) = self
            .session(&key, artifact.clone(), control.clone())
            .await
            .map_err(|error| failure(error, Phase::Startup, false))?;
        if let Err(error) = control.check() {
            self.retire(Idle {
                key,
                session,
                _artifact: artifact,
            })
            .await
            .map_err(|cleanup| failure(cleanup, Phase::Cleanup, false))?;
            return Err(failure(error, Phase::Startup, false));
        }
        let pid = session.pid();
        tracing::info!(
            event_id = request.event.id(),
            attempt_id = request.event.attempt_id(),
            pid,
            reused,
            traceparent = request.event.traceparent().unwrap_or(""),
            phase = "execution",
            "invoking program"
        );
        let outcome = session.execute(request.event.clone(), control).await;
        match outcome {
            Ok(outcome) => {
                self.inner.pool.lock().await.idle.push(Idle {
                    key,
                    session,
                    _artifact: artifact,
                });
                tracing::info!(
                    event_id = request.event.id(),
                    attempt_id = request.event.attempt_id(),
                    pid,
                    phase = "completed",
                    "program returned"
                );
                Ok(ExecutionReport {
                    event_id: request.event.id().into(),
                    attempt_id: request.event.attempt_id().into(),
                    task_id: request.event.task_id().into(),
                    digest: request.descriptor.digest,
                    process_id: pid,
                    reused_process: reused,
                    outcome,
                    elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                })
            }
            Err(error) => {
                self.retire(Idle {
                    key,
                    session,
                    _artifact: artifact,
                })
                .await
                .map_err(|cleanup| failure(cleanup, Phase::Cleanup, true))?;
                Err(failure(error, Phase::Execution, true))
            }
        }
    }

    async fn prepare(
        &self,
        descriptor: &ProgramDescriptor,
        control: &RunControl,
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
        let _preparation = loop {
            control.check()?;
            tokio::select! {
                guard = lock.lock() => break guard,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        };
        if let Some(hit) = self.inner.cache.lookup(descriptor).await? {
            tracing::debug!(digest = %descriptor.digest.0, cache_hit = true, "artifact ready");
            return Ok(hit);
        }
        // The owner completes this shared fetch even if cancelled, so another
        // consumer can reuse it. It retains its consumer permit until done.
        let bytes = tokio::time::timeout(
            self.inner.config.fetch_timeout,
            self.inner.store.fetch(descriptor),
        )
        .await
        .map_err(|_| Error::new(ErrorKind::TimedOut, "artifact fetch timed out"))??;
        loop {
            match self.inner.cache.publish(descriptor, bytes.clone()).await {
                Ok(artifact) => {
                    tracing::debug!(digest = %descriptor.digest.0, cache_hit = false, "artifact prepared");
                    return Ok(artifact);
                }
                Err(error) if error.kind == ErrorKind::Capacity => {
                    if !self.retire_idle().await? {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn session(
        &self,
        key: &SessionKey,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> Result<(Box<dyn ExecutionSession>, bool)> {
        let retired = {
            let mut pool = self.inner.pool.lock().await;
            if let Some(index) = pool.idle.iter().position(|idle| idle.key == *key) {
                return Ok((pool.idle.swap_remove(index).session, true));
            }
            if let Some(idle) = pool.idle.pop() {
                Some(idle)
            } else if pool.occupied < self.inner.config.concurrency {
                pool.occupied += 1;
                None
            } else {
                return Err(Error::new(
                    ErrorKind::Capacity,
                    "process slots are awaiting retirement",
                ));
            }
        };
        if let Some(mut idle) = retired
            && let Err(error) = idle.session.close().await
        {
            self.inner.pool.lock().await.quarantined.push(idle);
            return Err(error);
        }
        // Keep this reservation until the replacement is started or fails.
        if let Err(error) = control.check() {
            self.inner.pool.lock().await.occupied -= 1;
            return Err(error);
        }
        match self.inner.runtime.start(artifact.clone(), control).await {
            Ok(StartOutcome::Ready(session)) => Ok((session, false)),
            Ok(StartOutcome::CleanupRequired { error, session }) => {
                self.inner.pool.lock().await.quarantined.push(Idle {
                    key: key.clone(),
                    session,
                    _artifact: artifact,
                });
                Err(error)
            }
            Err(error) => {
                self.inner.pool.lock().await.occupied -= 1;
                Err(error)
            }
        }
    }

    async fn retire(&self, mut idle: Idle) -> Result<()> {
        match idle.session.close().await {
            Ok(()) => {
                self.inner.pool.lock().await.occupied -= 1;
                Ok(())
            }
            Err(error) => {
                self.inner.pool.lock().await.quarantined.push(idle);
                Err(error)
            }
        }
    }

    async fn retire_idle(&self) -> Result<bool> {
        let idle = self.inner.pool.lock().await.idle.pop();
        if let Some(idle) = idle {
            self.retire(idle).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn stats(&self) -> WorkerStats {
        let pool = self.inner.pool.lock().await;
        let registry = self.inner.registry.lock().expect("registry lock poisoned");
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
            .expect("registry lock poisoned")
            .accepting = false;
        let drain_until = Instant::now()
            .checked_add(grace)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "grace period is too large"))?;
        while self.active_count() != 0 && Instant::now() < drain_until {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        {
            let registry = self.inner.registry.lock().expect("registry lock poisoned");
            for control in registry.active.values() {
                control.cancel();
            }
        }
        let cleanup_until = Instant::now()
            .checked_add(cleanup)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "cleanup period is too large"))?;
        while self.active_count() != 0 {
            if Instant::now() >= cleanup_until {
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    "shutdown incomplete: active consumers still own work",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop {
            let session = {
                let mut pool = self.inner.pool.lock().await;
                pool.idle.pop().or_else(|| pool.quarantined.pop())
            };
            let Some(mut idle) = session else {
                break;
            };
            let remaining = cleanup_until.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, idle.session.close()).await {
                Ok(Ok(())) => self.inner.pool.lock().await.occupied -= 1,
                result => {
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
        if self.inner.pool.lock().await.occupied != 0 {
            return Err(Error::new(
                ErrorKind::Runtime,
                "shutdown incomplete: unresolved process reservations",
            ));
        }
        Ok(())
    }
    fn active_count(&self) -> usize {
        self.inner
            .registry
            .lock()
            .expect("registry lock poisoned")
            .active
            .len()
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
struct Registration {
    inner: Arc<Inner>,
    id: u64,
    key: AttemptKey,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut registry = self.inner.registry.lock().expect("registry lock poisoned");
        registry.active.remove(&self.id);
        registry.keys.remove(&self.key);
    }
}
fn failure(error: Error, phase: Phase, execution_may_have_started: bool) -> ExecutionFailure {
    ExecutionFailure {
        error,
        phase,
        execution_may_have_started,
    }
}
