//! Supervised delivery over [`TaskService`], independent of its transport/store.
//!
//! Reserve capacity before acquisition; retain it through ambiguous replies and
//! required cleanup. Dropping a handle requests shutdown, not abandonment. The
//! Tokio runtime must remain alive until shutdown finishes. Process crashes rely
//! on service expiry/retry; this driver has no local durable recovery journal.

mod attempt;
mod renewal;

use ledgence_orchestration_api::*;
use ledgence_worker_api::RunControl;
use ledgence_worker_core::Worker;
use std::{
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
    /// Delay after a committed Empty. This is immediate polling, not long polling.
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
        if self.request_timeout > Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS)
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
    pub session_id: Option<String>,
    pub stopping: bool,
    pub finished: bool,
    pub settled_attempts: u64,
    pub lost_attempts: u64,
    pub last_error: Option<ContractError>,
}

struct Shared {
    stop: AtomicBool,
    status: Mutex<DeliveryStatus>,
}
impl Shared {
    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }
    fn stop(&self) {
        self.stop.store(true, Ordering::Release);
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

pub struct DeliveryDriver {
    worker: Worker,
    service: Arc<dyn TaskService>,
    config: DeliveryConfig,
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
            service,
            config,
        })
    }
    /// Start one non-resumable service session with N supervised consumers.
    /// Session expiry drains this driver; callers may start a new worker after
    /// shutdown. Never transplant old cursors into a new service session.
    pub fn start(self) -> DeliveryHandle {
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
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
        let session = self.open(&shared).await;
        let mut consumers = Vec::new();
        let maintenance_done = Arc::new(AtomicBool::new(false));
        let mut maintenance = None;
        if let Some(session) = session {
            shared
                .status
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .session_id = Some(session.id.clone());
            let context = Arc::new(Context {
                worker: self.worker.clone(),
                service: self.service,
                config: self.config,
                shared: shared.clone(),
            });
            let current = context.clone();
            let registered = session.clone();
            let done = maintenance_done.clone();
            maintenance = Some(tokio::spawn(
                async move { current.maintain_session(registered, done).await }
                    .with_current_subscriber(),
            ));
            for consumer in 0..session.concurrency {
                let current = context.clone();
                let command = AcquireCommand {
                    scope: session.scope.clone(),
                    queue: session.queue.clone(),
                    worker_session_id: session.id.clone(),
                    consumer_id: consumer,
                    sequence: 1,
                };
                consumers.push(tokio::spawn(
                    async move { current.consume(command).await }.with_current_subscriber(),
                ));
            }
        }
        // Cleanup must run concurrently with reconciliation: both hold parts of
        // the same reservation. Waiting for consumers first would deadlock it.
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
            if shared.stopping() {
                let _ = self
                    .worker
                    .shutdown(Duration::ZERO, Duration::from_millis(100))
                    .await;
            }
            tokio::time::sleep(TICK).await;
        }
        maintenance_done.store(true, Ordering::Release);
        if let Some(maintenance) = maintenance {
            let _ = maintenance.await;
        }
        shared.stop();
        // Repeated calls advance retryable quarantined cleanup. Unrecoverable
        // local operations keep the driver visibly pending, never 'finished'.
        loop {
            match self
                .worker
                .shutdown(Duration::ZERO, Duration::from_millis(100))
                .await
            {
                Ok(()) => break,
                Err(error) => {
                    shared.error(ContractError::Unavailable(error.to_string()));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    async fn open(&self, shared: &Shared) -> Option<WorkerSession> {
        while !shared.stopping() {
            if !self.worker.stats().await.accepting {
                shared.stop();
                return None;
            }
            let future = exchange(self.config.request_timeout, || {
                self.service.open_session(
                    &self.config.scope,
                    &self.config.queue,
                    self.worker.concurrency() as u32,
                )
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
                        && session.queue == self.config.queue
                        && session.concurrency == self.worker.concurrency() as u32
                        && validate_text(&session.id, 128).is_ok() =>
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

struct Context {
    worker: Worker,
    service: Arc<dyn TaskService>,
    config: DeliveryConfig,
    shared: Arc<Shared>,
}
impl Context {
    async fn maintain_session(&self, session: WorkerSession, done: Arc<AtomicBool>) {
        let mut next = Instant::now() + self.config.session_extend_interval;
        while !done.load(Ordering::Acquire) {
            if Instant::now() < next {
                tokio::time::sleep(TICK).await;
                continue;
            }
            let request = exchange(self.config.request_timeout, || {
                self.service.extend_session(&session.id)
            });
            tokio::pin!(request);
            let result = loop {
                tokio::select! {
                    result = &mut request => break result,
                    _ = tokio::time::sleep(TICK) => if done.load(Ordering::Acquire) {return;},
                }
            };
            match result {
                Ok(reply)
                    if reply.id == session.id
                        && reply.scope == session.scope
                        && reply.queue == session.queue
                        && reply.concurrency == session.concurrency =>
                {
                    next = Instant::now() + self.config.session_extend_interval
                }
                Ok(_) => {
                    self.shared
                        .fatal(protocol("extension changed session identity"));
                    return;
                }
                Err(error) if retryable(&error) => {
                    self.shared.error(error);
                    next = Instant::now() + self.config.retry_delay;
                }
                Err(error) => {
                    self.shared.fatal(error);
                    return;
                }
            }
        }
    }
    #[tracing::instrument(name = "delivery_consumer", skip_all, fields(
        tenant_id = %command.scope.tenant_id, namespace = %command.scope.namespace,
        queue = %command.queue, worker_session_id = %command.worker_session_id,
        consumer_id = command.consumer_id
    ))]
    async fn consume(self: Arc<Self>, mut command: AcquireCommand) {
        while !self.shared.stopping() {
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
            if self.shared.stopping() {
                return;
            }
            // Once sent, shutdown cannot abandon this sequence: even a timed-out
            // exchange may already have committed a claim.
            let (reply, started) = loop {
                let started = Instant::now();
                match exchange(self.config.request_timeout, || {
                    self.service.acquire(&command)
                })
                .await
                {
                    Ok(reply) => {
                        let sequence = match &reply {
                            AcquireReply::Empty { sequence }
                            | AcquireReply::Assigned { sequence, .. }
                            | AcquireReply::OwnershipLost { sequence, .. } => *sequence,
                        };
                        let valid = if sequence != command.sequence {
                            Err(protocol("acquisition reply changed sequence"))
                        } else if let AcquireReply::Assigned { assignment, .. } = &reply {
                            attempt::validate_assignment(&command, assignment)
                        } else {
                            Ok(())
                        };
                        match valid {
                            Ok(()) => break (reply, started),
                            Err(error) => self.shared.fatal(error),
                        }
                    }
                    Err(
                        error @ (ContractError::UnknownSession | ContractError::SessionExpired),
                    ) => {
                        self.shared.fatal(error);
                        return;
                    }
                    Err(error) if retryable(&error) => self.shared.error(error),
                    Err(error) => self.shared.fatal(error),
                }
                tokio::time::sleep(self.config.retry_delay).await;
            };
            match reply {
                AcquireReply::Assigned { assignment, .. } => {
                    self.attempt(reservation, *assignment, started).await
                }
                AcquireReply::Empty { .. } => {
                    drop(reservation);
                    pause(self.config.idle_delay, &self.shared).await;
                }
                AcquireReply::OwnershipLost { .. } => {
                    self.shared
                        .status
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .lost_attempts += 1;
                }
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
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let future = catch_unwind(AssertUnwindSafe(call)).map_err(|_| {
        ContractError::Unavailable("control adapter panicked; outcome is unknown".into())
    })?;
    tokio::pin!(future);
    let future = std::future::poll_fn(|cx| {
        catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))).unwrap_or_else(|_| {
            std::task::Poll::Ready(Err(ContractError::Unavailable(
                "control adapter panicked; outcome is unknown".into(),
            )))
        })
    });
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| {
            Err(ContractError::Unavailable(
                "control request timed out; outcome is unknown".into(),
            ))
        })
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
