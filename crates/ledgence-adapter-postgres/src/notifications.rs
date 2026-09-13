//! Optional PostgreSQL wake hints. Durability never depends on this channel.

use crate::*;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use std::{
    collections::{HashSet, VecDeque},
    sync::{
        Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};
use tracing::instrument::WithSubscriber;

const CHANNEL: &str = "ledgence_acquisition_v1";
const MAX_HINTS: usize = 1_024;
const MAX_PAYLOAD_BYTES: usize = 2_048;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_OBSERVATION: Duration = Duration::from_secs(3);
const MAX_RECEIVE_BURST: usize = 64;

/// Snapshot of optional notification delivery. Drops do not imply lost tasks.
#[derive(Debug, Clone, Copy)]
pub struct AcquisitionNotificationStatistics {
    /// Unique pending keys, including the currently publishing key.
    pub queued: usize,
    pub published: u64,
    pub dropped: u64,
    pub malformed: u64,
    pub subscriptions: u64,
    pub listener_connected: bool,
}

#[derive(Default)]
pub(crate) struct WakeDispatch {
    local: RwLock<Option<Arc<dyn AcquisitionWake>>>,
    remote: Mutex<Option<Weak<Publisher>>>,
}
impl WakeDispatch {
    pub(crate) fn set_local(&self, sink: Arc<dyn AcquisitionWake>) {
        *self.local.write().unwrap_or_else(|e| e.into_inner()) = Some(sink);
    }

    fn local(&self, hint: AcquisitionHint) {
        let sink = self.local.read().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(sink) = sink {
            // A faulty optional adapter must not turn a committed task mutation
            // into a reported failure. The port still requires prompt delivery.
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.wake(hint))).is_err() {
                tracing::warn!(
                    "local acquisition wake sink panicked; periodic fallback remains active"
                );
            }
        }
    }

    pub(crate) fn publish(&self, hint: AcquisitionHint) {
        self.local(hint.clone());
        let remote = self
            .remote
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(remote) = remote {
            remote.enqueue(hint);
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    hint: WireHint,
}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "key",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WireHint {
    QueueChanged(AcquisitionQueue),
    AcquisitionCompleted(AcquisitionKey),
}

fn validate_hint(hint: &AcquisitionHint) -> bool {
    let queue = match hint {
        AcquisitionHint::QueueChanged(queue) => queue,
        AcquisitionHint::AcquisitionCompleted(key) => {
            if validate_text(&key.worker_session_id, 128).is_err() || key.sequence == 0 {
                return false;
            }
            &key.queue
        }
        AcquisitionHint::Rescan => return false,
    };
    queue.scope.validate().is_ok() && validate_text(&queue.queue, 128).is_ok()
}
fn encode(hint: AcquisitionHint) -> Option<String> {
    if !validate_hint(&hint) {
        return None;
    }
    let hint = match hint {
        AcquisitionHint::QueueChanged(key) => WireHint::QueueChanged(key),
        AcquisitionHint::AcquisitionCompleted(key) => WireHint::AcquisitionCompleted(key),
        AcquisitionHint::Rescan => return None,
    };
    let payload = serde_json::to_string(&Envelope { version: 1, hint }).ok()?;
    (payload.len() <= MAX_PAYLOAD_BYTES).then_some(payload)
}
fn decode(payload: &str) -> Option<AcquisitionHint> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return None;
    }
    let envelope: Envelope = serde_json::from_str(payload).ok()?;
    if envelope.version != 1 {
        return None;
    }
    let hint = match envelope.hint {
        WireHint::QueueChanged(key) => AcquisitionHint::QueueChanged(key),
        WireHint::AcquisitionCompleted(key) => AcquisitionHint::AcquisitionCompleted(key),
    };
    validate_hint(&hint).then_some(hint)
}

#[derive(Default)]
struct Backlog {
    queue: VecDeque<String>,
    keys: HashSet<String>,
    closed: bool,
}
#[derive(Default)]
struct Publisher {
    backlog: Mutex<Backlog>,
    available: Notify,
    published: AtomicU64,
    dropped: AtomicU64,
    malformed: AtomicU64,
    subscriptions: AtomicU64,
    connected: AtomicBool,
    last_diagnostic: Mutex<Option<Instant>>,
}
impl Publisher {
    fn enqueue(&self, hint: AcquisitionHint) {
        let Some(payload) = encode(hint) else {
            self.malformed.fetch_add(1, Ordering::Relaxed);
            self.diagnose("invalid_local_hint");
            return;
        };
        let mut backlog = self.backlog.lock().unwrap_or_else(|e| e.into_inner());
        if backlog.closed || backlog.keys.contains(&payload) {
            return;
        }
        if backlog.keys.len() == MAX_HINTS {
            drop(backlog);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.diagnose("publisher_full");
            return;
        }
        backlog.keys.insert(payload.clone());
        backlog.queue.push_back(payload);
        drop(backlog);
        self.available.notify_one();
    }

    fn pop(&self) -> Option<String> {
        let mut backlog = self.backlog.lock().unwrap_or_else(|e| e.into_inner());
        backlog.queue.pop_front()
    }

    fn finished(&self, payload: &str) {
        self.backlog
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys
            .remove(payload);
    }

    fn close(&self) {
        let mut backlog = self.backlog.lock().unwrap_or_else(|e| e.into_inner());
        backlog.closed = true;
        self.dropped
            .fetch_add(backlog.keys.len() as u64, Ordering::Relaxed);
        backlog.queue.clear();
        backlog.keys.clear();
        self.available.notify_waiters();
    }

    fn statistics(&self) -> AcquisitionNotificationStatistics {
        AcquisitionNotificationStatistics {
            queued: self
                .backlog
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .keys
                .len(),
            published: self.published.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            malformed: self.malformed.load(Ordering::Relaxed),
            subscriptions: self.subscriptions.load(Ordering::Relaxed),
            listener_connected: self.connected.load(Ordering::Relaxed),
        }
    }

    fn diagnose(&self, reason: &'static str) {
        let mut last = self
            .last_diagnostic
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if last.is_none_or(|at| at.elapsed() >= Duration::from_secs(30)) {
            *last = Some(Instant::now());
            tracing::warn!(
                reason,
                dropped = self.dropped.load(Ordering::Relaxed),
                malformed = self.malformed.load(Ordering::Relaxed),
                "acquisition notifications degraded; periodic fallback remains active"
            );
        }
    }
}

/// Owns the optional listener/publisher and their separate two-connection pool.
/// Keep this owner alive through accepted request drain, then call `shutdown`.
pub struct AcquisitionNotifications {
    publisher: Arc<Publisher>,
    pool: PgPool,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}
impl AcquisitionNotifications {
    pub fn statistics(&self) -> AcquisitionNotificationStatistics {
        self.publisher.statistics()
    }

    /// Stop delivery while retaining task and pool ownership until closure.
    /// Observes cleanup every three seconds with aggregated diagnostics; the executable's signal
    /// supervisor remains responsible for an explicit force exit.
    pub async fn shutdown(mut self) -> AcquisitionNotificationStatistics {
        self.publisher.close();
        let _ = self.stop.send(true);
        {
            let work = async {
                for task in &mut self.tasks {
                    let _ = task.await;
                }
                self.pool.close().await;
            };
            tokio::pin!(work);
            loop {
                tokio::select! {
                    _ = &mut work => break,
                    _ = tokio::time::sleep(SHUTDOWN_OBSERVATION) => {
                        self.publisher.diagnose("shutdown_still_pending");
                    }
                }
            }
        }
        self.publisher.connected.store(false, Ordering::Relaxed);
        self.statistics()
    }
}
impl Drop for AcquisitionNotifications {
    fn drop(&mut self) {
        self.publisher.close();
        let _ = self.stop.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub(crate) fn start(url: &str, dispatch: Arc<WakeDispatch>) -> Result<AcquisitionNotifications> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        ContractError::InvalidInput("notification startup requires an active Tokio runtime".into())
    })?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .max_lifetime(None)
        .idle_timeout(None)
        .acquire_timeout(IO_TIMEOUT)
        .after_connect(|connection, _| Box::pin(async move {
            sqlx::query("SELECT set_config('statement_timeout','2000',false), set_config('lock_timeout','2000',false), set_config('application_name','ledgence-wake',false)")
                .execute(connection).await?;
            Ok(())
        }))
        .connect_lazy(url).map_err(database_error)?;
    let publisher = Arc::new(Publisher::default());
    {
        let mut remote = dispatch.remote.lock().unwrap_or_else(|e| e.into_inner());
        if remote
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|p| !p.backlog.lock().unwrap_or_else(|e| e.into_inner()).closed)
        {
            return Err(ContractError::InvalidInput(
                "acquisition notifications are already running".into(),
            ));
        }
        *remote = Some(Arc::downgrade(&publisher));
    }
    let (stop, stopped) = watch::channel(false);
    let tasks = vec![
        tokio::spawn(
            receive(pool.clone(), dispatch, publisher.clone(), stopped.clone())
                .with_current_subscriber(),
        ),
        tokio::spawn(publish(pool.clone(), publisher.clone(), stopped).with_current_subscriber()),
    ];
    Ok(AcquisitionNotifications {
        publisher,
        pool,
        stop,
        tasks,
    })
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            break;
        }
    }
}

async fn publish(pool: PgPool, publisher: Arc<Publisher>, mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if let Some(payload) = publisher.pop() {
            let sent = tokio::select! {
                biased;
                _ = stopped(&mut stop) => return,
                sent = tokio::time::timeout(IO_TIMEOUT, sqlx::query("SELECT pg_notify($1,$2)").bind(CHANNEL).bind(&payload).execute(&pool)) => sent,
            };
            publisher.finished(&payload);
            if matches!(sent, Ok(Ok(_))) {
                publisher.published.fetch_add(1, Ordering::Relaxed);
            } else {
                publisher.dropped.fetch_add(1, Ordering::Relaxed);
                publisher.diagnose("publish_failed");
            }
        } else {
            tokio::select! {
                _ = stopped(&mut stop) => return,
                _ = publisher.available.notified() => {},
            }
        }
    }
}

async fn receive(
    pool: PgPool,
    dispatch: Arc<WakeDispatch>,
    publisher: Arc<Publisher>,
    mut stop: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_millis(100);
    loop {
        let setup = async {
            let mut listener = PgListener::connect_with(&pool).await?;
            // Own reconnect setup explicitly so LISTEN itself has a deadline.
            // A new generation is rescanned only after subscription completes.
            listener.eager_reconnect(false);
            listener.listen(CHANNEL).await?;
            Ok::<_, sqlx::Error>(listener)
        };
        let connected = tokio::select! {
            biased;
            _ = stopped(&mut stop) => return,
            connected = tokio::time::timeout(IO_TIMEOUT, setup) => connected,
        };
        if let Ok(Ok(mut listener)) = connected {
            publisher.connected.store(true, Ordering::Relaxed);
            publisher.subscriptions.fetch_add(1, Ordering::Relaxed);
            dispatch.local(AcquisitionHint::Rescan);
            backoff = Duration::from_millis(100);
            let mut burst = 0;
            loop {
                let received = tokio::select! {
                    biased;
                    _ = stopped(&mut stop) => return,
                    received = listener.try_recv() => received,
                };
                match received {
                    Ok(Some(notification)) => {
                        if notification.channel() == CHANNEL {
                            if let Some(hint) = decode(notification.payload()) {
                                dispatch.local(hint);
                            } else {
                                publisher.malformed.fetch_add(1, Ordering::Relaxed);
                                publisher.diagnose("invalid_remote_hint");
                            }
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
                burst += 1;
                if burst == MAX_RECEIVE_BURST {
                    tokio::task::yield_now().await;
                    burst = 0;
                }
            }
        }
        publisher.connected.store(false, Ordering::Relaxed);
        publisher.diagnose("listener_disconnected");
        tokio::select! {
            _ = stopped(&mut stop) => return,
            _ = tokio::time::sleep(backoff) => {},
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
    }
}

#[cfg(test)]
mod tests;
