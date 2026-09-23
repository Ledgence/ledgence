//! Closed-vocabulary operational observations, independent of any exporter SDK.
//!
//! The existing tracing dispatch carries these events to optional adapters. They
//! are not logs, spans, durable accounting, or execution authority. Exporters must
//! aggregate without doing I/O in the observation path.
use std::time::Instant;

pub const METRIC_TARGET: &str = "ledgence::metrics";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum Metric {
    HttpDuration,
    DatabaseDuration,
    ExecutionDuration,
    PreparationDuration,
    CallbackDuration,
    RecoveryDuration,
    QueueAge,
    CallbackAge,
    CacheLookup,
    ProcessSelection,
    DatabaseRetry,
    RecoveryExpired,
    ConsumerSlots,
    ExecutingPrograms,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Histogram,
    Counter,
    UpDownCounter,
}
impl Metric {
    pub const ALL: [Self; 14] = [
        Self::HttpDuration,
        Self::DatabaseDuration,
        Self::ExecutionDuration,
        Self::PreparationDuration,
        Self::CallbackDuration,
        Self::RecoveryDuration,
        Self::QueueAge,
        Self::CallbackAge,
        Self::CacheLookup,
        Self::ProcessSelection,
        Self::DatabaseRetry,
        Self::RecoveryExpired,
        Self::ConsumerSlots,
        Self::ExecutingPrograms,
    ];
    pub fn from_id(id: u64) -> Option<Self> {
        Self::ALL.get(usize::try_from(id).ok()?).copied()
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::HttpDuration => "ledgence.http.request.duration",
            Self::DatabaseDuration => "ledgence.database.operation.duration",
            Self::ExecutionDuration => "ledgence.worker.execution.duration",
            Self::PreparationDuration => "ledgence.worker.preparation.duration",
            Self::CallbackDuration => "ledgence.completion.delivery.duration",
            Self::RecoveryDuration => "ledgence.recovery.scan.duration",
            Self::QueueAge => "ledgence.task.claim.queue_age",
            Self::CallbackAge => "ledgence.completion.delivery.age",
            Self::CacheLookup => "ledgence.worker.cache.lookup",
            Self::ProcessSelection => "ledgence.worker.process.selection",
            Self::DatabaseRetry => "ledgence.database.operation.retry",
            Self::RecoveryExpired => "ledgence.recovery.expired",
            Self::ConsumerSlots => "ledgence.worker.consumer.occupied",
            Self::ExecutingPrograms => "ledgence.worker.execution.active",
        }
    }
    pub fn kind(self) -> MetricKind {
        match self {
            Self::CacheLookup
            | Self::ProcessSelection
            | Self::DatabaseRetry
            | Self::RecoveryExpired => MetricKind::Counter,
            Self::ConsumerSlots | Self::ExecutingPrograms => MetricKind::UpDownCounter,
            _ => MetricKind::Histogram,
        }
    }
    pub fn unit(self) -> &'static str {
        match self.kind() {
            MetricKind::Histogram => "s",
            _ => "1",
        }
    }
    pub fn accepts(self, outcome: MetricOutcome) -> bool {
        use MetricOutcome::*;
        match self {
            Self::HttpDuration => matches!(outcome, Ok | ClientError | ServerError | Cancelled),
            Self::DatabaseDuration | Self::PreparationDuration | Self::RecoveryDuration => {
                matches!(outcome, Ok | Failed | Cancelled)
            }
            Self::ExecutionDuration => matches!(outcome, Ok | Failed | RuntimeError | Cancelled),
            Self::CallbackDuration => matches!(outcome, Ok | Retry | Cancelled),
            Self::QueueAge => matches!(outcome, Integrated | External),
            Self::CallbackAge
            | Self::DatabaseRetry
            | Self::RecoveryExpired
            | Self::ConsumerSlots
            | Self::ExecutingPrograms => outcome == None,
            Self::CacheLookup => matches!(outcome, Hit | Miss),
            Self::ProcessSelection => matches!(outcome, Reused | Started),
        }
    }
    /// Only fixed enum dimensions are emitted. Negative values are valid only
    /// for slot releases; nonfinite values and invalid combinations are ignored.
    #[inline]
    pub fn record(self, value: f64, outcome: MetricOutcome) {
        if value.is_finite()
            && (value >= 0.0 || self.kind() == MetricKind::UpDownCounter)
            && self.accepts(outcome)
        {
            tracing::event!(target: "ledgence::metrics", tracing::Level::DEBUG, metric = self as u64, value, outcome = outcome as u64);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum MetricOutcome {
    None,
    Ok,
    Failed,
    RuntimeError,
    Cancelled,
    ClientError,
    ServerError,
    Hit,
    Miss,
    Reused,
    Started,
    Retry,
    Integrated,
    External,
}
impl MetricOutcome {
    pub const ALL: [Self; 14] = [
        Self::None,
        Self::Ok,
        Self::Failed,
        Self::RuntimeError,
        Self::Cancelled,
        Self::ClientError,
        Self::ServerError,
        Self::Hit,
        Self::Miss,
        Self::Reused,
        Self::Started,
        Self::Retry,
        Self::Integrated,
        Self::External,
    ];
    pub fn from_id(id: u64) -> Option<Self> {
        Self::ALL.get(usize::try_from(id).ok()?).copied()
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::RuntimeError => "runtime_error",
            Self::Cancelled => "cancelled",
            Self::ClientError => "client_error",
            Self::ServerError => "server_error",
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Reused => "reused",
            Self::Started => "started",
            Self::Retry => "retry",
            Self::Integrated => "integrated",
            Self::External => "external",
        }
    }
}

/// Records cancellation when a future is dropped before an explicit finish.
/// Captures the original dispatch so task migration/drop cannot split a series.
pub struct MetricTimer {
    metric: Metric,
    started: Option<(Instant, tracing::Dispatch)>,
    outcome: MetricOutcome,
}
impl MetricTimer {
    pub fn start(metric: Metric) -> Self {
        Self {
            metric,
            started: enabled().then(|| {
                (
                    Instant::now(),
                    tracing::dispatcher::get_default(Clone::clone),
                )
            }),
            outcome: MetricOutcome::Cancelled,
        }
    }
    pub fn finish(mut self, outcome: MetricOutcome) {
        self.outcome = outcome;
    }
}
impl Drop for MetricTimer {
    fn drop(&mut self) {
        if let Some((started, dispatch)) = &self.started {
            tracing::dispatcher::with_default(dispatch, || {
                self.metric
                    .record(started.elapsed().as_secs_f64(), self.outcome)
            });
        }
    }
}

/// Own alongside the actual semaphore permit, including retained cleanup work.
pub struct MetricGuard {
    metric: Metric,
    dispatch: Option<tracing::Dispatch>,
}
impl MetricGuard {
    pub fn consumer() -> Self {
        Self::new(Metric::ConsumerSlots)
    }
    pub fn execution() -> Self {
        Self::new(Metric::ExecutingPrograms)
    }
    fn new(metric: Metric) -> Self {
        let dispatch = enabled().then(|| tracing::dispatcher::get_default(Clone::clone));
        if let Some(dispatch) = &dispatch {
            tracing::dispatcher::with_default(dispatch, || metric.record(1.0, MetricOutcome::None));
        }
        Self { metric, dispatch }
    }
}
impl Drop for MetricGuard {
    fn drop(&mut self) {
        if let Some(dispatch) = &self.dispatch {
            tracing::dispatcher::with_default(dispatch, || {
                self.metric.record(-1.0, MetricOutcome::None)
            });
        }
    }
}
#[inline]
fn enabled() -> bool {
    tracing::enabled!(target: "ledgence::metrics", tracing::Level::DEBUG)
}
