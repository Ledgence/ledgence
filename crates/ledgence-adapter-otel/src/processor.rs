use opentelemetry::{Array, Context, KeyValue, Value, trace::Status};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{BatchSpanProcessor, Span, SpanData, SpanExporter, SpanProcessor},
};
use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

pub(crate) const QUEUE_SPANS: usize = 1024;
pub(crate) const BATCH_SPANS: usize = 256;
pub(crate) const VALUE_BYTES: usize = 256;

/// Local best-effort telemetry delivery counters, independent from task outcomes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Statistics {
    pub queue_dropped_spans: u64,
    pub failed_batches: u64,
    pub failed_spans: u64,
    pub rejected_spans: u64,
    pub collector_warnings: u64,
}

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) pending: AtomicUsize,
    dropped: AtomicU64,
    failed_batches: AtomicU64,
    failed_spans: AtomicU64,
    rejected_spans: AtomicU64,
    collector_warnings: AtomicU64,
    started: Instant,
    next_warning_ms: AtomicU64,
}
impl Default for State {
    fn default() -> Self {
        Self {
            pending: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            failed_batches: AtomicU64::new(0),
            failed_spans: AtomicU64::new(0),
            rejected_spans: AtomicU64::new(0),
            collector_warnings: AtomicU64::new(0),
            started: Instant::now(),
            next_warning_ms: AtomicU64::new(0),
        }
    }
}
impl State {
    pub(crate) fn statistics(&self) -> Statistics {
        Statistics {
            queue_dropped_spans: self.dropped.load(Ordering::Relaxed),
            failed_batches: self.failed_batches.load(Ordering::Relaxed),
            failed_spans: self.failed_spans.load(Ordering::Relaxed),
            rejected_spans: self.rejected_spans.load(Ordering::Relaxed),
            collector_warnings: self.collector_warnings.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn partial_success(&self, rejected: u64, warning: bool) {
        self.rejected_spans.fetch_add(rejected, Ordering::Relaxed);
        if warning {
            self.collector_warnings.fetch_add(1, Ordering::Relaxed);
        }
        if rejected > 0 || warning {
            self.warning();
        }
    }
    fn failed(&self, count: usize) {
        self.failed_batches.fetch_add(1, Ordering::Relaxed);
        self.failed_spans.fetch_add(count as u64, Ordering::Relaxed);
        self.warning();
    }
    fn warning(&self) {
        let now = self.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if self
            .next_warning_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                (now >= next).then_some(now.saturating_add(30_000))
            })
            .is_ok()
        {
            let stats = self.statistics();
            tracing::warn!(target: "ledgence::telemetry", parent: None, failed_batches = stats.failed_batches, failed_spans = stats.failed_spans, rejected_spans = stats.rejected_spans, collector_warnings = stats.collector_warnings, "OTLP trace delivery reported failures or collector warnings");
        }
    }
}

#[derive(Debug)]
pub(crate) struct BoundedProcessor {
    pub inner: BatchSpanProcessor,
    pub state: Arc<State>,
}
impl SpanProcessor for BoundedProcessor {
    fn on_start(&self, span: &mut Span, parent: &Context) {
        self.inner.on_start(span, parent);
    }
    fn on_end(&self, mut span: SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }
        // Includes the in-flight batch. This stricter budget prevents the SDK
        // queue filling behind an exporter and makes drops observable locally.
        if self
            .state
            .pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < QUEUE_SPANS).then_some(n + 1)
            })
            .is_err()
        {
            if self.state.dropped.fetch_add(1, Ordering::Relaxed) == 0 {
                tracing::warn!(target: "ledgence::telemetry", parent: None, "trace queue full; additional completed spans are discarded");
            }
            return;
        }
        bound_span(&mut span);
        self.inner.on_end(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[derive(Debug)]
pub(crate) struct ObservedExporter<E> {
    pub inner: E,
    pub state: Arc<State>,
}
impl<E: SpanExporter> SpanExporter for ObservedExporter<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let count = batch.len();
        let result = self.inner.export(batch).await;
        self.state.pending.fetch_sub(count, Ordering::Relaxed);
        if result.is_err() {
            self.state.failed(count);
        }
        result
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

pub(crate) fn truncate(value: &str, bytes: usize) -> String {
    let mut end = value.len().min(bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
fn attributes(values: &mut [KeyValue]) {
    for value in values {
        // Copy even short owned values: their backing capacity can be much
        // larger than their logical length after a caller truncates them.
        value.key = truncate(value.key.as_str(), 128).into();
        match &mut value.value {
            Value::String(text) => *text = truncate(text.as_str(), VALUE_BYTES).into(),
            Value::Array(array) => match array {
                Array::Bool(items) => *items = items.iter().copied().take(16).collect(),
                Array::I64(items) => *items = items.iter().copied().take(16).collect(),
                Array::F64(items) => *items = items.iter().copied().take(16).collect(),
                Array::String(items) => {
                    *items = items
                        .iter()
                        .take(16)
                        .map(|item| truncate(item.as_str(), 16).into())
                        .collect()
                }
                _ => {}
            },
            _ => {}
        }
    }
}
pub(crate) fn bound_span(span: &mut SpanData) {
    span.name = Cow::Owned(truncate(&span.name, 128));
    span.attributes.truncate(32);
    span.attributes.shrink_to_fit();
    attributes(&mut span.attributes);
    span.events.events.truncate(4);
    span.events.events.shrink_to_fit();
    for event in &mut span.events.events {
        event.name = Cow::Owned(truncate(&event.name, 128));
        event.attributes.truncate(8);
        event.attributes.shrink_to_fit();
        attributes(&mut event.attributes);
    }
    span.links.links.truncate(8);
    span.links.links.shrink_to_fit();
    for link in &mut span.links.links {
        link.attributes.truncate(4);
        link.attributes.shrink_to_fit();
        attributes(&mut link.attributes);
    }
    if let Status::Error { description } = &mut span.status {
        *description = Cow::Owned(truncate(description, VALUE_BYTES));
    }
}
