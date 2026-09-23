use crate::{Config, ConfigError, HTTP_TIMEOUT, client};
use ledgence_worker_api::metrics::{METRIC_TARGET, Metric, MetricKind, MetricOutcome};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, MeterProvider, UpDownCounter},
};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    metrics::{
        PeriodicReader, SdkMeterProvider, Stream, Temporality, data::ResourceMetrics,
        exporter::PushMetricExporter,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};

pub const EXPORT_INTERVAL: Duration = Duration::from_secs(10);
pub const CARDINALITY_LIMIT: usize = 32;
const BOUNDARIES: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0,
    3600.0,
];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetricsStatistics {
    pub failed_exports: u64,
    pub rejected_points: u64,
    pub collector_warnings: u64,
}
#[derive(Debug)]
pub(crate) struct State {
    failed: AtomicU64,
    rejected: AtomicU64,
    warnings: AtomicU64,
    next_warning_ms: AtomicU64,
    started: Instant,
}
impl Default for State {
    fn default() -> Self {
        Self {
            failed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            warnings: AtomicU64::new(0),
            next_warning_ms: AtomicU64::new(0),
            started: Instant::now(),
        }
    }
}
impl State {
    pub fn statistics(&self) -> MetricsStatistics {
        MetricsStatistics {
            failed_exports: self.failed.load(Ordering::Relaxed),
            rejected_points: self.rejected.load(Ordering::Relaxed),
            collector_warnings: self.warnings.load(Ordering::Relaxed),
        }
    }
    pub fn partial(&self, rejected: u64, warning: bool) {
        self.rejected.fetch_add(rejected, Ordering::Relaxed);
        if warning {
            self.warnings.fetch_add(1, Ordering::Relaxed);
        }
        if rejected > 0 || warning {
            self.warn();
        }
    }
    fn warn(&self) {
        let now = self.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if self
            .next_warning_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                (now >= next).then_some(now.saturating_add(30_000))
            })
            .is_ok()
        {
            let stats = self.statistics();
            tracing::warn!(target:"ledgence::telemetry", parent:None, failed_exports=stats.failed_exports, rejected_points=stats.rejected_points, collector_warnings=stats.collector_warnings, "OTLP metrics delivery reported failures or collector warnings");
        }
    }
}
struct ObservedExporter {
    inner: opentelemetry_otlp::MetricExporter,
    state: Arc<State>,
}
impl PushMetricExporter for ObservedExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let result = self.inner.export(metrics).await;
        if result.is_err() {
            self.state.failed.fetch_add(1, Ordering::Relaxed);
            self.state.warn();
        }
        result
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

pub(crate) fn build(
    config: &Config,
    resource: Resource,
    state: Arc<State>,
    traces: Arc<crate::processor::State>,
) -> Result<Option<SdkMeterProvider>, ConfigError> {
    let Some(endpoint) = &config.metrics_endpoint else {
        return Ok(None);
    };
    let client = reqwest::blocking::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .connect_timeout(HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| ConfigError("failed to construct OTLP metrics HTTP client".into()))?;
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_timeout(HTTP_TIMEOUT)
        .with_temporality(Temporality::Cumulative)
        .with_http_client(client::BoundedHttp {
            client,
            state: traces,
            metric_state: Some(state.clone()),
        })
        .build()
        .map_err(|_| ConfigError("failed to configure OTLP metrics exporter".into()))?;
    let reader = PeriodicReader::builder(ObservedExporter {
        inner: exporter,
        state,
    })
    .with_interval(EXPORT_INTERVAL)
    .build();
    Ok(Some(
        SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .with_view(|_| {
                Stream::builder()
                    .with_cardinality_limit(CARDINALITY_LIMIT)
                    .build()
                    .ok()
            })
            .build(),
    ))
}

#[derive(Clone)]
enum Instrument {
    Histogram(Histogram<f64>),
    Counter(Counter<f64>),
    Occupancy(UpDownCounter<f64>),
}
#[derive(Clone)]
pub(crate) struct MetricsLayer {
    instruments: Arc<[Instrument]>,
}
impl MetricsLayer {
    pub fn new(provider: &SdkMeterProvider) -> Self {
        let meter = provider.meter("ledgence");
        let instruments: Vec<_> = Metric::ALL
            .into_iter()
            .map(|metric| match metric.kind() {
                MetricKind::Histogram => Instrument::Histogram(
                    meter
                        .f64_histogram(metric.name())
                        .with_unit(metric.unit())
                        .with_boundaries(BOUNDARIES.to_vec())
                        .build(),
                ),
                MetricKind::Counter => Instrument::Counter(
                    meter
                        .f64_counter(metric.name())
                        .with_unit(metric.unit())
                        .build(),
                ),
                MetricKind::UpDownCounter => Instrument::Occupancy(
                    meter
                        .f64_up_down_counter(metric.name())
                        .with_unit(metric.unit())
                        .build(),
                ),
            })
            .collect();
        Self {
            instruments: instruments.into(),
        }
    }
}
impl<S: Subscriber> Layer<S> for MetricsLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != METRIC_TARGET {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let Some((metric, outcome, value)) = fields
            .metric
            .and_then(Metric::from_id)
            .zip(fields.outcome.and_then(MetricOutcome::from_id))
            .zip(fields.value)
            .map(|((metric, outcome), value)| (metric, outcome, value))
        else {
            return;
        };
        if !metric.accepts(outcome)
            || !value.is_finite()
            || (value < 0.0 && metric.kind() != MetricKind::UpDownCounter)
        {
            return;
        }
        let attribute = [KeyValue::new("outcome", outcome.name())];
        let attributes = if outcome == MetricOutcome::None {
            &[][..]
        } else {
            &attribute[..]
        };
        match &self.instruments[metric as usize] {
            Instrument::Histogram(instrument) => instrument.record(value, attributes),
            Instrument::Counter(instrument) => instrument.add(value, attributes),
            Instrument::Occupancy(instrument) => instrument.add(value, attributes),
        }
    }
}
#[derive(Default)]
struct Fields {
    metric: Option<u64>,
    outcome: Option<u64>,
    value: Option<f64>,
}
impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "metric" => self.metric = Some(value),
            "outcome" => self.outcome = Some(value),
            _ => {}
        }
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "value" {
            self.value = Some(value);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
