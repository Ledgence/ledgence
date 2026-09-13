//! Optional Rust trace export and correlated JSON logging.
//!
//! Construct before an async runtime. Install the returned subscriber at the
//! executable boundary, carry its dispatch into owned threads, and call
//! [`Telemetry::shutdown`] on a separate OS thread while retaining signal handling.
//! No global provider or subscriber is installed by this crate.
mod bridge;
mod client;
mod config;
mod logs;
mod processor;

use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanLimits},
};
use std::{sync::Arc, time::Duration};
use tracing_subscriber::{
    EnvFilter, Layer,
    filter::{FilterExt, filter_fn},
    fmt::{MakeWriter, format::JsonFields},
    prelude::*,
};

pub use bridge::OtelTraceBridge;
pub use config::{Config, ConfigError};
pub use processor::Statistics;

pub const HTTP_TIMEOUT: Duration = Duration::from_secs(2);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// Owns the optional exporter lifecycle. Dropping does not block an async runtime.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
    state: Arc<processor::State>,
}
impl Telemetry {
    pub fn from_env(service_name: &str, service_version: &str) -> Result<Self, ConfigError> {
        Self::new(Config::from_env(service_name, service_version)?)
    }
    /// Construct before starting the application async runtime.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let state = Arc::new(processor::State::default());
        let provider = match &config.endpoint {
            None => None,
            Some(endpoint) => {
                // The blocking client owns a private runtime and must be built
                // and destroyed outside any application Tokio task.
                let client = reqwest::blocking::Client::builder()
                    .timeout(HTTP_TIMEOUT)
                    .connect_timeout(HTTP_TIMEOUT)
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|_| ConfigError("failed to construct OTLP HTTP client".into()))?;
                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_endpoint(endpoint)
                    .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                    .with_timeout(HTTP_TIMEOUT)
                    .with_http_client(client::BoundedHttp {
                        client,
                        state: state.clone(),
                    })
                    .build()
                    .map_err(|_| {
                        ConfigError("failed to configure OTLP HTTP/protobuf exporter".into())
                    })?;
                let exporter = processor::ObservedExporter {
                    inner: exporter,
                    state: state.clone(),
                };
                let batch = BatchSpanProcessor::builder(exporter)
                    .with_batch_config(
                        BatchConfigBuilder::default()
                            .with_max_queue_size(processor::QUEUE_SPANS)
                            .with_max_export_batch_size(processor::BATCH_SPANS)
                            .with_scheduled_delay(Duration::from_secs(1))
                            .build(),
                    )
                    .build();
                let processor = processor::BoundedProcessor {
                    inner: batch,
                    state: state.clone(),
                };
                let mut attrs = vec![
                    KeyValue::new("service.name", config.service_name),
                    KeyValue::new("service.version", config.service_version),
                    KeyValue::new("service.instance.id", config.instance_id),
                ];
                if let Some(environment) = config.environment {
                    attrs.push(KeyValue::new("deployment.environment.name", environment));
                }
                Some(
                    SdkTracerProvider::builder()
                        .with_span_processor(processor)
                        .with_resource(Resource::builder_empty().with_attributes(attrs).build())
                        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                            config.root_ratio,
                        ))))
                        .with_span_limits(SpanLimits {
                            max_attributes_per_span: 32,
                            max_events_per_span: 4,
                            max_links_per_span: 8,
                            max_attributes_per_event: 8,
                            max_attributes_per_link: 4,
                        })
                        .build(),
                )
            }
        };
        Ok(Self { provider, state })
    }
    pub fn bridge(&self) -> Arc<dyn TraceBridge> {
        if self.provider.is_some() {
            Arc::new(OtelTraceBridge)
        } else {
            Arc::new(NoopTraceBridge)
        }
    }
    pub fn enabled(&self) -> bool {
        self.provider.is_some()
    }
    pub fn statistics(&self) -> Statistics {
        self.state.statistics()
    }

    /// Logs honor their own filter; enabled platform spans remain available at
    /// every level. Exporter/internal-library traffic cannot create trace loops.
    pub fn subscriber<W>(
        &self,
        writer: W,
        logs: EnvFilter,
    ) -> impl tracing::Subscriber + Send + Sync + 'static
    where
        W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
    {
        let otel = self.provider.as_ref().map(|provider| {
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("ledgence"))
                .with_location(false)
                .with_threads(false)
                .with_target(false)
                .with_tracked_inactivity(false)
                .with_context_activation(true)
                .with_filter(filter_fn(|metadata| {
                    metadata.is_span()
                        && (metadata.target().starts_with("ledgence_")
                            || metadata.target().starts_with("ledgence::"))
                        && metadata.target() != "ledgence::context"
                }))
        });
        let log_filter = logs.and(filter_fn(|metadata| {
            !metadata.target().starts_with("opentelemetry")
                && !metadata.target().starts_with("tracing_opentelemetry")
        }));
        let dispatch = Arc::new(std::sync::OnceLock::new());
        tracing_subscriber::registry()
            .with(otel)
            .with(logs::CaptureDispatch(dispatch.clone()))
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(logs::CorrelatedJson(
                        tracing_subscriber::fmt::format().json(),
                        dispatch,
                    ))
                    .fmt_fields(JsonFields::new())
                    .with_writer(writer)
                    .with_filter(log_filter),
            )
    }
    /// Best-effort total bounded drain. Run on a dedicated OS thread, not in a
    /// Tokio task or spawn_blocking job whose runtime destructor could then wait.
    pub fn shutdown(mut self) -> Result<(), ShutdownError> {
        let result = self.provider.take().map_or(Ok(()), |provider| {
            shutdown_provider(provider, SHUTDOWN_TIMEOUT)
        });
        let stats = self.statistics();
        if stats != Statistics::default() {
            tracing::warn!(target: "ledgence::telemetry", parent: None, queue_dropped_spans = stats.queue_dropped_spans, failed_batches = stats.failed_batches, failed_spans = stats.failed_spans, rejected_spans = stats.rejected_spans, collector_warnings = stats.collector_warnings, "trace delivery finished with telemetry loss");
        }
        result
    }
}
impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            // Keep provider destruction off the application runtime, including
            // explicit force, startup errors and cancelled shutdown observers.
            let provider = std::mem::ManuallyDrop::new(provider);
            let _ = std::thread::Builder::new()
                .name("ledgence-telemetry-abandon".into())
                .spawn(move || {
                    let provider = std::mem::ManuallyDrop::into_inner(provider);
                    let _ = provider.shutdown_with_timeout(Duration::ZERO);
                });
            // If OS thread creation fails, intentionally retain the provider.
            // Dropping its blocking runtime on the caller would break force.
        }
    }
}

fn shutdown_provider(provider: SdkTracerProvider, timeout: Duration) -> Result<(), ShutdownError> {
    let started = std::time::Instant::now();
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let provider = std::mem::ManuallyDrop::new(provider);
    std::thread::Builder::new()
        .name("ledgence-telemetry-shutdown".into())
        .spawn(move || {
            let provider = std::mem::ManuallyDrop::into_inner(provider);
            let result = provider
                .shutdown_with_timeout(timeout.saturating_sub(started.elapsed()))
                .map_err(|error| ShutdownError(error.to_string()));
            // SDK shutdown can acknowledge before its exporter is destroyed.
            // Observe both SDK shutdown and destruction within the outer budget.
            drop(provider);
            let _ = sent.send(result);
        })
        .map_err(|_| {
            ShutdownError("could not start telemetry shutdown thread; telemetry abandoned".into())
        })?;
    received
        .recv_timeout(timeout.saturating_sub(started.elapsed()))
        .map_err(|_| {
            ShutdownError(
                "telemetry shutdown deadline exceeded; remaining telemetry abandoned".into(),
            )
        })?
}

#[derive(Debug)]
pub struct ShutdownError(String);
impl std::fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}
impl std::error::Error for ShutdownError {}

#[cfg(test)]
mod tests;
