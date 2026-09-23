//! Optional tracing lifecycle, owned outside execution and signal runtimes.
#[cfg(not(feature = "otel"))]
use ledgence_worker_api::NoopTraceBridge;
use ledgence_worker_api::TraceBridge;
use std::sync::Arc;
use tracing_subscriber::fmt::MakeWriter;
#[cfg(feature = "otel")]
use tracing_subscriber::util::SubscriberInitExt;

pub struct Telemetry {
    #[cfg(feature = "otel")]
    inner: ledgence_adapter_otel::Telemetry,
}
impl Telemetry {
    pub fn start<W>(service: &str, writer: W) -> Result<Self, String>
    where
        W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
    {
        let filter =
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
        #[cfg(feature = "otel")]
        {
            let inner =
                ledgence_adapter_otel::Telemetry::from_env(service, env!("CARGO_PKG_VERSION"))
                    .map_err(|error| error.to_string())?;
            inner.subscriber(writer, filter).init();
            Ok(Self { inner })
        }
        #[cfg(not(feature = "otel"))]
        {
            let _ = service;
            if std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
                || std::env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_some()
            {
                return Err("this executable was built without the otel feature".into());
            }
            tracing_subscriber::fmt()
                .with_env_filter(
                    filter.add_directive(
                        "ledgence::metrics=off"
                            .parse()
                            .expect("static metrics filter"),
                    ),
                )
                .with_writer(writer)
                .json()
                .init();
            Ok(Self {})
        }
    }
    pub fn bridge(&self) -> Arc<dyn TraceBridge> {
        #[cfg(feature = "otel")]
        {
            self.inner.bridge()
        }
        #[cfg(not(feature = "otel"))]
        {
            Arc::new(NoopTraceBridge)
        }
    }
    /// Never register exporter shutdown as a Tokio blocking task: a forced exit
    /// must not wait for that task during runtime destruction.
    pub async fn finish(self) {
        #[cfg(feature = "otel")]
        {
            let (send, receive) = tokio::sync::oneshot::channel();
            let subscriber = tracing::dispatcher::get_default(Clone::clone);
            let started = std::thread::Builder::new().name("ledgence-telemetry-drain".into()).spawn(move || {
                let _subscriber = tracing::dispatcher::set_default(&subscriber);
                if let Err(error) = self.inner.shutdown() {
                    tracing::warn!(%error, "telemetry drain incomplete; execution results are unchanged");
                }
                let _ = send.send(());
            });
            if started.is_ok() {
                if tokio::time::timeout(std::time::Duration::from_secs(3), receive)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        "telemetry drain deadline reached; abandoning optional telemetry"
                    );
                }
            } else {
                tracing::warn!("could not start telemetry drain; abandoning optional telemetry");
            }
        }
    }
}
