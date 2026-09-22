//! Optional HTTP completion delivery. The durable store owns retry scheduling.

use ledgence_orchestration_api::{
    CompletionDeliveryOutcome, CompletionDestination, CompletionLease, CompletionSender,
    ContractError, ContractFuture, Result, Scope, decode_unique_json,
};
use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::Instrument;

pub const CONFIG_MAX_BYTES: usize = 64 * 1024;
pub const MAX_DESTINATIONS: usize = 16;
pub const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const RESPONSE_HEADER_MAX_BYTES: usize = 16 * 1024;
const RESPONSE_HEADER_MAX_COUNT: usize = 64;
const RETRY_AFTER_MAX_MS: u64 = 300_000;

/// Operator configuration: request callers choose an alias, never a URL.
#[derive(Clone)]
pub struct HttpCompletionDestination {
    pub destination: CompletionDestination,
    url: Url,
}

#[derive(Clone)]
pub struct CompletionConfig {
    pub destinations: Vec<HttpCompletionDestination>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    destinations: Vec<DestinationConfiguration>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DestinationConfiguration {
    scope: Scope,
    destination: String,
    url: String,
}
#[derive(Serialize)]
struct HttpBinding<'a> {
    kind: &'static str,
    url: &'a str,
}

impl CompletionConfig {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let config: Configuration = decode_unique_json(bytes, CONFIG_MAX_BYTES)?;
        if config.destinations.is_empty() || config.destinations.len() > MAX_DESTINATIONS {
            return Err(invalid(
                "completion configuration requires 1..16 destinations",
            ));
        }
        let mut destinations = Vec::with_capacity(config.destinations.len());
        let mut aliases = HashSet::new();
        for configured in config.destinations {
            let url = validated_url(&configured.url)?;
            let destination = CompletionDestination {
                scope: configured.scope,
                destination: configured.destination,
                binding: serde_json::to_string(&HttpBinding {
                    kind: "http",
                    url: url.as_str(),
                })
                .map_err(|_| invalid("cannot encode completion destination binding"))?,
            };
            destination.validate()?;
            if !aliases.insert((destination.scope.clone(), destination.destination.clone())) {
                return Err(invalid("duplicate scoped completion destination alias"));
            }
            destinations.push(HttpCompletionDestination { destination, url });
        }
        Ok(Self { destinations })
    }

    /// Finite-size file reads belong on a blocking thread in the executable.
    pub fn load(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path)
            .map_err(|_| invalid("cannot inspect completion configuration file"))?;
        if !metadata.is_file() || metadata.len() > CONFIG_MAX_BYTES as u64 {
            return Err(invalid(
                "completion configuration must be a regular file of at most 64 KiB",
            ));
        }
        let mut bytes = Vec::new();
        File::open(path)
            .map_err(|_| invalid("cannot open completion configuration file"))?
            .take(CONFIG_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid("cannot read completion configuration file"))?;
        Self::decode(&bytes)
    }
}

fn validated_url(value: &str) -> Result<Url> {
    if value.len() > 2048 || value.trim() != value {
        return Err(invalid(
            "completion URL exceeds 2048 bytes or contains surrounding whitespace",
        ));
    }
    let url = Url::parse(value).map_err(|_| invalid("invalid completion URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "completion URL must be HTTP/HTTPS without credentials or fragment",
        ));
    }
    Ok(url)
}

/// Reuses a bounded HTTP/1.1 connection pool for one configured destination.
/// Responses acknowledge acceptance at their headers; bodies are never buffered.
pub struct HttpCompletionSender {
    configured: HttpCompletionDestination,
    client: Client,
    trace: Arc<dyn TraceBridge>,
}
impl HttpCompletionSender {
    pub fn new(configured: HttpCompletionDestination) -> Result<Self> {
        configured.destination.validate()?;
        let expected_binding = serde_json::to_string(&HttpBinding {
            kind: "http",
            url: configured.url.as_str(),
        })
        .map_err(|_| invalid("cannot encode completion destination binding"))?;
        if configured.destination.binding != expected_binding {
            return Err(invalid(
                "HTTP completion destination binding differs from its URL",
            ));
        }
        let client = Client::builder()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .timeout(DELIVERY_TIMEOUT)
            .build()
            .map_err(|_| {
                ContractError::Unavailable("cannot initialize completion HTTP client".into())
            })?;
        Ok(Self {
            configured,
            client,
            trace: Arc::new(NoopTraceBridge),
        })
    }

    pub fn with_trace_bridge(mut self, trace: Arc<dyn TraceBridge>) -> Self {
        self.trace = trace;
        self
    }

    async fn send(
        &self,
        lease: &CompletionLease,
        deadline: Instant,
    ) -> Result<CompletionDeliveryOutcome> {
        lease.validate()?;
        if lease.subscription.command.scope != self.configured.destination.scope
            || lease.subscription.command.destination != self.configured.destination.destination
        {
            return Err(invalid(
                "completion lease does not match configured destination",
            ));
        }
        let started = Instant::now();
        let deadline = deadline.min(started + DELIVERY_TIMEOUT);
        if started >= deadline {
            return Ok(retry("delivery_deadline_elapsed", None));
        }
        let span = tracing::info_span!(
            "ledgence.completion.deliver",
            otel.kind = "client",
            http.request.method = "POST",
            ledgence.tenant.id = %lease.subscription.command.scope.tenant_id,
            ledgence.namespace = %lease.subscription.command.scope.namespace,
            ledgence.completion.subscription.id = %lease.subscription.subscription_id,
            ledgence.completion.destination = %lease.subscription.command.destination,
            ledgence.completion.generation = lease.subscription.generation,
            ledgence.completion.attempt = lease.subscription.attempts,
            http.response.status_code = tracing::field::Empty,
            ledgence.duration_ms = tracing::field::Empty,
            error.type = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        // Each durable relay attempt starts a separate transport trace and links
        // to the immutable event creation context. The payload never changes.
        self.trace.set_parent(&span, None);
        if let Some(event) = &lease.subscription.event
            && let Some(context) = event.trace_context()
        {
            self.trace.add_link(&span, &context);
        }
        let transport = self.trace.context(&span);
        async {
            let mut request = self
                .client
                .post(self.configured.url.clone())
                .header(header::CONTENT_TYPE, "application/cloudevents+json")
                .header(
                    "ledgence-subscription-id",
                    &lease.subscription.subscription_id,
                )
                .header(
                    "ledgence-delivery-generation",
                    lease.subscription.generation,
                )
                .header("ledgence-delivery-attempt", lease.subscription.attempts)
                .body(lease.event_bytes.clone());
            if let Some(context) = &transport {
                request = request.header("traceparent", &context.traceparent);
                if let Some(state) = &context.tracestate {
                    request = request.header("tracestate", state);
                }
            }
            let response = tokio::time::timeout_at(deadline.into(), request.send()).await;
            let outcome = match response {
                _ if Instant::now() >= deadline => retry("delivery_deadline_elapsed", None),
                Ok(Ok(response)) => {
                    let status = response.status().as_u16();
                    tracing::Span::current().record("http.response.status_code", status);
                    let header_bytes =
                        response
                            .headers()
                            .iter()
                            .fold(0usize, |size, (name, value)| {
                                size.saturating_add(name.as_str().len())
                                    .saturating_add(value.as_bytes().len())
                            });
                    if header_bytes > RESPONSE_HEADER_MAX_BYTES
                        || response.headers().len() > RESPONSE_HEADER_MAX_COUNT
                    {
                        retry("response_headers_exceeded", None)
                    } else if response.status().is_success() {
                        CompletionDeliveryOutcome::Confirmed
                    } else {
                        retry(&format!("http_{status}"), retry_after(response.headers()))
                    }
                    // Dropping the response never reads an unbounded receiver
                    // body. Empty responses can reuse their connection.
                }
                Ok(Err(_)) => retry("http_exchange_failed", None),
                Err(_) => retry("delivery_deadline_elapsed", None),
            };
            tracing::Span::current()
                .record("ledgence.duration_ms", started.elapsed().as_millis() as u64);
            if let CompletionDeliveryOutcome::Retry { reason, .. } = &outcome {
                tracing::Span::current().record("error.type", reason.as_str());
                tracing::Span::current().record("otel.status_code", "ERROR");
            }
            Ok(outcome)
        }
        .instrument(span)
        .await
    }
}
impl CompletionSender for HttpCompletionSender {
    fn deliver<'a>(
        &'a self,
        lease: &'a CompletionLease,
        deadline: Instant,
    ) -> ContractFuture<'a, CompletionDeliveryOutcome> {
        Box::pin(self.send(lease, deadline))
    }
}

fn retry_after(headers: &header::HeaderMap) -> Option<u64> {
    if headers.get_all(header::RETRY_AFTER).iter().count() != 1 {
        return None;
    }
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(
        value
            .parse::<u64>()
            .ok()?
            .saturating_mul(1000)
            .min(RETRY_AFTER_MAX_MS),
    )
}
fn retry(reason: &str, retry_after_ms: Option<u64>) -> CompletionDeliveryOutcome {
    CompletionDeliveryOutcome::Retry {
        reason: reason.into(),
        retry_after_ms,
    }
}
fn invalid(reason: &str) -> ContractError {
    ContractError::InvalidInput(reason.into())
}

#[cfg(test)]
mod tests;
