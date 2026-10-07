//! Binary artifact publication uses an independent transfer budget. It never
//! changes the thirty-second deadline of task/catalog control operations.
use super::{ExchangeMetadata, Observer};
use ledgence_worker_api::{
    Digest, ProgramDescriptor, PublicationCapabilities, PublicationError, PublicationErrorKind,
    PublicationLimits, PublicationResult, PublishArtifactResult,
};
use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
use reqwest::{Client, Response, Url, header};
use sha2::{Digest as _, Sha256};
use std::{sync::Arc, time::Duration};
use tracing::Instrument;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const RESPONSE_LIMIT: usize = 16 * 1024;
#[derive(Clone)]
pub struct HttpProgramPublisher {
    client: Client,
    base: Url,
    timeout: Duration,
    observer: Option<Arc<Observer>>,
    trace_bridge: Arc<dyn TraceBridge>,
}
impl HttpProgramPublisher {
    pub fn new(base_url: &str) -> PublicationResult<Self> {
        Self::with_timeout(base_url, DEFAULT_TIMEOUT)
    }
    pub fn with_timeout(base_url: &str, timeout: Duration) -> PublicationResult<Self> {
        if timeout.is_zero() || timeout > DEFAULT_TIMEOUT {
            return Err(invalid(
                "upload timeout must be positive and at most 120 seconds",
            ));
        }
        let mut base = Url::parse(base_url).map_err(|_| invalid("invalid server URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(invalid(
                "server URL must be HTTP/HTTPS without credentials, query, or fragment",
            ));
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .timeout(timeout)
            .build()
            .map_err(|_| unavailable())?;
        Ok(Self {
            client,
            base,
            timeout,
            observer: None,
            trace_bridge: Arc::new(NoopTraceBridge),
        })
    }
    pub fn with_observer(
        mut self,
        observer: impl Fn(&ExchangeMetadata) + Send + Sync + 'static,
    ) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }
    pub fn with_trace_bridge(mut self, bridge: Arc<dyn TraceBridge>) -> Self {
        self.trace_bridge = bridge;
        self
    }
    fn request_trace(
        &self,
        mut request: reqwest::RequestBuilder,
        span: &tracing::Span,
    ) -> reqwest::RequestBuilder {
        if let Some(context) = self
            .trace_bridge
            .context(span)
            .filter(|context| context.validate().is_ok())
        {
            request = request.header("traceparent", context.traceparent);
            if let Some(state) = context.tracestate {
                request = request.header("tracestate", state);
            }
        }
        request
    }
    fn observe<T>(
        &self,
        mut metadata: ExchangeMetadata,
        span: &tracing::Span,
        started: std::time::Instant,
        result: &PublicationResult<T>,
    ) {
        metadata.elapsed = started.elapsed();
        span.record("ledgence.duration_ms", metadata.elapsed.as_millis() as u64);
        if result.is_err() {
            span.record("otel.status_code", "ERROR");
            span.record("error.type", "publication_exchange");
        }
        if let Some(observer) = &self.observer {
            observer(&metadata);
        }
    }
    pub async fn capabilities(&self) -> PublicationResult<PublicationCapabilities> {
        let started = std::time::Instant::now();
        let span = exchange_span("GET", "/v1/programs/publication-capabilities");
        let mut metadata = ExchangeMetadata {
            request_id: None,
            status: None,
            elapsed: Duration::ZERO,
        };
        let result = tokio::time::timeout(Duration::from_secs(30).min(self.timeout), async {
            let url = self.base.join("v1/programs/publication-capabilities").map_err(|_| invalid("invalid server URL"))?;
            let response = self.request_trace(self.client.get(url), &span).send().await.map_err(|_| unavailable())?;
            observe_headers(&response, &mut metadata, &span);
            if response.status().as_u16() == 404 {
                return Err(PublicationError::new(PublicationErrorKind::Disabled,
                    "server does not support program publication; use --store or upgrade the server"));
            }
            if response.status().as_u16() != 200 { return Err(unavailable()); }
            let bytes = read_response(response).await?;
            let capabilities: PublicationCapabilities = ledgence_orchestration_api::decode_unique_json(&bytes, RESPONSE_LIMIT)
                .map_err(|_| unavailable())?;
            validate_capabilities(&capabilities)?;
            Ok(capabilities)
        }.instrument(span.clone())).await.unwrap_or_else(|_| Err(unavailable()));
        self.observe(metadata, &span, started, &result);
        result
    }
    /// Negotiate before sending any artifact bytes. Performs one PUT, with no
    /// automatic retries. Keep the archive externally until the outcome is known.
    pub async fn publish(
        &self,
        descriptor: &ProgramDescriptor,
        archive: Vec<u8>,
    ) -> PublicationResult<PublishArtifactResult> {
        descriptor
            .validate()
            .map_err(|_| invalid("invalid prepared descriptor"))?;
        if descriptor.size != archive.len() as u64 {
            return Err(invalid("prepared archive size does not match descriptor"));
        }
        if archive.len() as u64 > PublicationLimits::default().max_archive_bytes {
            return Err(PublicationError::new(
                PublicationErrorKind::TooLarge,
                "prepared archive exceeds supported limit",
            ));
        }
        let capabilities = self.capabilities().await?;
        if !capabilities.enabled {
            return Err(PublicationError::new(
                PublicationErrorKind::Disabled,
                "program publication is not enabled on this server",
            ));
        }
        if archive.len() as u64 > capabilities.limits.max_archive_bytes {
            return Err(PublicationError::new(
                PublicationErrorKind::TooLarge,
                "prepared archive exceeds server publication limit",
            ));
        }
        let expected = descriptor.clone();
        let archive = tokio::task::spawn_blocking(move || {
            let actual = Digest(format!("sha256:{:x}", Sha256::digest(&archive)));
            if actual != expected.digest {
                return Err(invalid("prepared archive hash does not match descriptor"));
            }
            Ok(archive)
        })
        .await
        .map_err(|_| unavailable())??;
        let mut url = self
            .base
            .join("v1/programs/")
            .map_err(|_| invalid("invalid server URL"))?;
        url.path_segments_mut()
            .map_err(|_| invalid("invalid server URL"))?
            .pop_if_empty()
            .push(&descriptor.program.id)
            .push(&descriptor.program.version)
            .push("artifact");
        let started = std::time::Instant::now();
        let span = exchange_span("PUT", "/v1/programs/{program_id}/{version}/artifact");
        let mut metadata = ExchangeMetadata {
            request_id: None,
            status: None,
            elapsed: Duration::ZERO,
        };
        let outcome = tokio::time::timeout(
            self.timeout,
            async {
                let request = self
                    .client
                    .put(url)
                    .header(header::CONTENT_TYPE, "application/zip")
                    .header("x-ledgence-archive-sha256", &descriptor.digest.0[7..])
                    .body(archive);
                let response = self
                    .request_trace(request, &span)
                    .send()
                    .await
                    .map_err(|_| unknown())?;
                observe_headers(&response, &mut metadata, &span);
                let status = response.status().as_u16();
                let bytes = read_response(response).await.map_err(|_| unknown())?;
                if !matches!(status, 200 | 201) {
                    let rejection: PublicationError =
                        ledgence_orchestration_api::decode_unique_json(&bytes, RESPONSE_LIMIT)
                            .map_err(|_| unknown())?;
                    if expected_status(rejection.kind) != status || rejection.message.len() > 4096 {
                        return Err(unknown());
                    }
                    // Use local messages: remote diagnostics may contain secrets.
                    return Err(PublicationError::new(
                        rejection.kind,
                        message(rejection.kind),
                    ));
                }
                let reply: PublishArtifactResult =
                    ledgence_orchestration_api::decode_unique_json(&bytes, RESPONSE_LIMIT)
                        .map_err(|_| unknown())?;
                if reply.descriptor.validate().is_err()
                    || reply.descriptor != *descriptor
                    || reply.already_published != (status == 200)
                {
                    return Err(unknown());
                }
                Ok(reply)
            }
            .instrument(span.clone()),
        )
        .await
        .unwrap_or_else(|_| Err(unknown()));
        self.observe(metadata, &span, started, &outcome);
        outcome
    }
}
fn validate_capabilities(c: &PublicationCapabilities) -> PublicationResult<()> {
    let maximum = PublicationLimits::default();
    if c.transfer_timeout_ms == 0
        || c.transfer_timeout_ms > 120_000
        || (c.enabled && (c.mode.as_deref() != Some("immutable") || c.max_concurrent_uploads == 0))
        || (!c.enabled && (c.mode.is_some() || c.max_concurrent_uploads != 0))
        || [
            (c.limits.max_archive_bytes, maximum.max_archive_bytes),
            (c.limits.max_expanded_bytes, maximum.max_expanded_bytes),
            (c.limits.max_file_bytes, maximum.max_file_bytes),
            (c.limits.max_entries, maximum.max_entries),
            (c.limits.max_manifest_bytes, maximum.max_manifest_bytes),
            (c.limits.max_descriptor_bytes, maximum.max_descriptor_bytes),
        ]
        .iter()
        .any(|(limit, maximum)| *limit == 0 || limit > maximum)
    {
        return Err(unavailable());
    }
    Ok(())
}
async fn read_response(mut response: Response) -> PublicationResult<Vec<u8>> {
    if response
        .headers()
        .get_all(header::CONTENT_TYPE)
        .iter()
        .count()
        != 1
        || response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/json")
        || response.headers().contains_key(header::CONTENT_ENCODING)
        || response
            .content_length()
            .is_some_and(|n| n > RESPONSE_LIMIT as u64)
    {
        return Err(unavailable());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| unavailable())? {
        if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
            return Err(unavailable());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
fn expected_status(kind: PublicationErrorKind) -> u16 {
    match kind {
        PublicationErrorKind::Disabled => 404,
        PublicationErrorKind::InvalidArtifact => 400,
        PublicationErrorKind::TooLarge => 413,
        PublicationErrorKind::ImmutableConflict => 409,
        PublicationErrorKind::Saturated => 429,
        PublicationErrorKind::Storage => 503,
        PublicationErrorKind::OutcomeUnknown => 504,
    }
}
fn message(kind: PublicationErrorKind) -> &'static str {
    match kind {
        PublicationErrorKind::Disabled => "program publication is not enabled on this server",
        PublicationErrorKind::InvalidArtifact => "server rejected the program artifact",
        PublicationErrorKind::TooLarge => "program archive exceeds server publication limits",
        PublicationErrorKind::ImmutableConflict => {
            "program version already identifies different immutable content"
        }
        PublicationErrorKind::Saturated => {
            "publication capacity is busy; retry exactly the same artifact later"
        }
        PublicationErrorKind::Storage => {
            "program storage is unavailable; reconcile with exactly the same artifact"
        }
        PublicationErrorKind::OutcomeUnknown => {
            "publication outcome is unknown; resend exactly the same artifact to reconcile"
        }
    }
}
fn invalid(message: &str) -> PublicationError {
    PublicationError::new(PublicationErrorKind::InvalidArtifact, message)
}
fn unavailable() -> PublicationError {
    PublicationError::new(
        PublicationErrorKind::Storage,
        "publication transport or capability response is unavailable",
    )
}
fn unknown() -> PublicationError {
    PublicationError::new(
        PublicationErrorKind::OutcomeUnknown,
        message(PublicationErrorKind::OutcomeUnknown),
    )
}

fn exchange_span(method: &str, route: &str) -> tracing::Span {
    tracing::info_span!("ledgence.http.client", otel.name = %format!("{method} {route}"), otel.kind = "client",
        http.request.method = method, http.route = route, ledgence.request.id = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty, ledgence.duration_ms = tracing::field::Empty,
        error.type = tracing::field::Empty, otel.status_code = tracing::field::Empty)
}
fn observe_headers(response: &Response, metadata: &mut ExchangeMetadata, span: &tracing::Span) {
    metadata.status = Some(response.status().as_u16());
    metadata.request_id = response
        .headers()
        .get("request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|value| {
            !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| b.is_ascii_graphic())
        })
        .map(str::to_owned);
    span.record(
        "http.response.status_code",
        u64::from(response.status().as_u16()),
    );
    if let Some(id) = &metadata.request_id {
        span.record("ledgence.request.id", id.as_str());
    }
}
