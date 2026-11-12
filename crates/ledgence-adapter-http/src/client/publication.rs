//! Binary artifact publication uses an independent transfer budget. It never
//! changes the thirty-second deadline of task/catalog control operations.
use super::{ExchangeMetadata, HttpCredentials, Observer};
use crate::program_package_profile;
use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
use ledgence_worker_api::{
    ProgramDescriptor, ProgramPackageInspector, ProgramPackageProfile, PublicationCapabilities,
    PublicationError, PublicationErrorKind, PublicationLimits, PublicationResult,
    PublishArtifactResult,
};
use reqwest::{Client, Response, Url, header};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tracing::Instrument;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const RESPONSE_LIMIT: usize = 16 * 1024;
const MAX_CONCURRENT_INSPECTIONS: usize = 2;

struct NegotiatedPublication {
    capabilities: PublicationCapabilities,
    profile: ProgramPackageProfile,
}
#[derive(Clone)]
pub struct HttpProgramPublisher {
    client: Client,
    base: Url,
    authorization: Option<header::HeaderValue>,
    timeout: Duration,
    observer: Option<Arc<Observer>>,
    trace_bridge: Arc<dyn TraceBridge>,
    inspector: Arc<dyn ProgramPackageInspector>,
    inspection_slots: Arc<Semaphore>,
}
impl HttpProgramPublisher {
    /// Package inspection is mandatory and borrows the exact bytes later sent.
    /// Clones share at most two active blocking inspections; caller-owned input
    /// vectors waiting for admission are outside that active-work bound.
    pub fn new(
        base_url: &str,
        inspector: Arc<dyn ProgramPackageInspector>,
    ) -> PublicationResult<Self> {
        Self::with_timeout(base_url, DEFAULT_TIMEOUT, inspector)
    }
    pub fn with_timeout(
        base_url: &str,
        timeout: Duration,
        inspector: Arc<dyn ProgramPackageInspector>,
    ) -> PublicationResult<Self> {
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
            authorization: None,
            timeout,
            observer: None,
            trace_bridge: Arc::new(NoopTraceBridge),
            inspector,
            inspection_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_INSPECTIONS)),
        })
    }
    /// Credentials are scoped to the same destination as control operations.
    pub fn with_credentials(mut self, credentials: HttpCredentials) -> PublicationResult<Self> {
        self.authorization =
            Some(credentials.for_server(&self.base).map_err(|_| {
                invalid("credential destination does not match publication server")
            })?);
        Ok(self)
    }
    pub fn with_bearer_token(self, token: &str) -> PublicationResult<Self> {
        let credentials = HttpCredentials::new(self.base.as_str(), token)
            .map_err(|_| invalid("invalid publication credential or destination"))?;
        self.with_credentials(credentials)
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
        if let Some(authorization) = &self.authorization {
            request = request.header(header::AUTHORIZATION, authorization.clone());
        }
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
        self.negotiate()
            .await
            .map(|negotiated| negotiated.capabilities)
    }
    async fn negotiate(&self) -> PublicationResult<NegotiatedPublication> {
        let requested = ProgramPackageProfile::Entries8192V1;
        let started = std::time::Instant::now();
        let span = exchange_span("GET", "/v1/programs/publication-capabilities");
        let mut metadata = ExchangeMetadata {
            request_id: None,
            status: None,
            elapsed: Duration::ZERO,
        };
        let result = tokio::time::timeout(Duration::from_secs(30).min(self.timeout), async {
            let url = self.base.join("v1/programs/publication-capabilities").map_err(|_| invalid("invalid server URL"))?;
            let request = self.client.get(url).header(program_package_profile::HEADER, program_package_profile::EXPANDED);
            let response = self.request_trace(request, &span).send().await.map_err(|_| unavailable())?;
            observe_headers(&response, &mut metadata, &span);
            if matches!(response.status().as_u16(), 401 | 403) {
                return Err(access_rejection(response).await?);
            }
            if response.status().as_u16() == 404 {
                return Err(PublicationError::new(PublicationErrorKind::Disabled,
                    "server does not support program publication; use --store or upgrade the server"));
            }
            if response.status().as_u16() != 200 { return Err(unavailable()); }
            let profile = program_package_profile::selected(requested, response.headers().get_all(program_package_profile::HEADER).iter().map(|value| value.as_bytes())).map_err(|_| unavailable())?;
            let bytes = read_response(response).await?;
            let capabilities: PublicationCapabilities = ledgence_orchestration_api::decode_unique_json(&bytes, RESPONSE_LIMIT)
                .map_err(|_| unavailable())?;
            validate_capabilities(&capabilities, profile)?;
            Ok(NegotiatedPublication { capabilities, profile })
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
        let negotiated = self.negotiate().await?;
        let capabilities = negotiated.capabilities;
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
        let permit = self
            .inspection_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| unavailable())?;
        let expected = descriptor.clone();
        let inspector = self.inspector.clone();
        let limits = capabilities.limits;
        let archive = tokio::task::spawn_blocking(move || {
            // A cancelled caller cannot release capacity while inspection still
            // owns the archive. Clones share the same active inspection budget.
            let _permit = permit;
            let inspection = inspector.inspect(&expected, &archive, &limits)?;
            if inspection.entry_count == 0 || inspection.manifest.program != expected.program {
                return Err(invalid(
                    "package inspection does not match prepared descriptor",
                ));
            }
            if inspection.entry_count > limits.max_entries {
                return Err(PublicationError::new(
                    PublicationErrorKind::TooLarge,
                    "prepared archive exceeds server publication entry limit",
                ));
            }
            // Upload this exact owned vector, not reconstructed package bytes.
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
                let mut request = self
                    .client
                    .put(url)
                    .header(header::CONTENT_TYPE, "application/zip")
                    .header("x-ledgence-archive-sha256", &descriptor.digest.0[7..])
                    .body(archive);
                if let Some(value) = program_package_profile::value(negotiated.profile) {
                    request = request.header(program_package_profile::HEADER, value);
                }
                let response = self
                    .request_trace(request, &span)
                    .send()
                    .await
                    .map_err(|_| unknown())?;
                observe_headers(&response, &mut metadata, &span);
                let status = response.status().as_u16();
                if matches!(status, 401 | 403) {
                    return Err(access_rejection(response).await.map_err(|_| unknown())?);
                }
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
fn validate_capabilities(
    c: &PublicationCapabilities,
    profile: ProgramPackageProfile,
) -> PublicationResult<()> {
    let maximum = profile.limits();
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
async fn access_rejection(response: Response) -> PublicationResult<PublicationError> {
    use ledgence_orchestration_api::{ContractError, decode_unique_json};
    let status = response.status().as_u16();
    let bytes = read_response(response).await?;
    let error: ContractError =
        decode_unique_json(&bytes, RESPONSE_LIMIT).map_err(|_| unavailable())?;
    let kind = match (status, error) {
        (401, ContractError::Unauthenticated) => PublicationErrorKind::Unauthenticated,
        (403, ContractError::Forbidden) => PublicationErrorKind::Forbidden,
        _ => return Err(unavailable()),
    };
    Ok(PublicationError::new(kind, message(kind)))
}
fn expected_status(kind: PublicationErrorKind) -> u16 {
    match kind {
        PublicationErrorKind::Unauthenticated => 401,
        PublicationErrorKind::Forbidden => 403,
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
        PublicationErrorKind::Unauthenticated => "publication requires a valid credential",
        PublicationErrorKind::Forbidden => "credential does not permit program publication",
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
