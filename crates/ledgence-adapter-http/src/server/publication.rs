//! Bounded immutable upload transport. Upload admission and deadlines are
//! independent of JSON control operations and their thirty-second budget.
use super::*;
use ledgence_worker_api::{
    Digest, ProgramArtifactPublisher, ProgramRef, PublicationCapabilities, PublicationError,
    PublicationErrorKind, PublicationLimits, PublicationResult, PublishArtifactResult,
};
use std::time::Duration;

pub const UPLOAD_TIMEOUT_MS: u64 = 120_000;
pub const MAX_CONCURRENT_UPLOADS: u32 = 2;
const ARTIFACT_ROUTE: (&str, &str) = ("/v1/programs/{program_id}/{version}/artifact", "PUT");
pub(super) const ROUTES: &[(&str, &str)] = &[
    ("/v1/programs/publication-capabilities", "GET"),
    ARTIFACT_ROUTE,
];

/// Cloneable installation service. A permit spans transfer, validation and
/// persistence. Once admitted persistence starts, the owned task survives an
/// HTTP timeout/disconnect; shutdown waits for its actual completion.
#[derive(Clone)]
pub struct PublicationService {
    writer: Arc<dyn ProgramArtifactPublisher>,
    slots: Arc<Semaphore>,
    limits: PublicationLimits,
    concurrency: u32,
    timeout: Duration,
}
impl PublicationService {
    /// The supplied writer must enforce the standard PublicationLimits.
    pub fn new(writer: Arc<dyn ProgramArtifactPublisher>) -> Self {
        Self {
            writer,
            slots: Arc::new(Semaphore::new(MAX_CONCURRENT_UPLOADS as usize)),
            limits: PublicationLimits::default(),
            concurrency: MAX_CONCURRENT_UPLOADS,
            timeout: Duration::from_millis(UPLOAD_TIMEOUT_MS),
        }
    }
    /// Lower transport admission budgets without changing writer verification
    /// limits. ZIP limits remain the standard publisher contract.
    pub fn with_transport_limits(
        mut self,
        max_archive_bytes: u64,
        concurrency: u32,
        timeout: Duration,
    ) -> PublicationResult<Self> {
        if concurrency == 0
            || concurrency > MAX_CONCURRENT_UPLOADS
            || timeout.is_zero()
            || timeout > Duration::from_millis(UPLOAD_TIMEOUT_MS)
            || max_archive_bytes == 0
            || max_archive_bytes > self.limits.max_archive_bytes
        {
            return Err(error(
                PublicationErrorKind::InvalidArtifact,
                "invalid publication limits",
            ));
        }
        self.limits.max_archive_bytes = max_archive_bytes;
        self.concurrency = concurrency;
        self.slots = Arc::new(Semaphore::new(concurrency as usize));
        self.timeout = timeout;
        Ok(self)
    }
    /// Call after stopping HTTP admission. Holds all slots once every transfer
    /// and owned persistence task has really completed, without aborting writes.
    pub async fn drain(&self) {
        let _all = self
            .slots
            .clone()
            .acquire_many_owned(self.concurrency)
            .await;
    }
    fn capabilities(&self, registration_enabled: bool) -> PublicationCapabilities {
        PublicationCapabilities {
            enabled: true,
            registration_enabled,
            mode: Some("immutable".into()),
            limits: self.limits.clone(),
            max_concurrent_uploads: self.concurrency,
            transfer_timeout_ms: self.timeout.as_millis() as u64,
        }
    }
}
pub(super) fn is_route(route: &str) -> bool {
    ROUTES.iter().any(|(path, _)| *path == route)
}
pub(super) fn match_route(path: &str) -> Option<&'static (&'static str, &'static str)> {
    let parts: Vec<_> = path.split('/').collect();
    (parts.len() == 6 && parts[1] == "v1" && parts[2] == "programs" && parts[5] == "artifact")
        .then_some(&ARTIFACT_ROUTE)
}
fn error(kind: PublicationErrorKind, message: &str) -> PublicationError {
    PublicationError::new(kind, message)
}
fn disabled() -> PublicationError {
    error(
        PublicationErrorKind::Disabled,
        "program publication is not enabled; use a configured writable server or --store",
    )
}
fn unknown() -> PublicationError {
    error(
        PublicationErrorKind::OutcomeUnknown,
        "publication outcome is unknown; resend exactly the same artifact to reconcile",
    )
}
fn invalid_artifact() -> PublicationError {
    error(
        PublicationErrorKind::InvalidArtifact,
        "invalid publication request",
    )
}
fn failure(error: PublicationError) -> (u16, Vec<u8>, Option<&'static str>) {
    let (status, code, message) = match error.kind {
        PublicationErrorKind::Disabled => (404, "publication_disabled", disabled().message),
        PublicationErrorKind::InvalidArtifact => {
            (400, "invalid_artifact", invalid_artifact().message)
        }
        PublicationErrorKind::TooLarge => (
            413,
            "artifact_too_large",
            "program archive exceeds publication limits".into(),
        ),
        PublicationErrorKind::ImmutableConflict => (
            409,
            "immutable_conflict",
            "program version already identifies different immutable content".into(),
        ),
        PublicationErrorKind::Saturated => (
            429,
            "publication_saturated",
            "publication capacity is busy; retry the same artifact later".into(),
        ),
        PublicationErrorKind::Storage => (
            503,
            "publication_storage",
            "program storage unavailable; reconcile with the same artifact".into(),
        ),
        PublicationErrorKind::OutcomeUnknown => {
            (504, "publication_outcome_unknown", unknown().message)
        }
    };
    // Never reflect adapter messages, paths or archive contents into the reply.
    (
        status,
        serde_json::to_vec(&PublicationError::new(error.kind, message)).expect("error serializes"),
        Some(code),
    )
}

pub(super) async fn dispatch(
    server: &Server,
    request: Request,
) -> (u16, Vec<u8>, Option<&'static str>) {
    if server.stopping.load(Ordering::Acquire) {
        return failure(error(
            PublicationErrorKind::Storage,
            "orchestrator is shutting down",
        ));
    }
    let registration_enabled = server
        .console
        .as_ref()
        .is_some_and(|console| console.catalog.is_some());
    let service = server
        .console
        .as_ref()
        .and_then(|console| console.publication.as_ref());
    if request.uri().path() == ROUTES[0].0 {
        if request.uri().query().is_some() {
            return failure(invalid_artifact());
        }
        let empty = tokio::time::timeout(
            Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS),
            to_bytes(request.into_body(), 0),
        )
        .await;
        if !matches!(empty, Ok(Ok(_))) {
            return failure(invalid_artifact());
        }
        let capabilities = service.map_or_else(
            || PublicationCapabilities {
                enabled: false,
                registration_enabled,
                mode: None,
                limits: PublicationLimits::default(),
                max_concurrent_uploads: 0,
                transfer_timeout_ms: UPLOAD_TIMEOUT_MS,
            },
            |service| service.capabilities(registration_enabled),
        );
        return (
            200,
            serde_json::to_vec(&capabilities).expect("capabilities serialize"),
            None,
        );
    }
    let Some(service) = service else {
        return failure(disabled());
    };
    if let Some(console) = &server.console
        && console.check_origin(request.headers()).is_err()
    {
        return failure(invalid_artifact());
    }
    let deadline = Instant::now() + service.timeout;
    match tokio::time::timeout_at(deadline, upload(service, request)).await {
        Ok(Ok(reply)) if Instant::now() < deadline => (
            if reply.already_published { 200 } else { 201 },
            serde_json::to_vec(&reply).expect("publication result serializes"),
            None,
        ),
        Ok(Err(error)) => failure(error),
        _ => failure(unknown()),
    }
}
async fn upload(
    service: &PublicationService,
    request: Request,
) -> PublicationResult<PublishArtifactResult> {
    let permit = service.slots.clone().try_acquire_owned().map_err(|_| {
        error(
            PublicationErrorKind::Saturated,
            "publication capacity is busy",
        )
    })?;
    let (parts, body) = request.into_parts();
    if parts.uri.query().is_some() {
        return Err(invalid_artifact());
    }
    let segment: Vec<_> = parts.uri.path().split('/').collect();
    // ProgramRef permits only URL-safe ASCII IDs; percent escapes are rejected
    // rather than introducing another identity normalization convention.
    let program = ProgramRef {
        id: segment[3].into(),
        version: segment[4].into(),
    };
    program.validate().map_err(|_| invalid_artifact())?;
    let headers = &parts.headers;
    if headers.get_all(header::CONTENT_TYPE).iter().count() != 1
        || headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/zip")
        || headers.contains_key(header::CONTENT_ENCODING)
    {
        return Err(invalid_artifact());
    }
    let digests = headers.get_all("x-ledgence-archive-sha256");
    if digests.iter().count() != 1 {
        return Err(invalid_artifact());
    }
    let digest = digests
        .iter()
        .next()
        .and_then(|v| v.to_str().ok())
        .ok_or_else(invalid_artifact)?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
    {
        return Err(invalid_artifact());
    }
    let expected_digest = Digest(format!("sha256:{digest}"));
    if headers.get(header::CONTENT_LENGTH).is_some_and(|value| {
        value
            .to_str()
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .is_none_or(|n| n > service.limits.max_archive_bytes)
    }) {
        return Err(error(PublicationErrorKind::TooLarge, "archive too large"));
    }
    use http_body_util::BodyExt;
    let mut body = body;
    let mut archive = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| invalid_artifact())?;
        // HTTP trailers cannot replace or add the unique digest header.
        let bytes = frame.into_data().map_err(|_| invalid_artifact())?;
        if bytes.len() > (service.limits.max_archive_bytes as usize).saturating_sub(archive.len()) {
            return Err(error(PublicationErrorKind::TooLarge, "archive too large"));
        }
        archive.extend_from_slice(&bytes);
    }
    if archive.is_empty() {
        return Err(invalid_artifact());
    }
    let writer = service.writer.clone();
    let span = tracing::Span::current();
    span.record("ledgence.program.id", program.id.as_str());
    span.record("ledgence.program.version", program.version.as_str());
    span.record("ledgence.program.digest", expected_digest.0.as_str());
    // JoinHandle drop detaches; the owned task retains admission while an
    // adapter finishes, even if the requesting socket or deadline disappears.
    tokio::spawn(
        async move {
            let _permit = permit;
            let expected = ledgence_worker_api::ProgramDescriptor {
                program: program.clone(),
                digest: expected_digest.clone(),
                size: archive.len() as u64,
            };
            let result = writer.publish(program, expected_digest, archive).await?;
            if result.descriptor != expected {
                return Err(unknown());
            }
            Ok(result)
        }
        .instrument(span),
    )
    .await
    .map_err(|_| unknown())?
}
