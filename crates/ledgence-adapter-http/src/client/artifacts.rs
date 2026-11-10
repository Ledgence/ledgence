//! File bytes use streaming requests under an absolute transfer budget. The
//! normal JSON client's thirty-second budget is not widened.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use std::time::Instant as Deadline;

#[derive(Clone)]
pub struct HttpArtifactClient {
    inner: HttpTaskService,
}
impl HttpArtifactClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self {
            inner: HttpTaskService::new(base_url)?,
        })
    }
    pub fn with_credentials(mut self, credentials: HttpCredentials) -> Result<Self> {
        self.inner = self.inner.with_credentials(credentials)?;
        Ok(self)
    }
    pub fn with_observer(
        mut self,
        observer: impl Fn(&ExchangeMetadata) + Send + Sync + 'static,
    ) -> Self {
        self.inner = self.inner.with_observer(observer);
        self
    }
    pub fn with_trace_bridge(mut self, bridge: Arc<dyn TraceBridge>) -> Self {
        self.inner = self.inner.with_trace_bridge(bridge);
        self
    }
    fn deadline(requested: Deadline, binary: bool) -> ArtifactClientResult<Deadline> {
        let now = Deadline::now();
        if now >= requested {
            return Err(invalid_response(
                "artifact deadline exceeded before sending request",
            ));
        }
        Ok(requested.min(
            now + Duration::from_millis(if binary {
                ARTIFACT_TRANSFER_TIMEOUT_MS
            } else {
                CONTROL_REQUEST_TIMEOUT_MS
            }),
        ))
    }
    fn request(
        &self,
        method: Method,
        route: &str,
        deadline: Deadline,
    ) -> ArtifactClientResult<reqwest::RequestBuilder> {
        let remaining = deadline
            .checked_duration_since(Deadline::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| invalid_response("artifact deadline exceeded"))?;
        let url = self
            .inner
            .base
            .join(route)
            .map_err(|_| invalid_response("invalid artifact route"))?;
        let mut request = self
            .inner
            .client
            .request(method, url)
            .timeout(remaining)
            .header(header::ACCEPT, "application/json");
        if let Some(auth) = &self.inner.authorization {
            request = request.header(header::AUTHORIZATION, auth.clone());
        }
        if let Some(context) = self.inner.trace_bridge.context(&tracing::Span::current()) {
            request = request.header("traceparent", context.traceparent);
            if let Some(state) = context.tracestate {
                request = request.header("tracestate", state);
            }
        }
        Ok(request)
    }
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        deadline: Deadline,
    ) -> ArtifactClientResult<reqwest::Response> {
        let start = Deadline::now();
        let response = tokio::time::timeout_at(deadline.into(), request.send())
            .await
            .map_err(|_| uncertain("artifact exchange timed out; reconcile before retrying"))?
            .map_err(|_| uncertain("artifact exchange failed; outcome may be unknown"));
        if let Some(observer) = &self.inner.observer {
            observer(&ExchangeMetadata {
                request_id: response
                    .as_ref()
                    .ok()
                    .and_then(|r| r.headers().get("request-id"))
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| v.len() <= 256 && v.bytes().all(|b| b.is_ascii_graphic()))
                    .map(str::to_owned),
                status: response.as_ref().ok().map(|r| r.status().as_u16()),
                elapsed: start.elapsed(),
            });
        }
        if Deadline::now() >= deadline {
            return Err(uncertain("artifact exchange exceeded deadline"));
        }
        response
    }
    async fn read_json<R: DeserializeOwned>(
        &self,
        mut response: reqwest::Response,
        deadline: Deadline,
    ) -> ArtifactClientResult<R> {
        let status = response.status().as_u16();
        if response
            .headers()
            .get_all(header::CONTENT_TYPE)
            .iter()
            .count()
            != 1
            || !response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(is_json)
            || response
                .headers()
                .get_all(header::CONTENT_ENCODING)
                .iter()
                .count()
                > 1
            || response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|v| v != "identity")
        {
            return Err(invalid_response(
                "invalid artifact response content type or encoding",
            ));
        }
        let max = if status == 200 {
            ARTIFACT_PAGE_MAX_BYTES
        } else {
            ERROR_MAX_BYTES
        };
        if response.content_length().is_some_and(|v| v > max as u64) {
            return Err(invalid_response("artifact response exceeds limit"));
        }
        let mut bytes = Vec::new();
        loop {
            let chunk = tokio::time::timeout_at(deadline.into(), response.chunk())
                .await
                .map_err(|_| uncertain("artifact response deadline exceeded"))?
                .map_err(|_| uncertain("artifact response interrupted"))?;
            let Some(chunk) = chunk else { break };
            if chunk.len() > max.saturating_sub(bytes.len()) {
                return Err(invalid_response("artifact response exceeds limit"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if Deadline::now() >= deadline {
            return Err(uncertain("artifact response deadline exceeded"));
        }
        if status != 200 {
            let error: ContractError =
                decode_unique_json(&bytes, ERROR_MAX_BYTES).map_err(|_| {
                    invalid_response("artifact response is not a known domain rejection")
                })?;
            if status != error_status(&error)
                && !(matches!(error, ContractError::InvalidInput(_)) && matches!(status, 413 | 415))
            {
                return Err(invalid_response(
                    "artifact rejection status disagrees with domain code",
                ));
            }
            return Err(match error {
                ContractError::Unavailable(message) => {
                    ArtifactClientError::OutcomeUnknown { message }
                }
                error => ArtifactClientError::Rejected(error),
            });
        }
        decode_unique_json(&bytes, max)
            .map_err(|_| invalid_response("invalid artifact response JSON"))
    }
    async fn json<R: DeserializeOwned>(
        &self,
        route: &str,
        body: impl Serialize,
        deadline: Deadline,
        binary: bool,
    ) -> ArtifactClientResult<R> {
        let deadline = Self::deadline(deadline, binary)?;
        let bytes = serde_json::to_vec(&body)
            .map_err(|_| ContractError::InvalidInput("invalid artifact command".into()))?;
        if bytes.len() > 16 * 1024 {
            return Err(
                ContractError::InvalidInput("artifact command exceeds limit".into()).into(),
            );
        }
        let request = self
            .request(Method::POST, route, deadline)?
            .header(header::CONTENT_TYPE, "application/json")
            .body(bytes);
        let response = self.send(request, deadline).await?;
        self.read_json(response, deadline).await
    }
}
impl HttpTaskService {
    /// Independent binary transport; this does not advertise worker availability.
    pub fn artifacts(&self) -> HttpArtifactClient {
        HttpArtifactClient {
            inner: self.clone(),
        }
    }
}
fn prefix(requester: &ArtifactRequester) -> Result<&'static str> {
    requester.scope().validate()?;
    match requester {
        ArtifactRequester::External { .. } => Ok("v1/artifacts"),
        ArtifactRequester::Worker { owner } => {
            owner.validate()?;
            Ok("v1/worker/artifacts")
        }
    }
}
fn upload_route(requester: &ArtifactRequester, id: &str, operation: &str) -> Result<String> {
    validate_text(id, 128)?;
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(ContractError::InvalidInput(
            "invalid artifact upload identifier".into(),
        ));
    }
    Ok(format!("{}/uploads/{id}/{operation}", prefix(requester)?))
}
fn encoded<T: Serialize>(value: &T) -> Result<String> {
    Ok(STANDARD.encode(
        serde_json::to_vec(value)
            .map_err(|_| ContractError::InvalidInput("invalid artifact header".into()))?,
    ))
}
impl ExecutionArtifactClient for HttpArtifactClient {
    fn prepare<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        command: &'a ArtifactPrepareCommand,
        deadline: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactPrepareReply> {
        Box::pin(async move {
            command.validate()?;
            if requester.scope() != &command.scope {
                return Err(
                    ContractError::InvalidInput("artifact command scope mismatch".into()).into(),
                );
            }
            let result: ArtifactPrepareReply = self
                .json(
                    &format!("{}/prepare", prefix(requester)?),
                    serde_json::json!({"requester":requester,"command":command}),
                    deadline,
                    false,
                )
                .await?;
            result
                .upload
                .validate()
                .map_err(|_| invalid_response("invalid artifact response contract"))?;
            if result.upload.reference.as_ref().is_some_and(|r| {
                r.digest != command.metadata.digest
                    || r.size != command.metadata.size
                    || r.media_type != command.metadata.media_type
            }) {
                return Err(invalid_response("artifact prepare reference mismatch"));
            }
            Ok(result)
        })
    }
    fn upload_status<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        upload_id: &'a str,
        deadline: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactUploadStatus> {
        Box::pin(async move {
            let result: ArtifactUploadStatus = self
                .json(
                    &upload_route(requester, upload_id, "status")?,
                    serde_json::json!({"requester":requester}),
                    deadline,
                    false,
                )
                .await?;
            result
                .validate()
                .map_err(|_| invalid_response("invalid artifact response contract"))?;
            if result.selection.upload_id != upload_id {
                return Err(invalid_response("artifact status identity mismatch"));
            }
            Ok(result)
        })
    }
    fn replace<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        command: &'a ArtifactReplaceCommand,
        deadline: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactReplacementReceipt> {
        Box::pin(async move {
            command.validate()?;
            let result: ArtifactReplacementReceipt = self
                .json(
                    &upload_route(requester, &command.upload_id, "replace")?,
                    serde_json::json!({"requester":requester,"command":command}),
                    deadline,
                    false,
                )
                .await?;
            result
                .validate()
                .map_err(|_| invalid_response("invalid artifact replacement contract"))?;
            if result.replaced.upload_id != command.upload_id
                || result.replaced.revision != command.expected_selection_revision
                || result.replacement.artifact_id != result.replaced.artifact_id
                || result.replacement.revision != result.replaced.revision + 1
                || result.replacement.upload_id == result.replaced.upload_id
            {
                return Err(invalid_response("artifact replacement receipt mismatch"));
            }
            Ok(result)
        })
    }
    fn put(
        &self,
        requester: ArtifactRequester,
        selection: ArtifactSelection,
        body: Box<dyn ArtifactReader>,
        deadline: Deadline,
    ) -> ArtifactClientFuture<'_, ArtifactUploadReply> {
        Box::pin(async move {
            selection.validate()?;
            let deadline = Self::deadline(deadline, true)?;
            let request = self
                .request(
                    Method::PUT,
                    &upload_route(&requester, &selection.upload_id, "content")?,
                    deadline,
                )?
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("ledgence-artifact-requester", encoded(&requester)?)
                .header("ledgence-artifact-selection", encoded(&selection)?)
                .body(reqwest::Body::wrap(
                    crate::artifact_transfer::ReaderBody::new(body, None, deadline),
                ));
            let response = self.send(request, deadline).await?;
            let result: ArtifactUploadReply = self.read_json(response, deadline).await?;
            result
                .upload
                .validate()
                .map_err(|_| invalid_response("invalid artifact response contract"))?;
            if result.upload.selection != selection {
                return Err(invalid_response(
                    "artifact upload receipt identity mismatch",
                ));
            }
            Ok(result)
        })
    }
    fn reconcile<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        selection: &'a ArtifactSelection,
        deadline: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactUploadReply> {
        Box::pin(async move {
            selection.validate()?;
            let result: ArtifactUploadReply = self
                .json(
                    &upload_route(requester, &selection.upload_id, "reconcile")?,
                    serde_json::json!({"requester":requester,"command":selection}),
                    deadline,
                    true,
                )
                .await?;
            result
                .upload
                .validate()
                .map_err(|_| invalid_response("invalid artifact response contract"))?;
            if result.upload.selection != *selection {
                return Err(invalid_response(
                    "artifact reconciliation receipt identity mismatch",
                ));
            }
            Ok(result)
        })
    }
    fn download(
        &self,
        requester: ArtifactRequester,
        reference: ArtifactRef,
        deadline: Deadline,
    ) -> ArtifactClientFuture<'_, ArtifactDownload> {
        Box::pin(async move {
            reference.validate()?;
            let deadline = Self::deadline(deadline, true)?;
            let bytes =
                serde_json::to_vec(&serde_json::json!({"requester":requester,"command":reference}))
                    .map_err(|_| invalid_response("artifact request encoding failed"))?;
            let request = self
                .request(
                    Method::POST,
                    &format!("{}/download", prefix(&requester)?),
                    deadline,
                )?
                .header(header::CONTENT_TYPE, "application/json")
                .body(bytes);
            let response = self.send(request, deadline).await?;
            if response.status().as_u16() != 200 {
                let _: serde_json::Value = self.read_json(response, deadline).await?;
                return Err(invalid_response("invalid artifact download response"));
            }
            if response
                .headers()
                .get_all(header::CONTENT_TYPE)
                .iter()
                .count()
                != 1
                || response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .is_none_or(|v| v != "application/octet-stream")
                || response.headers().contains_key(header::CONTENT_ENCODING)
                || response.content_length() != Some(reference.size)
            {
                return Err(invalid_response("invalid artifact download framing"));
            }
            let headers = response.headers().get_all("ledgence-artifact-manifest");
            if headers.iter().count() != 1 {
                return Err(invalid_response("missing or duplicate artifact manifest"));
            }
            let header = headers.iter().next().expect("manifest header").as_bytes();
            if header.len() > ARTIFACT_MANIFEST_MAX_BYTES * 2 {
                return Err(invalid_response("artifact manifest exceeds limit"));
            }
            let raw = STANDARD
                .decode(header)
                .map_err(|_| invalid_response("invalid artifact manifest encoding"))?;
            let manifest: ArtifactManifest = decode_unique_json(&raw, ARTIFACT_MANIFEST_MAX_BYTES)
                .map_err(|_| invalid_response("invalid artifact manifest"))?;
            manifest
                .validate()
                .map_err(|_| invalid_response("invalid artifact response contract"))?;
            if manifest.reference != reference || &manifest.scope != requester.scope() {
                return Err(invalid_response("artifact download manifest mismatch"));
            }
            Ok(ArtifactDownload {
                manifest,
                reader: Box::new(ResponseReader {
                    response,
                    pending: Bytes::new(),
                    size: 0,
                    hash: Sha256::new(),
                    reference,
                    deadline,
                    done: false,
                    failure: None,
                }),
                deadline,
            })
        })
    }
}
impl ArtifactMetadataReader for HttpArtifactClient {
    fn inspect<'a>(
        &'a self,
        query: &'a ArtifactInspectQuery,
        deadline: Deadline,
    ) -> ContractFuture<'a, ArtifactManifest> {
        Box::pin(async move {
            query.validate()?;
            let result: ArtifactManifest = self
                .json("v1/artifacts/inspect", query, deadline, false)
                .await?;
            result.validate()?;
            if result.reference.id != query.artifact_id || result.scope != query.scope {
                return Err(ContractError::from(invalid_response(
                    "artifact inspection identity mismatch",
                )));
            }
            Ok(result)
        })
    }
    fn list<'a>(
        &'a self,
        query: &'a ArtifactListQuery,
        deadline: Deadline,
    ) -> ContractFuture<'a, ArtifactPage> {
        Box::pin(async move {
            query.validate()?;
            let result: ArtifactPage = self
                .json("v1/artifacts/list", query, deadline, false)
                .await?;
            result.validate_for(query).map_err(|_| {
                ContractError::from(invalid_response("invalid artifact page contract"))
            })?;
            Ok(result)
        })
    }
}
struct ResponseReader {
    response: reqwest::Response,
    pending: Bytes,
    size: u64,
    hash: Sha256,
    reference: ArtifactRef,
    deadline: Deadline,
    done: bool,
    failure: Option<&'static str>,
}
impl ArtifactReader for ResponseReader {
    fn read_chunk(&mut self, max_bytes: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            if max_bytes == 0 || max_bytes > ARTIFACT_CHUNK_MAX_BYTES {
                return Err(ContractError::InvalidInput(
                    "invalid artifact chunk size".into(),
                ));
            }
            loop {
                if let Some(message) = self.failure {
                    return Err(unavailable(message));
                }
                if Deadline::now() >= self.deadline {
                    return Err(unavailable("artifact download deadline exceeded"));
                }
                if !self.pending.is_empty() {
                    let size = max_bytes.min(self.pending.len());
                    return Ok(self.pending.split_to(size).to_vec());
                }
                if self.done {
                    return Ok(Vec::new());
                }
                let chunk = tokio::time::timeout_at(self.deadline.into(), self.response.chunk())
                    .await
                    .map_err(|_| unavailable("artifact download deadline exceeded"))?
                    .map_err(|_| unavailable("artifact download interrupted"))?;
                match chunk {
                    Some(chunk) => {
                        self.size = self.size.saturating_add(chunk.len() as u64);
                        if self.size > self.reference.size {
                            self.failure = Some("artifact download exceeds declared size");
                            return Err(unavailable("artifact download exceeds declared size"));
                        }
                        self.hash.update(&chunk);
                        self.pending = chunk;
                    }
                    None => {
                        self.done = true;
                        if self.size != self.reference.size
                            || format!("sha256:{:x}", self.hash.clone().finalize())
                                != self.reference.digest.0
                        {
                            self.failure = Some("artifact download integrity check failed");
                            return Err(unavailable("artifact download integrity check failed"));
                        }
                        return Ok(Vec::new());
                    }
                }
            }
        })
    }
}
// The runtime accessor is separately gated by successful feature discovery.
impl ExecutionArtifactClient for HttpTaskService {
    fn prepare<'a>(
        &'a self,
        r: &'a ArtifactRequester,
        c: &'a ArtifactPrepareCommand,
        d: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactPrepareReply> {
        Box::pin(async move { self.artifacts().prepare(r, c, d).await })
    }
    fn upload_status<'a>(
        &'a self,
        r: &'a ArtifactRequester,
        id: &'a str,
        d: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactUploadStatus> {
        Box::pin(async move { self.artifacts().upload_status(r, id, d).await })
    }
    fn replace<'a>(
        &'a self,
        r: &'a ArtifactRequester,
        c: &'a ArtifactReplaceCommand,
        d: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactReplacementReceipt> {
        Box::pin(async move { self.artifacts().replace(r, c, d).await })
    }
    fn put(
        &self,
        r: ArtifactRequester,
        s: ArtifactSelection,
        b: Box<dyn ArtifactReader>,
        d: Deadline,
    ) -> ArtifactClientFuture<'_, ArtifactUploadReply> {
        Box::pin(async move { self.artifacts().put(r, s, b, d).await })
    }
    fn reconcile<'a>(
        &'a self,
        r: &'a ArtifactRequester,
        s: &'a ArtifactSelection,
        d: Deadline,
    ) -> ArtifactClientFuture<'a, ArtifactUploadReply> {
        Box::pin(async move { self.artifacts().reconcile(r, s, d).await })
    }
    fn download(
        &self,
        r: ArtifactRequester,
        a: ArtifactRef,
        d: Deadline,
    ) -> ArtifactClientFuture<'_, ArtifactDownload> {
        Box::pin(async move { self.artifacts().download(r, a, d).await })
    }
}

fn invalid_response(message: &str) -> ArtifactClientError {
    ArtifactClientError::InvalidResponse {
        message: message.into(),
    }
}
fn uncertain(message: &str) -> ArtifactClientError {
    ArtifactClientError::OutcomeUnknown {
        message: message.into(),
    }
}
