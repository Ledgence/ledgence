//! Artifact transfers have a separate absolute budget and conditional audited
//! byte-release boundary. JSON remains bounded independently from file bytes.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http_body_util::BodyExt;
use serde::Deserialize;

pub(super) const ROUTES: &[Route] = &[
    Route::post("/v1/artifacts/prepare", Policy::ArtifactWrite),
    Route::post(
        "/v1/artifacts/uploads/{upload_id}/status",
        Policy::ArtifactWrite,
    ),
    Route::post(
        "/v1/artifacts/uploads/{upload_id}/replace",
        Policy::ArtifactWrite,
    ),
    Route::put(
        "/v1/artifacts/uploads/{upload_id}/content",
        Policy::ArtifactWrite,
    ),
    Route::post(
        "/v1/artifacts/uploads/{upload_id}/reconcile",
        Policy::ArtifactWrite,
    ),
    Route::post("/v1/artifacts/inspect", Policy::Metadata),
    Route::post("/v1/artifacts/list", Policy::Metadata),
    Route::sensitive_post("/v1/artifacts/download", AuditReadKind::ArtifactDownload),
    Route::post("/v1/worker/artifacts/prepare", Policy::Worker),
    Route::post(
        "/v1/worker/artifacts/uploads/{upload_id}/status",
        Policy::Worker,
    ),
    Route::post(
        "/v1/worker/artifacts/uploads/{upload_id}/replace",
        Policy::Worker,
    ),
    Route::put(
        "/v1/worker/artifacts/uploads/{upload_id}/content",
        Policy::Worker,
    ),
    Route::post(
        "/v1/worker/artifacts/uploads/{upload_id}/reconcile",
        Policy::Worker,
    ),
    Route::post("/v1/worker/artifacts/download", Policy::Worker),
    Route::post("/v1/console/artifacts/inspect", Policy::Metadata),
    Route::post("/v1/console/artifacts/list", Policy::Metadata),
    Route::sensitive_post(
        "/v1/console/artifacts/download",
        AuditReadKind::ArtifactDownload,
    ),
];
pub(super) fn available(server: &Server) -> bool {
    server.instance.is_some() && server.service.execution_artifacts().is_ok()
}
pub(super) fn is_route(path: &str) -> bool {
    path.starts_with("/v1/artifacts/")
        || path.starts_with("/v1/worker/artifacts/")
        || path.starts_with("/v1/console/artifacts/")
}
pub(super) fn match_route(path: &str) -> Option<&'static Route> {
    ROUTES.iter().find(|route| {
        let Some((prefix, suffix)) = route.path.split_once("{upload_id}") else {
            return route.path == path;
        };
        path.strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            .is_some_and(valid_id)
    })
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Routed<T> {
    requester: ArtifactRequester,
    command: T,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadQuery {
    requester: ArtifactRequester,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleInspect {
    artifact_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleList {
    target: ExecutionTarget,
    limit: u32,
    cursor: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleDownload {
    reference: ArtifactRef,
}

fn authority(
    server: &Server,
    context: &audit::RequestContext,
    requester: &ArtifactRequester,
    worker: bool,
) -> Result<ArtifactAuthority> {
    let instance = server
        .instance
        .as_ref()
        .ok_or_else(|| unavailable("artifacts require a fixed instance"))?;
    if requester.scope() != &instance.scope {
        return Err(ContractError::NotFound);
    }
    let caller = match context.principal() {
        Ok(principal) => ArtifactCaller::Verified(principal.clone()),
        Err(_) if access::control(server).is_none() => {
            ArtifactCaller::TrustedInstance(instance.clone())
        }
        Err(error) => return Err(error),
    };
    match (requester, worker) {
        (ArtifactRequester::External { .. }, false) => Ok(ArtifactAuthority::External(caller)),
        (ArtifactRequester::Worker { owner }, true) => {
            owner.validate()?;
            Ok(ArtifactAuthority::Worker {
                caller,
                lease: owner.clone(),
            })
        }
        _ => Err(ContractError::Forbidden),
    }
}
fn external(server: &Server) -> Result<ArtifactRequester> {
    Ok(ArtifactRequester::External {
        scope: server
            .instance
            .as_ref()
            .ok_or(ContractError::NotFound)?
            .scope
            .clone(),
    })
}
fn header_json<T: DeserializeOwned>(
    headers: &axum::http::HeaderMap,
    name: &str,
    max: usize,
) -> Result<T> {
    let values = headers.get_all(name);
    if values.iter().count() != 1 {
        return Err(invalid("missing or duplicate artifact transfer header"));
    }
    let bytes = values.iter().next().expect("one header").as_bytes();
    if bytes.len() > max * 2 {
        return Err(invalid("artifact header exceeds limit"));
    }
    let decoded = STANDARD
        .decode(bytes)
        .map_err(|_| invalid("invalid artifact transfer header"))?;
    decode_unique_json(&decoded, max).map_err(|_| invalid("invalid artifact transfer header"))
}
async fn json_body<T: DeserializeOwned>(request: Request) -> std::result::Result<T, Failure> {
    console::check_content_type(request.headers())?;
    let bytes = to_bytes(request.into_body(), 16 * 1024)
        .await
        .map_err(|_| invalid("artifact request exceeds JSON limit or was interrupted"))?;
    decode_unique_json(&bytes, 16 * 1024).map_err(|_| invalid("invalid artifact request").into())
}
fn response<T: Serialize>(value: T) -> std::result::Result<(Body, axum::http::HeaderMap), Failure> {
    let bytes = serde_json::to_vec(&value)
        .map_err(|_| unavailable("could not encode artifact response"))?;
    if bytes.len() > ARTIFACT_PAGE_MAX_BYTES {
        return Err(unavailable("artifact response exceeds limit").into());
    }
    Ok((Body::from(bytes), axum::http::HeaderMap::new()))
}
pub(super) async fn dispatch(
    server: &Server,
    request: Request,
    context: &audit::RequestContext,
    deadline: std::time::Instant,
) -> std::result::Result<(Body, axum::http::HeaderMap), Failure> {
    if request.uri().query().is_some() {
        return Err(invalid("artifact routes do not accept query parameters").into());
    }
    if !available(server) {
        return Err(unavailable("execution artifact storage is not configured").into());
    }
    let service = server.service.execution_artifacts()?;
    let path = request.uri().path().to_owned();
    let worker = path.starts_with("/v1/worker/");
    let console_route = path.starts_with("/v1/console/");
    if console_route && server.console.is_none() {
        return Err(ContractError::NotFound.into());
    }
    let operation = path.rsplit('/').next().expect("route");
    let upload_id = if path.contains("/uploads/") {
        path.split('/').nth_back(1)
    } else {
        None
    };
    match operation {
        "content" => {
            let requester: ArtifactRequester =
                header_json(request.headers(), "ledgence-artifact-requester", 4096)?;
            let selection: ArtifactSelection =
                header_json(request.headers(), "ledgence-artifact-selection", 1024)?;
            selection.validate()?;
            if Some(selection.upload_id.as_str()) != upload_id {
                return Err(invalid("artifact upload path disagrees with selection").into());
            }
            if request
                .headers()
                .get_all(header::CONTENT_TYPE)
                .iter()
                .count()
                != 1
                || request
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .is_none_or(|v| v != "application/octet-stream")
                || request.headers().contains_key(header::CONTENT_ENCODING)
            {
                return Err(
                    invalid("artifact upload requires unencoded application/octet-stream").into(),
                );
            }
            if let Some(length) = request.headers().get(header::CONTENT_LENGTH) {
                let length = length
                    .to_str()
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| invalid("invalid artifact content length"))?;
                if length > ARTIFACT_MAX_BYTES {
                    return Err(invalid("artifact exceeds maximum size").into());
                }
            }
            let authority = authority(server, context, &requester, worker)?;
            let reader = Incoming {
                body: request.into_body(),
                pending: Bytes::new(),
                received: 0,
                deadline,
            };
            response(
                service
                    .put(authority, selection, Box::new(reader), deadline)
                    .await?,
            )
        }
        "prepare" => {
            let body: Routed<ArtifactPrepareCommand> = json_body(request).await?;
            body.command.validate()?;
            let authority = authority(server, context, &body.requester, worker)?;
            response(service.prepare(&authority, &body.command, deadline).await?)
        }
        "status" => {
            let body: UploadQuery = json_body(request).await?;
            let authority = authority(server, context, &body.requester, worker)?;
            response(
                service
                    .upload_status(&authority, upload_id.expect("status path"), deadline)
                    .await?,
            )
        }
        "replace" => {
            let body: Routed<ArtifactReplaceCommand> = json_body(request).await?;
            body.command.validate()?;
            if Some(body.command.upload_id.as_str()) != upload_id {
                return Err(invalid("artifact replacement path disagrees with command").into());
            }
            let authority = authority(server, context, &body.requester, worker)?;
            response(service.replace(&authority, &body.command, deadline).await?)
        }
        "reconcile" => {
            let body: Routed<ArtifactSelection> = json_body(request).await?;
            body.command.validate()?;
            if Some(body.command.upload_id.as_str()) != upload_id {
                return Err(
                    invalid("artifact reconciliation path disagrees with selection").into(),
                );
            }
            let authority = authority(server, context, &body.requester, worker)?;
            response(
                service
                    .reconcile(&authority, &body.command, deadline)
                    .await?,
            )
        }
        "inspect" => {
            let query: ArtifactInspectQuery = if console_route {
                let body: ConsoleInspect = json_body(request).await?;
                ArtifactInspectQuery {
                    scope: external(server)?.scope().clone(),
                    artifact_id: body.artifact_id,
                }
            } else {
                json_body(request).await?
            };
            query.validate()?;
            let authority = authority(
                server,
                context,
                &ArtifactRequester::External {
                    scope: query.scope.clone(),
                },
                false,
            )?;
            let manifest = service.inspect(&authority, &query, deadline).await?;
            manifest
                .validate()
                .map_err(|_| unavailable("invalid artifact manifest response"))?;
            if manifest.scope != query.scope || manifest.reference.id != query.artifact_id {
                return Err(unavailable("artifact inspection identity mismatch").into());
            }
            if console_route {
                response(
                    ledgence_orchestration_api::console::ConsoleArtifactManifest::from(manifest),
                )
            } else {
                response(manifest)
            }
        }
        "list" => {
            let query: ArtifactListQuery = if console_route {
                let body: ConsoleList = json_body(request).await?;
                ArtifactListQuery {
                    scope: external(server)?.scope().clone(),
                    target: body.target,
                    limit: body.limit,
                    cursor: body.cursor,
                }
            } else {
                json_body(request).await?
            };
            query.validate()?;
            let authority = authority(
                server,
                context,
                &ArtifactRequester::External {
                    scope: query.scope.clone(),
                },
                false,
            )?;
            let page = service.list(&authority, &query, deadline).await?;
            page.validate_for(&query)
                .map_err(|_| unavailable("invalid artifact page response"))?;
            if console_route {
                response(ledgence_orchestration_api::console::ConsoleArtifactPage::from(page))
            } else {
                response(page)
            }
        }
        "download" => {
            let (requester, reference) = if console_route {
                let body: ConsoleDownload = json_body(request).await?;
                (external(server)?, body.reference)
            } else {
                let body: Routed<ArtifactRef> = json_body(request).await?;
                (body.requester, body.command)
            };
            reference.validate()?;
            let authority = authority(server, context, &requester, worker)?;
            let prepared = service
                .prepare_download(&authority, &reference, deadline)
                .await?;
            let prepared_manifest = prepared.manifest().clone();
            prepared_manifest
                .validate()
                .map_err(|_| unavailable("invalid prepared artifact manifest"))?;
            if prepared_manifest.reference != reference
                || &prepared_manifest.scope != authority.scope()
            {
                return Err(unavailable("artifact download reference mismatch").into());
            }
            if !worker {
                context.sensitive(AuditResource::Artifact {
                    root: prepared.manifest().root.clone(),
                    artifact_id: reference.id.clone(),
                })?;
            }
            audit::release_context(server, context).await?;
            let download = service
                .release_download(&authority, prepared, deadline)
                .await?;
            download
                .manifest
                .validate()
                .map_err(|_| unavailable("invalid artifact download manifest"))?;
            if download.manifest != prepared_manifest || download.deadline > deadline {
                return Err(unavailable("invalid artifact download boundary").into());
            }
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment"),
            );
            headers.insert(header::CONTENT_LENGTH, HeaderValue::from(reference.size));
            let manifest = if console_route {
                serde_json::to_vec(
                    &ledgence_orchestration_api::console::ConsoleArtifactManifest::from(
                        download.manifest,
                    ),
                )
            } else {
                serde_json::to_vec(&download.manifest)
            }
            .map_err(|_| unavailable("artifact manifest encoding failed"))?;
            headers.insert(
                "ledgence-artifact-manifest",
                HeaderValue::from_str(&STANDARD.encode(manifest))
                    .map_err(|_| unavailable("artifact manifest header failed"))?,
            );
            Ok((
                Body::new(crate::artifact_transfer::ReaderBody::new(
                    download.reader,
                    Some(reference.size),
                    download.deadline,
                )),
                headers,
            ))
        }
        _ => Err(ContractError::NotFound.into()),
    }
}
struct Incoming {
    body: Body,
    pending: Bytes,
    received: u64,
    deadline: std::time::Instant,
}
impl ArtifactReader for Incoming {
    fn read_chunk(&mut self, max_bytes: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            if max_bytes == 0 || max_bytes > ARTIFACT_CHUNK_MAX_BYTES {
                return Err(invalid("invalid artifact chunk bound"));
            }
            loop {
                if std::time::Instant::now() >= self.deadline {
                    return Err(unavailable("artifact upload deadline exceeded"));
                }
                if !self.pending.is_empty() {
                    let size = max_bytes.min(self.pending.len());
                    return Ok(self.pending.split_to(size).to_vec());
                }
                let frame = tokio::time::timeout_at(self.deadline.into(), self.body.frame())
                    .await
                    .map_err(|_| unavailable("artifact upload deadline exceeded"))?;
                let Some(frame) = frame else {
                    return Ok(Vec::new());
                };
                let frame = frame.map_err(|_| unavailable("artifact upload body interrupted"))?;
                if let Ok(bytes) = frame.into_data() {
                    self.received = self.received.saturating_add(bytes.len() as u64);
                    if self.received > ARTIFACT_MAX_BYTES {
                        return Err(invalid("artifact exceeds maximum size"));
                    }
                    self.pending = bytes;
                }
            }
        })
    }
}
