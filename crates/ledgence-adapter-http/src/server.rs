//! Axum composition boundary. Application and persistence remain behind
//! [`TaskService`]; readiness and database lifecycle belong to the executable.

use crate::{RESPONSE_MAX_BYTES, wire::*};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::any,
};
use ledgence_orchestration_api::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Semaphore, time::Instant};
use tracing::Instrument;

#[derive(Clone)]
struct Server {
    service: Arc<dyn TaskService>,
    stopping: Arc<AtomicBool>,
    blocking: Arc<Semaphore>,
    request_prefix: Arc<str>,
    next_request: Arc<AtomicU64>,
}

/// Bind the portable service to the `/v1` routes. Acquisition is immediate.
/// All replies, including route/method failures, carry Request-Id and no-store.
/// Merge health routes in the composition executable before serving.
pub fn router(service: Arc<dyn TaskService>) -> Router {
    router_with_admission(service, Arc::new(AtomicBool::new(false)))
}

/// A true `stopping` flag rejects new operations before reading their body.
/// In-flight calls retain their ordinary thirty-second request deadline.
pub fn router_with_admission(service: Arc<dyn TaskService>, stopping: Arc<AtomicBool>) -> Router {
    let state = Server {
        service,
        stopping,
        blocking: Arc::new(Semaphore::new(4)),
        request_prefix: format!(
            "req_{:x}_{:x}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
        .into(),
        next_request: Arc::new(AtomicU64::new(1)),
    };
    let mut router = Router::new();
    for (path, _) in ROUTES {
        router = router.route(path, any(handle));
    }
    router.fallback(handle).with_state(state)
}

const ROUTES: &[(&str, &str)] = &[
    ("/v1/tasks", "POST"),
    ("/v1/tasks/inspect", "GET"),
    ("/v1/attempts/inspect", "GET"),
    ("/v1/tasks/history", "GET"),
    ("/v1/tasks/cancel", "POST"),
    ("/v1/worker-sessions", "POST"),
    ("/v1/worker-sessions/extend", "POST"),
    ("/v1/acquisitions", "POST"),
    ("/v1/renewals", "POST"),
    ("/v1/settlements", "POST"),
    ("/v1/quiescence-confirmations", "POST"),
];

struct Failure {
    status: u16,
    error: ContractError,
}
impl From<ContractError> for Failure {
    fn from(error: ContractError) -> Self {
        Self {
            status: error_status(&error),
            error,
        }
    }
}
impl From<ledgence_worker_api::Error> for Failure {
    fn from(_: ledgence_worker_api::Error) -> Self {
        invalid("malformed JSON command").into()
    }
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

async fn handle(State(server): State<Server>, request: Request) -> Response {
    let start = Instant::now();
    let request_id = format!(
        "{}_{:x}",
        server.request_prefix,
        server.next_request.fetch_add(1, Ordering::Relaxed)
    );
    let method = request.method().as_str().to_owned();
    let route = ROUTES
        .iter()
        .find(|(path, _)| *path == request.uri().path());
    let route_label = route.map_or("unmatched", |(path, _)| *path);
    let (status, body, code) = match route {
        None => (
            404,
            br#"{"code":"route_not_found"}"#.to_vec(),
            Some("route_not_found"),
        ),
        Some((_, expected)) if method != *expected => (
            405,
            br#"{"code":"method_not_allowed"}"#.to_vec(),
            Some("method_not_allowed"),
        ),
        Some(_) => {
            let result = if server.stopping.load(Ordering::Acquire) {
                Err(unavailable("orchestrator is shutting down").into())
            } else {
                tokio::time::timeout(Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS), dispatch(&server, request).instrument(tracing::info_span!("http_operation", request_id = %request_id, route = route_label))).await
                    .unwrap_or_else(|_| Err(unavailable("server request deadline exceeded; operation outcome is uncertain").into()))
            };
            match result {
                Ok(bytes) => (200, bytes, None),
                Err(Failure { status, mut error }) => {
                    let code = error_code(&error);
                    if let ContractError::InvalidInput(message)
                    | ContractError::Unavailable(message) = &mut error
                    {
                        *message = message.chars().take(4096).collect();
                    }
                    (
                        status,
                        serde_json::to_vec(&error).expect("error has JSON representation"),
                        Some(code),
                    )
                }
            }
        }
    };
    tracing::info!(
        request_id,
        route = route_label,
        method,
        status,
        elapsed_ms = start.elapsed().as_millis() as u64,
        error_code = code,
        "orchestration HTTP request"
    );
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::from_u16(status).expect("known HTTP status");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        "request-id",
        HeaderValue::from_str(&request_id).expect("generated ASCII request ID"),
    );
    if status == 405 {
        response.headers_mut().insert(
            header::ALLOW,
            HeaderValue::from_static(route.expect("known route").1),
        );
    }
    response
}

impl Server {
    async fn blocking<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> std::result::Result<T, Failure> + Send + 'static,
    ) -> std::result::Result<T, Failure> {
        let permit = self
            .blocking
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| unavailable("JSON executor closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            job()
        })
        .await
        .map_err(|_| unavailable("JSON executor failed"))?
    }
    async fn decode<T: DeserializeOwned + Send + 'static>(
        &self,
        bytes: Vec<u8>,
        maximum: usize,
    ) -> std::result::Result<T, Failure> {
        self.blocking(move || {
            decode_unique_json(&bytes, maximum)
                .map_err(|_| invalid("malformed JSON command").into())
        })
        .await
    }
    async fn encode<T: Serialize + Send + 'static>(
        &self,
        value: T,
    ) -> std::result::Result<Vec<u8>, Failure> {
        self.blocking(move || {
            let bytes = serde_json::to_vec(&value)
                .map_err(|_| unavailable("service response cannot be encoded"))?;
            if bytes.len() > RESPONSE_MAX_BYTES {
                return Err(unavailable("service response exceeds HTTP limit").into());
            }
            Ok(bytes)
        })
        .await
    }
}

async fn dispatch(server: &Server, request: Request) -> std::result::Result<Vec<u8>, Failure> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    if parts.method == axum::http::Method::GET {
        to_bytes(body, 0)
            .await
            .map_err(|_| invalid("GET operation does not accept a request body"))?;
        let fields = query_fields(parts.uri.query().unwrap_or(""), path)?;
        let scope = Scope {
            tenant_id: fields["tenant_id"].clone(),
            namespace: fields["namespace"].clone(),
        };
        scope.validate()?;
        let task = &fields["task_id"];
        validate_text(task, 128)?;
        tracing::info!(
            tenant_id = scope.tenant_id,
            namespace = scope.namespace,
            task_id = task,
            "HTTP task lookup"
        );
        return match path {
            "/v1/tasks/inspect" => {
                let reply = server.service.inspect(&scope, task).await?;
                log_task(&reply);
                server.encode(reply).await
            }
            "/v1/attempts/inspect" => {
                let attempt = &fields["attempt_id"];
                validate_text(attempt, 128)?;
                tracing::info!(attempt_id = attempt, "HTTP attempt lookup");
                let reply = server
                    .service
                    .inspect_attempt(&scope, task, attempt)
                    .await?;
                log_binding(&reply.event, &reply.descriptor);
                server.encode(reply).await
            }
            "/v1/tasks/history" => {
                let after = fields
                    .get("after_sequence")
                    .map(|value| {
                        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                            return Err(invalid("invalid history sequence"));
                        }
                        value
                            .parse::<u64>()
                            .map_err(|_| invalid("invalid history sequence"))
                    })
                    .transpose()?
                    .unwrap_or(0);
                tracing::info!(after_sequence = after, "HTTP history cursor");
                server
                    .encode(server.service.history(&scope, task, after).await?)
                    .await
            }
            _ => unreachable!("GET route checked"),
        };
    }
    if parts.uri.query().is_some() {
        return Err(invalid("query parameters are not accepted for this operation").into());
    }
    if parts.headers.get_all(header::CONTENT_TYPE).iter().count() != 1
        || !parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_json)
        || parts
            .headers
            .get_all(header::CONTENT_ENCODING)
            .iter()
            .count()
            > 1
        || parts
            .headers
            .get(header::CONTENT_ENCODING)
            .is_some_and(|value| value != "identity")
    {
        return Err(Failure {
            status: 415,
            error: invalid("expected uncompressed application/json with UTF-8 encoding"),
        });
    }
    let maximum = if path == "/v1/settlements" {
        SETTLEMENT_MAX_BYTES
    } else {
        SUBMISSION_MAX_BYTES
    };
    let bytes = to_bytes(body, maximum)
        .await
        .map_err(|error| {
            use std::error::Error;
            let too_large = error
                .source()
                .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
            Failure {
                status: if too_large { 413 } else { 400 },
                error: invalid(if too_large {
                    "request body exceeds HTTP limit"
                } else {
                    "request body was interrupted"
                }),
            }
        })?
        .to_vec();
    match path {
        "/v1/tasks" => {
            let command = server
                .blocking(move || {
                    SubmitCommand::decode(&bytes)
                        .map_err(|_| invalid("malformed submission command").into())
                })
                .await?;
            tracing::info!(
                tenant_id = command.input.tenant_id,
                namespace = command.input.namespace,
                queue = command.input.queue,
                program_id = command.input.program.id,
                program_version = command.input.program.version,
                "HTTP submission"
            );
            let reply = server.service.submit(&command).await?;
            log_task(&reply);
            server.encode(reply).await
        }
        "/v1/worker-sessions" => {
            let command: OpenSession = server.decode(bytes, maximum).await?;
            command.scope.validate()?;
            validate_text(&command.queue, 128)?;
            if command.concurrency == 0 {
                return Err(invalid("concurrency must be positive").into());
            }
            tracing::info!(
                tenant_id = command.scope.tenant_id,
                namespace = command.scope.namespace,
                queue = command.queue,
                concurrency = command.concurrency,
                "HTTP session registration"
            );
            let reply = server
                .service
                .open_session(&command.scope, &command.queue, command.concurrency)
                .await?;
            log_session(&reply);
            server.encode(reply).await
        }
        "/v1/worker-sessions/extend" => {
            let command: ExtendSession = server.decode(bytes, maximum).await?;
            validate_text(&command.worker_session_id, 128)?;
            tracing::info!(
                worker_session_id = command.worker_session_id,
                "HTTP session extension"
            );
            let reply = server
                .service
                .extend_session(&command.worker_session_id)
                .await?;
            log_session(&reply);
            server.encode(reply).await
        }
        "/v1/tasks/cancel" => {
            let command: Cancel = server.decode(bytes, maximum).await?;
            command.scope.validate()?;
            validate_text(&command.task_id, 128)?;
            tracing::info!(
                tenant_id = command.scope.tenant_id,
                namespace = command.scope.namespace,
                task_id = command.task_id,
                "HTTP task cancellation"
            );
            server
                .encode(
                    server
                        .service
                        .cancel(&command.scope, &command.task_id)
                        .await?,
                )
                .await
        }
        "/v1/acquisitions" => {
            let command: AcquireCommand = server.decode(bytes, maximum).await?;
            command.scope.validate()?;
            validate_text(&command.queue, 128)?;
            validate_text(&command.worker_session_id, 128)?;
            tracing::info!(
                tenant_id = command.scope.tenant_id,
                namespace = command.scope.namespace,
                queue = command.queue,
                worker_session_id = command.worker_session_id,
                consumer_id = command.consumer_id,
                sequence = command.sequence,
                "HTTP acquisition"
            );
            let reply = server.service.acquire(&command).await?;
            match &reply {
                AcquireReply::Assigned { assignment, .. } => {
                    log_binding(&assignment.event, &assignment.descriptor)
                }
                AcquireReply::OwnershipLost { assignment, .. } => tracing::info!(
                    task_id = bounded(&assignment.task_id),
                    attempt_id = bounded(&assignment.attempt_id),
                    "HTTP replay found lost assignment"
                ),
                AcquireReply::Empty { .. } => {}
            }
            server.encode(reply).await
        }
        "/v1/renewals" => {
            let command: RenewCommand = server.decode(bytes, maximum).await?;
            log_owner(&command.owner)?;
            tracing::info!(sequence = command.sequence, intent = ?command.intent, "HTTP renewal");
            server.encode(server.service.renew(&command).await?).await
        }
        "/v1/settlements" => {
            let command = server
                .blocking(move || {
                    SettleCommand::decode(&bytes)
                        .map_err(|_| invalid("malformed settlement command").into())
                })
                .await?;
            log_owner(&command.owner)?;
            tracing::info!(operation_id = command.operation_id, "HTTP settlement");
            server.encode(server.service.settle(&command).await?).await
        }
        "/v1/quiescence-confirmations" => {
            let owner: LeaseOwner = server.decode(bytes, maximum).await?;
            log_owner(&owner)?;
            server
                .encode(server.service.confirm_quiescence(&owner).await?)
                .await
        }
        _ => unreachable!("POST route checked"),
    }
}

fn log_owner(owner: &LeaseOwner) -> Result<()> {
    owner.scope.validate()?;
    for text in [
        &owner.task_id,
        &owner.attempt_id,
        &owner.lease_id,
        &owner.worker_session_id,
    ] {
        validate_text(text, 128)?;
    }
    tracing::info!(
        tenant_id = owner.scope.tenant_id,
        namespace = owner.scope.namespace,
        task_id = owner.task_id,
        attempt_id = owner.attempt_id,
        worker_session_id = owner.worker_session_id,
        consumer_id = owner.consumer_id,
        "HTTP lease operation"
    );
    Ok(())
}

fn query_fields(raw: &str, route: &str) -> Result<BTreeMap<String, String>> {
    if raw.len() > 8192 {
        return Err(invalid("query exceeds supported length"));
    }
    let mut fields = BTreeMap::new();
    for pair in raw.split('&') {
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| invalid("malformed query parameter"))?;
        let name = decode_component(name)?;
        let allowed = matches!(name.as_str(), "tenant_id" | "namespace" | "task_id")
            || (route == "/v1/attempts/inspect" && name == "attempt_id")
            || (route == "/v1/tasks/history" && name == "after_sequence");
        if !allowed || fields.contains_key(&name) {
            return Err(invalid("unknown or duplicate query parameter"));
        }
        fields.insert(name, decode_component(value)?);
    }
    for required in ["tenant_id", "namespace", "task_id"] {
        if !fields.contains_key(required) {
            return Err(invalid("missing required query parameter"));
        }
    }
    if route == "/v1/attempts/inspect" && !fields.contains_key("attempt_id") {
        return Err(invalid("missing attempt_id query parameter"));
    }
    Ok(fields)
}

fn decode_component(raw: &str) -> Result<String> {
    let mut bytes = Vec::with_capacity(raw.len());
    let mut input = raw.bytes();
    while let Some(byte) = input.next() {
        bytes.push(match byte {
            b'+' => b' ',
            b'%' => {
                let high = input.next().and_then(|byte| char::from(byte).to_digit(16));
                let low = input.next().and_then(|byte| char::from(byte).to_digit(16));
                match (high, low) {
                    (Some(high), Some(low)) => (high * 16 + low) as u8,
                    _ => return Err(invalid("malformed percent encoding")),
                }
            }
            byte => byte,
        });
    }
    String::from_utf8(bytes).map_err(|_| invalid("query parameter is not UTF-8"))
}

fn bounded(value: &str) -> String {
    value.chars().take(512).collect()
}
fn log_session(session: &WorkerSession) {
    tracing::info!(
        worker_session_id = bounded(&session.id),
        tenant_id = bounded(&session.scope.tenant_id),
        namespace = bounded(&session.scope.namespace),
        queue = bounded(&session.queue),
        concurrency = session.concurrency,
        "HTTP session result"
    );
}
fn log_task(task: &TaskSnapshot) {
    tracing::info!(
        task_id = bounded(&task.task_id),
        run_id = bounded(&task.run_id),
        attempt_id = task.current_attempt_id.as_deref().map(bounded),
        digest = bounded(&task.descriptor.digest.0),
        "HTTP task result"
    );
}
fn log_binding(
    event: &ledgence_worker_api::CloudEvent,
    descriptor: &ledgence_worker_api::ProgramDescriptor,
) {
    tracing::info!(
        task_id = bounded(event.task_id()),
        attempt_id = bounded(event.attempt_id()),
        run_id = event.value()["ldgrunid"].as_str().map(bounded),
        digest = bounded(&descriptor.digest.0),
        program_id = bounded(&descriptor.program.id),
        program_version = bounded(&descriptor.program.version),
        "HTTP invocation binding"
    );
}
