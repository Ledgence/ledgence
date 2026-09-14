//! Axum composition boundary. Application and persistence remain behind
//! [`TaskService`]; readiness and database lifecycle belong to the executable.

mod workflow;

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
use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
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
    workflows: Option<Arc<dyn WorkflowService>>,
    stopping: Arc<AtomicBool>,
    blocking: Arc<Semaphore>,
    request_prefix: Arc<str>,
    next_request: Arc<AtomicU64>,
    trace_bridge: Arc<dyn TraceBridge>,
    invalid_trace_headers: Arc<AtomicU64>,
}

/// Bind the portable service to the `/v1` routes. Acquisition waits are bounded.
/// All replies, including route/method failures, carry Request-Id and no-store.
/// Merge health routes in the composition executable before serving.
pub fn router(service: Arc<dyn TaskService>) -> Router {
    router_with_admission(service, Arc::new(AtomicBool::new(false)))
}

/// A true `stopping` flag rejects new operations before reading their body.
/// In-flight calls retain their ordinary thirty-second request deadline.
pub fn router_with_admission(service: Arc<dyn TaskService>, stopping: Arc<AtomicBool>) -> Router {
    router_with_observability(service, stopping, Arc::new(NoopTraceBridge))
}

/// Bind HTTP tracing without exposing exporter types to the application service.
/// Invalid or duplicate traceparent fields are ignored. Tracestate fields are
/// combined in order; invalid state is dropped without losing a valid parent.
pub fn router_with_observability(
    service: Arc<dyn TaskService>,
    stopping: Arc<AtomicBool>,
    trace_bridge: Arc<dyn TraceBridge>,
) -> Router {
    build_router(service, None, stopping, trace_bridge)
}

/// Add independently supplied workflow operations to the task transport.
pub fn router_with_workflows(
    service: Arc<dyn TaskService>,
    workflows: Arc<dyn WorkflowService>,
    stopping: Arc<AtomicBool>,
    trace_bridge: Arc<dyn TraceBridge>,
) -> Router {
    build_router(service, Some(workflows), stopping, trace_bridge)
}

fn build_router(
    service: Arc<dyn TaskService>,
    workflows: Option<Arc<dyn WorkflowService>>,
    stopping: Arc<AtomicBool>,
    trace_bridge: Arc<dyn TraceBridge>,
) -> Router {
    let state = Server {
        service,
        workflows,
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
        trace_bridge,
        invalid_trace_headers: Arc::new(AtomicU64::new(0)),
    };
    let mut router = Router::new();
    for (path, _) in ROUTES {
        router = router.route(path, any(handle));
    }
    router.fallback(handle).with_state(state)
}

const ROUTES: &[(&str, &str)] = &[
    ("/v1/workflows", "POST"),
    ("/v1/workflows/status", "GET"),
    ("/v1/workflows/result", "GET"),
    ("/v1/workflows/cancel", "POST"),
    ("/v1/workflows/events", "POST"),
    ("/v1/workflows/activations/context", "POST"),
    ("/v1/workflows/local-results", "POST"),
    ("/v1/tasks", "GET, POST"),
    ("/v1/tasks/inspect", "GET"),
    ("/v1/tasks/status", "GET"),
    ("/v1/tasks/result", "GET"),
    ("/v1/attempts/inspect", "GET"),
    ("/v1/tasks/history", "GET"),
    ("/v1/tasks/cancel", "POST"),
    ("/v1/worker-sessions", "POST"),
    ("/v1/worker-sessions/extend", "POST"),
    ("/v1/acquisitions", "POST"),
    ("/v1/dispatch/claim", "POST"),
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
    let deadline = start + Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS);
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
    let span = tracing::info_span!(
        parent: None,
        "ledgence.http.server",
        otel.name = %format!("{} {}", method, route_label),
        otel.kind = "server",
        http.request.method = method.as_str(),
        http.route = route_label,
        ledgence.request.id = request_id.as_str(),
        ledgence.tenant.id = tracing::field::Empty,
        ledgence.namespace = tracing::field::Empty,
        ledgence.run.id = tracing::field::Empty,
        ledgence.workflow.id = tracing::field::Empty,
        ledgence.activation.id = tracing::field::Empty,
        ledgence.task.id = tracing::field::Empty,
        ledgence.attempt.id = tracing::field::Empty,
        ledgence.attempt.number = tracing::field::Empty,
        ledgence.dispatch.generation = tracing::field::Empty,
        ledgence.worker.session.id = tracing::field::Empty,
        ledgence.consumer.id = tracing::field::Empty,
        ledgence.program.id = tracing::field::Empty,
        ledgence.program.version = tracing::field::Empty,
        ledgence.program.digest = tracing::field::Empty,
        ledgence.business.correlation_key = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        ledgence.duration_ms = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let transport_context = extract_trace(request.headers(), &server.invalid_trace_headers);
    server
        .trace_bridge
        .set_parent(&span, transport_context.as_ref());
    async {
        let (status, body, code) = match route {
            None => (
                404,
                br#"{"code":"route_not_found"}"#.to_vec(),
                Some("route_not_found"),
            ),
            Some((_, expected)) if !expected.split(", ").any(|allowed| method == allowed) => (
                405,
                br#"{"code":"method_not_allowed"}"#.to_vec(),
                Some("method_not_allowed"),
            ),
            Some(_) => {
                let result = if server.stopping.load(Ordering::Acquire) {
                    Err(unavailable("orchestrator is shutting down").into())
                } else {
                    tokio::time::timeout_at(deadline, dispatch(&server, request, deadline))
                        .await
                        .unwrap_or_else(|_| {
                            Err(unavailable(
                                "server request deadline exceeded; operation outcome is uncertain",
                            )
                            .into())
                        })
                };
                // A synchronous poll can finish after timeout_at's deadline.
                // Never acknowledge a late success as fresh authority.
                let result = if Instant::now() >= deadline {
                    Err(unavailable(
                        "server request deadline exceeded; operation outcome is uncertain",
                    )
                    .into())
                } else {
                    result
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
        let span = tracing::Span::current();
        span.record("http.response.status_code", i64::from(status));
        span.record(
            "ledgence.duration_ms",
            i64::try_from(start.elapsed().as_millis()).unwrap_or(i64::MAX),
        );
        if status >= 500 {
            span.record("otel.status_code", "ERROR");
            span.record("error.type", code.unwrap_or("server_error"));
        }
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
    .instrument(span)
    .await
}

/// One fixed W3C carrier. The HTTP context never changes the JSON command.
fn extract_trace(
    headers: &axum::http::HeaderMap,
    invalid_count: &AtomicU64,
) -> Option<TraceContext> {
    let parents = headers.get_all("traceparent");
    let states = headers.get_all("tracestate");
    if parents.iter().count() == 0 && states.iter().count() == 0 {
        return None;
    }
    let parent = (|| {
        if parents.iter().count() != 1 {
            return None;
        }
        let traceparent = parents.iter().next()?.to_str().ok()?;
        // Version 00 is the supported carrier shape. Bound before allocating.
        if traceparent.len() != 55 {
            return None;
        }
        let context = TraceContext {
            traceparent: traceparent.to_owned(),
            tracestate: None,
        };
        context.validate().ok()?;
        Some(context)
    })();
    let Some(mut context) = parent else {
        invalid_trace_diagnostic(invalid_count);
        return None;
    };
    if states.iter().count() != 0 {
        let state = (|| {
            // W3C permits multiple tracestate fields and requires ordered joining.
            let mut joined = String::new();
            for (index, value) in states.iter().enumerate() {
                let value = value.to_str().ok()?;
                let separator = usize::from(index != 0);
                if value.len().saturating_add(separator) > 512_usize.saturating_sub(joined.len()) {
                    return None;
                }
                if separator != 0 {
                    joined.push(',');
                }
                joined.push_str(value);
            }
            // HTTP allows horizontal tabs as optional list-member whitespace;
            // the portable event carrier uses the equivalent normalized form.
            Some(
                joined
                    .split(',')
                    .map(|member| member.trim_matches([' ', '\t']))
                    .collect::<Vec<_>>()
                    .join(","),
            )
        })();
        context.tracestate = state;
        if context.tracestate.is_none() || context.validate().is_err() {
            // W3C §3.3: invalid state must not invalidate the valid parent.
            context.tracestate = None;
            invalid_trace_diagnostic(invalid_count);
        }
    }
    Some(context)
}

fn invalid_trace_diagnostic(invalid_count: &AtomicU64) {
    let previous = invalid_count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
        count.checked_add(1)
    });
    if let Ok(previous) = previous {
        let count = previous + 1;
        // At most 64 diagnostics per server instance, without reflecting values.
        if count.is_power_of_two() {
            tracing::warn!(
                invalid_trace_headers = count,
                "ignored invalid HTTP trace context"
            );
        }
    }
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
        let span = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        tokio::task::spawn_blocking(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                span.in_scope(|| {
                    let _permit = permit;
                    job()
                })
            })
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
        self.blocking(move || encode_bounded(&value, RESPONSE_MAX_BYTES))
            .await
    }
}

fn encode_bounded(value: &impl Serialize, limit: usize) -> std::result::Result<Vec<u8>, Failure> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| unavailable("service response cannot be encoded"))?;
    if bytes.len() > limit {
        return Err(unavailable("service response exceeds HTTP limit").into());
    }
    Ok(bytes)
}

fn check_task_identity(reply: &TaskStatus, scope: &Scope, task_id: &str) -> Result<()> {
    if &reply.scope != scope || reply.task_id != task_id {
        return Err(unavailable(
            "service task response identity disagrees with its request",
        ));
    }
    tracing::Span::current().record("ledgence.run.id", bounded(&reply.run_id));
    Ok(())
}

async fn dispatch(
    server: &Server,
    request: Request,
    deadline: Instant,
) -> std::result::Result<Vec<u8>, Failure> {
    if Instant::now() >= deadline {
        return Err(unavailable("server request deadline exceeded before dispatch").into());
    }
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
        if path == "/v1/tasks" {
            record_scope(&scope);
            let query = list_query(&fields)?;
            query.validate(&scope)?;
            let reply = server.service.list_tasks(&scope, &query).await?;
            return server
                .blocking(move || {
                    reply
                        .validate(&scope, &query)
                        .map_err(|_| unavailable("invalid service task page"))?;
                    encode_bounded(&reply, TASK_PAGE_MAX_BYTES)
                })
                .await;
        }
        if path.starts_with("/v1/workflows/") {
            return workflow::get(server, path, scope, fields["workflow_id"].clone()).await;
        }
        let task = &fields["task_id"];
        validate_text(task, 128)?;
        record_scope(&scope);
        tracing::Span::current().record("ledgence.task.id", task);
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
            "/v1/tasks/status" => {
                let reply = server.service.status(&scope, task).await?;
                check_task_identity(&reply, &scope, task)?;
                server
                    .blocking(move || {
                        reply
                            .validate()
                            .map_err(|_| unavailable("invalid service task status"))?;
                        encode_bounded(&reply, crate::STATUS_MAX_BYTES)
                    })
                    .await
            }
            "/v1/tasks/result" => {
                let reply = server.service.result(&scope, task).await?;
                check_task_identity(&reply.task, &scope, task)?;
                server
                    .blocking(move || {
                        reply
                            .validate()
                            .map_err(|_| unavailable("invalid service task result"))?;
                        encode_bounded(&reply, RESPONSE_MAX_BYTES)
                    })
                    .await
            }
            "/v1/attempts/inspect" => {
                let attempt = &fields["attempt_id"];
                validate_text(attempt, 128)?;
                tracing::Span::current().record("ledgence.attempt.id", attempt);
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
    let maximum = match path {
        "/v1/settlements" => SETTLEMENT_MAX_BYTES,
        "/v1/dispatch/claim" => DISPATCH_MAX_BYTES,
        "/v1/workflows/events" => WORKFLOW_EVENT_COMMAND_MAX_BYTES,
        _ => SUBMISSION_MAX_BYTES,
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
    if path.starts_with("/v1/workflows") {
        return workflow::post(server, path, bytes, maximum).await;
    }
    match path {
        "/v1/tasks" => {
            let command = server
                .blocking(move || {
                    SubmitCommand::decode(&bytes)
                        .map_err(|_| invalid("malformed submission command").into())
                })
                .await?;
            let span = tracing::Span::current();
            span.record("ledgence.tenant.id", &command.input.tenant_id);
            span.record("ledgence.namespace", &command.input.namespace);
            span.record("ledgence.program.id", &command.input.program.id);
            span.record("ledgence.program.version", &command.input.program.version);
            if let Some(key) = &command.input.correlation_key {
                span.record("ledgence.business.correlation_key", key);
            }
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
            record_scope(&command.scope);
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
            tracing::Span::current()
                .record("ledgence.worker.session.id", &command.worker_session_id);
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
            tracing::Span::current().record("ledgence.task.id", &command.task_id);
            record_scope(&command.scope);
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
        "/v1/dispatch/claim" => {
            let command = server
                .blocking(move || {
                    ClaimCommand::decode(&bytes)
                        .map_err(|_| invalid("malformed dispatch claim command").into())
                })
                .await?;
            record_scope(&command.dispatch.scope);
            let span = tracing::Span::current();
            span.record(
                "ledgence.worker.session.id",
                &command.acquisition.worker_session_id,
            );
            span.record(
                "ledgence.consumer.id",
                i64::from(command.acquisition.consumer_id),
            );
            span.record("ledgence.task.id", &command.dispatch.task_id);
            span.record(
                "ledgence.dispatch.generation",
                i64::from(command.dispatch.generation),
            );
            tracing::info!(
                tenant_id = command.dispatch.scope.tenant_id,
                namespace = command.dispatch.scope.namespace,
                queue = command.dispatch.queue,
                worker_session_id = command.acquisition.worker_session_id,
                consumer_id = command.acquisition.consumer_id,
                sequence = command.acquisition.sequence,
                task_id = command.dispatch.task_id,
                generation = command.dispatch.generation,
                "HTTP dispatch claim"
            );
            let reply = server.service.claim_dispatch(&command).await?;
            // A custom service's successful return is not enough: only a
            // response bound to this exact claim can establish durable handoff.
            reply.validate_reply_against(&command)?;
            if let ClaimDisposition::Claimed {
                reply: AcquireReply::Assigned { assignment, .. },
            } = &reply.disposition
            {
                log_binding(&assignment.event, &assignment.descriptor);
            }
            server
                .blocking(move || encode_bounded(&reply, CLAIM_REPLY_MAX_BYTES))
                .await
        }
        "/v1/acquisitions" => {
            let request: AcquisitionRequest = server.decode(bytes, maximum).await?;
            // Preserve the preference separately from identity and carry the
            // original budget through body transfer, decode, and service waiting.
            // The service reserves finalization time inside this fixed deadline.
            let options =
                AcquireOptions::new(Duration::from_millis(request.wait_ms), deadline.into_std())?;
            let command = request.into_command();
            command.scope.validate()?;
            validate_text(&command.queue, 128)?;
            validate_text(&command.worker_session_id, 128)?;
            tracing::Span::current()
                .record("ledgence.worker.session.id", &command.worker_session_id);
            tracing::Span::current().record("ledgence.consumer.id", i64::from(command.consumer_id));
            record_scope(&command.scope);
            tracing::info!(
                tenant_id = command.scope.tenant_id,
                namespace = command.scope.namespace,
                queue = command.queue,
                worker_session_id = command.worker_session_id,
                consumer_id = command.consumer_id,
                sequence = command.sequence,
                "HTTP acquisition"
            );
            let reply = server.service.acquire(&command, options).await?;
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
    record_scope(&owner.scope);
    let span = tracing::Span::current();
    span.record("ledgence.task.id", &owner.task_id);
    span.record("ledgence.attempt.id", &owner.attempt_id);
    span.record("ledgence.worker.session.id", &owner.worker_session_id);
    span.record("ledgence.consumer.id", i64::from(owner.consumer_id));
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
    if raw.len()
        > if route == "/v1/tasks" {
            16 * 1024
        } else {
            8192
        }
    {
        return Err(invalid("query exceeds supported length"));
    }
    let identity = if route.starts_with("/v1/workflows/") {
        "workflow_id"
    } else {
        "task_id"
    };
    let mut fields = BTreeMap::new();
    for pair in raw.split('&') {
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| invalid("malformed query parameter"))?;
        let name = decode_component(name)?;
        let allowed = matches!(name.as_str(), "tenant_id" | "namespace")
            || (route != "/v1/tasks" && name == identity)
            || (route == "/v1/tasks"
                && matches!(
                    name.as_str(),
                    "state"
                        | "queue"
                        | "correlation_key"
                        | "submitted_from"
                        | "submitted_until"
                        | "limit"
                        | "cursor"
                ))
            || (route == "/v1/attempts/inspect" && name == "attempt_id")
            || (route == "/v1/tasks/history" && name == "after_sequence");
        if !allowed || fields.contains_key(&name) {
            return Err(invalid("unknown or duplicate query parameter"));
        }
        fields.insert(name, decode_component(value)?);
    }
    for required in ["tenant_id", "namespace", identity] {
        if (required != identity || route != "/v1/tasks") && !fields.contains_key(required) {
            return Err(invalid("missing required query parameter"));
        }
    }
    if route == "/v1/attempts/inspect" && !fields.contains_key("attempt_id") {
        return Err(invalid("missing attempt_id query parameter"));
    }
    Ok(fields)
}

fn list_query(fields: &BTreeMap<String, String>) -> Result<TaskListQuery> {
    let number = |key: &str| -> Result<Option<u64>> {
        fields
            .get(key)
            .map(|value| {
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(invalid("list numbers must be unsigned integers"));
                }
                value
                    .parse()
                    .map_err(|_| invalid("list number exceeds supported range"))
            })
            .transpose()
    };
    Ok(TaskListQuery {
        filters: TaskFilters {
            state: fields
                .get("state")
                .map(|value| {
                    serde_json::from_value(serde_json::Value::String(value.clone()))
                        .map_err(|_| invalid("invalid task state"))
                })
                .transpose()?,
            queue: fields.get("queue").cloned(),
            correlation_key: fields.get("correlation_key").cloned(),
            submitted_from: number("submitted_from")?,
            submitted_until: number("submitted_until")?,
        },
        limit: number("limit")?
            .map(u32::try_from)
            .transpose()
            .map_err(|_| invalid("invalid list limit"))?
            .unwrap_or(TASK_LIST_DEFAULT_LIMIT),
        cursor: fields.get("cursor").cloned(),
    })
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
fn record_scope(scope: &Scope) {
    let span = tracing::Span::current();
    span.record("ledgence.tenant.id", bounded(&scope.tenant_id));
    span.record("ledgence.namespace", bounded(&scope.namespace));
}
fn log_session(session: &WorkerSession) {
    record_scope(&session.scope);
    tracing::Span::current().record("ledgence.worker.session.id", bounded(&session.id));
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
    let span = tracing::Span::current();
    span.record("ledgence.task.id", bounded(&task.task_id));
    span.record("ledgence.run.id", bounded(&task.run_id));
    span.record(
        "ledgence.program.digest",
        bounded(&task.descriptor.digest.0),
    );
    span.record("ledgence.program.id", bounded(&task.descriptor.program.id));
    span.record(
        "ledgence.program.version",
        bounded(&task.descriptor.program.version),
    );
    if let Some(attempt) = &task.current_attempt_id {
        span.record("ledgence.attempt.id", bounded(attempt));
    }
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
    let span = tracing::Span::current();
    span.record("ledgence.task.id", bounded(event.task_id()));
    span.record("ledgence.attempt.id", bounded(event.attempt_id()));
    for (field, key) in [
        ("ledgence.run.id", "ldgrunid"),
        ("ledgence.workflow.id", "ldgworkflowid"),
        ("ledgence.activation.id", "ldgactivationid"),
        ("ledgence.tenant.id", "ldgtenantid"),
        ("ledgence.namespace", "ldgnamespace"),
    ] {
        if let Some(value) = event.value()[key].as_str() {
            span.record(field, bounded(value));
        }
    }
    if let Some(number) = event.value()["ldgattemptno"].as_u64() {
        span.record(
            "ledgence.attempt.number",
            i64::try_from(number).unwrap_or(i64::MAX),
        );
    }
    span.record("ledgence.program.digest", bounded(&descriptor.digest.0));
    span.record("ledgence.program.id", bounded(&descriptor.program.id));
    span.record(
        "ledgence.program.version",
        bounded(&descriptor.program.version),
    );
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

#[cfg(test)]
mod trace_tests {
    use super::*;

    #[test]
    fn transport_carrier_accepts_unsampled_context_and_ignores_bad_or_duplicate_headers() {
        let invalid = AtomicU64::new(0);
        let mut headers = axum::http::HeaderMap::new();
        assert_eq!(extract_trace(&headers, &invalid), None);
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-1111111111111111-00"),
        );
        headers.insert("tracestate", HeaderValue::from_static("vendor=sampled0"));
        let accepted = extract_trace(&headers, &invalid).unwrap();
        assert!(accepted.traceparent.ends_with("-00"));
        assert_eq!(accepted.tracestate.as_deref(), Some("vendor=sampled0"));
        assert_eq!(invalid.load(Ordering::Relaxed), 0);

        headers.append(
            "traceparent",
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-2222222222222222-01"),
        );
        assert_eq!(extract_trace(&headers, &invalid), None);
        headers.insert("traceparent", HeaderValue::from_static("not-a-context"));
        assert_eq!(extract_trace(&headers, &invalid), None);
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-1111111111111111-00"),
        );
        headers.append("tracestate", HeaderValue::from_static("another=value"));
        let combined = extract_trace(&headers, &invalid).unwrap();
        assert_eq!(
            combined.tracestate.as_deref(),
            Some("vendor=sampled0,another=value")
        );
        headers.insert(
            "tracestate",
            HeaderValue::from_static("vendor=one,vendor=two"),
        );
        let parent_only = extract_trace(&headers, &invalid).unwrap();
        assert_eq!(parent_only.traceparent, accepted.traceparent);
        assert_eq!(parent_only.tracestate, None);
        headers.remove("traceparent");
        assert_eq!(extract_trace(&headers, &invalid), None);
        assert_eq!(invalid.load(Ordering::Relaxed), 4);
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-1111111111111111-00"),
        );
        headers.insert(
            "tracestate",
            HeaderValue::from_static("\t vendor=one \t, another=two\t"),
        );
        assert_eq!(
            extract_trace(&headers, &invalid)
                .unwrap()
                .tracestate
                .as_deref(),
            Some("vendor=one,another=two")
        );
        assert_eq!(invalid.load(Ordering::Relaxed), 4);
    }
}
