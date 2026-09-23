use crate::{ERROR_MAX_BYTES, response::ResponseValue, wire::*};
mod completion;
mod workflow;

use ledgence_orchestration_api::*;
use ledgence_worker_api::{NoopTraceBridge, TraceBridge};
use reqwest::{Client, Method, Url, header};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::Instant};
use tracing::Instrument;

/// Diagnostics for one completed exchange. Durable operation identities belong
/// to its command; this request ID changes on retries and may be absent.
#[derive(Debug, Clone)]
pub struct ExchangeMetadata {
    pub request_id: Option<String>,
    pub status: Option<u16>,
    pub elapsed: Duration,
}

type Observer = dyn Fn(&ExchangeMetadata) + Send + Sync;

struct ExchangeBudget {
    start: Instant,
    deadline: Instant,
}

/// Pooled, concurrent HTTP task service. There are no automatic retries,
/// redirects, or response caches. A timeout is an uncertain outcome.
#[derive(Clone)]
pub struct HttpTaskService {
    client: Client,
    base: Url,
    timeout: Duration,
    blocking: Arc<Semaphore>,
    observer: Option<Arc<Observer>>,
    trace_bridge: Arc<dyn TraceBridge>,
}

impl HttpTaskService {
    pub fn new(base_url: &str) -> Result<Self> {
        Self::with_timeout(base_url, Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS))
    }

    /// The complete budget includes request encoding, connection, body reads,
    /// and strict response decoding. Timed-out blocking jobs retain their permit
    /// until they finish, so repeated timeouts cannot enqueue unbounded decodes.
    pub fn with_timeout(base_url: &str, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS) {
            return Err(ContractError::InvalidInput(
                "HTTP timeout must be positive and at most 30 seconds".into(),
            ));
        }
        let mut base = Url::parse(base_url)
            .map_err(|_| ContractError::InvalidInput("invalid server URL".into()))?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(ContractError::InvalidInput(
                "server URL must be HTTP/HTTPS without credentials, query, or fragment".into(),
            ));
        }
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
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
            .map_err(|_| unavailable("HTTP client initialization failed"))?;
        Ok(Self {
            client,
            base,
            timeout,
            blocking: Arc::new(Semaphore::new(4)),
            observer: None,
            trace_bridge: Arc::new(NoopTraceBridge),
        })
    }

    /// Called once after each observed exchange, including malformed responses.
    /// Keep observers short and nonblocking; they run on the calling task.
    pub fn with_observer(
        mut self,
        observer: impl Fn(&ExchangeMetadata) + Send + Sync + 'static,
    ) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }

    /// Supply the optional trace-context bridge. Each call propagates a fresh
    /// exchange span; serialized command origins remain unchanged.
    pub fn with_trace_bridge(mut self, bridge: Arc<dyn TraceBridge>) -> Self {
        self.trace_bridge = bridge;
        self
    }

    async fn blocking<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
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

    async fn post<T: Serialize + Clone + Send + 'static, R: ResponseValue>(
        &self,
        route: &str,
        command: &T,
        limit: usize,
    ) -> Result<R> {
        self.post_until(
            route,
            command,
            limit,
            std::time::Instant::now() + self.timeout,
        )
        .await
    }

    async fn post_until<T: Serialize + Clone + Send + 'static, R: ResponseValue>(
        &self,
        route: &str,
        command: &T,
        limit: usize,
        deadline: std::time::Instant,
    ) -> Result<R> {
        self.post_until_validated(route, command, limit, deadline, |_| Ok(()))
            .await
    }

    async fn post_validated<T: Serialize + Clone + Send + 'static, R: ResponseValue>(
        &self,
        route: &str,
        command: &T,
        limit: usize,
        validate_response: impl FnOnce(&R) -> Result<()> + Send + 'static,
    ) -> Result<R> {
        self.post_until_validated(
            route,
            command,
            limit,
            std::time::Instant::now() + self.timeout,
            validate_response,
        )
        .await
    }

    async fn post_until_validated<T: Serialize + Clone + Send + 'static, R: ResponseValue>(
        &self,
        route: &str,
        command: &T,
        limit: usize,
        deadline: std::time::Instant,
        validate_response: impl FnOnce(&R) -> Result<()> + Send + 'static,
    ) -> Result<R> {
        let start = Instant::now();
        let deadline = Instant::from_std(deadline).min(start + self.timeout);
        let command = command.clone();
        self.exchange(
            Method::POST,
            route,
            &[],
            ExchangeBudget { start, deadline },
            async {
                self.blocking(move || {
                    let bytes = serde_json::to_vec(&command).map_err(|_| {
                        ContractError::InvalidInput("command cannot be encoded as JSON".into())
                    })?;
                    if bytes.len() > limit {
                        return Err(ContractError::InvalidInput(
                            "command exceeds HTTP body limit".into(),
                        ));
                    }
                    Ok(Some(bytes))
                })
                .await
            },
            validate_response,
        )
        .await
    }

    async fn get<R: ResponseValue>(&self, route: &str, query: &[(&str, String)]) -> Result<R> {
        self.get_validated(route, query, |_| Ok(())).await
    }

    async fn get_validated<R: ResponseValue>(
        &self,
        route: &str,
        query: &[(&str, String)],
        validate_response: impl FnOnce(&R) -> Result<()> + Send + 'static,
    ) -> Result<R> {
        let start = Instant::now();
        self.exchange(
            Method::GET,
            route,
            query,
            ExchangeBudget {
                start,
                deadline: start + self.timeout,
            },
            async { Ok(None) },
            validate_response,
        )
        .await
    }

    async fn exchange<R: ResponseValue>(
        &self,
        method: Method,
        route: &str,
        query: &[(&str, String)],
        budget: ExchangeBudget,
        body: impl std::future::Future<Output = Result<Option<Vec<u8>>>>,
        validate_response: impl FnOnce(&R) -> Result<()> + Send + 'static,
    ) -> Result<R> {
        let ExchangeBudget { start, deadline } = budget;
        let span = tracing::info_span!(
            "ledgence.http.client",
            otel.name = %format!("{} {}", method, route),
            otel.kind = "client",
            http.request.method = method.as_str(),
            url.path = route,
            server.address = self.base.host_str().unwrap_or_default(),
            server.port = self.base.port_or_known_default().map(i64::from),
            ledgence.tenant.id = tracing::field::Empty,
            ledgence.namespace = tracing::field::Empty,
            ledgence.worker.session.id = tracing::field::Empty,
            ledgence.consumer.id = tracing::field::Empty,
            ledgence.task.id = tracing::field::Empty,
            ledgence.dispatch.generation = tracing::field::Empty,
            http.response.status_code = tracing::field::Empty,
            ledgence.request.id = tracing::field::Empty,
            ledgence.duration_ms = tracing::field::Empty,
            error.type = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let exchange_trace = self.trace_bridge.context(&span);
        async {
            let mut metadata = ExchangeMetadata {
                request_id: None,
                status: None,
                elapsed: Duration::ZERO,
            };
            let result = tokio::time::timeout_at(deadline, async {
                if Instant::now() >= deadline {
                    return Err(unavailable(
                        "HTTP exchange deadline exceeded before sending the request",
                    ));
                }
                let mut url = self
                    .base
                    .join(route)
                    .map_err(|_| unavailable("HTTP route could not be constructed"))?;
                if !query.is_empty() {
                    url.query_pairs_mut()
                        .extend_pairs(query.iter().map(|(key, value)| (*key, value.as_str())));
                }
                let body = body.await?;
                let mut request = self
                    .client
                    .request(method, url)
                    .header(header::ACCEPT, "application/json");
                if let Some(context) = &exchange_trace {
                    request = request.header("traceparent", &context.traceparent);
                    if let Some(state) = &context.tracestate {
                        request = request.header("tracestate", state);
                    }
                }
                if let Some(body) = body {
                    request = request
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(body);
                }
                let mut response = request.send().await.map_err(|_| {
                    unavailable("HTTP exchange failed; the operation may have committed")
                })?;
                let status = response.status().as_u16();
                metadata.status = Some(status);
                metadata.request_id = response
                    .headers()
                    .get("request-id")
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| {
                        value.len() <= 256 && value.bytes().all(|byte| byte.is_ascii_graphic())
                    })
                    .map(str::to_owned);
                if response
                    .headers()
                    .get_all(header::CONTENT_TYPE)
                    .iter()
                    .count()
                    != 1
                    || !response
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
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
                        .is_some_and(|value| value != "identity")
                {
                    return Err(unavailable(
                        "HTTP response has unsupported content type or encoding",
                    ));
                }
                let limit = if status == 200 {
                    R::MAX_BYTES
                } else {
                    ERROR_MAX_BYTES
                };
                if response
                    .content_length()
                    .is_some_and(|length| length > limit as u64)
                {
                    return Err(unavailable("HTTP response exceeds its body limit"));
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await.map_err(|_| {
                    unavailable("HTTP response body was interrupted; outcome is uncertain")
                })? {
                    if chunk.len() > limit.saturating_sub(bytes.len()) {
                        return Err(unavailable("HTTP response exceeds its body limit"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                self.blocking(move || {
                    if status == 200 {
                        let value: R = decode_unique_json(&bytes, R::MAX_BYTES)
                            .map_err(|_| unavailable("HTTP success response is malformed"))?;
                        value.validate_values().map_err(|_| {
                            unavailable("HTTP success response violates application value limits")
                        })?;
                        validate_response(&value)?;
                        Ok(value)
                    } else {
                        let error: ContractError = decode_unique_json(&bytes, ERROR_MAX_BYTES)
                            .map_err(|_| {
                                unavailable("HTTP error response is not a known domain rejection")
                            })?;
                        let matches = status == error_status(&error)
                            || (matches!(error, ContractError::InvalidInput(_))
                                && matches!(status, 413 | 415));
                        if matches {
                            Err(error)
                        } else {
                            Err(unavailable(
                                "HTTP error status disagrees with its domain code",
                            ))
                        }
                    }
                })
                .await
            })
            .await
            .unwrap_or_else(|_| {
                Err(unavailable(
                    "HTTP exchange deadline exceeded; the operation may have committed",
                ))
            });
            metadata.elapsed = start.elapsed();
            // timeout cannot interrupt a synchronous poll that returns after its deadline.
            let result = if Instant::now() >= deadline {
                Err(unavailable(
                    "HTTP exchange deadline exceeded; the operation may have committed",
                ))
            } else {
                result
            };
            let span = tracing::Span::current();
            if let Some(status) = metadata.status {
                span.record("http.response.status_code", i64::from(status));
            }
            if let Some(request_id) = &metadata.request_id {
                span.record("ledgence.request.id", request_id);
            }
            span.record(
                "ledgence.duration_ms",
                i64::try_from(metadata.elapsed.as_millis()).unwrap_or(i64::MAX),
            );
            if let Err(error) = &result {
                span.record("error.type", error_code(error));
                span.record("otel.status_code", "ERROR");
            }
            tracing::info!(
                route,
                status = metadata.status,
                request_id = metadata.request_id.as_deref(),
                elapsed_ms = metadata.elapsed.as_millis() as u64,
                error_code = result.as_ref().err().map(error_code),
                "orchestration HTTP exchange"
            );
            if let Some(observer) = &self.observer {
                observer(&metadata);
            }
            result
        }
        .instrument(span)
        .await
    }
}

fn check_identity(reply: &TaskStatus, scope: &Scope, task_id: &str) -> Result<()> {
    if &reply.scope != scope || reply.task_id != task_id {
        return Err(unavailable(
            "HTTP task response identity disagrees with its request",
        ));
    }
    Ok(())
}

fn query(scope: &Scope, task_id: &str) -> Vec<(&'static str, String)> {
    vec![
        ("tenant_id", scope.tenant_id.clone()),
        ("namespace", scope.namespace.clone()),
        ("task_id", task_id.to_owned()),
    ]
}

impl TaskService for HttpTaskService {
    fn claim_dispatch<'a>(&'a self, command: &'a ClaimCommand) -> ContractFuture<'a, ClaimReply> {
        Box::pin(async move {
            command.validate()?;
            let start = Instant::now();
            let request = command.clone();
            let expected = command.clone();
            self.exchange(
                Method::POST,
                "v1/dispatch/claim",
                &[],
                ExchangeBudget {
                    start,
                    deadline: start + self.timeout,
                },
                async {
                    self.blocking(move || {
                        let span = tracing::Span::current();
                        span.record("ledgence.tenant.id", &request.dispatch.scope.tenant_id);
                        span.record("ledgence.namespace", &request.dispatch.scope.namespace);
                        span.record(
                            "ledgence.worker.session.id",
                            &request.acquisition.worker_session_id,
                        );
                        span.record(
                            "ledgence.consumer.id",
                            i64::from(request.acquisition.consumer_id),
                        );
                        span.record("ledgence.task.id", &request.dispatch.task_id);
                        span.record(
                            "ledgence.dispatch.generation",
                            i64::from(request.dispatch.generation),
                        );
                        let bytes = serde_json::to_vec(&request).map_err(|_| {
                            ContractError::InvalidInput("claim cannot be encoded as JSON".into())
                        })?;
                        if bytes.len() > DISPATCH_MAX_BYTES {
                            return Err(ContractError::InvalidInput(
                                "claim exceeds HTTP body limit".into(),
                            ));
                        }
                        Ok(Some(bytes))
                    })
                    .await
                },
                move |reply: &ClaimReply| reply.validate_reply_against(&expected),
            )
            .await
        })
    }

    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            self.post(
                "v1/worker-sessions",
                &OpenSession {
                    scope: scope.clone(),
                    queue: queue.into(),
                    concurrency,
                },
                SUBMISSION_MAX_BYTES,
            )
            .await
        })
    }
    fn extend_session<'a>(
        &'a self,
        worker_session_id: &'a str,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            self.post(
                "v1/worker-sessions/extend",
                &ExtendSession {
                    worker_session_id: worker_session_id.into(),
                },
                SUBMISSION_MAX_BYTES,
            )
            .await
        })
    }
    fn submit<'a>(&'a self, command: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(self.post("v1/tasks", command, SUBMISSION_MAX_BYTES))
    }
    fn list_tasks<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a TaskListQuery,
    ) -> ContractFuture<'a, TaskPage> {
        Box::pin(async move {
            let start = Instant::now();
            query.validate(scope)?;
            let mut fields = vec![
                ("tenant_id", scope.tenant_id.clone()),
                ("namespace", scope.namespace.clone()),
                ("limit", query.limit.to_string()),
            ];
            if let Some(state) = query.filters.state {
                let value =
                    serde_json::to_value(state).expect("task state has a JSON representation");
                fields.push((
                    "state",
                    value.as_str().expect("task state is a string").to_owned(),
                ));
            }
            for (name, value) in [
                ("queue", &query.filters.queue),
                ("correlation_key", &query.filters.correlation_key),
                ("cursor", &query.cursor),
            ] {
                if let Some(value) = value {
                    fields.push((name, value.clone()));
                }
            }
            for (name, value) in [
                ("submitted_from", query.filters.submitted_from),
                ("submitted_until", query.filters.submitted_until),
            ] {
                if let Some(value) = value {
                    fields.push((name, value.to_string()));
                }
            }
            let scope = scope.clone();
            let query = query.clone();
            self.exchange(
                Method::GET,
                "v1/tasks",
                &fields,
                ExchangeBudget {
                    start,
                    deadline: start + self.timeout,
                },
                async { Ok(None) },
                move |page: &TaskPage| {
                    page.validate(&scope, &query)
                        .map_err(|_| unavailable("HTTP task page contradicts its query"))
                },
            )
            .await
        })
    }
    fn inspect<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
    ) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move { self.get("v1/tasks/inspect", &query(scope, task_id)).await })
    }
    fn status<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskStatus> {
        Box::pin(async move {
            let query = query(scope, task_id);
            let scope = scope.clone();
            let task_id = task_id.to_owned();
            self.get_validated("v1/tasks/status", &query, move |reply: &TaskStatus| {
                check_identity(reply, &scope, &task_id)
            })
            .await
        })
    }
    fn result<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskResult> {
        Box::pin(async move {
            let query = query(scope, task_id);
            let scope = scope.clone();
            let task_id = task_id.to_owned();
            self.get_validated("v1/tasks/result", &query, move |reply: &TaskResult| {
                check_identity(&reply.task, &scope, &task_id)
            })
            .await
        })
    }
    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        attempt_id: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        Box::pin(async move {
            let mut query = query(scope, task_id);
            query.push(("attempt_id", attempt_id.into()));
            self.get("v1/attempts/inspect", &query).await
        })
    }
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        after_sequence: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        Box::pin(async move {
            let mut query = query(scope, task_id);
            query.push(("after_sequence", after_sequence.to_string()));
            self.get("v1/tasks/history", &query).await
        })
    }
    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, AcquireReply> {
        Box::pin(async move {
            let deadline = options
                .deadline
                .min(std::time::Instant::now() + self.timeout);
            options.validate()?;
            let request = AcquisitionRequest::new(command, options.max_wait.as_millis() as u64);
            self.post_until("v1/acquisitions", &request, SUBMISSION_MAX_BYTES, deadline)
                .await
        })
    }
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        Box::pin(self.post("v1/renewals", command, SUBMISSION_MAX_BYTES))
    }
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        Box::pin(self.post("v1/settlements", command, SETTLEMENT_MAX_BYTES))
    }
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        Box::pin(self.post("v1/quiescence-confirmations", owner, SUBMISSION_MAX_BYTES))
    }
    fn cancel<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskState> {
        Box::pin(async move {
            self.post(
                "v1/tasks/cancel",
                &Cancel {
                    scope: scope.clone(),
                    task_id: task_id.into(),
                },
                SUBMISSION_MAX_BYTES,
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::{layer::Context, prelude::*};

    fn claim_fixture() -> ClaimReply {
        use ledgence_worker_api::{CloudEvent, Digest, ProgramDescriptor, ProgramRef};
        let scope = Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        };
        let command = ClaimCommand {
            acquisition: AcquireCommand {
                scope: scope.clone(),
                queue: "queue".into(),
                worker_session_id: "session".into(),
                consumer_id: 0,
                sequence: 1,
            },
            dispatch: DispatchRef {
                scope: scope.clone(),
                queue: "queue".into(),
                task_id: "task".into(),
                generation: 1,
            },
        };
        let owner = LeaseOwner {
            scope,
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        };
        let assignment = Assignment {
            workflow_activation_id: None,            descriptor: ProgramDescriptor { program: ProgramRef { id: "invoice".into(), version: "1".into() },
                digest: Digest(format!("sha256:{}", "a".repeat(64))), size: 123 },
            event: CloudEvent::new(serde_json::json!({"specversion":"1.0", "id":"event", "source":"urn:ledgence:orchestrator",
                "type":"com.ledgence.task.invocation.requested.v1", "datacontenttype":"application/json",
                "ldgtenantid":"tenant", "ldgnamespace":"namespace", "ldgrunid":"run", "ldgtaskid":"task",
                "ldgattemptid":"attempt", "ldgattemptno":1, "data":null})).unwrap(),
            lease: Lease { owner: owner.clone(), expires_at: 60000 },
            authority: Authority { owner, expires_at: 60000, remaining_ms: 59000, execution_remaining_ms: 299000,
                renew_sequence: 0, cancel_requested: false, dispatch_allowed: false },
            attempt_deadline: 300000,
        };
        ClaimReply {
            command,
            disposition: ClaimDisposition::Claimed {
                reply: AcquireReply::Assigned {
                    sequence: 1,
                    assignment: Box::new(assignment),
                },
            },
        }
    }

    #[tokio::test]
    async fn claim_identity_and_malformed_reply_fail_before_exchange_telemetry_and_observer() {
        use tracing::instrument::WithSubscriber;
        let expected = claim_fixture();
        for mismatch in [
            "none",
            "command_task",
            "command_scope",
            "command_queue",
            "command_session",
            "command_consumer",
            "command_sequence",
            "command_generation",
            "authority_owner",
            "authority_expiry",
            "event_task",
            "reply_sequence",
            "malformed",
            "duplicate",
        ] {
            let mut value = serde_json::to_value(&expected).unwrap();
            match mismatch {
                "command_task" => value["command"]["dispatch"]["task_id"] = "other_task".into(),
                "command_scope" => {
                    value["command"]["dispatch"]["scope"]["namespace"] = "other_namespace".into()
                }
                "command_queue" => value["command"]["dispatch"]["queue"] = "other_queue".into(),
                "command_session" => {
                    value["command"]["acquisition"]["worker_session_id"] = "other_session".into()
                }
                "command_consumer" => value["command"]["acquisition"]["consumer_id"] = 1.into(),
                "command_sequence" => value["command"]["acquisition"]["sequence"] = 2.into(),
                "command_generation" => value["command"]["dispatch"]["generation"] = 2.into(),
                "authority_owner" => {
                    value["disposition"]["reply"]["assignment"]["authority"]["owner"]["attempt_id"] =
                        "other_attempt".into()
                }
                "authority_expiry" => {
                    value["disposition"]["reply"]["assignment"]["authority"]["expires_at"] =
                        70000.into()
                }
                "event_task" => {
                    value["disposition"]["reply"]["assignment"]["event"]["ldgtaskid"] =
                        "other_task".into()
                }
                "reply_sequence" => value["disposition"]["reply"]["sequence"] = 2.into(),
                "none" | "malformed" | "duplicate" => {}
                _ => unreachable!(),
            }
            let bytes = match mismatch {
                "malformed" => b"{invalid".to_vec(),
                "duplicate" => serde_json::to_string(&value)
                    .unwrap()
                    .replacen("\"sequence\":1", "\"sequence\":1,\"sequence\":1", 1)
                    .into_bytes(),
                _ => serde_json::to_vec(&value).unwrap(),
            };
            let (url, server) = serve_json(bytes).await;
            let records = SpanRecords::default();
            let callback_records = records.clone();
            let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let callbacks = observed.clone();
            let client = HttpTaskService::new(&url)
                .unwrap()
                .with_observer(move |metadata| {
                    let records = callback_records.0.lock().unwrap();
                    let completed = records.iter().any(|(key, value)| {
                        key == "message" && value == "orchestration HTTP exchange"
                    });
                    let failed = records
                        .iter()
                        .any(|(key, value)| key == "error.type" && value == "\"unavailable\"");
                    callbacks
                        .lock()
                        .unwrap()
                        .push((metadata.clone(), completed, failed));
                });
            let result = client
                .claim_dispatch(&expected.command)
                .with_subscriber(recording_dispatch(&records))
                .await;
            server.await.unwrap();
            if mismatch == "none" {
                result
                    .unwrap()
                    .validate_reply_against(&expected.command)
                    .unwrap();
            } else {
                assert!(
                    matches!(result, Err(ContractError::Unavailable(_))),
                    "{mismatch}: {result:?}"
                );
            }
            let observed = observed.lock().unwrap();
            assert_eq!(observed.len(), 1, "{mismatch}");
            assert_eq!(observed[0].0.status, Some(200));
            assert_eq!(observed[0].0.request_id.as_deref(), Some("req_discovery"));
            assert!(
                observed[0].1,
                "completion telemetry must precede observer: {mismatch}"
            );
            assert_eq!(observed[0].2, mismatch != "none", "{mismatch}");
            let records = records.0.lock().unwrap();
            for (field, expected) in [
                ("ledgence.tenant.id", "\"tenant\""),
                ("ledgence.namespace", "\"namespace\""),
                ("ledgence.worker.session.id", "\"session\""),
                ("ledgence.consumer.id", "0"),
                ("ledgence.task.id", "\"task\""),
            ] {
                assert!(
                    records
                        .iter()
                        .any(|(key, value)| key == field && value == expected),
                    "missing {field}: {records:?}"
                );
            }
        }
    }

    #[derive(Clone, Default)]
    struct SpanRecords(Arc<std::sync::Mutex<Vec<(String, String)>>>);

    impl tracing::field::Visit for SpanRecords {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .lock()
                .unwrap()
                .push((field.name().into(), format!("{value:?}")));
        }
    }
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanRecords {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _: &tracing::span::Id,
            _: Context<'_, S>,
        ) {
            attributes.record(&mut self.clone());
        }
        fn on_record(
            &self,
            _: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _: Context<'_, S>,
        ) {
            values.record(&mut self.clone());
        }
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            event.record(&mut self.clone());
        }
    }

    fn recording_dispatch(records: &SpanRecords) -> tracing::Dispatch {
        // tracing-core's single-dispatcher callsite fast path consults the
        // current thread. A concurrent test without our scoped subscriber can
        // otherwise register a shared callsite as permanently disabled. Retain
        // an inert dispatcher so registrations account for all scoped collectors.
        // This neither installs a global subscriber nor mixes per-test records.
        static ANCHOR: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
        let _anchor = ANCHOR
            .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        tracing::Dispatch::new(tracing_subscriber::registry().with(records.clone()))
    }

    #[test]
    fn scoped_capture_survives_callsite_first_used_without_a_subscriber() {
        const CHILD: &str = "LEDGENCE_HTTP_TRACE_REGISTRATION_CHILD";
        if std::env::var_os(CHILD).is_none() {
            // The tracing callsite registry is process-wide. A child makes the
            // initial single-subscriber state deterministic despite parallel tests.
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "client::tests::scoped_capture_survives_callsite_first_used_without_a_subscriber",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated tracing fixture failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        fn emit() {
            tracing::info!("scoped capture first-use probe");
        }
        let records = SpanRecords::default();
        let dispatch = recording_dispatch(&records);
        // Another test can use the same generic HTTP event callsite while its
        // thread has no collector, even when our scoped collector already exists.
        std::thread::spawn(emit).join().unwrap();
        tracing::dispatcher::with_default(&dispatch, emit);
        assert_eq!(
            records.0.lock().unwrap().iter().filter(|(key, value)|
                key == "message" && value == "scoped capture first-use probe").count(),
            1
        );
    }

    async fn serve_json(
        bytes: impl AsRef<[u8]> + Send + 'static,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let bytes = bytes.as_ref();
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            let length = String::from_utf8_lossy(&request)
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            assert!(length <= SUBMISSION_MAX_BYTES);
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRequest-Id: req_discovery\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(bytes).await.unwrap();
        });
        (url, server)
    }

    #[tokio::test]
    async fn contradictory_task_page_records_failed_exchange_and_request_diagnostics() {
        use tracing::instrument::WithSubscriber;
        let (url, server) = serve_json(br#"{"items":[],"next_cursor":"00"}"#).await;
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = observed.clone();
        let client = HttpTaskService::new(&url)
            .unwrap()
            .with_observer(move |metadata| captured.lock().unwrap().push(metadata.clone()));
        let scope = Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        };
        let records = SpanRecords::default();
        let result = client
            .list_tasks(&scope, &TaskListQuery::default())
            .with_subscriber(recording_dispatch(&records))
            .await;
        server.await.unwrap();
        assert!(matches!(result, Err(ContractError::Unavailable(_))));
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].request_id.as_deref(), Some("req_discovery"));
        assert_eq!(observed[0].status, Some(200));
        let records = records.0.lock().unwrap();
        assert!(
            records
                .iter()
                .any(|(key, value)| key == "error_code" && value.contains("unavailable")),
            "the exchange completion event must report query validation failure: {records:?}",
        );
    }

    #[tokio::test]
    async fn task_read_identity_validation_finishes_before_exchange_diagnostics() {
        use tracing::instrument::WithSubscriber;
        let scope = Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        };
        let expected = TaskStatus {
            scope: scope.clone(),
            task_id: "task".into(),
            run_id: "run".into(),
            workflow_id: None,
            workflow_activation_id: None,
            queue: "queue".into(),
            correlation_key: None,
            state: TaskState::Queued,
            attempt_count: 0,
            current_attempt_id: None,
            latest_attempt_id: None,
            submitted_at: 1,
            available_at: 1,
            terminal_at: None,
            cancel_requested_at: None,
        };
        for route in ["status", "result"] {
            for mismatch in [None, Some("task"), Some("tenant"), Some("namespace")] {
                let mut reply = expected.clone();
                match mismatch {
                    Some("task") => reply.task_id = "other_task".into(),
                    Some("tenant") => reply.scope.tenant_id = "other_tenant".into(),
                    Some("namespace") => reply.scope.namespace = "other_namespace".into(),
                    None => {}
                    _ => unreachable!(),
                }
                // The wire response is structurally valid; only its relationship
                // to this request distinguishes a semantic rejection from success.
                reply.validate().unwrap();
                let bytes = if route == "status" {
                    serde_json::to_vec(&reply).unwrap()
                } else {
                    let reply = TaskResult {
                        task: reply,
                        outcome: None,
                    };
                    reply.validate().unwrap();
                    serde_json::to_vec(&reply).unwrap()
                };
                let (url, server) = serve_json(bytes).await;
                let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
                let captured = observed.clone();
                let client = HttpTaskService::new(&url)
                    .unwrap()
                    .with_observer(move |metadata| {
                        captured.lock().unwrap().push(metadata.clone());
                    });
                let records = SpanRecords::default();
                let result = async {
                    if route == "status" {
                        client.status(&scope, "task").await
                    } else {
                        client.result(&scope, "task").await.map(|reply| reply.task)
                    }
                }
                .with_subscriber(recording_dispatch(&records))
                .await;
                server.await.unwrap();
                if mismatch.is_some() {
                    assert!(
                        matches!(
                            result,
                            Err(ContractError::Unavailable(ref message))
                                if message == "HTTP task response identity disagrees with its request"
                        ),
                        "{route} {mismatch:?}: {result:?}"
                    );
                } else {
                    assert_eq!(result.unwrap(), expected);
                }
                let observed = observed.lock().unwrap();
                assert_eq!(observed.len(), 1, "{route} {mismatch:?}");
                assert_eq!(observed[0].request_id.as_deref(), Some("req_discovery"));
                assert_eq!(observed[0].status, Some(200));
                let records = records.0.lock().unwrap();
                let count = |name: &str, value: &str| {
                    records
                        .iter()
                        .filter(|(key, recorded)| key == name && recorded == value)
                        .count()
                };
                assert_eq!(count("message", "orchestration HTTP exchange"), 1);
                assert_eq!(count("status", "200"), 1);
                assert_eq!(count("http.response.status_code", "200"), 1);
                assert_eq!(count("request_id", "\"req_discovery\""), 1);
                assert_eq!(count("ledgence.request.id", "\"req_discovery\""), 1);
                let failures = usize::from(mismatch.is_some());
                for (field, value) in [
                    ("error_code", "\"unavailable\""),
                    ("error.type", "\"unavailable\""),
                    ("otel.status_code", "\"ERROR\""),
                ] {
                    assert_eq!(
                        count(field, value),
                        failures,
                        "{route} {mismatch:?}: {records:?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn listing_timeout_while_waiting_for_decode_preserves_observer_and_request_id() {
        let (url, server) = serve_json(br#"{"items":[],"next_cursor":null}"#).await;
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = observed.clone();
        let client = HttpTaskService::with_timeout(&url, Duration::from_millis(150))
            .unwrap()
            .with_observer(move |metadata| captured.lock().unwrap().push(metadata.clone()));
        let held = client.blocking.clone().acquire_many_owned(4).await.unwrap();
        let scope = Scope {
            tenant_id: "tenant".into(),
            namespace: "namespace".into(),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.list_tasks(&scope, &TaskListQuery::default()),
        )
        .await;
        drop(held);
        server.await.unwrap();
        assert!(matches!(
            result.unwrap(),
            Err(ContractError::Unavailable(_))
        ));
        let observed = observed.lock().unwrap();
        assert_eq!(
            observed.len(),
            1,
            "the listing deadline must finalize its exchange observer"
        );
        assert_eq!(observed[0].request_id.as_deref(), Some("req_discovery"));
        assert_eq!(observed[0].status, Some(200));
        assert!(observed[0].elapsed >= Duration::from_millis(150));
        assert_eq!(client.blocking.available_permits(), 4);
    }

    #[tokio::test]
    async fn acquisition_deadline_includes_waiting_for_json_encoding_capacity() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client =
            HttpTaskService::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let held = client.blocking.clone().acquire_many_owned(4).await.unwrap();
        let command = AcquireCommand {
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
            queue: "queue".into(),
            worker_session_id: "session".into(),
            consumer_id: 0,
            sequence: 1,
        };
        let options = AcquireOptions::new(
            Duration::from_secs(20),
            std::time::Instant::now() + Duration::from_millis(50),
        )
        .unwrap();
        let result =
            tokio::time::timeout(Duration::from_secs(1), client.acquire(&command, options))
                .await
                .expect("encoding queue must share the caller's deadline");
        assert!(matches!(result, Err(ContractError::Unavailable(_))));
        drop(held);
        assert_eq!(client.blocking.available_permits(), 4);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn timed_out_json_observation_keeps_running_job_capacity_reserved() {
        let client = HttpTaskService::new("http://127.0.0.1").unwrap();
        let permits = client.blocking.clone();
        let (release, held) = std::sync::mpsc::channel();
        let (started, entered) = tokio::sync::oneshot::channel();
        let job = tokio::spawn(async move {
            client
                .blocking(move || {
                    let _ = started.send(());
                    held.recv().unwrap();
                    Ok(())
                })
                .await
        });
        entered.await.unwrap();
        assert_eq!(permits.available_permits(), 3);
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert_eq!(permits.available_permits(), 3);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            let all = permits.acquire_many(4).await.unwrap();
            drop(all);
        })
        .await
        .unwrap();
        assert_eq!(permits.available_permits(), 4);
    }
    #[tokio::test]
    async fn workflow_post_identity_failure_is_recorded_before_exchange_observer() {
        use tracing::instrument::WithSubscriber;
        let reply = LocalResultReceipt {
            key: "wrong".into(),
            already_accepted: true,
        };
        let (url, server) = serve_json(serde_json::to_vec(&reply).unwrap()).await;
        let records = SpanRecords::default();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = observed.clone();
        let client = HttpTaskService::new(&url)
            .unwrap()
            .with_observer(move |metadata| captured.lock().unwrap().push(metadata.clone()));
        let command = LocalResultCommand {
            owner: LeaseOwner {
                scope: Scope {
                    tenant_id: "t".into(),
                    namespace: "n".into(),
                },
                task_id: "task".into(),
                attempt_id: "attempt".into(),
                lease_id: "lease".into(),
                generation: 1,
                worker_session_id: "worker".into(),
                consumer_id: 0,
            },
            record: LocalStepRecord {
                key: "expected".into(),
                callable: "app:f".into(),
                input: serde_json::Value::Null,
                output: serde_json::Value::Null,
            },
        };
        let result = client
            .record_local_result(&command)
            .with_subscriber(recording_dispatch(&records))
            .await;
        server.await.unwrap();
        assert!(matches!(result, Err(ContractError::Unavailable(_))));
        assert_eq!(observed.lock().unwrap().len(), 1);
        assert!(
            records
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|(key, value)| key == "error_code" && value.contains("unavailable")),
            "workflow identity validation must finish inside the exchange"
        );
    }
}
