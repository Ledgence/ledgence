use crate::{ERROR_MAX_BYTES, RESPONSE_MAX_BYTES, response::ResponseValue, wire::*};
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
        let start = Instant::now();
        let deadline = Instant::from_std(deadline).min(start + self.timeout);
        let command = command.clone();
        self.exchange(Method::POST, route, &[], start, deadline, async {
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
        })
        .await
    }

    async fn get<R: ResponseValue>(&self, route: &str, query: &[(&str, String)]) -> Result<R> {
        let start = Instant::now();
        self.exchange(
            Method::GET,
            route,
            query,
            start,
            start + self.timeout,
            async { Ok(None) },
        )
        .await
    }

    async fn exchange<R: ResponseValue>(
        &self,
        method: Method,
        route: &str,
        query: &[(&str, String)],
        start: Instant,
        deadline: Instant,
        body: impl std::future::Future<Output = Result<Option<Vec<u8>>>>,
    ) -> Result<R> {
        let span = tracing::info_span!(
            "ledgence.http.client",
            otel.name = %format!("{} {}", method, route),
            otel.kind = "client",
            http.request.method = method.as_str(),
            url.path = route,
            server.address = self.base.host_str().unwrap_or_default(),
            server.port = self.base.port_or_known_default().map(i64::from),
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
                    RESPONSE_MAX_BYTES
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
                        let value: R = decode_unique_json(&bytes, RESPONSE_MAX_BYTES)
                            .map_err(|_| unavailable("HTTP success response is malformed"))?;
                        value.validate_values().map_err(|_| {
                            unavailable("HTTP success response violates application value limits")
                        })?;
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

fn query(scope: &Scope, task_id: &str) -> Vec<(&'static str, String)> {
    vec![
        ("tenant_id", scope.tenant_id.clone()),
        ("namespace", scope.namespace.clone()),
        ("task_id", task_id.to_owned()),
    ]
}

impl TaskService for HttpTaskService {
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
    fn inspect<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
    ) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move { self.get("v1/tasks/inspect", &query(scope, task_id)).await })
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
}
