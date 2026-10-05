//! Scope-bound tool dispatch over portable service ports, without a framework SDK.
use crate::schemas::tool_definitions;
use ledgence_orchestration_api::{console::*, *};
use ledgence_worker_api::ProgramRef;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{fmt, sync::Arc};

pub const TOOL_OUTPUT_MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
pub struct ToolError {
    pub code: &'static str,
    pub message: String,
    /// A failed response must not be mistaken for proof that a mutation failed.
    pub outcome_unknown: bool,
}
impl fmt::Display for ToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for ToolError {}
impl ToolError {
    fn arguments(error: impl fmt::Display) -> Self {
        Self {
            code: "invalid_arguments",
            message: error.to_string(),
            outcome_unknown: false,
        }
    }
    fn backend(error: ContractError, mutation: bool) -> Self {
        let code = match &error {
            ContractError::InvalidInput(_) => "invalid_input",
            ContractError::ExternalDispatchRequired => "external_dispatch_required",
            ContractError::InvalidQueueDelivery(_) => "invalid_queue_delivery",
            ContractError::Conflict => "conflict",
            ContractError::OwnershipLost => "ownership_lost",
            ContractError::UnknownSession => "unknown_session",
            ContractError::SessionExpired => "session_expired",
            ContractError::ObsoleteOperation => "obsolete_operation",
            ContractError::OutOfOrder => "out_of_order",
            ContractError::Busy => "busy",
            ContractError::NotFound => "not_found",
            ContractError::Unavailable(_) => "unavailable",
        };
        let outcome_unknown = mutation && matches!(error, ContractError::Unavailable(_));
        Self {
            code,
            message: error.to_string(),
            outcome_unknown,
        }
    }
}

/// The operator fixes the scope and available capabilities once at startup.
/// The adapter performs no automatic retries, polling, execution, or approvals.
#[derive(Clone)]
pub struct ToolDispatcher {
    tasks: Arc<dyn TaskService>,
    workflows: Arc<dyn WorkflowService>,
    catalog: Arc<dyn ProgramCatalogService>,
    scope: Scope,
    read_only: bool,
    tools: Arc<Vec<crate::schemas::ToolDefinition>>,
}
impl ToolDispatcher {
    pub fn new(
        tasks: Arc<dyn TaskService>,
        workflows: Arc<dyn WorkflowService>,
        catalog: Arc<dyn ProgramCatalogService>,
        scope: Scope,
        read_only: bool,
    ) -> ledgence_orchestration_api::Result<Self> {
        scope.validate()?;
        Ok(Self {
            tasks,
            workflows,
            catalog,
            scope,
            read_only,
            tools: Arc::new(tool_definitions(false)),
        })
    }
    pub fn read_only(&self) -> bool {
        self.read_only
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub async fn call(
        &self,
        name: &str,
        arguments: &Value,
    ) -> std::result::Result<Value, ToolError> {
        let definition = self
            .tools
            .iter()
            .find(|tool| tool.name == name)
            .ok_or_else(|| ToolError {
                code: "unknown_tool",
                message: "Unknown Ledgence tool".into(),
                outcome_unknown: false,
            })?;
        if self.read_only && !definition.read_only {
            return Err(ToolError {
                code: "read_only",
                message: "This MCP server only exposes read operations".into(),
                outcome_unknown: false,
            });
        }
        let value = match name {
            "ledgence_program_list" => {
                let args: ProgramList = parse(arguments)?;
                let query = ProgramCatalogQuery::Catalog {
                    kind: args.kind,
                    page: ConsolePagination {
                        limit: args.limit,
                        cursor: args.cursor,
                    },
                };
                self.programs(query).await?
            }
            "ledgence_program_versions" => {
                let args: ProgramVersions = parse(arguments)?;
                self.programs(ProgramCatalogQuery::Versions {
                    program_id: args.program,
                    page: ConsolePagination {
                        limit: args.limit,
                        cursor: args.cursor,
                    },
                })
                .await?
            }
            "ledgence_program_inspect" => {
                let args: ProgramIdentity = parse(arguments)?;
                self.programs(ProgramCatalogQuery::Inspect(ProgramRef {
                    id: args.program,
                    version: args.version,
                }))
                .await?
            }
            "ledgence_task_submit" | "ledgence_workflow_submit" => {
                let args: Submission = parse(arguments)?;
                let command = args.command(&self.scope)?;
                if name == "ledgence_task_submit" {
                    let reply = remote(self.tasks.submit(&command).await, true)?;
                    reply
                        .descriptor
                        .validate()
                        .map_err(|_| invalid_reply(true))?;
                    let matches = reply
                        .input
                        .semantically_matches(&command.input)
                        .map_err(|_| invalid_reply(true))?;
                    if !matches
                        || reply.idempotency_key != command.idempotency_key
                        || reply.descriptor.program != command.input.program
                        || reply.scope() != self.scope
                        || reply.workflow_id.is_some()
                        || reply.workflow_activation_id.is_some()
                        || reply.parent_workflow_id.is_some()
                        || reply.root_workflow_id.is_some()
                    {
                        return Err(invalid_reply(true));
                    }
                    if let Some(trace) = &reply.origin_trace {
                        trace.validate().map_err(|_| invalid_reply(true))?;
                    }
                    validate_text(&reply.task_id, 128).map_err(|_| invalid_reply(true))?;
                    validate_text(&reply.run_id, 128).map_err(|_| invalid_reply(true))?;
                    json!({"scope":self.scope,"task_id":reply.task_id,"run_id":reply.run_id,"state":reply.state,"idempotency_key":reply.idempotency_key,"submitted_at":reply.submitted_at})
                } else {
                    let reply = remote(self.workflows.submit_workflow(&command).await, true)?;
                    check_workflow(&reply, &self.scope, None, true)?;
                    if reply.correlation_key != command.input.correlation_key
                        || reply.parent_workflow_id.is_some()
                        || reply.root_workflow_id.is_some()
                    {
                        return Err(invalid_reply(true));
                    }
                    let mut value = encode(reply, true)?;
                    value
                        .as_object_mut()
                        .expect("workflow snapshot is an object")
                        .insert("idempotency_key".into(), json!(command.idempotency_key));
                    value
                }
            }
            "ledgence_task_list" => {
                let query: TaskListQuery = parse(arguments)?;
                query.validate(&self.scope).map_err(ToolError::arguments)?;
                let reply = remote(self.tasks.list_tasks(&self.scope, &query).await, false)?;
                reply
                    .validate(&self.scope, &query)
                    .map_err(|_| invalid_reply(false))?;
                encode(reply, false)?
            }
            "ledgence_task_status" | "ledgence_task_result" | "ledgence_task_cancel" => {
                let args: TaskReference = parse(arguments)?;
                validate_text(&args.task_id, 128).map_err(ToolError::arguments)?;
                match name {
                    "ledgence_task_status" => {
                        let reply =
                            remote(self.tasks.status(&self.scope, &args.task_id).await, false)?;
                        check_task(&reply, &self.scope, &args.task_id)?;
                        encode(reply, false)?
                    }
                    "ledgence_task_result" => {
                        let reply =
                            remote(self.tasks.result(&self.scope, &args.task_id).await, false)?;
                        reply.validate().map_err(|_| invalid_reply(false))?;
                        check_task(&reply.task, &self.scope, &args.task_id)?;
                        encode(reply, false)?
                    }
                    _ => {
                        let state =
                            remote(self.tasks.cancel(&self.scope, &args.task_id).await, true)?;
                        json!({"scope":self.scope,"task_id":args.task_id,"state":state})
                    }
                }
            }
            "ledgence_workflow_status"
            | "ledgence_workflow_result"
            | "ledgence_workflow_cancel" => {
                let args: WorkflowReference = parse(arguments)?;
                validate_text(&args.workflow_id, 128).map_err(ToolError::arguments)?;
                match name {
                    "ledgence_workflow_result" => {
                        let reply = remote(
                            self.workflows
                                .workflow_result(&self.scope, &args.workflow_id)
                                .await,
                            false,
                        )?;
                        reply.validate().map_err(|_| invalid_reply(false))?;
                        check_workflow(
                            &reply.workflow,
                            &self.scope,
                            Some(&args.workflow_id),
                            false,
                        )?;
                        encode(reply, false)?
                    }
                    _ => {
                        let mutation = name == "ledgence_workflow_cancel";
                        let reply = if mutation {
                            remote(
                                self.workflows
                                    .cancel_workflow(&self.scope, &args.workflow_id)
                                    .await,
                                true,
                            )?
                        } else {
                            remote(
                                self.workflows
                                    .workflow_status(&self.scope, &args.workflow_id)
                                    .await,
                                false,
                            )?
                        };
                        check_workflow(&reply, &self.scope, Some(&args.workflow_id), mutation)?;
                        encode(reply, mutation)?
                    }
                }
            }
            "ledgence_workflow_send_event" => {
                let args: SendEvent = parse(arguments)?;
                let command = WorkflowEventCommand {
                    scope: self.scope.clone(),
                    workflow_id: args.workflow_id,
                    key: args.key,
                    event: WorkflowEvent::new(args.event).map_err(ToolError::arguments)?,
                };
                command.validate().map_err(ToolError::arguments)?;
                let reply = remote(self.workflows.send_workflow_event(&command).await, true)?;
                reply.validate().map_err(|_| invalid_reply(true))?;
                if !reply.matches(&command) {
                    return Err(invalid_reply(true));
                }
                encode(reply, true)?
            }
            "ledgence_approval_list" => {
                let args: ApprovalList = parse(arguments)?;
                validate_text(&args.workflow_id, 128).map_err(ToolError::arguments)?;
                validate_approval_page(args.after_key.as_deref(), args.limit)
                    .map_err(ToolError::arguments)?;
                let reply = remote(
                    self.workflows
                        .list_approvals(
                            &self.scope,
                            &args.workflow_id,
                            args.after_key.as_deref(),
                            args.limit,
                        )
                        .await,
                    false,
                )?;
                reply.validate().map_err(|_| invalid_reply(false))?;
                if !reply.matches(
                    &self.scope,
                    &args.workflow_id,
                    args.after_key.as_deref(),
                    args.limit,
                ) {
                    return Err(invalid_reply(false));
                }
                encode(reply, false)?
            }
            "ledgence_approval_inspect" => {
                let args: ApprovalReference = parse(arguments)?;
                validate_text(&args.workflow_id, 128).map_err(ToolError::arguments)?;
                validate_text(&args.key, 128).map_err(ToolError::arguments)?;
                let reply = remote(
                    self.workflows
                        .approval(&self.scope, &args.workflow_id, &args.key)
                        .await,
                    false,
                )?;
                reply.validate().map_err(|_| invalid_reply(false))?;
                if reply.scope != self.scope
                    || reply.workflow_id != args.workflow_id
                    || reply.key != args.key
                {
                    return Err(invalid_reply(false));
                }
                encode(reply, false)?
            }
            _ => unreachable!("tool catalog and dispatcher must stay in sync"),
        };
        encode(value, !definition.read_only)
    }

    async fn programs(&self, query: ProgramCatalogQuery) -> std::result::Result<Value, ToolError> {
        query.validate(&self.scope).map_err(ToolError::arguments)?;
        let reply = remote(self.catalog.query_programs(&query).await, false)?;
        reply
            .validate(&self.scope, &query)
            .map_err(|_| invalid_reply(false))?;
        match reply {
            ProgramCatalogReply::Catalog(value) => encode(value, false),
            ProgramCatalogReply::Programs(value) => encode(value, false),
            ProgramCatalogReply::Versions(value) => encode(value, false),
            ProgramCatalogReply::Inspect(value) => encode(value, false),
        }
    }
}

fn remote<T>(
    result: ledgence_orchestration_api::Result<T>,
    mutation: bool,
) -> std::result::Result<T, ToolError> {
    result.map_err(|error| ToolError::backend(error, mutation))
}
fn parse<T: DeserializeOwned>(arguments: &Value) -> std::result::Result<T, ToolError> {
    if !arguments.is_object() {
        return Err(ToolError::arguments("Tool arguments must be an object"));
    }
    serde_json::from_value(arguments.clone()).map_err(ToolError::arguments)
}
fn invalid_reply(mutation: bool) -> ToolError {
    ToolError::backend(
        ContractError::Unavailable("Service response does not match its validated request".into()),
        mutation,
    )
}
fn encode(value: impl Serialize, mutation: bool) -> std::result::Result<Value, ToolError> {
    let value = serde_json::to_value(value).map_err(|_| invalid_reply(mutation))?;
    let bytes = serde_json::to_vec(&value).map_err(|_| invalid_reply(mutation))?;
    if bytes.len() > TOOL_OUTPUT_MAX_BYTES {
        return Err(ToolError { code: "response_too_large", message: "Result exceeds the 2 MiB MCP limit; use the HTTP API or a smaller page. No data was truncated.".into(), outcome_unknown: mutation });
    }
    Ok(value)
}
fn check_task(reply: &TaskStatus, scope: &Scope, id: &str) -> std::result::Result<(), ToolError> {
    reply.validate().map_err(|_| invalid_reply(false))?;
    if &reply.scope != scope || reply.task_id != id {
        return Err(invalid_reply(false));
    }
    Ok(())
}
fn check_workflow(
    reply: &WorkflowSnapshot,
    scope: &Scope,
    id: Option<&str>,
    mutation: bool,
) -> std::result::Result<(), ToolError> {
    reply.validate().map_err(|_| invalid_reply(mutation))?;
    if &reply.scope != scope || id.is_some_and(|id| reply.workflow_id != id) {
        return Err(invalid_reply(mutation));
    }
    Ok(())
}
fn default_limit() -> u32 {
    50
}
fn default_approval_limit() -> u32 {
    APPROVAL_PAGE_MAX_ITEMS
}
fn default_timeout() -> u64 {
    300_000
}
fn required_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Value, D::Error> {
    Value::deserialize(deserializer)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgramList {
    kind: Option<ConsoleProgramKind>,
    #[serde(default = "default_limit")]
    limit: u32,
    cursor: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgramVersions {
    program: String,
    #[serde(default = "default_limit")]
    limit: u32,
    cursor: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgramIdentity {
    program: String,
    version: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskReference {
    task_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowReference {
    workflow_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalReference {
    workflow_id: String,
    key: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalList {
    workflow_id: String,
    after_key: Option<String>,
    #[serde(default = "default_approval_limit")]
    limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendEvent {
    workflow_id: String,
    key: String,
    #[serde(deserialize_with = "required_value")]
    event: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Submission {
    program: String,
    version: String,
    queue: String,
    #[serde(deserialize_with = "required_value")]
    data: Value,
    idempotency_key: String,
    correlation_key: Option<String>,
    #[serde(default)]
    retry_policy: RetryPolicy,
    #[serde(default = "default_timeout")]
    attempt_timeout_ms: u64,
    origin_trace: Option<TraceContext>,
}
impl Submission {
    fn command(self, scope: &Scope) -> std::result::Result<SubmitCommand, ToolError> {
        validate_text(&self.idempotency_key, 255).map_err(ToolError::arguments)?;
        if let Some(trace) = &self.origin_trace {
            trace.validate().map_err(ToolError::arguments)?;
        }
        let input = SubmitTask {
            tenant_id: scope.tenant_id.clone(),
            namespace: scope.namespace.clone(),
            queue: self.queue,
            program: ProgramRef {
                id: self.program,
                version: self.version,
            },
            correlation_key: self.correlation_key,
            data: self.data,
            retry_policy: self.retry_policy,
            attempt_timeout_ms: self.attempt_timeout_ms,
        };
        input.validate().map_err(ToolError::arguments)?;
        Ok(SubmitCommand {
            idempotency_key: self.idempotency_key,
            input,
            origin_trace: self.origin_trace,
        })
    }
}

#[cfg(test)]
pub(crate) fn test_dispatcher(read_only: bool) -> ToolDispatcher {
    let scope = Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    };
    let http = ledgence_adapter_http::HttpTaskService::new("http://127.0.0.1:1").unwrap();
    let catalog =
        ledgence_adapter_http::HttpProgramCatalogService::new(http.clone(), scope.clone()).unwrap();
    ToolDispatcher::new(
        Arc::new(http.clone()),
        Arc::new(http),
        Arc::new(catalog),
        scope,
        read_only,
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> Scope {
        Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        }
    }
    fn submission() -> Value {
        json!({"program":"invoice","version":"1","queue":"billing","data":null,"idempotency_key":"issue:1"})
    }
    #[tokio::test]
    async fn tools_reject_scope_overrides_invalid_ids_and_unavailable_capabilities_before_http() {
        let dispatcher = test_dispatcher(false);
        for (name, args) in [
            (
                "ledgence_task_submit",
                json!({"program":"invoice","version":"1","queue":"billing","data":null,"idempotency_key":"x","tenant_id":"other"}),
            ),
            (
                "ledgence_task_list",
                json!({"scope":{"tenant_id":"other","namespace":"other"}}),
            ),
            ("ledgence_task_status", json!({"task_id":""})),
            (
                "ledgence_task_result",
                json!({"task_id":"task","namespace":"other"}),
            ),
            (
                "ledgence_workflow_submit",
                json!({"program":"invoice","version":"1","queue":"billing","idempotency_key":"x"}),
            ),
            ("ledgence_workflow_cancel", json!({"workflow_id":"\n"})),
            (
                "ledgence_workflow_send_event",
                json!({"workflow_id":"workflow","key":"event"}),
            ),
            (
                "ledgence_approval_list",
                json!({"workflow_id":"workflow","limit":101}),
            ),
            (
                "ledgence_approval_inspect",
                json!({"workflow_id":"workflow","key":"approve","decision":"approve"}),
            ),
            ("ledgence_program_list", json!({"tenant_id":"other"})),
            ("ledgence_program_versions", json!({"program":".."})),
            (
                "ledgence_program_inspect",
                json!({"program":"invoice","version":""}),
            ),
        ] {
            let error = dispatcher.call(name, &args).await.unwrap_err();
            assert_eq!(error.code, "invalid_arguments", "{name}: {error}");
            assert!(!error.outcome_unknown);
        }
        assert_eq!(
            dispatcher
                .call("ledgence_approval_decide", &json!({}))
                .await
                .unwrap_err()
                .code,
            "unknown_tool"
        );
        let readonly = test_dispatcher(true);
        for definition in tool_definitions(false)
            .into_iter()
            .filter(|tool| !tool.read_only)
        {
            let error = readonly
                .call(definition.name, &json!({}))
                .await
                .unwrap_err();
            assert_eq!(error.code, "read_only");
            assert!(!error.outcome_unknown);
        }
    }
    #[test]
    fn default_task_list_and_explicit_cloud_event_identity_are_preserved() {
        let approvals: ApprovalList = parse(&json!({"workflow_id":"workflow"})).unwrap();
        validate_approval_page(None, approvals.limit).unwrap();
        assert_eq!(approvals.limit, 10);
        let query: TaskListQuery = parse(&json!({})).unwrap();
        assert_eq!(query.limit, 50);
        query.validate(&scope()).unwrap();
        let event = json!({"specversion":"1.0","id":"callback:1","source":"urn:billing:callback","type":"invoice.paid","datacontenttype":"application/json","data":{"n":u64::MAX},"businessid":"INV-1"});
        let args: SendEvent =
            parse(&json!({"workflow_id":"workflow","key":"paid:1","event":event})).unwrap();
        let parsed = WorkflowEvent::new(args.event).unwrap();
        assert_eq!(parsed.id(), "callback:1");
        assert_eq!(parsed.source(), "urn:billing:callback");
        assert_eq!(parsed.value(), &event);
    }
    #[tokio::test]
    async fn replay_keeps_original_trace_when_retry_omits_or_changes_it() {
        use ledgence_adapter_http::{HttpProgramCatalogService, HttpTaskService};
        use ledgence_worker_api::{Digest, ProgramDescriptor};
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let trace = TraceContext {
            traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
            tracestate: None,
        };
        let original = parse::<Submission>(&submission())
            .unwrap()
            .command(&scope())
            .unwrap();
        let snapshot = TaskSnapshot {
            parent_workflow_id: None,
            root_workflow_id: None,
            workflow_id: None,
            workflow_activation_id: None,
            task_id: "task-original".into(),
            run_id: "run-original".into(),
            idempotency_key: original.idempotency_key,
            descriptor: ProgramDescriptor {
                program: original.input.program.clone(),
                digest: Digest(format!("sha256:{}", "a".repeat(64))),
                size: 1,
            },
            input: original.input,
            origin_trace: Some(trace.clone()),
            state: TaskState::Queued,
            submitted_at: 1,
            available_at: 1,
            terminal_at: None,
            current_attempt_id: None,
            attempt_count: 0,
            cancel_requested_at: None,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http =
            HttpTaskService::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let catalog = HttpProgramCatalogService::new(http.clone(), scope()).unwrap();
        let dispatcher = ToolDispatcher::new(
            Arc::new(http.clone()),
            Arc::new(http),
            Arc::new(catalog),
            scope(),
            false,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&bytes[..count]);
                    if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|value| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                let body = serde_json::to_string(&snapshot).unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        for origin in [
            Some(trace),
            None,
            Some(TraceContext {
                traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
                tracestate: None,
            }),
        ] {
            let mut arguments = submission();
            if let Some(origin) = origin {
                arguments["origin_trace"] = json!(origin);
            }
            let result = dispatcher
                .call("ledgence_task_submit", &arguments)
                .await
                .unwrap();
            assert_eq!(result["task_id"], "task-original");
            assert_eq!(result["idempotency_key"], "issue:1");
        }
        server.await.unwrap();
    }
    #[test]
    fn strict_submission_preserves_values_and_requires_explicit_identity() {
        let mut args = submission();
        args["data"] = json!([u64::MAX, i64::MIN, 1, 1.0, -0.0, null]);
        let command = parse::<Submission>(&args)
            .unwrap()
            .command(&scope())
            .unwrap();
        assert_eq!(command.input.tenant_id, "acme");
        assert_eq!(command.input.namespace, "billing");
        assert_eq!(
            serde_json::to_string(&command.input.data).unwrap(),
            "[18446744073709551615,-9223372036854775808,1,1.0,-0.0,null]"
        );
        assert_eq!(command.idempotency_key, "issue:1");
        assert_eq!(command.input.attempt_timeout_ms, 300_000);
        for key in ["idempotency_key", "data", "program"] {
            let mut invalid = submission();
            invalid.as_object_mut().unwrap().remove(key);
            assert_eq!(
                parse::<Submission>(&invalid).err().unwrap().code,
                "invalid_arguments"
            );
        }
        for key in [
            "tenant_id",
            "namespace",
            "scope",
            "attempt_id",
            "unexpected",
        ] {
            let mut invalid = submission();
            invalid[key] = json!("override");
            assert!(parse::<Submission>(&invalid).is_err());
        }
    }
    #[test]
    fn validation_happens_before_dispatch_and_uncertainty_is_explicit() {
        let mut invalid = submission();
        invalid["idempotency_key"] = json!("");
        let error = parse::<Submission>(&invalid)
            .unwrap()
            .command(&scope())
            .unwrap_err();
        assert_eq!(error.code, "invalid_arguments");
        assert!(!error.outcome_unknown);
        invalid = submission();
        invalid["attempt_timeout_ms"] = json!(1);
        assert!(
            parse::<Submission>(&invalid)
                .unwrap()
                .command(&scope())
                .is_err()
        );
        for mutation in [true, false] {
            assert_eq!(
                ToolError::backend(ContractError::Unavailable("transport".into()), mutation)
                    .outcome_unknown,
                mutation
            );
            assert!(!ToolError::backend(ContractError::Conflict, mutation).outcome_unknown);
            assert_eq!(
                ToolError::backend(ContractError::InvalidInput("backend".into()), mutation).code,
                "invalid_input"
            );
        }
    }
    #[test]
    fn result_limit_never_truncates_or_confuses_pending_with_null() {
        assert_eq!(
            encode(json!({"outcome":null}), false).unwrap()["outcome"],
            Value::Null
        );
        assert_eq!(
            encode(json!({"outcome":{"kind":"succeeded","output":null}}), false).unwrap()["outcome"]
                ["kind"],
            "succeeded"
        );
        let error = encode("x".repeat(TOOL_OUTPUT_MAX_BYTES), false).unwrap_err();
        assert_eq!(error.code, "response_too_large");
        assert!(!error.outcome_unknown);
    }
}
