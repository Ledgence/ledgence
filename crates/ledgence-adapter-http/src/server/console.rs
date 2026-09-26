//! Same-origin Console adapter. Commands delegate to existing lifecycle services.
use super::*;
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::ProgramRef;
use serde::Deserialize;

#[derive(Clone)]
pub struct ConsoleServices {
    pub(crate) instance: SelfHostedInstanceConfig,
    config: ConsoleConfig,
    queries: Arc<dyn ConsoleQueryService>,
    catalog: Option<Arc<dyn ProgramCatalogService>>,
    observations: Option<Arc<dyn WorkerObservationService>>,
}
impl ConsoleServices {
    pub fn new(
        instance: SelfHostedInstanceConfig,
        server_version: String,
        queries: Arc<dyn ConsoleQueryService>,
        catalog: Option<Arc<dyn ProgramCatalogService>>,
        observations: Option<Arc<dyn WorkerObservationService>>,
    ) -> Result<Self> {
        instance.validate()?;
        HeaderValue::from_str(&instance.instance_id)
            .map_err(|_| invalid("instance_id must be representable in an HTTP header"))?;
        for origin in &instance.allowed_origins {
            let uri: axum::http::Uri = origin
                .parse()
                .map_err(|_| invalid("invalid allowed origin"))?;
            if !matches!(uri.scheme_str(), Some("http" | "https"))
                || uri.authority().is_none()
                || uri.authority().is_some_and(|a| a.as_str().contains('@'))
                || uri.query().is_some()
                || origin.ends_with('/')
                || uri.path() != "/"
            {
                return Err(invalid(
                    "allowed origins must contain only scheme and authority",
                ));
            }
        }
        let config = ConsoleConfig {
            contract_version: CONSOLE_CONTRACT_VERSION,
            server_version,
            instance_id: instance.instance_id.clone(),
            instance_name: instance.name.clone(),
            capabilities: ConsoleCapabilities {
                executions: true,
                workflows: true,
                programs: catalog.is_some(),
                workers: observations.is_some(),
            },
            suggested_queues: instance.suggested_queues.clone(),
            limits: ConsoleLimits::default(),
            polling: ConsolePolling::default(),
        };
        Ok(Self {
            instance,
            config,
            queries,
            catalog,
            observations,
        })
    }
    pub(super) fn check_origin(&self, headers: &axum::http::HeaderMap) -> Result<()> {
        let origins = headers.get_all(header::ORIGIN);
        if origins.iter().count() == 0 {
            return Ok(());
        }
        if origins.iter().count() != 1
            || !origins
                .iter()
                .next()
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    self.instance
                        .allowed_origins
                        .iter()
                        .any(|allowed| allowed == value)
                })
        {
            return Err(invalid(
                "request Origin is not configured for this instance",
            ));
        }
        Ok(())
    }
}
pub(super) const ROUTES: &[(&str, &str)] = &[
    ("/v1/console/config", "GET"),
    ("/v1/console/tasks", "GET, POST"),
    ("/v1/console/tasks/inspect", "GET"),
    ("/v1/console/tasks/status", "GET"),
    ("/v1/console/tasks/result", "GET"),
    ("/v1/console/tasks/history", "GET"),
    ("/v1/console/tasks/cancel", "POST"),
    ("/v1/console/tasks/attempts", "GET"),
    ("/v1/console/attempts/inspect", "GET"),
    ("/v1/console/workflows", "GET, POST"),
    ("/v1/console/workflows/inspect", "GET"),
    ("/v1/console/workflows/status", "GET"),
    ("/v1/console/workflows/result", "GET"),
    ("/v1/console/workflows/cancel", "POST"),
    ("/v1/console/workflows/events", "POST"),
    ("/v1/console/workflows/activations", "GET"),
    ("/v1/console/workflows/children", "GET"),
    ("/v1/console/workflows/waits", "GET"),
    ("/v1/console/workflows/history", "GET"),
    ("/v1/console/workflows/local-steps", "GET"),
    ("/v1/console/programs", "GET"),
    ("/v1/console/programs/versions", "GET"),
    ("/v1/console/programs/inspect", "GET"),
    ("/v1/console/programs/register", "POST"),
    ("/v1/console/workers", "GET"),
    ("/v1/console/workers/inspect", "GET"),
    ("/v1/worker-observations", "POST"),
];
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Submission {
    idempotency_key: String,
    input: ConsoleSubmitTask,
    origin_trace: Option<TraceContext>,
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
#[derive(Serialize)]
struct TaskCancellation {
    task_id: String,
    state: TaskState,
    observed_at: Timestamp,
}

pub(super) async fn dispatch(
    server: &Server,
    request: Request,
) -> std::result::Result<Vec<u8>, Failure> {
    let console = server.console.as_ref().ok_or(ContractError::NotFound)?;
    let scope = &console.instance.scope;
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    if parts.method == axum::http::Method::GET {
        to_bytes(body, 0)
            .await
            .map_err(|_| invalid("GET operation does not accept a request body"))?;
        let fields = fields(parts.uri.query().unwrap_or(""), path)?;
        if path == "/v1/console/config" {
            return server.encode(console.config.clone()).await;
        }
        let page = pagination(&fields)?;
        if path.starts_with("/v1/console/programs") {
            let service = console.catalog.as_ref().ok_or(ContractError::NotFound)?;
            let query = match path {
                "/v1/console/programs" => ProgramCatalogQuery::Programs(page),
                "/v1/console/programs/versions" => ProgramCatalogQuery::Versions {
                    program_id: required(&fields, "program_id")?,
                    page,
                },
                _ => ProgramCatalogQuery::Inspect(ProgramRef {
                    id: required(&fields, "program_id")?,
                    version: required(&fields, "version")?,
                }),
            };
            query.validate(scope)?;
            let reply = service.query_programs(&query).await?;
            reply.validate(scope, &query)?;
            return match reply {
                ProgramCatalogReply::Programs(v) => metadata(server, v).await,
                ProgramCatalogReply::Versions(v) => metadata(server, v).await,
                ProgramCatalogReply::Inspect(v) => metadata(server, v).await,
            };
        }
        if path.starts_with("/v1/console/workers") {
            let service = console
                .observations
                .as_ref()
                .ok_or(ContractError::NotFound)?;
            let query = if path == "/v1/console/workers" {
                WorkerObservationQuery::Workers {
                    queue: fields.get("queue").cloned(),
                    page,
                }
            } else {
                WorkerObservationQuery::Inspect {
                    worker_session_id: required(&fields, "worker_session_id")?,
                    page,
                }
            };
            query.validate(scope)?;
            let reply = service.query_workers(&query).await?;
            reply.validate(scope, &query)?;
            return match reply {
                WorkerObservationReply::Workers(v) => metadata(server, v).await,
                WorkerObservationReply::Inspect(v) => metadata(server, v).await,
            };
        }
        if matches!(
            path,
            "/v1/console/tasks/status"
                | "/v1/console/tasks/result"
                | "/v1/console/tasks/inspect"
                | "/v1/console/tasks/history"
        ) {
            let id = required(&fields, "task_id")?;
            return match path {
                "/v1/console/tasks/status" => {
                    let reply = server.service.status(scope, &id).await?;
                    check_task_identity(&reply, scope, &id)?;
                    reply.validate()?;
                    metadata(
                        server,
                        ConsoleObservedTaskStatus {
                            task: reply.into(),
                            observed_at: now()?,
                        },
                    )
                    .await
                }
                "/v1/console/tasks/result" => {
                    let reply = server.service.result(scope, &id).await?;
                    check_task_identity(&reply.task, scope, &id)?;
                    reply.validate()?;
                    server
                        .encode(ConsoleTaskResult {
                            task: reply.task.into(),
                            outcome: reply.outcome,
                            observed_at: now()?,
                        })
                        .await
                }
                "/v1/console/tasks/inspect" => {
                    let reply = server.service.inspect(scope, &id).await?;
                    check_snapshot(&reply, scope, &id)?;
                    server.encode(task_detail(reply)?).await
                }
                _ => task_history(server, scope, &id, page).await,
            };
        }
        if matches!(
            path,
            "/v1/console/workflows/status" | "/v1/console/workflows/result"
        ) {
            let service = server.workflows.as_ref().ok_or(ContractError::NotFound)?;
            let id = required(&fields, "workflow_id")?;
            if path.ends_with("/status") {
                let reply = service.workflow_status(scope, &id).await?;
                check_workflow(&reply, scope, &id)?;
                return metadata(
                    server,
                    ConsoleObservedWorkflowStatus {
                        workflow: reply.into(),
                        observed_at: now()?,
                    },
                )
                .await;
            }
            let reply = service.workflow_result(scope, &id).await?;
            check_workflow(&reply.workflow, scope, &id)?;
            reply.validate()?;
            return server
                .encode(ConsoleWorkflowResult {
                    workflow: reply.workflow.into(),
                    outcome: reply.outcome,
                    observed_at: now()?,
                })
                .await;
        }
        let query = match path {
            "/v1/console/tasks" => ConsoleQuery::Tasks {
                filters: list_query(&fields)?.filters,
                page,
            },
            "/v1/console/tasks/attempts" => ConsoleQuery::Attempts {
                task_id: required(&fields, "task_id")?,
                page,
            },
            "/v1/console/attempts/inspect" => ConsoleQuery::Attempt {
                attempt_id: required(&fields, "attempt_id")?,
            },
            "/v1/console/workflows" => ConsoleQuery::Workflows {
                filters: workflow_filters(&fields)?,
                page,
            },
            "/v1/console/workflows/inspect" => ConsoleQuery::Workflow {
                workflow_id: required(&fields, "workflow_id")?,
            },
            "/v1/console/workflows/activations" => ConsoleQuery::Activations {
                workflow_id: required(&fields, "workflow_id")?,
                page,
            },
            "/v1/console/workflows/children" => ConsoleQuery::Children {
                workflow_id: required(&fields, "workflow_id")?,
                page,
            },
            "/v1/console/workflows/waits" => ConsoleQuery::Waits {
                workflow_id: required(&fields, "workflow_id")?,
                page,
            },
            "/v1/console/workflows/history" => ConsoleQuery::History {
                workflow_id: required(&fields, "workflow_id")?,
                page,
            },
            "/v1/console/workflows/local-steps" => ConsoleQuery::LocalSteps {
                workflow_id: required(&fields, "workflow_id")?,
                activation_id: required(&fields, "activation_id")?,
                page,
            },
            _ => return Err(ContractError::NotFound.into()),
        };
        query.validate(scope)?;
        let reply = console.queries.query_console(&query).await?;
        reply.validate(scope, &query)?;
        return match reply {
            ConsoleQueryReply::Tasks(v) => metadata(server, v).await,
            ConsoleQueryReply::Attempts(v) => metadata(server, v).await,
            ConsoleQueryReply::Attempt(v) => metadata(server, v).await,
            ConsoleQueryReply::Workflows(v) => metadata(server, v).await,
            ConsoleQueryReply::Workflow(v) => metadata(server, v).await,
            ConsoleQueryReply::Activations(v) => metadata(server, v).await,
            ConsoleQueryReply::Children(v) => metadata(server, v).await,
            ConsoleQueryReply::Waits(v) => metadata(server, v).await,
            ConsoleQueryReply::History(v) => metadata(server, v).await,
            ConsoleQueryReply::LocalSteps(v) => metadata(server, v).await,
        };
    }
    if parts.uri.query().is_some() {
        return Err(invalid("query parameters are not accepted for mutations").into());
    }
    console.check_origin(&parts.headers)?;
    check_content_type(&parts.headers)?;
    let maximum = if path == "/v1/console/workflows/events" {
        WORKFLOW_EVENT_COMMAND_MAX_BYTES
    } else {
        SUBMISSION_MAX_BYTES
    };
    let bytes = to_bytes(body, maximum)
        .await
        .map_err(|_| Failure {
            status: 413,
            error: invalid("request body exceeds limit or was interrupted"),
        })?
        .to_vec();
    match path {
        "/v1/console/tasks" | "/v1/console/workflows" => {
            let submission: Submission = server.decode(bytes, maximum).await?;
            validate_text(&submission.idempotency_key, 255)?;
            if let Some(trace) = &submission.origin_trace {
                trace.validate()?;
            }
            let command = SubmitCommand {
                idempotency_key: submission.idempotency_key,
                input: submission.input.into_submission(scope)?,
                origin_trace: submission.origin_trace,
            };
            if path.ends_with("/tasks") {
                let reply = server.service.submit(&command).await?;
                check_snapshot(&reply, scope, &reply.task_id)?;
                if reply.idempotency_key != command.idempotency_key
                    || !reply.input.semantically_matches(&command.input)?
                {
                    return Err(unavailable("inconsistent task submission response").into());
                }
                server.encode(task_detail(reply)?).await
            } else {
                let service = server.workflows.as_ref().ok_or(ContractError::NotFound)?;
                let reply = service.submit_workflow(&command).await?;
                check_workflow(&reply, scope, &reply.workflow_id)?;
                if reply.correlation_key != command.input.correlation_key
                    || reply.parent_workflow_id.is_some()
                {
                    return Err(unavailable("inconsistent workflow submission response").into());
                }
                metadata(
                    server,
                    ConsoleObservedWorkflowStatus {
                        workflow: reply.into(),
                        observed_at: now()?,
                    },
                )
                .await
            }
        }
        "/v1/console/tasks/cancel" => {
            let command: TaskReference = server.decode(bytes, maximum).await?;
            validate_text(&command.task_id, 128)?;
            let state = server.service.cancel(scope, &command.task_id).await?;
            metadata(
                server,
                TaskCancellation {
                    task_id: command.task_id,
                    state,
                    observed_at: now()?,
                },
            )
            .await
        }
        "/v1/console/workflows/cancel" => {
            let command: WorkflowReference = server.decode(bytes, maximum).await?;
            validate_text(&command.workflow_id, 128)?;
            let reply = server
                .workflows
                .as_ref()
                .ok_or(ContractError::NotFound)?
                .cancel_workflow(scope, &command.workflow_id)
                .await?;
            check_workflow(&reply, scope, &command.workflow_id)?;
            metadata(
                server,
                ConsoleObservedWorkflowStatus {
                    workflow: reply.into(),
                    observed_at: now()?,
                },
            )
            .await
        }
        "/v1/console/workflows/events" => {
            let event: ConsoleWorkflowEvent = server.decode(bytes, maximum).await?;
            let command = WorkflowEventCommand {
                scope: scope.clone(),
                workflow_id: event.workflow_id,
                key: event.key,
                event: event.event,
            };
            command.validate()?;
            let reply = server
                .workflows
                .as_ref()
                .ok_or(ContractError::NotFound)?
                .send_workflow_event(&command)
                .await?;
            if !reply.matches(&command) {
                return Err(unavailable("inconsistent workflow event receipt").into());
            }
            reply.validate()?;
            metadata(server, ConsoleWorkflowEventReceipt::from(reply)).await
        }
        "/v1/console/programs/register" => {
            let command: RegisterProgram = server.decode(bytes, maximum).await?;
            command.validate()?;
            let reply = console
                .catalog
                .as_ref()
                .ok_or(ContractError::NotFound)?
                .register_program(&command)
                .await?;
            reply.version.validate()?;
            if reply.version.descriptor.program != command.program {
                return Err(unavailable("inconsistent catalog receipt").into());
            }
            metadata(server, reply).await
        }
        "/v1/worker-observations" => {
            let command: WorkerObservationCommand = server.decode(bytes, maximum).await?;
            command.validate()?;
            let reply = console
                .observations
                .as_ref()
                .ok_or(ContractError::NotFound)?
                .publish_observation(&command)
                .await?;
            if reply.worker_session_id != command.worker_session_id
                || reply.sequence != command.sequence
            {
                return Err(unavailable("inconsistent observation receipt").into());
            }
            metadata(server, reply).await
        }
        _ => Err(ContractError::NotFound.into()),
    }
}
async fn metadata<T: Serialize + Send + 'static>(
    server: &Server,
    value: T,
) -> std::result::Result<Vec<u8>, Failure> {
    server
        .blocking(move || encode_bounded(&value, CONSOLE_METADATA_MAX_BYTES))
        .await
}
fn now() -> Result<Timestamp> {
    let at = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| unavailable("server clock unavailable"))?
            .as_millis(),
    )
    .map_err(|_| unavailable("server clock outside supported range"))?;
    if at > CONSOLE_MAX_TIMESTAMP {
        return Err(unavailable("server clock outside supported range"));
    }
    Ok(at)
}
fn required(fields: &BTreeMap<String, String>, key: &str) -> Result<String> {
    let value = fields
        .get(key)
        .ok_or_else(|| invalid("missing required query parameter"))?;
    validate_text(value, 128)?;
    Ok(value.clone())
}
fn number(fields: &BTreeMap<String, String>, key: &str) -> Result<Option<u64>> {
    fields
        .get(key)
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid("expected unsigned query number"));
            }
            value
                .parse()
                .map_err(|_| invalid("query number out of range"))
        })
        .transpose()
}
fn pagination(fields: &BTreeMap<String, String>) -> Result<ConsolePagination> {
    let limit = number(fields, "limit")?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| invalid("invalid page size"))?
        .unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(invalid("page size must be between 1 and 100"));
    }
    Ok(ConsolePagination {
        limit,
        cursor: fields.get("cursor").cloned(),
    })
}
fn workflow_filters(fields: &BTreeMap<String, String>) -> Result<ConsoleWorkflowFilters> {
    Ok(ConsoleWorkflowFilters {
        state: fields
            .get("state")
            .map(|state| {
                serde_json::from_value(serde_json::Value::String(state.clone()))
                    .map_err(|_| invalid("invalid workflow state"))
            })
            .transpose()?,
        correlation_key: fields.get("correlation_key").cloned(),
        submitted_from: number(fields, "submitted_from")?,
        submitted_until: number(fields, "submitted_until")?,
        parent_workflow_id: fields.get("parent_workflow_id").cloned(),
        root_only: fields
            .get("root_only")
            .map(|v| match v.as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(invalid("root_only must be true or false")),
            })
            .transpose()?
            .unwrap_or(false),
    })
}
fn fields(raw: &str, path: &str) -> Result<BTreeMap<String, String>> {
    if raw.len() > CONSOLE_QUERY_MAX_BYTES {
        return Err(invalid("console query exceeds limit"));
    }
    let allowed: &[&str] = match path {
        "/v1/console/config" => &[],
        "/v1/console/tasks" => &[
            "state",
            "queue",
            "correlation_key",
            "submitted_from",
            "submitted_until",
            "limit",
            "cursor",
        ],
        "/v1/console/workflows" => &[
            "state",
            "correlation_key",
            "submitted_from",
            "submitted_until",
            "parent_workflow_id",
            "root_only",
            "limit",
            "cursor",
        ],
        "/v1/console/tasks/attempts" | "/v1/console/tasks/history" => {
            &["task_id", "limit", "cursor"]
        }
        "/v1/console/tasks/status" | "/v1/console/tasks/result" | "/v1/console/tasks/inspect" => {
            &["task_id"]
        }
        "/v1/console/attempts/inspect" => &["attempt_id"],
        "/v1/console/workflows/inspect"
        | "/v1/console/workflows/status"
        | "/v1/console/workflows/result" => &["workflow_id"],
        "/v1/console/workflows/local-steps" => &["workflow_id", "activation_id", "limit", "cursor"],
        "/v1/console/workflows/activations"
        | "/v1/console/workflows/children"
        | "/v1/console/workflows/waits"
        | "/v1/console/workflows/history" => &["workflow_id", "limit", "cursor"],
        "/v1/console/programs" => &["limit", "cursor"],
        "/v1/console/programs/versions" => &["program_id", "limit", "cursor"],
        "/v1/console/programs/inspect" => &["program_id", "version"],
        "/v1/console/workers" => &["queue", "limit", "cursor"],
        "/v1/console/workers/inspect" => &["worker_session_id", "limit", "cursor"],
        _ => return Err(ContractError::NotFound),
    };
    let mut fields = BTreeMap::new();
    if raw.is_empty() {
        return Ok(fields);
    }
    for pair in raw.split('&') {
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| invalid("malformed query parameter"))?;
        let name = decode_component(name)?;
        if !allowed.contains(&name.as_str())
            || fields.insert(name, decode_component(value)?).is_some()
        {
            return Err(invalid("unknown or duplicate console query field"));
        }
    }
    Ok(fields)
}
fn check_snapshot(reply: &TaskSnapshot, scope: &Scope, id: &str) -> Result<()> {
    if reply.scope() != *scope
        || reply.task_id != id
        || reply.descriptor.program != reply.input.program
    {
        return Err(unavailable("inconsistent task snapshot identity"));
    }
    Ok(())
}
fn task_detail(reply: TaskSnapshot) -> Result<ConsoleTaskDetail> {
    let detail = ConsoleTaskDetail::from_snapshot(reply, now()?);
    detail
        .validate()
        .map_err(|_| unavailable("inconsistent task detail"))?;
    Ok(detail)
}
fn check_workflow(reply: &WorkflowSnapshot, scope: &Scope, id: &str) -> Result<()> {
    if reply.scope != *scope || reply.workflow_id != id {
        return Err(unavailable("inconsistent workflow identity"));
    }
    reply.validate()
}
fn check_content_type(headers: &axum::http::HeaderMap) -> std::result::Result<(), Failure> {
    if headers.get_all(header::CONTENT_TYPE).iter().count() != 1
        || !headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(is_json)
        || headers.get_all(header::CONTENT_ENCODING).iter().count() > 1
        || headers
            .get(header::CONTENT_ENCODING)
            .is_some_and(|v| v != "identity")
    {
        return Err(Failure {
            status: 415,
            error: invalid("expected uncompressed application/json with UTF-8 encoding"),
        });
    }
    Ok(())
}
async fn task_history(
    server: &Server,
    scope: &Scope,
    task_id: &str,
    page: ConsolePagination,
) -> std::result::Result<Vec<u8>, Failure> {
    let binding = ConsoleCursorBinding {
        endpoint: "tasks/history",
        scope: scope.clone(),
        parent: vec![task_id.into()],
        filters: serde_json::Value::Null,
        descending: false,
        numeric_keys: vec![true],
    };
    let after = match page.validate(&binding)?.as_deref() {
        None => 0,
        Some([ConsoleKey::Number(value)]) => value.0,
        _ => return Err(invalid("invalid task history cursor").into()),
    };
    let events = server.service.history(scope, task_id, after).await?;
    if events.len() > 100 {
        return Err(unavailable("inconsistent history page").into());
    }
    let more = events.len() > page.limit as usize || events.len() == 100;
    let items: Vec<_> = events
        .into_iter()
        .take(page.limit as usize)
        .map(|v| ConsoleTaskHistory {
            sequence: ConsoleU64(v.sequence),
            task_id: v.event.task_id,
            attempt_id: v.event.attempt_id,
            at: v.event.at,
            reason: v.event.reason,
        })
        .collect();
    if items.iter().any(|v| v.task_id != task_id) {
        return Err(unavailable("inconsistent history identity").into());
    }
    let next_cursor = if more {
        items
            .last()
            .map(|v| page.next_cursor(&binding, &v.position()))
            .transpose()?
    } else {
        None
    };
    let reply = ConsolePage {
        items,
        next_cursor,
        observed_at: now()?,
    };
    reply.validate(&page, &binding)?;
    metadata(server, reply).await
}
