use super::*;

pub(super) async fn get(
    server: &Server,
    path: &str,
    scope: Scope,
    id: String,
) -> std::result::Result<Vec<u8>, Failure> {
    validate_text(&id, 128)?;
    record_scope(&scope);
    tracing::Span::current().record("ledgence.workflow.id", &id);
    let service = service(server)?;
    match path {
        "/v1/workflows/status" => {
            let reply = service.workflow_status(&scope, &id).await?;
            check_identity(&reply, &scope, &id)?;
            encode_snapshot(server, reply).await
        }
        "/v1/workflows/result" => {
            let reply = service.workflow_result(&scope, &id).await?;
            check_identity(&reply.workflow, &scope, &id)?;
            server
                .blocking(move || {
                    reply
                        .validate()
                        .map_err(|_| unavailable("invalid workflow service response"))?;
                    record_workflow(&reply.workflow);
                    encode_bounded(&reply, RESPONSE_MAX_BYTES)
                })
                .await
        }
        _ => Err(invalid("unsupported workflow lookup").into()),
    }
}

pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    maximum: usize,
) -> std::result::Result<Vec<u8>, Failure> {
    let service = service(server)?;
    match path {
        "/v1/workflows" => {
            let command = server
                .blocking(move || SubmitCommand::decode(&bytes).map_err(Into::into))
                .await?;
            record_scope(&Scope {
                tenant_id: command.input.tenant_id.clone(),
                namespace: command.input.namespace.clone(),
            });
            let reply = service.submit_workflow(&command).await?;
            if reply.scope.tenant_id != command.input.tenant_id
                || reply.scope.namespace != command.input.namespace
                || reply.correlation_key != command.input.correlation_key
            {
                return Err(unavailable("workflow submission response identity mismatch").into());
            }
            encode_snapshot(server, reply).await
        }
        "/v1/workflows/cancel" => {
            let command: WorkflowReference = server.decode(bytes, maximum).await?;
            command.scope.validate()?;
            validate_text(&command.workflow_id, 128)?;
            let reply = service
                .cancel_workflow(&command.scope, &command.workflow_id)
                .await?;
            check_identity(&reply, &command.scope, &command.workflow_id)?;
            encode_snapshot(server, reply).await
        }
        "/v1/workflows/activations/context" => {
            let owner: LeaseOwner = server.decode(bytes, maximum).await?;
            log_owner(&owner)?;
            let reply = service.activation_context(&owner).await?;
            if reply.activation_id != owner.task_id {
                return Err(
                    unavailable("activation response identity disagrees with request").into(),
                );
            }
            server
                .blocking(move || {
                    reply
                        .validate()
                        .map_err(|_| unavailable("invalid workflow service response"))?;
                    tracing::Span::current().record("ledgence.workflow.id", &reply.workflow_id);
                    tracing::Span::current().record("ledgence.activation.id", &reply.activation_id);
                    encode_bounded(&reply, WORKFLOW_CONTEXT_MAX_BYTES)
                })
                .await
        }
        "/v1/workflows/local-results" => {
            let command = server
                .blocking(move || {
                    let command: LocalResultCommand = decode_unique_json(&bytes, maximum)
                        .map_err(|_| invalid("malformed JSON command"))?;
                    command.record.validate()?;
                    Ok(command)
                })
                .await?;
            log_owner(&command.owner)?;
            let reply = service.record_local_result(&command).await?;
            if reply.key != command.record.key {
                return Err(unavailable("local result receipt disagrees with request").into());
            }
            server.encode(reply).await
        }
        _ => Err(invalid("unsupported workflow operation").into()),
    }
}
fn service(server: &Server) -> Result<&dyn WorkflowService> {
    server
        .workflows
        .as_deref()
        .ok_or_else(|| invalid("workflow support is not configured"))
}
fn check_identity(reply: &WorkflowSnapshot, scope: &Scope, id: &str) -> Result<()> {
    if &reply.scope != scope || reply.workflow_id != id {
        return Err(unavailable(
            "workflow response identity disagrees with request",
        ));
    }
    Ok(())
}

fn record_workflow(snapshot: &WorkflowSnapshot) {
    let span = tracing::Span::current();
    span.record("ledgence.workflow.id", &snapshot.workflow_id);
    if let Some(id) = &snapshot.activation_id {
        span.record("ledgence.activation.id", id);
    }
}

async fn encode_snapshot(
    server: &Server,
    reply: WorkflowSnapshot,
) -> std::result::Result<Vec<u8>, Failure> {
    server
        .blocking(move || {
            reply
                .validate()
                .map_err(|_| unavailable("invalid workflow service response"))?;
            record_workflow(&reply);
            encode_bounded(&reply, TASK_STATUS_MAX_BYTES)
        })
        .await
}
