use super::*;

pub(super) async fn get(
    server: &Server,
    scope: Scope,
    subscription_id: String,
) -> std::result::Result<Vec<u8>, Failure> {
    validate_text(&subscription_id, 128)?;
    record_scope(&scope);
    tracing::Span::current().record("ledgence.completion.subscription.id", &subscription_id);
    let reply = service(server)?
        .completion_status(&scope, &subscription_id)
        .await?;
    check_identity(&reply, &scope, &subscription_id)?;
    encode(server, reply).await
}

pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
) -> std::result::Result<Vec<u8>, Failure> {
    let service = service(server)?;
    match path {
        "/v1/completion-subscriptions" => {
            let command: CompletionSubscribeCommand =
                server.decode(bytes, COMPLETION_COMMAND_MAX_BYTES).await?;
            command.validate()?;
            record_command(&command);
            let reply = service.subscribe_completion(&command).await?;
            if !reply.matches(&command) {
                return Err(
                    unavailable("completion subscription response identity mismatch").into(),
                );
            }
            encode(server, reply).await
        }
        "/v1/completion-subscriptions/retry" => {
            let command: CompletionRetryCommand =
                server.decode(bytes, COMPLETION_COMMAND_MAX_BYTES).await?;
            command.validate()?;
            record_scope(&command.scope);
            tracing::Span::current().record(
                "ledgence.completion.subscription.id",
                &command.subscription_id,
            );
            let reply = service.retry_completion(&command).await?;
            check_identity(&reply, &command.scope, &command.subscription_id)?;
            if reply.generation <= command.expected_generation {
                return Err(unavailable(
                    "completion retry response did not acknowledge generation",
                )
                .into());
            }
            encode(server, reply).await
        }
        _ => Err(invalid("unsupported completion operation").into()),
    }
}

fn service(server: &Server) -> Result<&dyn CompletionService> {
    server
        .completions
        .as_deref()
        .ok_or_else(|| invalid("completion subscriptions are not configured"))
}

fn check_identity(reply: &CompletionSubscription, scope: &Scope, id: &str) -> Result<()> {
    if &reply.command.scope != scope || reply.subscription_id != id {
        return Err(unavailable(
            "completion response identity disagrees with request",
        ));
    }
    Ok(())
}

async fn encode(
    server: &Server,
    reply: CompletionSubscription,
) -> std::result::Result<Vec<u8>, Failure> {
    server
        .blocking(move || {
            reply
                .validate()
                .map_err(|_| unavailable("invalid completion subscription response"))?;
            record_command(&reply.command);
            let span = tracing::Span::current();
            span.record(
                "ledgence.completion.subscription.id",
                &reply.subscription_id,
            );
            span.record("ledgence.completion.generation", reply.generation);
            encode_bounded(&reply, COMPLETION_STATUS_MAX_BYTES)
        })
        .await
}

fn record_command(command: &CompletionSubscribeCommand) {
    record_scope(&command.scope);
    let span = tracing::Span::current();
    span.record("ledgence.completion.destination", &command.destination);
    match &command.target {
        CompletionTarget::Task { id } => {
            span.record("ledgence.task.id", id);
        }
        CompletionTarget::Workflow { id } => {
            span.record("ledgence.workflow.id", id);
        }
    }
}
