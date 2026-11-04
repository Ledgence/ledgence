//! Authenticated standalone task admission. No route returns replayable authority.
use super::*;

pub(super) fn available(server: &Server) -> bool {
    access::control(server).is_some()
        && audit::enabled(server)
        && server.service.task_admissions().is_ok()
}
fn invalid_reply() -> ContractError {
    unavailable("invalid task admission response; reconcile the identical command")
}
pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let principal = context.principal()?;
    if !available(server) {
        return Err(unavailable("standalone task admission is unavailable").into());
    }
    let service = server.service.task_admissions()?;
    macro_rules! decode {
        ($ty:ty, $($scope:ident).+) => {{
            let command: $ty = server.decode(bytes, ADMISSION_COMMAND_MAX_BYTES).await?;
            command.validate()?;
            server.require_scope(&command.$($scope).+)?;
            context.recheck(server)?;
            command
        }};
    }
    match path {
        "/v1/task-admissions/sessions" => {
            let command = decode!(TaskAdmissionSessionCommand, scope);
            let reply = service
                .open_task_admission_session(principal, &command)
                .await?;
            context.decision_committed();
            if !command.matches(&reply) {
                return Err(invalid_reply().into());
            }
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        "/v1/task-admissions/start" => {
            let command = decode!(TaskAdmissionStartCommand, owner.scope);
            let reply = service.start_task_admission(principal, &command).await?;
            context.decision_committed();
            reply
                .validate_for(principal, &command)
                .map_err(|_| invalid_reply())?;
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        "/v1/task-admissions/close" => {
            let command = decode!(TaskAdmissionCloseCommand, owner.scope);
            let reply = service.close_task_admission(principal, &command).await?;
            context.decision_committed();
            reply
                .validate_for(principal, &command)
                .map_err(|_| invalid_reply())?;
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        "/v1/task-admissions/inspect" => {
            let query = decode!(TaskAdmissionQuery, occurrence.scope);
            let reply = service.inspect_task_admission(principal, &query).await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if !reply.matches(&query) {
                return Err(invalid_reply().into());
            }
            let encoded = server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await?;
            context.sensitive(AuditResource::TaskAdmission {
                occurrence: query.occurrence,
            })?;
            Ok(encoded)
        }
        "/v1/task-admissions/release" => {
            let command = decode!(TaskAdmissionReleaseCommand, occurrence.scope);
            let reply = service.release_task_admission(principal, &command).await?;
            context.decision_committed();
            reply.validate().map_err(|_| invalid_reply())?;
            if !reply.matches(&command) {
                return Err(invalid_reply().into());
            }
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        _ => Err(ContractError::NotFound.into()),
    }
}

pub(super) async fn console_inspect(
    server: &Server,
    bytes: Vec<u8>,
    scope: &Scope,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Query {
        task_id: String,
        generation: u32,
    }
    let query: Query = server.decode(bytes, 4096).await?;
    let query = TaskAdmissionQuery {
        occurrence: TaskAdmissionOccurrence {
            scope: scope.clone(),
            task_id: query.task_id,
            generation: query.generation,
        },
    };
    query.validate()?;
    post(
        server,
        "/v1/task-admissions/inspect",
        encode_bounded(&query, ADMISSION_COMMAND_MAX_BYTES)?,
        context,
    )
    .await
}
