//! Generic resource authority, independent of provider/effect capabilities.
use super::*;

pub(super) fn available(server: &Server) -> bool {
    access::control(server).is_some()
        && audit::enabled(server)
        && server
            .workflows
            .as_ref()
            .is_some_and(|s| s.admissions().is_ok() && s.operator_access().is_ok())
}
fn invalid_reply() -> ContractError {
    unavailable("invalid admission response; reconcile the identical command")
}
pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let principal = context.principal()?;
    if !available(server) {
        return Err(unavailable("durable resource admission is unavailable").into());
    }
    let service = server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow service unavailable"))?
        .admissions()?;
    macro_rules! decode {
        ($ty:ty, $($scope:ident).+) => {{
            let command: $ty = server.decode(bytes, ADMISSION_COMMAND_MAX_BYTES).await?;
            command.validate()?;
            server.require_scope(&command.$($scope).+)?;
            context.recheck(server)?;
            command
        }};
    }
    macro_rules! mutate {
        ($ty:ty, $($scope:ident).+, $method:ident) => {{
            let command = decode!($ty, $($scope).+);
            let reply = service.$method(principal, &command).await?;
            context.decision_committed();
            reply.validate().map_err(|_| invalid_reply())?;
            if !reply.matches(&command) { return Err(invalid_reply().into()); }
            server.blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES)).await
        }};
    }
    match path {
        "/v1/resources/put" => mutate!(ResourcePutCommand, scope, put_resource),
        "/v1/admissions/release" => {
            mutate!(AdmissionReleaseCommand, occurrence.scope, release_admission)
        }
        "/v1/resources/query" => {
            let query = decode!(ResourceQuery, scope);
            let reply = service.resource(principal, &query).await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if reply.resource.scope != query.scope || reply.resource.name != query.name {
                return Err(invalid_reply().into());
            }
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        "/v1/resources/list" => {
            let query = decode!(ResourceListQuery, scope);
            let reply = service.list_resources(principal, &query).await?;
            reply.validate_for(&query).map_err(|_| invalid_reply())?;
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_PAGE_MAX_BYTES))
                .await
        }
        "/v1/admissions/start" => {
            let command = decode!(AdmissionStartCommand, owner.scope);
            let reply = service.start_admission(principal, &command).await?;
            context.decision_committed();
            reply
                .validate_for(principal, &command)
                .map_err(|_| invalid_reply())?;
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        "/v1/admissions/finish" => {
            let command = decode!(AdmissionFinishCommand, source.occurrence.scope);
            let reply = service.finish_admission(principal, &command).await?;
            context.decision_committed();
            reply
                .validate_for(principal, &command)
                .map_err(|_| invalid_reply())?;
            server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await
        }
        "/v1/admissions/inspect" => {
            let query = decode!(AdmissionQuery, occurrence.scope);
            let reply = service.inspect_admission(principal, &query).await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if reply.occurrence != query.occurrence {
                return Err(invalid_reply().into());
            }
            let encoded = server
                .blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES))
                .await?;
            context.sensitive(AuditResource::Admission {
                occurrence: query.occurrence,
            })?;
            Ok(encoded)
        }
        _ => Err(ContractError::NotFound.into()),
    }
}

/// Console requests never choose a tenant; the installation binds their scope.
pub(super) async fn console_inspect(
    server: &Server,
    bytes: Vec<u8>,
    scope: &Scope,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Query {
        workflow_id: String,
        origin_activation_id: String,
        key: String,
    }
    let q: Query = server.decode(bytes, 4096).await?;
    let q = AdmissionQuery {
        occurrence: AdmissionOccurrence {
            scope: scope.clone(),
            workflow_id: q.workflow_id,
            origin_activation_id: q.origin_activation_id,
            key: q.key,
        },
    };
    q.validate()?;
    let bytes = encode_bounded(&q, ADMISSION_COMMAND_MAX_BYTES)?;
    post(server, "/v1/admissions/inspect", bytes, context).await
}
