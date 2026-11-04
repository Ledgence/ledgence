//! Composed authority is explicit; two independent capabilities cannot grant it.
use super::*;

pub(super) fn available(server: &Server) -> bool {
    provider::available(server)
        && admission::available(server)
        && server
            .workflows
            .as_ref()
            .is_some_and(|s| s.provider_admissions().is_ok())
}

pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let principal = context.principal()?;
    if !available(server) {
        return Err(unavailable("atomic provider admission is unavailable").into());
    }
    let service = server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow service unavailable"))?
        .provider_admissions()?;
    macro_rules! command {
        ($ty:ty, $($scope:ident).+, $method:ident) => {{
            let command: $ty = server.decode(bytes, PROVIDER_COMMAND_MAX_BYTES).await?;
            command.validate()?;
            server.require_scope(&command.$($scope).+)?;
            context.recheck(server)?;
            let reply = service.$method(principal, &command).await?;
            context.decision_committed();
            reply.validate_for(principal, &command).map_err(|_| unavailable("invalid provider admission reply after possible commit; reconcile identical evidence"))?;
            server.blocking(move || encode_bounded(&reply, ADMISSION_RESPONSE_MAX_BYTES)).await
                .map_err(|_| unavailable("provider admission reply failed after possible commit").into())
        }};
    }
    match path {
        "/v1/provider-admissions/start" => command!(
            ProviderAdmissionStartCommand,
            admission.owner.scope,
            start_provider_admission
        ),
        "/v1/provider-admissions/report" => command!(
            ProviderAdmissionReportCommand,
            source.admission.occurrence.scope,
            report_provider_admission
        ),
        _ => Err(ContractError::NotFound.into()),
    }
}
