//! Authenticated provider operations. No application payload supplies authority.
use super::*;

pub(super) fn available(server: &Server) -> bool {
    effect_resolution::available(server)
        && server
            .workflows
            .as_ref()
            .is_some_and(|s| s.provider_operations().is_ok())
}

pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let principal = context.principal()?;
    if !available(server) {
        return Err(unavailable("complete provider operation capability is unavailable").into());
    }
    let service = server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow service unavailable"))?
        .provider_operations()?;
    macro_rules! command {
        ($ty:ty, $($scope:ident).+, $method:ident, $maximum:expr $(, $fence:ident)?) => {{
            let command: $ty = server.decode(bytes, PROVIDER_COMMAND_MAX_BYTES).await?;
            $(let command={let mut c=command;c.$fence=context.budget_fence;c};)?
            command.validate()?;
            server.require_scope(&command.$($scope).+)?;
            context.recheck(server)?;
            let reply = service.$method(principal, &command).await?;
            context.decision_committed();
            reply.validate_for(principal, &command).map_err(|_| unavailable(
                "invalid provider operation response; reconcile the identical command"))?;
            server.blocking(move || encode_bounded(&reply, $maximum)).await
                .map_err(|_| unavailable("provider response failed after possible commit; reconcile the identical command").into())
        }};
    }
    match path {
        "/v1/workflows/provider-operations/claim" => command!(
            ProviderClaimCommand,
            owner.scope,
            claim_provider,
            PROVIDER_RECEIPT_MAX_BYTES,
            budget_fence
        ),
        "/v1/workflows/provider-operations/report" => command!(
            ProviderDispatchReportCommand,
            owner.scope,
            report_dispatch,
            PROVIDER_RECEIPT_MAX_BYTES
        ),
        "/v1/workflows/provider-operations/observe" => command!(
            ProviderObservationCommand,
            ticket.occurrence.scope,
            observe_provider,
            PROVIDER_RECEIPT_MAX_BYTES
        ),
        "/v1/provider-probes/context" => command!(
            ProviderProbeContextCommand,
            owner.scope,
            probe_context,
            PROVIDER_CONTEXT_MAX_BYTES
        ),
        "/v1/provider-probes/report" => command!(
            ProviderProbeReportCommand,
            owner.scope,
            report_probe,
            PROVIDER_RECEIPT_MAX_BYTES
        ),
        _ => Err(ContractError::NotFound.into()),
    }
}
