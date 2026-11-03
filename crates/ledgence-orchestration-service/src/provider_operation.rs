//! Authenticated provider operations over one complete transactional capability.
use crate::*;
impl ApplicationService {
    pub async fn coordinate_provider_operations(
        &self,
        query: &ProviderMaintenanceQuery,
    ) -> Result<ProviderMaintenanceProgress> {
        query.validate()?;
        self.effect_resolution()?;
        let progress = self
            .workflows()?
            .provider_operations()?
            .advance_provider_operations(query)
            .await?;
        if progress.examined > query.limit
            || progress
                .allocated
                .saturating_add(progress.stopped)
                .saturating_add(progress.skipped_busy)
                > progress.examined
        {
            return Err(ContractError::Unavailable(
                "invalid provider maintenance response".into(),
            ));
        }
        if let Some(cursor) = &progress.next_cursor {
            ProviderMaintenanceQuery {
                cursor: Some(cursor.clone()),
                limit: 1,
            }
            .validate()
            .map_err(|_| {
                ContractError::Unavailable("invalid provider maintenance cursor".into())
            })?;
        }
        Ok(progress)
    }
}
impl ProviderOperations for ApplicationService {
    fn claim_provider<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt> {
        Box::pin(async move {
            command.validate()?;
            self.effect_resolution()?;
            require_worker(principal, &command.owner.scope)?;
            let reply = self
                .workflows()?
                .provider_operations()?
                .claim_provider(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
    fn report_dispatch<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move {
            command.validate()?;
            self.effect_resolution()?;
            require_worker(principal, &command.ticket.occurrence.scope)?;
            let reply = self
                .workflows()?
                .provider_operations()?
                .report_dispatch(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
    fn probe_context<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext> {
        Box::pin(async move {
            command.validate()?;
            self.effect_resolution()?;
            require_worker(principal, &command.owner.scope)?;
            let reply = self
                .workflows()?
                .provider_operations()?
                .probe_context(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
    fn report_probe<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move {
            command.validate()?;
            self.effect_resolution()?;
            require_worker(principal, &command.ticket.occurrence.scope)?;
            let reply = self
                .workflows()?
                .provider_operations()?
                .report_probe(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
    fn observe_provider<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move {
            command.validate()?;
            self.effect_resolution()?;
            require_worker(principal, &command.ticket.occurrence.scope)?;
            let reply = self
                .workflows()?
                .provider_operations()?
                .observe_provider(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
}
fn require_worker(principal: &VerifiedPrincipal, scope: &Scope) -> Result<()> {
    EffectWorkerIdentity::from_principal(principal)?;
    if &principal.audience().scope != scope {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}
