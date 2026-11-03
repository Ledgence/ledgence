//! Compose complete verified evidence operations and reject inconsistent adapter replies.
use crate::*;
impl ApprovedEffectEvidence for ApplicationService {
    fn claim_v2<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        Box::pin(async move {
            command.validate()?;
            require_worker(principal, &command.owner.scope)?;
            let reply = self
                .workflows()?
                .approved_effect_evidence()?
                .claim_v2(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
    fn confirm_v2<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        Box::pin(async move {
            command.validate()?;
            require_worker(principal, &command.owner.scope)?;
            let reply = self
                .workflows()?
                .approved_effect_evidence()?
                .confirm_v2(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
    fn observe<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        Box::pin(async move {
            command.validate()?;
            require_worker(principal, &command.occurrence.scope)?;
            let reply = self
                .workflows()?
                .approved_effect_evidence()?
                .observe(principal, command)
                .await?;
            reply.validate_for(principal, command)?;
            Ok(reply)
        })
    }
}
impl EffectEvidenceReader for ApplicationService {
    fn effect_inbox<'a>(
        &'a self,
        query: &'a EffectInboxQuery,
    ) -> ContractFuture<'a, EffectInboxPage> {
        Box::pin(async move {
            query.validate()?;
            let reply = self
                .workflows()?
                .effect_evidence_reader()?
                .effect_inbox(query)
                .await?;
            reply.validate_for(query).map_err(|_| bad_reply())?;
            Ok(reply)
        })
    }
    fn inspect_effect<'a>(
        &'a self,
        query: &'a EffectInspectQuery,
    ) -> ContractFuture<'a, EffectEvidenceDetail> {
        Box::pin(async move {
            query.validate()?;
            let reply = self
                .workflows()?
                .effect_evidence_reader()?
                .inspect_effect(query)
                .await?;
            reply.validate_for(query).map_err(|_| bad_reply())?;
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
fn bad_reply() -> ContractError {
    ContractError::Unavailable("invalid approved effect evidence response".into())
}
