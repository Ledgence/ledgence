use crate::*;
impl EffectResolution for ApplicationService {
    fn resolve_effect<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, AuditedEffectResolution> {
        Box::pin(async move {
            c.validate()?;
            p.require(AccessCapability::EffectResolve)?;
            let proof = self
                .workflows()?
                .effect_resolution()?
                .resolve_effect(p, c)
                .await?;
            proof.validate_for(p, c)?;
            Ok(proof)
        })
    }
}
