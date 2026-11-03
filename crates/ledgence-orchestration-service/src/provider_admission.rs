//! A composed capability must be supplied by one atomic store implementation.
use crate::*;
impl ProviderAdmissionOperations for ApplicationService {
    fn start_provider_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt> {
        Box::pin(async move {
            self.provider_admissions()?;
            command.validate()?;
            require_worker(principal, &command.admission.occurrence.scope)?;
            let receipt = self
                .workflows()?
                .provider_admissions()?
                .start_provider_admission(principal, command)
                .await?;
            receipt.validate_for(principal, command)?;
            Ok(receipt)
        })
    }
    fn report_provider_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt> {
        Box::pin(async move {
            self.provider_admissions()?;
            command.validate()?;
            require_worker(principal, &command.source.admission.occurrence.scope)?;
            let receipt = self
                .workflows()?
                .provider_admissions()?
                .report_provider_admission(principal, command)
                .await?;
            receipt.validate_for(principal, command)?;
            Ok(receipt)
        })
    }
}
fn require_worker(p: &VerifiedPrincipal, scope: &Scope) -> Result<()> {
    AdmissionWorkerIdentity::from_principal(p)?;
    if &p.audience().scope != scope {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}
