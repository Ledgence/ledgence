//! Provider recovery owns immutable callback sources and finite, event-driven probes.
//! Live mutations lock workflow -> active task -> retained occurrence -> source.
//! Protected admission additionally locks capacity before retained effect evidence.
mod admission;
mod claim;
mod reports;
pub(super) mod scheduler;
pub(super) mod state;
mod wait;
use super::effect_retained::{self as retained, Retained};
use super::*;
pub(super) use scheduler::{
    close_for_assessment, drain, finalize, lock_drain_tasks, terminal_obligation,
};
pub(super) use state::lock_active;
use state::*;
pub(super) use wait::{apply_wait, close_wait, install_wait};
impl ProviderOperations for PostgresStore {
    fn claim_provider<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt> {
        Box::pin(self.run(move || self.claim_provider_once(p, c)))
    }
    fn report_dispatch<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(self.run(move || self.provider_report_once(p, c, Some(&c.owner))))
    }
    fn report_probe<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(self.run(move || self.provider_report_once(p, c, Some(&c.owner))))
    }
    fn observe_provider<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(self.run(move || self.provider_report_once(p, c, None)))
    }
    fn probe_context<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext> {
        Box::pin(self.run(move || self.provider_context_once(p, c)))
    }
}
impl ProviderOperationStore for PostgresStore {
    fn advance_provider_operations<'a>(
        &'a self,
        q: &'a ProviderMaintenanceQuery,
    ) -> ContractFuture<'a, ProviderMaintenanceProgress> {
        Box::pin(self.run(move || self.advance_provider_once(q)))
    }
    fn apply_provider_work<'a>(
        &'a self,
        w: &'a WorkflowWork,
        r: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress> {
        Box::pin(self.run(move || async move {
            self.provider_configured()?;
            self.apply_work_once(w, r).await
        }))
    }
}
impl PostgresStore {
    pub(super) fn provider_configured(&self) -> Result<&ProviderOperationConfig> {
        let c = self.provider_operation_config().ok_or_else(|| {
            ContractError::Unavailable("provider operations are not configured".into())
        })?;
        c.validate_for(self.audit_audience()?)?;
        Ok(c)
    }
}
fn canonical<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    canonical_json_bytes(&serde_json::to_value(v).map_err(|_| corrupt("provider encoding"))?)
        .map_err(Into::into)
}
fn proof(value: Option<bool>, label: &str) -> StoreResult<()> {
    if value == Some(true) {
        Ok(())
    } else {
        Err(corrupt(label).into())
    }
}
fn ticket(r: &Retained) -> Result<ProviderOperationTicket> {
    Ok(ProviderOperationTicket {
        occurrence: r.binding.occurrence.clone(),
        binding_digest: binding(r)?.digest(&r.binding.approval.action)?,
    })
}
fn binding(r: &Retained) -> Result<&ProviderBinding> {
    r.binding
        .approval
        .provider_binding
        .as_ref()
        .ok_or_else(|| corrupt("missing provider binding"))
}
fn summary(r: &Retained) -> Result<&ProviderEffectSummary> {
    r.summary
        .provider
        .as_ref()
        .ok_or_else(|| corrupt("missing provider summary"))
}
fn summary_mut(r: &mut Retained) -> Result<&mut ProviderEffectSummary> {
    r.summary
        .provider
        .as_mut()
        .ok_or_else(|| corrupt("missing provider summary"))
}
pub(super) async fn detail(
    tx: &mut PgConnection,
    r: &Retained,
) -> StoreResult<Option<ProviderEffectDetail>> {
    if r.summary.provider.is_none() {
        return Ok(None);
    }
    Ok(Some(ProviderEffectDetail {
        current_report: load(tx, r).await?.current_report.map(Box::new),
    }))
}
