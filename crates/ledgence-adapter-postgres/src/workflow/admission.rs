//! Durable resource authority: workflow -> task -> resource -> occurrence locks.
//! A committed Start is one-shot authority; its lost reply is never another grant.
mod execution;
mod policy;
mod scheduler;
mod state;
mod wait;
use super::*;
pub(super) use scheduler::drain;
use state::*;
pub(super) use wait::{apply_wait, close_wait, install_wait};
impl AdmissionOperations for PostgresStore {
    fn put_resource<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt> {
        Box::pin(self.run(move || self.put_resource_once(p, c)))
    }
    fn resource<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a ResourceQuery,
    ) -> ContractFuture<'a, ResourceSnapshot> {
        Box::pin(self.run(move || self.resource_once(p, q)))
    }
    fn list_resources<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a ResourceListQuery,
    ) -> ContractFuture<'a, ResourcePage> {
        Box::pin(self.run(move || self.resources_once(p, q)))
    }
    fn start_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt> {
        Box::pin(self.run(move || self.start_admission_once(p, c)))
    }
    fn finish_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt> {
        Box::pin(self.run(move || self.finish_admission_once(p, c)))
    }
    fn inspect_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a AdmissionQuery,
    ) -> ContractFuture<'a, AdmissionSnapshot> {
        Box::pin(self.run(move || self.inspect_admission_once(p, q)))
    }
    fn release_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a AdmissionReleaseCommand,
    ) -> ContractFuture<'a, AdmissionReleaseReceipt> {
        Box::pin(self.run(move || self.release_admission_once(p, c)))
    }
}
impl AdmissionStore for PostgresStore {
    fn advance_admissions<'a>(
        &'a self,
        q: &'a AdmissionMaintenanceQuery,
    ) -> ContractFuture<'a, AdmissionMaintenanceProgress> {
        Box::pin(self.run(move || self.advance_admissions_once(q)))
    }
    fn apply_admission_work<'a>(
        &'a self,
        w: &'a WorkflowWork,
        r: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress> {
        Box::pin(self.run(move || async move {
            self.admission_configured()?;
            self.apply_work_once(w, r).await
        }))
    }
}
impl PostgresStore {
    pub(super) fn admission_configured(&self) -> Result<&AdmissionConfig> {
        let c = self.admission_config().ok_or_else(|| {
            ContractError::Unavailable("resource admission is not configured".into())
        })?;
        c.validate_for(self.audit_audience()?)?;
        Ok(c)
    }
    fn admission_worker(
        &self,
        p: &VerifiedPrincipal,
        scope: &Scope,
        now: u64,
    ) -> Result<AdmissionWorkerIdentity> {
        self.admission_configured()?;
        self.require_scope(scope)?;
        p.require_at(self.audit_audience()?, now)?;
        let w = AdmissionWorkerIdentity::from_principal(p)?;
        if &w.audience.scope != scope {
            return Err(ContractError::Forbidden);
        }
        Ok(w)
    }
}
fn canonical<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    canonical_json_bytes(&serde_json::to_value(v).map_err(|_| corrupt("admission encoding"))?)
        .map_err(Into::into)
}
fn proof(v: Option<bool>, label: &str) -> StoreResult<()> {
    if v == Some(true) {
        Ok(())
    } else {
        Err(corrupt(label).into())
    }
}
