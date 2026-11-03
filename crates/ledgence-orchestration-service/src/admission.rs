//! The admission capability is complete and independent of approved effects.
use crate::*;
impl ApplicationService {
    pub async fn coordinate_admissions(
        &self,
        q: &AdmissionMaintenanceQuery,
    ) -> Result<AdmissionMaintenanceProgress> {
        q.validate()?;
        self.admissions()?;
        let r = self
            .workflows()?
            .admissions()?
            .advance_admissions(q)
            .await?;
        if r.examined > q.limit
            || r.offered > r.resumed
            || r.resumed > r.examined
            || r.expired > r.examined
            || r.offered.saturating_add(r.expired) > r.examined
        {
            return Err(ContractError::Unavailable(
                "invalid admission maintenance reply".into(),
            ));
        }
        Ok(r)
    }
}
impl AdmissionOperations for ApplicationService {
    fn put_resource<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt> {
        Box::pin(async move {
            self.admissions()?;
            c.validate()?;
            operator(p, &c.scope, AccessCapability::ResourceControl)?;
            let r = self.workflows()?.admissions()?.put_resource(p, c).await?;
            r.validate().map_err(|_| invalid_reply())?;
            if !r.matches(c) {
                return Err(invalid_reply());
            }
            Ok(r)
        })
    }
    fn resource<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a ResourceQuery,
    ) -> ContractFuture<'a, ResourceSnapshot> {
        Box::pin(async move {
            self.admissions()?;
            q.validate()?;
            operator(p, &q.scope, AccessCapability::MetadataRead)?;
            let r = self.workflows()?.admissions()?.resource(p, q).await?;
            r.validate().map_err(|_| invalid_reply())?;
            if r.resource.scope != q.scope || r.resource.name != q.name {
                return Err(invalid_reply());
            }
            Ok(r)
        })
    }
    fn list_resources<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a ResourceListQuery,
    ) -> ContractFuture<'a, ResourcePage> {
        Box::pin(async move {
            self.admissions()?;
            q.validate()?;
            operator(p, &q.scope, AccessCapability::MetadataRead)?;
            let r = self.workflows()?.admissions()?.list_resources(p, q).await?;
            r.validate_for(q).map_err(|_| invalid_reply())?;
            Ok(r)
        })
    }
    fn start_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt> {
        Box::pin(async move {
            self.admissions()?;
            c.validate()?;
            worker(p, &c.occurrence.scope)?;
            let r = self
                .workflows()?
                .admissions()?
                .start_admission(p, c)
                .await?;
            r.validate_for(p, c).map_err(|_| invalid_reply())?;
            Ok(r)
        })
    }
    fn finish_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt> {
        Box::pin(async move {
            self.admissions()?;
            c.validate()?;
            worker(p, &c.source.occurrence.scope)?;
            let r = self
                .workflows()?
                .admissions()?
                .finish_admission(p, c)
                .await?;
            r.validate_for(p, c).map_err(|_| invalid_reply())?;
            Ok(r)
        })
    }
    fn inspect_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a AdmissionQuery,
    ) -> ContractFuture<'a, AdmissionSnapshot> {
        Box::pin(async move {
            self.admissions()?;
            q.validate()?;
            operator(p, &q.occurrence.scope, AccessCapability::SensitiveRead)?;
            let r = self
                .workflows()?
                .admissions()?
                .inspect_admission(p, q)
                .await?;
            r.validate().map_err(|_| invalid_reply())?;
            if !r.matches(q) {
                return Err(invalid_reply());
            }
            Ok(r)
        })
    }
    fn release_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a AdmissionReleaseCommand,
    ) -> ContractFuture<'a, AdmissionReleaseReceipt> {
        Box::pin(async move {
            self.admissions()?;
            c.validate()?;
            operator(p, &c.occurrence.scope, AccessCapability::ResourceControl)?;
            let r = self
                .workflows()?
                .admissions()?
                .release_admission(p, c)
                .await?;
            r.validate().map_err(|_| invalid_reply())?;
            if !r.matches(c) {
                return Err(invalid_reply());
            }
            Ok(r)
        })
    }
}
fn operator(p: &VerifiedPrincipal, scope: &Scope, capability: AccessCapability) -> Result<()> {
    p.require(capability)?;
    if p.class() == PrincipalClass::Worker || &p.audience().scope != scope {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}
fn worker(p: &VerifiedPrincipal, scope: &Scope) -> Result<()> {
    AdmissionWorkerIdentity::from_principal(p)?;
    if &p.audience().scope != scope {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}

fn invalid_reply() -> ContractError {
    ContractError::Unavailable("invalid resource admission adapter response".into())
}
