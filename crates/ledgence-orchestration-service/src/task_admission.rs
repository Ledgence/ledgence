//! Standalone task admission is a complete, independently discoverable port.
use crate::*;

impl TaskAdmissionOperations for ApplicationService {
    fn open_task_admission_session<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionSessionCommand,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            self.task_admissions()?;
            command.validate()?;
            worker(principal, &command.scope)?;
            let session = self
                .store
                .task_admissions()?
                .open_task_admission_session(principal, command)
                .await?;
            if !command.matches(&session) {
                return Err(invalid_reply());
            }
            Ok(session)
        })
    }
    fn start_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionStartCommand,
    ) -> ContractFuture<'a, TaskAdmissionStartReceipt> {
        Box::pin(async move {
            self.task_admissions()?;
            command.validate()?;
            worker(principal, &command.owner.scope)?;
            let reply = self
                .store
                .task_admissions()?
                .start_task_admission(principal, command)
                .await?;
            reply
                .validate_for(principal, command)
                .map_err(|_| invalid_reply())?;
            Ok(reply)
        })
    }
    fn close_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionCloseCommand,
    ) -> ContractFuture<'a, TaskAdmissionCloseReceipt> {
        Box::pin(async move {
            self.task_admissions()?;
            command.validate()?;
            worker(principal, &command.owner.scope)?;
            let reply = self
                .store
                .task_admissions()?
                .close_task_admission(principal, command)
                .await?;
            reply
                .validate_for(principal, command)
                .map_err(|_| invalid_reply())?;
            Ok(reply)
        })
    }
    fn inspect_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a TaskAdmissionQuery,
    ) -> ContractFuture<'a, TaskAdmissionSnapshot> {
        Box::pin(async move {
            self.task_admissions()?;
            query.validate()?;
            operator(
                principal,
                &query.occurrence.scope,
                AccessCapability::SensitiveRead,
            )?;
            let reply = self
                .store
                .task_admissions()?
                .inspect_task_admission(principal, query)
                .await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if !reply.matches(query) {
                return Err(invalid_reply());
            }
            Ok(reply)
        })
    }
    fn release_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionReleaseCommand,
    ) -> ContractFuture<'a, TaskAdmissionReleaseReceipt> {
        Box::pin(async move {
            self.task_admissions()?;
            command.validate()?;
            operator(
                principal,
                &command.occurrence.scope,
                AccessCapability::ResourceControl,
            )?;
            let reply = self
                .store
                .task_admissions()?
                .release_task_admission(principal, command)
                .await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if !reply.matches(command) {
                return Err(invalid_reply());
            }
            Ok(reply)
        })
    }
}
fn worker(p: &VerifiedPrincipal, scope: &Scope) -> Result<()> {
    AdmissionWorkerIdentity::from_principal(p)?;
    if &p.audience().scope != scope {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}
fn operator(p: &VerifiedPrincipal, scope: &Scope, capability: AccessCapability) -> Result<()> {
    p.require(capability)?;
    if p.class() == PrincipalClass::Worker || &p.audience().scope != scope {
        return Err(ContractError::Forbidden);
    }
    Ok(())
}
fn invalid_reply() -> ContractError {
    ContractError::Unavailable(
        "invalid task admission adapter response; reconcile the identical command".into(),
    )
}
