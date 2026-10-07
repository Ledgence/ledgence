//! Explicit registration against the configured program store, outside DB locks.
use ledgence_orchestration_api::{console::*, *};
use ledgence_worker_api::ProgramStore;
use std::sync::Arc;

#[derive(Clone)]
pub struct ProgramCatalogApplicationService {
    scope: Scope,
    store: Arc<dyn ProgramCatalogStore>,
    programs: Arc<dyn ProgramStore>,
    verifier: Arc<dyn ProgramPackageVerifier>,
    registrations: Arc<tokio::sync::Semaphore>,
}
impl ProgramCatalogApplicationService {
    pub fn new(
        scope: Scope,
        store: Arc<dyn ProgramCatalogStore>,
        programs: Arc<dyn ProgramStore>,
        verifier: Arc<dyn ProgramPackageVerifier>,
    ) -> Result<Self> {
        scope.validate()?;
        Ok(Self {
            scope,
            store,
            programs,
            verifier,
            registrations: Arc::new(tokio::sync::Semaphore::new(2)),
        })
    }
}
impl ProgramCatalogService for ProgramCatalogApplicationService {
    fn register_program<'a>(
        &'a self,
        command: &'a RegisterProgram,
    ) -> ContractFuture<'a, RegisterProgramReply> {
        Box::pin(async move {
            command.validate()?;
            // Hold a shared registration slot before fetch allocates an archive,
            // including while bounded package verification waits for CPU work.
            let _permit = self.registrations.acquire().await.map_err(|_| {
                ContractError::Unavailable("catalog registration unavailable".into())
            })?;
            let descriptor = self
                .programs
                .resolve(&command.program)
                .await
                .map_err(super::resolution_error)?;
            descriptor.validate()?;
            if descriptor.program != command.program {
                return Err(ContractError::Unavailable(
                    "program store returned another reference".into(),
                ));
            }
            if command
                .expected_descriptor
                .as_ref()
                .is_some_and(|expected| expected != &descriptor)
            {
                return Err(ContractError::InvalidInput(
                    "configured store descriptor does not match published artifact".into(),
                ));
            }
            let bytes = self
                .programs
                .fetch(&descriptor)
                .await
                .map_err(super::resolution_error)?;
            let manifest = self.verifier.verify_package(&descriptor, bytes).await?;
            manifest.validate()?;
            if manifest.program != command.program {
                return Err(ContractError::Unavailable(
                    "package verifier returned another reference".into(),
                ));
            }
            let reply = self
                .store
                .register_program(&self.scope, command, &descriptor, &manifest)
                .await?;
            reply.version.validate().map_err(|_| {
                ContractError::Unavailable("catalog returned inconsistent registration".into())
            })?;
            if reply.version.descriptor != ConsoleProgramDescriptor::from(descriptor)
                || reply.version.manifest != manifest
                || reply.version.metadata != command.metadata
            {
                return Err(ContractError::Unavailable(
                    "catalog returned inconsistent registration".into(),
                ));
            }
            Ok(reply)
        })
    }
    fn query_programs<'a>(
        &'a self,
        query: &'a ProgramCatalogQuery,
    ) -> ContractFuture<'a, ProgramCatalogReply> {
        Box::pin(async move {
            query.validate(&self.scope)?;
            let reply = self.store.query_programs(&self.scope, query).await?;
            reply.validate(&self.scope, query)?;
            Ok(reply)
        })
    }
}
