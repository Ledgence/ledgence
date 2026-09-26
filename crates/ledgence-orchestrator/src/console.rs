//! Console composition selects adapters; query/application ports remain portable.
use ledgence_adapter_artifact::{ArtifactLimits, verify_program_package};
use ledgence_adapter_http::server::console::ConsoleServices;
use ledgence_adapter_postgres::PostgresStore;
use ledgence_orchestration_api::{
    ContractError, ContractFuture, SelfHostedInstanceConfig, console::ProgramPackageVerifier,
};
use ledgence_orchestration_service::{
    catalog::ProgramCatalogApplicationService, console::ConsoleApplicationService,
    worker_observations::WorkerObservationApplicationService,
};
use ledgence_worker_api::{ProgramDescriptor, ProgramManifest, ProgramStore};
use std::sync::Arc;

struct PackageVerifier {
    slots: Arc<tokio::sync::Semaphore>,
}
impl ProgramPackageVerifier for PackageVerifier {
    fn verify_package<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
        bytes: Vec<u8>,
    ) -> ContractFuture<'a, ProgramManifest> {
        Box::pin(async move {
            let permit =
                self.slots.clone().acquire_owned().await.map_err(|_| {
                    ContractError::Unavailable("package verifier unavailable".into())
                })?;
            let descriptor = descriptor.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                verify_program_package(bytes, &descriptor, &ArtifactLimits::default()).map_err(
                    |_| {
                        ContractError::InvalidInput(
                            "program package failed archive verification".into(),
                        )
                    },
                )
            })
            .await
            .map_err(|_| ContractError::Unavailable("package verifier failed".into()))?
        })
    }
}
pub fn services(
    store: Arc<PostgresStore>,
    programs: Arc<dyn ProgramStore>,
    mut config: SelfHostedInstanceConfig,
    bind: std::net::SocketAddr,
) -> Result<ConsoleServices, String> {
    // Default local origins are derived from the actual bound port. Remote
    // reverse proxies must configure their exact public origin explicitly.
    if config.allowed_origins.is_empty() && bind.ip().is_loopback() {
        config.allowed_origins.push(format!("http://{bind}"));
        config
            .allowed_origins
            .push(format!("http://localhost:{}", bind.port()));
    }
    let query = Arc::new(
        ConsoleApplicationService::new(store.clone(), config.scope.clone())
            .map_err(|e| e.to_string())?,
    );
    let verifier = Arc::new(PackageVerifier {
        slots: Arc::new(tokio::sync::Semaphore::new(2)),
    });
    let catalog = Arc::new(
        ProgramCatalogApplicationService::new(
            config.scope.clone(),
            store.clone(),
            programs,
            verifier,
        )
        .map_err(|error| error.to_string())?,
    );
    let observations = Arc::new(
        WorkerObservationApplicationService::new(config.scope.clone(), store)
            .map_err(|e| e.to_string())?,
    );
    ConsoleServices::new(
        config,
        env!("CARGO_PKG_VERSION").into(),
        query,
        Some(catalog),
        Some(observations),
    )
    .map_err(|e| e.to_string())
}
