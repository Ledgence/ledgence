use super::*;
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::{Platform, ProgramManifest, PythonRuntime};

pub(super) fn scope() -> Scope {
    Scope {
        tenant_id: command().input.tenant_id,
        namespace: command().input.namespace,
    }
}
fn manifest(descriptor: &ProgramDescriptor) -> ProgramManifest {
    ProgramManifest {
        schema_version: 1,
        program: descriptor.program.clone(),
        runtime: PythonRuntime {
            kind: "python".into(),
            python: "3.12".into(),
            protocol: 1,
        },
        handler: "app:handle".into(),
        platform: Platform {
            os: "linux".into(),
            arch: "x86_64".into(),
        },
    }
}
fn version(descriptor: &ProgramDescriptor) -> ConsoleProgramVersion {
    ConsoleProgramVersion {
        descriptor: descriptor.clone().into(),
        manifest: manifest(descriptor),
        metadata: ProgramDisplayMetadata::default(),
        registered_at: 1,
        provenance: ProgramRegistrationProvenance::ConfiguredProgramStore,
    }
}
pub(super) struct Catalog {
    pub(super) descriptor: Mutex<Result<Option<ProgramDescriptor>>>,
    pub(super) reads: AtomicUsize,
    entered: tokio::sync::Notify,
    gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
}
impl Catalog {
    pub(super) fn new(descriptor: Option<ProgramDescriptor>) -> Self {
        Self {
            descriptor: Mutex::new(Ok(descriptor)),
            reads: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            gate: Mutex::new(None),
        }
    }
}
impl ProgramCatalogStore for Catalog {
    fn register_program<'a>(
        &'a self,
        _: &'a Scope,
        command: &'a RegisterProgram,
        descriptor: &'a ProgramDescriptor,
        manifest: &'a ProgramManifest,
    ) -> ContractFuture<'a, RegisterProgramReply> {
        Box::pin(async move {
            let mut version = version(descriptor);
            version.manifest = manifest.clone();
            version.metadata = command.metadata.clone();
            Ok(RegisterProgramReply {
                version,
                already_registered: false,
                metadata_updated: false,
            })
        })
    }
    fn query_programs<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a ProgramCatalogQuery,
    ) -> ContractFuture<'a, ProgramCatalogReply> {
        Box::pin(async move {
            assert_eq!(*scope, self::scope());
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            let gate = self.gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            let descriptor = self
                .descriptor
                .lock()
                .unwrap()
                .clone()?
                .ok_or(ContractError::NotFound)?;
            let ProgramCatalogQuery::Inspect(reference) = query else {
                panic!()
            };
            assert_eq!(reference, &descriptor.program);
            Ok(ProgramCatalogReply::Inspect(Box::new(
                ConsoleProgramDetail {
                    version: version(&descriptor),
                    observed_at: 2,
                },
            )))
        })
    }
}

#[tokio::test]
async fn new_task_requires_registered_digest_and_size_but_allows_unregistered_programs() {
    for (registered, resolved, expected) in [
        (Some(descriptor('a')), descriptor('b'), false),
        (Some(descriptor('a')), descriptor('a'), true),
        (None, descriptor('b'), true),
    ] {
        let mut fixture = Fixture::new();
        let catalog = Arc::new(Catalog::new(registered));
        fixture.service = fixture
            .service
            .with_program_catalog(scope(), catalog.clone())
            .unwrap();
        let pending = fixture.start(command());
        fixture
            .resolution()
            .await
            .send(Ok(resolved.clone()))
            .unwrap();
        let result = bounded(pending).await.unwrap();
        if expected {
            assert_eq!(result.unwrap().descriptor, resolved);
        } else {
            assert_eq!(result.unwrap_err(), ContractError::Conflict);
        }
        assert_eq!(
            fixture.store.accepts.load(Ordering::SeqCst),
            usize::from(expected)
        );
        assert_eq!(catalog.reads.load(Ordering::SeqCst), 1);
        assert!(fixture.requests.try_recv().is_err());
    }
    let mut fixture = Fixture::new();
    let catalog = Arc::new(Catalog::new(Some(descriptor('a'))));
    fixture.service = fixture
        .service
        .with_program_catalog(scope(), catalog)
        .unwrap();
    let pending = fixture.start(command());
    let mut changed = descriptor('a');
    changed.size += 1;
    fixture.resolution().await.send(Ok(changed)).unwrap();
    assert_eq!(
        bounded(pending).await.unwrap().unwrap_err(),
        ContractError::Conflict
    );
}

#[tokio::test]
async fn accepted_task_replays_bypass_catalog_and_catalog_failure_never_accepts_new_work() {
    let mut fixture = Fixture::new();
    let catalog = Arc::new(Catalog::new(None));
    *catalog.descriptor.lock().unwrap() = Err(ContractError::Unavailable("catalog offline".into()));
    fixture.service = fixture
        .service
        .with_program_catalog(scope(), catalog.clone())
        .unwrap();
    let pending = fixture.start(command());
    fixture
        .resolution()
        .await
        .send(Ok(descriptor('a')))
        .unwrap();
    assert!(matches!(
        bounded(pending).await.unwrap(),
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 0);
    let accepted = fixture
        .store
        .accept_resolved_submission(&command(), &descriptor('a'))
        .await
        .unwrap();
    let replay = fixture.service.submit(&command()).await.unwrap();
    assert_eq!(replay.task_id, accepted.task_id);
    assert_eq!(catalog.reads.load(Ordering::SeqCst), 1);
    assert!(fixture.requests.try_recv().is_err());
    let mut foreign = command();
    foreign.input.namespace = "foreign".into();
    assert_eq!(
        fixture.service.submit(&foreign).await.unwrap_err(),
        ContractError::NotFound
    );
}

#[tokio::test]
async fn concurrent_accepted_task_binding_wins_over_later_catalog_conflict() {
    let mut fixture = Fixture::new();
    let catalog = Arc::new(Catalog::new(Some(descriptor('a'))));
    let release = Arc::new(tokio::sync::Notify::new());
    *catalog.gate.lock().unwrap() = Some(release.clone());
    fixture.service = fixture
        .service
        .with_program_catalog(scope(), catalog.clone())
        .unwrap();
    let pending = fixture.start(command());
    fixture
        .resolution()
        .await
        .send(Ok(descriptor('b')))
        .unwrap();
    bounded(catalog.entered.notified()).await;
    let accepted = fixture
        .store
        .accept_resolved_submission(&command(), &descriptor('a'))
        .await
        .unwrap();
    release.notify_one();
    let result = bounded(pending).await.unwrap().unwrap();
    assert_eq!(result.task_id, accepted.task_id);
    assert_eq!(result.descriptor, descriptor('a'));
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 1);
}

struct FetchPrograms {
    fetches: AtomicUsize,
}
impl ProgramStore for FetchPrograms {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async { Ok(descriptor('a')) })
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(vec![1])
        })
    }
}
struct BlockingVerifier {
    entered: mpsc::UnboundedSender<()>,
    release: tokio::sync::Semaphore,
}
impl ProgramPackageVerifier for BlockingVerifier {
    fn verify_package<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> ContractFuture<'a, ProgramManifest> {
        Box::pin(async {
            self.entered.send(()).unwrap();
            let permit = self.release.acquire().await.unwrap();
            permit.forget();
            Ok(manifest(descriptor))
        })
    }
}
#[tokio::test]
async fn registration_admission_bounds_fetches_before_waiting_for_verification() {
    let (entered, mut events) = mpsc::unbounded_channel();
    let verifier = Arc::new(BlockingVerifier {
        entered,
        release: tokio::sync::Semaphore::new(0),
    });
    let programs = Arc::new(FetchPrograms {
        fetches: AtomicUsize::new(0),
    });
    let service = crate::catalog::ProgramCatalogApplicationService::new(
        scope(),
        Arc::new(Catalog::new(None)),
        programs.clone(),
        verifier.clone(),
    )
    .unwrap();
    let command = RegisterProgram {
        program: descriptor('a').program,
        metadata: ProgramDisplayMetadata::default(),
        update_metadata: false,
    };
    let mut pending = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let service = service.clone();
        let command = command.clone();
        pending.spawn(async move { service.register_program(&command).await });
    }
    bounded(events.recv()).await.unwrap();
    bounded(events.recv()).await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(programs.fetches.load(Ordering::SeqCst), 2);
    assert!(events.try_recv().is_err());
    verifier.release.add_permits(8);
    while let Some(result) = bounded(pending.join_next()).await {
        result.unwrap().unwrap();
    }
    assert_eq!(programs.fetches.load(Ordering::SeqCst), 8);
}
