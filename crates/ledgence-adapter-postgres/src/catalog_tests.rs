use crate::{
    tests::{TestDb, descriptor, scope},
    *,
};
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::{Platform, ProgramManifest, PythonRuntime};
fn manifest() -> ProgramManifest {
    ProgramManifest {
        schema_version: 1,
        program: descriptor().program,
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
fn registration() -> RegisterProgram {
    RegisterProgram {
        program: descriptor().program,
        metadata: ProgramDisplayMetadata {
            display_name: Some("Invoice issuer".into()),
            description: None,
            kind: ConsoleProgramKind::Task,
        },
        update_metadata: false,
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_registration_is_atomic_idempotent_and_metadata_updates_are_explicit() {
    let db = TestDb::new().await;
    let scope = scope();
    let command = registration();
    let descriptor = descriptor();
    let manifest = manifest();
    let (a, b) = tokio::join!(
        db.store
            .register_program(&scope, &command, &descriptor, &manifest),
        db.store
            .register_program(&scope, &command, &descriptor, &manifest)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_ne!(a.already_registered, b.already_registered);
    assert_eq!(a.version.registered_at, b.version.registered_at);
    let mut changed = command.clone();
    changed.metadata.display_name = Some("New title".into());
    assert_eq!(
        db.store
            .register_program(&scope, &changed, &descriptor, &manifest)
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    changed.update_metadata = true;
    let updated = db
        .store
        .register_program(&scope, &changed, &descriptor, &manifest)
        .await
        .unwrap();
    assert!(updated.already_registered && updated.metadata_updated);
    assert_eq!(updated.version.registered_at, a.version.registered_at);
    let mut other = descriptor.clone();
    other.digest.0 = format!("sha256:{}", "f".repeat(64));
    assert_eq!(
        db.store
            .register_program(&scope, &changed, &other, &manifest)
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    let ProgramCatalogReply::Programs(page) = db
        .store
        .query_programs(
            &scope,
            &ProgramCatalogQuery::Programs(ConsolePagination::default()),
        )
        .await
        .unwrap()
    else {
        panic!("programs")
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].registered_versions.0, 1);
    assert_eq!(page.items[0].metadata, changed.metadata);
    let executions: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(executions, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_pages_use_registration_order_and_preserve_scope_parent_cursor() {
    let db = TestDb::new().await;
    let scope = scope();
    for version in ["v9", "v10", "release-z", "a"] {
        let mut command = registration();
        command.program.version = version.into();
        let mut d = descriptor();
        d.program = command.program.clone();
        let mut m = manifest();
        m.program = d.program.clone();
        db.store
            .register_program(&scope, &command, &d, &m)
            .await
            .unwrap();
    }
    let program_id = descriptor().program.id;
    sqlx::query("UPDATE console_program_versions SET registered_at_ms=1000")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let mut page = ConsolePagination {
        limit: 2,
        cursor: None,
    };
    let mut found = vec![];
    loop {
        let query = ProgramCatalogQuery::Versions {
            program_id: program_id.clone(),
            page: page.clone(),
        };
        let ProgramCatalogReply::Versions(reply) =
            db.store.query_programs(&scope, &query).await.unwrap()
        else {
            panic!("versions")
        };
        found.extend(
            reply
                .items
                .iter()
                .map(|item| item.descriptor.program.version.clone()),
        );
        let Some(cursor) = reply.next_cursor else {
            break;
        };
        page.cursor = Some(cursor);
        let foreign = Scope {
            tenant_id: "other".into(),
            namespace: scope.namespace.clone(),
        };
        assert!(
            db.store
                .query_programs(
                    &foreign,
                    &ProgramCatalogQuery::Versions {
                        program_id: program_id.clone(),
                        page: page.clone()
                    }
                )
                .await
                .is_err()
        );
    }
    assert_eq!(found, vec!["v9", "v10", "release-z", "a"]);
    assert_eq!(
        db.store
            .query_programs(
                &scope,
                &ProgramCatalogQuery::Versions {
                    program_id: "missing".into(),
                    page: ConsolePagination::default()
                }
            )
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_guards_new_tasks_and_workflows_without_rebinding_accepted_replays() {
    use ledgence_orchestration_service::ApplicationService;
    use ledgence_worker_api::{PortFuture, ProgramDescriptor, ProgramRef, ProgramStore};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct Programs {
        descriptor: Mutex<ProgramDescriptor>,
        resolutions: AtomicUsize,
    }
    impl ProgramStore for Programs {
        fn resolve<'a>(&'a self, reference: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
            Box::pin(async move {
                self.resolutions.fetch_add(1, Ordering::SeqCst);
                let descriptor = self.descriptor.lock().unwrap().clone();
                assert_eq!(*reference, descriptor.program);
                Ok(descriptor)
            })
        }
        fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
            panic!("submission never downloads packages")
        }
    }
    let db = TestDb::new().await;
    db.store
        .register_program(&scope(), &registration(), &descriptor(), &manifest())
        .await
        .unwrap();
    let programs = Arc::new(Programs {
        descriptor: Mutex::new(descriptor()),
        resolutions: AtomicUsize::new(0),
    });
    let store = Arc::new(db.store.clone());
    let service = ApplicationService::new(store.clone(), programs.clone())
        .with_workflows(store.clone())
        .with_program_catalog(scope(), store)
        .unwrap();
    let command = crate::tests::command();
    let mut workflow_command = command.clone();
    workflow_command.idempotency_key = "workflow-original".into();
    let task = service.submit(&command).await.unwrap();
    let workflow = service.submit_workflow(&workflow_command).await.unwrap();
    assert_eq!(programs.resolutions.load(Ordering::SeqCst), 2);
    programs.descriptor.lock().unwrap().digest.0 = format!("sha256:{}", "b".repeat(64));
    let task_replay = service.submit(&command).await.unwrap();
    assert_eq!(task_replay.task_id, task.task_id);
    assert_eq!(task_replay.descriptor, descriptor());
    assert_eq!(
        service
            .submit_workflow(&workflow_command)
            .await
            .unwrap()
            .workflow_id,
        workflow.workflow_id
    );
    assert_eq!(programs.resolutions.load(Ordering::SeqCst), 2);
    let mut new_task = command;
    new_task.idempotency_key = "new-task".into();
    assert_eq!(
        service.submit(&new_task).await.unwrap_err(),
        ContractError::Conflict
    );
    workflow_command.idempotency_key = "new-workflow".into();
    assert_eq!(
        service
            .submit_workflow(&workflow_command)
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    let workflows: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_runs")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!((tasks, workflows), (2, 1));
    db.finish().await;
}
