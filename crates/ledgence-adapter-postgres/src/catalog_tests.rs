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
        expected_descriptor: None,
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
async fn catalog_kind_membership_tracks_all_versions_and_explicit_kind_changes() {
    let db = TestDb::new().await;
    for (version, kind) in [
        ("1", ConsoleProgramKind::Task),
        ("2", ConsoleProgramKind::Workflow),
        ("3", ConsoleProgramKind::Unspecified),
    ] {
        let mut d = descriptor();
        d.program.version = version.into();
        let mut m = manifest();
        m.program = d.program.clone();
        let c = RegisterProgram {
            expected_descriptor: None,
            program: d.program.clone(),
            metadata: ProgramDisplayMetadata {
                kind,
                ..Default::default()
            },
            update_metadata: false,
        };
        db.store
            .register_program(&scope(), &c, &d, &m)
            .await
            .unwrap();
    }
    for filter in [
        None,
        Some(ConsoleProgramKind::Task),
        Some(ConsoleProgramKind::Workflow),
        Some(ConsoleProgramKind::Unspecified),
    ] {
        let ProgramCatalogReply::Catalog(p) = db
            .store
            .query_programs(
                &scope(),
                &ProgramCatalogQuery::Catalog {
                    kind: filter,
                    page: Default::default(),
                },
            )
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(p.items.len(), 1);
        assert_eq!(
            p.items[0].kinds,
            vec![
                ConsoleProgramKind::Task,
                ConsoleProgramKind::Workflow,
                ConsoleProgramKind::Unspecified
            ]
        );
        assert_eq!(p.items[0].program.registered_versions.0, 3);
        // First registration's summary kind is not authoritative membership.
        assert_eq!(p.items[0].program.metadata.kind, ConsoleProgramKind::Task);
    }
    let mut d = descriptor();
    d.program.version = "1".into();
    let mut m = manifest();
    m.program = d.program.clone();
    let c = RegisterProgram {
        expected_descriptor: None,
        program: d.program.clone(),
        metadata: ProgramDisplayMetadata {
            kind: ConsoleProgramKind::Workflow,
            ..Default::default()
        },
        update_metadata: true,
    };
    db.store
        .register_program(&scope(), &c, &d, &m)
        .await
        .unwrap();
    let ProgramCatalogReply::Catalog(p) = db
        .store
        .query_programs(
            &scope(),
            &ProgramCatalogQuery::Catalog {
                kind: Some(ConsoleProgramKind::Task),
                page: Default::default(),
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(p.items.is_empty());
    let ProgramCatalogReply::Catalog(p) = db
        .store
        .query_programs(
            &scope(),
            &ProgramCatalogQuery::Catalog {
                kind: None,
                page: Default::default(),
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        p.items[0].kinds,
        vec![
            ConsoleProgramKind::Workflow,
            ConsoleProgramKind::Unspecified
        ]
    );
    db.finish().await;
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

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn registered_catalog_survives_task_retirement_and_progressive_purge() {
    let db = TestDb::new().await;
    let scope = scope();
    let registration = registration();
    let expected = db
        .store
        .register_program(&scope, &registration, &descriptor(), &manifest())
        .await
        .unwrap()
        .version;
    let reference = registration.program.clone();
    let input = crate::tests::command();
    let task = db
        .store
        .accept_resolved_submission(&input, &descriptor())
        .await
        .unwrap();
    db.store.cancel(&scope, &task.task_id).await.unwrap();
    // Fixture-only aging preserves the production ninety-day retention floor.
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2,cancel_requested_at_ms=2 WHERE task_id=$1")
        .bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO task_history(task_id,sequence,at_ms,reason) SELECT $1,n,2,'submitted' FROM generate_series(100,109)n")
        .bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    let policy = RetentionPolicy {
        batch_size: 1,
        ..Default::default()
    };
    let mut observed_retiring = false;
    let mut removed = false;
    // Retention rotates three lanes and discovery/collection cursors; permit
    // their bounded empty turns as well as one-row history deletion pages.
    for _ in 0..256 {
        db.store
            .retain_batch(
                &scope,
                &policy,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
        let marker: Option<Option<i64>> =
            sqlx::query_scalar("SELECT retiring_at_ms FROM tasks WHERE task_id=$1")
                .bind(&task.task_id)
                .fetch_optional(&db.store.pool)
                .await
                .unwrap();
        let ProgramCatalogReply::Inspect(detail) = db
            .store
            .query_programs(&scope, &ProgramCatalogQuery::Inspect(reference.clone()))
            .await
            .unwrap()
        else {
            panic!("wrong catalog response")
        };
        assert_eq!(
            serde_json::to_value(&detail.version).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        if marker.is_some_and(|at| at.is_some()) {
            observed_retiring = true;
            assert_eq!(
                db.store.inspect(&scope, &task.task_id).await.unwrap_err(),
                ContractError::NotFound
            );
        }
        if marker.is_none() {
            removed = true;
            break;
        }
    }
    assert!(
        observed_retiring,
        "fixture must exercise progressive retirement"
    );
    assert!(removed, "retention must physically remove the old task");
    let history: i64 = sqlx::query_scalar("SELECT count(*) FROM task_history WHERE task_id=$1")
        .bind(&task.task_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(history, 0);
    let ProgramCatalogReply::Programs(page) = db
        .store
        .query_programs(
            &scope,
            &ProgramCatalogQuery::Programs(ConsolePagination::default()),
        )
        .await
        .unwrap()
    else {
        panic!("wrong catalog response")
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].program_id, reference.id);
    assert_eq!(page.items[0].registered_versions, ConsoleU64(1));
    assert_eq!(page.items[0].metadata, expected.metadata);
    assert_eq!(page.items[0].last_registered_at, expected.registered_at);
    let replay = db
        .store
        .register_program(&scope, &registration, &descriptor(), &manifest())
        .await
        .unwrap();
    assert!(replay.already_registered);
    assert_eq!(
        serde_json::to_value(replay.version).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    db.finish().await;
}

fn maximum_metadata() -> ProgramDisplayMetadata {
    ProgramDisplayMetadata {
        display_name: Some("\\".repeat(128)),
        description: Some("\t\n\\\"".repeat(1024)),
        kind: ConsoleProgramKind::Unspecified,
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_accepts_maximum_escaped_metadata_for_registration_and_updates() {
    let db = TestDb::new().await;
    let scope = scope();
    let mut command = registration();
    command.metadata = maximum_metadata();
    command.validate().unwrap();
    assert_eq!(
        codec::encode(&command.metadata).unwrap().len(),
        PROGRAM_DISPLAY_METADATA_MAX_BYTES
    );
    let first = db
        .store
        .register_program(&scope, &command, &descriptor(), &manifest())
        .await
        .unwrap();
    assert_eq!(first.version.metadata, command.metadata);
    let replay = db
        .store
        .register_program(&scope, &command, &descriptor(), &manifest())
        .await
        .unwrap();
    assert!(replay.already_registered);
    assert!(!replay.metadata_updated);

    let mut changed = command.clone();
    changed.metadata.description = Some("\"".repeat(4096));
    assert_eq!(
        db.store
            .register_program(&scope, &changed, &descriptor(), &manifest())
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    changed.update_metadata = true;
    let updated = db
        .store
        .register_program(&scope, &changed, &descriptor(), &manifest())
        .await
        .unwrap();
    assert!(updated.already_registered && updated.metadata_updated);
    assert_eq!(updated.version.registered_at, first.version.registered_at);
    assert_eq!(updated.version.descriptor, first.version.descriptor);
    assert_eq!(updated.version.manifest, first.version.manifest);
    assert_eq!(updated.version.metadata, changed.metadata);
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
    assert_eq!(page.items[0].metadata, changed.metadata);
    let ProgramCatalogReply::Inspect(detail) = db
        .store
        .query_programs(&scope, &ProgramCatalogQuery::Inspect(command.program))
        .await
        .unwrap()
    else {
        panic!("inspect")
    };
    assert_eq!(detail.version.metadata, changed.metadata);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_version_pages_fit_byte_budget_and_traverse_all_versions() {
    use ledgence_adapter_artifact::{ArtifactLimits, publish_directory, verify_program_package};
    let db = TestDb::new().await;
    let fixture = tempfile::tempdir().unwrap();
    let source = fixture.path().join("source");
    let artifacts = fixture.path().join("store");
    std::fs::create_dir(&source).unwrap();
    let limits = ArtifactLimits::default();
    let mut references = std::collections::BTreeSet::new();
    let program_id = "p".repeat(128);
    for index in 0..101 {
        let mut command = registration();
        command.program.id = program_id.clone();
        command.program.version = format!("v{index:03}{}", "a".repeat(124));
        command.metadata = maximum_metadata();
        let mut manifest = manifest();
        manifest.program = command.program.clone();
        manifest.handler = "app:".into();
        let padding = limits.max_manifest_bytes as usize - codec::encode(&manifest).unwrap().len();
        manifest.handler.push_str(&"h".repeat(padding));
        manifest.validate().unwrap();
        let bytes = codec::encode(&manifest).unwrap();
        assert_eq!(bytes.len(), limits.max_manifest_bytes as usize);
        std::fs::write(source.join("ledgence-program.json"), bytes).unwrap();
        std::fs::write(
            source.join("app.py"),
            format!("def {}(event):\n    return None\n", &manifest.handler[4..]),
        )
        .unwrap();
        let descriptor = publish_directory(&source, &artifacts, &limits).unwrap();
        let archive = std::fs::read(
            artifacts
                .join("blobs")
                .join(format!("{}.zip", descriptor.digest.hex())),
        )
        .unwrap();
        let verified = verify_program_package(archive, &descriptor, &limits).unwrap();
        assert_eq!(verified, manifest);
        db.store
            .register_program(&scope(), &command, &descriptor, &verified)
            .await
            .unwrap();
        references.insert(command.program.version);
    }
    let mut page = ConsolePagination {
        limit: 100,
        cursor: None,
    };
    let mut seen = std::collections::BTreeSet::new();
    let mut page_count = 0;
    loop {
        let query = ProgramCatalogQuery::Versions {
            program_id: program_id.clone(),
            page: page.clone(),
        };
        let reply = db.store.query_programs(&scope(), &query).await.unwrap();
        reply.validate(&scope(), &query).unwrap();
        let ProgramCatalogReply::Versions(reply) = reply else {
            panic!("versions")
        };
        assert!(codec::encode(&reply).unwrap().len() <= CONSOLE_METADATA_MAX_BYTES);
        assert!(!reply.items.is_empty());
        assert!(
            reply.items.len() < 100,
            "large manifests must shorten the page"
        );
        for item in &reply.items {
            assert!(
                seen.insert(item.descriptor.program.version.clone()),
                "duplicate version across page boundary"
            );
            assert_eq!(item.manifest.program, item.descriptor.program);
            assert_eq!(item.metadata, maximum_metadata());
        }
        page_count += 1;
        assert!(page_count <= 101);
        let Some(cursor) = reply.next_cursor else {
            break;
        };
        page.cursor = Some(cursor);
    }
    assert_eq!(seen, references);
    assert!(page_count > 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_metadata_bound_upgrade_preserves_existing_bytes_and_enables_updates() {
    let db = TestDb::without_migrations().await;
    let previous = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20260926040000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    let mut command = registration();
    let original = db
        .store
        .register_program(&scope(), &command, &descriptor(), &manifest())
        .await
        .unwrap();
    let original_bytes: (Vec<u8>, Vec<u8>, Vec<u8>, i64) = sqlx::query_as(
        "SELECT descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms FROM console_program_versions",
    ).fetch_one(&db.store.pool).await.unwrap();
    let summary_bytes: Vec<u8> = sqlx::query_scalar("SELECT metadata_bytes FROM console_programs")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    command.metadata = maximum_metadata();
    command.update_metadata = true;
    assert!(matches!(
        db.store
            .register_program(&scope(), &command, &descriptor(), &manifest())
            .await,
        Err(ContractError::Unavailable(_))
    ));
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    let migrated_bytes: (Vec<u8>, Vec<u8>, Vec<u8>, i64) = sqlx::query_as(
        "SELECT descriptor_bytes,manifest_bytes,metadata_bytes,registered_at_ms FROM console_program_versions",
    ).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(migrated_bytes, original_bytes);
    let migrated_summary: Vec<u8> =
        sqlx::query_scalar("SELECT metadata_bytes FROM console_programs")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(migrated_summary, summary_bytes);
    let updated = db
        .store
        .register_program(&scope(), &command, &descriptor(), &manifest())
        .await
        .unwrap();
    assert!(updated.already_registered && updated.metadata_updated);
    assert_eq!(updated.version.metadata, command.metadata);
    assert_eq!(
        updated.version.registered_at,
        original.version.registered_at
    );
    assert_eq!(updated.version.descriptor, original.version.descriptor);
    assert_eq!(updated.version.manifest, original.version.manifest);
    let updated_bytes: (Vec<u8>, Vec<u8>) =
        sqlx::query_as("SELECT descriptor_bytes,manifest_bytes FROM console_program_versions")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(updated_bytes, (original_bytes.0, original_bytes.1));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn catalog_program_pages_keep_100_maximum_metadata_items_and_continue() {
    let db = TestDb::new().await;
    let mut references = Vec::new();
    for index in 0..101 {
        let mut command = registration();
        command.program.id = format!("p{index:03}{}", "p".repeat(124));
        command.program.version = "v".repeat(128);
        command.metadata = maximum_metadata();
        let mut descriptor = descriptor();
        descriptor.program = command.program.clone();
        let mut manifest = manifest();
        manifest.program = command.program.clone();
        db.store
            .register_program(&scope(), &command, &descriptor, &manifest)
            .await
            .unwrap();
        references.push(command.program.id);
    }
    let mut request = ConsolePagination {
        limit: 100,
        cursor: None,
    };
    let mut seen = Vec::new();
    for expected_items in [100, 1] {
        let query = ProgramCatalogQuery::Programs(request.clone());
        let reply = db.store.query_programs(&scope(), &query).await.unwrap();
        reply.validate(&scope(), &query).unwrap();
        let ProgramCatalogReply::Programs(reply) = reply else {
            panic!("programs")
        };
        assert_eq!(reply.items.len(), expected_items);
        assert!(codec::encode(&reply).unwrap().len() <= CONSOLE_METADATA_MAX_BYTES);
        assert_eq!(reply.next_cursor.is_some(), expected_items == 100);
        for item in reply.items {
            assert_eq!(item.metadata, maximum_metadata());
            assert_eq!(item.registered_versions, ConsoleU64(1));
            seen.push(item.program_id);
        }
        request.cursor = reply.next_cursor;
    }
    assert_eq!(seen, references);
    db.finish().await;
}
