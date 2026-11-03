//! Recovery of partial program lookup across independent coordinator claims.
use super::*;
use ledgence_orchestration_service::ApplicationService;
use ledgence_worker_api::{PortFuture, ProgramRef, ProgramStore};
use tokio::sync::{mpsc, oneshot};

type Lookup = (
    ProgramRef,
    oneshot::Sender<ledgence_worker_api::Result<ProgramDescriptor>>,
);

struct Programs(mpsc::UnboundedSender<Lookup>);
impl ProgramStore for Programs {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async move {
            let (send, response) = oneshot::channel();
            self.0.send((program.clone(), send)).unwrap();
            response.await.unwrap()
        })
    }

    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        panic!("coordinators only resolve descriptors");
    }
}

fn coordinator(store: &PostgresStore) -> (ApplicationService, mpsc::UnboundedReceiver<Lookup>) {
    let (requests, receive) = mpsc::unbounded_channel();
    let store = std::sync::Arc::new(store.clone());
    let service = ApplicationService::new(store.clone(), std::sync::Arc::new(Programs(requests)))
        .with_workflows(store);
    (service, receive)
}

fn resolve_ok((program, reply): Lookup) -> ProgramRef {
    let mut value = descriptor();
    value.program = program.clone();
    reply.send(Ok(value)).unwrap();
    program
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn new_coordinator_resumes_successful_resolutions_after_partial_failure() {
    let db = TestDb::new().await;
    start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    let commands: Vec<_> = (0..5)
        .map(|n| {
            let key = format!("tool-{n}");
            let mut cmd = child(&key);
            cmd.program.id = key;
            cmd
        })
        .collect();
    let until = commands.iter().map(|c| c.key.clone()).collect();
    let result = decision(
        &controller,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "collect".into(),
            commands,
            until,
        },
    );
    report(
        &db.store,
        &controller,
        serde_json::to_value(result).unwrap(),
    )
    .await;
    let (first, mut requests) = coordinator(&db.store);
    let attempt = tokio::spawn(async move { first.advance_workflows(1).await });
    let mut wave = Vec::new();
    // All four slots are occupied. Issuing a fifth lookup proves the first
    // successful result was processed before we induce a failure.
    for _ in 0..4 {
        wave.push(crate::tests::bounded(requests.recv()).await.unwrap());
    }
    let pinned = resolve_ok(wave.pop().unwrap());
    let (_, failure) = crate::tests::bounded(requests.recv()).await.unwrap();
    failure
        .send(Err(Error::new(ErrorKind::Unavailable, "test outage")))
        .unwrap();
    let progress = crate::tests::bounded(attempt).await.unwrap().unwrap();
    assert_eq!(progress.children_scheduled, 0);
    drop(wave);
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE processed_at_ms IS NULL")
        .execute(&reopened.pool)
        .await
        .unwrap();
    let (second, mut requests) = coordinator(&reopened);
    let attempt = tokio::spawn(async move { second.advance_workflows(1).await });
    for _ in 0..4 {
        assert_ne!(
            resolve_ok(crate::tests::bounded(requests.recv()).await.unwrap()),
            pinned
        );
    }
    let progress = crate::tests::bounded(attempt).await.unwrap().unwrap();
    assert_eq!(progress.children_scheduled, 5);
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn partial_resolution_is_fenced_and_survives_reclaim_without_scheduling_children() {
    let mut db = TestDb::without_migrations().await;
    let previous = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20261102000000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    db.begin_legacy_workflow_seed().await;
    let workflow = start(&db.store).await;
    let controller = acquire(&db.store, "python").await;
    let children = vec![child("first"), child("second")];
    let decision = decision(
        &controller,
        0,
        WorkflowAction::Suspend {
            state: Value::Null,
            continuation: "collect".into(),
            commands: children,
            until: vec!["first".into(), "second".into()],
        },
    );
    report(
        &db.store,
        &controller,
        serde_json::to_value(decision).unwrap(),
    )
    .await;
    // Upgrade a pending controller decision from the previous schema.
    db.finish_legacy_workflow_seed().await;
    db.store.migrate().await.unwrap();
    // Upgrade deploys stop old writers and reopen pools with the new row shape.
    db.store.close().await;
    db.store = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let old = one_work(&db.store).await;
    let mut wrong_kind = resolved("first");
    wrong_kind.kind = WorkflowChildKind::Workflow;
    let mut wrong_program = resolved("second");
    wrong_program.descriptor.program.version = "other".into();
    for bad in [resolved("unknown"), wrong_kind, wrong_program] {
        assert!(matches!(
            db.store
                .retry_work_with_bindings(&old, "invalid", &[bad])
                .await,
            Err(ContractError::Conflict)
        ));
    }

    db.store
        .retry_work_with_bindings(&old, "slow store", &[resolved("first")])
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .retry_work_with_bindings(&old, "late", &[resolved("second")])
            .await,
        Err(ContractError::OwnershipLost)
    ));
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_task_links WHERE workflow_id=$1 AND NOT is_activation",
    )
    .bind(&workflow.workflow_id)
    .fetch_one(&reopened.pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    sqlx::query("UPDATE workflow_work SET available_at_ms=0 WHERE id=$1")
        .bind(&old.id)
        .execute(&reopened.pool)
        .await
        .unwrap();
    let next = one_work(&reopened).await;
    assert_eq!(next.resolved_children, [resolved("first")]);
    let mut changed = resolved("first");
    changed.descriptor.digest.0 = format!("sha256:{}", "b".repeat(64));
    assert!(matches!(
        reopened
            .retry_work_with_bindings(&next, "different binding", &[changed])
            .await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(
        reopened
            .apply_work(&next, &[resolved("second")])
            .await
            .unwrap()
            .children_scheduled,
        2
    );
    let cleared: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (SELECT 1 FROM workflow_resolution_progress WHERE work_id=$1)",
    )
    .bind(&old.id)
    .fetch_one(&reopened.pool)
    .await
    .unwrap();
    assert!(cleared);
    reopened.close().await;
    db.finish().await;
}
