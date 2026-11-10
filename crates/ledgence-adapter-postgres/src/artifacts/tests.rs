//! Production catalog tests, each running against its own migrated PostgreSQL DB.
use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, descriptor, scope as test_scope};
use ledgence_worker_api::Digest;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
fn identity() -> ArtifactStoreIdentity {
    ArtifactStoreIdentity {
        store_id: "artifacts".into(),
        fingerprint: Digest(format!("sha256:{}", "a".repeat(64))),
    }
}
fn metadata() -> ArtifactMetadata {
    ArtifactMetadata {
        digest: Digest(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        ),
        size: 0,
        media_type: "application/octet-stream".into(),
        filename: Some("empty.bin".into()),
    }
}
async fn fixture() -> (TestDb, ArtifactAuthority, ArtifactPrepareCommand) {
    let db = TestDb::new().await;
    let config = SelfHostedInstanceConfig {
        instance_id: "artifacts-test".into(),
        name: "Artifacts".into(),
        scope: test_scope(),
        suggested_queues: vec![],
        allowed_origins: vec![],
    };
    let instance = db
        .store
        .initialize_instance(Some(&config))
        .await
        .unwrap()
        .unwrap();
    db.store.bind_store(&identity(), deadline()).await.unwrap();
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let c = ArtifactPrepareCommand {
        scope: test_scope(),
        target: ExecutionTarget::Task { id: task.task_id },
        key: "report".into(),
        metadata: metadata(),
    };
    (
        db,
        ArtifactAuthority::External(ArtifactCaller::TrustedInstance(instance)),
        c,
    )
}
async fn prepare(
    store: &PostgresStore,
    a: &ArtifactAuthority,
    c: &ArtifactPrepareCommand,
) -> ArtifactSelection {
    store
        .prepare(a, c, deadline())
        .await
        .unwrap()
        .upload
        .selection
}
async fn writing(
    store: &PostgresStore,
    a: &ArtifactAuthority,
    s: &ArtifactSelection,
) -> ArtifactIngressClaim {
    let claim = store.claim_ingress(a, s, deadline()).await.unwrap();
    assert!(matches!(
        store.begin_write(a, &claim, deadline()).await.unwrap(),
        ArtifactBeginWrite::Dispatch(_)
    ));
    claim
}
async fn accepted(
    store: &PostgresStore,
    a: &ArtifactAuthority,
    s: &ArtifactSelection,
) -> ArtifactManifest {
    writing(store, a, s).await;
    store
        .record_storage_observation(
            &s.upload_id,
            ArtifactStorageObservation::VerifiedComplete,
            deadline(),
        )
        .await
        .unwrap();
    store.accept(a, s, deadline()).await.unwrap()
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn concurrent_binding_is_immutable_and_actor_bound() {
    let (db, a, c) = fixture().await;
    let (one, two) = tokio::join!(
        db.store.prepare(&a, &c, deadline()),
        db.store.prepare(&a, &c, deadline())
    );
    let s = one.unwrap().upload.selection;
    assert_eq!(s, two.unwrap().upload.selection);
    let mut changed = c.clone();
    changed.metadata.filename = Some("changed.bin".into());
    assert_eq!(
        db.store
            .prepare(&a, &changed, deadline())
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    let manifest = accepted(&db.store, &a, &s).await;
    manifest.validate().unwrap();
    assert_eq!(
        db.store
            .prepare(&a, &c, deadline())
            .await
            .unwrap()
            .upload
            .reference,
        Some(manifest.reference.clone())
    );
    let mut foreign = c.clone();
    foreign.scope.namespace = "foreign".into();
    assert_eq!(
        db.store
            .prepare(&a, &foreign, deadline())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    let mut wrong = a.clone();
    if let ArtifactAuthority::External(ArtifactCaller::TrustedInstance(i)) = &mut wrong {
        i.instance_id = "forged".into();
    }
    assert_eq!(
        db.store
            .upload_status(&wrong, &s.upload_id, deadline())
            .await
            .unwrap_err(),
        ContractError::Forbidden
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn first_confirmed_begin_write_only_and_lost_ack_cannot_release() {
    let (db, a, c) = fixture().await;
    let s = prepare(&db.store, &a, &c).await;
    let claim = writing(&db.store, &a, &s).await;
    assert!(matches!(
        db.store.begin_write(&a, &claim, deadline()).await.unwrap(),
        ArtifactBeginWrite::ReconcileRequired(_)
    ));
    assert_eq!(
        db.store
            .release_ingress(&claim, deadline())
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    assert_eq!(
        db.store
            .upload_status(&a, &s.upload_id, deadline())
            .await
            .unwrap()
            .stage,
        ArtifactUploadStage::Writing
    );
    sqlx::query("UPDATE artifact_uploads SET dispatch_expires_at_ms=1 WHERE upload_id=$1")
        .bind(&s.upload_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let replacement = db
        .store
        .replace(
            &a,
            &ArtifactReplaceCommand {
                upload_id: s.upload_id.clone(),
                expected_selection_revision: 1,
            },
            deadline(),
        )
        .await
        .unwrap();
    replacement.validate().unwrap();
    assert_eq!(
        db.store.accept(&a, &s, deadline()).await.unwrap_err(),
        ContractError::Conflict
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn abandoned_preparations_expire_without_poll_extensions_or_quota_leak() {
    let (db, a, c) = fixture().await;
    let mut selections = Vec::new();
    for index in 0..ARTIFACT_ROOT_PENDING_LIMIT {
        let mut c = c.clone();
        c.key = format!("item-{index}");
        selections.push(prepare(&db.store, &a, &c).await);
    }
    let first = db
        .store
        .upload_status(&a, &selections[0].upload_id, deadline())
        .await
        .unwrap();
    assert_eq!(
        db.store.prepare(&a, &c, deadline()).await.unwrap_err(),
        ContractError::Busy
    );
    assert_eq!(
        first,
        db.store
            .upload_status(&a, &selections[0].upload_id, deadline())
            .await
            .unwrap()
    );
    sqlx::query("UPDATE artifact_uploads SET prepare_expires_at_ms=1 WHERE stage='prepared'")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let new = prepare(&db.store, &a, &c).await;
    assert_ne!(new, selections[0]);
    assert_eq!(
        db.store
            .upload_status(&a, &selections[0].upload_id, deadline())
            .await
            .unwrap()
            .stage,
        ArtifactUploadStage::ExpiredNoWrite
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifact_uploads WHERE stage='prepared'")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn failed_ingress_releases_immediately_and_fences_old_writer() {
    let (db, a, c) = fixture().await;
    let s = prepare(&db.store, &a, &c).await;
    let old = db.store.claim_ingress(&a, &s, deadline()).await.unwrap();
    assert_eq!(
        db.store
            .release_ingress(&old, deadline())
            .await
            .unwrap()
            .stage,
        ArtifactUploadStage::Prepared
    );
    let fresh = db.store.claim_ingress(&a, &s, deadline()).await.unwrap();
    assert_ne!(old.token, fresh.token);
    assert_eq!(
        db.store
            .begin_write(&a, &old, deadline())
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    assert!(matches!(
        db.store.begin_write(&a, &fresh, deadline()).await.unwrap(),
        ArtifactBeginWrite::Dispatch(_)
    ));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn explicit_replacement_receipt_never_reselects_historical_generation() {
    let (db, a, c) = fixture().await;
    let old = prepare(&db.store, &a, &c).await;
    sqlx::query("UPDATE artifact_uploads SET prepare_expires_at_ms=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let command = ArtifactReplaceCommand {
        upload_id: old.upload_id.clone(),
        expected_selection_revision: 1,
    };
    let (one, two) = tokio::join!(
        db.store.replace(&a, &command, deadline()),
        db.store.replace(&a, &command, deadline())
    );
    let next = one.unwrap();
    assert_eq!(next, two.unwrap());
    sqlx::query("UPDATE artifact_uploads SET prepare_expires_at_ms=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let third = db
        .store
        .replace(
            &a,
            &ArtifactReplaceCommand {
                upload_id: next.replacement.upload_id.clone(),
                expected_selection_revision: 2,
            },
            deadline(),
        )
        .await
        .unwrap();
    let replay = db.store.replace(&a, &command, deadline()).await.unwrap();
    assert_eq!(replay.replacement, next.replacement);
    assert!(!replay.current.selected);
    replay.validate().unwrap();
    assert_eq!(
        db.store
            .prepare(&a, &c, deadline())
            .await
            .unwrap()
            .upload
            .selection,
        third.replacement
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn unknown_absent_cleanup_survives_root_collection_and_late_completion() {
    let (db, a, c) = fixture().await;
    let s = prepare(&db.store, &a, &c).await;
    writing(&db.store, &a, &s).await;
    // Root retirement and metadata collection cannot delete the physical ledger.
    db.store.cancel(&test_scope(), c.target.id()).await.unwrap();
    sqlx::query(
        "UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2 WHERE task_id=$1",
    )
    .bind(c.target.id())
    .execute(&db.store.pool)
    .await
    .unwrap();
    for _ in 0..30 {
        db.store
            .retain_batch(&test_scope(), &RetentionPolicy::default(), deadline())
            .await
            .unwrap();
    }

    for _ in 0..30 {
        db.store
            .retain_batch(&test_scope(), &RetentionPolicy::default(), deadline())
            .await
            .unwrap();
    }
    let remains: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_id=$1)")
        .bind(c.target.id())
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert!(!remains);
    assert_eq!(
        db.store.accept(&a, &s, deadline()).await.unwrap_err(),
        ContractError::NotFound
    );
    let work = cleanup_work(&db.store).await;
    assert!(matches!(&work[..], [ArtifactMaintenanceWork::Inspect(_)]));
    db.store
        .record_storage_observation(&s.upload_id, ArtifactStorageObservation::Absent, deadline())
        .await
        .unwrap();
    let pending = db.store.maintenance_progress(deadline()).await.unwrap();
    assert_eq!((pending.pending, pending.uncertain), (1, 1));
    // A late physical completion is recorded with no execution impersonation.
    db.store
        .record_storage_observation(
            &s.upload_id,
            ArtifactStorageObservation::VerifiedComplete,
            deadline(),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE artifact_uploads SET cleanup_expires_at_ms=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let (work, batches) = cleanup_work_counted(&db.store).await;
    // We previously inspected this sole root/upload. The next root-lane visit
    // wraps both indexed cursors within the same transaction and finds the
    // now-quiescent upload. One interleaved replaced-lane visit makes two calls.
    assert_eq!(batches, 2);
    let ArtifactMaintenanceWork::Delete(lease) = &work[0] else {
        panic!("known completed object must be deletable")
    };
    db.store
        .finish_delete(lease, ArtifactDeleteObservation::Deleted, deadline())
        .await
        .unwrap();
    db.store
        .finish_delete(lease, ArtifactDeleteObservation::Deleted, deadline())
        .await
        .unwrap();
    assert_eq!(
        db.store
            .maintenance_progress(deadline())
            .await
            .unwrap()
            .pending,
        0
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn retirement_hides_ready_manifest_and_store_identity_is_pinned() {
    let (db, a, c) = fixture().await;
    let s = prepare(&db.store, &a, &c).await;
    let manifest = accepted(&db.store, &a, &s).await;
    let mut other = identity();
    other.fingerprint.0 = format!("sha256:{}", "b".repeat(64));
    assert_eq!(
        db.store.bind_store(&other, deadline()).await.unwrap_err(),
        ContractError::Conflict
    );
    assert_eq!(
        db.store
            .resolve(&a, &s.artifact_id, ArtifactReadKind::Content, deadline())
            .await
            .unwrap()
            .manifest,
        manifest
    );
    db.store.cancel(&test_scope(), c.target.id()).await.unwrap();
    sqlx::query(
        "UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2 WHERE task_id=$1",
    )
    .bind(c.target.id())
    .execute(&db.store.pool)
    .await
    .unwrap();
    for _ in 0..30 {
        db.store
            .retain_batch(&test_scope(), &RetentionPolicy::default(), deadline())
            .await
            .unwrap();
    }
    assert_eq!(
        db.store
            .resolve(&a, &s.artifact_id, ArtifactReadKind::Content, deadline())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    let work = cleanup_work(&db.store).await;
    assert!(matches!(&work[..], [ArtifactMaintenanceWork::Delete(_)]));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn worker_authority_uses_current_lease_and_blocks_foreign_root() {
    let (db, external, c) = fixture().await;
    let session = db
        .store
        .open_session(&test_scope(), &command().input.queue, 1)
        .await
        .unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    db.store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    let a = ArtifactAuthority::Worker {
        caller: external.caller().clone(),
        lease: assigned.lease.owner.clone(),
    };
    let s = prepare(&db.store, &a, &c).await;
    accepted(&db.store, &a, &s).await;
    let mut other = command();
    other.idempotency_key = "other-root".into();
    let other = db
        .store
        .accept_resolved_submission(&other, &descriptor())
        .await
        .unwrap();
    let foreign = ArtifactPrepareCommand {
        target: ExecutionTarget::Task { id: other.task_id },
        ..c.clone()
    };
    assert_eq!(
        db.store
            .prepare(&a, &foreign, deadline())
            .await
            .unwrap_err(),
        ContractError::Forbidden
    );
    sqlx::query("UPDATE attempts SET expires_at_ms=1 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .resolve(&a, &s.artifact_id, ArtifactReadKind::Content, deadline())
            .await
            .unwrap_err(),
        ContractError::OwnershipLost
    );
    db.finish().await;
}

async fn acquire_dispatched(store: &PostgresStore, queue: &str) -> Assignment {
    let session = store.open_session(&test_scope(), queue, 1).await.unwrap();
    let assigned = assignment(
        store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    assigned
}
async fn workflow_decide(
    store: &PostgresStore,
    assigned: &Assignment,
    revision: u64,
    action: WorkflowAction,
    resolved: &[ResolvedWorkflowChild],
) {
    let output = serde_json::to_value(WorkflowDecision {
        v: 1,
        activation_id: assigned.lease.owner.task_id.clone(),
        revision,
        action,
    })
    .unwrap();
    store
        .settle(&crate::tests::completed(
            assigned,
            Quiescence::Confirmed,
            output,
        ))
        .await
        .unwrap();
    let work = store.claim_work(16).await.unwrap();
    assert_eq!(work.len(), 1);
    store.apply_work(&work[0], resolved).await.unwrap();
}
async fn drain_work(store: &PostgresStore) {
    for _ in 0..32 {
        let work = store.claim_work(16).await.unwrap();
        if work.is_empty() {
            return;
        }
        for item in work {
            store.apply_work(&item, &[]).await.unwrap();
        }
    }
    panic!("workflow did not drain");
}
async fn wait_lock(store: &PostgresStore, pattern: &str) {
    tokio::time::timeout(Duration::from_secs(5),async{loop{let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)").bind(pattern).fetch_one(&store.pool).await.unwrap();if waiting{break;}tokio::task::yield_now().await;}}).await.unwrap();
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn acceptance_root_lock_allows_task_first_settlement_fk_key_share() {
    let (db, external, c) = fixture().await;
    db.store.cancel(&test_scope(), c.target.id()).await.unwrap();
    let workflow = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let assigned = acquire_dispatched(&db.store, "python").await;
    let a = ArtifactAuthority::Worker {
        caller: external.caller().clone(),
        lease: assigned.lease.owner.clone(),
    };
    let command = ArtifactPrepareCommand {
        target: ExecutionTarget::Task {
            id: assigned.lease.owner.task_id.clone(),
        },
        ..c
    };
    let s = prepare(&db.store, &a, &command).await;
    writing(&db.store, &a, &s).await;
    db.store
        .record_storage_observation(
            &s.upload_id,
            ArtifactStorageObservation::VerifiedComplete,
            deadline(),
        )
        .await
        .unwrap();
    let mut held = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&assigned.lease.owner.task_id)
        .execute(&mut *held)
        .await
        .unwrap();
    let store = db.store.clone();
    let authority = a.clone();
    let selected = s.clone();
    let accept = tokio::spawn(async move { store.accept(&authority, &selected, deadline()).await });
    wait_lock(
        &db.store,
        "SELECT state,current_attempt_id,cancel_requested_at_ms,%",
    )
    .await;
    // Existing settlement's task-first FK check acquires KEY SHARE on the
    // workflow. A FOR UPDATE artifact root lock would deadlock this schedule.
    tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR KEY SHARE")
            .bind(&workflow.workflow_id)
            .execute(&mut *held),
    )
    .await
    .unwrap()
    .unwrap();
    let store = db.store.clone();
    let settled = crate::tests::completed(
        &assigned,
        Quiescence::Confirmed,
        serde_json::json!({"done":true}),
    );
    let settle = tokio::spawn(async move { store.settle(&settled).await });
    held.commit().await.unwrap();
    let accepted = accept.await.unwrap();
    assert!(accepted.is_ok() || matches!(accepted, Err(ContractError::OwnershipLost)));
    settle.await.unwrap().unwrap();
    assert_eq!(
        db.store.accept(&a, &s, deadline()).await.unwrap_err(),
        ContractError::OwnershipLost
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn collected_subworkflow_producer_remains_readable_while_root_callback_is_pending() {
    let (db, external, c) = fixture().await;
    db.store.cancel(&test_scope(), c.target.id()).await.unwrap();
    let root = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let first = acquire_dispatched(&db.store, "python").await;
    let child = WorkflowChildCommand {
        kind: WorkflowChildKind::Workflow,
        key: "subflow".into(),
        program: descriptor().program,
        queue: "subflows".into(),
        data: serde_json::json!({}),
        retry_policy: RetryPolicy::default(),
        attempt_timeout_ms: 300_000,
    };
    workflow_decide(
        &db.store,
        &first,
        0,
        WorkflowAction::Suspend {
            state: serde_json::json!({}),
            continuation: "finish".into(),
            commands: vec![child],
            until: vec!["subflow".into()],
        },
        &[ResolvedWorkflowChild {
            kind: WorkflowChildKind::Workflow,
            key: "subflow".into(),
            descriptor: descriptor(),
        }],
    )
    .await;
    let child = acquire_dispatched(&db.store, "subflows").await;
    let child_context = db
        .store
        .activation_context(&child.lease.owner)
        .await
        .unwrap();
    let worker = ArtifactAuthority::Worker {
        caller: external.caller().clone(),
        lease: child.lease.owner.clone(),
    };
    let c = ArtifactPrepareCommand {
        target: ExecutionTarget::Task {
            id: child.lease.owner.task_id.clone(),
        },
        ..c
    };
    let selected = prepare(&db.store, &worker, &c).await;
    let manifest = accepted(&db.store, &worker, &selected).await;
    assert_eq!(
        manifest.root,
        ExecutionTarget::Workflow {
            id: root.workflow_id.clone()
        }
    );
    workflow_decide(
        &db.store,
        &child,
        0,
        WorkflowAction::Complete {
            output: serde_json::json!({}),
        },
        &[],
    )
    .await;
    drain_work(&db.store).await;
    let resumed = acquire_dispatched(&db.store, "python").await;
    workflow_decide(
        &db.store,
        &resumed,
        1,
        WorkflowAction::Complete {
            output: serde_json::json!({}),
        },
        &[],
    )
    .await;
    drain_work(&db.store).await;
    db.store
        .configure_completion_destination(&CompletionDestination {
            scope: test_scope(),
            destination: "protect-root".into(),
            binding: "http://localhost/unused".into(),
        })
        .await
        .unwrap();
    db.store
        .subscribe_completion(&CompletionSubscribeCommand {
            scope: test_scope(),
            target: ExecutionTarget::Workflow {
                id: root.workflow_id.clone(),
            },
            destination: "protect-root".into(),
            idempotency_key: "hold".into(),
        })
        .await
        .unwrap();
    // Only timestamps/session staleness are fixture controls; actual retention
    // decides which workflow, task, manifest and ledger rows may be collected.
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    for _ in 0..180 {
        db.store
            .retain_batch(
                &test_scope(),
                &RetentionPolicy {
                    batch_size: 2,
                    ..Default::default()
                },
                deadline(),
            )
            .await
            .unwrap();
    }
    let child_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id=$1)")
            .bind(&child_context.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(
        !child_exists,
        "child producer should be physically collected"
    );
    let root_exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id=$1 AND retiring_at_ms IS NULL)").bind(&root.workflow_id).fetch_one(&db.store.pool).await.unwrap();
    assert!(root_exists);
    assert_eq!(
        db.store
            .resolve(
                &external,
                &selected.artifact_id,
                ArtifactReadKind::Content,
                deadline()
            )
            .await
            .unwrap()
            .manifest,
        manifest
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn dispatched_unknown_generations_do_not_age_out_of_root_quota() {
    let (db, a, c) = fixture().await;
    for index in 0..ARTIFACT_ROOT_PENDING_LIMIT {
        let c = ArtifactPrepareCommand {
            key: format!("pending-{index}"),
            ..c.clone()
        };
        let selected = prepare(&db.store, &a, &c).await;
        writing(&db.store, &a, &selected).await;
    }
    sqlx::query("UPDATE artifact_uploads SET prepare_expires_at_ms=1,dispatch_expires_at_ms=1,stage='uncertain'").execute(&db.store.pool).await.unwrap();
    assert_eq!(
        db.store.prepare(&a, &c, deadline()).await.unwrap_err(),
        ContractError::Busy
    );
    assert!(
        db.store
            .claim_maintenance(&identity(), 100, deadline())
            .await
            .unwrap()
            .is_empty(),
        "uncertain selected writes are not cleanup eligible"
    );
    db.finish().await;
}

async fn cleanup_work(store: &PostgresStore) -> Vec<ArtifactMaintenanceWork> {
    cleanup_work_counted(store).await.0
}
async fn cleanup_work_counted(store: &PostgresStore) -> (Vec<ArtifactMaintenanceWork>, usize) {
    // Each fixture has one retired root. Root and upload cursors each wrap
    // once within the root-lane transaction; one alternating replaced-lane
    // visit makes at most two calls. Report transitions on a missed bound.
    let mut transitions = Vec::new();
    for batch in 1..=2 {
        let work = store
            .claim_maintenance(&identity(), 10, deadline())
            .await
            .unwrap();
        let state: (bool, String, String) = sqlx::query_as(
            "SELECT maintenance_lane,maintenance_cursor_kind,maintenance_cursor_id FROM artifact_stores WHERE singleton",
        ).fetch_one(&store.pool).await.unwrap();
        transitions.push((batch, state));
        if !work.is_empty() {
            return (work, batch);
        }
    }
    panic!("eligible cleanup missed its two-batch cursor bound: {transitions:?}")
}

mod service;

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn cleanup_root_discovery_uses_partial_index_without_scanning_healthy_roots() {
    let (db, _, _) = fixture().await;
    // A large inactive metadata population models retained healthy roots; only
    // the 10 retired roots should enter the maintenance discovery index.
    sqlx::query("INSERT INTO artifact_roots(root_kind,root_id,tenant_id,namespace,retired_at_ms) SELECT 'task','indexed-'||lpad(n::text,6,'0'),$1,$2,CASE WHEN n<=10 THEN 1 ELSE NULL END FROM generate_series(1,5000) n").bind(&test_scope().tenant_id).bind(&test_scope().namespace).execute(&db.store.pool).await.unwrap();
    sqlx::query("ANALYZE artifact_roots")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let plan:serde_json::Value=sqlx::query_scalar("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT root_kind,root_id FROM artifact_roots WHERE retired_at_ms IS NOT NULL AND (root_kind,root_id)>('','') ORDER BY root_kind,root_id LIMIT 5").fetch_one(&db.store.pool).await.unwrap();
    let encoded = plan.to_string();
    assert!(
        encoded.contains("artifact_roots_retired"),
        "expected retired-root partial index: {encoded}"
    );
    assert!(
        !encoded.contains("Seq Scan"),
        "healthy roots must not be scanned: {encoded}"
    );
    assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(5.0));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn verified_actor_binding_survives_rotation_but_not_impersonation_or_expiry_wait() {
    let (db, _, c) = fixture().await;
    let config = SelfHostedInstanceConfig {
        instance_id: "artifacts-test".into(),
        name: "Artifacts".into(),
        scope: test_scope(),
        suggested_queues: vec![],
        allowed_origins: vec![],
    };
    let audience = AccessAudience {
        authority_id: "artifact-authority".into(),
        instance_id: config.instance_id.clone(),
        scope: test_scope(),
    };
    db.store
        .initialize_instance_with_access(Some(&config), Some(&audience))
        .await
        .unwrap();
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let actor = |subject: &str, credential: &str, expiry: u64| {
        ArtifactAuthority::External(ArtifactCaller::Verified(
            VerifiedPrincipal::new(
                PrincipalIdentity {
                    issuer: audience.issuer(),
                    subject: subject.into(),
                },
                audience.clone(),
                credential.into(),
                PrincipalClass::Service,
                expiry,
                vec![
                    AccessCapability::ArtifactWrite,
                    AccessCapability::MetadataRead,
                    AccessCapability::SensitiveRead,
                ],
            )
            .unwrap(),
        ))
    };
    let first = actor("service-a", "old", now as u64 + 60_000);
    let rotated = actor("service-a", "new", now as u64 + 60_000);
    let other = actor("service-b", "other", now as u64 + 60_000);
    let selected = prepare(&db.store, &first, &c).await;
    assert_eq!(selected, prepare(&db.store, &rotated, &c).await);
    assert_eq!(
        db.store
            .upload_status(&other, &selected.upload_id, deadline())
            .await
            .unwrap_err(),
        ContractError::Forbidden
    );
    assert_ne!(selected, prepare(&db.store, &other, &c).await);
    let expiring = actor("short", "short", now as u64 + 200);
    let mut held = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(c.target.id())
        .execute(&mut *held)
        .await
        .unwrap();
    let store = db.store.clone();
    let command = ArtifactPrepareCommand {
        key: "expired-after-lock".into(),
        ..c
    };
    let pending = tokio::spawn(async move { store.prepare(&expiring, &command, deadline()).await });
    wait_lock(&db.store, "SELECT task_id FROM tasks WHERE task_id=%").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    held.commit().await.unwrap();
    assert!(
        matches!(pending.await.unwrap(), Err(ContractError::Unauthenticated)),
        "authority clock must be obtained after root lock wait"
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn first_preparation_serializes_store_rebinding_until_its_generation_commits() {
    let (db, authority, command) = fixture().await;
    sqlx::raw_sql("CREATE FUNCTION pause_first_artifact() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(9173,1); RETURN NEW; END $$; CREATE TRIGGER pause_first_artifact BEFORE INSERT ON execution_artifacts FOR EACH ROW EXECUTE FUNCTION pause_first_artifact()")
        .execute(&db.store.pool).await.unwrap();
    let mut held = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(9173,1)")
        .execute(&mut *held)
        .await
        .unwrap();
    let store = db.store.clone();
    let a = authority.clone();
    let c = command.clone();
    let preparing = tokio::spawn(async move { store.prepare(&a, &c, deadline()).await });
    wait_lock(&db.store, "%INSERT INTO execution_artifacts%").await;
    let store = db.store.clone();
    let rebound = ArtifactStoreIdentity {
        store_id: "replacement-store".into(),
        fingerprint: Digest(format!("sha256:{}", "b".repeat(64))),
    };
    let binding = tokio::spawn(async move { store.bind_store(&rebound, deadline()).await });
    wait_lock(&db.store, "%LOCK TABLE artifact_stores%").await;
    assert!(!preparing.is_finished() && !binding.is_finished());
    held.rollback().await.unwrap();
    let selected = preparing.await.unwrap().unwrap().upload.selection;
    assert!(matches!(
        binding.await.unwrap(),
        Err(ContractError::Conflict)
    ));
    let object = db
        .store
        .upload_object(&authority, &selected, deadline())
        .await
        .unwrap();
    assert_eq!(object.store, identity());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT store_id FROM artifact_stores WHERE singleton")
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
        identity().store_id
    );
    db.finish().await;
}

async fn slow_cleanup_does_not_starve_generations(replaced_lane: bool) {
    let (db, authority, command) = fixture().await;
    let mut selections = Vec::new();
    for key in ["first", "second"] {
        let command = ArtifactPrepareCommand {
            key: key.into(),
            ..command.clone()
        };
        let selection = prepare(&db.store, &authority, &command).await;
        writing(&db.store, &authority, &selection).await;
        selections.push(selection);
    }
    selections.sort_by(|left, right| left.upload_id.cmp(&right.upload_id));
    let unknown = &selections[0];
    let deletable = &selections[1];
    if replaced_lane {
        sqlx::query("UPDATE artifact_uploads SET dispatch_expires_at_ms=1")
            .execute(&db.store.pool)
            .await
            .unwrap();
        for selection in &selections {
            db.store
                .replace(
                    &authority,
                    &ArtifactReplaceCommand {
                        upload_id: selection.upload_id.clone(),
                        expected_selection_revision: selection.revision,
                    },
                    deadline(),
                )
                .await
                .unwrap();
        }
    } else {
        sqlx::query("UPDATE artifact_roots SET retired_at_ms=1")
            .execute(&db.store.pool)
            .await
            .unwrap();
    }
    db.store
        .record_storage_observation(
            &deletable.upload_id,
            ArtifactStorageObservation::ConfirmedNoWrite,
            deadline(),
        )
        .await
        .unwrap();
    let mut observed_unknown = false;
    let mut deleted_later = false;
    // Two eligible generations need two visits to their alternating lane:
    // replaced runs on calls 1/3, retired-root runs on calls 2/4.
    let bound = if replaced_lane { 3 } else { 4 };
    for _ in 0..bound {
        // Model separate periodic retain commands after every cleanup lease has
        // elapsed. The low-ID unresolved write is eligible on every invocation.
        sqlx::query(
            "UPDATE artifact_uploads SET cleanup_expires_at_ms=1 WHERE cleanup_token IS NOT NULL",
        )
        .execute(&db.store.pool)
        .await
        .unwrap();
        let work = db
            .store
            .claim_maintenance(&identity(), 1, deadline())
            .await
            .unwrap();
        assert!(work.len() <= 1);
        for item in work {
            match item {
                ArtifactMaintenanceWork::Inspect(lease) => {
                    assert_eq!(lease.upload_id, unknown.upload_id);
                    observed_unknown = true;
                    db.store
                        .record_storage_observation(
                            &lease.upload_id,
                            ArtifactStorageObservation::Absent,
                            deadline(),
                        )
                        .await
                        .unwrap();
                }
                ArtifactMaintenanceWork::Delete(lease) => {
                    assert_eq!(lease.upload_id, deletable.upload_id);
                    db.store
                        .finish_delete(&lease, ArtifactDeleteObservation::Deleted, deadline())
                        .await
                        .unwrap();
                    deleted_later = true;
                }
            }
        }
        if deleted_later {
            break;
        }
    }
    assert!(
        observed_unknown,
        "fixture must encounter the unresolved lower ID first"
    );
    assert!(
        deleted_later,
        "later generation missed its {bound}-batch bound across expired cleanup leases"
    );
    let facts: (bool, bool) =
        sqlx::query_as("SELECT quiescent,deleted FROM artifact_uploads WHERE upload_id=$1")
            .bind(&unknown.upload_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(
        facts,
        (false, false),
        "fairness must preserve unresolved physical obligations"
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn slow_retired_root_cleanup_does_not_starve_later_generations() {
    slow_cleanup_does_not_starve_generations(false).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn slow_replaced_cleanup_does_not_starve_later_generations() {
    slow_cleanup_does_not_starve_generations(true).await;
}
