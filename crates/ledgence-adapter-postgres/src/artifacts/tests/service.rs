//! Real catalog + real filesystem/spool service tests; the delay adapter adds a
//! deterministic acknowledgement boundary without substituting persistence.
use super::*;
use ledgence_adapter_blob::{FileArtifactSpoolFactory, FileExecutionArtifactStore};
use ledgence_orchestration_service::ArtifactService;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct Bytes(Option<Vec<u8>>);
impl ArtifactReader for Bytes {
    fn read_chunk(&mut self, _: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move { Ok(self.0.take().unwrap_or_default()) })
    }
}
fn empty() -> Box<dyn ArtifactReader> {
    Box::new(Bytes(None))
}
async fn real_service(
    store: &PostgresStore,
) -> (
    tempfile::TempDir,
    ArtifactService,
    Arc<FileArtifactSpoolFactory>,
) {
    let directory = tempfile::tempdir().unwrap();
    let blobs = Arc::new(
        FileExecutionArtifactStore::open("files", directory.path().join("objects"))
            .await
            .unwrap(),
    );
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(directory.path().join("spools"))
            .await
            .unwrap(),
    );
    let service = ArtifactService::new(Arc::new(store.clone()), blobs, spools.clone(), deadline())
        .await
        .unwrap();
    (directory, service, spools)
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn invalid_body_releases_ingress_and_valid_retry_publishes_verified_bytes() {
    let (db, a, c) = fixture().await;
    let (_directory, service, _spools) = real_service(&db.store).await;
    let selected = service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let bad = service
        .put(
            a.clone(),
            selected.clone(),
            Box::new(Bytes(Some(vec![1]))),
            deadline(),
        )
        .await;
    assert!(matches!(bad, Err(ContractError::InvalidInput(_))));
    let status = service
        .upload_status(&a, &selected.upload_id, deadline())
        .await
        .unwrap();
    assert_eq!(status.stage, ArtifactUploadStage::Prepared);
    assert!(status.ingress_expires_at.is_none());
    let reply = service
        .put(a.clone(), selected.clone(), empty(), deadline())
        .await
        .unwrap();
    let reference = reply.upload.reference.unwrap();
    let prepared = service
        .prepare_download(&a, &reference, deadline())
        .await
        .unwrap();
    let mut downloaded = service
        .release_download(&a, prepared, deadline())
        .await
        .unwrap();
    assert!(downloaded.reader.read_chunk(1024).await.unwrap().is_empty());
    drop(downloaded);
    service.drain(deadline()).await.unwrap();
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn final_release_rechecks_retirement_after_verified_spooling_and_audit_boundary() {
    let (db, a, c) = fixture().await;
    let (_directory, service, _spools) = real_service(&db.store).await;
    let selected = service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let reference = service
        .put(a.clone(), selected, empty(), deadline())
        .await
        .unwrap()
        .upload
        .reference
        .unwrap();
    let prepared = service
        .prepare_download(&a, &reference, deadline())
        .await
        .unwrap();
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
    assert!(matches!(
        service.release_download(&a, prepared, deadline()).await,
        Err(ContractError::NotFound)
    ));
    service.drain(deadline()).await.unwrap();
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn worker_final_release_rechecks_lease_after_spooling() {
    let (db, external, c) = fixture().await;
    let (_directory, service, _spools) = real_service(&db.store).await;
    let assigned = acquire_dispatched(&db.store, "python").await;
    let a = ArtifactAuthority::Worker {
        caller: external.caller().clone(),
        lease: assigned.lease.owner.clone(),
    };
    let selected = service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let reference = service
        .put(a.clone(), selected, empty(), deadline())
        .await
        .unwrap()
        .upload
        .reference
        .unwrap();
    let prepared = service
        .prepare_download(&a, &reference, deadline())
        .await
        .unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=1 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        service.release_download(&a, prepared, deadline()).await,
        Err(ContractError::OwnershipLost)
    ));
    service.drain(deadline()).await.unwrap();
    db.finish().await;
}
struct DelayedBlob {
    inner: FileExecutionArtifactStore,
    consumed: Semaphore,
    resume: Semaphore,
    calls: AtomicUsize,
}
impl ExecutionArtifactBlobStore for DelayedBlob {
    fn identity(&self) -> &ArtifactStoreIdentity {
        self.inner.identity()
    }
    fn put_once<'a>(
        &'a self,
        o: &'a ArtifactObjectDescriptor,
        r: Box<dyn ArtifactReader>,
        d: Instant,
    ) -> ContractFuture<'a, BlobPutOutcome> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self.inner.put_once(o, r, d).await?;
            // The concrete SDK/store has dropped its body but not returned its
            // outcome to the application; service ownership must retain the spool.
            self.consumed.add_permits(1);
            self.resume.acquire().await.unwrap().forget();
            Ok(outcome)
        })
    }
    fn open<'a>(
        &'a self,
        o: &'a ArtifactObjectDescriptor,
        d: Instant,
    ) -> ContractFuture<'a, Option<BlobRead>> {
        self.inner.open(o, d)
    }
    fn delete<'a>(
        &'a self,
        o: &'a ArtifactObjectDescriptor,
        d: Instant,
    ) -> ContractFuture<'a, BlobDeleteOutcome> {
        self.inner.delete(o, d)
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn dropped_requests_retain_spool_slots_and_drain_owned_writes_after_sdk_body_drop() {
    let (db, a, c) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Arc::new(DelayedBlob {
        inner: FileExecutionArtifactStore::open("delayed", directory.path().join("objects"))
            .await
            .unwrap(),
        consumed: Semaphore::new(0),
        resume: Semaphore::new(0),
        calls: AtomicUsize::new(0),
    });
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(directory.path().join("spools"))
            .await
            .unwrap(),
    );
    let service = ArtifactService::new(
        Arc::new(db.store.clone()),
        blobs.clone(),
        spools.clone(),
        deadline(),
    )
    .await
    .unwrap();
    let one = service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let two = service
        .prepare(
            &a,
            &ArtifactPrepareCommand {
                key: "second".into(),
                ..c
            },
            deadline(),
        )
        .await
        .unwrap()
        .upload
        .selection;
    let svc = service.clone();
    let authority = a.clone();
    let selected = one.clone();
    let first =
        tokio::spawn(async move { svc.put(authority, selected, empty(), deadline()).await });
    let svc = service.clone();
    let authority = a.clone();
    let selected = two.clone();
    let second =
        tokio::spawn(async move { svc.put(authority, selected, empty(), deadline()).await });
    tokio::time::timeout(Duration::from_secs(5), blobs.consumed.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let held_transactions: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(
        held_transactions, 0,
        "blob persistence must not retain a database transaction"
    );
    assert!(
        matches!(
            spools
                .create(ArtifactSpoolPurpose::Upload, 0, deadline())
                .await,
            Err(ContractError::Busy)
        ),
        "service must retain both EOF spool readers after the concrete store drops its bodies"
    );
    first.abort();
    second.abort();
    assert!(matches!(
        service
            .drain(Instant::now() + Duration::from_millis(20))
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        spools
            .create(ArtifactSpoolPurpose::Upload, 0, deadline())
            .await,
        Err(ContractError::Busy)
    ));
    blobs.resume.add_permits(2);
    service.drain(deadline()).await.unwrap();
    assert_eq!(blobs.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        db.store
            .upload_status(&a, &one.upload_id, deadline())
            .await
            .unwrap()
            .stage,
        ArtifactUploadStage::Ready
    );
    assert_eq!(
        db.store
            .upload_status(&a, &two.upload_id, deadline())
            .await
            .unwrap()
            .stage,
        ArtifactUploadStage::Ready
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn retry_lease_can_reconcile_same_producer_and_replace_but_stale_lease_cannot() {
    let (db, external, c) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Arc::new(DelayedBlob {
        inner: FileExecutionArtifactStore::open("retry", directory.path().join("objects"))
            .await
            .unwrap(),
        consumed: Semaphore::new(0),
        resume: Semaphore::new(0),
        calls: AtomicUsize::new(0),
    });
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(directory.path().join("spools"))
            .await
            .unwrap(),
    );
    let service = ArtifactService::new(
        Arc::new(db.store.clone()),
        blobs.clone(),
        spools,
        deadline(),
    )
    .await
    .unwrap();
    let first = acquire_dispatched(&db.store, "python").await;
    let old = ArtifactAuthority::Worker {
        caller: external.caller().clone(),
        lease: first.lease.owner.clone(),
    };
    let selected = service
        .prepare(&old, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let prepared_only = service
        .prepare(
            &old,
            &ArtifactPrepareCommand {
                key: "lost-before-dispatch".into(),
                ..c.clone()
            },
            deadline(),
        )
        .await
        .unwrap()
        .upload
        .selection;
    writing(&db.store, &old, &prepared_only).await;
    let svc = service.clone();
    let auth = old.clone();
    let selection = selected.clone();
    let pending = tokio::spawn(async move { svc.put(auth, selection, empty(), deadline()).await });
    tokio::time::timeout(Duration::from_secs(5), blobs.consumed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let completed = crate::tests::completed(&first, Quiescence::Confirmed, serde_json::json!(null));
    let AttemptReport::Completed(report) = completed.report else {
        unreachable!()
    };
    let failed = SettleCommand {
        report: AttemptReport::Failed(ledgence_worker_api::ExecutionFailure {
            observations: None,
            context: report.context,
            error: ledgence_worker_api::Error::new(
                ledgence_worker_api::ErrorKind::Io,
                "controlled attempt failure",
            ),
            phase: ledgence_worker_api::Phase::Execution,
            cleanup_error: None,
            execution_may_have_started: true,
        }),
        ..completed
    };
    assert_eq!(
        db.store.settle(&failed).await.unwrap().task_state,
        TaskState::Queued
    );
    let second = acquire_dispatched(&db.store, "python").await;
    assert_ne!(first.lease.owner.attempt_id, second.lease.owner.attempt_id);
    let current = ArtifactAuthority::Worker {
        caller: external.caller().clone(),
        lease: second.lease.owner.clone(),
    };
    blobs.resume.add_permits(1);
    assert!(matches!(
        pending.await.unwrap(),
        Err(ContractError::OwnershipLost)
    ));
    assert!(matches!(
        service.reconcile(&old, &selected, deadline()).await,
        Err(ContractError::OwnershipLost)
    ));
    assert_eq!(
        service
            .prepare(&current, &c, deadline())
            .await
            .unwrap()
            .upload
            .selection,
        selected
    );
    let recovered = service
        .reconcile(&current, &selected, deadline())
        .await
        .unwrap();
    assert_eq!(recovered.upload.stage, ArtifactUploadStage::Ready);
    assert_eq!(blobs.calls.load(Ordering::SeqCst), 1);
    sqlx::query("UPDATE artifact_uploads SET dispatch_expires_at_ms=1 WHERE upload_id=$1")
        .bind(&prepared_only.upload_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let command = ArtifactReplaceCommand {
        upload_id: prepared_only.upload_id.clone(),
        expected_selection_revision: 1,
    };
    assert!(matches!(
        service.replace(&old, &command, deadline()).await,
        Err(ContractError::OwnershipLost)
    ));
    let receipt = service
        .replace(&current, &command, deadline())
        .await
        .unwrap();
    assert_eq!(receipt.replaced, prepared_only);
    assert_ne!(receipt.replacement.upload_id, prepared_only.upload_id);
    assert_eq!(
        receipt,
        service
            .replace(&current, &command, deadline())
            .await
            .unwrap()
    );
    service.drain(deadline()).await.unwrap();
    db.finish().await;
}

struct LostDeleteAck {
    inner: FileExecutionArtifactStore,
    lost: std::sync::atomic::AtomicBool,
}
impl ExecutionArtifactBlobStore for LostDeleteAck {
    fn identity(&self) -> &ArtifactStoreIdentity {
        self.inner.identity()
    }
    fn put_once<'a>(
        &'a self,
        o: &'a ArtifactObjectDescriptor,
        r: Box<dyn ArtifactReader>,
        d: Instant,
    ) -> ContractFuture<'a, BlobPutOutcome> {
        self.inner.put_once(o, r, d)
    }
    fn open<'a>(
        &'a self,
        o: &'a ArtifactObjectDescriptor,
        d: Instant,
    ) -> ContractFuture<'a, Option<BlobRead>> {
        self.inner.open(o, d)
    }
    fn delete<'a>(
        &'a self,
        o: &'a ArtifactObjectDescriptor,
        d: Instant,
    ) -> ContractFuture<'a, BlobDeleteOutcome> {
        Box::pin(async move {
            let result = self.inner.delete(o, d).await?;
            if !self.lost.swap(true, Ordering::SeqCst) {
                Err(ContractError::Unavailable(
                    "controlled lost delete acknowledgement".into(),
                ))
            } else {
                Ok(result)
            }
        })
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn deleted_object_lost_ack_recovers_from_absence_only_with_persisted_quiescence() {
    let (db, a, c) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Arc::new(LostDeleteAck {
        inner: FileExecutionArtifactStore::open("delete-ack", directory.path().join("objects"))
            .await
            .unwrap(),
        lost: std::sync::atomic::AtomicBool::new(false),
    });
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(directory.path().join("spools"))
            .await
            .unwrap(),
    );
    let service = ArtifactService::new(
        Arc::new(db.store.clone()),
        blobs.clone(),
        spools,
        deadline(),
    )
    .await
    .unwrap();
    let selected = service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    service
        .put(a.clone(), selected.clone(), empty(), deadline())
        .await
        .unwrap();
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
    let mut failed_once = false;
    let mut completed = false;
    for _ in 0..12 {
        match service.maintain(10, deadline()).await {
            Err(ContractError::Unavailable(_)) => {
                assert!(!failed_once);
                failed_once = true;
                let row = sqlx::query(
                    "SELECT deleted,quiescent FROM artifact_uploads WHERE upload_id=$1",
                )
                .bind(&selected.upload_id)
                .fetch_one(&db.store.pool)
                .await
                .unwrap();
                assert!(!row.get::<bool, _>("deleted"));
                assert!(row.get::<bool, _>("quiescent"));
                let pending = db.store.maintenance_progress(deadline()).await.unwrap();
                assert_eq!((pending.pending, pending.uncertain), (1, 0));
            }
            Ok(progress) if progress.pending == 0 => {
                completed = true;
                break;
            }
            Ok(_) => (),
            Err(error) => panic!("unexpected cleanup error: {error}"),
        }
    }
    assert!(failed_once && completed);
    service.drain(deadline()).await.unwrap();
    db.finish().await;
}

async fn changed_bytes_after_verification(corrupt: bool) {
    let (db, a, c) = fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Arc::new(
        FileExecutionArtifactStore::open("reconciliation", directory.path().join("objects"))
            .await
            .unwrap(),
    );
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(directory.path().join("spools"))
            .await
            .unwrap(),
    );
    let service = ArtifactService::new(
        Arc::new(db.store.clone()),
        blobs.clone(),
        spools,
        deadline(),
    )
    .await
    .unwrap();
    let selection = service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let claim = writing(&db.store, &a, &selection).await;
    assert_eq!(
        blobs
            .put_once(&claim.object, empty(), deadline())
            .await
            .unwrap(),
        BlobPutOutcome::Created
    );
    // Model the durable physical fact surviving an uncommitted acceptance. The
    // independent retry-lease test proves that exact boundary after lease loss.
    db.store
        .record_storage_observation(
            &selection.upload_id,
            ArtifactStorageObservation::VerifiedComplete,
            deadline(),
        )
        .await
        .unwrap();
    let path = blobs.directory().join(&claim.object.key);
    if corrupt {
        std::fs::write(&path, b"unexpected bytes").unwrap();
    } else {
        std::fs::remove_file(&path).unwrap();
    }
    let result = service.reconcile(&a, &selection, deadline()).await;
    if corrupt {
        // The concrete filesystem reader detects corruption itself; its error
        // must not fall through to acceptance of historically verified bytes.
        assert!(result.is_err());
        // Exercise the catalog race fence for a verifier that reports Corrupt
        // directly, independently of the service's fresh-verification guard.
        db.store
            .record_storage_observation(
                &selection.upload_id,
                ArtifactStorageObservation::Corrupt,
                deadline(),
            )
            .await
            .unwrap();
    } else {
        let result = result.unwrap();
        assert_eq!(result.upload.stage, ArtifactUploadStage::Writing);
        assert!(result.upload.reference.is_none());
    }
    let facts: (bool, bool) =
        sqlx::query_as("SELECT verified,quiescent FROM artifact_uploads WHERE upload_id=$1")
            .bind(&selection.upload_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(facts, (false, true));
    assert!(matches!(
        db.store.accept(&a, &selection, deadline()).await,
        Err(ContractError::Busy)
    ));
    assert!(
        service
            .upload_status(&a, &selection.upload_id, deadline())
            .await
            .unwrap()
            .reference
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM execution_artifacts WHERE manifest_bytes IS NOT NULL"
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap(),
        0
    );
    // Controlled storage repair, not another PUT or a generation replacement.
    std::fs::write(&path, b"").unwrap();
    let repaired = service.reconcile(&a, &selection, deadline()).await.unwrap();
    assert_eq!(repaired.upload.stage, ArtifactUploadStage::Ready);
    assert!(repaired.upload.reference.is_some());
    assert_eq!(repaired.upload.selection, selection);
    service.drain(deadline()).await.unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn missing_bytes_after_prior_verification_cannot_be_accepted_by_reconcile() {
    changed_bytes_after_verification(false).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn corrupt_bytes_after_prior_verification_cannot_be_accepted_by_reconcile() {
    changed_bytes_after_verification(true).await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn old_live_service_releases_ingress_before_dispatch_after_empty_store_rebinding() {
    let (db, a, c) = fixture().await;
    let (_old_directory, old_service, _) = real_service(&db.store).await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Arc::new(
        FileExecutionArtifactStore::open("replacement", directory.path().join("objects"))
            .await
            .unwrap(),
    );
    db.store
        .bind_store(blobs.identity(), deadline())
        .await
        .unwrap();
    let selection = old_service
        .prepare(&a, &c, deadline())
        .await
        .unwrap()
        .upload
        .selection;
    let result = old_service
        .put(a.clone(), selection.clone(), empty(), deadline())
        .await;
    assert!(
        matches!(result, Err(ContractError::Unavailable(_))),
        "stale adapter must fail before dispatch: {result:?}"
    );
    let status = db
        .store
        .upload_status(&a, &selection.upload_id, deadline())
        .await
        .unwrap();
    assert_eq!(status.stage, ArtifactUploadStage::Prepared);
    assert!(status.dispatch_expires_at.is_none() && status.ingress_expires_at.is_none());
    let facts: (bool, bool) =
        sqlx::query_as("SELECT verified,quiescent FROM artifact_uploads WHERE upload_id=$1")
            .bind(&selection.upload_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(facts, (false, true));
    // A correctly configured service can continue the same prepared generation.
    let spools = Arc::new(
        FileArtifactSpoolFactory::open(directory.path().join("spools"))
            .await
            .unwrap(),
    );
    let current = ArtifactService::new(Arc::new(db.store.clone()), blobs, spools, deadline())
        .await
        .unwrap();
    let accepted = current
        .put(a, selection.clone(), empty(), deadline())
        .await
        .unwrap();
    assert_eq!(accepted.upload.selection, selection);
    assert_eq!(accepted.upload.stage, ArtifactUploadStage::Ready);
    old_service.drain(deadline()).await.unwrap();
    current.drain(deadline()).await.unwrap();
    db.finish().await;
}
