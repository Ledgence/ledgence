use super::*;
use ledgence_orchestration_api::ArtifactMetadata;
use std::time::Duration;
use tempfile::tempdir;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}
struct Repeated {
    byte: u8,
    remaining: u64,
}
impl ArtifactReader for Repeated {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let length = self.remaining.min(max as u64) as usize;
            self.remaining -= length as u64;
            Ok(vec![self.byte; length])
        })
    }
}
fn digest(byte: u8, size: u64) -> Digest {
    let mut hash = Sha256::new();
    let chunk = vec![byte; CHUNK_BYTES];
    let mut remaining = size;
    while remaining > 0 {
        let count = remaining.min(CHUNK_BYTES as u64) as usize;
        hash.update(&chunk[..count]);
        remaining -= count as u64;
    }
    Digest(format!("sha256:{:x}", hash.finalize()))
}
fn object(store: &FileExecutionArtifactStore, size: u64, byte: u8) -> ArtifactObjectDescriptor {
    ArtifactObjectDescriptor {
        store: store.identity().clone(),
        artifact_id: "art_test".into(),
        upload_id: "upload_test".into(),
        key: "objects/art_test/upload_test".into(),
        metadata: ArtifactMetadata {
            digest: digest(byte, size),
            size,
            media_type: "application/octet-stream".into(),
            filename: None,
        },
    }
}
async fn put(
    store: &FileExecutionArtifactStore,
    object: &ArtifactObjectDescriptor,
    byte: u8,
    size: u64,
) -> BlobPutOutcome {
    store
        .put_once(
            object,
            Box::new(Repeated {
                byte,
                remaining: size,
            }),
            deadline(),
        )
        .await
        .unwrap()
}
async fn verify(
    store: &FileExecutionArtifactStore,
    object: &ArtifactObjectDescriptor,
) -> Result<()> {
    let mut reader = store.open(object, deadline()).await?.unwrap().reader;
    let mut hash = Sha256::new();
    let mut size = 0;
    loop {
        let chunk = reader.read_chunk(CHUNK_BYTES).await?;
        if chunk.is_empty() {
            break;
        }
        size += chunk.len() as u64;
        hash.update(chunk);
    }
    assert_eq!(size, object.metadata.size);
    assert_eq!(
        format!("sha256:{:x}", hash.finalize()),
        object.metadata.digest.0
    );
    Ok(())
}
#[tokio::test]
async fn empty_and_exact_maximum_survive_restart_and_delete() {
    for size in [0, crate::MAX_BYTES] {
        let directory = tempdir().unwrap();
        let store = FileExecutionArtifactStore::open("test", directory.path())
            .await
            .unwrap();
        let object = object(&store, size, 77);
        assert_eq!(
            put(&store, &object, 77, size).await,
            BlobPutOutcome::Created
        );
        verify(&store, &object).await.unwrap();
        let identity = store.identity().clone();
        drop(store);
        let restarted = FileExecutionArtifactStore::open("test", directory.path())
            .await
            .unwrap();
        assert_eq!(*restarted.identity(), identity);
        verify(&restarted, &object).await.unwrap();
        assert_eq!(
            restarted.delete(&object, deadline()).await.unwrap(),
            BlobDeleteOutcome::Deleted
        );
        assert!(restarted.open(&object, deadline()).await.unwrap().is_none());
        assert_eq!(
            restarted.delete(&object, deadline()).await.unwrap(),
            BlobDeleteOutcome::Absent
        );
    }
}
#[tokio::test]
async fn rejected_digest_and_length_never_publish() {
    let directory = tempdir().unwrap();
    let store = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    let object = object(&store, 5, 1);
    assert_eq!(
        put(&store, &object, 2, 5).await,
        BlobPutOutcome::ConfirmedNoWrite
    );
    assert_eq!(
        put(&store, &object, 1, 4).await,
        BlobPutOutcome::ConfirmedNoWrite
    );
    assert_eq!(
        put(&store, &object, 1, 6).await,
        BlobPutOutcome::ConfirmedNoWrite
    );
    assert!(store.open(&object, deadline()).await.unwrap().is_none());
    let mut oversized = object.clone();
    oversized.metadata.size = crate::MAX_BYTES + 1;
    assert!(
        store
            .put_once(
                &oversized,
                Box::new(Repeated {
                    byte: 0,
                    remaining: 0
                }),
                deadline()
            )
            .await
            .is_err()
    );
}
#[tokio::test]
async fn immutable_conflict_never_replaces_bytes() {
    let directory = tempdir().unwrap();
    let store = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    let first = object(&store, 4, 1);
    assert_eq!(put(&store, &first, 1, 4).await, BlobPutOutcome::Created);
    let mut second = first.clone();
    second.metadata.digest = digest(2, 4);
    assert_eq!(
        put(&store, &second, 2, 4).await,
        BlobPutOutcome::ConfirmedNoWrite
    );
    verify(&store, &first).await.unwrap();
}
#[tokio::test]
async fn corrupt_and_truncated_files_are_rejected_by_reader() {
    for data in [vec![2; 5], vec![1; 4], vec![1; 6]] {
        let directory = tempdir().unwrap();
        let store = FileExecutionArtifactStore::open("test", directory.path())
            .await
            .unwrap();
        let object = object(&store, 5, 1);
        assert_eq!(put(&store, &object, 1, 5).await, BlobPutOutcome::Created);
        std::fs::write(directory.path().join(&object.key), data).unwrap();
        assert!(verify(&store, &object).await.is_err());
    }
}
#[tokio::test]
async fn store_marker_not_path_is_identity_and_key_cannot_escape() {
    let one = tempdir().unwrap();
    let two = tempdir().unwrap();
    let a = FileExecutionArtifactStore::open("same", one.path())
        .await
        .unwrap();
    let b = FileExecutionArtifactStore::open("same", two.path())
        .await
        .unwrap();
    assert_ne!(a.identity(), b.identity());
    let mut value = object(&a, 0, 0);
    assert!(b.open(&value, deadline()).await.is_err());
    value.key = "../../outside".into();
    assert!(a.open(&value, deadline()).await.is_err());
}
#[tokio::test]
async fn concurrent_store_startup_never_sweeps_live_staging() {
    let directory = tempdir().unwrap();
    let first = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    let path = first
        .inner
        .staging
        .join(format!("put-{}.tmp", "a".repeat(32)));
    std::fs::write(&path, b"retained").unwrap();
    let second = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    assert!(path.exists());
    assert_eq!(first.identity(), second.identity());
    let old_stage = first.inner.staging.clone();
    drop(first);
    let third = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    assert!(!old_stage.exists());
    assert_eq!(second.identity(), third.identity());
}
#[tokio::test]
async fn delete_fault_is_not_reported_as_success() {
    let directory = tempdir().unwrap();
    let store = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    let object = object(&store, 0, 0);
    let path = directory.path().join(&object.key);
    std::fs::create_dir_all(&path).unwrap();
    assert!(store.delete(&object, deadline()).await.is_err());
}

#[tokio::test]
async fn crash_between_namespace_creation_and_owner_lock_is_recoverable() {
    let directory = tempdir().unwrap();
    let abandoned = directory.path().join(".staging").join("a".repeat(32));
    std::fs::create_dir_all(&abandoned).unwrap();
    let marker = directory
        .path()
        .join(format!(".marker-{}.tmp", "b".repeat(32)));
    std::fs::write(&marker, b"partial marker").unwrap();
    let store = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    assert!(!abandoned.exists());
    assert!(!marker.exists());
    store.identity().validate().unwrap();
}

struct ObstructCleanup {
    staging: PathBuf,
    obstructed: Arc<Notify>,
}
impl ArtifactReader for ObstructCleanup {
    fn read_chunk(&mut self, _max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let stage = std::fs::read_dir(&self.staging)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    path.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("put-")
                })
                .unwrap();
            std::fs::remove_file(&stage).unwrap();
            std::fs::create_dir(&stage).unwrap();
            self.obstructed.notify_one();
            Err(unavailable("injected reader failure"))
        })
    }
}

#[tokio::test]
async fn drain_retains_timed_out_write_until_staging_cleanup_is_confirmed() {
    let directory = tempdir().unwrap();
    let store = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    let object = object(&store, 1, 1);
    let obstructed = Arc::new(Notify::new());
    let operation = {
        let store = store.clone();
        let object = object.clone();
        let reader = ObstructCleanup {
            staging: store.inner.staging.clone(),
            obstructed: obstructed.clone(),
        };
        tokio::spawn(async move {
            store
                .put_once(
                    &object,
                    Box::new(reader),
                    Instant::now() + Duration::from_millis(100),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), obstructed.notified())
        .await
        .unwrap();
    assert_eq!(operation.await.unwrap().unwrap(), BlobPutOutcome::Uncertain);
    assert_eq!(store.inner.stages.available_permits(), 1);
    assert!(
        store
            .drain(Instant::now() + Duration::from_millis(20))
            .await
            .is_err()
    );
    assert_eq!(store.inner.writes.lock().unwrap().active, 1);
    assert!(
        store
            .put_once(
                &object,
                Box::new(Repeated {
                    byte: 1,
                    remaining: 1
                }),
                deadline()
            )
            .await
            .is_err()
    );
    // Repair the failed unlink. The detached owner can now release its stage
    // and slot, and only then is a repeated drain allowed to report success.
    let stage = std::fs::read_dir(&store.inner.staging)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .unwrap();
    std::fs::remove_dir(stage).unwrap();
    store.drain(deadline()).await.unwrap();
    assert_eq!(store.inner.writes.lock().unwrap().active, 0);
    assert_eq!(store.inner.stages.available_permits(), 2);
}

#[tokio::test]
async fn online_maintenance_can_read_and_delete_without_sweeping_live_writer() {
    let directory = tempdir().unwrap();
    let writer = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    let object = object(&writer, 7, 1);
    assert_eq!(put(&writer, &object, 1, 7).await, BlobPutOutcome::Created);
    let retained = writer
        .inner
        .staging
        .join(format!("put-{}.tmp", "c".repeat(32)));
    std::fs::write(&retained, b"live staging").unwrap();
    let maintainer = FileExecutionArtifactStore::open("test", directory.path())
        .await
        .unwrap();
    verify(&maintainer, &object).await.unwrap();
    assert_eq!(
        maintainer.delete(&object, deadline()).await.unwrap(),
        BlobDeleteOutcome::Deleted
    );
    assert!(retained.exists());
    assert!(writer.open(&object, deadline()).await.unwrap().is_none());
    maintainer.drain(deadline()).await.unwrap();
    writer.drain(deadline()).await.unwrap();
}
