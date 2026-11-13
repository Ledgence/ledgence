use super::*;
use std::{future::poll_fn, task::Poll};
use tempfile::tempdir;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
async fn wait_empty(factory: &FileArtifactSpoolFactory) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if factory.inner.state.lock().unwrap().leases.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn slots_precede_files_and_survive_finish_and_eof() {
    let directory = tempdir().unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    let mut one = factory.create_inner(true, 4, deadline()).await.unwrap();
    let two = factory.create_inner(true, 0, deadline()).await.unwrap();
    assert!(matches!(
        factory.create_inner(true, 1, deadline()).await,
        Err(ContractError::Busy)
    ));
    assert_eq!(factory.inner.state.lock().unwrap().leases.len(), 2);
    one.append(b"abcd".to_vec()).await.unwrap();
    let mut reader = Box::new(one).finish().await.unwrap();
    assert_eq!(reader.read_chunk(2).await.unwrap(), b"ab");
    assert_eq!(reader.read_chunk(2).await.unwrap(), b"cd");
    assert!(reader.read_chunk(2).await.unwrap().is_empty());
    // The service can retain this reader until catalog finalization even after
    // the SDK has consumed EOF and dropped its shared reader wrapper.
    assert!(matches!(
        factory.create_inner(true, 1, deadline()).await,
        Err(ContractError::Busy)
    ));
    let mut downloads = Vec::new();
    for _ in 0..4 {
        downloads.push(factory.create_inner(false, 0, deadline()).await.unwrap());
    }
    assert!(matches!(
        factory.create_inner(false, 1, deadline()).await,
        Err(ContractError::Busy)
    ));
    drop(downloads);
    drop(reader);
    drop(two);
    wait_empty(&factory).await;
    factory.drain_inner(deadline()).await.unwrap();
    assert!(factory.create_inner(true, 0, deadline()).await.is_err());
}
#[tokio::test]
async fn bound_is_total_and_finish_is_not_integrity_verification() {
    let directory = tempdir().unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    assert!(
        factory
            .create_inner(true, MAX_BYTES + 1, deadline())
            .await
            .is_err()
    );
    let mut writer = factory.create_inner(true, 3, deadline()).await.unwrap();
    writer.append(vec![1, 2]).await.unwrap();
    assert!(writer.append(vec![3, 4]).await.is_err());
    writer.append(vec![3]).await.unwrap();
    assert!(writer.append(vec![0; CHUNK_BYTES + 1]).await.is_err());
    let mut reader = Box::new(writer).finish().await.unwrap();
    assert!(reader.read_chunk(0).await.is_err());
    assert!(reader.read_chunk(CHUNK_BYTES + 1).await.is_err());
    assert_eq!(reader.read_chunk(3).await.unwrap(), vec![1, 2, 3]);
    drop(reader);
    wait_empty(&factory).await;
}
#[tokio::test]
async fn retained_unpolled_reader_expires_independently() {
    let directory = tempdir().unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    let writer = factory.create_inner(false, 0, deadline()).await.unwrap();
    let lease = writer.lease.clone();
    let mut reader = Box::new(writer).finish().await.unwrap();
    assert!(lease.path().is_file());
    assert!(!lease.complete.load(Ordering::Acquire));
    assert_eq!(factory.inner.state.lock().unwrap().leases.len(), 1);
    assert_eq!(factory.inner.downloads.available_permits(), 3);

    // Advance only after real filesystem preparation. Expiry must clean up a
    // live reader without relying on a read, Drop, or fast sync_all in CI.
    tokio::time::pause();
    tokio::time::advance(
        tokio::time::Instant::from_std(lease.deadline) - tokio::time::Instant::now()
            + Duration::from_millis(1),
    )
    .await;
    tokio::time::resume();
    wait_empty(&factory).await;
    assert!(lease.complete.load(Ordering::Acquire));
    assert!(!lease.path().exists());
    assert_eq!(factory.inner.downloads.available_permits(), 4);
    assert!(reader.read_chunk(1).await.is_err());
    drop(reader);
}
#[tokio::test]
async fn deadline_is_clipped_not_reset_and_stale_owner_is_exclusive() {
    let directory = tempdir().unwrap();
    let stale = directory
        .path()
        .join(format!("spool-{}.tmp", "a".repeat(32)));
    std::fs::write(&stale, b"old").unwrap();
    std::fs::write(directory.path().join("keep-me"), b"unrelated").unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    assert!(!stale.exists());
    assert!(directory.path().join("keep-me").exists());
    assert!(matches!(
        FileArtifactSpoolFactory::open(directory.path()).await,
        Err(ContractError::Busy)
    ));
    let writer = factory
        .create_inner(true, 0, Instant::now() + Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(writer.lease.deadline <= Instant::now() + TRANSFER_TIMEOUT);
    drop(writer);
    wait_empty(&factory).await;
    drop(factory);
    FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
}
#[tokio::test]
async fn drain_timeout_does_not_cancel_ownership_or_reopen_admission() {
    let directory = tempdir().unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    let writer = factory.create_inner(true, 0, deadline()).await.unwrap();
    assert!(
        factory
            .drain_inner(Instant::now() + Duration::from_millis(20))
            .await
            .is_err()
    );
    assert_eq!(factory.inner.uploads.available_permits(), 1);
    assert!(factory.create_inner(false, 0, deadline()).await.is_err());
    drop(writer);
    factory.drain_inner(deadline()).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_failure_retains_slot_until_actual_repair() {
    let directory = tempdir().unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    let writer = factory.create_inner(true, 0, deadline()).await.unwrap();
    let lease = writer.lease.clone();
    let path = lease.path();
    // A directory at this path deterministically makes remove_file fail, even
    // when CI runs with permissions that would bypass chmod-based fixtures.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    drop(writer);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !lease.cleanup_failed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(factory.inner.uploads.available_permits(), 1);
    assert!(
        factory
            .drain_inner(Instant::now() + Duration::from_millis(20))
            .await
            .is_err()
    );
    std::fs::remove_dir(&path).unwrap();
    wait_empty(&factory).await;
    assert_eq!(factory.inner.uploads.available_permits(), 2);
}
async fn interrupted_io(kind: u8) {
    let directory = tempdir().unwrap();
    let factory = FileArtifactSpoolFactory::open(directory.path())
        .await
        .unwrap();
    let mut writer = factory.create_inner(true, 3, deadline()).await.unwrap();
    writer.append(b"abc".to_vec()).await.unwrap();
    let lease = writer.lease.clone();
    let (writer, reader) = if kind == 2 {
        (None, Some(Box::new(writer).finish().await.unwrap()))
    } else {
        (Some(writer), None)
    };
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let retained = lease.clone();
    let blocker = tokio::task::spawn_blocking(move || {
        let _state = retained.state.lock().unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.await.unwrap();
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let io = tokio::spawn(async move {
        let operation = async move {
            match kind {
                0 => {
                    writer.unwrap().append(vec![]).await.unwrap();
                }
                1 => {
                    let _ = Box::new(writer.unwrap()).finish().await;
                }
                _ => {
                    let _ = reader.unwrap().read_chunk(3).await;
                }
            }
        };
        tokio::pin!(operation);
        let mut pending_tx = Some(pending_tx);
        poll_fn(|cx| {
            // The lease mutex is still held. Polling to Pending establishes
            // that blocking IO was queued before the caller is cancelled.
            assert!(operation.as_mut().poll(cx).is_pending());
            if let Some(pending_tx) = pending_tx.take() {
                pending_tx.send(()).unwrap();
            }
            Poll::<()>::Pending
        })
        .await;
    });
    tokio::time::timeout(Duration::from_secs(2), pending_rx)
        .await
        .unwrap()
        .unwrap();
    io.abort();
    assert!(io.await.unwrap_err().is_cancelled());
    assert_eq!(factory.inner.uploads.available_permits(), 1);
    assert!(
        factory
            .drain_inner(Instant::now() + Duration::from_millis(20))
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    blocker.await.unwrap();
    wait_empty(&factory).await;
    assert!(!lease.path().exists());
    assert_eq!(factory.inner.uploads.available_permits(), 2);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_append_keeps_pending_blocking_io_owned() {
    interrupted_io(0).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_finish_keeps_pending_blocking_io_owned() {
    interrupted_io(1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_read_keeps_pending_blocking_io_owned() {
    interrupted_io(2).await;
}
