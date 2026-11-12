use super::{tests::test_artifact, *};
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[test]
fn blocked_verification_allows_an_unrelated_hit() {
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    let (first, archive, _) = test_artifact("v1");
    drop(cache.publish_sync(first.clone(), archive).unwrap());
    let (other, archive, _) = test_artifact("v2");
    drop(cache.publish_sync(other.clone(), archive).unwrap());

    let entry = cache.state().unwrap().pin(&first).unwrap().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::scope(|scope| {
        let pause = entry.verification.lock().unwrap();
        let first_hit = scope.spawn(|| cache.lookup_sync(&first));
        // The index and this test hold two references. The third is the lookup
        // waiting for this digest's verifier, with no timing assumption about disk I/O.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Arc::strong_count(&entry) == 2 {
            assert!(Instant::now() < deadline, "lookup never pinned its entry");
            thread::yield_now();
        }
        scope.spawn(|| tx.send(cache.lookup_sync(&other)).unwrap());
        let other_hit = rx.recv_timeout(Duration::from_secs(5));
        drop(pause);
        assert!(
            other_hit
                .expect("independent lookup blocked")
                .unwrap()
                .is_some()
        );
        assert!(first_hit.join().unwrap().unwrap().is_some());
    });
}

#[test]
fn failed_verification_releases_its_pin_for_eviction() {
    let root = tempfile::tempdir().unwrap();
    let (first, archive, expanded) = test_artifact("v1");
    let (other, next_archive, next_expanded) = test_artifact("v2");
    let charge = |descriptor: &ProgramDescriptor, expanded: u64| {
        entry_size(
            descriptor,
            expanded,
            serde_json::to_vec(descriptor).unwrap().len() as u64,
        )
        .unwrap()
    };
    let cache = FileArtifactCache::new(
        root.path(),
        ArtifactLimits {
            max_cache_bytes: charge(&first, expanded).max(charge(&other, next_expanded)),
            ..Default::default()
        },
    )
    .unwrap();
    drop(cache.publish_sync(first.clone(), archive).unwrap());

    // This is the only live pin besides the index, before a lookup has
    // verified the files or returned a PreparedArtifact to a caller.
    let verifying = cache.state().unwrap().pin(&first).unwrap().unwrap();
    assert!(matches!(
        cache.publish_sync(other.clone(), next_archive.clone()),
        Err(AdapterError::Pressure)
    ));
    fs::write(
        root.path().join(first.digest.hex()).join("unexpected"),
        b"corruption",
    )
    .unwrap();
    assert!(matches!(
        cache.verified_hit(verifying),
        Err(AdapterError::Invalid(_))
    ));
    assert!(cache.publish_sync(other, next_archive).is_ok());
    assert!(cache.lookup_sync(&first).unwrap().is_none());
}

#[test]
fn cancelled_expanded_lookup_keeps_its_verification_pin_until_completion() {
    use ledgence_worker_api::{ArtifactCache, ProgramPackageProfile};
    use std::io::{Cursor, Write};
    use zip::{ZipWriter, write::SimpleFileOptions};

    let root = tempfile::tempdir().unwrap();
    let limits = ArtifactLimits::for_profile(ProgramPackageProfile::Entries8192V1);
    let (mut descriptor, archive, _) = test_artifact("expanded-cancel");
    let mut writer = ZipWriter::new_append(Cursor::new(archive)).unwrap();
    for index in 2..8192 {
        writer
            .start_file(format!("dependency-{index}"), SimpleFileOptions::default())
            .unwrap();
        writer.write_all(b"x").unwrap();
    }
    let archive = writer.finish().unwrap().into_inner();
    descriptor.size = archive.len() as u64;
    descriptor.digest =
        ledgence_worker_api::Digest(format!("sha256:{}", crate::archive::digest_hex(&archive)));
    let cache = FileArtifactCache::new(root.path(), limits.clone()).unwrap();
    drop(cache.publish_sync(descriptor.clone(), archive).unwrap());
    let entry = cache.state().unwrap().pin(&descriptor).unwrap().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let pause = entry.verification.lock().unwrap();
    runtime.block_on(async {
        let lookup_cache = cache.clone();
        let lookup_descriptor = descriptor.clone();
        let task = tokio::spawn(async move { lookup_cache.lookup(&lookup_descriptor).await });
        let deadline = Instant::now() + Duration::from_secs(5);
        while Arc::strong_count(&entry) == 2 {
            assert!(
                Instant::now() < deadline,
                "lookup never retained its verification pin"
            );
            tokio::task::yield_now().await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        // The caller is gone, but the running blocking verifier still owns its
        // pin in addition to the index and this test. It cannot be evicted.
        assert_eq!(Arc::strong_count(&entry), 3);
    });
    drop(pause);
    runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Arc::strong_count(&entry) > 2 {
            assert!(
                Instant::now() < deadline,
                "cancelled lookup never finished verification"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    drop(entry);
    drop(cache);
    let reopened = FileArtifactCache::new(root.path(), limits).unwrap();
    assert!(reopened.lookup_sync(&descriptor).unwrap().is_some());
}
