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
