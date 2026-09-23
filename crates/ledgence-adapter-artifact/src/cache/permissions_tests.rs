use super::*;
use std::os::unix::fs::PermissionsExt;

fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

fn assert_immutable_content(wrapper: &Path) {
    assert_eq!(mode(&wrapper.join("content")), 0o555);
    for path in [
        "artifact.zip",
        "descriptor.json",
        "content/app.py",
        "content/ledgence-program.json",
    ] {
        assert_eq!(mode(&wrapper.join(path)), 0o444, "{path}");
    }
}

#[test]
fn publication_keeps_private_wrapper_writable_and_program_content_readonly() {
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    let (descriptor, archive, _) = super::tests::test_artifact("v1");
    let prepared = cache.publish_sync(descriptor.clone(), archive).unwrap();
    let wrapper = root.path().join(descriptor.digest.hex());
    assert_eq!(mode(&wrapper), 0o700);
    assert_immutable_content(&wrapper);
    assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
    assert!(cache.lookup_sync(&descriptor).unwrap().is_some());
}

#[test]
fn reopening_a_legacy_readonly_wrapper_preserves_content_and_allows_eviction() {
    let root = tempfile::tempdir().unwrap();
    let (first, first_archive, expanded) = super::tests::test_artifact("v1");
    let (second, second_archive, second_expanded) = super::tests::test_artifact("v2");
    let charge = entry_size(
        &first,
        expanded,
        serde_json::to_vec(&first).unwrap().len() as u64,
    )
    .unwrap();
    let second_charge = entry_size(
        &second,
        second_expanded,
        serde_json::to_vec(&second).unwrap().len() as u64,
    )
    .unwrap();
    let limits = ArtifactLimits {
        max_cache_bytes: charge.max(second_charge),
        ..Default::default()
    };
    let cache = FileArtifactCache::new(root.path(), limits.clone()).unwrap();
    drop(
        cache
            .publish_sync(first.clone(), first_archive.clone())
            .unwrap(),
    );
    drop(cache);
    let wrapper = root.path().join(first.digest.hex());
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o555)).unwrap();
    let reopened = FileArtifactCache::new(root.path(), limits).unwrap();
    assert_eq!(mode(&wrapper), 0o700);
    assert_immutable_content(&wrapper);
    assert_eq!(
        fs::read(wrapper.join("artifact.zip")).unwrap(),
        first_archive
    );
    assert!(reopened.lookup_sync(&first).unwrap().is_some());
    let prepared = reopened
        .publish_sync(second.clone(), second_archive)
        .unwrap();
    assert!(!wrapper.exists());
    assert!(reopened.lookup_sync(&first).unwrap().is_none());
    assert!(reopened.lookup_sync(&second).unwrap().is_some());
    assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
    assert_immutable_content(&root.path().join(second.digest.hex()));
}
