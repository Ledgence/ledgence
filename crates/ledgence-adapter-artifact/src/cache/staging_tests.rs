use super::*;

fn artifact(version: &str) -> (ProgramDescriptor, Vec<u8>, u64) {
    let (descriptor, archive, expanded) = super::tests::test_artifact(version);
    let charged = entry_size(
        &descriptor,
        expanded,
        serde_json::to_vec(&descriptor).unwrap().len() as u64,
    )
    .unwrap();
    (descriptor, archive, charged)
}
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}
fn failed_rename(plan: &mut ArchivePlan, stage: &Path, _: &Path) -> Result<()> {
    plan.extract(&stage.join("content"))?;
    prepare_cache_wrapper(stage)?;
    sync_directory(stage)?;
    Err(std::io::Error::other("injected publication rename failure").into())
}
fn failed_remove(_: &Path) -> Result<()> {
    Err(std::io::Error::other("injected rollback removal failure").into())
}
fn pending_path(cache: &FileArtifactCache) -> PathBuf {
    let state = cache.inner.state.lock().unwrap();
    assert_eq!(state.staging.len(), 1);
    cache
        .inner
        .owner
        .root
        .join(state.staging.first_key_value().unwrap().0)
}
fn assert_accounted(cache: &FileArtifactCache, pending: u64, live: u64) {
    let state = cache.inner.state.lock().unwrap();
    assert_eq!(state.staging.values().sum::<u64>(), pending);
    assert_eq!(
        state.entries.values().map(|entry| entry.bytes).sum::<u64>(),
        live
    );
    assert!(state.evicting.is_empty());
    assert_eq!(state.bytes, pending + live);
    assert_eq!(
        remaining_bytes(&cache.inner.owner.root).unwrap(),
        state.bytes
    );
}

#[test]
fn failed_staging_rollback_is_charged_and_retried_without_disturbing_pinned_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    let (pinned_descriptor, pinned_archive, pinned_charge) = artifact("pinned");
    let pinned = cache
        .publish_sync(pinned_descriptor.clone(), pinned_archive)
        .unwrap();
    let (descriptor, archive, charge) = artifact("recover");
    let error: Error = cache
        .publish_with_stage(
            descriptor.clone(),
            archive.clone(),
            failed_rename,
            failed_remove,
            sync_directory,
        )
        .unwrap_err()
        .into();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.message.contains("publication rename failure"));
    assert!(error.message.contains("staging cleanup pending"));
    assert!(error.message.contains("rollback removal failure"));
    let stage = pending_path(&cache);
    assert_eq!(remaining_bytes(&stage).unwrap(), charge);
    assert_accounted(&cache, charge, pinned_charge);
    assert!(cache.lookup_sync(&descriptor).unwrap().is_none());
    assert!(cache.lookup_sync(&pinned_descriptor).unwrap().is_some());

    // An unrepaired filesystem must not cause another staging allocation.
    let retry: Error = cache
        .publish_with_stage(
            descriptor.clone(),
            archive.clone(),
            |_, _, _| panic!("pending cleanup must finish before new materialization"),
            failed_remove,
            sync_directory,
        )
        .unwrap_err()
        .into();
    assert_eq!(retry.kind, ErrorKind::Io);
    assert_eq!(pending_path(&cache), stage);
    assert_accounted(&cache, charge, pinned_charge);

    let recovered = cache.publish_sync(descriptor.clone(), archive).unwrap();
    assert!(!stage.exists());
    assert!(cache.inner.state.lock().unwrap().staging.is_empty());
    assert_accounted(&cache, 0, pinned_charge + charge);
    assert_eq!(fs::read(pinned.root().join("app.py")).unwrap(), b"pass");
    assert_eq!(fs::read(recovered.root().join("app.py")).unwrap(), b"pass");
    assert!(cache.lookup_sync(&descriptor).unwrap().is_some());
}

#[test]
fn partial_extraction_and_partial_rollback_keep_only_remaining_bytes_charged() {
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    let (descriptor, archive, charge) = artifact("partial");
    let error: Error = cache
        .publish_with_stage(
            descriptor.clone(),
            archive.clone(),
            |plan, stage, _| {
                plan.extract_with_create(&stage.join("content"), |file| {
                    if file.file_name().unwrap() == "app.py" {
                        assert!(stage.join("content/ledgence-program.json").is_file());
                        return Err(std::io::Error::other(
                            "injected extracted file creation failure",
                        ));
                    }
                    File::create_new(file)
                })
            },
            |stage| {
                fs::remove_file(stage.join("artifact.zip"))?;
                Err(
                    std::io::Error::other("injected rollback failure after removing archive")
                        .into(),
                )
            },
            sync_directory,
        )
        .unwrap_err()
        .into();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(error.message.contains("extracted file creation failure"));
    assert!(
        error
            .message
            .contains("rollback failure after removing archive")
    );
    let stage = pending_path(&cache);
    assert!(!stage.join("content/app.py").exists());
    assert!(!stage.join("artifact.zip").exists());
    let remaining = charge - descriptor.size - 4; // app.py was never created.
    assert_accounted(&cache, remaining, 0);
    assert!(cache.lookup_sync(&descriptor).unwrap().is_none());
    let prepared = cache.publish_sync(descriptor, archive).unwrap();
    assert!(!stage.exists());
    assert_accounted(&cache, 0, charge);
    assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
}

#[cfg(unix)]
#[test]
fn unreadable_rollback_accounting_retains_its_prior_charge_until_reconciliation() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    let (descriptor, archive, charge) = artifact("unreadable");
    cache
        .publish_with_stage(
            descriptor.clone(),
            archive.clone(),
            failed_rename,
            failed_remove,
            sync_directory,
        )
        .unwrap_err();
    let stage = pending_path(&cache);
    {
        let mut state = cache.inner.state.lock().unwrap();
        let State { bytes, staging, .. } = &mut *state;
        clean_pending(
            &cache.inner.owner,
            bytes,
            staging,
            &|path| {
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
                fs::remove_file(path.join("artifact.zip"))?;
                Err(std::io::Error::other("injected partial removal failure").into())
            },
            &|_| {
                Err(std::io::Error::other(
                    "injected accounting traversal failure",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(*bytes, charge);
        assert_eq!(staging.values().sum::<u64>(), charge);
        assert_eq!(remaining_bytes(&stage).unwrap(), charge - descriptor.size);
    }
    // The conservative reservation is released only by a successful cleanup.
    let prepared = cache.publish_sync(descriptor, archive).unwrap();
    assert!(!stage.exists());
    assert_accounted(&cache, 0, charge);
    assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
}

#[test]
fn cache_reopen_cleans_pending_staging_before_admitting_new_content() {
    let root = tempfile::tempdir().unwrap();
    let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    let (descriptor, archive, charge) = artifact("reopen");
    cache
        .publish_with_stage(
            descriptor.clone(),
            archive.clone(),
            failed_rename,
            failed_remove,
            sync_directory,
        )
        .unwrap_err();
    let stage = pending_path(&cache);
    assert_accounted(&cache, charge, 0);
    drop(cache);
    let reopened = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
    assert!(!stage.exists());
    assert_accounted(&reopened, 0, 0);
    let prepared = reopened.publish_sync(descriptor, archive).unwrap();
    assert_accounted(&reopened, 0, charge);
    assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
}
