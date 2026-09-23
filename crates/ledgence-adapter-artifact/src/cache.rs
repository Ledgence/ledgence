use crate::{
    ArtifactLimits,
    archive::{ArchivePlan, inspect, make_readonly, read_bounded, remove_tree},
    error::{AdapterError, Result},
    filesystem::open_regular,
    store::blocking,
};
use ledgence_worker_api::{
    ArtifactCache, Error, ErrorKind, PortFuture, PreparedArtifact, ProgramDescriptor,
    ProgramManifest,
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions, TryLockError},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};

/// A dedicated filesystem cache with exclusive process ownership. Clones share
/// that ownership. PreparedArtifact handles pin entries, including after this
/// cache is dropped. Eviction chooses the least recently accessed unpinned entry.
/// Hits revalidate persisted bytes, detecting accidental cache corruption.
/// Eviction renames an unpinned entry into `.evicting-<digest>` and persists that
/// rename before deleting files. Incomplete deletion retains its remaining-byte
/// charge (or its prior reservation when unreadable), is retried before further
/// publication, and is cleaned on reopen before entries become available.
/// Staging reserves its planned content bytes before writing. Failed rollback
/// retains a pending stage and its remaining-byte charge, reconciled before a
/// later publication; verified cache hits and their pins remain available.
#[derive(Clone)]
pub struct FileArtifactCache {
    inner: Arc<Inner>,
}
struct Inner {
    owner: Arc<Owner>,
    limits: ArtifactLimits,
    state: Mutex<State>,
}
struct Owner {
    root: PathBuf,
    _lock: File,
}
impl Drop for Owner {
    fn drop(&mut self) {
        // Cache clones and artifact pins retain this Arc. Once its final owner
        // drops, explicitly release the logical cache lock: a descriptor
        // inherited during process creation can outlive our File handle.
        loop {
            match self._lock.unlock() {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                _ => break,
            }
        }
    }
}
struct Pin {
    _owner: Arc<Owner>,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<String, Entry>,
    // Tombstones retain their byte charge until their remaining files are gone.
    evicting: BTreeMap<String, u64>,
    staging: BTreeMap<String, u64>,
    clock: u64,
    bytes: u64,
}
struct Entry {
    descriptor: ProgramDescriptor,
    manifest: ProgramManifest,
    bytes: u64,
    touched: u64,
    pin: Weak<Pin>,
}
impl FileArtifactCache {
    pub fn new(
        root: impl Into<PathBuf>,
        limits: ArtifactLimits,
    ) -> ledgence_worker_api::Result<Self> {
        limits.validate()?;
        Self::open(root.into(), limits).map_err(Error::from)
    }
    fn open(root: PathBuf, limits: ArtifactLimits) -> Result<Self> {
        if !root.exists() {
            fs::create_dir_all(&root)?;
        }
        if fs::symlink_metadata(&root)?.file_type().is_symlink() {
            return Err(AdapterError::Invalid(
                "cache root cannot be a symlink".into(),
            ));
        }
        let root = fs::canonicalize(root)?;
        let lock_path = root.join(".lock");
        if fs::symlink_metadata(&lock_path)
            .is_ok_and(|m| !m.is_file() || m.file_type().is_symlink())
        {
            return Err(AdapterError::Invalid("invalid cache lock file".into()));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) => {
                    return Err(AdapterError::Public(Error::new(
                        ErrorKind::Unavailable,
                        "cache directory already has an owner",
                    )));
                }
                Err(TryLockError::Error(error))
                    if error.kind() == std::io::ErrorKind::Interrupted =>
                {
                    continue;
                }
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
        let owner = Arc::new(Owner { root, _lock: lock });
        let mut state = State::default();
        for item in fs::read_dir(&owner.root)? {
            let item = item?;
            let name = item
                .file_name()
                .into_string()
                .map_err(|_| AdapterError::Invalid("unexpected cache entry".into()))?;
            if name == ".lock" {
                continue;
            }
            if name.starts_with(".staging-")
                || name.strip_prefix(".evicting-").is_some_and(is_digest_name)
            {
                // Persist any previously interrupted rename before deleting its
                // target. A cleanup failure aborts opening, admitting no work.
                File::open(&owner.root)?.sync_all()?;
                remove_tree(&item.path())?;
                File::open(&owner.root)?.sync_all()?;
                continue;
            }
            if !is_digest_name(&name) {
                return Err(AdapterError::Invalid(
                    "cache must use a dedicated directory".into(),
                ));
            }
            let (descriptor, mut plan, descriptor_bytes) = read_entry(&item.path(), &limits)?;
            if descriptor.digest.hex() != name {
                return Err(AdapterError::Invalid(
                    "cache directory differs from digest".into(),
                ));
            }
            plan.verify(&item.path().join("content"))?;
            if prepare_cache_wrapper(&item.path())? {
                File::open(item.path())?.sync_all()?;
            }
            let bytes = entry_size(&descriptor, plan.expanded_bytes, descriptor_bytes)?;
            state.bytes = state
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| AdapterError::Limit("cache size overflow".into()))?;
            state.entries.insert(
                name,
                Entry {
                    descriptor,
                    manifest: plan.manifest,
                    bytes,
                    touched: 0,
                    pin: Weak::new(),
                },
            );
        }
        evict(&owner, &mut state, 0, limits.max_cache_bytes)?;
        Ok(Self {
            inner: Arc::new(Inner {
                owner,
                limits,
                state: Mutex::new(state),
            }),
        })
    }
    fn lookup_sync(&self, descriptor: &ProgramDescriptor) -> Result<Option<PreparedArtifact>> {
        self.inner.limits.descriptor(descriptor)?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| AdapterError::Invalid("cache state poisoned".into()))?;
        self.hit(&mut state, descriptor)
    }
    fn hit(
        &self,
        state: &mut State,
        descriptor: &ProgramDescriptor,
    ) -> Result<Option<PreparedArtifact>> {
        let Some(entry) = state.entries.get_mut(descriptor.digest.hex()) else {
            return Ok(None);
        };
        if entry.descriptor != *descriptor {
            return Err(AdapterError::Invalid(
                "descriptor differs from cached immutable identity".into(),
            ));
        }
        let directory = self.inner.owner.root.join(descriptor.digest.hex());
        let (stored, mut plan, _) = read_entry(&directory, &self.inner.limits)?;
        if stored != *descriptor {
            return Err(AdapterError::Invalid("cached descriptor changed".into()));
        }
        plan.verify(&directory.join("content"))?;
        state.clock = state.clock.saturating_add(1);
        entry.touched = state.clock;
        Ok(Some(pin_entry(&self.inner.owner, entry)))
    }
    fn publish_sync(
        &self,
        descriptor: ProgramDescriptor,
        archive: Vec<u8>,
    ) -> Result<PreparedArtifact> {
        self.publish_with_sync(descriptor, archive, |directory| {
            File::open(directory)?.sync_all()
        })
    }
    fn publish_with_sync(
        &self,
        descriptor: ProgramDescriptor,
        archive: Vec<u8>,
        sync_parent: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<PreparedArtifact> {
        self.publish_with_stage(descriptor, archive, finish_stage, remove_tree, sync_parent)
    }
    fn publish_with_stage(
        &self,
        descriptor: ProgramDescriptor,
        archive: Vec<u8>,
        finish: impl FnOnce(&mut ArchivePlan, &Path, &Path) -> Result<()>,
        remove: impl Fn(&Path) -> Result<()>,
        sync_parent: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> Result<PreparedArtifact> {
        let mut plan = inspect(archive.clone(), &descriptor, &self.inner.limits)?;
        let encoded = serde_json::to_vec(&descriptor)?;
        if encoded.len() as u64 > self.inner.limits.max_descriptor_bytes {
            return Err(AdapterError::Limit("descriptor bytes".into()));
        }
        let bytes = entry_size(&descriptor, plan.expanded_bytes, encoded.len() as u64)?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| AdapterError::Invalid("cache state poisoned".into()))?;
        clean_staging(&self.inner.owner, &mut state, &remove)?;
        if let Some(prepared) = self.hit(&mut state, &descriptor)? {
            return Ok(prepared);
        }
        evict(
            &self.inner.owner,
            &mut state,
            bytes,
            self.inner.limits.max_cache_bytes,
        )?;
        let temporary = tempfile::Builder::new()
            .prefix(".staging-")
            .tempdir_in(&self.inner.owner.root)?;
        let mut stage = Stage {
            path: temporary.keep(),
            fallback: true,
        };
        let name = stage
            .path
            .file_name()
            .expect("generated staging name")
            .to_str()
            .expect("generated ASCII staging name")
            .to_owned();
        // Reserve before any file write: partial writes/extraction must remain
        // charged even if rollback or its accounting traversal also fails.
        state.bytes += bytes;
        state.staging.insert(name.clone(), bytes);
        let final_path = self.inner.owner.root.join(descriptor.digest.hex());
        let result = (|| {
            write_immutable(&stage.path.join("artifact.zip"), &archive)?;
            write_immutable(&stage.path.join("descriptor.json"), &encoded)?;
            finish(&mut plan, &stage.path, &final_path)
        })();
        // Normal recovery is explicit. The state keeps ownership on failure;
        // Drop is only a best-effort fallback for unexpected unwinding.
        stage.fallback = false;
        if let Err(original) = result {
            return match clean_staging(&self.inner.owner, &mut state, &remove) {
                Ok(()) => Err(original),
                Err(cleanup) => {
                    let mut error = Error::from(original);
                    error.message = format!(
                        "{}; staging cleanup pending for {}: {cleanup}",
                        error.message,
                        stage.path.display()
                    );
                    Err(AdapterError::Public(error))
                }
            };
        }
        state.staging.remove(&name);
        state.clock = state.clock.saturating_add(1);
        let mut entry = Entry {
            descriptor,
            manifest: plan.manifest,
            bytes,
            touched: state.clock,
            pin: Weak::new(),
        };
        let prepared = pin_entry(&self.inner.owner, &mut entry);
        state
            .entries
            .insert(entry.descriptor.digest.hex().to_owned(), entry);
        // Rename already made a complete entry visible. Register it before the
        // final durability barrier, so a failed parent sync remains recoverable
        // through lookup/retry without restarting the cache owner.
        sync_parent(&self.inner.owner.root)?;
        Ok(prepared)
    }
}
impl ArtifactCache for FileArtifactCache {
    fn lookup<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
    ) -> PortFuture<'a, Option<PreparedArtifact>> {
        let cache = self.clone();
        let descriptor = descriptor.clone();
        Box::pin(async move { blocking(move || cache.lookup_sync(&descriptor)).await })
    }
    fn publish<'a>(
        &'a self,
        descriptor: &'a ProgramDescriptor,
        archive: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        let cache = self.clone();
        let descriptor = descriptor.clone();
        Box::pin(async move { blocking(move || cache.publish_sync(descriptor, archive)).await })
    }
}
fn is_digest_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
fn pin_entry(owner: &Arc<Owner>, entry: &mut Entry) -> PreparedArtifact {
    let pin = entry.pin.upgrade().unwrap_or_else(|| {
        Arc::new(Pin {
            _owner: Arc::clone(owner),
        })
    });
    entry.pin = Arc::downgrade(&pin);
    PreparedArtifact::new(
        owner
            .root
            .join(entry.descriptor.digest.hex())
            .join("content"),
        entry.manifest.clone(),
        entry.descriptor.digest.clone(),
        pin,
    )
}
fn evict(owner: &Owner, state: &mut State, required: u64, budget: u64) -> Result<()> {
    evict_with_remove(owner, state, required, budget, remove_tree)
}
fn evict_with_remove(
    owner: &Owner,
    state: &mut State,
    required: u64,
    budget: u64,
    remove: impl Fn(&Path) -> Result<()>,
) -> Result<()> {
    if required > budget {
        return Err(AdapterError::Limit(
            "artifact exceeds entire cache budget".into(),
        ));
    }
    // Retry incomplete deletion even if this request otherwise fits. No new
    // publication can ignore space still held by a failed cleanup.
    clean_evictions(owner, state, &remove)?;
    while state.bytes > budget - required {
        let victim = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.pin.strong_count() == 0)
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(key, _)| key.clone())
            .ok_or(AdapterError::Pressure)?;
        let tombstone = format!(".evicting-{victim}");
        let destination = owner.root.join(&tombstone);
        match fs::symlink_metadata(&destination) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(AdapterError::Invalid("unexpected eviction target".into())),
        }
        // A failed rename leaves the complete live entry and its accounting
        // untouched. Once renamed it is never offered as a cache hit again.
        fs::rename(owner.root.join(&victim), destination)?;
        if let Some(entry) = state.entries.remove(&victim) {
            state.evicting.insert(tombstone, entry.bytes);
        }
        clean_evictions(owner, state, &remove)?;
    }
    Ok(())
}
fn clean_evictions(
    owner: &Owner,
    state: &mut State,
    remove: &impl Fn(&Path) -> Result<()>,
) -> Result<()> {
    clean_pending(
        owner,
        &mut state.bytes,
        &mut state.evicting,
        remove,
        &remaining_bytes,
    )
}
fn clean_staging(
    owner: &Owner,
    state: &mut State,
    remove: &impl Fn(&Path) -> Result<()>,
) -> Result<()> {
    clean_pending(
        owner,
        &mut state.bytes,
        &mut state.staging,
        remove,
        &remaining_bytes,
    )
}
fn clean_pending(
    owner: &Owner,
    bytes: &mut u64,
    pending: &mut BTreeMap<String, u64>,
    remove: &impl Fn(&Path) -> Result<()>,
    measure: &impl Fn(&Path) -> std::io::Result<u64>,
) -> Result<()> {
    while let Some((name, charged)) = pending.first_key_value() {
        let (name, charged) = (name.clone(), *charged);
        let path = owner.root.join(&name);
        // Persist the private cleanup name before recursive deletion. For an
        // eviction this commits the rename, preventing a crash from exposing
        // partially deleted content under the former live digest name.
        File::open(&owner.root)?.sync_all()?;
        let result = match remove(&path) {
            Err(AdapterError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound
                    && matches!(fs::symlink_metadata(&path), Err(missing) if missing.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(())
            }
            result => result,
        };
        // Failed removal may already have deleted some files. Count remaining
        // regular-file bytes; if traversal itself fails, conservatively retain
        // the previous reservation rather than falsely freeing cache capacity.
        let remaining = match &result {
            Ok(()) => 0,
            Err(_) => measure(&path).unwrap_or(charged),
        };
        *bytes = bytes
            .checked_sub(charged)
            .and_then(|bytes| bytes.checked_add(remaining))
            .ok_or_else(|| AdapterError::Limit("cache size overflow".into()))?;
        pending.insert(name.clone(), remaining);
        result?;
        File::open(&owner.root)?.sync_all()?;
        pending.remove(&name);
    }
    Ok(())
}
fn remaining_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        let mut bytes = 0u64;
        for entry in fs::read_dir(path)? {
            bytes = bytes
                .checked_add(remaining_bytes(&entry?.path())?)
                .ok_or_else(|| std::io::Error::other("cache size overflow"))?;
        }
        return Ok(bytes);
    }
    Ok(0)
}

fn read_entry(
    directory: &Path,
    limits: &ArtifactLimits,
) -> Result<(ProgramDescriptor, crate::archive::ArchivePlan, u64)> {
    if fs::symlink_metadata(directory)?.file_type().is_symlink() {
        return Err(AdapterError::Invalid(
            "cache entry cannot be a symlink".into(),
        ));
    }
    let encoded = read_regular(
        &directory.join("descriptor.json"),
        limits.max_descriptor_bytes,
    )?;
    let descriptor: ProgramDescriptor = serde_json::from_slice(&encoded)?;
    limits.descriptor(&descriptor)?;
    let archive = read_regular(&directory.join("artifact.zip"), descriptor.size)?;
    let plan = inspect(archive, &descriptor, limits)?;
    let names: std::collections::BTreeSet<_> = fs::read_dir(directory)?
        .map(|item| item.map(|e| e.file_name()))
        .collect::<std::io::Result<_>>()?;
    let expected = ["artifact.zip", "descriptor.json", "content"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    if names != expected {
        return Err(AdapterError::Invalid("unexpected cache entry files".into()));
    }
    Ok((descriptor, plan, encoded.len() as u64))
}
fn read_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    if file.metadata()?.len() > limit {
        return Err(AdapterError::Limit("cached bytes".into()));
    }
    read_bounded(&mut file, limit)
}
// Earlier caches also made their wrapper read-only. Normalize it after
// verification and under exclusive ownership, before eviction can rename it.
// Program content permissions and immutable file bytes remain unchanged.
fn prepare_cache_wrapper(path: &Path) -> Result<bool> {
    let permissions = fs::metadata(path)?.permissions();
    #[cfg(unix)]
    let desired = {
        use std::os::unix::fs::PermissionsExt;
        (permissions.mode() & 0o7777 != 0o700).then(|| fs::Permissions::from_mode(0o700))
    };
    #[cfg(not(unix))]
    let desired = permissions.readonly().then(|| {
        let mut writable = permissions;
        writable.set_readonly(false);
        writable
    });
    if let Some(desired) = desired {
        fs::set_permissions(path, desired)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
fn write_immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create_new(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    make_readonly(path, false)
}
fn entry_size(descriptor: &ProgramDescriptor, expanded: u64, metadata: u64) -> Result<u64> {
    descriptor
        .size
        .checked_add(expanded)
        .and_then(|v| v.checked_add(metadata))
        .ok_or_else(|| AdapterError::Limit("cache size overflow".into()))
}
fn finish_stage(plan: &mut ArchivePlan, stage: &Path, final_path: &Path) -> Result<()> {
    plan.extract(&stage.join("content"))?;
    // Keep the private wrapper owner-writable: macOS can reject renaming a
    // read-only directory. The program content and metadata files are already
    // read-only; only the wrapper itself needs owner write permission.
    prepare_cache_wrapper(stage)?;
    File::open(stage)?.sync_all()?;
    fs::rename(stage, final_path)?;
    Ok(())
}
struct Stage {
    path: PathBuf,
    fallback: bool,
}
impl Drop for Stage {
    fn drop(&mut self) {
        if self.fallback {
            let _ = remove_tree(&self.path);
        }
    }
}

#[cfg(test)]
mod staging_tests;

#[cfg(all(test, unix))]
mod permissions_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_worker_api::{Platform, ProgramRef, PythonRuntime};

    #[test]
    fn final_pin_drop_releases_cache_lock_even_with_an_inherited_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let cache = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
        let (descriptor, archive, _) = test_artifact("inherited-descriptor");
        let prepared = cache.publish_sync(descriptor, archive).unwrap();
        // A duplicate models the same open-file description retained across
        // process creation, without a timing-dependent fork/exec race.
        let inherited = cache.inner.owner._lock.try_clone().unwrap();
        drop(cache);
        assert_eq!(
            FileArtifactCache::new(root.path(), ArtifactLimits::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Unavailable
        );
        drop(prepared);
        let reopened = FileArtifactCache::new(root.path(), ArtifactLimits::default()).unwrap();
        // Releasing the old descriptor must not release the replacement owner's
        // independently acquired lock.
        drop(inherited);
        assert_eq!(
            FileArtifactCache::new(root.path(), ArtifactLimits::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Unavailable
        );
        drop(reopened);
        assert!(FileArtifactCache::new(root.path(), ArtifactLimits::default()).is_ok());
    }

    #[test]
    fn failed_parent_sync_preserves_a_recoverable_registered_entry() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        fs::create_dir(&source).unwrap();
        let manifest = ProgramManifest {
            schema_version: 1,
            program: ProgramRef {
                id: "sync-test".into(),
                version: "v1".into(),
            },
            runtime: PythonRuntime {
                kind: "python".into(),
                python: "3.12".into(),
                protocol: 1,
            },
            handler: "app:handle".into(),
            platform: Platform {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
            },
        };
        fs::write(
            source.join("ledgence-program.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        fs::write(source.join("app.py"), b"pass").unwrap();
        let store = root.path().join("store");
        let descriptor =
            crate::publish_directory(&source, &store, &ArtifactLimits::default()).unwrap();
        let archive = fs::read(
            store
                .join("blobs")
                .join(format!("{}.zip", descriptor.digest.hex())),
        )
        .unwrap();
        let cache_root = root.path().join("cache");
        let cache = FileArtifactCache::new(&cache_root, ArtifactLimits::default()).unwrap();
        let error = cache
            .publish_with_sync(descriptor.clone(), archive.clone(), |_| {
                Err(std::io::Error::other(
                    "injected parent directory sync failure",
                ))
            })
            .unwrap_err();
        assert!(matches!(error, AdapterError::Io(_)));
        assert!(cache.inner.state.lock().unwrap().bytes > 0);
        let hit = cache.lookup_sync(&descriptor).unwrap().unwrap();
        assert_eq!(fs::read(hit.root().join("app.py")).unwrap(), b"pass");
        let retry = cache.publish_sync(descriptor, archive).unwrap();
        assert_eq!(hit.root(), retry.root());
        assert_eq!(fs::read_dir(cache_root).unwrap().count(), 2);
    }

    pub(super) fn test_artifact(version: &str) -> (ProgramDescriptor, Vec<u8>, u64) {
        use std::io::Cursor;
        use zip::{ZipWriter, write::SimpleFileOptions};
        let manifest = ProgramManifest {
            schema_version: 1,
            program: ProgramRef {
                id: "eviction-test".into(),
                version: version.into(),
            },
            runtime: PythonRuntime {
                kind: "python".into(),
                python: "3.12".into(),
                protocol: 1,
            },
            handler: "app:handle".into(),
            platform: Platform {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
            },
        };
        let json = serde_json::to_vec(&manifest).unwrap();
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        for (name, bytes) in [
            ("ledgence-program.json", json.as_slice()),
            ("app.py", b"pass".as_slice()),
        ] {
            writer
                .start_file(name, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(bytes).unwrap();
        }
        let archive = writer.finish().unwrap().into_inner();
        let descriptor = ProgramDescriptor {
            program: manifest.program,
            digest: ledgence_worker_api::Digest(format!(
                "sha256:{}",
                crate::archive::digest_hex(&archive)
            )),
            size: archive.len() as u64,
        };
        (descriptor, archive, json.len() as u64 + 4)
    }

    #[cfg(unix)]
    fn interrupted_eviction(restart: bool) {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let (descriptor, archive, expanded) = test_artifact("v1");
        let charge = entry_size(
            &descriptor,
            expanded,
            serde_json::to_vec(&descriptor).unwrap().len() as u64,
        )
        .unwrap();
        let limits = ArtifactLimits {
            max_cache_bytes: charge,
            ..Default::default()
        };
        let cache = FileArtifactCache::new(root.path(), limits.clone()).unwrap();
        drop(
            cache
                .publish_sync(descriptor.clone(), archive.clone())
                .unwrap(),
        );
        {
            let mut state = cache.inner.state.lock().unwrap();
            let error = evict_with_remove(&cache.inner.owner, &mut state, charge, charge, |path| {
                assert!(
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with(".evicting-")
                );
                assert!(!root.path().join(descriptor.digest.hex()).exists());
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
                remove_tree(&path.join("content"))?;
                Err(AdapterError::Io(std::io::Error::other(
                    "injected partial deletion failure",
                )))
            })
            .unwrap_err();
            assert!(matches!(error, AdapterError::Io(_)));
            assert!(state.entries.is_empty());
            assert_eq!(state.evicting.len(), 1);
            assert_eq!(state.bytes, charge - expanded);
            assert_eq!(state.evicting.values().copied().sum::<u64>(), state.bytes);
        }
        assert!(cache.lookup_sync(&descriptor).unwrap().is_none());
        let tombstone = root
            .path()
            .join(format!(".evicting-{}", descriptor.digest.hex()));
        assert!(tombstone.join("artifact.zip").is_file());
        let cache = if restart {
            drop(cache);
            let reopened = FileArtifactCache::new(root.path(), limits).unwrap();
            assert_eq!(reopened.inner.state.lock().unwrap().bytes, 0);
            assert!(!tombstone.exists());
            reopened
        } else {
            cache
        };
        let prepared = cache.publish_sync(descriptor.clone(), archive).unwrap();
        assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
        assert!(!tombstone.exists());
        assert!(cache.inner.state.lock().unwrap().evicting.is_empty());
        assert_eq!(cache.inner.state.lock().unwrap().bytes, charge);
        assert!(cache.lookup_sync(&descriptor).unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn failed_partial_eviction_keeps_remaining_bytes_charged_and_retries() {
        interrupted_eviction(false);
    }

    #[cfg(unix)]
    #[test]
    fn cache_restart_finishes_interrupted_eviction_before_loading_live_entries() {
        interrupted_eviction(true);
    }

    #[test]
    fn failed_eviction_rename_preserves_complete_live_entry() {
        let root = tempfile::tempdir().unwrap();
        let (descriptor, archive, expanded) = test_artifact("v1");
        let charge = entry_size(
            &descriptor,
            expanded,
            serde_json::to_vec(&descriptor).unwrap().len() as u64,
        )
        .unwrap();
        let cache = FileArtifactCache::new(
            root.path(),
            ArtifactLimits {
                max_cache_bytes: charge,
                ..Default::default()
            },
        )
        .unwrap();
        drop(cache.publish_sync(descriptor.clone(), archive).unwrap());
        // A filesystem collision must not overwrite a pending cleanup tree.
        let collision = root
            .path()
            .join(format!(".evicting-{}", descriptor.digest.hex()));
        fs::create_dir(&collision).unwrap();
        fs::write(collision.join("sentinel"), b"preserve").unwrap();
        {
            let mut state = cache.inner.state.lock().unwrap();
            assert!(evict(&cache.inner.owner, &mut state, charge, charge).is_err());
            assert_eq!(state.bytes, charge);
            assert_eq!(state.entries.len(), 1);
            assert!(state.evicting.is_empty());
        }
        assert_eq!(fs::read(collision.join("sentinel")).unwrap(), b"preserve");
        let prepared = cache.lookup_sync(&descriptor).unwrap().unwrap();
        assert_eq!(fs::read(prepared.root().join("app.py")).unwrap(), b"pass");
    }
}
