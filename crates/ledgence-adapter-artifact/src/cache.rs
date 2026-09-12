use crate::{
    ArtifactLimits,
    archive::{inspect, make_readonly, read_bounded, remove_tree},
    error::{AdapterError, Result},
    store::blocking,
};
use ledgence_worker_api::{
    ArtifactCache, Error, ErrorKind, PortFuture, PreparedArtifact, ProgramDescriptor,
    ProgramManifest,
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};

/// A dedicated filesystem cache with exclusive process ownership. Clones share
/// that ownership. PreparedArtifact handles pin entries, including after this
/// cache is dropped. Eviction chooses the least recently accessed unpinned entry.
/// Hits revalidate persisted bytes, detecting accidental cache corruption.
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
struct Pin {
    _owner: Arc<Owner>,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<String, Entry>,
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
        lock.try_lock().map_err(|_| {
            AdapterError::Public(Error::new(
                ErrorKind::Unavailable,
                "cache directory already has an owner",
            ))
        })?;
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
            if name.starts_with(".staging-") {
                remove_tree(&item.path())?;
                continue;
            }
            if name.len() != 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
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
        let stage = Stage(temporary);
        write_immutable(&stage.0.path().join("artifact.zip"), &archive)?;
        write_immutable(&stage.0.path().join("descriptor.json"), &encoded)?;
        plan.extract(&stage.0.path().join("content"))?;
        make_readonly(stage.0.path(), true)?;
        File::open(stage.0.path())?.sync_all()?;
        let final_path = self.inner.owner.root.join(descriptor.digest.hex());
        fs::rename(stage.0.path(), &final_path)?;
        state.clock = state.clock.saturating_add(1);
        let mut entry = Entry {
            descriptor,
            manifest: plan.manifest,
            bytes,
            touched: state.clock,
            pin: Weak::new(),
        };
        let prepared = pin_entry(&self.inner.owner, &mut entry);
        state.bytes += bytes;
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
    if required > budget {
        return Err(AdapterError::Limit(
            "artifact exceeds entire cache budget".into(),
        ));
    }
    while state.bytes > budget - required {
        let victim = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.pin.strong_count() == 0)
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(key, _)| key.clone())
            .ok_or(AdapterError::Pressure)?;
        remove_tree(&owner.root.join(&victim))?;
        if let Some(entry) = state.entries.remove(&victim) {
            state.bytes -= entry.bytes;
        }
    }
    Ok(())
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
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AdapterError::Invalid("cached file must be regular".into()));
    }
    if metadata.len() > limit {
        return Err(AdapterError::Limit("cached bytes".into()));
    }
    read_bounded(&mut File::open(path)?, limit)
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
struct Stage(tempfile::TempDir);
impl Drop for Stage {
    fn drop(&mut self) {
        if self.0.path().exists() {
            let _ = remove_tree(self.0.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_worker_api::{Platform, ProgramRef, PythonRuntime};

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
}
