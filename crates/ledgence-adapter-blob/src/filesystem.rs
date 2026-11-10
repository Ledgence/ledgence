use crate::{CHUNK_BYTES, check_deadline, chunk_limit, invalid, random_id, unavailable};
use ledgence_orchestration_api::{
    ArtifactObjectDescriptor, ArtifactReader, ArtifactStoreIdentity, BlobDeleteOutcome,
    BlobPutOutcome, BlobRead, ContractFuture, ExecutionArtifactBlobStore, Result,
};
use ledgence_worker_api::Digest;
use sha2::{Digest as _, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// Durable local bytes. Clones share two atomic-write staging slots. Compose one
/// instance per orchestrator; independent instances do not share that IO limit.
#[derive(Clone)]
pub struct FileExecutionArtifactStore {
    inner: Arc<Store>,
}
struct Store {
    identity: ArtifactStoreIdentity,
    root: PathBuf,
    staging: PathBuf,
    _stage_lock: File,
    stages: Arc<Semaphore>,
    writes: Mutex<WriteState>,
    finished: Notify,
}
#[derive(Default)]
struct WriteState {
    closed: bool,
    active: usize,
}
struct WriteGuard(Arc<Store>);
impl Drop for WriteGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.writes.lock() {
            state.active -= 1;
        }
        self.0.finished.notify_waiters();
    }
}
struct Stage {
    path: PathBuf,
    file: File,
    _permit: OwnedSemaphorePermit,
    _store: Arc<Store>,
    _job: Arc<WriteGuard>,
}
impl FileExecutionArtifactStore {
    pub async fn open(store_id: impl Into<String>, path: impl AsRef<Path>) -> Result<Self> {
        let store_id = store_id.into();
        let path = path.as_ref().to_owned();
        let inner = tokio::task::spawn_blocking(move || initialize(store_id, &path))
            .await
            .map_err(|_| unavailable("artifact filesystem initialization interrupted"))??;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
    /// Diagnostic location only. Store identity is bound to its durable marker.
    pub fn directory(&self) -> &Path {
        &self.inner.root
    }
    fn validate(&self, object: &ArtifactObjectDescriptor) -> Result<()> {
        object.validate()?;
        if object.store != self.inner.identity {
            return Err(invalid("artifact belongs to a different store"));
        }
        Ok(())
    }
}
impl ExecutionArtifactBlobStore for FileExecutionArtifactStore {
    fn identity(&self) -> &ArtifactStoreIdentity {
        &self.inner.identity
    }
    fn put_once<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        reader: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> ContractFuture<'a, BlobPutOutcome> {
        Box::pin(async move {
            self.validate(object)?;
            let deadline = crate::transfer_deadline(deadline);
            check_deadline(deadline)?;
            // Register ownership while holding the admission lock. Drain cannot
            // observe zero jobs between admitting a write and spawning it.
            let (permit, job) = {
                let mut state = self
                    .inner
                    .writes
                    .lock()
                    .map_err(|_| unavailable("artifact write state unavailable"))?;
                if state.closed {
                    return Err(unavailable("artifact store is draining"));
                }
                let permit = self
                    .inner
                    .stages
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ledgence_orchestration_api::ContractError::Busy)?;
                state.active += 1;
                (permit, Arc::new(WriteGuard(self.inner.clone())))
            };
            let store = self.inner.clone();
            let object = object.clone();
            // The task owns staging and its slot until physical IO and cleanup
            // finish, even when the caller times out or drops this future.
            let operation = tokio::spawn(async move {
                write_once(store, object, reader, deadline, permit, job).await
            });
            match tokio::time::timeout_at(deadline.into(), operation).await {
                Ok(Ok(result)) => result,
                _ => Ok(BlobPutOutcome::Uncertain),
            }
        })
    }
    fn drain(&self, deadline: Instant) -> ContractFuture<'_, ()> {
        Box::pin(async move {
            self.inner
                .writes
                .lock()
                .map_err(|_| unavailable("artifact write state unavailable"))?
                .closed = true;
            self.inner.stages.close();
            loop {
                let notified = self.inner.finished.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self
                    .inner
                    .writes
                    .lock()
                    .map_err(|_| unavailable("artifact write state unavailable"))?
                    .active
                    == 0
                {
                    return Ok(());
                }
                tokio::time::timeout_at(deadline.into(), notified)
                    .await
                    .map_err(|_| {
                        unavailable("artifact store drain incomplete; writes or cleanup retained")
                    })?;
            }
        })
    }
    fn open<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        deadline: Instant,
    ) -> ContractFuture<'a, Option<BlobRead>> {
        Box::pin(async move {
            self.validate(object)?;
            let deadline = crate::transfer_deadline(deadline);
            check_deadline(deadline)?;
            let store = self.inner.clone();
            let key = object.key.clone();
            let file = tokio::time::timeout_at(
                deadline.into(),
                tokio::task::spawn_blocking(move || {
                    let path = checked_object_path(&store.root, &key, false)?;
                    match File::open(path) {
                        Ok(file) if file.metadata().is_ok_and(|m| m.is_file()) => Ok(Some(file)),
                        Ok(_) => Err(unavailable("artifact object is not a regular file")),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                        Err(_) => Err(unavailable("artifact object open failed")),
                    }
                }),
            )
            .await
            .map_err(|_| unavailable("artifact open deadline exceeded"))?
            .map_err(|_| unavailable("artifact open interrupted"))??;
            Ok(file.map(|file| BlobRead {
                reader: Box::new(FileReader::new(file, object.metadata.clone(), deadline)),
            }))
        })
    }
    fn delete<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        deadline: Instant,
    ) -> ContractFuture<'a, BlobDeleteOutcome> {
        Box::pin(async move {
            self.validate(object)?;
            let deadline = crate::transfer_deadline(deadline);
            check_deadline(deadline)?;
            let store = self.inner.clone();
            let key = object.key.clone();
            tokio::time::timeout_at(
                deadline.into(),
                tokio::task::spawn_blocking(move || {
                    check_deadline(deadline)?;
                    let path = checked_object_path(&store.root, &key, false)?;
                    match std::fs::remove_file(&path) {
                        Ok(()) => {
                            sync_directory(
                                path.parent()
                                    .ok_or_else(|| unavailable("artifact parent missing"))?,
                            )?;
                            Ok(BlobDeleteOutcome::Deleted)
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            Ok(BlobDeleteOutcome::Absent)
                        }
                        Err(_) => Err(unavailable("artifact object removal failed")),
                    }
                }),
            )
            .await
            .map_err(|_| unavailable("artifact delete deadline exceeded; outcome unknown"))?
            .map_err(|_| unavailable("artifact delete interrupted; outcome unknown"))?
        })
    }
}
async fn write_once(
    store: Arc<Store>,
    object: ArtifactObjectDescriptor,
    mut reader: Box<dyn ArtifactReader>,
    deadline: Instant,
    permit: OwnedSemaphorePermit,
    job: Arc<WriteGuard>,
) -> Result<BlobPutOutcome> {
    let owner = store.clone();
    let stage_job = job.clone();
    let stage: Result<Stage> = tokio::task::spawn_blocking(move || {
        check_deadline(deadline)?;
        let path = owner.staging.join(format!("put-{}.tmp", random_id()?));
        let file = private_create(&path)?;
        Ok(Stage {
            path,
            file,
            _permit: permit,
            _store: owner,
            _job: stage_job,
        })
    })
    .await
    .map_err(|_| unavailable("artifact staging interrupted"))?;
    let mut stage: Stage = match stage {
        Ok(stage) => stage,
        Err(_) => return Ok(BlobPutOutcome::ConfirmedNoWrite),
    };
    let mut hash = Sha256::new();
    let mut size = 0_u64;
    loop {
        let chunk =
            match tokio::time::timeout_at(deadline.into(), reader.read_chunk(CHUNK_BYTES)).await {
                Ok(Ok(chunk)) if chunk.len() <= CHUNK_BYTES => chunk,
                _ => {
                    cleanup_stage(stage).await;
                    return Ok(BlobPutOutcome::ConfirmedNoWrite);
                }
            };
        if chunk.is_empty() {
            break;
        }
        size += chunk.len() as u64;
        if size > object.metadata.size {
            cleanup_stage(stage).await;
            return Ok(BlobPutOutcome::ConfirmedNoWrite);
        }
        hash.update(&chunk);
        let written = tokio::task::spawn_blocking(move || {
            let result = check_deadline(deadline).and_then(|()| {
                stage
                    .file
                    .write_all(&chunk)
                    .map_err(|_| unavailable("artifact staging write failed"))
            });
            (stage, result)
        })
        .await;
        match written {
            Ok((retained, Ok(()))) => stage = retained,
            Ok((retained, Err(_))) => {
                cleanup_stage(retained).await;
                return Ok(BlobPutOutcome::ConfirmedNoWrite);
            }
            Err(_) => return Ok(BlobPutOutcome::Uncertain),
        }
    }
    if size != object.metadata.size
        || format!("sha256:{:x}", hash.finalize()) != object.metadata.digest.0
    {
        cleanup_stage(stage).await;
        return Ok(BlobPutOutcome::ConfirmedNoWrite);
    }
    let result = tokio::task::spawn_blocking(move || {
        let result = (|| {
            check_deadline(deadline)?;
            stage
                .file
                .sync_all()
                .map_err(|_| unavailable("artifact staging sync failed"))?;
            let target = checked_object_path(&store.root, &object.key, true)?;
            check_deadline(deadline)?;
            // Same-filesystem link publishes a complete inode without replacing
            // any prior generation. Rename would overwrite an immutable key.
            match std::fs::hard_link(&stage.path, &target) {
                Ok(()) => {
                    sync_directory(
                        target
                            .parent()
                            .ok_or_else(|| unavailable("artifact parent missing"))?,
                    )?;
                    Ok(BlobPutOutcome::Created)
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    Ok(BlobPutOutcome::ConfirmedNoWrite)
                }
                Err(_) => Err(unavailable("artifact immutable publication failed")),
            }
        })();
        (stage, result)
    })
    .await;
    match result {
        Ok((stage, result)) => {
            cleanup_stage(stage).await;
            Ok(result.unwrap_or(BlobPutOutcome::Uncertain))
        }
        Err(_) => Ok(BlobPutOutcome::Uncertain),
    }
}
async fn cleanup_stage(stage: Stage) {
    // Failed deletion retains the stage and its permit until repaired. The
    // owning task remains detached from any cancelled HTTP request.
    let mut stage = stage;
    loop {
        let result = tokio::task::spawn_blocking(move || {
            let removed = match std::fs::remove_file(&stage.path) {
                Ok(()) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Err(_) => false,
            };
            (stage, removed)
        })
        .await;
        match result {
            Ok((_, true)) => return,
            Ok((retained, false)) => stage = retained,
            Err(_) => return,
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}
fn initialize(store_id: String, root: &Path) -> Result<Store> {
    std::fs::create_dir_all(root).map_err(|_| unavailable("cannot create artifact store"))?;
    let root = root
        .canonicalize()
        .map_err(|_| unavailable("cannot resolve artifact store"))?;
    reject_nonregular(&root.join(".ledgence-artifact-init.lock"))?;
    let initialization = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".ledgence-artifact-init.lock"))
        .map_err(|_| unavailable("cannot open artifact initialization lock"))?;
    initialization
        .try_lock()
        .map_err(|_| ledgence_orchestration_api::ContractError::Busy)?;
    // Initialization's exclusive lock proves no other initializer still owns
    // one of these marker staging files, including a crash before publication.
    for entry in
        std::fs::read_dir(&root).map_err(|_| unavailable("cannot inspect artifact store"))?
    {
        let entry = entry.map_err(|_| unavailable("cannot inspect artifact marker staging"))?;
        if entry.file_name().to_str().is_some_and(|name| {
            name.strip_prefix(".marker-")
                .and_then(|name| name.strip_suffix(".tmp"))
                .is_some_and(generated_id)
        }) {
            reject_nonregular(&entry.path())?;
            std::fs::remove_file(entry.path())
                .map_err(|_| unavailable("artifact marker staging cleanup failed"))?;
        }
    }
    let marker_path = root.join(".ledgence-artifact-store");
    reject_nonregular(&marker_path)?;
    if !marker_path.exists() {
        let temporary = root.join(format!(".marker-{}.tmp", random_id()?));
        let mut marker = private_create(&temporary)?;
        marker
            .write_all(random_id()?.as_bytes())
            .and_then(|()| marker.sync_all())
            .map_err(|_| unavailable("cannot persist artifact store marker"))?;
        let published = std::fs::hard_link(&temporary, &marker_path);
        let _ = std::fs::remove_file(&temporary);
        published.map_err(|_| unavailable("cannot publish artifact store marker"))?;
        sync_directory(&root)?;
    }
    let mut marker = Vec::new();
    File::open(&marker_path)
        .and_then(|file| file.take(33).read_to_end(&mut marker))
        .map_err(|_| unavailable("cannot read artifact store marker"))?;
    if marker.len() != 32
        || !marker
            .iter()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(unavailable("invalid artifact store marker"));
    }
    let identity = ArtifactStoreIdentity {
        store_id,
        fingerprint: Digest(format!(
            "sha256:{:x}",
            Sha256::digest([b"ledgence-filesystem-v1\n".as_slice(), &marker].concat())
        )),
    };
    identity.validate()?;
    let stages = root.join(".staging");
    ensure_directory(&stages)?;
    for entry in
        std::fs::read_dir(&stages).map_err(|_| unavailable("cannot inspect artifact staging"))?
    {
        let entry = entry.map_err(|_| unavailable("cannot inspect artifact staging entry"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !generated_id(name) {
            continue;
        }
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            return Err(unavailable("invalid artifact staging namespace"));
        }
        let owner_path = entry.path().join(".owner");
        reject_nonregular(&owner_path)?;
        let owner = match OpenOptions::new().read(true).write(true).open(&owner_path) {
            Ok(owner) => owner,
            // The initialization lock is still held, so a missing owner can
            // only be a crashed initializer, never a live partially built one.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                cleanup_stale_stage(&entry.path())?;
                continue;
            }
            Err(_) => return Err(unavailable("cannot open artifact staging owner")),
        };
        match owner.try_lock() {
            Ok(()) => cleanup_stale_stage(&entry.path())?,
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(_) => return Err(unavailable("cannot lock stale artifact staging")),
        }
    }
    let staging = stages.join(random_id()?);
    std::fs::create_dir(&staging)
        .map_err(|_| unavailable("cannot create artifact staging namespace"))?;
    let lock = private_create(&staging.join(".owner"))?;
    lock.try_lock()
        .map_err(|_| unavailable("cannot acquire artifact staging ownership"))?;
    sync_directory(&staging)?;
    sync_directory(&stages)?;
    Ok(Store {
        identity,
        root,
        staging,
        _stage_lock: lock,
        stages: Arc::new(Semaphore::new(2)),
        writes: Mutex::new(WriteState::default()),
        finished: Notify::new(),
    })
}
fn cleanup_stale_stage(path: &Path) -> Result<()> {
    for entry in
        std::fs::read_dir(path).map_err(|_| unavailable("cannot inspect stale artifact staging"))?
    {
        let entry = entry.map_err(|_| unavailable("cannot inspect stale artifact stage"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(unavailable("unknown stale artifact stage"));
        };
        if name == ".owner" {
            continue;
        }
        if !name
            .strip_prefix("put-")
            .and_then(|value| value.strip_suffix(".tmp"))
            .is_some_and(generated_id)
            || !entry.file_type().is_ok_and(|kind| kind.is_file())
        {
            return Err(unavailable("unrecognized stale artifact staging entry"));
        }
        std::fs::remove_file(entry.path())
            .map_err(|_| unavailable("stale artifact staging cleanup failed"))?;
    }
    match std::fs::remove_file(path.join(".owner")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(unavailable("stale artifact ownership cleanup failed")),
    }
    std::fs::remove_dir(path).map_err(|_| unavailable("stale artifact namespace cleanup failed"))
}
fn generated_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn ensure_directory(path: &Path) -> Result<()> {
    match std::fs::create_dir(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(unavailable("cannot create artifact directory")),
    }
    if !std::fs::symlink_metadata(path)
        .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
    {
        return Err(unavailable("artifact directory is not a local directory"));
    }
    Ok(())
}
fn reject_nonregular(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            Err(unavailable("artifact path is not a regular file"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(unavailable("cannot inspect artifact path")),
    }
}
fn checked_object_path(root: &Path, key: &str, create: bool) -> Result<PathBuf> {
    let mut path = root.to_owned();
    let mut components = key.split('/').peekable();
    while let Some(component) = components.next() {
        path.push(component);
        if components.peek().is_some() {
            if create {
                ensure_directory(&path)?;
            } else {
                match std::fs::symlink_metadata(&path) {
                    Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                        return Err(unavailable("artifact directory is not a local directory"));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(unavailable("cannot inspect artifact directory")),
                }
            }
        } else {
            reject_nonregular(&path)?;
        }
    }
    Ok(path)
}
fn private_create(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|_| unavailable("cannot create private artifact file"))
}
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| unavailable("artifact directory synchronization failed"))
}

struct FileReader {
    file: Arc<Mutex<Option<File>>>,
    deadline: Instant,
    eof: bool,
    remaining: u64,
    digest: Digest,
    hash: Sha256,
}
impl FileReader {
    fn new(
        file: File,
        metadata: ledgence_orchestration_api::ArtifactMetadata,
        deadline: Instant,
    ) -> Self {
        let file = Arc::new(Mutex::new(Some(file)));
        let weak = Arc::downgrade(&file);
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline.into()).await;
            if let Some(file) = weak.upgrade() {
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(mut held) = file.lock() {
                        held.take();
                    }
                })
                .await;
            }
        });
        Self {
            file,
            deadline,
            eof: false,
            remaining: metadata.size,
            digest: metadata.digest,
            hash: Sha256::new(),
        }
    }
}
impl ArtifactReader for FileReader {
    fn read_chunk(&mut self, max_bytes: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            chunk_limit(max_bytes)?;
            if self.eof {
                return Ok(Vec::new());
            }
            check_deadline(self.deadline)?;
            let file = self.file.clone();
            let deadline = self.deadline;
            let bytes = tokio::time::timeout_at(
                deadline.into(),
                tokio::task::spawn_blocking(move || {
                    let mut held = file
                        .lock()
                        .map_err(|_| unavailable("artifact reader unavailable"))?;
                    check_deadline(deadline)?;
                    let reader = held
                        .as_mut()
                        .ok_or_else(|| unavailable("artifact reader expired"))?;
                    let mut bytes = vec![0; max_bytes];
                    let length = reader
                        .read(&mut bytes)
                        .map_err(|_| unavailable("artifact read failed"))?;
                    bytes.truncate(length);
                    if length == 0 {
                        held.take();
                    }
                    Ok::<_, ledgence_orchestration_api::ContractError>(bytes)
                }),
            )
            .await
            .map_err(|_| unavailable("artifact read deadline exceeded"))?
            .map_err(|_| unavailable("artifact read interrupted"))??;
            if bytes.len() as u64 > self.remaining {
                return Err(unavailable(
                    "artifact filesystem body exceeds declared length",
                ));
            }
            self.remaining -= bytes.len() as u64;
            self.hash.update(&bytes);
            if bytes.is_empty() {
                if self.remaining != 0
                    || format!("sha256:{:x}", self.hash.clone().finalize()) != self.digest.0
                {
                    return Err(unavailable("artifact filesystem integrity mismatch"));
                }
                self.eof = true;
            }
            Ok(bytes)
        })
    }
}

#[cfg(test)]
mod tests;
