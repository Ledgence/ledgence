use crate::{CHUNK_BYTES, MAX_BYTES, check_deadline, chunk_limit, invalid, random_id, unavailable};
use ledgence_orchestration_api::{
    ArtifactReader, ArtifactSpoolFactory, ArtifactSpoolPurpose, ArtifactSpoolWriter, ContractError,
    ContractFuture, Result,
};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const TRANSFER_TIMEOUT: Duration = Duration::from_secs(120);

/// Local per-process transfer space. Two upload and four download slots bound
/// spool data to 384 MiB. Filesystem-store staging is a separate 128 MiB budget.
/// A dropped transfer never releases its slot ahead of retained blocking IO.
#[derive(Clone)]
pub struct FileArtifactSpoolFactory {
    inner: Arc<Factory>,
}
struct Factory {
    namespace: Arc<Namespace>,
    uploads: Arc<Semaphore>,
    downloads: Arc<Semaphore>,
    state: Mutex<FactoryState>,
    changed: Notify,
}
struct FactoryState {
    closed: bool,
    leases: HashMap<String, Arc<Lease>>,
}
struct Namespace {
    path: PathBuf,
    // The lock file is deliberately never unlinked: replacing its inode would
    // allow another process to acquire a different lock for the same namespace.
    _lock: File,
}
struct Lease {
    id: String,
    namespace: Arc<Namespace>,
    factory: Weak<Factory>,
    deadline: Instant,
    maximum: u64,
    cancelled: AtomicBool,
    cleanup_started: AtomicBool,
    complete: AtomicBool,
    cleanup_failed: AtomicBool,
    complete_notify: Notify,
    state: Mutex<FileState>,
}
struct FileState {
    file: Option<File>,
    size: u64,
    immutable: bool,
    permit: Option<OwnedSemaphorePermit>,
}
struct SpoolWriter {
    lease: Arc<Lease>,
    cleanup_on_drop: bool,
}
struct SpoolReader {
    lease: Arc<Lease>,
    eof: bool,
}

impl FileArtifactSpoolFactory {
    /// Acquire exclusive namespace ownership and remove recognized stale spools
    /// before admitting transfers. Concurrent processes need distinct paths.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let namespace = tokio::task::spawn_blocking(move || open_namespace(&path))
            .await
            .map_err(|_| unavailable("artifact spool initialization interrupted"))??;
        Ok(Self {
            inner: Arc::new(Factory {
                namespace: Arc::new(namespace),
                uploads: Arc::new(Semaphore::new(2)),
                downloads: Arc::new(Semaphore::new(4)),
                state: Mutex::new(FactoryState {
                    closed: false,
                    leases: HashMap::new(),
                }),
                changed: Notify::new(),
            }),
        })
    }

    async fn create_inner(
        &self,
        upload: bool,
        maximum: u64,
        deadline: Instant,
    ) -> Result<SpoolWriter> {
        if maximum > MAX_BYTES {
            return Err(invalid("artifact spool exceeds 64 MiB"));
        }
        check_deadline(deadline)?;
        let deadline = deadline.min(Instant::now() + TRANSFER_TIMEOUT);
        let semaphore = if upload {
            &self.inner.uploads
        } else {
            &self.inner.downloads
        };
        // Saturation fails promptly; HTTP callers can retry. No queued request
        // may consume disk or retain a request body while waiting for a slot.
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => {
                    unavailable("artifact spool admission is closed")
                }
                tokio::sync::TryAcquireError::NoPermits => ContractError::Busy,
            })?;
        let lease = Arc::new(Lease {
            id: random_id()?,
            namespace: self.inner.namespace.clone(),
            factory: Arc::downgrade(&self.inner),
            deadline,
            maximum,
            cancelled: AtomicBool::new(false),
            cleanup_started: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
            complete_notify: Notify::new(),
            state: Mutex::new(FileState {
                file: None,
                size: 0,
                immutable: false,
                permit: Some(permit),
            }),
        });
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| unavailable("artifact spool state unavailable"))?;
            if state.closed {
                return Err(unavailable("artifact spool admission is closed"));
            }
            state.leases.insert(lease.id.clone(), lease.clone());
        }
        start_expiry(lease.clone());
        // This owner guard cleans up even if create's caller drops its future.
        let writer = SpoolWriter {
            lease: lease.clone(),
            cleanup_on_drop: true,
        };
        blocking(&lease, |lease| {
            let mut state = lease.lock_live()?;
            state.file = Some(private_file(&lease.path())?);
            Ok(())
        })
        .await?;
        Ok(writer)
    }

    async fn drain_inner(&self, deadline: Instant) -> Result<()> {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| unavailable("artifact spool state unavailable"))?;
            state.closed = true;
            self.inner.uploads.close();
            self.inner.downloads.close();
        }
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .inner
                .state
                .lock()
                .map_err(|_| unavailable("artifact spool state unavailable"))?
                .leases
                .is_empty()
            {
                return Ok(());
            }
            tokio::time::timeout_at(deadline.into(), changed).await
                .map_err(|_| unavailable("artifact spool drain incomplete; retained IO or cleanup still owns capacity"))?;
        }
    }
}

fn open_namespace(path: &Path) -> Result<Namespace> {
    std::fs::create_dir_all(path)
        .map_err(|_| unavailable("cannot create artifact spool namespace"))?;
    let path = path
        .canonicalize()
        .map_err(|_| unavailable("cannot resolve artifact spool namespace"))?;
    let lock_path = path.join(".ledgence-spool.lock");
    reject_symlink(&lock_path)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|_| unavailable("cannot open artifact spool ownership lock"))?;
    lock.try_lock().map_err(|_| ContractError::Busy)?;
    for item in std::fs::read_dir(&path)
        .map_err(|_| unavailable("cannot inspect artifact spool namespace"))?
    {
        let item = item.map_err(|_| unavailable("cannot inspect artifact spool entry"))?;
        let name = item.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if recognized_spool(name) {
            let kind = item
                .file_type()
                .map_err(|_| unavailable("cannot inspect stale artifact spool"))?;
            if !kind.is_file() {
                return Err(unavailable("stale artifact spool is not a regular file"));
            }
            std::fs::remove_file(item.path())
                .map_err(|_| unavailable("stale artifact spool cleanup failed"))?;
        }
    }
    File::open(&path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| unavailable("cannot synchronize artifact spool namespace"))?;
    Ok(Namespace { path, _lock: lock })
}
fn recognized_spool(name: &str) -> bool {
    name.strip_prefix("spool-")
        .and_then(|s| s.strip_suffix(".tmp"))
        .is_some_and(|id| {
            id.len() == 32
                && id
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
}
fn reject_symlink(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(unavailable("artifact ownership file must not be a symlink"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(unavailable("cannot inspect artifact ownership file")),
    }
}
fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|_| unavailable("cannot create artifact spool file"))
}
impl Lease {
    fn path(&self) -> PathBuf {
        self.namespace.path.join(format!("spool-{}.tmp", self.id))
    }
    fn lock_live(&self) -> Result<std::sync::MutexGuard<'_, FileState>> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("artifact spool state unavailable"))?;
        if self.cancelled.load(Ordering::Acquire) || self.complete.load(Ordering::Acquire) {
            return Err(unavailable("artifact spool transfer is closed"));
        }
        check_deadline(self.deadline)?;
        Ok(state)
    }
}
async fn blocking<T: Send + 'static>(
    lease: &Arc<Lease>,
    operation: impl FnOnce(Arc<Lease>) -> Result<T> + Send + 'static,
) -> Result<T> {
    check_deadline(lease.deadline)?;
    let retained = lease.clone();
    tokio::time::timeout_at(
        lease.deadline.into(),
        tokio::task::spawn_blocking(move || operation(retained)),
    )
    .await
    .map_err(|_| unavailable("artifact spool IO exceeded transfer deadline"))?
    .map_err(|_| unavailable("artifact spool IO interrupted"))?
}
fn start_expiry(lease: Arc<Lease>) {
    tokio::spawn(async move {
        let complete = lease.complete_notify.notified();
        tokio::pin!(complete);
        complete.as_mut().enable();
        if lease.complete.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(lease.deadline.into()) => schedule_cleanup(lease.clone()),
            _ = complete => {},
        }
    });
}
fn schedule_cleanup(lease: Arc<Lease>) {
    lease.cancelled.store(true, Ordering::Release);
    if lease.cleanup_started.swap(true, Ordering::AcqRel) {
        return;
    }
    tokio::spawn(async move {
        loop {
            let retained = lease.clone();
            let removed = tokio::task::spawn_blocking(move || {
                // Outstanding blocking work holds this mutex. File closure,
                // removal and permit release happen only after it completes.
                let mut state = retained
                    .state
                    .lock()
                    .map_err(|_| unavailable("artifact spool cleanup state unavailable"))?;
                state.file.take();
                match std::fs::remove_file(retained.path()) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        return Err(unavailable(
                            "artifact spool removal failed; capacity retained",
                        ));
                    }
                }
                state.permit.take();
                Ok(())
            })
            .await;
            if matches!(removed, Ok(Ok(()))) {
                lease.complete.store(true, Ordering::Release);
                lease.complete_notify.notify_waiters();
                if let Some(factory) = lease.factory.upgrade() {
                    if let Ok(mut state) = factory.state.lock() {
                        state.leases.remove(&lease.id);
                    }
                    factory.changed.notify_waiters();
                }
                return;
            }
            lease.cleanup_failed.store(true, Ordering::Release);
            if let Some(factory) = lease.factory.upgrade() {
                factory.changed.notify_waiters();
            }
            // Keep resource ownership and retry a repairable removal failure.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}
impl Drop for SpoolWriter {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            schedule_cleanup(self.lease.clone());
        }
    }
}
impl Drop for SpoolReader {
    fn drop(&mut self) {
        schedule_cleanup(self.lease.clone());
    }
}

impl ArtifactSpoolFactory for FileArtifactSpoolFactory {
    fn create(
        &self,
        purpose: ArtifactSpoolPurpose,
        max_bytes: u64,
        deadline: Instant,
    ) -> ContractFuture<'_, Box<dyn ArtifactSpoolWriter>> {
        Box::pin(async move {
            let writer = self
                .create_inner(purpose == ArtifactSpoolPurpose::Upload, max_bytes, deadline)
                .await?;
            Ok(Box::new(writer) as Box<dyn ArtifactSpoolWriter>)
        })
    }
    fn drain(&self, deadline: Instant) -> ContractFuture<'_, ()> {
        Box::pin(self.drain_inner(deadline))
    }
}
impl ArtifactSpoolWriter for SpoolWriter {
    fn append(&mut self, chunk: Vec<u8>) -> ContractFuture<'_, ()> {
        Box::pin(async move {
            if chunk.len() > CHUNK_BYTES {
                return Err(invalid("artifact chunk exceeds 262144 bytes"));
            }
            let result = blocking(&self.lease, move |lease| {
                let mut state = lease.lock_live()?;
                if state.immutable {
                    return Err(invalid("artifact spool is immutable"));
                }
                let size = state
                    .size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| invalid("artifact spool size overflow"))?;
                if size > lease.maximum {
                    return Err(invalid("artifact spool exceeds declared bound"));
                }
                state
                    .file
                    .as_mut()
                    .ok_or_else(|| unavailable("artifact spool file is unavailable"))?
                    .write_all(&chunk)
                    .map_err(|_| unavailable("artifact spool write failed"))?;
                state.size = size;
                Ok(())
            })
            .await;
            if matches!(&result, Err(ContractError::Unavailable(_))) {
                schedule_cleanup(self.lease.clone());
            }
            result
        })
    }
    fn finish(mut self: Box<Self>) -> ContractFuture<'static, Box<dyn ArtifactReader>> {
        Box::pin(async move {
            blocking(&self.lease, |lease| {
                let mut state = lease.lock_live()?;
                let file = state
                    .file
                    .as_mut()
                    .ok_or_else(|| unavailable("artifact spool file is unavailable"))?;
                file.flush()
                    .and_then(|()| file.sync_all())
                    .map_err(|_| unavailable("artifact spool flush failed"))?;
                let mut readonly = File::open(lease.path())
                    .map_err(|_| unavailable("artifact spool reopen failed"))?;
                readonly
                    .seek(SeekFrom::Start(0))
                    .map_err(|_| unavailable("artifact spool rewind failed"))?;
                state.file = Some(readonly);
                state.immutable = true;
                Ok(())
            })
            .await?;
            self.cleanup_on_drop = false;
            Ok(Box::new(SpoolReader {
                lease: self.lease.clone(),
                eof: false,
            }) as Box<dyn ArtifactReader>)
        })
    }
}
impl ArtifactReader for SpoolReader {
    fn read_chunk(&mut self, max_bytes: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            chunk_limit(max_bytes)?;
            if self.eof {
                return Ok(Vec::new());
            }
            let chunk = blocking(&self.lease, move |lease| {
                let mut state = lease.lock_live()?;
                if !state.immutable {
                    return Err(unavailable("artifact spool is not finished"));
                }
                let mut chunk = vec![0; max_bytes];
                let count = state
                    .file
                    .as_mut()
                    .ok_or_else(|| unavailable("artifact spool file is unavailable"))?
                    .read(&mut chunk)
                    .map_err(|_| unavailable("artifact spool read failed"))?;
                chunk.truncate(count);
                Ok(chunk)
            })
            .await?;
            if chunk.is_empty() {
                self.eof = true;
            }
            Ok(chunk)
        })
    }
}

#[cfg(test)]
mod tests;
