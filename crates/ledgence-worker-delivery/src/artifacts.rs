//! Invocation-bound execution artifacts. File bytes never enter runtime frames.
use super::*;
use ledgence_worker_api::{
    Error, ErrorKind, PortFuture, RuntimeFeatures, RuntimeReply, RuntimeRequest,
    RuntimeRequestHandler,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use workflow::runtime_error;

#[derive(Default)]
struct OwnedIo {
    jobs: AsyncMutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl OwnedIo {
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> ledgence_worker_api::Result<T> + Send + 'static,
    ) -> ledgence_worker_api::Result<T> {
        let (send, receive) = oneshot::channel();
        let mut jobs = self.jobs.lock().await;
        // Serialize owned disk operations as well as bytes. Tiny reader chunks
        // cannot accumulate an unbounded list of completed task handles.
        while let Some(job) = jobs.last_mut() {
            job.await
                .map_err(|_| Error::new(ErrorKind::Io, "artifact IO lifetime is unresolved"))?;
            jobs.pop();
        }
        // Register before yielding. A cancelled read cannot detach blocking IO.
        jobs.push(tokio::task::spawn_blocking(move || {
            let _ = send.send(operation());
        }));
        drop(jobs);
        receive
            .await
            .map_err(|_| Error::new(ErrorKind::Io, "artifact IO task failed"))?
    }
    async fn drain(&self) -> ledgence_worker_api::Result<()> {
        let mut jobs = self.jobs.lock().await;
        while let Some(job) = jobs.last_mut() {
            // Keep the handle in the collection if a cleanup waiter disappears.
            job.await
                .map_err(|_| Error::new(ErrorKind::Io, "artifact IO lifetime is unresolved"))?;
            jobs.pop();
        }
        Ok(())
    }
}

pub(super) struct ArtifactOperations {
    service: Arc<dyn TaskService>,
    requester: ArtifactRequester,
    target: ExecutionTarget,
    io: Arc<OwnedIo>,
    request: AsyncMutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Context {
    pub(super) fn artifact_handler(&self, owner: LeaseOwner) -> Option<Arc<ArtifactOperations>> {
        if !self.runtime_features.execution_artifacts() {
            return None;
        }
        Some(Arc::new(ArtifactOperations {
            service: self.service.clone(),
            // Even a workflow activation runs as a leased task. The backend
            // derives its logical producer and root from this retained task.
            target: ExecutionTarget::Task {
                id: owner.task_id.clone(),
            },
            requester: ArtifactRequester::Worker { owner },
            io: Arc::new(OwnedIo::default()),
            request: AsyncMutex::new(None),
        }))
    }
}
impl RuntimeRequestHandler for ArtifactOperations {
    fn optional_for_legacy_protocol(&self) -> bool {
        true
    }
    fn features(&self) -> RuntimeFeatures {
        RuntimeFeatures {
            v: 1,
            supported: vec![ledgence_worker_api::EXECUTION_ARTIFACTS_FEATURE.into()],
        }
    }
    fn drain(&self) -> PortFuture<'_, ()> {
        Box::pin(async {
            let mut request = self.request.lock().await;
            if let Some(job) = request.as_mut() {
                job.await.map_err(|_| {
                    Error::new(ErrorKind::Io, "artifact request lifetime is unresolved")
                })?;
                *request = None;
            }
            self.io.drain().await
        })
    }
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async move {
            request.validate()?;
            control.check()?;
            if !matches!(
                request.operation.as_str(),
                "artifacts.put_file"
                    | "artifacts.download"
                    | "artifacts.upload_status"
                    | "artifacts.reconcile"
                    | "artifacts.replace"
            ) {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    "plain tasks can only request artifact operations",
                ));
            }
            self.drain().await?;
            let service = self.service.clone();
            let requester = self.requester.clone();
            let target = self.target.clone();
            let io = self.io.clone();
            let (send, receive) = oneshot::channel();
            let mut pending = self.request.lock().await;
            *pending = Some(tokio::spawn(async move {
                let id = request.id;
                let control = control.shorter_deadline(
                    Instant::now() + Duration::from_millis(ARTIFACT_TRANSFER_TIMEOUT_MS),
                );
                let result = async {
                    let client = service.artifact_client().map_err(runtime_error)?;
                    execute(client, requester, target, io, request, control).await
                }
                .await
                .map(|result| RuntimeReply { id, result });
                let _ = send.send(result);
            }));
            drop(pending);
            receive
                .await
                .map_err(|_| Error::new(ErrorKind::Io, "artifact request task failed"))?
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutFile {
    path: PathBuf,
    key: String,
    media_type: String,
    filename: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadStatusRequest {
    upload_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileRequest {
    selection: ArtifactSelection,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadFile {
    path: PathBuf,
    reference: ArtifactRef,
}
fn absolute(path: &Path) -> ledgence_worker_api::Result<()> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "artifact path must be an absolute file path",
        ));
    }
    Ok(())
}
fn lock_file<T>(value: &Mutex<T>) -> ledgence_worker_api::Result<std::sync::MutexGuard<'_, T>> {
    value
        .lock()
        .map_err(|_| Error::new(ErrorKind::Io, "artifact file ownership is unresolved"))
}
fn io_error(error: std::io::Error) -> Error {
    Error::new(ErrorKind::Io, error.to_string())
}
fn conflict(message: &str) -> Error {
    Error::new(ErrorKind::Integrity, message)
}

async fn execute(
    client: &dyn ExecutionArtifactClient,
    requester: ArtifactRequester,
    target: ExecutionTarget,
    io: Arc<OwnedIo>,
    request: RuntimeRequest,
    control: RunControl,
) -> ledgence_worker_api::Result<serde_json::Value> {
    match request.operation.as_str() {
        "artifacts.upload_status" => {
            let input: UploadStatusRequest = serde_json::from_value(request.payload)
                .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()))?;
            let status = client
                .upload_status(&requester, &input.upload_id, control.deadline())
                .await
                .map_err(client_error)?;
            control.check()?;
            status.validate().map_err(runtime_error)?;
            if status.selection.upload_id != input.upload_id {
                return Err(conflict("artifact status changed upload identity"));
            }
            Ok(serde_json::json!(status))
        }
        "artifacts.reconcile" => {
            let input: ReconcileRequest = serde_json::from_value(request.payload)
                .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()))?;
            input.selection.validate().map_err(runtime_error)?;
            let reply = match client
                .reconcile(&requester, &input.selection, control.deadline())
                .await
            {
                Ok(reply) => reply,
                Err(ArtifactClientError::OutcomeUnknown { .. }) => {
                    control.check()?;
                    return Ok(uncertain(
                        Some(&input.selection),
                        serde_json::json!({"kind":"reconcile","selection":input.selection}),
                    ));
                }
                Err(error) => return Err(client_error(error)),
            };
            control.check()?;
            reply.validate().map_err(runtime_error)?;
            if reply.upload.selection != input.selection {
                return Err(conflict("artifact reconciliation changed selection"));
            }
            Ok(serde_json::json!(reply.upload))
        }
        "artifacts.replace" => {
            let command: ArtifactReplaceCommand = serde_json::from_value(request.payload)
                .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()))?;
            command.validate().map_err(runtime_error)?;
            let reply = match client
                .replace(&requester, &command, control.deadline())
                .await
            {
                Ok(reply) => reply,
                Err(ArtifactClientError::OutcomeUnknown { .. }) => {
                    control.check()?;
                    return Ok(uncertain(
                        None,
                        serde_json::json!({"kind":"replace","upload_id":command.upload_id,"expected_selection_revision":command.expected_selection_revision}),
                    ));
                }
                Err(error) => return Err(client_error(error)),
            };
            control.check()?;
            reply.validate().map_err(runtime_error)?;
            if reply.replaced.upload_id != command.upload_id
                || reply.replaced.revision != command.expected_selection_revision
            {
                return Err(conflict(
                    "artifact replacement changed original CAS identity",
                ));
            }
            Ok(serde_json::json!(reply))
        }
        "artifacts.put_file" => {
            let put: PutFile = serde_json::from_value(request.payload)
                .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()))?;
            absolute(&put.path)?;
            let io_control = control.clone();
            let (file, size, digest) = io
                .run(move || {
                    io_control.check()?;
                    if !std::fs::symlink_metadata(&put.path)
                        .map_err(io_error)?
                        .file_type()
                        .is_file()
                    {
                        return Err(Error::new(
                            ErrorKind::InvalidInput,
                            "artifact source must be a regular file",
                        ));
                    }
                    let mut file = std::fs::File::open(&put.path).map_err(io_error)?;
                    if !file.metadata().map_err(io_error)?.is_file() {
                        return Err(Error::new(
                            ErrorKind::InvalidInput,
                            "artifact source must be a regular file",
                        ));
                    }
                    let mut hash = Sha256::new();
                    let mut size = 0u64;
                    let mut chunk = vec![0; ARTIFACT_CHUNK_MAX_BYTES];
                    loop {
                        io_control.check()?;
                        let count = file.read(&mut chunk).map_err(io_error)?;
                        if count == 0 {
                            break;
                        }
                        size += count as u64;
                        if size > ARTIFACT_MAX_BYTES {
                            return Err(Error::new(
                                ErrorKind::InvalidInput,
                                "artifact exceeds maximum size",
                            ));
                        }
                        hash.update(&chunk[..count]);
                    }
                    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
                    Ok((
                        file,
                        size,
                        ledgence_worker_api::Digest(format!("sha256:{:x}", hash.finalize())),
                    ))
                })
                .await?;
            control.check()?;
            let metadata = ArtifactMetadata {
                digest,
                size,
                media_type: put.media_type,
                filename: put.filename,
            };
            let command = ArtifactPrepareCommand {
                scope: requester.scope().clone(),
                target,
                key: put.key,
                metadata: metadata.clone(),
            };
            command.validate().map_err(runtime_error)?;
            let prepared = client
                .prepare(&requester, &command, control.deadline())
                .await
                .map_err(client_error)?;
            control.check()?;
            if let Some(reference) = accepted(&prepared.upload, &metadata)? {
                return serde_json::to_value(reference)
                    .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()));
            }
            if !prepared.upload.selected
                || prepared.upload.stage != ArtifactUploadStage::Prepared
                || prepared.upload.retry != ArtifactRetryGuidance::Upload
            {
                return Ok(recovery(&prepared.upload, "put_file"));
            }
            prepared
                .upload
                .selection
                .validate()
                .map_err(runtime_error)?;
            let selection = prepared.upload.selection;
            let body = Box::new(FileReader {
                file: Arc::new(Mutex::new(file)),
                io,
                control: control.clone(),
                remaining: size,
            });
            let result = match client
                .put(requester, selection.clone(), body, control.deadline())
                .await
            {
                Ok(reply) => reply,
                Err(ArtifactClientError::OutcomeUnknown { .. }) => {
                    control.check()?;
                    return Ok(uncertain(
                        Some(&selection),
                        serde_json::json!({"kind":"put_file"}),
                    ));
                }
                Err(error) => {
                    let mut error = client_error(error);
                    error.message =
                        format!("artifact upload {}: {}", selection.upload_id, error.message);
                    return Err(error);
                }
            };
            control.check()?;
            if result.upload.selection != selection {
                return Err(conflict("artifact upload response changed selection"));
            }
            let Some(reference) = accepted(&result.upload, &metadata)? else {
                return Ok(recovery(&result.upload, "put_file"));
            };
            serde_json::to_value(reference)
                .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()))
        }
        "artifacts.download" => {
            let download: DownloadFile = serde_json::from_value(request.payload)
                .map_err(|e| Error::new(ErrorKind::Protocol, e.to_string()))?;
            absolute(&download.path)?;
            download.reference.validate().map_err(runtime_error)?;
            let path = download.path.clone();
            let temporary = io
                .run(move || {
                    match std::fs::symlink_metadata(&path) {
                        Ok(_) => {
                            return Err(Error::new(
                                ErrorKind::InvalidInput,
                                "artifact destination already exists",
                            ));
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(io_error(e)),
                    }
                    tempfile::NamedTempFile::new_in(path.parent().expect("absolute file path"))
                        .map_err(io_error)
                })
                .await?;
            let temporary = Arc::new(Mutex::new(Some(temporary)));
            control.check()?;
            let mut body = client
                .download(requester, download.reference.clone(), control.deadline())
                .await
                .map_err(client_error)?;
            if body.manifest.reference != download.reference {
                return Err(conflict("artifact download changed reference"));
            }
            body.manifest.validate().map_err(runtime_error)?;
            let transfer_control = control.shorter_deadline(body.deadline);
            let mut hash = Sha256::new();
            let mut size = 0u64;
            loop {
                transfer_control.check()?;
                let chunk = body
                    .reader
                    .read_chunk(ARTIFACT_CHUNK_MAX_BYTES)
                    .await
                    .map_err(runtime_error)?;
                if chunk.len() > ARTIFACT_CHUNK_MAX_BYTES {
                    return Err(conflict("artifact reader exceeded chunk bound"));
                }
                if chunk.is_empty() {
                    break;
                }
                size += chunk.len() as u64;
                if size > download.reference.size {
                    return Err(conflict("artifact download exceeds declared size"));
                }
                hash.update(&chunk);
                let temp = temporary.clone();
                io.run(move || {
                    lock_file(&temp)?
                        .as_mut()
                        .expect("unpublished temporary")
                        .write_all(&chunk)
                        .map_err(io_error)
                })
                .await?;
            }
            if size != download.reference.size
                || format!("sha256:{:x}", hash.finalize()) != download.reference.digest.0
            {
                return Err(conflict("artifact download checksum or size mismatch"));
            }
            let path = download.path.clone();
            io.run(move || {
                transfer_control.check()?;
                let mut temp = lock_file(&temporary)?
                    .take()
                    .expect("unpublished temporary");
                temp.flush().map_err(io_error)?;
                temp.as_file().sync_all().map_err(io_error)?;
                transfer_control.check()?;
                // Unlike rename, persist_noclobber fails if a concurrent writer
                // created the destination after our initial existence check.
                temp.persist_noclobber(path)
                    .map_err(|e| io_error(e.error))?;
                Ok(())
            })
            .await?;
            Ok(serde_json::json!({"path": download.path}))
        }
        _ => Err(Error::new(
            ErrorKind::Protocol,
            "unsupported artifact operation",
        )),
    }
}
fn client_error(error: ArtifactClientError) -> Error {
    match error {
        ArtifactClientError::Rejected(error) => runtime_error(error),
        ArtifactClientError::InvalidResponse { message } => {
            Error::new(ErrorKind::Protocol, message)
        }
        ArtifactClientError::OutcomeUnknown { message } => {
            Error::new(ErrorKind::Unavailable, message)
        }
    }
}
fn uncertain(
    selection: Option<&ArtifactSelection>,
    operation: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({"artifact_error":{"kind":"uncertain","selection":selection,"status":null,"operation":operation,"message":"artifact operation outcome is unknown; inspect its original identity before recovery"}})
}
fn recovery(status: &ArtifactUploadStatus, operation: &str) -> serde_json::Value {
    serde_json::json!({"artifact_error": {"kind":"recovery_required", "selection":status.selection, "status":status, "operation":{"kind":operation}, "message":"upload requires explicit recovery"}})
}
fn accepted(
    status: &ArtifactUploadStatus,
    metadata: &ArtifactMetadata,
) -> ledgence_worker_api::Result<Option<ArtifactRef>> {
    status.validate().map_err(runtime_error)?;
    let Some(reference) = &status.reference else {
        return Ok(None);
    };
    reference.validate().map_err(runtime_error)?;
    if !status.selected
        || status.stage != ArtifactUploadStage::Ready
        || status.retry != ArtifactRetryGuidance::Complete
        || reference.id != status.selection.artifact_id
        || reference.digest != metadata.digest
        || reference.size != metadata.size
        || reference.media_type != metadata.media_type
    {
        return Err(conflict(
            "artifact acceptance disagrees with requested content",
        ));
    }
    Ok(Some(reference.clone()))
}
struct FileReader {
    file: Arc<Mutex<std::fs::File>>,
    io: Arc<OwnedIo>,
    control: RunControl,
    remaining: u64,
}
impl ArtifactReader for FileReader {
    fn read_chunk(&mut self, maximum: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            self.control
                .check()
                .map_err(|e| ContractError::Unavailable(e.to_string()))?;
            if maximum == 0 || maximum > ARTIFACT_CHUNK_MAX_BYTES {
                return Err(ContractError::InvalidInput(
                    "invalid artifact chunk bound".into(),
                ));
            }
            let file = self.file.clone();
            let bytes = self
                .io
                .run(move || {
                    let mut bytes = vec![0; maximum];
                    let count = lock_file(&file)?.read(&mut bytes).map_err(io_error)?;
                    bytes.truncate(count);
                    Ok(bytes)
                })
                .await
                .map_err(|e| ContractError::Unavailable(e.to_string()))?;
            if bytes.len() as u64 > self.remaining {
                return Err(ContractError::InvalidInput(
                    "artifact source grew during upload".into(),
                ));
            }
            self.remaining -= bytes.len() as u64;
            if bytes.is_empty() && self.remaining != 0 {
                return Err(ContractError::InvalidInput(
                    "artifact source shrank during upload".into(),
                ));
            }
            Ok(bytes)
        })
    }
}

/// Compose invocation-specific IO with workflow requests without granting those
/// requests to plain tasks. Both owners must drain before a session is reusable.
pub(super) fn compose(
    inner: Arc<dyn RuntimeRequestHandler>,
    artifacts: Option<Arc<ArtifactOperations>>,
) -> Arc<dyn RuntimeRequestHandler> {
    match artifacts {
        Some(artifacts) => Arc::new(Composite { inner, artifacts }),
        None => inner,
    }
}
struct Composite {
    inner: Arc<dyn RuntimeRequestHandler>,
    artifacts: Arc<ArtifactOperations>,
}
impl RuntimeRequestHandler for Composite {
    fn features(&self) -> RuntimeFeatures {
        self.inner.features()
    }
    fn capabilities(&self) -> ledgence_worker_api::RuntimeCapabilities {
        self.inner.capabilities()
    }
    fn invocation_timeout(&self) -> Option<Duration> {
        self.inner.invocation_timeout()
    }
    fn completion_reserve(&self) -> Duration {
        self.inner.completion_reserve()
    }
    fn drain(&self) -> PortFuture<'_, ()> {
        Box::pin(async {
            self.artifacts.drain().await?;
            self.inner.drain().await
        })
    }
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        if request.operation.starts_with("artifacts.") {
            self.artifacts.handle(request, control)
        } else {
            self.inner.handle(request, control)
        }
    }
}

#[cfg(test)]
#[path = "artifacts_tests.rs"]
mod tests;
