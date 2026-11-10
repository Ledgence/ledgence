//! Bounded file IO; blocking jobs remain owned by the command runtime.
use super::*;
use sha2::{Digest as _, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
fn check(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(unavailable("artifact transfer deadline exceeded"))
    } else {
        Ok(())
    }
}
fn io(error: std::io::Error) -> ContractError {
    unavailable(format!("artifact file IO failed: {error}"))
}
async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| unavailable("artifact file operation failed"))?
}
pub(super) async fn prepare(
    path: PathBuf,
    media_type: String,
    deadline: Instant,
) -> Result<(Box<dyn ArtifactReader>, ArtifactMetadata)> {
    let (file, metadata) = blocking(move || {
        check(deadline)?;
        if !std::fs::symlink_metadata(&path)
            .map_err(io)?
            .file_type()
            .is_file()
        {
            return Err(invalid("artifact source must be a regular file"));
        }
        let mut file = std::fs::File::open(&path).map_err(io)?;
        if !file.metadata().map_err(io)?.is_file() {
            return Err(invalid("artifact source must be a regular file"));
        }
        let mut hash = Sha256::new();
        let mut size = 0u64;
        let mut buffer = vec![0; ARTIFACT_CHUNK_MAX_BYTES];
        loop {
            check(deadline)?;
            let count = file.read(&mut buffer).map_err(io)?;
            if count == 0 {
                break;
            }
            size += count as u64;
            if size > ARTIFACT_MAX_BYTES {
                return Err(invalid("artifact exceeds 64 MiB"));
            }
            hash.update(&buffer[..count]);
        }
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        let metadata = ArtifactMetadata {
            digest: ledgence_worker_api::Digest(format!("sha256:{:x}", hash.finalize())),
            size,
            media_type,
            filename: path.file_name().and_then(|v| v.to_str()).map(str::to_owned),
        };
        metadata.validate()?;
        Ok((file, metadata))
    })
    .await?;
    Ok((
        Box::new(FileReader {
            file: Arc::new(Mutex::new(file)),
            deadline,
            remaining: metadata.size,
        }),
        metadata,
    ))
}
struct FileReader {
    file: Arc<Mutex<std::fs::File>>,
    deadline: Instant,
    remaining: u64,
}
impl ArtifactReader for FileReader {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            check(self.deadline)?;
            if max == 0 || max > ARTIFACT_CHUNK_MAX_BYTES {
                return Err(invalid("invalid artifact chunk size"));
            }
            let file = self.file.clone();
            let deadline = self.deadline;
            let bytes = blocking(move || {
                check(deadline)?;
                let mut bytes = vec![0; max];
                let count = file
                    .lock()
                    .map_err(|_| unavailable("artifact file lock failed"))?
                    .read(&mut bytes)
                    .map_err(io)?;
                bytes.truncate(count);
                Ok(bytes)
            })
            .await?;
            if bytes.len() as u64 > self.remaining {
                return Err(invalid("source changed after preparation"));
            }
            self.remaining -= bytes.len() as u64;
            if bytes.is_empty() && self.remaining != 0 {
                return Err(invalid("source changed after preparation"));
            }
            Ok(bytes)
        })
    }
}
pub(super) fn accepted(
    status: &ArtifactUploadStatus,
    metadata: &ArtifactMetadata,
) -> ArtifactClientResult<Option<ArtifactRef>> {
    status
        .validate()
        .map_err(|_| ArtifactClientError::InvalidResponse {
            message: "invalid artifact acceptance status".into(),
        })?;
    let Some(reference) = &status.reference else {
        return Ok(None);
    };
    if !status.selected
        || status.stage != ArtifactUploadStage::Ready
        || reference.digest != metadata.digest
        || reference.size != metadata.size
        || reference.media_type != metadata.media_type
    {
        return Err(ArtifactClientError::InvalidResponse {
            message: "artifact acceptance disagrees with source content".into(),
        });
    }
    Ok(Some(reference.clone()))
}
pub(super) async fn download(
    mut body: ArtifactDownload,
    reference: &ArtifactRef,
    path: PathBuf,
    deadline: Instant,
) -> Result<()> {
    reference.validate()?;
    body.manifest.validate()?;
    if body.manifest.reference != *reference {
        return Err(unavailable("download reference mismatch"));
    }
    let deadline = deadline.min(body.deadline);
    let destination = path.clone();
    let temp = blocking(move || {
        check(deadline)?;
        match std::fs::symlink_metadata(&destination) {
            Ok(_) => return Err(invalid("destination already exists")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(e)),
        };
        let parent = destination
            .parent()
            .filter(|v| !v.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        tempfile::NamedTempFile::new_in(parent).map_err(io)
    })
    .await?;
    let temp = Arc::new(Mutex::new(Some(temp)));
    let mut size = 0u64;
    let mut hash = Sha256::new();
    loop {
        check(deadline)?;
        let chunk = body.reader.read_chunk(ARTIFACT_CHUNK_MAX_BYTES).await?;
        if chunk.len() > ARTIFACT_CHUNK_MAX_BYTES {
            return Err(unavailable("reader exceeded chunk bound"));
        }
        if chunk.is_empty() {
            break;
        }
        size += chunk.len() as u64;
        if size > reference.size {
            return Err(unavailable("download exceeds declared size"));
        }
        hash.update(&chunk);
        let file = temp.clone();
        blocking(move || {
            check(deadline)?;
            file.lock()
                .map_err(|_| unavailable("temporary file lock failed"))?
                .as_mut()
                .expect("unpublished temporary")
                .write_all(&chunk)
                .map_err(io)
        })
        .await?;
    }
    if size != reference.size || format!("sha256:{:x}", hash.finalize()) != reference.digest.0 {
        return Err(unavailable("download integrity check failed"));
    }
    blocking(move || {
        check(deadline)?;
        let mut file = temp
            .lock()
            .map_err(|_| unavailable("temporary file lock failed"))?
            .take()
            .expect("unpublished temporary");
        file.flush().map_err(io)?;
        file.as_file().sync_all().map_err(io)?;
        check(deadline)?;
        file.persist_noclobber(path).map_err(|e| io(e.error))?;
        Ok(())
    })
    .await
}
