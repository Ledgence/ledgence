//! A publication receipt owns the exact archive until remote reconciliation.
use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
};

const RECEIPT_LIMIT: u64 = 64 * 1024;
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Prepared,
    Published,
    Registered,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema_version: u32,
    pub server: String,
    pub descriptor: ProgramDescriptor,
    pub registration: Option<RegisterProgram>,
    pub phase: Phase,
    pub publication: Option<PublishArtifactResult>,
    pub registered: Option<RegisterProgramReply>,
}
pub struct Session {
    pub path: PathBuf,
    pub receipt: Receipt,
    _lock: File,
}
impl Session {
    pub fn create(
        source: &Path,
        server: String,
        packed: PackedProgram,
        registration: Option<RegisterProgram>,
    ) -> Result<Self> {
        let source_parent = source
            .parent()
            .ok_or_else(|| invalid("prepared source has no parent directory"))?;
        let parent = source_parent.join("publications");
        fs::create_dir_all(&parent).map_err(io_error)?;
        let directory = tempfile::Builder::new()
            .prefix("publication-")
            .tempdir_in(&parent)
            .map_err(io_error)?;
        let archive_path = directory.path().join("artifact.zip");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&archive_path)
            .map_err(io_error)?;
        file.write_all(&packed.archive).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        // Once exposed, the bundle is durable recovery material, never temporary
        // cleanup owned by an HTTP request or by a failed catalog registration.
        let path = directory.path().join("receipt.json");
        let lock = acquire_lock(directory.path())?;
        let mut session = Self {
            path,
            _lock: lock,
            receipt: Receipt {
                schema_version: 1,
                server,
                descriptor: packed.descriptor,
                registration,
                phase: Phase::Prepared,
                publication: None,
                registered: None,
            },
        };
        session.save()?;
        sync_directory(directory.path())?;
        sync_directory(&parent)?;
        // The recovery bundle must also survive creation of its publications
        // ancestor: syncing that directory alone does not persist its own link.
        sync_directory(source_parent)?;
        let _ = directory.keep();
        Ok(session)
    }
    pub fn open(path: &Path) -> Result<Self> {
        // Reject links before canonicalization rather than following a renamed
        // file to an unrelated bundle. The fixed sibling archive is never read
        // from a path supplied inside JSON.
        regular(path)?;
        let path = fs::canonicalize(path).map_err(io_error)?;
        if path.file_name().and_then(|v| v.to_str()) != Some("receipt.json") {
            return Err(invalid("resume expects the saved receipt.json"));
        }
        let parent = path
            .parent()
            .ok_or_else(|| invalid("receipt has no parent"))?;
        let lock = acquire_lock(parent)?;
        let bytes = read_bounded(&path, RECEIPT_LIMIT)?;
        let receipt: Receipt =
            ledgence_orchestration_api::decode_unique_json(&bytes, RECEIPT_LIMIT as usize)
                .map_err(|_| invalid("invalid publication receipt"))?;
        if receipt.schema_version != 1 {
            return Err(invalid("unsupported publication receipt version"));
        }
        receipt.descriptor.validate().map_err(worker_error)?;
        HttpProgramPublisher::new(&receipt.server).map_err(publication_error)?;
        if let Some(command) = &receipt.registration {
            command.validate()?;
            if command.expected_descriptor.as_ref() != Some(&receipt.descriptor) {
                return Err(invalid(
                    "registration receipt must bind the exact published descriptor",
                ));
            }
        }
        let published = receipt.publication.as_ref();
        if published.is_some_and(|p| p.descriptor != receipt.descriptor)
            || match receipt.phase {
                Phase::Prepared => published.is_some() || receipt.registered.is_some(),
                Phase::Published => published.is_none() || receipt.registered.is_some(),
                Phase::Registered => {
                    published.is_none()
                        || receipt.registration.is_none()
                        || receipt.registered.is_none()
                }
            }
        {
            return Err(invalid("inconsistent publication receipt state"));
        }
        if let Some(reply) = &receipt.registered {
            use ledgence_orchestration_api::console::ConsoleRecord;
            reply.version.validate()?;
            let command = receipt
                .registration
                .as_ref()
                .ok_or_else(|| invalid("missing registration command"))?;
            if reply.version.descriptor != receipt.descriptor.clone().into()
                || reply.version.metadata != command.metadata
                || (reply.metadata_updated
                    && (!command.update_metadata || !reply.already_registered))
            {
                return Err(invalid(
                    "registration receipt does not match recorded command",
                ));
            }
        }
        Ok(Self {
            path,
            receipt,
            _lock: lock,
        })
    }
    pub fn archive(&self) -> Result<Vec<u8>> {
        let path = self.path.with_file_name("artifact.zip");
        let bytes = read_bounded(&path, ArtifactLimits::default().max_archive_bytes)?;
        if bytes.len() as u64 != self.receipt.descriptor.size
            || format!("sha256:{:x}", Sha256::digest(&bytes)) != self.receipt.descriptor.digest.0
        {
            return Err(invalid(
                "saved artifact differs from receipt; preserve the original ZIP and do not rebuild to retry",
            ));
        }
        Ok(bytes)
    }
    pub fn save(&mut self) -> Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| invalid("receipt has no parent"))?;
        let bytes = serde_json::to_vec_pretty(&self.receipt)
            .map_err(|_| invalid("cannot encode publication receipt"))?;
        if bytes.len() as u64 > RECEIPT_LIMIT {
            return Err(invalid("publication receipt exceeds limit"));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
        temporary.write_all(&bytes).map_err(io_error)?;
        temporary.as_file().sync_all().map_err(io_error)?;
        temporary
            .persist(&self.path)
            .map_err(|e| io_error(e.error))?;
        sync_directory(parent)
    }
}
fn acquire_lock(directory: &Path) -> Result<File> {
    let path = directory.join(".lock");
    if path.try_exists().map_err(io_error)? {
        regular(&path)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(io_error)?;
    file.try_lock()
        .map_err(|_| invalid("this publication receipt is already in use by another command"))?;
    Ok(file)
}
fn regular(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.file_type().is_file() {
        return Err(invalid(
            "publication receipt and artifact must be regular files",
        ));
    }
    Ok(())
}
fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    regular(path)?;
    let file = File::open(path).map_err(io_error)?;
    if file.metadata().map_err(io_error)?.len() > max {
        return Err(invalid("publication file exceeds limit"));
    }
    let mut bytes = Vec::new();
    file.take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > max {
        return Err(invalid("publication file exceeds limit"));
    }
    Ok(bytes)
}
fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_error)?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
