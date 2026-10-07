use crate::{
    ArtifactLimits,
    archive::{digest_hex, make_readonly, read_bounded, verify_package},
    error::{AdapterError, Result},
    filesystem::{executable_bits, open_regular},
};
use ledgence_worker_api::{
    Digest, Error, ErrorKind, ProgramArtifactPublisher, ProgramDescriptor, ProgramManifest,
    ProgramRef, PublicationError, PublicationErrorKind, PublicationFuture, PublicationResult,
    PublishArtifactResult,
};
use std::{
    fs::{self, File},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};
use zip::{ZipWriter, write::SimpleFileOptions};

/// The exact verified bytes to retain and reuse for retries. Packaging never
/// modifies the store and does not require compatibility with the publishing host.
#[derive(Debug)]
pub struct PackedProgram {
    pub descriptor: ProgramDescriptor,
    pub archive: Vec<u8>,
}

pub fn pack_directory(
    source: impl AsRef<Path>,
    limits: &ArtifactLimits,
) -> ledgence_worker_api::Result<PackedProgram> {
    pack(source.as_ref(), limits).map_err(Into::into)
}

/// Legacy local publication retains its descriptor-only result shape.
pub fn publish_directory(
    source_dir: impl AsRef<Path>,
    store_dir: impl AsRef<Path>,
    limits: &ArtifactLimits,
) -> ledgence_worker_api::Result<ProgramDescriptor> {
    let packed = pack_directory(source_dir, limits)?;
    persist_archive(
        packed.descriptor.program,
        packed.descriptor.digest,
        packed.archive,
        store_dir,
        limits,
    )
    .map(|result| result.descriptor)
    .map_err(|error| {
        Error::new(
            match error.kind {
                PublicationErrorKind::TooLarge => ErrorKind::InvalidInput,
                PublicationErrorKind::InvalidArtifact | PublicationErrorKind::ImmutableConflict => {
                    ErrorKind::Integrity
                }
                _ => ErrorKind::Io,
            },
            error.message,
        )
    })
}

fn pack(source: &Path, limits: &ArtifactLimits) -> Result<PackedProgram> {
    limits.validate()?;
    let manifest_bytes = read_bounded(
        &mut open_regular(&source.join("ledgence-program.json"))?,
        limits.max_manifest_bytes,
    )?;
    let manifest: ProgramManifest = serde_json::from_slice(&manifest_bytes)?;
    manifest.validate()?;
    let mut files = Vec::new();
    let mut entries = 0;
    collect_files(source, source, &mut files, &mut entries, limits.max_entries)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .last_modified_time(zip::DateTime::default());
    let mut expanded = 0u64;
    for (path, directory) in files {
        let name = path
            .to_str()
            .ok_or_else(|| AdapterError::Invalid("program path must be ASCII".into()))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        if directory {
            writer.add_directory(format!("{name}/"), options.unix_permissions(0o755))?;
        } else {
            let mut file = open_regular(&source.join(&path))?;
            let mode = 0o644 | executable_bits(&file.metadata()?);
            let bytes = read_bounded(&mut file, limits.max_file_bytes)?;
            expanded = expanded
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| AdapterError::Limit("program size overflow".into()))?;
            if expanded > limits.max_expanded_bytes {
                return Err(AdapterError::Limit("program expanded bytes".into()));
            }
            writer.start_file(name, options.unix_permissions(mode))?;
            writer.write_all(&bytes)?;
        }
        // Bound the in-memory archive during construction, including large input
        // that does not compress. Final headers are checked after finishing.
        if writer
            .get_ref()
            .is_some_and(|cursor| cursor.get_ref().len() as u64 > limits.max_archive_bytes)
        {
            return Err(AdapterError::Limit("program archive bytes".into()));
        }
    }
    let archive = writer.finish()?.into_inner();
    let descriptor = ProgramDescriptor {
        program: manifest.program,
        digest: Digest(format!("sha256:{}", digest_hex(&archive))),
        size: archive.len() as u64,
    };
    // Borrow the owned buffer: full CRC verification must not double the maximum
    // compressed archive memory retained by an admitted publication.
    verify_package(archive.as_slice(), &descriptor, limits)?;
    check_encoded_descriptor(&descriptor, limits)?;
    Ok(PackedProgram {
        descriptor,
        archive,
    })
}

/// Filesystem writer paired with FileProgramStore. Construction requires the
/// configured root to exist; ordinary publication does not silently create a
/// second store after a configuration mistake.
#[derive(Clone)]
pub struct FileProgramArtifactPublisher {
    root: PathBuf,
    limits: ArtifactLimits,
}
impl FileProgramArtifactPublisher {
    pub fn new(root: impl Into<PathBuf>, limits: ArtifactLimits) -> PublicationResult<Self> {
        limits.validate().map_err(|_| invalid())?;
        let root = fs::canonicalize(root.into()).map_err(|_| storage())?;
        if !root.is_dir() {
            return Err(storage());
        }
        Ok(Self { root, limits })
    }
}
impl ProgramArtifactPublisher for FileProgramArtifactPublisher {
    fn publish<'a>(
        &'a self,
        program: ProgramRef,
        expected_digest: Digest,
        archive: Vec<u8>,
    ) -> PublicationFuture<'a, PublishArtifactResult> {
        let publisher = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                persist_archive(
                    program,
                    expected_digest,
                    archive,
                    &publisher.root,
                    &publisher.limits,
                )
            })
            .await
            .map_err(|_| unknown())?
        })
    }
}

/// Verify every archive member before publishing its blob and then its release
/// descriptor. This synchronous primitive belongs on a bounded blocking executor.
/// Interrupted writes can leave unreferenced blobs or owned temporary files;
/// never delete shared blobs as rollback. Identical retries reconcile safely.
pub fn persist_archive(
    program: ProgramRef,
    expected_digest: Digest,
    archive: Vec<u8>,
    store: impl AsRef<Path>,
    limits: &ArtifactLimits,
) -> PublicationResult<PublishArtifactResult> {
    persist_with_observer(
        program,
        expected_digest,
        archive,
        store.as_ref(),
        limits,
        |_| Ok(()),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationStage {
    BeforeBlob,
    BlobStaged,
    BlobDurable,
    DescriptorStaged,
    DescriptorDurable,
}

fn persist_with_observer(
    program: ProgramRef,
    expected_digest: Digest,
    archive: Vec<u8>,
    store: &Path,
    limits: &ArtifactLimits,
    mut observe: impl FnMut(PublicationStage) -> std::io::Result<()>,
) -> PublicationResult<PublishArtifactResult> {
    limits.validate().map_err(|_| invalid())?;
    let descriptor = ProgramDescriptor {
        program,
        digest: expected_digest,
        size: archive.len() as u64,
    };
    descriptor.validate().map_err(|_| invalid())?;
    if descriptor.size > limits.max_archive_bytes {
        return Err(too_large());
    }
    verify_package(archive.as_slice(), &descriptor, limits).map_err(validation_error)?;
    let encoded = check_encoded_descriptor(&descriptor, limits).map_err(validation_error)?;
    let release = store
        .join("programs")
        .join(&descriptor.program.id)
        .join(&descriptor.program.version);
    let blobs = store.join("blobs");
    for directory in [
        store.to_path_buf(),
        store.join("programs"),
        store.join("programs").join(&descriptor.program.id),
        release.clone(),
    ] {
        ensure_directory(&directory).map_err(|_| storage())?;
    }
    ensure_directory(&blobs).map_err(|_| storage())?;
    observe(PublicationStage::BeforeBlob).map_err(|_| storage())?;
    publish_immutable(
        &blobs.join(format!("{}.zip", descriptor.digest.hex())),
        &archive,
        false,
        &mut observe,
    )?;
    observe(PublicationStage::BlobDurable).map_err(|_| storage())?;
    let already_published = publish_immutable(
        &release.join("descriptor.json"),
        &encoded,
        true,
        &mut observe,
    )?;
    observe(PublicationStage::DescriptorDurable).map_err(|_| unknown())?;
    Ok(PublishArtifactResult {
        descriptor,
        already_published,
    })
}

fn check_encoded_descriptor(
    descriptor: &ProgramDescriptor,
    limits: &ArtifactLimits,
) -> Result<Vec<u8>> {
    let encoded = serde_json::to_vec(descriptor)?;
    if encoded.len() as u64 > limits.max_descriptor_bytes {
        return Err(AdapterError::Limit("descriptor bytes".into()));
    }
    Ok(encoded)
}

fn invalid() -> PublicationError {
    PublicationError::new(
        PublicationErrorKind::InvalidArtifact,
        "artifact failed package verification",
    )
}
fn too_large() -> PublicationError {
    PublicationError::new(
        PublicationErrorKind::TooLarge,
        "artifact exceeds publication limits",
    )
}
fn storage() -> PublicationError {
    PublicationError::new(
        PublicationErrorKind::Storage,
        "artifact store operation failed",
    )
}
fn unknown() -> PublicationError {
    PublicationError::new(
        PublicationErrorKind::OutcomeUnknown,
        "publication outcome is unknown; retry the identical archive",
    )
}
fn validation_error(error: AdapterError) -> PublicationError {
    if matches!(error, AdapterError::Limit(_)) {
        too_large()
    } else {
        invalid()
    }
}

// Sync each new directory and its parent before publishing children. Also sync
// existing parents: a retry may encounter entries created before an interruption.
fn ensure_directory(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => (),
        Ok(_) => return Err(std::io::Error::other("store component is not a directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                ensure_directory(parent)?;
            }
            match fs::create_dir(path) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(path)?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(std::io::Error::other("store component is not a directory"));
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    }
    File::open(path)?.sync_all()?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn collect_files(
    root: &Path,
    directory: &Path,
    output: &mut Vec<(std::path::PathBuf, bool)>,
    entries: &mut usize,
    max_entries: usize,
) -> Result<()> {
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AdapterError::Invalid(
            "program source must be a real directory".into(),
        ));
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        *entries += 1;
        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|e| AdapterError::Invalid(e.to_string()))?
            .to_owned();
        if *entries > max_entries || relative.as_os_str().len() > 1024 {
            return Err(AdapterError::Limit("program entries or path length".into()));
        }
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            return Err(AdapterError::Invalid(
                "program source cannot contain links or special files".into(),
            ));
        }
        output.push((relative, metadata.is_dir()));
        if metadata.is_dir() {
            collect_files(root, &entry.path(), output, entries, max_entries)?;
        }
    }
    Ok(())
}
// The no-clobber link is the cross-process linearization point. Return whether
// this target existed, not whether another object (such as its blob) existed.
fn publish_immutable(
    target: &Path,
    bytes: &[u8],
    descriptor: bool,
    observe: &mut impl FnMut(PublicationStage) -> std::io::Result<()>,
) -> PublicationResult<bool> {
    let parent = target.parent().ok_or_else(storage)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|_| storage())?;
    temporary.write_all(bytes).map_err(|_| storage())?;
    make_readonly(temporary.path(), false).map_err(|_| storage())?;
    temporary.as_file().sync_all().map_err(|_| storage())?;
    observe(if descriptor {
        PublicationStage::DescriptorStaged
    } else {
        PublicationStage::BlobStaged
    })
    .map_err(|_| storage())?;
    match temporary.persist_noclobber(target) {
        Ok(file) => {
            file.sync_all()
                .and_then(|()| File::open(parent)?.sync_all())
                .map_err(|_| if descriptor { unknown() } else { storage() })?;
            Ok(false)
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut existing = open_regular(target).map_err(|_| storage())?;
            if !same_bytes(&mut existing, bytes).map_err(|_| storage())? {
                return Err(if descriptor {
                    PublicationError::new(
                        PublicationErrorKind::ImmutableConflict,
                        "program identity already refers to different bytes",
                    )
                } else {
                    storage()
                });
            }
            existing
                .sync_all()
                .and_then(|()| File::open(parent)?.sync_all())
                .map_err(|_| if descriptor { unknown() } else { storage() })?;
            Ok(true)
        }
        Err(_) => Err(storage()),
    }
}

fn same_bytes(file: &mut File, bytes: &[u8]) -> std::io::Result<bool> {
    if file.metadata()?.len() != bytes.len() as u64 {
        return Ok(false);
    }
    let mut buffer = [0u8; 16 * 1024];
    for expected in bytes.chunks(buffer.len()) {
        file.read_exact(&mut buffer[..expected.len()])?;
        if &buffer[..expected.len()] != expected {
            return Ok(false);
        }
    }
    Ok(file.read(&mut buffer[..1])? == 0)
}

#[cfg(test)]
mod tests;
