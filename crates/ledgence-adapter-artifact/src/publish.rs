use crate::{
    ArtifactLimits,
    archive::{digest_hex, inspect_for_publication, make_readonly, read_bounded},
    error::{AdapterError, Result},
    filesystem::{executable_bits, open_regular},
};
use ledgence_worker_api::{Digest, ProgramDescriptor, ProgramManifest};
use std::{
    fs::{self, File},
    io::{Cursor, Write},
    path::Path,
};
use zip::{ZipWriter, write::SimpleFileOptions};

/// Publish a fully prepared program directory to a local immutable store.
/// Dependencies must already be prepared for the manifest target platform, which
/// may differ from this publishing host. Execution compatibility is checked by
/// the worker cache. A release identity may be published
/// again only with identical bytes. The descriptor becomes visible last.
pub fn publish_directory(
    source_dir: impl AsRef<Path>,
    store_dir: impl AsRef<Path>,
    limits: &ArtifactLimits,
) -> ledgence_worker_api::Result<ProgramDescriptor> {
    publish(source_dir.as_ref(), store_dir.as_ref(), limits).map_err(Into::into)
}
fn publish(source: &Path, store: &Path, limits: &ArtifactLimits) -> Result<ProgramDescriptor> {
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
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
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
    inspect_for_publication(archive.clone(), &descriptor, limits)?;
    let encoded = serde_json::to_vec(&descriptor)?;
    if encoded.len() as u64 > limits.max_descriptor_bytes {
        return Err(AdapterError::Limit("descriptor bytes".into()));
    }
    let release = store
        .join("programs")
        .join(&descriptor.program.id)
        .join(&descriptor.program.version);
    fs::create_dir_all(&release)?;
    let blobs = store.join("blobs");
    fs::create_dir_all(&blobs)?;
    publish_immutable(
        &blobs.join(format!("{}.zip", descriptor.digest.hex())),
        &archive,
    )?;
    publish_immutable(&release.join("descriptor.json"), &encoded)?;
    Ok(descriptor)
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
fn publish_immutable(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| AdapterError::Invalid("missing store directory".into()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    make_readonly(temporary.path(), false)?;
    match temporary.persist_noclobber(target) {
        Ok(_) => {
            File::open(parent)?.sync_all()?;
            Ok(())
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(&mut open_regular(target)?, bytes.len() as u64)?;
            if existing == bytes {
                Ok(())
            } else {
                Err(AdapterError::Invalid(
                    "immutable release already exists with different bytes".into(),
                ))
            }
        }
        Err(error) => Err(AdapterError::Io(error.error)),
    }
}
