//! Filesystem/HTTPS program storage and a verified, bounded local ZIP cache.
//!
//! Archives use ordinary single-disk ZIP (stored/deflate, no ZIP64), portable
//! ASCII paths, and exactly one root `ledgence-program.json`. The cache owns a
//! dedicated directory exclusively; share a cloned cache, not multiple owners.
//! Publication preserves empty directories and regular-file executable bits on
//! Unix. Materialization strips write and special permission bits, retaining
//! only read permissions and the archive's executable bits for regular files.

mod archive;
mod cache;
mod error;
mod filesystem;
mod publish;
mod store;

pub use cache::FileArtifactCache;
use ledgence_worker_api::{Error, ErrorKind, ProgramDescriptor};
pub use publish::{
    FileProgramArtifactPublisher, PackedProgram, pack_directory, persist_archive, publish_directory,
};
use std::time::Duration;
pub use store::{FileProgramStore, HttpProgramStore};

/// Bounds apply even when remote length headers or archive metadata are false.
/// Exceeding a hard limit reports `InvalidInput`; `Capacity` is reserved for
/// recoverable cache pressure that retiring pinned warm sessions can resolve.
#[derive(Clone, Debug)]
pub struct ArtifactLimits {
    pub max_archive_bytes: u64,
    pub max_expanded_bytes: u64,
    pub max_file_bytes: u64,
    pub max_entries: usize,
    pub max_manifest_bytes: u64,
    pub max_descriptor_bytes: u64,
    /// Compressed archives + extracted regular-file bytes + descriptor bytes.
    /// Filesystem allocation/metadata overhead is not included in this quota.
    pub max_cache_bytes: u64,
    pub request_timeout: Duration,
}
impl Default for ArtifactLimits {
    fn default() -> Self {
        let publication = ledgence_worker_api::PublicationLimits::default();
        Self {
            max_archive_bytes: publication.max_archive_bytes,
            max_expanded_bytes: publication.max_expanded_bytes,
            max_file_bytes: publication.max_file_bytes,
            max_entries: publication.max_entries as usize,
            max_manifest_bytes: publication.max_manifest_bytes,
            max_descriptor_bytes: publication.max_descriptor_bytes,
            max_cache_bytes: 1024 * 1024 * 1024,
            request_timeout: Duration::from_secs(60),
        }
    }
}
impl ArtifactLimits {
    pub fn validate(&self) -> ledgence_worker_api::Result<()> {
        if self.max_archive_bytes == 0
            || self.max_archive_bytes > u32::MAX as u64
            || self.max_expanded_bytes == 0
            || self.max_file_bytes == 0
            || self.max_entries == 0
            || self.max_entries >= u16::MAX as usize
            || self.max_manifest_bytes == 0
            || self.max_descriptor_bytes == 0
            || self.max_cache_bytes == 0
            || self.request_timeout.is_zero()
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "artifact limits must be positive; ZIP32 archive/entry limits apply",
            ));
        }
        Ok(())
    }
    pub(crate) fn descriptor(&self, descriptor: &ProgramDescriptor) -> error::Result<()> {
        descriptor.validate()?;
        if descriptor.size > self.max_archive_bytes {
            return Err(error::AdapterError::Limit(
                "compressed archive bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Verify an immutable program archive for explicit catalog registration. This
/// checks the descriptor, manifest, paths, expansion and every member checksum,
/// without extraction, execution or a host-target compatibility requirement.
/// CPU-bound verification belongs on the caller's bounded blocking executor.
pub fn verify_program_package(
    bytes: Vec<u8>,
    descriptor: &ProgramDescriptor,
    limits: &ArtifactLimits,
) -> ledgence_worker_api::Result<ledgence_worker_api::ProgramManifest> {
    archive::verify_package(bytes, descriptor, limits).map_err(Into::into)
}
