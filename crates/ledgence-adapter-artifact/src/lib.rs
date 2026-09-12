//! Filesystem/HTTPS program storage and a verified, bounded local ZIP cache.
//!
//! Archives use ordinary single-disk ZIP (stored/deflate, no ZIP64), portable
//! ASCII paths, and exactly one root `ledgence-program.json`. The cache owns a
//! dedicated directory exclusively; share a cloned cache, not multiple owners.

mod archive;
mod cache;
mod error;
mod publish;
mod store;

pub use cache::FileArtifactCache;
use ledgence_worker_api::{Error, ErrorKind, ProgramDescriptor};
pub use publish::publish_directory;
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
        Self {
            max_archive_bytes: 64 * 1024 * 1024,
            max_expanded_bytes: 256 * 1024 * 1024,
            max_file_bytes: 64 * 1024 * 1024,
            max_entries: 4096,
            max_manifest_bytes: 64 * 1024,
            max_descriptor_bytes: 16 * 1024,
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
