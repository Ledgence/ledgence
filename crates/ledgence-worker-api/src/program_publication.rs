//! Immutable artifact publication, separate from the worker's read-only store.
use crate::{Digest, ProgramDescriptor, ProgramManifest, ProgramRef};
use serde::{Deserialize, Serialize};
use std::{fmt, future::Future, pin::Pin};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicationErrorKind {
    Unauthenticated,
    Forbidden,
    #[serde(rename = "publication_disabled")]
    Disabled,
    #[serde(rename = "invalid_artifact")]
    InvalidArtifact,
    #[serde(rename = "artifact_too_large")]
    TooLarge,
    #[serde(rename = "immutable_conflict")]
    ImmutableConflict,
    #[serde(rename = "publication_saturated")]
    Saturated,
    #[serde(rename = "publication_storage")]
    Storage,
    #[serde(rename = "publication_outcome_unknown")]
    OutcomeUnknown,
}

/// Messages crossing this boundary must be bounded and omit paths, credentials,
/// archive contents, and underlying transport errors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationError {
    #[serde(rename = "code")]
    pub kind: PublicationErrorKind,
    pub message: String,
}
impl PublicationError {
    pub fn new(kind: PublicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}
impl fmt::Display for PublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for PublicationError {}
pub type PublicationResult<T> = std::result::Result<T, PublicationError>;
pub type PublicationFuture<'a, T> = Pin<Box<dyn Future<Output = PublicationResult<T>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationLimits {
    pub max_archive_bytes: u64,
    pub max_expanded_bytes: u64,
    pub max_file_bytes: u64,
    pub max_entries: u64,
    pub max_manifest_bytes: u64,
    pub max_descriptor_bytes: u64,
}

/// Fixed package verification contracts. Profile selection is transport-local;
/// it does not add fields to the released capabilities or manifest JSON.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramPackageProfile {
    Legacy4096,
    Entries8192V1,
}

impl ProgramPackageProfile {
    /// Only the raw ZIP member maximum differs between these profiles.
    pub const fn limits(self) -> PublicationLimits {
        PublicationLimits {
            max_archive_bytes: 64 * 1024 * 1024,
            max_expanded_bytes: 256 * 1024 * 1024,
            max_file_bytes: 64 * 1024 * 1024,
            max_entries: match self {
                Self::Legacy4096 => 4096,
                Self::Entries8192V1 => 8192,
            },
            max_manifest_bytes: 64 * 1024,
            max_descriptor_bytes: 16 * 1024,
        }
    }
}

impl Default for PublicationLimits {
    fn default() -> Self {
        ProgramPackageProfile::Legacy4096.limits()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramPackageInspection {
    pub manifest: ProgramManifest,
    /// Actual validated raw ZIP members, including directories and the manifest.
    /// Implicit parent directories are not additional archive members.
    pub entry_count: u64,
}

/// Complete package verification without consuming or copying its archive.
/// Callers run this CPU-bound operation on their owned blocking executor and
/// retain the same bytes for publication. Implementors verify descriptor identity
/// and encoded size, actual archive length/digest, raw directory consistency,
/// every path, expansion, checksum, manifest, and all supplied limits. Inspection
/// must not execute programs or require compatibility with the inspecting host.
pub trait ProgramPackageInspector: Send + Sync {
    fn inspect(
        &self,
        descriptor: &ProgramDescriptor,
        archive: &[u8],
        limits: &PublicationLimits,
    ) -> PublicationResult<ProgramPackageInspection>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationCapabilities {
    pub enabled: bool,
    pub registration_enabled: bool,
    pub mode: Option<String>,
    pub limits: PublicationLimits,
    pub max_concurrent_uploads: u32,
    pub transfer_timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishArtifactResult {
    pub descriptor: ProgramDescriptor,
    /// True only when this identical descriptor already existed. A preexisting
    /// deduplicated blob does not make a new descriptor a repeated publication.
    pub already_published: bool,
}

/// Immutable publication on an executor chosen by the adapter. Callers retain
/// admission permits until this future finishes even when the request expires.
/// Ownership transfers without cloning a potentially large body. An unknown
/// outcome is reconciled by submitting exactly the same bytes again.
///
/// Implementors must validate the actual archive length and SHA-256, every
/// archive member and checksum, expansion/path/profile limits, and the manifest's
/// exact program identity before making content resolvable. The HTTP adapter
/// bounds transfer resources but deliberately does not duplicate this verification.
/// A successful result must match the supplied identity, digest and actual size.
///
/// Publication is immutable across processes: identical bytes reconcile, while
/// different bytes for an existing program/version return `ImmutableConflict`.
/// The complete durable blob must precede its descriptor becoming visible.
/// `already_published` concerns that identical descriptor, not a deduplicated blob.
/// Storage failures must not remove shared blobs; interrupted publication can be
/// reconciled by retrying the same bytes. Once visibility may have changed without
/// a confirmed outcome, return `OutcomeUnknown` instead of claiming rollback.
/// Adapters must retain any accepted blocking persistence until completion; the
/// caller owns admission and shutdown draining around the returned future.
pub trait ProgramArtifactPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        program: ProgramRef,
        expected_digest: Digest,
        archive: Vec<u8>,
    ) -> PublicationFuture<'a, PublishArtifactResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_preserve_legacy_limits_and_wire_shape() {
        let legacy = ProgramPackageProfile::Legacy4096.limits();
        assert_eq!(PublicationLimits::default(), legacy);
        assert_eq!(
            serde_json::to_value(&legacy).unwrap(),
            serde_json::json!({
                "max_archive_bytes": 67108864,
                "max_expanded_bytes": 268435456,
                "max_file_bytes": 67108864,
                "max_entries": 4096,
                "max_manifest_bytes": 65536,
                "max_descriptor_bytes": 16384
            })
        );
        let mut expanded = ProgramPackageProfile::Entries8192V1.limits();
        assert_eq!(expanded.max_entries, 8192);
        expanded.max_entries = legacy.max_entries;
        assert_eq!(expanded, legacy);
    }
}
