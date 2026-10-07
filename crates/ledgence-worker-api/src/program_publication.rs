//! Immutable artifact publication, separate from the worker's read-only store.
use crate::{Digest, ProgramDescriptor, ProgramRef};
use serde::{Deserialize, Serialize};
use std::{fmt, future::Future, pin::Pin};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicationErrorKind {
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

impl Default for PublicationLimits {
    fn default() -> Self {
        Self {
            max_archive_bytes: 64 * 1024 * 1024,
            max_expanded_bytes: 256 * 1024 * 1024,
            max_file_bytes: 64 * 1024 * 1024,
            max_entries: 4096,
            max_manifest_bytes: 64 * 1024,
            max_descriptor_bytes: 16 * 1024,
        }
    }
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
