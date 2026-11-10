//! Private-side portable persistence, blob and temporary-spool ports.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactStoreIdentity {
    pub store_id: String,
    pub fingerprint: Digest,
}
impl ArtifactStoreIdentity {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.store_id, 128)?;
        self.fingerprint.validate()?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactObjectDescriptor {
    pub store: ArtifactStoreIdentity,
    pub artifact_id: String,
    pub upload_id: String,
    pub key: String,
    pub metadata: ArtifactMetadata,
}
impl ArtifactObjectDescriptor {
    pub fn validate(&self) -> Result<()> {
        self.store.validate()?;
        self.metadata.validate()?;
        for id in [&self.artifact_id, &self.upload_id] {
            validate_text(id, 128)?;
            if !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
            {
                return Err(invalid("invalid generated artifact identifier"));
            }
        }
        if self.key != format!("objects/{}/{}", self.artifact_id, self.upload_id) {
            return Err(invalid("invalid artifact object key"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct ArtifactIngressClaim {
    pub selection: ArtifactSelection,
    pub token: String,
    pub expires_at: Timestamp,
    pub object: ArtifactObjectDescriptor,
}
#[derive(Debug, Clone)]
pub struct ArtifactWritePermit {
    pub selection: ArtifactSelection,
    pub object: ArtifactObjectDescriptor,
    pub dispatch_expires_at: Timestamp,
}
#[derive(Debug, Clone)]
pub enum ArtifactBeginWrite {
    Dispatch(ArtifactWritePermit),
    ReconcileRequired(ArtifactUploadStatus),
}
/// Facts only. VerifiedComplete means the full object was read and its declared
/// SHA-256/size checked, or a confirmed checksum-valid single-create completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStorageObservation {
    VerifiedComplete,
    ConfirmedNoWrite,
    Uncertain,
    Absent,
    Corrupt,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactReadKind {
    Metadata,
    Content,
}
#[derive(Debug, Clone)]
pub struct ArtifactRecord {
    pub manifest: ArtifactManifest,
    pub object: ArtifactObjectDescriptor,
}
#[derive(Debug, Clone)]
pub struct ArtifactCleanupLease {
    pub upload_id: String,
    pub token: String,
    pub object: ArtifactObjectDescriptor,
}
#[derive(Debug, Clone)]
pub enum ArtifactMaintenanceWork {
    Inspect(ArtifactCleanupLease),
    Delete(ArtifactCleanupLease),
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMaintenanceProgress {
    pub inspected: u32,
    pub deleted: u32,
    pub pending: u64,
    pub uncertain: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactDeleteObservation {
    Deleted,
    Failed,
}

pub trait ExecutionArtifactCatalog: Send + Sync {
    fn bind_store<'a>(
        &'a self,
        identity: &'a ArtifactStoreIdentity,
        deadline: Instant,
    ) -> ContractFuture<'a, ()>;
    fn prepare<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        command: &'a ArtifactPrepareCommand,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactPrepareReply>;
    fn upload_status<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        upload_id: &'a str,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactUploadStatus>;
    fn replace<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        command: &'a ArtifactReplaceCommand,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactReplacementReceipt>;
    fn claim_ingress<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        selection: &'a ArtifactSelection,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactIngressClaim>;
    fn release_ingress<'a>(
        &'a self,
        claim: &'a ArtifactIngressClaim,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactUploadStatus>;
    fn begin_write<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        claim: &'a ArtifactIngressClaim,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactBeginWrite>;
    fn record_storage_observation<'a>(
        &'a self,
        upload_id: &'a str,
        observation: ArtifactStorageObservation,
        deadline: Instant,
    ) -> ContractFuture<'a, ()>;
    fn accept<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        selection: &'a ArtifactSelection,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactManifest>;
    fn resolve<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        artifact_id: &'a str,
        kind: ArtifactReadKind,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactRecord>;
    /// Authenticated preparation/reconciliation lookup; not a public storage URL.
    fn upload_object<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        selection: &'a ArtifactSelection,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactObjectDescriptor>;
    fn list<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        query: &'a ArtifactListQuery,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactPage>;
    fn claim_maintenance<'a>(
        &'a self,
        identity: &'a ArtifactStoreIdentity,
        limit: u32,
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<ArtifactMaintenanceWork>>;
    fn finish_delete<'a>(
        &'a self,
        lease: &'a ArtifactCleanupLease,
        observation: ArtifactDeleteObservation,
        deadline: Instant,
    ) -> ContractFuture<'a, ()>;
    fn maintenance_progress(
        &self,
        deadline: Instant,
    ) -> ContractFuture<'_, ArtifactMaintenanceProgress>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobPutOutcome {
    Created,
    ConfirmedNoWrite,
    Uncertain,
}
pub struct BlobRead {
    pub reader: Box<dyn ArtifactReader>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobDeleteOutcome {
    Deleted,
    Absent,
}
pub trait ExecutionArtifactBlobStore: Send + Sync {
    fn identity(&self) -> &ArtifactStoreIdentity;
    /// Close write admission and wait for independently retained local writes
    /// and cleanup. A timeout must preserve ownership; callers must not treat
    /// it as successful shutdown. Stores without detached work need no drain.
    fn drain(&self, _deadline: Instant) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn put_once<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        reader: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> ContractFuture<'a, BlobPutOutcome>;
    fn open<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        deadline: Instant,
    ) -> ContractFuture<'a, Option<BlobRead>>;
    fn delete<'a>(
        &'a self,
        object: &'a ArtifactObjectDescriptor,
        deadline: Instant,
    ) -> ContractFuture<'a, BlobDeleteOutcome>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactSpoolPurpose {
    Upload,
    Download,
}
pub trait ArtifactSpoolFactory: Send + Sync {
    fn create(
        &self,
        purpose: ArtifactSpoolPurpose,
        max_bytes: u64,
        deadline: Instant,
    ) -> ContractFuture<'_, Box<dyn ArtifactSpoolWriter>>;
    fn drain(&self, deadline: Instant) -> ContractFuture<'_, ()>;
}
pub trait ArtifactSpoolWriter: Send {
    fn append(&mut self, chunk: Vec<u8>) -> ContractFuture<'_, ()>;
    fn finish(self: Box<Self>) -> ContractFuture<'static, Box<dyn ArtifactReader>>;
}
