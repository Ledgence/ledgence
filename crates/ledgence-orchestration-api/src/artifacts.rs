//! Immutable execution artifacts. Transport credentials and blob SDK types never
//! enter these contracts; authority is supplied by a trusted server adapter.
use crate::*;
use ledgence_worker_api::Digest;
use std::time::Instant;

mod store;
pub use store::*;

pub const ARTIFACT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const ARTIFACT_CHUNK_MAX_BYTES: usize = 256 * 1024;
pub const ARTIFACT_REF_MAX_BYTES: usize = 1024;
pub const ARTIFACT_MANIFEST_MAX_BYTES: usize = 4096;
pub const ARTIFACT_PAGE_MAX_BYTES: usize = 512 * 1024;
pub const ARTIFACT_TRANSFER_TIMEOUT_MS: u64 = 120_000;
pub const ARTIFACT_PREPARE_TIMEOUT_MS: u64 = 120_000;
pub const ARTIFACT_ROOT_PENDING_LIMIT: u32 = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub schema: String,
    pub id: String,
    pub digest: Digest,
    pub size: u64,
    pub media_type: String,
}
impl ArtifactRef {
    pub fn validate(&self) -> Result<()> {
        if self.schema != "ledgence.artifact.v1" {
            return Err(invalid("unsupported artifact reference"));
        }
        validate_text(&self.id, 128)?;
        ArtifactMetadata {
            digest: self.digest.clone(),
            size: self.size,
            media_type: self.media_type.clone(),
            filename: None,
        }
        .validate()?;
        bounded(self, ARTIFACT_REF_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMetadata {
    pub digest: Digest,
    pub size: u64,
    pub media_type: String,
    pub filename: Option<String>,
}
impl ArtifactMetadata {
    pub fn validate(&self) -> Result<()> {
        self.digest.validate()?;
        if self.size > ARTIFACT_MAX_BYTES {
            return Err(invalid("artifact exceeds maximum size"));
        }
        validate_text(&self.media_type, 128)?;
        if !self.media_type.is_ascii() || !self.media_type.contains('/') {
            return Err(invalid("invalid artifact media type"));
        }
        if let Some(name) = &self.filename {
            validate_text(name, 255)?;
            if name == "." || name == ".." || name.contains(['/', '\\']) {
                return Err(invalid("artifact filename must be a display basename"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactProducer {
    Task {
        task_id: String,
    },
    WorkflowActivation {
        workflow_id: String,
        activation_id: String,
    },
    VerifiedActor {
        identity: PrincipalIdentity,
    },
    TrustedInstance {
        instance_id: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifest {
    pub reference: ArtifactRef,
    pub scope: Scope,
    pub root: ExecutionTarget,
    pub producer: ArtifactProducer,
    pub key: String,
    pub filename: Option<String>,
    pub created_at: Timestamp,
    pub accepted_at: Timestamp,
    pub first_attempt_id: Option<String>,
}
impl ArtifactManifest {
    pub fn validate(&self) -> Result<()> {
        self.reference.validate()?;
        self.scope.validate()?;
        self.root.validate()?;
        validate_text(&self.key, 128)?;
        match &self.producer {
            ArtifactProducer::Task { task_id } => validate_text(task_id, 128)?,
            ArtifactProducer::WorkflowActivation {
                workflow_id,
                activation_id,
            } => {
                validate_text(workflow_id, 128)?;
                validate_text(activation_id, 128)?;
            }
            ArtifactProducer::VerifiedActor { identity } => identity.validate()?,
            ArtifactProducer::TrustedInstance { instance_id } => validate_text(instance_id, 128)?,
        }
        if let Some(id) = &self.first_attempt_id {
            validate_text(id, 128)?;
        }
        ArtifactMetadata {
            digest: self.reference.digest.clone(),
            size: self.reference.size,
            media_type: self.reference.media_type.clone(),
            filename: self.filename.clone(),
        }
        .validate()?;
        if self.created_at == 0
            || self.accepted_at > 253_402_300_799_999
            || self.accepted_at < self.created_at
        {
            return Err(invalid("invalid artifact timestamps"));
        }
        bounded(self, ARTIFACT_MANIFEST_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSelection {
    pub artifact_id: String,
    pub upload_id: String,
    pub revision: u64,
}
impl ArtifactSelection {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.artifact_id, 128)?;
        validate_text(&self.upload_id, 128)?;
        if self.revision == 0 || self.revision > i64::MAX as u64 {
            return Err(invalid("invalid artifact selection revision"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPrepareCommand {
    pub scope: Scope,
    pub target: ExecutionTarget,
    pub key: String,
    pub metadata: ArtifactMetadata,
}
impl ArtifactPrepareCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.target.validate()?;
        validate_text(&self.key, 128)?;
        self.metadata.validate()?;
        bounded(self, 4096)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode_unique_json(bytes, 4096)?;
        value.validate()?;
        Ok(value)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactUploadStage {
    Prepared,
    Receiving,
    Writing,
    Uncertain,
    Ready,
    ExpiredNoWrite,
    Retired,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRetryGuidance {
    Upload,
    Wait,
    Reconcile,
    Replace,
    Complete,
    Unavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUploadStatus {
    pub selection: ArtifactSelection,
    pub selected: bool,
    pub stage: ArtifactUploadStage,
    pub prepare_expires_at: Timestamp,
    pub ingress_expires_at: Option<Timestamp>,
    pub dispatch_expires_at: Option<Timestamp>,
    pub successor: Option<ArtifactSelection>,
    pub reference: Option<ArtifactRef>,
    pub retry: ArtifactRetryGuidance,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPrepareReply {
    pub upload: ArtifactUploadStatus,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUploadReply {
    pub upload: ArtifactUploadStatus,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReplaceCommand {
    pub upload_id: String,
    pub expected_selection_revision: u64,
}
impl ArtifactReplaceCommand {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.upload_id, 128)?;
        if self.expected_selection_revision == 0
            || self.expected_selection_revision >= i64::MAX as u64
        {
            return Err(invalid("invalid replacement revision"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReplacementReceipt {
    pub replaced: ArtifactSelection,
    pub replacement: ArtifactSelection,
    /// Fresh status of `replacement`, which may itself have been deselected.
    /// A receipt replay never substitutes or reselects a later generation.
    pub current: ArtifactUploadStatus,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInspectQuery {
    pub scope: Scope,
    pub artifact_id: String,
}
impl ArtifactInspectQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.artifact_id, 128)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactListQuery {
    pub scope: Scope,
    pub target: ExecutionTarget,
    pub limit: u32,
    pub cursor: Option<String>,
}
impl ArtifactListQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.target.validate()?;
        if !(1..=100).contains(&self.limit) {
            return Err(invalid("artifact page limit must be between 1 and 100"));
        }
        if let Some(cursor) = &self.cursor {
            validate_text(cursor, 4096)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPage {
    pub items: Vec<ArtifactManifest>,
    pub next_cursor: Option<String>,
}

/// Server assertions only, deliberately not deserializable.
#[derive(Debug, Clone)]
pub enum ArtifactCaller {
    Verified(VerifiedPrincipal),
    TrustedInstance(SelfHostedInstanceContext),
}
#[derive(Debug, Clone)]
pub enum ArtifactAuthority {
    External(ArtifactCaller),
    Worker {
        caller: ArtifactCaller,
        lease: LeaseOwner,
    },
}
impl ArtifactAuthority {
    pub fn caller(&self) -> &ArtifactCaller {
        match self {
            Self::External(caller) | Self::Worker { caller, .. } => caller,
        }
    }
    pub fn scope(&self) -> &Scope {
        match self.caller() {
            ArtifactCaller::Verified(p) => &p.audience().scope,
            ArtifactCaller::TrustedInstance(i) => &i.scope,
        }
    }
}
/// Routing information only; never evidence of authorization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactRequester {
    External { scope: Scope },
    Worker { owner: LeaseOwner },
}
impl ArtifactRequester {
    pub fn scope(&self) -> &Scope {
        match self {
            Self::External { scope } => scope,
            Self::Worker { owner } => &owner.scope,
        }
    }
}

pub trait ArtifactReader: Send {
    /// Empty is EOF. Implementations must never return more than `max_bytes`.
    fn read_chunk(&mut self, max_bytes: usize) -> ContractFuture<'_, Vec<u8>>;
}
pub struct ArtifactDownload {
    pub manifest: ArtifactManifest,
    pub reader: Box<dyn ArtifactReader>,
    pub deadline: Instant,
}
/// Trusted service-owned verified spool. No serializable input can create this handle.
pub struct PreparedArtifactDownload {
    manifest: ArtifactManifest,
    reader: Box<dyn ArtifactReader>,
    deadline: Instant,
}
impl PreparedArtifactDownload {
    pub fn new(
        manifest: ArtifactManifest,
        reader: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> Self {
        Self {
            manifest,
            reader,
            deadline,
        }
    }
    pub fn manifest(&self) -> &ArtifactManifest {
        &self.manifest
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn into_download(self) -> ArtifactDownload {
        ArtifactDownload {
            manifest: self.manifest,
            reader: self.reader,
            deadline: self.deadline,
        }
    }
}
pub trait ArtifactMetadataReader: Send + Sync {
    fn inspect<'a>(
        &'a self,
        query: &'a ArtifactInspectQuery,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactManifest>;
    fn list<'a>(
        &'a self,
        query: &'a ArtifactListQuery,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactPage>;
}
/// Transport ambiguity is distinct from a malformed or mismatched response.
/// Callers may reconcile a known logical upload after OutcomeUnknown, but must
/// never treat InvalidResponse as a trustworthy storage receipt.
#[derive(Debug, Clone, PartialEq)]
pub enum ArtifactClientError {
    Rejected(ContractError),
    OutcomeUnknown { message: String },
    InvalidResponse { message: String },
}
impl std::fmt::Display for ArtifactClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(error) => error.fmt(f),
            Self::OutcomeUnknown { message } | Self::InvalidResponse { message } => {
                f.write_str(message)
            }
        }
    }
}
impl std::error::Error for ArtifactClientError {}
impl From<ContractError> for ArtifactClientError {
    fn from(error: ContractError) -> Self {
        Self::Rejected(error)
    }
}
impl From<ArtifactClientError> for ContractError {
    fn from(error: ArtifactClientError) -> Self {
        match error {
            ArtifactClientError::Rejected(error) => error,
            ArtifactClientError::OutcomeUnknown { message }
            | ArtifactClientError::InvalidResponse { message } => Self::Unavailable(message),
        }
    }
}
pub type ArtifactClientResult<T> = std::result::Result<T, ArtifactClientError>;
pub type ArtifactClientFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = ArtifactClientResult<T>> + Send + 'a>>;

pub trait ExecutionArtifactClient: Send + Sync {
    fn prepare<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        command: &'a ArtifactPrepareCommand,
        deadline: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactPrepareReply>;
    fn upload_status<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        upload_id: &'a str,
        deadline: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactUploadStatus>;
    fn replace<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        command: &'a ArtifactReplaceCommand,
        deadline: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactReplacementReceipt>;
    fn put(
        &self,
        requester: ArtifactRequester,
        selection: ArtifactSelection,
        body: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> ArtifactClientFuture<'_, ArtifactUploadReply>;
    fn reconcile<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        selection: &'a ArtifactSelection,
        deadline: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactUploadReply>;
    fn download(
        &self,
        requester: ArtifactRequester,
        reference: ArtifactRef,
        deadline: Instant,
    ) -> ArtifactClientFuture<'_, ArtifactDownload>;
}
pub trait ExecutionArtifactService: Send + Sync {
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
    fn put(
        &self,
        authority: ArtifactAuthority,
        selection: ArtifactSelection,
        body: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> ContractFuture<'_, ArtifactUploadReply>;
    fn reconcile<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        selection: &'a ArtifactSelection,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactUploadReply>;
    fn inspect<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        query: &'a ArtifactInspectQuery,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactManifest>;
    fn list<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        query: &'a ArtifactListQuery,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactPage>;
    fn prepare_download<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        reference: &'a ArtifactRef,
        deadline: Instant,
    ) -> ContractFuture<'a, PreparedArtifactDownload>;
    fn release_download<'a>(
        &'a self,
        authority: &'a ArtifactAuthority,
        prepared: PreparedArtifactDownload,
        deadline: Instant,
    ) -> ContractFuture<'a, ArtifactDownload>;
    fn maintain(
        &self,
        limit: u32,
        deadline: Instant,
    ) -> ContractFuture<'_, ArtifactMaintenanceProgress>;
    fn drain(&self, deadline: Instant) -> ContractFuture<'_, ()>;
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn bounded<T: Serialize>(value: &T, maximum: usize) -> Result<()> {
    Ok(crate::submission::check_encoded_size(
        value, maximum, "artifact",
    )?)
}

impl ArtifactUploadStatus {
    pub fn validate(&self) -> Result<()> {
        self.selection.validate()?;
        for time in [
            Some(self.prepare_expires_at),
            self.ingress_expires_at,
            self.dispatch_expires_at,
        ]
        .into_iter()
        .flatten()
        {
            if time == 0 || time > 253_402_300_799_999 {
                return Err(invalid("invalid artifact upload timestamp"));
            }
        }
        if let Some(successor) = &self.successor {
            successor.validate()?;
            if successor.artifact_id != self.selection.artifact_id
                || successor.revision != self.selection.revision + 1
                || successor.upload_id == self.selection.upload_id
                || self.selected
            {
                return Err(invalid("invalid artifact successor"));
            }
        }
        if let Some(reference) = &self.reference {
            reference.validate()?;
            if reference.id != self.selection.artifact_id
                || !self.selected
                || self.stage != ArtifactUploadStage::Ready
            {
                return Err(invalid("invalid accepted artifact reference"));
            }
        }
        if (self.stage == ArtifactUploadStage::Receiving) != self.ingress_expires_at.is_some()
            || (self.selected && self.stage == ArtifactUploadStage::Ready)
                != self.reference.is_some()
        {
            return Err(invalid("inconsistent artifact upload stage"));
        }
        if matches!(
            self.stage,
            ArtifactUploadStage::Writing
                | ArtifactUploadStage::Uncertain
                | ArtifactUploadStage::Ready
        ) && self.dispatch_expires_at.is_none()
        {
            return Err(invalid("artifact write is missing dispatch provenance"));
        }
        let allowed = match self.stage {
            ArtifactUploadStage::Prepared => matches!(
                self.retry,
                ArtifactRetryGuidance::Upload | ArtifactRetryGuidance::Replace
            ),
            ArtifactUploadStage::Receiving => matches!(
                self.retry,
                ArtifactRetryGuidance::Wait | ArtifactRetryGuidance::Replace
            ),
            ArtifactUploadStage::Writing | ArtifactUploadStage::Uncertain => matches!(
                self.retry,
                ArtifactRetryGuidance::Reconcile | ArtifactRetryGuidance::Replace
            ),
            ArtifactUploadStage::Ready => self.retry == ArtifactRetryGuidance::Complete,
            ArtifactUploadStage::ExpiredNoWrite => self.retry == ArtifactRetryGuidance::Replace,
            ArtifactUploadStage::Retired => self.retry == ArtifactRetryGuidance::Unavailable,
        };
        if self.selected && !allowed {
            return Err(invalid("artifact retry guidance disagrees with stage"));
        }
        if !self.selected && self.retry != ArtifactRetryGuidance::Unavailable {
            return Err(invalid("historical upload cannot grant retry permission"));
        }
        bounded(self, 4096)
    }
}
impl ArtifactPrepareReply {
    pub fn validate(&self) -> Result<()> {
        self.upload.validate()
    }
}
impl ArtifactUploadReply {
    pub fn validate(&self) -> Result<()> {
        self.upload.validate()
    }
}
impl ArtifactReplacementReceipt {
    pub fn validate(&self) -> Result<()> {
        self.replaced.validate()?;
        self.replacement.validate()?;
        self.current.validate()?;
        if self.replaced.artifact_id != self.replacement.artifact_id
            || self.replaced.upload_id == self.replacement.upload_id
            || self.replacement.revision != self.replaced.revision + 1
            || self.current.selection != self.replacement
        {
            return Err(invalid("invalid artifact replacement receipt"));
        }
        Ok(())
    }
}
impl ArtifactPage {
    pub fn validate(&self) -> Result<()> {
        if self.items.len() > 100 {
            return Err(invalid("artifact page exceeds item bound"));
        }
        let mut ids = std::collections::HashSet::new();
        for item in &self.items {
            item.validate()?;
            if self
                .items
                .first()
                .is_some_and(|first| first.scope != item.scope || first.root != item.root)
            {
                return Err(invalid("artifact page mixes execution roots"));
            }
            if !ids.insert(&item.reference.id) {
                return Err(invalid("duplicate artifact in metadata page"));
            }
        }
        if let Some(cursor) = &self.next_cursor {
            validate_text(cursor, 4096)?;
            if self.items.is_empty() {
                return Err(invalid("empty artifact page cannot continue"));
            }
        }
        bounded(self, ARTIFACT_PAGE_MAX_BYTES)
    }
    pub fn validate_for(&self, query: &ArtifactListQuery) -> Result<()> {
        query.validate()?;
        self.validate()?;
        if self.items.len() > query.limit as usize
            || self.items.iter().any(|item| item.scope != query.scope)
        {
            return Err(invalid("artifact page does not match query"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metadata() -> ArtifactMetadata {
        ArtifactMetadata {
            digest: Digest(format!("sha256:{}", "a".repeat(64))),
            size: 0,
            media_type: "application/octet-stream".into(),
            filename: None,
        }
    }
    #[test]
    fn metadata_accepts_empty_and_exact_maximum_but_rejects_unsafe_names() {
        let mut value = metadata();
        value.validate().unwrap();
        value.size = ARTIFACT_MAX_BYTES;
        value.validate().unwrap();
        value.size += 1;
        assert!(value.validate().is_err());
        for name in ["../file", "path/file", "path\\file", ".", "..", "x\n"] {
            let mut value = metadata();
            value.filename = Some(name.into());
            assert!(value.validate().is_err());
        }
    }
    #[test]
    fn strict_prepare_rejects_duplicate_and_unknown_fields() {
        let command = ArtifactPrepareCommand {
            scope: Scope {
                tenant_id: "acme".into(),
                namespace: "test".into(),
            },
            target: ExecutionTarget::Task {
                id: "task_a".into(),
            },
            key: "report".into(),
            metadata: metadata(),
        };
        let encoded = serde_json::to_string(&command).unwrap();
        ArtifactPrepareCommand::decode(encoded.as_bytes()).unwrap();
        assert!(
            ArtifactPrepareCommand::decode(
                encoded
                    .replacen("{", "{\"root_id\":\"forged\",", 1)
                    .as_bytes()
            )
            .is_err()
        );
        assert!(
            ArtifactPrepareCommand::decode(
                encoded.replacen("{", "{\"key\":\"other\",", 1).as_bytes()
            )
            .is_err()
        );
    }
    #[test]
    fn descriptor_cannot_turn_display_filename_or_client_key_into_storage_path() {
        let mut object = ArtifactObjectDescriptor {
            store: ArtifactStoreIdentity {
                store_id: "store".into(),
                fingerprint: metadata().digest,
            },
            artifact_id: "art_one".into(),
            upload_id: "upl_one".into(),
            key: "objects/art_one/upl_one".into(),
            metadata: metadata(),
        };
        object.validate().unwrap();
        object.key = "../../etc/passwd".into();
        assert!(object.validate().is_err());
    }
}
