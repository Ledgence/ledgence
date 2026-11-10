//! Verified operator decisions and durable, payload-free response observations.
//! This capability is indivisible. A successful receipt follows durable commit.
use crate::*;
use std::collections::HashSet;

pub const AUDIT_EVENT_MAX_BYTES: usize = 8192;
pub const AUDIT_PAGE_MAX_BYTES: usize = 1024 * 1024;
pub const AUDIT_CURSOR_MAX_BYTES: usize = 8192;
pub const AUDIT_MAX_LIMIT: u32 = 100;

/// Portable read contract for authenticated remote clients. Authentication is
/// transport-owned; a client cannot manufacture a verified server principal.
pub trait AuditReader: Send + Sync {
    fn query_audit<'a>(&'a self, query: &'a AuditQuery) -> ContractFuture<'a, AuditPage>;
}

pub trait OperatorAccess: Send + Sync {
    fn decide_verified<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ApprovalDecisionCommand,
        request: &'a OperatorRequest,
    ) -> ContractFuture<'a, AuditedDecision>;
    fn record_sensitive_response<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        observation: &'a SensitiveObservation,
    ) -> ContractFuture<'a, AuditReceipt>;
    fn query_audit<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a AuditQuery,
    ) -> ContractFuture<'a, AuditPage>;
}

/// Trusted transport context. Correlation IDs never deduplicate observations.
/// ```compile_fail
/// use ledgence_orchestration_api::OperatorRequest;
/// let _: OperatorRequest = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct OperatorRequest {
    observation_id: String,
    request_id: Option<String>,
}
impl OperatorRequest {
    pub fn new(observation_id: String, request_id: Option<String>) -> Result<Self> {
        validate_text(&observation_id, 128)?;
        if let Some(id) = &request_id {
            validate_text(id, 128)?;
        }
        Ok(Self {
            observation_id,
            request_id,
        })
    }
    pub fn observation_id(&self) -> &str {
        &self.observation_id
    }
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
}
#[derive(Debug, Clone)]
pub struct SensitiveObservation {
    pub request: OperatorRequest,
    pub read: AuditReadKind,
    pub resource: AuditResource,
}
impl SensitiveObservation {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        if !self.read.matches(&self.resource) {
            return Err(invalid("audit read resource mismatch"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditActor {
    pub issuer: String,
    pub subject: String,
    pub class: PrincipalClass,
    pub credential_id: String,
}
impl AuditActor {
    pub fn from_principal(principal: &VerifiedPrincipal) -> Self {
        Self {
            issuer: principal.identity().issuer.clone(),
            subject: principal.identity().subject.clone(),
            class: principal.class(),
            credential_id: principal.credential_id().into(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        PrincipalIdentity {
            issuer: self.issuer.clone(),
            subject: self.subject.clone(),
        }
        .validate()?;
        validate_text(&self.credential_id, 128)?;
        if self.class == PrincipalClass::Worker {
            return Err(invalid("worker is not an operator audit actor"));
        }
        Ok(())
    }
    pub fn same_actor(&self, principal: &VerifiedPrincipal) -> bool {
        self.issuer == principal.identity().issuer
            && self.subject == principal.identity().subject
            && self.class == principal.class()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditReadKind {
    ArtifactDownload,
    TaskInspect,
    TaskResult,
    WorkflowResult,
    WorkflowInput,
    LocalStepInspect,
    ApprovalInspect,
    ApprovalList,
    ApprovalDecision,
    EffectInspect,
    AdmissionInspect,
    TaskAdmissionInspect,
    QueueHolds,
    ResourceSources,
}
impl AuditReadKind {
    fn matches(self, resource: &AuditResource) -> bool {
        matches!(
            (self, resource),
            (Self::ArtifactDownload, AuditResource::Artifact { .. })
                | (Self::QueueHolds, AuditResource::Queue { .. })
                | (Self::ResourceSources, AuditResource::Resource { .. })
                | (
                    Self::TaskInspect | Self::TaskResult,
                    AuditResource::Task { .. }
                )
                | (
                    Self::WorkflowResult | Self::WorkflowInput,
                    AuditResource::Workflow { .. }
                )
                | (Self::LocalStepInspect, AuditResource::LocalStep { .. })
                | (
                    Self::ApprovalInspect | Self::ApprovalDecision,
                    AuditResource::Approval { .. }
                )
                | (Self::ApprovalList, AuditResource::ApprovalPage { .. })
                | (Self::EffectInspect, AuditResource::Effect { .. })
                | (Self::AdmissionInspect, AuditResource::Admission { .. })
                | (
                    Self::TaskAdmissionInspect,
                    AuditResource::TaskAdmission { .. }
                )
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResourceKind {
    Queue,
    Resource,
    Task,
    Workflow,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditResource {
    Artifact {
        root: ExecutionTarget,
        artifact_id: String,
    },
    Queue {
        queue: String,
    },
    TaskAdmission {
        occurrence: TaskAdmissionOccurrence,
    },
    Resource {
        name: String,
    },
    Admission {
        occurrence: AdmissionOccurrence,
    },
    Effect {
        workflow_id: String,
        activation_id: String,
        approval_key: String,
    },
    Task {
        task_id: String,
    },
    Workflow {
        workflow_id: String,
    },
    LocalStep {
        workflow_id: String,
        activation_id: String,
        step_key: String,
    },
    Approval {
        workflow_id: String,
        key: String,
    },
    ApprovalPage {
        workflow_id: String,
        keys: Vec<String>,
    },
}
impl AuditResource {
    pub fn target(&self) -> (AuditResourceKind, &str) {
        match self {
            Self::Artifact { root, .. } => (
                match root {
                    ExecutionTarget::Task { .. } => AuditResourceKind::Task,
                    ExecutionTarget::Workflow { .. } => AuditResourceKind::Workflow,
                },
                root.id(),
            ),
            Self::Queue { queue } => (AuditResourceKind::Queue, queue),
            Self::Task { task_id } => (AuditResourceKind::Task, task_id),
            Self::TaskAdmission { occurrence } => (AuditResourceKind::Task, &occurrence.task_id),
            Self::Resource { name } => (AuditResourceKind::Resource, name),
            Self::Admission { occurrence } => {
                (AuditResourceKind::Workflow, &occurrence.workflow_id)
            }
            Self::Workflow { workflow_id }
            | Self::LocalStep { workflow_id, .. }
            | Self::Approval { workflow_id, .. }
            | Self::ApprovalPage { workflow_id, .. }
            | Self::Effect { workflow_id, .. } => (AuditResourceKind::Workflow, workflow_id),
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(self.target().1, 128)?;
        match self {
            Self::Artifact { root, artifact_id } => {
                root.validate()?;
                validate_text(artifact_id, 128)?;
            }
            Self::Admission { occurrence } => occurrence.validate()?,
            Self::TaskAdmission { occurrence } => occurrence.validate()?,
            Self::Resource { name } => crate::admission::name(name)?,
            Self::Effect {
                activation_id,
                approval_key,
                ..
            } => {
                validate_text(activation_id, 128)?;
                validate_text(approval_key, 128)?;
            }
            Self::LocalStep {
                activation_id,
                step_key,
                ..
            } => {
                validate_text(activation_id, 128)?;
                validate_text(step_key, 128)?;
            }
            Self::Approval { key, .. } => validate_text(key, 128)?,
            Self::ApprovalPage { keys, .. } => {
                let mut unique = HashSet::new();
                if keys.len() > APPROVAL_PAGE_MAX_ITEMS as usize {
                    return Err(invalid("audit approval page is too large"));
                }
                for key in keys {
                    validate_text(key, 128)?;
                    if !unique.insert(key) {
                        return Err(invalid("duplicate audited approval key"));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditOperation {
    QueuePolicyAccepted {
        queue: String,
        request_id: String,
        revision: u64,
        policy_digest: ledgence_worker_api::Digest,
        reason: String,
    },
    QueueHoldReleased {
        queue: String,
        task_id: String,
        attempt_id: String,
        request_id: String,
        revision: u64,
        reason: String,
    },
    TaskAdmissionReleased {
        occurrence: TaskAdmissionOccurrence,
        source_id: String,
        request_id: String,
        revision: u64,
        reason: String,
    },
    ResourcePolicyAccepted {
        name: String,
        request_id: String,
        revision: u64,
        policy_digest: ledgence_worker_api::Digest,
        reason: String,
    },
    AdmissionReleased {
        occurrence: AdmissionOccurrence,
        source_id: String,
        request_id: String,
        revision: u64,
        reason: String,
    },
    EffectResolved {
        occurrence: EffectOccurrence,
        resolution_id: String,
        accepted_revision: u64,
        basis_revision: u64,
        #[serde(deserialize_with = "crate::observation::required_option")]
        supersedes_resolution_id: Option<String>,
        assessment_kind: EffectAssessmentKind,
    },
    ApprovalAccepted {
        workflow_id: String,
        key: String,
        activation_id: String,
        revision: u64,
        decision_id: String,
        decision: ApprovalDecision,
    },
    SensitiveResponse {
        read: AuditReadKind,
        resource: AuditResource,
    },
}
impl AuditOperation {
    pub fn target(&self) -> (AuditResourceKind, &str) {
        match self {
            Self::QueuePolicyAccepted { queue, .. } | Self::QueueHoldReleased { queue, .. } => {
                (AuditResourceKind::Queue, queue)
            }
            Self::ResourcePolicyAccepted { name, .. } => (AuditResourceKind::Resource, name),
            Self::TaskAdmissionReleased { occurrence, .. } => {
                (AuditResourceKind::Task, &occurrence.task_id)
            }
            Self::AdmissionReleased { occurrence, .. } => {
                (AuditResourceKind::Workflow, &occurrence.workflow_id)
            }
            Self::EffectResolved { occurrence, .. } => {
                (AuditResourceKind::Workflow, &occurrence.workflow_id)
            }
            Self::ApprovalAccepted { workflow_id, .. } => {
                (AuditResourceKind::Workflow, workflow_id)
            }
            Self::SensitiveResponse { resource, .. } => resource.target(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::QueuePolicyAccepted {
                queue,
                request_id,
                revision,
                policy_digest,
                reason,
            } => {
                validate_text(queue, 128)?;
                validate_text(request_id, 128)?;
                crate::admission::positive(*revision)?;
                policy_digest.validate()?;
                validate_text(reason, 1024)?;
            }
            Self::QueueHoldReleased {
                queue,
                task_id,
                attempt_id,
                request_id,
                revision,
                reason,
            } => {
                for id in [queue, task_id, attempt_id, request_id] {
                    validate_text(id, 128)?;
                }
                crate::admission::positive(*revision)?;
                validate_text(reason, 1024)?;
            }
            Self::TaskAdmissionReleased {
                occurrence,
                source_id,
                request_id,
                revision,
                reason,
            } => {
                occurrence.validate()?;
                validate_text(source_id, 128)?;
                validate_text(request_id, 128)?;
                crate::admission::positive(*revision)?;
                validate_text(reason, 1024)?;
            }
            Self::ResourcePolicyAccepted {
                name,
                request_id,
                revision,
                policy_digest,
                reason,
            } => {
                crate::admission::name(name)?;
                validate_text(request_id, 128)?;
                crate::admission::positive(*revision)?;
                policy_digest.validate()?;
                validate_text(reason, 1024)?;
            }
            Self::AdmissionReleased {
                occurrence,
                source_id,
                request_id,
                revision,
                reason,
            } => {
                occurrence.validate()?;
                validate_text(source_id, 128)?;
                validate_text(request_id, 128)?;
                crate::admission::positive(*revision)?;
                validate_text(reason, 1024)?;
            }
            Self::EffectResolved {
                occurrence,
                resolution_id,
                accepted_revision,
                basis_revision,
                supersedes_resolution_id,
                ..
            } => {
                occurrence.validate()?;
                validate_text(resolution_id, 128)?;
                if *basis_revision == 0 || basis_revision.checked_add(1) != Some(*accepted_revision)
                {
                    return Err(invalid("invalid assessment revision"));
                }
                if let Some(previous) = supersedes_resolution_id {
                    validate_text(previous, 128)?;
                    if previous == resolution_id {
                        return Err(invalid("self superseding assessment"));
                    }
                }
            }
            Self::ApprovalAccepted {
                workflow_id,
                key,
                activation_id,
                decision_id,
                ..
            } => {
                for id in [workflow_id, key, activation_id, decision_id] {
                    validate_text(id, 128)?;
                }
            }
            Self::SensitiveResponse { read, resource } => {
                resource.validate()?;
                if !read.matches(resource) {
                    return Err(invalid("audit read resource mismatch"));
                }
            }
        }
        Ok(())
    }
    pub fn accepted(command: &ApprovalDecisionCommand) -> Self {
        Self::ApprovalAccepted {
            workflow_id: command.workflow_id.clone(),
            key: command.key.clone(),
            activation_id: command.activation_id.clone(),
            revision: command.revision,
            decision_id: command.decision_id.clone(),
            decision: command.decision,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditEvent {
    pub event_id: String,
    pub recorded_at: Timestamp,
    pub audience: AccessAudience,
    pub actor: AuditActor,
    pub operation: AuditOperation,
}
impl AuditEvent {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.event_id, 128)?;
        crate::workflow_approvals::timestamp(self.recorded_at)?;
        self.audience.validate()?;
        self.actor.validate()?;
        self.operation.validate()?;
        let occurrence_scope = match &self.operation {
            AuditOperation::EffectResolved { occurrence, .. } => Some(&occurrence.scope),
            AuditOperation::TaskAdmissionReleased { occurrence, .. }
            | AuditOperation::SensitiveResponse {
                resource: AuditResource::TaskAdmission { occurrence },
                ..
            } => Some(&occurrence.scope),
            AuditOperation::AdmissionReleased { occurrence, .. }
            | AuditOperation::SensitiveResponse {
                resource: AuditResource::Admission { occurrence },
                ..
            } => Some(&occurrence.scope),
            _ => None,
        };
        if occurrence_scope.is_some_and(|scope| scope != &self.audience.scope) {
            return Err(invalid("assessment audit scope"));
        }
        bounded(self, AUDIT_EVENT_MAX_BYTES, "audit event")
    }
    pub fn require_acceptance(
        &self,
        principal: &VerifiedPrincipal,
        command: &ApprovalDecisionCommand,
        receipt: &ApprovalDecisionReceipt,
    ) -> Result<()> {
        self.validate()?;
        if &self.audience != principal.audience()
            || !self.actor.same_actor(principal)
            || self.operation != AuditOperation::accepted(command)
            || receipt
                .approval
                .decision
                .as_ref()
                .is_none_or(|d| d.decided_at != self.recorded_at)
        {
            return Err(inconsistent());
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub enum DecisionAttribution {
    LegacyUnverified,
    Verified { event: Box<AuditEvent> },
}
#[derive(Debug, Clone)]
pub struct AuditedDecision {
    pub receipt: ApprovalDecisionReceipt,
    pub attribution: DecisionAttribution,
}
impl AuditedDecision {
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &ApprovalDecisionCommand,
    ) -> Result<()> {
        self.receipt.validate().map_err(|_| inconsistent())?;
        if !self.receipt.matches(command).map_err(|_| inconsistent())? {
            return Err(inconsistent());
        }
        match &self.attribution {
            DecisionAttribution::LegacyUnverified if self.receipt.already_accepted => Ok(()),
            DecisionAttribution::Verified { event } => event
                .require_acceptance(principal, command, &self.receipt)
                .map_err(|_| inconsistent()),
            _ => Err(inconsistent()),
        }
    }
}
#[derive(Debug, Clone)]
pub struct AuditReceipt {
    pub event: AuditEvent,
    pub already_recorded: bool,
}
impl AuditReceipt {
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        observation: &SensitiveObservation,
    ) -> Result<()> {
        self.event.validate().map_err(|_| inconsistent())?;
        if self.event.event_id != observation.request.observation_id()
            || &self.event.audience != principal.audience()
            || self.event.actor != AuditActor::from_principal(principal)
            || self.event.operation
                != (AuditOperation::SensitiveResponse {
                    read: observation.read,
                    resource: observation.resource.clone(),
                })
        {
            return Err(inconsistent());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuditFilter {
    All,
    Event {
        event_id: String,
    },
    Resource {
        resource_kind: AuditResourceKind,
        resource_id: String,
    },
    ApprovalAcceptance {
        workflow_id: String,
        key: String,
        activation_id: String,
        revision: u64,
        decision_id: String,
    },
}
impl AuditFilter {
    pub fn matches(&self, event: &AuditEvent) -> bool {
        match self {
            Self::All => true,
            Self::Event { event_id } => &event.event_id == event_id,
            Self::Resource {
                resource_kind,
                resource_id,
            } => event.operation.target() == (*resource_kind, resource_id.as_str()),
            Self::ApprovalAcceptance {
                workflow_id,
                key,
                activation_id,
                revision,
                decision_id,
            } => {
                matches!(&event.operation,AuditOperation::ApprovalAccepted{workflow_id:w,key:k,activation_id:a,revision:r,decision_id:d,..} if (w,k,a,r,d)==(workflow_id,key,activation_id,revision,decision_id))
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditQuery {
    pub scope: Scope,
    pub filter: AuditFilter,
    pub cursor: Option<String>,
    pub limit: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditPosition {
    pub sequence: u64,
    pub event_id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    scope: Scope,
    filter: AuditFilter,
    position: AuditPosition,
}
impl AuditQuery {
    pub fn validate(&self) -> Result<Option<AuditPosition>> {
        self.scope.validate()?;
        if !(1..=AUDIT_MAX_LIMIT).contains(&self.limit) {
            return Err(invalid("audit limit must be 1 through 100"));
        }
        match &self.filter {
            AuditFilter::All => {}
            AuditFilter::Event { event_id } => {
                validate_text(event_id, 128)?;
                if self.limit != 2 || self.cursor.is_some() {
                    return Err(invalid("exact event lookup requires limit 2 and no cursor"));
                }
            }
            AuditFilter::Resource { resource_id, .. } => validate_text(resource_id, 128)?,
            AuditFilter::ApprovalAcceptance {
                workflow_id,
                key,
                activation_id,
                decision_id,
                ..
            } => {
                for id in [workflow_id, key, activation_id, decision_id] {
                    validate_text(id, 128)?;
                }
                if self.limit != 2 || self.cursor.is_some() {
                    return Err(invalid(
                        "exact acceptance lookup requires limit 2 and no cursor",
                    ));
                }
            }
        }
        let Some(text) = &self.cursor else {
            return Ok(None);
        };
        let bytes = crate::cursor::decode(text, AUDIT_CURSOR_MAX_BYTES)
            .ok_or_else(|| invalid("invalid audit cursor"))?;
        let cursor: Cursor = decode_unique_json(&bytes, AUDIT_CURSOR_MAX_BYTES / 2)
            .map_err(|_| invalid("invalid audit cursor"))?;
        if cursor.version != 1
            || cursor.scope != self.scope
            || cursor.filter != self.filter
            || cursor.position.sequence == 0
            || cursor.position.sequence > i64::MAX as u64
        {
            return Err(invalid("mismatched audit cursor"));
        }
        validate_text(&cursor.position.event_id, 128)?;
        Ok(Some(cursor.position))
    }
    pub fn next_cursor(&self, position: AuditPosition) -> Result<String> {
        let value = Cursor {
            version: 1,
            scope: self.scope.clone(),
            filter: self.filter.clone(),
            position,
        };
        let encoded = serde_json::to_vec(&value).map_err(|_| invalid("invalid audit cursor"))?;
        if encoded.len() > AUDIT_CURSOR_MAX_BYTES / 2 {
            return Err(invalid("audit cursor too large"));
        }
        let text = crate::cursor::encode(&encoded);
        let mut next = self.clone();
        next.cursor = Some(text.clone());
        next.validate()?;
        Ok(text)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditPage {
    pub items: Vec<AuditEvent>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
}
impl AuditPage {
    /// Remote clients validate scope, filter and consistent installation identity.
    /// Server adapters additionally compare against their authenticated audience.
    pub fn validate_for_query(&self, query: &AuditQuery) -> Result<()> {
        query.validate()?;
        match self.items.first() {
            Some(first) => self.validate_for(query, &first.audience),
            None if self.next_cursor.is_none() => Ok(()),
            None => Err(inconsistent()),
        }
    }
    pub fn validate_for(&self, query: &AuditQuery, audience: &AccessAudience) -> Result<()> {
        let prior = query.validate()?;
        if query.scope != audience.scope
            || self.items.len() > query.limit as usize
            || (matches!(
                query.filter,
                AuditFilter::ApprovalAcceptance { .. } | AuditFilter::Event { .. }
            ) && (self.items.len() > 1 || self.next_cursor.is_some()))
        {
            return Err(inconsistent());
        }
        let mut ids = HashSet::new();
        for event in &self.items {
            event.validate().map_err(|_| inconsistent())?;
            if &event.audience != audience
                || !query.filter.matches(event)
                || !ids.insert(&event.event_id)
                || prior.as_ref().is_some_and(|p| p.event_id == event.event_id)
            {
                return Err(inconsistent());
            }
        }
        if let Some(cursor) = &self.next_cursor {
            let mut next = query.clone();
            next.cursor = Some(cursor.clone());
            let position = next
                .validate()
                .map_err(|_| inconsistent())?
                .ok_or_else(inconsistent)?;
            if self.items.len() != query.limit as usize
                || self
                    .items
                    .last()
                    .is_none_or(|e| e.event_id != position.event_id)
                || prior.is_some_and(|p| position.sequence >= p.sequence)
            {
                return Err(inconsistent());
            }
        }
        bounded(self, AUDIT_PAGE_MAX_BYTES, "audit page").map_err(|_| inconsistent())
    }
}
fn bounded(value: &impl Serialize, maximum: usize, name: &str) -> Result<()> {
    Ok(crate::submission::check_encoded_size(value, maximum, name)?)
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn inconsistent() -> ContractError {
    ContractError::Unavailable("invalid operator audit response".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn actor(credential: &str, subject: &str) -> VerifiedPrincipal {
        let audience = AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
        };
        VerifiedPrincipal::new(
            PrincipalIdentity {
                issuer: audience.issuer(),
                subject: subject.into(),
            },
            audience,
            credential.into(),
            PrincipalClass::Operator,
            1000,
            vec![
                AccessCapability::MetadataRead,
                AccessCapability::SensitiveRead,
                AccessCapability::ApprovalDecide,
            ],
        )
        .unwrap()
    }
    fn event(id: &str) -> AuditEvent {
        let p = actor("key1", "alice");
        AuditEvent {
            event_id: id.into(),
            recorded_at: 0,
            audience: p.audience().clone(),
            actor: AuditActor::from_principal(&p),
            operation: AuditOperation::SensitiveResponse {
                read: AuditReadKind::TaskResult,
                resource: AuditResource::Task {
                    task_id: "task".into(),
                },
            },
        }
    }
    fn query() -> AuditQuery {
        AuditQuery {
            scope: actor("key", "alice").audience().scope.clone(),
            filter: AuditFilter::All,
            cursor: None,
            limit: 2,
        }
    }
    #[test]
    fn metadata_wire_rejects_payloads_workers_and_inconsistent_resource_kinds() {
        let e = event("obs");
        e.validate().unwrap();
        let mut wire = serde_json::to_value(&e).unwrap();
        wire["output"] = json!({"secret":"never"});
        assert!(serde_json::from_value::<AuditEvent>(wire).is_err());
        let mut changed = e.clone();
        changed.actor.class = PrincipalClass::Worker;
        assert!(changed.validate().is_err());
        changed = e.clone();
        changed.operation = AuditOperation::SensitiveResponse {
            read: AuditReadKind::TaskResult,
            resource: AuditResource::Workflow {
                workflow_id: "workflow".into(),
            },
        };
        assert!(changed.validate().is_err());
        changed = e;
        changed.recorded_at = u64::MAX;
        assert!(changed.validate().is_err());
    }
    #[test]
    fn page_cursor_binds_scope_filter_and_last_identity_without_timestamp_inference() {
        let q = query();
        let mut first = event("newer");
        first.recorded_at = 1;
        let mut second = event("older");
        second.recorded_at = 2;
        let cursor = q
            .next_cursor(AuditPosition {
                sequence: 20,
                event_id: second.event_id.clone(),
            })
            .unwrap();
        let page = AuditPage {
            items: vec![first, second],
            next_cursor: Some(cursor.clone()),
        };
        page.validate_for_query(&q).unwrap();
        let mut wrong = q.clone();
        wrong.scope.namespace = "elsewhere".into();
        wrong.cursor = Some(cursor.clone());
        assert!(wrong.validate().is_err());
        wrong = q.clone();
        wrong.filter = AuditFilter::Resource {
            resource_kind: AuditResourceKind::Task,
            resource_id: "task".into(),
        };
        wrong.cursor = Some(cursor);
        assert!(wrong.validate().is_err());
        let mut broken = page.clone();
        broken.items[1].event_id = "unexpected".into();
        assert!(broken.validate_for_query(&q).is_err());
        broken = page.clone();
        broken.items[1].audience.instance_id = "elsewhere".into();
        assert!(broken.validate_for_query(&q).is_err());
        broken = page;
        broken.items[1] = broken.items[0].clone();
        assert!(broken.validate_for_query(&q).is_err());
    }
    #[test]
    fn exact_acceptance_lookup_cannot_be_paginated_or_ambiguous() {
        let p = actor("key", "alice");
        let mut q = query();
        q.filter = AuditFilter::ApprovalAcceptance {
            workflow_id: "wf".into(),
            key: "approve".into(),
            activation_id: "activation".into(),
            revision: u64::MAX,
            decision_id: "decision".into(),
        };
        let mut e = event("accepted");
        e.operation = AuditOperation::ApprovalAccepted {
            workflow_id: "wf".into(),
            key: "approve".into(),
            activation_id: "activation".into(),
            revision: u64::MAX,
            decision_id: "decision".into(),
            decision: ApprovalDecision::Approve,
        };
        AuditPage {
            items: vec![e.clone()],
            next_cursor: None,
        }
        .validate_for(&q, p.audience())
        .unwrap();
        let mut second = e.clone();
        second.event_id = "duplicate".into();
        assert!(
            AuditPage {
                items: vec![e, second],
                next_cursor: None
            }
            .validate_for_query(&q)
            .is_err()
        );
        q.limit = 1;
        assert!(q.validate().is_err());
        q.limit = 2;
        q.cursor = Some("00".into());
        assert!(q.validate().is_err());
    }
    #[test]
    fn response_receipt_binds_credential_resource_and_server_observation_identity() {
        let p = actor("key1", "alice");
        let obs = SensitiveObservation {
            request: OperatorRequest::new("obs".into(), Some("caller-correlation".into())).unwrap(),
            read: AuditReadKind::TaskResult,
            resource: AuditResource::Task {
                task_id: "task".into(),
            },
        };
        let receipt = AuditReceipt {
            event: event("obs"),
            already_recorded: false,
        };
        receipt.validate_for(&p, &obs).unwrap();
        assert!(receipt.validate_for(&actor("key2", "alice"), &obs).is_err());
        assert!(receipt.validate_for(&actor("key1", "bob"), &obs).is_err());
        let mut wrong = obs;
        wrong.resource = AuditResource::Task {
            task_id: "other".into(),
        };
        assert!(receipt.validate_for(&p, &wrong).is_err());
        assert!(AuditActor::from_principal(&p).same_actor(&actor("key2", "alice")));
    }
}
