//! Verified operator assessments and native, worker-free rendezvous. An assessment
//! is not provider truth, journal confirmation, or permission to dispatch again.
use crate::effect_evidence::{bounded, invalid, positive, time};
use crate::*;
use serde_json::Value;

pub const EFFECT_RESOLUTION_RESULT_MAX_BYTES: usize = 128 * 1024;
pub const EFFECT_RESOLUTION_COMMAND_MAX_BYTES: usize = 256 * 1024;
pub const EFFECT_RESOLUTION_ENTRY_MAX_BYTES: usize = 256 * 1024;
pub const EFFECT_RESOLUTION_RECEIPT_MAX_BYTES: usize = 16 * 1024;
pub const EFFECT_RESOLUTION_WAKE_MAX_BYTES: usize = 144 * 1024;
pub const EFFECT_RESOLUTION_REASON_MAX_BYTES: usize = 4096;
pub const EFFECT_RESOLUTION_MAX_REFERENCES: usize = 8;
pub const EFFECT_RESOLUTION_REFERENCE_MAX_BYTES: usize = 1024;
pub const EFFECT_RESOLUTION_SAVED_MAX_BYTES: usize = 1024 * 1024;
pub const EFFECT_RESOLUTION_SAVED_FORMAT: &str = "ledgence.effect-resolution.v1";

pub trait EffectResolution: Send + Sync {
    fn resolve_effect<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, AuditedEffectResolution>;
}
/// A complete capability includes the actual coordinator operation, not an
/// optimistic marker that an unrelated checkpoint adapter might implement.
pub trait EffectResolutionStore: EffectResolution {
    fn apply_effect_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        resolved_children: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress>;
}
pub trait EffectResolutionClient: Send + Sync {
    fn resolve_effect<'a>(
        &'a self,
        command: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, EffectResolutionReceipt>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectAssessmentKind {
    RecordSuccess,
    RecordFailure,
    CloseUnknown,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectResolutionAssessment {
    RecordSuccess {
        #[serde(deserialize_with = "crate::observation::required_value")]
        result: Value,
    },
    RecordFailure {
        failure: ApplicationError,
    },
    CloseUnknown,
}
impl EffectResolutionAssessment {
    pub fn kind(&self) -> EffectAssessmentKind {
        match self {
            Self::RecordSuccess { .. } => EffectAssessmentKind::RecordSuccess,
            Self::RecordFailure { .. } => EffectAssessmentKind::RecordFailure,
            Self::CloseUnknown => EffectAssessmentKind::CloseUnknown,
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::RecordSuccess { result } => {
                ledgence_worker_api::validate_wire_value(result)?;
                bounded(result, EFFECT_RESOLUTION_RESULT_MAX_BYTES)
            }
            Self::RecordFailure { failure } => validate_workflow_error(failure),
            Self::CloseUnknown => Ok(()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionCommand {
    pub occurrence: EffectOccurrence,
    pub resolution_id: String,
    pub expected_effect_revision: u64,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub supersedes_resolution_id: Option<String>,
    pub action: ApprovalAction,
    pub assessment: EffectResolutionAssessment,
    pub reason: String,
    pub evidence_references: Vec<String>,
}
impl EffectResolutionCommand {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        resolution_fields(
            &self.resolution_id,
            self.expected_effect_revision,
            &self.supersedes_resolution_id,
        )?;
        self.expected_effect_revision
            .checked_add(1)
            .ok_or_else(invalid)?;
        self.action.validate()?;
        assessment_fields(&self.assessment, &self.reason, &self.evidence_references)?;
        bounded(self, EFFECT_RESOLUTION_COMMAND_MAX_BYTES)
    }
}
pub(crate) fn assessment_fields(
    assessment: &EffectResolutionAssessment,
    reason: &str,
    references: &[String],
) -> Result<()> {
    assessment.validate()?;
    if reason.trim().is_empty()
        || reason.len() > EFFECT_RESOLUTION_REASON_MAX_BYTES
        || references.len() > EFFECT_RESOLUTION_MAX_REFERENCES
        || (references.is_empty() && assessment.kind() != EffectAssessmentKind::CloseUnknown)
    {
        return Err(invalid());
    }
    for reference in references {
        validate_text(reference, EFFECT_RESOLUTION_REFERENCE_MAX_BYTES)?;
    }
    Ok(())
}
fn resolution_fields(id: &str, basis: u64, previous: &Option<String>) -> Result<()> {
    validate_text(id, 128)?;
    positive(basis)?;
    if let Some(previous) = previous {
        validate_text(previous, 128)?;
        if previous == id {
            return Err(invalid());
        }
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionReceipt {
    pub occurrence: EffectOccurrence,
    pub resolution_id: String,
    pub accepted_at: Timestamp,
    pub accepted_revision: u64,
    pub basis_revision: u64,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub supersedes_resolution_id: Option<String>,
    pub assessment_kind: EffectAssessmentKind,
    pub acceptance_event_id: String,
    pub already_accepted: bool,
}
impl EffectResolutionReceipt {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        resolution_fields(
            &self.resolution_id,
            self.basis_revision,
            &self.supersedes_resolution_id,
        )?;
        if self.basis_revision.checked_add(1) != Some(self.accepted_revision) {
            return Err(invalid());
        }
        time(self.accepted_at)?;
        validate_text(&self.acceptance_event_id, 128)?;
        bounded(self, EFFECT_RESOLUTION_RECEIPT_MAX_BYTES)
    }
    pub fn matches(&self, command: &EffectResolutionCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.occurrence == command.occurrence
            && self.resolution_id == command.resolution_id
            && self.basis_revision == command.expected_effect_revision
            && self.supersedes_resolution_id == command.supersedes_resolution_id
            && self.assessment_kind == command.assessment.kind())
    }
    pub fn audit_operation(&self) -> AuditOperation {
        AuditOperation::EffectResolved {
            occurrence: self.occurrence.clone(),
            resolution_id: self.resolution_id.clone(),
            accepted_revision: self.accepted_revision,
            basis_revision: self.basis_revision,
            supersedes_resolution_id: self.supersedes_resolution_id.clone(),
            assessment_kind: self.assessment_kind,
        }
    }
}
/// Trusted acceptance proof; it cannot be supplied as a deserialized command.
#[derive(Debug, Clone)]
pub struct AuditedEffectResolution {
    pub receipt: EffectResolutionReceipt,
    pub acceptance_event: AuditEvent,
}
impl AuditedEffectResolution {
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &EffectResolutionCommand,
    ) -> Result<()> {
        let valid = || -> Result<bool> {
            self.acceptance_event.validate()?;
            Ok(self.receipt.matches(command)?
                && self.acceptance_event.audience == *principal.audience()
                && self.acceptance_event.actor.same_actor(principal)
                && self.acceptance_event.event_id == self.receipt.acceptance_event_id
                && self.acceptance_event.recorded_at == self.receipt.accepted_at
                && self.acceptance_event.operation == self.receipt.audit_operation())
        };
        if valid() != Ok(true) {
            return Err(ContractError::Unavailable(
                "invalid effect resolution acceptance proof".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionReference {
    pub resolution_id: String,
    pub sequence: u64,
    pub accepted_at: Timestamp,
    pub accepted_revision: u64,
    pub basis_revision: u64,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub supersedes_resolution_id: Option<String>,
    pub assessment_kind: EffectAssessmentKind,
    pub acceptance_event_id: String,
}
impl EffectResolutionReference {
    pub fn validate(&self) -> Result<()> {
        resolution_fields(
            &self.resolution_id,
            self.basis_revision,
            &self.supersedes_resolution_id,
        )?;
        positive(self.sequence)?;
        time(self.accepted_at)?;
        validate_text(&self.acceptance_event_id, 128)?;
        if self.basis_revision.checked_add(1) != Some(self.accepted_revision)
            || self.sequence >= self.accepted_revision
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionSummary {
    pub reference: EffectResolutionReference,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub first_conflicting_entry_sequence: Option<u64>,
}
impl EffectResolutionSummary {
    pub fn validate(&self, entries: u64, revision: u64) -> Result<()> {
        self.reference.validate()?;
        if self.reference.sequence > entries || self.reference.accepted_revision > revision {
            return Err(invalid());
        }
        if let Some(sequence) = self.first_conflicting_entry_sequence
            && (sequence <= self.reference.sequence
                || sequence > entries
                || !matches!(
                    self.reference.assessment_kind,
                    EffectAssessmentKind::RecordSuccess | EffectAssessmentKind::RecordFailure
                ))
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectResolutionOutcomeKind {
    RecordedSuccess,
    RecordedFailure,
    ClosedUnknown,
    Disputed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectResolutionOutcome {
    RecordedSuccess {
        #[serde(deserialize_with = "crate::observation::required_value")]
        result: Value,
    },
    RecordedFailure {
        failure: ApplicationError,
    },
    ClosedUnknown,
    Disputed {
        first_conflicting_entry_sequence: u64,
    },
}
impl EffectResolutionOutcome {
    pub fn kind(&self) -> EffectResolutionOutcomeKind {
        match self {
            Self::RecordedSuccess { .. } => EffectResolutionOutcomeKind::RecordedSuccess,
            Self::RecordedFailure { .. } => EffectResolutionOutcomeKind::RecordedFailure,
            Self::ClosedUnknown => EffectResolutionOutcomeKind::ClosedUnknown,
            Self::Disputed { .. } => EffectResolutionOutcomeKind::Disputed,
        }
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::RecordedSuccess { result } => {
                ledgence_worker_api::validate_wire_value(result)?;
                bounded(result, EFFECT_RESOLUTION_RESULT_MAX_BYTES)
            }
            Self::RecordedFailure { failure } => validate_workflow_error(failure),
            Self::ClosedUnknown => Ok(()),
            Self::Disputed {
                first_conflicting_entry_sequence,
            } => positive(*first_conflicting_entry_sequence),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResolutionWake {
    pub key: String,
    pub occurrence: EffectOccurrence,
    pub parking_activation_id: String,
    pub parking_revision: u64,
    pub resumed_activation_id: String,
    pub resumed_revision: u64,
    pub resolution_id: String,
    pub accepted_at: Timestamp,
    pub accepted_revision: u64,
    pub basis_revision: u64,
    pub selected_at: Timestamp,
    pub selected_effect_revision: u64,
    pub outcome: EffectResolutionOutcome,
}
impl EffectResolutionWake {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        for id in [
            &self.key,
            &self.parking_activation_id,
            &self.resumed_activation_id,
            &self.resolution_id,
        ] {
            validate_text(id, 128)?;
        }
        positive(self.basis_revision)?;
        time(self.accepted_at)?;
        time(self.selected_at)?;
        self.outcome.validate()?;
        if self.parking_revision.checked_add(1) != Some(self.resumed_revision)
            || self.basis_revision.checked_add(1) != Some(self.accepted_revision)
            || self.accepted_at > self.selected_at
            || self.accepted_revision > self.selected_effect_revision
            || self.parking_activation_id == self.resumed_activation_id
        {
            return Err(invalid());
        }
        if let EffectResolutionOutcome::Disputed {
            first_conflicting_entry_sequence,
        } = self.outcome
            && first_conflicting_entry_sequence >= self.selected_effect_revision
        {
            return Err(invalid());
        }
        bounded(self, EFFECT_RESOLUTION_WAKE_MAX_BYTES)
    }
}
/// Omitted optional projections support stored A bytes; explicit null is invalid.
pub(crate) fn present<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn command() -> EffectResolutionCommand {
        serde_json::from_value(json!({"occurrence":{"scope":{"tenant_id":"t","namespace":"n"},"workflow_id":"wf","activation_id":"effect","approval_key":"approve"},"resolution_id":"resolution","expected_effect_revision":9007199254740993_u64,"supersedes_resolution_id":null,"action":{"name":"app:send","version":"1","arguments":{}},"assessment":{"kind":"record_success","result":null},"reason":"Checked receipt","evidence_references":["receipt:r1"]})).unwrap()
    }
    #[test]
    fn resolution_command_preserves_full_width_and_requires_nullable_predecessor() {
        let c = command();
        c.validate().unwrap();
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["expected_effect_revision"], json!(9007199254740993_u64));
        let mut missing = v.clone();
        missing
            .as_object_mut()
            .unwrap()
            .remove("supersedes_resolution_id");
        assert!(serde_json::from_value::<EffectResolutionCommand>(missing).is_err());
        let mut missing = v.clone();
        missing["assessment"]
            .as_object_mut()
            .unwrap()
            .remove("result");
        assert!(serde_json::from_value::<EffectResolutionCommand>(missing).is_err());
        for edit in ["reason", "evidence_references"] {
            let mut bad = c.clone();
            if edit == "reason" {
                bad.reason = "  ".into();
            } else {
                bad.evidence_references.clear();
            }
            assert!(bad.validate().is_err());
        }
        let mut last = c.clone();
        last.expected_effect_revision = u64::MAX;
        assert!(last.validate().is_err());
    }
    #[test]
    fn exact_event_query_requires_bounded_identity_and_no_pagination() {
        let mut query = AuditQuery {
            scope: command().occurrence.scope,
            filter: AuditFilter::Event {
                event_id: "event".into(),
            },
            cursor: None,
            limit: 2,
        };
        query.validate().unwrap();
        query.limit = 1;
        assert!(query.validate().is_err());
        query.limit = 2;
        query.cursor = Some("cursor".into());
        assert!(query.validate().is_err());
    }
    #[test]
    fn native_wait_and_wake_bind_origin_parking_and_actual_resume() {
        let c = command();
        let wait = WorkflowWait::EffectResolution {
            key: "review".into(),
            occurrence: c.occurrence.clone(),
            after_resolution_id: None,
        };
        wait.validate().unwrap();
        assert_eq!(wait.deadline(42).unwrap(), None);
        let wake = EffectResolutionWake {
            key: "review".into(),
            occurrence: c.occurrence,
            parking_activation_id: "park".into(),
            parking_revision: 7,
            resumed_activation_id: "resume".into(),
            resumed_revision: 8,
            resolution_id: "resolution".into(),
            accepted_at: 41,
            accepted_revision: 3,
            basis_revision: 2,
            selected_at: 42,
            selected_effect_revision: 3,
            outcome: EffectResolutionOutcome::ClosedUnknown,
        };
        let wire =
            serde_json::to_value(WorkflowWake::EffectResolution(Box::new(wake.clone()))).unwrap();
        assert_eq!(wire["kind"], "effect_resolution");
        assert_eq!(wire["parking_activation_id"], "park");
        let mut context = WorkflowActivationContext {
            admission_result: None,
            provider_admission: None,
            parent_workflow_id: None,
            root_workflow_id: None,
            v: 1,
            workflow_id: "wf".into(),
            activation_id: "resume".into(),
            revision: 8,
            continuation: "after".into(),
            state: Value::Null,
            inputs: Default::default(),
            local_steps: vec![],
            wake: Some(serde_json::from_value(wire).unwrap()),
        };
        context.validate().unwrap();
        context.activation_id = "another".into();
        assert!(context.validate().is_err());
        let mut bad = wake;
        bad.parking_revision = u64::MAX;
        assert!(bad.validate().is_err());
    }
}
