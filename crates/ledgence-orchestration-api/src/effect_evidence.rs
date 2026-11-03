//! Retained reports about approved effects. A report is not provider truth or execution authority.
use crate::*;
use serde_json::Value;

pub const EFFECT_EVIDENCE_COMMAND_MAX_BYTES: usize = WORKFLOW_LOCAL_RECORD_MAX_BYTES + 16 * 1024;
pub const EFFECT_EVIDENCE_PAGE_MAX_BYTES: usize = 1024 * 1024;
pub const EFFECT_EVIDENCE_CURSOR_MAX_BYTES: usize = 8192;

pub trait ApprovedEffectEvidence: Send + Sync {
    fn claim_v2<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt>;
    fn confirm_v2<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt>;
    fn observe<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt>;
}
pub trait EffectEvidenceClient: Send + Sync {
    fn claim_v2<'a>(
        &'a self,
        command: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt>;
    fn confirm_v2<'a>(
        &'a self,
        command: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt>;
    fn observe<'a>(
        &'a self,
        command: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt>;
}
pub trait EffectEvidenceReader: Send + Sync {
    fn effect_inbox<'a>(
        &'a self,
        query: &'a EffectInboxQuery,
    ) -> ContractFuture<'a, EffectInboxPage>;
    fn inspect_effect<'a>(
        &'a self,
        query: &'a EffectInspectQuery,
    ) -> ContractFuture<'a, EffectEvidenceDetail>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectOccurrence {
    pub scope: Scope,
    pub workflow_id: String,
    pub activation_id: String,
    pub approval_key: String,
}
impl EffectOccurrence {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for id in [&self.workflow_id, &self.activation_id, &self.approval_key] {
            validate_text(id, 128)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectWorkerIdentity {
    pub audience: AccessAudience,
    pub issuer: String,
    pub subject: String,
    pub credential_id: String,
}
impl EffectWorkerIdentity {
    pub fn from_principal(principal: &VerifiedPrincipal) -> Result<Self> {
        principal.require(AccessCapability::WorkerExecute)?;
        if principal.class() != PrincipalClass::Worker {
            return Err(ContractError::Forbidden);
        }
        let result = Self {
            audience: principal.audience().clone(),
            issuer: principal.identity().issuer.clone(),
            subject: principal.identity().subject.clone(),
            credential_id: principal.credential_id().into(),
        };
        result.validate()?;
        Ok(result)
    }
    pub fn validate(&self) -> Result<()> {
        self.audience.validate()?;
        PrincipalIdentity {
            issuer: self.issuer.clone(),
            subject: self.subject.clone(),
        }
        .validate()?;
        validate_text(&self.credential_id, 128)
    }
    pub fn same_worker(&self, other: &Self) -> bool {
        self.audience == other.audience
            && self.issuer == other.issuer
            && self.subject == other.subject
    }
    pub fn matches_principal(&self, principal: &VerifiedPrincipal) -> bool {
        Self::from_principal(principal).is_ok_and(|other| self.same_worker(&other))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectClaimV2Command {
    pub owner: LeaseOwner,
    pub workflow_id: String,
    pub approval_key: String,
    pub action: ApprovalAction,
    pub lookup: String,
    pub claim_id: String,
}
impl EffectClaimV2Command {
    pub fn legacy(&self) -> ApprovedEffectClaimCommand {
        ApprovedEffectClaimCommand {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: self.approval_key.clone(),
            action: self.action.clone(),
            lookup: self.lookup.clone(),
        }
    }
    pub fn occurrence(&self) -> EffectOccurrence {
        occurrence(&self.owner, &self.workflow_id, &self.approval_key)
    }
    pub fn validate(&self) -> Result<()> {
        self.legacy().validate()?;
        validate_text(&self.claim_id, 128)?;
        bounded(self, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectClaimBinding {
    pub scope: Scope,
    pub workflow_id: String,
    pub activation_id: String,
    pub attempt_id: String,
    pub approval_key: String,
    pub action: ApprovalAction,
    pub lookup: String,
    pub claim_id: String,
    pub worker: EffectWorkerIdentity,
    pub claimed_at: Timestamp,
    pub initial_permission: ApprovedEffectDisposition,
    pub recorded_revision: u64,
    pub already_accepted: bool,
}
impl EffectClaimBinding {
    pub fn occurrence(&self) -> EffectOccurrence {
        EffectOccurrence {
            scope: self.scope.clone(),
            workflow_id: self.workflow_id.clone(),
            activation_id: self.activation_id.clone(),
            approval_key: self.approval_key.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.worker.validate()?;
        validate_text(&self.claim_id, 128)?;
        time(self.claimed_at)?;
        positive(self.recorded_revision)?;
        ApprovedEffectClaimReceipt {
            scope: self.scope.clone(),
            workflow_id: self.workflow_id.clone(),
            activation_id: self.activation_id.clone(),
            attempt_id: self.attempt_id.clone(),
            approval_key: self.approval_key.clone(),
            action: self.action.clone(),
            lookup: self.lookup.clone(),
            disposition: self.initial_permission,
        }
        .validate()?;
        if self.worker.audience.scope != self.scope {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectClaimV2Receipt {
    Dispatch {
        claim: EffectClaimBinding,
    },
    LookupOnly {
        claim: EffectClaimBinding,
    },
    Replay {
        audience: AccessAudience,
        occurrence: EffectOccurrence,
        record: LocalStepRecord,
    },
}
impl EffectClaimV2Receipt {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Dispatch { claim } | Self::LookupOnly { claim } => {
                claim.validate()?;
                if matches!(self, Self::LookupOnly { .. })
                    && claim.initial_permission == ApprovedEffectDisposition::Dispatch
                    && !claim.already_accepted
                {
                    return Err(invalid());
                }
                if matches!(self, Self::Dispatch { .. })
                    && (claim.already_accepted
                        || claim.initial_permission != ApprovedEffectDisposition::Dispatch)
                {
                    return Err(invalid());
                }
            }
            Self::Replay {
                audience,
                occurrence,
                record,
            } => {
                audience.validate()?;
                occurrence.validate()?;
                record.validate()?;
                if audience.scope != occurrence.scope {
                    return Err(invalid());
                }
            }
        }
        bounded(self, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
    }
    pub fn matches(&self, command: &EffectClaimV2Command) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        match self {
            Self::Dispatch { claim } | Self::LookupOnly { claim } => Ok(claim.occurrence()
                == command.occurrence()
                && claim.attempt_id == command.owner.attempt_id
                && claim.claim_id == command.claim_id
                && claim.lookup == command.lookup
                && claim.action.matches(&command.action)?),
            Self::Replay {
                occurrence, record, ..
            } => Ok(*occurrence == command.occurrence()
                && record.key == approved_effect_step_key(&command.approval_key)
                && effect_record_matches(record, &command.action, &command.lookup)?),
        }
    }
    pub fn validate_worker(&self, worker: &EffectWorkerIdentity) -> Result<()> {
        self.validate()?;
        let matches = match self {
            Self::Dispatch { claim } | Self::LookupOnly { claim } => {
                claim.worker.same_worker(worker)
            }
            Self::Replay { audience, .. } => audience == &worker.audience,
        };
        if !matches {
            return Err(reply_error());
        }
        Ok(())
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &EffectClaimV2Command,
    ) -> Result<()> {
        if !self.matches(command).unwrap_or(false) {
            return Err(reply_error());
        }
        self.validate_worker(
            &EffectWorkerIdentity::from_principal(principal).map_err(|_| reply_error())?,
        )
        .map_err(|_| reply_error())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectReportedCallback {
    Dispatch,
    Lookup,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectConfirmV2Command {
    pub owner: LeaseOwner,
    pub workflow_id: String,
    pub approval_key: String,
    pub lookup: String,
    pub record: LocalStepRecord,
    pub claim_id: String,
    pub observation_id: String,
    pub reported_callback: EffectReportedCallback,
}
impl EffectConfirmV2Command {
    pub fn legacy(&self) -> ApprovedEffectConfirmCommand {
        ApprovedEffectConfirmCommand {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: self.approval_key.clone(),
            lookup: self.lookup.clone(),
            record: self.record.clone(),
        }
    }
    pub fn occurrence(&self) -> EffectOccurrence {
        occurrence(&self.owner, &self.workflow_id, &self.approval_key)
    }
    pub fn validate(&self) -> Result<()> {
        self.legacy().validate()?;
        validate_text(&self.claim_id, 128)?;
        validate_text(&self.observation_id, 128)?;
        bounded(self, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectConfirmV2Receipt {
    pub scope: Scope,
    pub workflow_id: String,
    pub activation_id: String,
    pub attempt_id: String,
    pub approval_key: String,
    pub key: String,
    pub already_accepted: bool,
    pub claim_id: String,
    pub observation_id: String,
    pub worker: EffectWorkerIdentity,
    pub recorded_at: Timestamp,
    pub recorded_revision: u64,
}
impl EffectConfirmV2Receipt {
    pub fn validate(&self) -> Result<()> {
        ApprovedEffectConfirmReceipt {
            scope: self.scope.clone(),
            workflow_id: self.workflow_id.clone(),
            activation_id: self.activation_id.clone(),
            attempt_id: self.attempt_id.clone(),
            approval_key: self.approval_key.clone(),
            key: self.key.clone(),
            already_accepted: self.already_accepted,
        }
        .validate()?;
        self.worker.validate()?;
        validate_text(&self.claim_id, 128)?;
        validate_text(&self.observation_id, 128)?;
        time(self.recorded_at)?;
        positive(self.recorded_revision)?;
        if self.worker.audience.scope != self.scope {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn matches(&self, command: &EffectConfirmV2Command) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.scope == command.owner.scope
            && self.workflow_id == command.workflow_id
            && self.activation_id == command.owner.task_id
            && self.attempt_id == command.owner.attempt_id
            && self.approval_key == command.approval_key
            && self.key == command.record.key
            && self.claim_id == command.claim_id
            && self.observation_id == command.observation_id)
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &EffectConfirmV2Command,
    ) -> Result<()> {
        if !self.matches(command).unwrap_or(false) || !self.worker.matches_principal(principal) {
            return Err(reply_error());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectObservationCommand {
    pub occurrence: EffectOccurrence,
    pub claim_id: String,
    pub observation_id: String,
    pub reported_callback: EffectReportedCallback,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub output: Value,
}
impl EffectObservationCommand {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        validate_text(&self.claim_id, 128)?;
        validate_text(&self.observation_id, 128)?;
        ledgence_worker_api::validate_wire_value(&self.output)?;
        bounded(&self.output, WORKFLOW_LOCAL_RECORD_MAX_BYTES)?;
        bounded(self, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectObservationDisposition {
    EvidenceOnly,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectObservationReceipt {
    pub occurrence: EffectOccurrence,
    pub claim_id: String,
    pub observation_id: String,
    pub worker: EffectWorkerIdentity,
    pub recorded_at: Timestamp,
    pub recorded_revision: u64,
    pub already_accepted: bool,
    pub disposition: EffectObservationDisposition,
}
impl EffectObservationReceipt {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        self.worker.validate()?;
        validate_text(&self.claim_id, 128)?;
        validate_text(&self.observation_id, 128)?;
        time(self.recorded_at)?;
        positive(self.recorded_revision)?;
        if self.worker.audience.scope != self.occurrence.scope {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn matches(&self, command: &EffectObservationCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.occurrence == command.occurrence
            && self.claim_id == command.claim_id
            && self.observation_id == command.observation_id)
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &EffectObservationCommand,
    ) -> Result<()> {
        if !self.matches(command).unwrap_or(false) || !self.worker.matches_principal(principal) {
            return Err(reply_error());
        }
        Ok(())
    }
}
fn occurrence(owner: &LeaseOwner, workflow_id: &str, key: &str) -> EffectOccurrence {
    EffectOccurrence {
        scope: owner.scope.clone(),
        workflow_id: workflow_id.into(),
        activation_id: owner.task_id.clone(),
        approval_key: key.into(),
    }
}
pub(crate) fn bounded(value: &impl Serialize, max: usize) -> Result<()> {
    Ok(crate::submission::check_encoded_size(
        value,
        max,
        "effect evidence",
    )?)
}
pub(crate) fn time(at: u64) -> Result<()> {
    crate::workflow_approvals::timestamp(at)
}
pub(crate) fn positive(value: u64) -> Result<()> {
    if value == 0 { Err(invalid()) } else { Ok(()) }
}
pub(crate) fn invalid() -> ContractError {
    ContractError::InvalidInput("invalid approved effect evidence".into())
}
fn reply_error() -> ContractError {
    ContractError::Unavailable("invalid approved effect evidence response".into())
}
/// Validate the unchanged v1 binding; claim provenance never enters journal bytes.
pub fn effect_record_matches(
    record: &LocalStepRecord,
    action: &ApprovalAction,
    lookup: &str,
) -> Result<bool> {
    record.validate()?;
    action.validate()?;
    let Some(input) = record.input.as_object() else {
        return Ok(false);
    };
    let Some(json) = input.get("action_json").and_then(Value::as_str) else {
        return Ok(false);
    };
    let recorded: ApprovalAction = decode_unique_json(json.as_bytes(), json.len())?;
    Ok(input.len() == 3
        && input.get("v").and_then(Value::as_u64) == Some(1)
        && input.get("lookup").and_then(Value::as_str) == Some(lookup)
        && record.callable == action.name
        && record.key.starts_with("_approval:")
        && action.matches(&recorded)?)
}

/// Stable v1 journal address, shared by all adapters and response validators.
pub fn approved_effect_step_key(approval_key: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("_approval:{:x}", Sha256::digest(approval_key.as_bytes()))
}

#[cfg(test)]
#[path = "effect_evidence_tests.rs"]
mod tests;
