//! One-use dispatch permission for the activation resumed by an approval.
use crate::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectClaimRequest {
    pub approval_key: String,
    pub action: ApprovalAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectClaimCommand {
    pub owner: LeaseOwner,
    pub workflow_id: String,
    pub approval_key: String,
    pub action: ApprovalAction,
}

impl ApprovedEffectClaimCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.approval_key, 128)?;
        self.action.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovedEffectDisposition {
    /// Only the call which committed the first grant may send.
    Dispatch,
    /// A previous call may already have sent, even if its worker never received a reply.
    LookupOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectClaimReceipt {
    pub scope: Scope,
    pub workflow_id: String,
    pub activation_id: String,
    /// The attempt receiving this reply. A repeat from that attempt is lookup-only.
    pub attempt_id: String,
    pub approval_key: String,
    pub action: ApprovalAction,
    pub disposition: ApprovedEffectDisposition,
}

impl ApprovedEffectClaimReceipt {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for id in [
            &self.workflow_id,
            &self.activation_id,
            &self.attempt_id,
            &self.approval_key,
        ] {
            validate_text(id, 128)?;
        }
        self.action.validate()
    }

    pub fn matches(&self, command: &ApprovedEffectClaimCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.scope == command.owner.scope
            && self.workflow_id == command.workflow_id
            && self.activation_id == command.owner.task_id
            && self.attempt_id == command.owner.attempt_id
            && self.approval_key == command.approval_key
            && self.action.matches(&command.action)?)
    }
}
