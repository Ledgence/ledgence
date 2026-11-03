//! A single dispatch grant and durable confirmation of the approved external effect.
use crate::*;

/// Atomic store or service operations for a protected external write. An
/// adapter must support both operations before it may grant a first dispatch.
pub trait ApprovedEffects: Send + Sync {
    /// Only the call that commits the first grant may dispatch. Every later
    /// claim with live authority is lookup-only, even if the reply was lost.
    fn claim_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectClaimCommand,
    ) -> ContractFuture<'a, ApprovedEffectClaimReceipt>;

    /// Record the journal result and confirmation evidence atomically, or
    /// acknowledge the exact previously accepted confirmation.
    fn confirm_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectConfirmCommand,
    ) -> ContractFuture<'a, ApprovedEffectConfirmReceipt>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectClaimRequest {
    pub approval_key: String,
    pub action: ApprovalAction,
    pub lookup: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectClaimCommand {
    pub owner: LeaseOwner,
    pub workflow_id: String,
    pub approval_key: String,
    pub action: ApprovalAction,
    pub lookup: String,
}

impl ApprovedEffectClaimCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.approval_key, 128)?;
        self.action.validate()?;
        validate_lookup(&self.lookup, &self.action.name)
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
    pub lookup: String,
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
        self.action.validate()?;
        validate_lookup(&self.lookup, &self.action.name)
    }

    pub fn matches(&self, command: &ApprovedEffectClaimCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.scope == command.owner.scope
            && self.workflow_id == command.workflow_id
            && self.activation_id == command.owner.task_id
            && self.attempt_id == command.owner.attempt_id
            && self.approval_key == command.approval_key
            && self.lookup == command.lookup
            && self.action.matches(&command.action)?)
    }
}

/// Maximum confirmation body, including a 128 KiB record and its worker fence.
pub const APPROVED_EFFECT_CONFIRM_MAX_BYTES: usize = WORKFLOW_LOCAL_RECORD_MAX_BYTES + 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectConfirmRequest {
    pub approval_key: String,
    pub lookup: String,
    pub record: LocalStepRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectConfirmCommand {
    pub owner: LeaseOwner,
    pub workflow_id: String,
    pub approval_key: String,
    pub lookup: String,
    pub record: LocalStepRecord,
}

impl ApprovedEffectConfirmCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.approval_key, 128)?;
        validate_lookup(&self.lookup, &self.record.callable)?;
        validate_effect_step_key(&self.record.key)?;
        self.record.validate()
    }
}

/// A successful confirmation records the exact journal entry and evidence together.
/// An exact replay can observe acceptance without creating another journal entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEffectConfirmReceipt {
    pub scope: Scope,
    pub workflow_id: String,
    pub activation_id: String,
    pub attempt_id: String,
    pub approval_key: String,
    pub key: String,
    pub already_accepted: bool,
}

impl ApprovedEffectConfirmReceipt {
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
        validate_effect_step_key(&self.key)
    }

    pub fn matches(&self, command: &ApprovedEffectConfirmCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.scope == command.owner.scope
            && self.workflow_id == command.workflow_id
            && self.activation_id == command.owner.task_id
            && self.attempt_id == command.owner.attempt_id
            && self.approval_key == command.approval_key
            && self.key == command.record.key)
    }
}

fn validate_lookup(lookup: &str, dispatch: &str) -> Result<()> {
    validate_text(lookup, 512)?;
    if lookup == dispatch {
        return Err(ContractError::InvalidInput(
            "approved effect lookup must differ from dispatch".into(),
        ));
    }
    Ok(())
}

fn validate_effect_step_key(key: &str) -> Result<()> {
    // The store also checks the digest against the authoritative approval key.
    let valid = key.strip_prefix("_approval:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if !valid {
        return Err(ContractError::InvalidInput(
            "invalid approved effect journal key".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request() -> serde_json::Value {
        json!({
            "approval_key": "payment",
            "lookup": "payments:inspect",
            "record": {
                "key": format!("_approval:{}", "a".repeat(64)),
                "callable": "payments:send",
                "input": {"v": 1, "lookup": "payments:inspect", "action_json": "{}"},
                "output": null
            }
        })
    }

    fn command() -> ApprovedEffectConfirmCommand {
        let request: ApprovedEffectConfirmRequest = serde_json::from_value(request()).unwrap();
        ApprovedEffectConfirmCommand {
            owner: LeaseOwner {
                scope: Scope {
                    tenant_id: "tenant".into(),
                    namespace: "ns".into(),
                },
                task_id: "activation".into(),
                attempt_id: "attempt".into(),
                lease_id: "lease".into(),
                generation: 1,
                worker_session_id: "worker".into(),
                consumer_id: 0,
            },
            workflow_id: "workflow".into(),
            approval_key: request.approval_key,
            lookup: request.lookup,
            record: request.record,
        }
    }

    #[test]
    fn confirmation_requires_worker_authority_and_a_reserved_step_key() {
        for field in ["owner", "workflow_id", "already_accepted"] {
            let mut payload = request();
            payload[field] = json!("untrusted");
            assert!(serde_json::from_value::<ApprovedEffectConfirmRequest>(payload).is_err());
        }
        for key in [
            "ordinary".to_string(),
            format!("_approval:{}", "A".repeat(64)),
            format!("_approval:{}", "0".repeat(63)),
        ] {
            let mut invalid = command();
            invalid.record.key = key;
            assert!(invalid.validate().is_err());
        }
        let mut invalid = command();
        invalid.lookup = invalid.record.callable.clone();
        assert!(invalid.validate().is_err());
        invalid.lookup = "x".repeat(513);
        assert!(invalid.validate().is_err());
        let mut invalid = command();
        invalid.owner.generation = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn confirmation_receipt_identifies_current_scope_owner_and_protected_occurrence() {
        let command = command();
        let receipt = ApprovedEffectConfirmReceipt {
            scope: command.owner.scope.clone(),
            workflow_id: command.workflow_id.clone(),
            activation_id: command.owner.task_id.clone(),
            attempt_id: command.owner.attempt_id.clone(),
            approval_key: command.approval_key.clone(),
            key: command.record.key.clone(),
            already_accepted: true,
        };
        assert!(receipt.matches(&command).unwrap());
        for field in [
            "scope",
            "workflow_id",
            "activation_id",
            "attempt_id",
            "approval_key",
            "key",
        ] {
            let mut value = serde_json::to_value(&receipt).unwrap();
            match field {
                "scope" => value[field]["tenant_id"] = json!("different"),
                "key" => value[field] = json!(format!("_approval:{}", "0".repeat(64))),
                _ => value[field] = json!("different"),
            }
            let foreign: ApprovedEffectConfirmReceipt = serde_json::from_value(value).unwrap();
            assert!(!foreign.matches(&command).unwrap(), "{field}");
        }
    }
}
