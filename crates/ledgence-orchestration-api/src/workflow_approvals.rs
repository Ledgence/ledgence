//! Immutable workflow action proposals and exact, durable review decisions.
use crate::*;
use ledgence_worker_api::validate_wire_value;
use serde_json::Value;

pub const APPROVAL_ACTION_MAX_BYTES: usize = 32 * 1024;
pub const APPROVAL_PROPOSED_ARGUMENTS_MAX_BYTES: usize = 32 * 1024;
pub const APPROVAL_SNAPSHOT_MAX_BYTES: usize = 96 * 1024;
pub const APPROVAL_PAGE_MAX_ITEMS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalAction {
    pub name: String,
    pub version: String,
    pub arguments: Value,
}
impl ApprovalAction {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.name, 512)?;
        validate_text(&self.version, 128)?;
        arguments(&self.arguments, APPROVAL_ACTION_MAX_BYTES)?;
        bounded(self, APPROVAL_ACTION_MAX_BYTES, "approval action")
    }
    pub fn matches(&self, other: &Self) -> Result<bool> {
        self.validate()?;
        other.validate()?;
        Ok(canonical(self)? == canonical(other)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Rejected,
    Expired,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecisionRecord {
    pub decision_id: String,
    pub decision: ApprovalDecision,
    /// Caller supplied attribution, not an authenticated user identity.
    pub reviewer: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub reason: Option<String>,
    pub decided_at: Timestamp,
}
impl ApprovalDecisionRecord {
    pub fn validate(&self) -> Result<()> {
        decision_fields(&self.decision_id, &self.reviewer, self.reason.as_deref())?;
        timestamp(self.decided_at)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSnapshot {
    pub scope: Scope,
    pub workflow_id: String,
    pub key: String,
    pub activation_id: String,
    pub revision: u64,
    pub action: ApprovalAction,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub proposed_arguments: Option<Value>,
    pub created_at: Timestamp,
    pub deadline: Timestamp,
    pub status: ApprovalStatus,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub decision: Option<ApprovalDecisionRecord>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub resumed_activation_id: Option<String>,
}
impl ApprovalSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for id in [&self.workflow_id, &self.key, &self.activation_id] {
            validate_text(id, 128)?;
        }
        self.action.validate()?;
        if let Some(value) = &self.proposed_arguments {
            validate_approval_proposed_arguments(value)?;
        }
        timestamp(self.created_at)?;
        timestamp(self.deadline)?;
        if self.deadline < self.created_at
            || self.deadline - self.created_at > WORKFLOW_MAX_DELAY_MS
        {
            return Err(invalid("invalid approval deadline"));
        }
        if let Some(id) = &self.resumed_activation_id {
            validate_text(id, 128)?;
            if matches!(
                self.status,
                ApprovalStatus::Pending | ApprovalStatus::Cancelled
            ) || id == &self.activation_id
            {
                return Err(invalid(
                    "approval cannot resume with this status or activation",
                ));
            }
        }
        match (&self.decision, self.status) {
            (Some(record), ApprovalStatus::Approved | ApprovalStatus::Rejected) => {
                record.validate()?;
                if (record.decision == ApprovalDecision::Approve)
                    != (self.status == ApprovalStatus::Approved)
                    || record.decided_at < self.created_at
                    || record.decided_at >= self.deadline
                {
                    return Err(invalid("inconsistent approval decision"));
                }
            }
            (
                None,
                ApprovalStatus::Pending | ApprovalStatus::Expired | ApprovalStatus::Cancelled,
            ) => {}
            _ => return Err(invalid("inconsistent approval status")),
        }
        bounded(self, APPROVAL_SNAPSHOT_MAX_BYTES, "approval snapshot")
    }
    pub fn matches(&self, command: &ApprovalDecisionCommand) -> Result<bool> {
        Ok(self.scope == command.scope
            && self.workflow_id == command.workflow_id
            && self.key == command.key
            && self.activation_id == command.activation_id
            && self.revision == command.revision
            && self.action.matches(&command.action)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecisionCommand {
    pub scope: Scope,
    pub workflow_id: String,
    pub key: String,
    pub activation_id: String,
    pub revision: u64,
    pub action: ApprovalAction,
    pub decision_id: String,
    pub decision: ApprovalDecision,
    pub reviewer: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub reason: Option<String>,
}
impl ApprovalDecisionCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for id in [&self.workflow_id, &self.key, &self.activation_id] {
            validate_text(id, 128)?;
        }
        self.action.validate()?;
        decision_fields(&self.decision_id, &self.reviewer, self.reason.as_deref())?;
        bounded(
            self,
            APPROVAL_SNAPSHOT_MAX_BYTES,
            "approval decision command",
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalDecisionReceipt {
    pub approval: ApprovalSnapshot,
    pub already_accepted: bool,
}
impl ApprovalDecisionReceipt {
    pub fn validate(&self) -> Result<()> {
        self.approval.validate()?;
        if !matches!(
            self.approval.status,
            ApprovalStatus::Approved | ApprovalStatus::Rejected
        ) || self.approval.decision.is_none()
        {
            return Err(invalid("approval receipt requires an accepted decision"));
        }
        Ok(())
    }
    pub fn matches(&self, command: &ApprovalDecisionCommand) -> Result<bool> {
        Ok(self.approval.matches(command)?
            && self.approval.decision.as_ref().is_some_and(|record| {
                record.decision_id == command.decision_id
                    && record.decision == command.decision
                    && record.reviewer == command.reviewer
                    && record.reason == command.reason
            }))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalPage {
    pub items: Vec<ApprovalSnapshot>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
}
impl ApprovalPage {
    pub fn matches(
        &self,
        scope: &Scope,
        workflow_id: &str,
        after_key: Option<&str>,
        limit: u32,
    ) -> bool {
        self.items.len() <= limit as usize
            && self.items.iter().all(|item| {
                &item.scope == scope
                    && item.workflow_id == workflow_id
                    && after_key.is_none_or(|key| item.key.as_str() > key)
            })
            && (self.next_cursor.is_none()
                || (!self.items.is_empty() && self.items.len() == limit as usize))
    }
    pub fn validate(&self) -> Result<()> {
        if self.items.len() > APPROVAL_PAGE_MAX_ITEMS as usize {
            return Err(invalid("approval page exceeds limit"));
        }
        for item in &self.items {
            item.validate()?;
        }
        if self.items.windows(2).any(|pair| pair[0].key >= pair[1].key)
            || self
                .next_cursor
                .as_ref()
                .is_some_and(|key| self.items.last().is_none_or(|item| &item.key != key))
        {
            return Err(invalid("invalid approval page order or cursor"));
        }
        Ok(())
    }
}
pub fn validate_approval_proposed_arguments(value: &Value) -> Result<()> {
    arguments(value, APPROVAL_PROPOSED_ARGUMENTS_MAX_BYTES)
}
pub fn validate_approval_page(after_key: Option<&str>, limit: u32) -> Result<()> {
    if let Some(key) = after_key {
        validate_text(key, 128)?;
    }
    if !(1..=APPROVAL_PAGE_MAX_ITEMS).contains(&limit) {
        return Err(invalid("approval page limit must be 1..10"));
    }
    Ok(())
}
fn arguments(value: &Value, limit: usize) -> Result<()> {
    if !value.is_object() {
        return Err(invalid("approval arguments must be an object"));
    }
    validate_wire_value(value)?;
    bounded(value, limit, "approval arguments")
}
fn decision_fields(id: &str, reviewer: &str, reason: Option<&str>) -> Result<()> {
    validate_text(id, 128)?;
    validate_text(reviewer, 128)?;
    if reason.is_some_and(|reason| reason.len() > 4096) {
        return Err(invalid("approval reason exceeds 4096 bytes"));
    }
    Ok(())
}
fn timestamp(at: u64) -> Result<()> {
    if at > 253_402_300_799_999 {
        Err(invalid("approval timestamp exceeds supported range"))
    } else {
        Ok(())
    }
}
fn canonical(value: &impl Serialize) -> Result<Vec<u8>> {
    Ok(canonical_json_bytes(
        &serde_json::to_value(value).map_err(|_| invalid("invalid approval JSON"))?,
    )?)
}
fn bounded(value: &impl Serialize, max: usize, label: &str) -> Result<()> {
    crate::submission::check_encoded_size(value, max, label).map_err(Into::into)
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn action() -> ApprovalAction {
        ApprovalAction {
            name: "billing:send".into(),
            version: "v1".into(),
            arguments: json!({"amount":9007199254740993_u64,"count":1,"zero":-0.0,"note":"a\u{0000}b"}),
        }
    }
    fn snapshot() -> ApprovalSnapshot {
        ApprovalSnapshot {
            scope: Scope {
                tenant_id: "t".into(),
                namespace: "n".into(),
            },
            workflow_id: "wf_1".into(),
            key: "send".into(),
            activation_id: "task_1".into(),
            revision: u64::MAX - 1,
            action: action(),
            proposed_arguments: Some(json!({"count":3})),
            created_at: 1,
            deadline: 10,
            status: ApprovalStatus::Pending,
            decision: None,
            resumed_activation_id: None,
        }
    }
    #[test]
    fn action_binding_preserves_numeric_representation_and_original_proposal() {
        let a = action();
        let mut b = a.clone();
        assert!(a.matches(&b).unwrap());
        b.arguments["count"] = json!(1.0);
        assert!(!a.matches(&b).unwrap());
        b = a.clone();
        b.arguments["zero"] = json!(0.0);
        assert!(!a.matches(&b).unwrap());
        let bytes = canonical(&snapshot()).unwrap();
        let decoded: ApprovalSnapshot =
            decode_unique_json(&bytes, APPROVAL_SNAPSHOT_MAX_BYTES).unwrap();
        assert_eq!(
            decoded.action.arguments["amount"],
            json!(9007199254740993_u64)
        );
        assert_eq!(decoded.proposed_arguments, Some(json!({"count":3})));
    }
    #[test]
    fn approval_requires_bounded_objects_and_finite_deadline() {
        let mut a = action();
        a.arguments = Value::Null;
        assert!(a.validate().is_err());
        a = action();
        a.arguments = json!({"large":"x".repeat(APPROVAL_ACTION_MAX_BYTES)});
        assert!(a.validate().is_err());
        a = action();
        a.version = "".into();
        assert!(a.validate().is_err());
        let mut approval = snapshot();
        approval.deadline = 0;
        assert!(approval.validate().is_err());
        approval = snapshot();
        approval.deadline = approval.created_at + WORKFLOW_MAX_DELAY_MS + 1;
        assert!(approval.validate().is_err());
        let wait = WorkflowWait::Approval {
            key: "send".into(),
            action: action(),
            proposed_arguments: None,
            timeout_ms: 0,
        };
        assert_eq!(wait.deadline(123).unwrap(), Some(123));
        assert!(
            serde_json::to_value(wait)
                .unwrap()
                .get("proposed_arguments")
                .is_none()
        );
        assert!(serde_json::to_value(snapshot()).unwrap()["decision"].is_null());
    }
    #[test]
    fn decisions_require_timely_exact_status_and_receipts_require_decisions() {
        let mut approval = snapshot();
        assert!(
            ApprovalDecisionReceipt {
                approval: approval.clone(),
                already_accepted: false
            }
            .validate()
            .is_err()
        );
        approval.status = ApprovalStatus::Approved;
        approval.decision = Some(ApprovalDecisionRecord {
            decision_id: "decision_1".into(),
            decision: ApprovalDecision::Approve,
            reviewer: "operator".into(),
            reason: None,
            decided_at: 9,
        });
        approval.validate().unwrap();
        approval.decision.as_mut().unwrap().decided_at = 10;
        assert!(approval.validate().is_err());
        approval.decision.as_mut().unwrap().decided_at = 9;
        approval.status = ApprovalStatus::Rejected;
        assert!(approval.validate().is_err());
    }
    #[test]
    fn pages_validate_order_bounds_scope_and_requested_cursor() {
        let a = snapshot();
        let mut b = a.clone();
        b.key = "z".into();
        let page = ApprovalPage {
            items: vec![a.clone(), b],
            next_cursor: Some("z".into()),
        };
        page.validate().unwrap();
        assert!(page.matches(&a.scope, &a.workflow_id, None, 2));
        assert!(!page.matches(&a.scope, &a.workflow_id, Some("send"), 2));
        assert!(!page.matches(&a.scope, "other", None, 2));
        assert!(!page.matches(&a.scope, &a.workflow_id, None, 3));
        assert!(validate_approval_page(None, 0).is_err());
        assert!(validate_approval_page(None, 11).is_err());
    }
    #[test]
    fn nullable_observation_fields_are_required_and_original_arguments_are_objects() {
        for field in ["proposed_arguments", "decision", "resumed_activation_id"] {
            let mut value = serde_json::to_value(snapshot()).unwrap();
            value.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<ApprovalSnapshot>(value).is_err());
        }
        assert!(serde_json::from_value::<ApprovalPage>(json!({"items":[]})).is_err());
        let mut command = json!({"scope":{"tenant_id":"t","namespace":"n"},"workflow_id":"wf_1","key":"send","activation_id":"task_1","revision":0,"action":action(),"decision_id":"d","decision":"approve","reviewer":"operator","reason":null});
        assert!(serde_json::from_value::<ApprovalDecisionCommand>(command.clone()).is_ok());
        command.as_object_mut().unwrap().remove("reason");
        assert!(serde_json::from_value::<ApprovalDecisionCommand>(command).is_err());
        assert!(
            serde_json::from_value::<ApprovalDecisionRecord>(
                json!({"decision_id":"d","decision":"approve","reviewer":"operator","decided_at":1})
            )
            .is_err()
        );
        for proposal in [json!([]), json!(1), json!("raw")] {
            assert!(validate_approval_proposed_arguments(&proposal).is_err());
        }
    }
}
