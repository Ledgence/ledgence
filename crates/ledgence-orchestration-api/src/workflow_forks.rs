//! Durable, non-blocking branches of the current pinned workflow definition.
use crate::*;
use ledgence_worker_api::validate_wire_value;
use serde_json::Value;
use std::collections::BTreeSet;

pub const WORKFLOW_FORK_MAX_BYTES: usize = 128 * 1024;
pub const WORKFLOW_FORK_COMMAND_MAX_BYTES: usize = 144 * 1024;
pub const WORKFLOW_MAX_FORKS: usize = 64;
pub const WORKFLOW_FORK_LEDGER_MAX_BYTES: usize = 256 * 1024;

/// One owned workflow starting at a named entrypoint in its parent's exact
/// controller package. No program lookup or version selection is performed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowBranch {
    pub key: String,
    pub entrypoint: String,
    pub queue: String,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub data: Value,
    #[serde(default)]
    pub retry_policy: RetryPolicy,
    #[serde(default = "attempt_timeout")]
    pub attempt_timeout_ms: u64,
}
const fn attempt_timeout() -> u64 {
    300_000
}
impl WorkflowBranch {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        validate_text(&self.entrypoint, 128)?;
        validate_text(&self.queue, 128)?;
        validate_wire_value(&self.data)?;
        self.retry_policy.validate()?;
        if !(60_000..=86_400_000).contains(&self.attempt_timeout_ms) {
            return Err(ContractError::InvalidInput(
                "attempt_timeout_ms must be between 60000 and 86400000".into(),
            ));
        }
        Ok(())
    }
}

/// Fork and child keys are parent-workflow scoped. Membership, order and every
/// branch binding are immutable after acceptance; object field order is ignored
/// while integer/floating numeric representations remain distinct.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowForkRequest {
    pub key: String,
    pub branches: Vec<WorkflowBranch>,
}
impl WorkflowForkRequest {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        if self.branches.is_empty() || self.branches.len() > WORKFLOW_MAX_COMMANDS {
            return Err(ContractError::InvalidInput(
                "workflow fork must contain 1 to 64 branches".into(),
            ));
        }
        let mut keys = BTreeSet::new();
        for branch in &self.branches {
            branch.validate()?;
            if !keys.insert(&branch.key) {
                return Err(ContractError::InvalidInput(
                    "duplicate fork branch key".into(),
                ));
            }
        }
        crate::submission::check_encoded_size(self, WORKFLOW_FORK_MAX_BYTES, "workflow fork")?;
        Ok(())
    }
    pub fn matches(&self, other: &Self) -> Result<bool> {
        self.validate()?;
        other.validate()?;
        let canonical = |value: &Self| -> Result<Vec<u8>> {
            Ok(canonical_json_bytes(
                &serde_json::to_value(value).map_err(|error| {
                    ContractError::InvalidInput(format!("invalid workflow fork: {error}"))
                })?,
            )?)
        };
        Ok(canonical(self)? == canonical(other)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowForkCommand {
    pub owner: LeaseOwner,
    pub fork: WorkflowForkRequest,
    /// Causality for newly created branches; excluded from replay identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_trace: Option<TraceContext>,
}
impl WorkflowForkCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.scope.validate()?;
        if self.owner.generation == 0 {
            return Err(ContractError::InvalidInput(
                "lease generation must be positive".into(),
            ));
        }
        for id in [
            &self.owner.task_id,
            &self.owner.attempt_id,
            &self.owner.lease_id,
            &self.owner.worker_session_id,
        ] {
            validate_text(id, 128)?;
        }
        self.fork.validate()?;
        if let Some(trace) = &self.processing_trace {
            trace.validate()?;
        }
        crate::submission::check_encoded_size(
            self,
            WORKFLOW_FORK_COMMAND_MAX_BYTES,
            "workflow fork command",
        )?;
        Ok(())
    }
}

/// Acknowledges atomically committed owned branches and dispatch obligations.
/// Acceptance does not checkpoint, suspend or advance the parent activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowForkReceipt {
    pub key: String,
    pub branch_keys: Vec<String>,
    pub already_accepted: bool,
}
impl WorkflowForkReceipt {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        if self.branch_keys.is_empty() || self.branch_keys.len() > WORKFLOW_MAX_COMMANDS {
            return Err(ContractError::InvalidInput(
                "invalid fork receipt membership".into(),
            ));
        }
        let mut keys = BTreeSet::new();
        for key in &self.branch_keys {
            validate_text(key, 128)?;
            if !keys.insert(key) {
                return Err(ContractError::InvalidInput(
                    "duplicate fork receipt key".into(),
                ));
            }
        }
        Ok(())
    }
    pub fn matches(&self, command: &WorkflowForkCommand) -> bool {
        self.key == command.fork.key
            && self
                .branch_keys
                .iter()
                .eq(command.fork.branches.iter().map(|branch| &branch.key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request() -> Value {
        json!({"key":"fanout","branches":[{"key":"a","entrypoint":"calculate","queue":"python","data":{"x":1,"y":2}}]})
    }
    #[test]
    fn fork_shape_limits_and_replay_binding_are_strict() {
        let fork: WorkflowForkRequest = serde_json::from_value(request()).unwrap();
        fork.validate().unwrap();
        assert_eq!(fork.branches[0].attempt_timeout_ms, 300_000);
        let mut replay = fork.clone();
        replay.branches[0].data = json!({"y":2,"x":1});
        assert!(fork.matches(&replay).unwrap());
        replay.branches[0].data = json!({"y":2,"x":1.0});
        assert!(!fork.matches(&replay).unwrap());
        replay = fork.clone();
        replay.branches[0].entrypoint = "other".into();
        assert!(!fork.matches(&replay).unwrap());
        replay.branches.push(replay.branches[0].clone());
        assert!(replay.validate().is_err());
        replay.branches.clear();
        assert!(replay.validate().is_err());
        replay = fork.clone();
        replay.branches[0].data = json!("x".repeat(WORKFLOW_FORK_MAX_BYTES));
        assert!(replay.validate().is_err());
        let mut missing = request();
        missing["branches"][0]
            .as_object_mut()
            .unwrap()
            .remove("data");
        assert!(serde_json::from_value::<WorkflowForkRequest>(missing).is_err());
        let mut extra = request();
        extra["branches"][0]["program"] = json!("other");
        assert!(serde_json::from_value::<WorkflowForkRequest>(extra).is_err());
    }

    // Older adapters implement the existing ports without overriding forks.
    // Their capability rejection must be definitive, never a retryable unknown
    // outcome that would occupy the worker until its execution deadline.
    struct LegacyAdapter;
    macro_rules! unused_method {
        ($name:ident($($arg:ident: $ty:ty),*) -> $reply:ty) => {
            fn $name<'a>(&'a self, $($arg: &'a $ty),*) -> ContractFuture<'a, $reply> {
                Box::pin(async { panic!("unexpected legacy adapter call") })
            }
        };
    }
    macro_rules! common_methods {
        () => {
            unused_method!(send_workflow_event(_command: WorkflowEventCommand) -> WorkflowEventReceipt);
            unused_method!(workflow_status(_scope: Scope, _id: str) -> WorkflowSnapshot);
            unused_method!(workflow_result(_scope: Scope, _id: str) -> WorkflowResult);
            unused_method!(activation_context(_owner: LeaseOwner) -> WorkflowActivationContext);
            unused_method!(record_local_result(_command: LocalResultCommand) -> LocalResultReceipt);
            unused_method!(cancel_workflow(_scope: Scope, _id: str) -> WorkflowSnapshot);
        };
    }
    impl WorkflowService for LegacyAdapter {
        common_methods!();
        unused_method!(submit_workflow(_command: SubmitCommand) -> WorkflowSnapshot);
    }
    impl WorkflowStore for LegacyAdapter {
        common_methods!();
        unused_method!(lookup_workflow_submission(_scope: Scope, _key: str) -> Option<WorkflowSnapshot>);
        unused_method!(replay_workflow_submission(_command: SubmitCommand) -> Option<WorkflowSnapshot>);
        unused_method!(accept_resolved_workflow(_command: SubmitCommand, _controller: ledgence_worker_api::ProgramDescriptor) -> WorkflowSnapshot);
        unused_method!(apply_work(_work: WorkflowWork, _resolved: [ResolvedWorkflowChild]) -> WorkflowProgress);
        unused_method!(retry_work(_work: WorkflowWork, _reason: str) -> ());
        unused_method!(reject_work(_work: WorkflowWork, _error: ApplicationError) -> ());
        fn claim_work(&self, _limit: u32) -> ContractFuture<'_, Vec<WorkflowWork>> {
            Box::pin(async { panic!("unexpected legacy adapter call") })
        }
    }
    #[test]
    fn legacy_store_and_service_reject_unsupported_forks_without_retryable_uncertainty() {
        let command = WorkflowForkCommand {
            owner: LeaseOwner {
                scope: Scope {
                    tenant_id: "tenant".into(),
                    namespace: "namespace".into(),
                },
                task_id: "task".into(),
                attempt_id: "attempt".into(),
                lease_id: "lease".into(),
                generation: 1,
                worker_session_id: "session".into(),
                consumer_id: 0,
            },
            fork: serde_json::from_value(request()).unwrap(),
            processing_trace: None,
        };
        for mut response in [
            WorkflowStore::fork_workflow(&LegacyAdapter, &command),
            WorkflowService::fork_workflow(&LegacyAdapter, &command),
        ] {
            let result = response
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
            assert!(
                matches!(result, std::task::Poll::Ready(Err(ContractError::InvalidInput(message))) if message == "workflow forks are unsupported by this adapter")
            );
        }
    }
}
