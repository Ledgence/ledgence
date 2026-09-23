//! Checkpoint workflow contracts. Workflow state never occupies CloudEvent data.
//!
//! A logical activation is an ordinary leased task plus an explicit, immutable
//! continuation context. Its retries share one local-step journal. A terminal
//! controller result is interpreted as a decision only for registered activations.
use crate::*;
use ledgence_worker_api::{ProgramDescriptor, ProgramRef, validate_wire_value};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const WORKFLOW_RUNTIME_SCHEMA: &str = "ledgence.workflow.activation.v1";
pub const WORKFLOW_VERSION: u8 = 1;
pub const WORKFLOW_MAX_COMMANDS: usize = 64;
pub const WORKFLOW_MAX_LOCAL_STEPS: usize = 128;
pub const WORKFLOW_CHECKPOINT_MAX_BYTES: usize = 64 * 1024;
pub const WORKFLOW_DECISION_MAX_BYTES: usize = 256 * 1024;
pub const WORKFLOW_INPUTS_MAX_BYTES: usize = 256 * 1024;
pub const WORKFLOW_LOCAL_RECORD_MAX_BYTES: usize = 128 * 1024;
pub const WORKFLOW_LOCAL_LEDGER_MAX_BYTES: usize = 256 * 1024;
pub const WORKFLOW_CONTEXT_MAX_BYTES: usize = 640 * 1024;
pub const WORKFLOW_MAX_WORK_BATCH: u32 = 16;

/// Controller results include a platform decision envelope around application
/// values. Ordinary task output retains its depth-64 contract; registered
/// activations allow metadata depth 96 before the coordinator validates each
/// application value and the exact decision shape. The lifecycle core verifies
/// the report's activation identity against the acquired task before acceptance.
pub fn validate_task_output(output: &Value, workflow_activation: bool) -> Result<()> {
    if workflow_activation {
        ledgence_worker_api::validate_runtime_payload(output, SETTLEMENT_MAX_BYTES)?;
    } else {
        validate_wire_value(output)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStepRecord {
    pub key: String,
    pub callable: String,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub input: Value,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub output: Value,
}
impl LocalStepRecord {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        validate_text(&self.callable, 512)?;
        validate_wire_value(&self.input)?;
        validate_wire_value(&self.output)?;
        bounded(self, WORKFLOW_LOCAL_RECORD_MAX_BYTES, "local step")
    }

    /// Binding and value are immutable once acknowledged, including numeric
    /// representation (1 and 1.0 are different). JSON object order is irrelevant.
    pub fn matches(&self, other: &Self) -> Result<bool> {
        self.validate()?;
        other.validate()?;
        Ok(
            canonical_json_bytes(&serde_json::to_value(self).map_err(encoding)?)?
                == canonical_json_bytes(&serde_json::to_value(other).map_err(encoding)?)?,
        )
    }
}

/// Frozen activation inputs and checkpoint, plus the current committed journal.
/// New child completions do not mutate the frozen input batch or revision.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowActivationContext {
    /// Present together only for a nested owned workflow; roots omit both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_workflow_id: Option<String>,
    pub v: u8,
    pub workflow_id: String,
    pub activation_id: String,
    pub revision: u64,
    pub continuation: String,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub state: Value,
    pub inputs: BTreeMap<String, WorkflowChildResult>,
    pub local_steps: Vec<LocalStepRecord>,
    /// Frozen result of one external event/timer wait. Absent on older contexts
    /// and ordinary child continuations; never merged into user CloudEvent data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake: Option<WorkflowWake>,
}
impl WorkflowActivationContext {
    pub fn validate(&self) -> Result<()> {
        version(self.v)?;
        validate_workflow_lineage(
            Some(&self.workflow_id),
            self.parent_workflow_id.as_deref(),
            self.root_workflow_id.as_deref(),
        )?;
        validate_text(&self.activation_id, 128)?;
        validate_text(&self.continuation, 128)?;
        checkpoint(&self.state)?;
        if self.inputs.len() > WORKFLOW_MAX_COMMANDS
            || self.local_steps.len() > WORKFLOW_MAX_LOCAL_STEPS
        {
            return Err(invalid("workflow context has too many entries"));
        }
        for (key, child) in &self.inputs {
            validate_text(key, 128)?;
            child.validate()?;
        }
        if let Some(wake) = &self.wake {
            wake.validate()?;
            #[derive(Serialize)]
            struct FrozenInputs<'a> {
                inputs: &'a BTreeMap<String, WorkflowChildResult>,
                wake: &'a WorkflowWake,
            }
            bounded(
                &FrozenInputs {
                    inputs: &self.inputs,
                    wake,
                },
                WORKFLOW_INPUTS_MAX_BYTES,
                "workflow inputs and wake",
            )?;
        } else {
            bounded(&self.inputs, WORKFLOW_INPUTS_MAX_BYTES, "workflow inputs")?;
        }
        let mut keys = BTreeSet::new();
        for step in &self.local_steps {
            step.validate()?;
            if !keys.insert(&step.key) {
                return Err(invalid("duplicate local step key"));
            }
        }
        bounded(
            &self.local_steps,
            WORKFLOW_LOCAL_LEDGER_MAX_BYTES,
            "local step ledger",
        )?;
        bounded(self, WORKFLOW_CONTEXT_MAX_BYTES, "workflow context")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowChildCommand {
    #[serde(default, skip_serializing_if = "WorkflowChildKind::is_task")]
    pub kind: WorkflowChildKind,
    pub key: String,
    pub program: ProgramRef,
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
/// Compatibility name for the original task-only command contract.
pub type WorkflowTaskCommand = WorkflowChildCommand;

impl WorkflowChildCommand {
    pub fn submission(&self, scope: &Scope, correlation_key: Option<String>) -> SubmitTask {
        SubmitTask {
            tenant_id: scope.tenant_id.clone(),
            namespace: scope.namespace.clone(),
            queue: self.queue.clone(),
            program: self.program.clone(),
            correlation_key,
            data: self.data.clone(),
            retry_policy: self.retry_policy.clone(),
            attempt_timeout_ms: self.attempt_timeout_ms,
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        self.submission(
            &Scope {
                tenant_id: "validation".into(),
                namespace: "validation".into(),
            },
            None,
        )
        .validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDecision {
    pub v: u8,
    pub activation_id: String,
    pub revision: u64,
    #[serde(flatten)]
    pub action: WorkflowAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkflowAction {
    Wait {
        state: Value,
        continuation: String,
        commands: Vec<WorkflowChildCommand>,
        wait: WorkflowWait,
    },
    Suspend {
        state: Value,
        continuation: String,
        commands: Vec<WorkflowChildCommand>,
        /// Sealed all-terminal membership. Empty membership is immediately ready.
        until: Vec<String>,
    },
    Continue {
        state: Value,
        continuation: String,
        commands: Vec<WorkflowChildCommand>,
    },
    Complete {
        output: Value,
    },
    Fail {
        error: ApplicationError,
    },
}
impl WorkflowDecision {
    /// Strictly decode a controller outcome; serde flatten alone cannot reject
    /// unknown fields reliably, so the exact top-level shape is checked first.
    pub fn decode(value: &Value) -> Result<Self> {
        // Bound a borrowed value before deserialization clones its payload.
        bounded(value, WORKFLOW_DECISION_MAX_BYTES, "workflow decision")?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("workflow decision must be an object"))?;
        let action_fields: &[&str] = match object.get("kind").and_then(Value::as_str) {
            Some("wait") => &["state", "continuation", "commands", "wait"],
            Some("suspend") => &["state", "continuation", "commands", "until"],
            Some("continue") => &["state", "continuation", "commands"],
            Some("complete") => &["output"],
            Some("fail") => &["error"],
            _ => return Err(invalid("unknown workflow decision kind")),
        };
        let common = ["v", "activation_id", "revision", "kind"];
        if object.len() != common.len() + action_fields.len()
            || object.keys().any(|key| {
                !common.contains(&key.as_str()) && !action_fields.contains(&key.as_str())
            })
        {
            return Err(invalid("unknown or missing workflow decision fields"));
        }
        let decision: Self = serde_json::from_value(value.clone()).map_err(encoding)?;
        decision.validate()?;
        Ok(decision)
    }
    pub fn validate(&self) -> Result<()> {
        version(self.v)?;
        validate_text(&self.activation_id, 128)?;
        match &self.action {
            WorkflowAction::Wait {
                state,
                continuation,
                commands,
                wait,
            } => {
                validate_continuation(state, continuation, commands)?;
                wait.validate()?;
            }
            WorkflowAction::Suspend {
                state,
                continuation,
                commands,
                until,
            } => {
                validate_continuation(state, continuation, commands)?;
                if until.len() > WORKFLOW_MAX_COMMANDS {
                    return Err(invalid("wait membership exceeds supported limit"));
                }
                let mut keys = BTreeSet::new();
                for key in until {
                    validate_text(key, 128)?;
                    if !keys.insert(key) {
                        return Err(invalid("duplicate wait member"));
                    }
                }
            }
            WorkflowAction::Continue {
                state,
                continuation,
                commands,
            } => validate_continuation(state, continuation, commands)?,
            WorkflowAction::Complete { output } => validate_wire_value(output)?,
            WorkflowAction::Fail { error } => validate_error(error)?,
        }
        bounded(self, WORKFLOW_DECISION_MAX_BYTES, "workflow decision")
    }
    pub fn commands(&self) -> &[WorkflowChildCommand] {
        match &self.action {
            WorkflowAction::Wait { commands, .. }
            | WorkflowAction::Suspend { commands, .. }
            | WorkflowAction::Continue { commands, .. } => commands,
            WorkflowAction::Complete { .. } | WorkflowAction::Fail { .. } => &[],
        }
    }
}
fn validate_continuation(
    state: &Value,
    continuation: &str,
    commands: &[WorkflowChildCommand],
) -> Result<()> {
    checkpoint(state)?;
    validate_text(continuation, 128)?;
    if commands.len() > WORKFLOW_MAX_COMMANDS {
        return Err(invalid("workflow command batch exceeds supported limit"));
    }
    let mut keys = BTreeSet::new();
    for command in commands {
        command.validate()?;
        if !keys.insert(&command.key) {
            return Err(invalid("duplicate workflow command key"));
        }
    }
    Ok(())
}
fn checkpoint(state: &Value) -> Result<()> {
    validate_wire_value(state)?;
    bounded(state, WORKFLOW_CHECKPOINT_MAX_BYTES, "workflow checkpoint")
}
fn version(v: u8) -> Result<()> {
    if v != WORKFLOW_VERSION {
        return Err(invalid("unsupported workflow version"));
    }
    Ok(())
}
pub fn validate_workflow_error(error: &ApplicationError) -> Result<()> {
    validate_error(error)
}
fn validate_error(error: &ApplicationError) -> Result<()> {
    validate_text(&error.kind, 128)?;
    if error.message.len() > 4096 {
        return Err(invalid("workflow error message exceeds 4096 bytes"));
    }
    Ok(())
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn encoding(error: serde_json::Error) -> ContractError {
    ContractError::InvalidInput(format!("invalid workflow JSON: {error}"))
}
fn bounded(value: &impl Serialize, bytes: usize, label: &str) -> Result<()> {
    crate::submission::check_encoded_size(value, bytes, label).map_err(Into::into)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Running,
    Waiting,
    Failing,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}
impl WorkflowState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSnapshot {
    /// Present together only for a nested owned workflow; roots omit both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_workflow_id: Option<String>,
    pub workflow_id: String,
    pub scope: Scope,
    pub state: WorkflowState,
    pub revision: u64,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub activation_id: Option<String>,
    pub submitted_at: Timestamp,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub terminal_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub correlation_key: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowOutcome {
    Succeeded {
        #[serde(deserialize_with = "crate::observation::required_value")]
        output: Value,
    },
    Failed {
        error: ApplicationError,
    },
    Cancelled {},
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowResult {
    pub workflow: WorkflowSnapshot,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub outcome: Option<WorkflowOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalResultCommand {
    pub owner: LeaseOwner,
    pub record: LocalStepRecord,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalResultReceipt {
    pub key: String,
    pub already_accepted: bool,
}
#[derive(Debug, Clone)]
pub struct WorkflowWork {
    pub id: String,
    pub token: String,
    pub workflow_id: String,
    pub source: WorkflowWorkSource,
    /// Only controller outcomes are loaded here. Child completion notifications
    /// stay compact; their payloads are read once a continuation needs them.
    pub outcome: Option<TaskOutcome>,
    /// Previously registered child bindings; their pinned descriptors win on replay.
    pub resolved_children: Vec<ResolvedWorkflowChild>,
}
#[derive(Debug, Clone)]
pub struct ResolvedWorkflowChild {
    pub kind: WorkflowChildKind,
    pub key: String,
    pub descriptor: ProgramDescriptor,
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct WorkflowProgress {
    pub processed: u32,
    pub activations_scheduled: u32,
    /// Newly registered ordinary tasks and owned workflow runs, combined.
    pub children_scheduled: u32,
}

/// Optional workflow persistence over the same transactional authority as the
/// application's task store. Implementations must atomically create tasks and
/// dispatch obligations, append terminal completion work with task finalization,
/// and apply checkpoints/child bindings/waits with their scheduling obligations.
/// Separate, non-atomic task and workflow backends do not satisfy this port.
///
/// Child keys are workflow-scoped and retain their first normalized submission
/// and pinned descriptor. Local keys are activation-scoped: exact records may be
/// acknowledged again to their original accepting owner after expiry; a different
/// attempt must hold current live authority. New records always require that
/// authority and must be fenced against cancellation/continuation changes.
/// Frozen activation inputs and revision never change as child events arrive.
/// Work claims are bounded, leased, recoverable, and released before external
/// program resolution; application rechecks ownership and persisted source state.
/// Successful replies follow commit of every required write. Transport loss may
/// leave a committed operation whose immutable identity must be reconciled.
pub trait WorkflowStore: Send + Sync {
    /// Accept a directly addressed, one-shot event after committing its receipt.
    /// Exact source/ID/key/payload replays must reconcile before terminal checks.
    /// Implementations serialize acceptance and wait resolution under workflow
    /// authority; caller timestamps never decide event/deadline eligibility.
    fn send_workflow_event<'a>(
        &'a self,
        command: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt>;
    fn lookup_workflow_submission<'a>(
        &'a self,
        scope: &'a Scope,
        key: &'a str,
    ) -> ContractFuture<'a, Option<WorkflowSnapshot>>;
    /// Replays the original normalized submission before resolving packages.
    fn replay_workflow_submission<'a>(
        &'a self,
        command: &'a SubmitCommand,
    ) -> ContractFuture<'a, Option<WorkflowSnapshot>>;
    fn accept_resolved_workflow<'a>(
        &'a self,
        command: &'a SubmitCommand,
        controller: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, WorkflowSnapshot>;
    fn workflow_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot>;
    fn workflow_result<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowResult>;
    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext>;
    fn record_local_result<'a>(
        &'a self,
        command: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt>;
    /// Work leases own coordinator application, never worker execution. Claiming
    /// releases database locks before any external descriptor resolution.
    fn claim_work(&self, limit: u32) -> ContractFuture<'_, Vec<WorkflowWork>>;
    fn apply_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        resolved: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress>;
    fn retry_work<'a>(&'a self, work: &'a WorkflowWork, reason: &'a str) -> ContractFuture<'a, ()>;
    /// Persist permanent decision/application failure and drain owned children.
    fn reject_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        error: &'a ApplicationError,
    ) -> ContractFuture<'a, ()>;
    fn cancel_workflow<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot>;
}

/// Client and interactive-worker operations. Unsupported implementations must
/// reject explicitly instead of silently submitting an ordinary task.
pub trait WorkflowService: Send + Sync {
    /// Accept a directly addressed, one-shot event after committing its receipt.
    /// Exact source/ID/key/payload replays must reconcile before terminal checks.
    /// Implementations serialize acceptance and wait resolution under workflow
    /// authority; caller timestamps never decide event/deadline eligibility.
    fn send_workflow_event<'a>(
        &'a self,
        command: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt>;
    fn submit_workflow<'a>(
        &'a self,
        command: &'a SubmitCommand,
    ) -> ContractFuture<'a, WorkflowSnapshot>;
    fn workflow_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot>;
    fn workflow_result<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowResult>;
    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext>;
    fn record_local_result<'a>(
        &'a self,
        command: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt>;
    fn cancel_workflow<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot>;
}

impl WorkflowSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_workflow_lineage(
            Some(&self.workflow_id),
            self.parent_workflow_id.as_deref(),
            self.root_workflow_id.as_deref(),
        )?;
        if let Some(id) = &self.activation_id {
            validate_text(id, 128)?;
        }
        if self.state.is_terminal() != self.terminal_at.is_some()
            || (self.state.is_terminal() && self.activation_id.is_some())
            || (self.state == WorkflowState::Running && self.activation_id.is_none())
            || self.terminal_at.is_some_and(|at| at < self.submitted_at)
            || self.submitted_at > 253_402_300_799_999
            || self.terminal_at.is_some_and(|at| at > 253_402_300_799_999)
        {
            return Err(invalid("inconsistent workflow status"));
        }
        if self
            .correlation_key
            .as_ref()
            .is_some_and(|key| key.len() > 512 || key.chars().any(char::is_control))
        {
            return Err(invalid("invalid workflow correlation key"));
        }
        Ok(())
    }
}
impl WorkflowResult {
    pub fn validate(&self) -> Result<()> {
        self.workflow.validate()?;
        match (&self.outcome, self.workflow.state) {
            (None, state) if !state.is_terminal() => Ok(()),
            (Some(WorkflowOutcome::Succeeded { output }), WorkflowState::Succeeded) => {
                validate_wire_value(output)?;
                bounded(output, WORKFLOW_DECISION_MAX_BYTES, "workflow output")
            }
            (Some(WorkflowOutcome::Failed { error }), WorkflowState::Failed) => {
                validate_error(error)
            }
            (Some(WorkflowOutcome::Cancelled {}), WorkflowState::Cancelled) => Ok(()),
            _ => Err(invalid("inconsistent workflow result")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn decision() -> Value {
        json!({"v":1,"activation_id":"task_1","revision":0,"kind":"suspend","continuation":"after","state":null,"commands":[],"until":[]})
    }
    #[test]
    fn decisions_require_exact_versioned_shape_and_sealed_unique_membership() {
        assert!(WorkflowDecision::decode(&decision()).is_ok());
        for (key, value) in [
            ("v", json!(2)),
            ("commands", json!(null)),
            ("until", json!(["a", "a"])),
            ("extra", json!(true)),
        ] {
            let mut input = decision();
            input[key] = value;
            assert!(WorkflowDecision::decode(&input).is_err(), "{key}");
        }
        let mut input = decision();
        input.as_object_mut().unwrap().remove("state");
        assert!(WorkflowDecision::decode(&input).is_err());
    }
    #[test]
    fn durable_local_replay_binds_numeric_representation_and_result() {
        let record = LocalStepRecord {
            key: "fetch".into(),
            callable: "app:fetch".into(),
            input: json!({"a":1,"b":2}),
            output: Value::Null,
        };
        let mut replay = record.clone();
        replay.input = json!({"b":2,"a":1});
        assert!(record.matches(&replay).unwrap());
        replay.input = json!({"a":1.0,"b":2});
        assert!(!record.matches(&replay).unwrap());
        replay = record.clone();
        replay.output = json!(false);
        assert!(!record.matches(&replay).unwrap());
    }
    #[test]
    fn oversized_checkpoints_fail_before_persistence() {
        let mut input = decision();
        input["state"] = Value::String("x".repeat(WORKFLOW_CHECKPOINT_MAX_BYTES));
        assert!(WorkflowDecision::decode(&input).is_err());
    }
    #[test]
    fn activation_context_rejects_nested_child_values_beyond_the_application_bound() {
        let mut output = Value::Null;
        for _ in 0..64 {
            output = json!([output]);
        }
        let child = WorkflowChildResult::Task(WorkflowTaskResult {
            task_id: "child".into(),
            state: TaskState::Succeeded,
            outcome: TaskOutcome::Succeeded {
                attempt_id: "attempt".into(),
                quiescence: Quiescence::Confirmed,
                execution_may_have_started: true,
                output,
            },
        });
        let mut context = WorkflowActivationContext {
            parent_workflow_id: None,
            root_workflow_id: None,
            v: 1,
            workflow_id: "workflow".into(),
            activation_id: "activation".into(),
            revision: 0,
            continuation: "next".into(),
            state: Value::Null,
            inputs: BTreeMap::from([("child".into(), child)]),
            local_steps: vec![],
            wake: None,
        };
        context.validate().unwrap();
        let WorkflowChildResult::Task(child) = context.inputs.get_mut("child").unwrap() else {
            unreachable!()
        };
        let TaskOutcome::Succeeded { output, .. } = &mut child.outcome else {
            unreachable!()
        };
        *output = json!([output.take()]);
        assert!(context.validate().is_err());
    }

    #[test]
    fn workflow_wire_distinguishes_explicit_null_from_missing_required_fields() {
        let snapshot = json!({"workflow_id":"workflow","scope":{"tenant_id":"t","namespace":"n"},"state":"waiting","revision":1,"activation_id":null,"submitted_at":1,"terminal_at":null,"correlation_key":null});
        serde_json::from_value::<WorkflowSnapshot>(snapshot.clone())
            .unwrap()
            .validate()
            .unwrap();
        for key in ["activation_id", "terminal_at", "correlation_key"] {
            let mut missing = snapshot.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<WorkflowSnapshot>(missing).is_err(),
                "{key}"
            );
        }
        assert!(
            serde_json::from_value::<WorkflowResult>(json!({"workflow":snapshot,"outcome":null}))
                .is_ok()
        );
        assert!(serde_json::from_value::<WorkflowResult>(json!({"workflow":snapshot})).is_err());
        assert!(
            serde_json::from_value::<WorkflowOutcome>(json!({"kind":"succeeded","output":null}))
                .is_ok()
        );
        assert!(serde_json::from_value::<WorkflowOutcome>(json!({"kind":"succeeded"})).is_err());
        let local = json!({"key":"key","callable":"app:f","input":null,"output":null});
        serde_json::from_value::<LocalStepRecord>(local.clone())
            .unwrap()
            .validate()
            .unwrap();
        for key in ["input", "output"] {
            let mut missing = local.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<LocalStepRecord>(missing).is_err());
        }
    }
}
