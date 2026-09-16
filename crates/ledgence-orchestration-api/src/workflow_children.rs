//! Owned workflow composition, distinct from an individual controller attempt.
use crate::*;
use ledgence_worker_api::validate_wire_value;

/// Root depth is zero. This bounds cancellation paths without a tree-wide lock.
pub const WORKFLOW_MAX_DEPTH: u32 = 16;
/// Limits simultaneous owned subworkflows, not retained historical child keys.
pub const WORKFLOW_MAX_LIVE_SUBWORKFLOWS: u32 = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowChildKind {
    #[default]
    Task,
    Workflow,
}
impl WorkflowChildKind {
    pub fn is_task(&self) -> bool {
        *self == Self::Task
    }
}

/// Legacy task inputs retain their wire shape. Workflow inputs have an explicit
/// kind and workflow outcome. Both variants reject mixed or unknown fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkflowChildResult {
    Task(WorkflowTaskResult),
    Workflow(WorkflowSubworkflowResult),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTaskResult {
    pub task_id: String,
    pub state: TaskState,
    pub outcome: TaskOutcome,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSubworkflowResult {
    pub kind: WorkflowChildKind,
    pub workflow_id: String,
    pub state: WorkflowState,
    pub outcome: WorkflowOutcome,
}
impl WorkflowChildResult {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Task(child) => {
                validate_text(&child.task_id, 128)?;
                match (&child.state, &child.outcome) {
                    (
                        TaskState::Succeeded,
                        TaskOutcome::Succeeded {
                            output,
                            attempt_id,
                            execution_may_have_started: true,
                            ..
                        },
                    ) => {
                        validate_text(attempt_id, 128)?;
                        validate_wire_value(output)?;
                    }
                    (
                        TaskState::Failed,
                        TaskOutcome::Failed {
                            attempt_id,
                            quiescence,
                            execution_may_have_started,
                            failure,
                        },
                    ) => {
                        validate_text(attempt_id, 128)?;
                        if (matches!(failure, TaskFailure::AttemptLost {})
                            && *quiescence != Quiescence::Unconfirmed)
                            || (matches!(failure, TaskFailure::Application { .. })
                                && !execution_may_have_started)
                        {
                            return Err(invalid("inconsistent failed task input"));
                        }
                    }
                    (TaskState::Cancelled, TaskOutcome::Cancelled {}) => {}
                    _ => return Err(invalid("inconsistent terminal task input")),
                }
            }
            Self::Workflow(child) => {
                if child.kind != WorkflowChildKind::Workflow {
                    return Err(invalid("workflow input requires workflow kind"));
                }
                validate_text(&child.workflow_id, 128)?;
                match (&child.state, &child.outcome) {
                    (WorkflowState::Succeeded, WorkflowOutcome::Succeeded { output }) => {
                        validate_wire_value(output)?
                    }
                    (WorkflowState::Failed, WorkflowOutcome::Failed { error }) => {
                        validate_workflow_error(error)?
                    }
                    (WorkflowState::Cancelled, WorkflowOutcome::Cancelled {}) => {}
                    _ => return Err(invalid("inconsistent terminal workflow input")),
                }
            }
        }
        Ok(())
    }
}

/// A controller task finishing is not its workflow finishing. Non-completion
/// work carries no fabricated public task identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowWorkSource {
    TaskTerminal { task_id: String, activation: bool },
    WorkflowTerminal { workflow_id: String },
    Drain,
    CancelOwned,
    Wait,
}

/// Nested lineage is paired and immutable. Roots/legacy runs omit both fields.
pub fn validate_workflow_lineage(
    workflow_id: Option<&str>,
    parent: Option<&str>,
    root: Option<&str>,
) -> Result<()> {
    if let Some(id) = workflow_id {
        validate_text(id, 128)?;
    }
    match (workflow_id, parent, root) {
        (_, None, None) => Ok(()),
        (Some(id), Some(parent), Some(root)) => {
            validate_text(parent, 128)?;
            validate_text(root, 128)?;
            if parent == id || root == id {
                return Err(invalid("workflow cannot be its own ancestor"));
            }
            Ok(())
        }
        _ => Err(invalid(
            "workflow parent and root identities must be paired",
        )),
    }
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn child() -> Value {
        json!({"key":"child","program":{"id":"program","version":"1.0.0"},"queue":"queue","data":null,"retry_policy":RetryPolicy::default(),"attempt_timeout_ms":300000})
    }
    fn decision(commands: Value) -> Value {
        json!({"v":1,"activation_id":"activation","revision":0,"kind":"suspend","state":null,"continuation":"after","commands":commands,"until":["child"]})
    }
    fn context(input: Value) -> Value {
        json!({"v":1,"workflow_id":"parent","activation_id":"activation","revision":1,"continuation":"after","state":null,"inputs":{"child":input},"local_steps":[]})
    }
    fn workflow_result() -> Value {
        json!({"kind":"workflow","workflow_id":"nested","state":"succeeded","outcome":{"kind":"succeeded","output":null}})
    }
    #[test]
    fn command_kind_preserves_legacy_shape_and_shared_key_namespace() {
        let value = decision(json!([child()]));
        let decoded = WorkflowDecision::decode(&value).unwrap();
        assert_eq!(decoded.commands()[0].kind, WorkflowChildKind::Task);
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
        let mut nested = child();
        nested["kind"] = json!("workflow");
        let decoded = WorkflowDecision::decode(&decision(json!([nested]))).unwrap();
        assert_eq!(decoded.commands()[0].kind, WorkflowChildKind::Workflow);
        assert!(WorkflowDecision::decode(&decision(json!([child(), nested]))).is_err());
        for kind in [json!("unknown"), Value::Null, json!(1)] {
            let mut wrong = child();
            wrong["kind"] = kind;
            assert!(WorkflowDecision::decode(&decision(json!([wrong]))).is_err());
        }
    }
    #[test]
    fn workflow_inputs_have_their_own_terminal_identity_and_outcome() {
        let valid = context(workflow_result());
        let decoded: WorkflowActivationContext = serde_json::from_value(valid.clone()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), valid);
        for change in 0..7 {
            let mut input = workflow_result();
            match change {
                0 => input["task_id"] = json!("controller"),
                1 => {
                    input.as_object_mut().unwrap().remove("kind");
                }
                2 => input["kind"] = json!("task"),
                3 => input["state"] = json!("waiting"),
                4 => {
                    input["outcome"] =
                        json!({"kind":"failed","error":{"kind":"business","message":"failed"}})
                }
                5 => {
                    input["outcome"].as_object_mut().unwrap().remove("output");
                }
                _ => input["workflow_id"] = json!(""),
            }
            assert!(
                serde_json::from_value::<WorkflowActivationContext>(context(input))
                    .map_or(true, |v| v.validate().is_err()),
                "case {change}"
            );
        }
        for (state, outcome) in [
            (
                "failed",
                json!({"kind":"failed","error":{"kind":"business","message":"failed"}}),
            ),
            ("cancelled", json!({"kind":"cancelled"})),
        ] {
            let mut value = workflow_result();
            value["state"] = json!(state);
            value["outcome"] = outcome;
            serde_json::from_value::<WorkflowActivationContext>(context(value))
                .unwrap()
                .validate()
                .unwrap();
        }
    }
    #[test]
    fn legacy_and_workflow_inputs_share_the_existing_byte_and_member_budget() {
        let mut value = context(workflow_result());
        let legacy = json!({"task_id":"task","state":"succeeded","outcome":{"kind":"succeeded","attempt_id":"attempt","execution_may_have_started":true,"quiescence":"confirmed","output":null}});
        value["inputs"]["task"] = legacy.clone();
        let decoded: WorkflowActivationContext = serde_json::from_value(value.clone()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
        for index in 0..WORKFLOW_MAX_COMMANDS {
            value["inputs"][format!("extra_{index}")] = legacy.clone();
        }
        assert!(
            serde_json::from_value::<WorkflowActivationContext>(value)
                .unwrap()
                .validate()
                .is_err()
        );
        let mut large = workflow_result();
        large["outcome"]["output"] = json!("x".repeat(WORKFLOW_INPUTS_MAX_BYTES));
        assert!(
            serde_json::from_value::<WorkflowActivationContext>(context(large))
                .unwrap()
                .validate()
                .is_err()
        );
    }
    #[test]
    fn failed_task_inputs_preserve_execution_and_cleanup_evidence() {
        let mut input = json!({"task_id":"task","state":"failed","outcome":{
            "kind":"failed","attempt_id":"attempt","quiescence":"unconfirmed",
            "execution_may_have_started":true,"failure":{"kind":"attempt_lost"}}});
        let validate = |input| {
            serde_json::from_value::<WorkflowActivationContext>(context(input))
                .unwrap()
                .validate()
        };
        validate(input.clone()).unwrap();
        input["outcome"]["quiescence"] = json!("confirmed");
        assert!(validate(input.clone()).is_err());
        input["outcome"]["failure"] =
            json!({"kind":"application","error":{"kind":"business","message":"failed"}});
        validate(input.clone()).unwrap();
        input["outcome"]["execution_may_have_started"] = json!(false);
        assert!(validate(input.clone()).is_err());
        input["outcome"]["attempt_id"] = json!("");
        assert!(validate(input).is_err());
    }
    #[test]
    fn lineage_is_paired_and_cannot_claim_self_ancestry() {
        assert!(validate_workflow_lineage(None, None, None).is_ok());
        assert!(validate_workflow_lineage(Some("root"), None, None).is_ok());
        assert!(validate_workflow_lineage(Some("child"), Some("root"), Some("root")).is_ok());
        assert!(validate_workflow_lineage(Some("grandchild"), Some("child"), Some("root")).is_ok());
        for (id, parent, root) in [
            (None, Some("parent"), Some("root")),
            (Some("child"), None, Some("root")),
            (Some("child"), Some("parent"), None),
            (Some("child"), Some("child"), Some("root")),
            (Some("child"), Some("parent"), Some("child")),
            (Some("child"), Some(""), Some("root")),
        ] {
            assert!(validate_workflow_lineage(id, parent, root).is_err());
        }
    }
}
