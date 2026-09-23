//! Pure workflow decision rules over an authoritative, locked run snapshot.
//!
//! Stores own child-binding checks, terminal observations, locks and atomic
//! persistence. This planner never resolves programs or starts user work.
use ledgence_orchestration_api::*;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct WorkflowCheckpoint {
    pub state: Value,
    pub continuation: String,
}

#[derive(Debug, Clone)]
pub enum WorkflowDisposition {
    ExternalWait {
        wait: WorkflowWait,
    },
    Wait {
        members: Vec<String>,
    },
    Continue,
    Complete {
        output: Value,
    },
    /// Failing is a drain intent; it is not a terminal cleanup certificate.
    Fail {
        error: ApplicationError,
    },
}

#[derive(Debug, Clone)]
pub struct WorkflowDecisionPlan {
    pub revision: u64,
    pub checkpoint: Option<WorkflowCheckpoint>,
    pub disposition: WorkflowDisposition,
}

/// Plan one accepted controller decision. `unfinished_owned_children` must be an
/// authoritative observation under the workflow lock, including tasks and owned workflows from all
/// prior continuations. A retry of already applied work uses its stored receipt
/// before invoking this planner; stale unaccepted decisions never grant changes.
pub fn plan_workflow_decision(
    current: &WorkflowSnapshot,
    decision: &WorkflowDecision,
    unfinished_owned_children: bool,
) -> Result<WorkflowDecisionPlan> {
    decision.validate()?;
    current.validate()?;
    if current.state != WorkflowState::Running
        || current.activation_id.as_deref() != Some(decision.activation_id.as_str())
        || current.revision != decision.revision
    {
        return Err(ContractError::OwnershipLost);
    }
    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| ContractError::InvalidInput("workflow revision exhausted".into()))?;
    let (checkpoint, disposition) = match &decision.action {
        WorkflowAction::Wait {
            state,
            continuation,
            wait,
            ..
        } => (
            Some(WorkflowCheckpoint {
                state: state.clone(),
                continuation: continuation.clone(),
            }),
            WorkflowDisposition::ExternalWait { wait: wait.clone() },
        ),
        WorkflowAction::Suspend {
            state,
            continuation,
            until,
            ..
        } => (
            Some(WorkflowCheckpoint {
                state: state.clone(),
                continuation: continuation.clone(),
            }),
            WorkflowDisposition::Wait {
                members: until.clone(),
            },
        ),
        WorkflowAction::Continue {
            state,
            continuation,
            ..
        } => (
            Some(WorkflowCheckpoint {
                state: state.clone(),
                continuation: continuation.clone(),
            }),
            WorkflowDisposition::Continue,
        ),
        WorkflowAction::Complete { output } => {
            if unfinished_owned_children {
                return Err(ContractError::InvalidInput(
                    "workflow cannot complete with unfinished owned children".into(),
                ));
            }
            (
                None,
                WorkflowDisposition::Complete {
                    output: output.clone(),
                },
            )
        }
        WorkflowAction::Fail { error } => (
            None,
            WorkflowDisposition::Fail {
                error: error.clone(),
            },
        ),
    };
    Ok(WorkflowDecisionPlan {
        revision,
        checkpoint,
        disposition,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn current() -> WorkflowSnapshot {
        WorkflowSnapshot {
            parent_workflow_id: None,
            root_workflow_id: None,
            workflow_id: "wf_1".into(),
            scope: Scope {
                tenant_id: "t".into(),
                namespace: "n".into(),
            },
            state: WorkflowState::Running,
            revision: 7,
            activation_id: Some("task_1".into()),
            submitted_at: 1,
            terminal_at: None,
            correlation_key: None,
        }
    }
    fn decision(action: WorkflowAction) -> WorkflowDecision {
        WorkflowDecision {
            v: 1,
            activation_id: "task_1".into(),
            revision: 7,
            action,
        }
    }
    #[test]
    fn suspend_and_continue_checkpoint_without_finishing_owned_work() {
        let decision = decision(WorkflowAction::Suspend {
            state: json!({"stage":2}),
            continuation: "after".into(),
            commands: vec![],
            until: vec!["child".into()],
        });
        let plan = plan_workflow_decision(&current(), &decision, true).unwrap();
        assert_eq!(plan.revision, 8);
        assert_eq!(plan.checkpoint.unwrap().state, json!({"stage":2}));
        assert!(
            matches!(plan.disposition,WorkflowDisposition::Wait{members} if members == ["child"])
        );
        assert_eq!(current().revision, 7);
    }
    #[test]
    fn complete_requires_terminal_children_but_fail_remains_a_drain_intent() {
        let complete = decision(WorkflowAction::Complete {
            output: Value::Null,
        });
        assert!(matches!(
            plan_workflow_decision(&current(), &complete, true),
            Err(ContractError::InvalidInput(_))
        ));
        assert_eq!(
            plan_workflow_decision(&current(), &complete, false)
                .unwrap()
                .revision,
            8
        );
        let fail = decision(WorkflowAction::Fail {
            error: ApplicationError {
                kind: "failed".into(),
                message: "reason".into(),
            },
        });
        assert!(matches!(
            plan_workflow_decision(&current(), &fail, true)
                .unwrap()
                .disposition,
            WorkflowDisposition::Fail { .. }
        ));
    }
    #[test]
    fn stale_identity_revision_cancellation_and_overflow_grant_no_plan() {
        let decision = decision(WorkflowAction::Complete {
            output: Value::Null,
        });
        let mut state = current();
        state.activation_id = Some("task_other".into());
        assert!(matches!(
            plan_workflow_decision(&state, &decision, false),
            Err(ContractError::OwnershipLost)
        ));
        state = current();
        state.revision += 1;
        assert!(matches!(
            plan_workflow_decision(&state, &decision, false),
            Err(ContractError::OwnershipLost)
        ));
        state = current();
        state.state = WorkflowState::Cancelling;
        assert!(matches!(
            plan_workflow_decision(&state, &decision, false),
            Err(ContractError::OwnershipLost)
        ));
        state = current();
        state.revision = u64::MAX;
        let mut overflow = decision;
        overflow.revision = u64::MAX;
        assert!(matches!(
            plan_workflow_decision(&state, &overflow, false),
            Err(ContractError::InvalidInput(_))
        ));
    }
}
