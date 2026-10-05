//! Application value checks that serde's structural decoding does not perform.
//! Execution authority and replay semantics remain with the delivery/core layers.

use ledgence_orchestration_api::*;
use ledgence_worker_api::{CloudEvent, ProgramOutcome, validate_wire_value};
use serde::de::DeserializeOwned;

pub(crate) trait ResponseValue: DeserializeOwned + Send + 'static {
    const MAX_BYTES: usize = crate::RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        Ok(())
    }
}
impl ResponseValue for TaskStatus {
    const MAX_BYTES: usize = crate::STATUS_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for TaskPage {
    const MAX_BYTES: usize = TASK_PAGE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        if self.items.len() > TASK_LIST_MAX_LIMIT as usize {
            return Err(ContractError::Unavailable(
                "task page exceeds item limit".into(),
            ));
        }
        for item in &self.items {
            item.validate()?;
        }
        Ok(())
    }
}
impl ResponseValue for TaskResult {
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for WorkerSession {}
impl ResponseValue for Authority {}
impl ResponseValue for SettleReply {}
impl ResponseValue for TaskState {}
impl ResponseValue for Vec<RecordedHistoryEvent> {}
impl ResponseValue for TaskSnapshot {
    fn validate_values(&self) -> Result<()> {
        self.input.validate()?;
        self.descriptor.validate()?;
        validate_text(&self.task_id, 128)?;
        validate_text(&self.run_id, 128)?;
        validate_text(&self.idempotency_key, 255)?;
        if let Some(trace) = &self.origin_trace {
            trace.validate()?;
        }
        if self.descriptor.program != self.input.program
            || self
                .workflow_activation_id
                .as_ref()
                .is_some_and(|id| id != &self.task_id || self.workflow_id.is_none())
        {
            return Err(ContractError::Unavailable(
                "inconsistent task snapshot identity".into(),
            ));
        }
        validate_workflow_lineage(
            self.workflow_id.as_deref(),
            self.parent_workflow_id.as_deref(),
            self.root_workflow_id.as_deref(),
        )
    }
}
impl ResponseValue for ClaimReply {
    const MAX_BYTES: usize = CLAIM_REPLY_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        if let ClaimDisposition::Claimed { reply } = &self.disposition {
            reply.validate_values()?;
        }
        Ok(())
    }
}
impl ResponseValue for AcquireReply {
    fn validate_values(&self) -> Result<()> {
        if let Self::Assigned { assignment, .. } = self {
            validate_event_data(&assignment.event)?;
            assignment.descriptor.validate()?;
            assignment.validate_workflow_identity()?;
        }
        Ok(())
    }
}
impl ResponseValue for AttemptSnapshot {
    fn validate_values(&self) -> Result<()> {
        validate_event_data(&self.event)?;
        self.descriptor.validate()?;
        let owner = &self.lease.owner;
        if self.event.tenant_id() != owner.scope.tenant_id
            || self.event.namespace() != owner.scope.namespace
            || self.event.task_id() != owner.task_id
            || self.event.attempt_id() != owner.attempt_id
            || self.event.value()["ldgattemptno"].as_u64() != Some(u64::from(owner.generation))
            || self
                .last_renewal
                .as_ref()
                .is_some_and(|renewal| &renewal.owner != owner)
            || self.settlement.as_ref().is_some_and(|accepted| {
                &accepted.command.owner != owner
                    || accepted.receipt.task_id != owner.task_id
                    || accepted.receipt.attempt_id != owner.attempt_id
            })
        {
            return Err(ContractError::Unavailable(
                "inconsistent attempt snapshot identity".into(),
            ));
        }
        if let Some(accepted) = &self.settlement {
            if let AttemptReport::Completed(report) = &accepted.command.report
                && let ProgramOutcome::Success { output } = &report.outcome
            {
                validate_task_output(output, report.context.identity.activation_id.is_some())?;
            }
            let command = serde_json::to_vec(&accepted.command).map_err(|_| {
                ContractError::InvalidInput("invalid accepted settlement JSON".into())
            })?;
            if command.len() > SETTLEMENT_MAX_BYTES {
                return Err(ContractError::InvalidInput(
                    "accepted settlement exceeds its body limit".into(),
                ));
            }
        }
        Ok(())
    }
}
fn validate_event_data(event: &CloudEvent) -> Result<()> {
    let data = &event.value()["data"];
    validate_wire_value(data)?;
    let bytes = serde_json::to_vec(data)
        .map_err(|_| ContractError::InvalidInput("invalid event data JSON".into()))?;
    if bytes.len() > SUBMISSION_DATA_MAX_BYTES {
        return Err(ContractError::InvalidInput(
            "event data exceeds submission limit".into(),
        ));
    }
    Ok(())
}

impl ResponseValue for WorkflowSnapshot {
    const MAX_BYTES: usize = TASK_STATUS_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for WorkflowResult {
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for WorkflowActivationContext {
    const MAX_BYTES: usize = WORKFLOW_CONTEXT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for LocalResultReceipt {
    const MAX_BYTES: usize = 1024;
    fn validate_values(&self) -> Result<()> {
        validate_text(&self.key, 128)
    }
}

impl ResponseValue for WorkflowForkReceipt {
    const MAX_BYTES: usize = crate::WORKFLOW_FORK_RECEIPT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}

impl ResponseValue for WorkflowEventReceipt {
    const MAX_BYTES: usize = TASK_STATUS_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}

impl ResponseValue for CompletionSubscription {
    const MAX_BYTES: usize = COMPLETION_STATUS_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}

impl ResponseValue for ApprovalSnapshot {
    const MAX_BYTES: usize = APPROVAL_SNAPSHOT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ApprovalDecisionReceipt {
    const MAX_BYTES: usize = APPROVAL_SNAPSHOT_MAX_BYTES + 1024;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ApprovalPage {
    const MAX_BYTES: usize = 1024 * 1024;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
