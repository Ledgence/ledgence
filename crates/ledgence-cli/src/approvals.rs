use crate::{args::ApprovalOperation, encode, submission};
use ledgence_orchestration_api::{
    APPROVAL_SNAPSHOT_MAX_BYTES, ApprovalDecisionCommand, ContractError, Result, WorkflowService,
    decode_unique_json,
};

pub async fn execute(
    service: &dyn WorkflowService,
    operation: ApprovalOperation,
) -> Result<Vec<u8>> {
    match operation {
        ApprovalOperation::Inspect {
            scope,
            workflow_id,
            key,
        } => encode(service.approval(&scope, &workflow_id, &key).await?),
        ApprovalOperation::List {
            scope,
            workflow_id,
            after_key,
            limit,
        } => encode(
            service
                .list_approvals(&scope, &workflow_id, after_key.as_deref(), limit)
                .await?,
        ),
        ApprovalOperation::Decide(path) => {
            let command = tokio::task::spawn_blocking(move || {
                let bytes = submission::read_bytes(
                    &path,
                    APPROVAL_SNAPSHOT_MAX_BYTES,
                    "approval decision",
                )?;
                let command: ApprovalDecisionCommand =
                    decode_unique_json(&bytes, APPROVAL_SNAPSHOT_MAX_BYTES)?;
                command.validate()?;
                Ok::<_, ContractError>(command)
            })
            .await
            .map_err(|_| {
                ContractError::Unavailable("approval decision preparation failed".into())
            })??;
            encode(service.decide_approval(&command).await?)
        }
    }
}
