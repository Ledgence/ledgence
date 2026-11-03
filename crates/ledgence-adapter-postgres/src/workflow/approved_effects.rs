use super::*;

impl PostgresStore {
    pub(super) async fn claim_approved_effect_once(
        &self,
        command: &ApprovedEffectClaimCommand,
    ) -> StoreResult<ApprovedEffectClaimReceipt> {
        command.validate()?;
        let owner = &command.owner;
        self.require_scope(&owner.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = execution::lock_activation(&mut tx, owner).await?;
        execution::check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
        if workflow.workflow_id != command.workflow_id {
            return Err(ContractError::Conflict.into());
        }

        // schedule_activation binds this approval to the new activation in the
        // transaction which writes its frozen wake. The workflow row lock also
        // serializes cancellation, expiry, resumption and competing grants.
        let approval =
            approvals::load(&mut tx, &workflow.workflow_id, &command.approval_key).await?;
        if approval.scope != owner.scope {
            return Err(corrupt("approval scope").into());
        }
        if approval.status != ApprovalStatus::Approved
            || approval.resumed_activation_id.as_ref() != Some(&owner.task_id)
        {
            return Err(ContractError::ObsoleteOperation.into());
        }
        if !approval.action.matches(&command.action)? {
            return Err(ContractError::Conflict.into());
        }
        let claim = sqlx::query(
            "UPDATE workflow_approvals SET effect_dispatch_claimed=true, effect_dispatch_attempt_id=$3
             WHERE workflow_id=$1 AND wait_key=$2 AND NOT effect_dispatch_claimed",
        )
        .bind(&workflow.workflow_id)
        .bind(&command.approval_key)
        .bind(&owner.attempt_id)
        .execute(&mut *tx)
        .await?;
        let disposition = if claim.rows_affected() == 1 {
            ApprovedEffectDisposition::Dispatch
        } else {
            ApprovedEffectDisposition::LookupOnly
        };
        // Recheck the time fence after the write. Dropping this transaction
        // rolls back a new claim if validation or lock acquisition outlived it.
        execution::check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
        tx.commit().await?;
        Ok(ApprovedEffectClaimReceipt {
            scope: approval.scope,
            workflow_id: approval.workflow_id,
            activation_id: owner.task_id.clone(),
            attempt_id: owner.attempt_id.clone(),
            approval_key: approval.key,
            action: approval.action,
            disposition,
        })
    }
}
