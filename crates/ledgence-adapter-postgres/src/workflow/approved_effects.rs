use super::*;

/// Recognize an SDK-confirmed record using the actual approved action. The legacy
/// approved_local helper uses the same key but journals plain business arguments.
pub(crate) fn confirmed_lookup<'a>(
    record: &'a LocalStepRecord,
    step_key: &str,
    action: &ApprovalAction,
) -> Result<&'a str> {
    record.validate()?;
    let input = record.input.as_object().ok_or(ContractError::Conflict)?;
    let lookup = input
        .get("lookup")
        .and_then(Value::as_str)
        .ok_or(ContractError::Conflict)?;
    let json = input
        .get("action_json")
        .and_then(Value::as_str)
        .ok_or(ContractError::Conflict)?;
    validate_text(lookup, 512)?;
    if input.len() != 3
        || input.get("v").and_then(Value::as_u64) != Some(1)
        || record.key != step_key
        || record.callable != action.name
        || lookup == action.name
    {
        return Err(ContractError::Conflict);
    }
    // The outer record already bounds the text. Domain comparison bounds the
    // decoded action independently and preserves integer/float and signed zero.
    let recorded: ApprovalAction = decode_unique_json(json.as_bytes(), json.len())?;
    if !action.matches(&recorded)? {
        return Err(ContractError::Conflict);
    }
    Ok(lookup)
}

struct Evidence {
    approval: ApprovalSnapshot,
    step_key: String,
    claimed: bool,
    state: Option<String>,
    lookup: Option<String>,
    confirmed_attempt: Option<String>,
    prior: Option<(Vec<u8>, String)>,
}

impl Evidence {
    /// Called under the workflow and task locks. Loading the journal here also
    /// prevents a fresh grant if an ordinary approved_local used the occurrence.
    async fn load(
        tx: &mut PgConnection,
        workflow: &WorkflowSnapshot,
        owner: &LeaseOwner,
        workflow_id: &str,
        key: &str,
        lookup: &str,
    ) -> StoreResult<Self> {
        if workflow.workflow_id != workflow_id {
            return Err(ContractError::Conflict.into());
        }
        let row = sqlx::query(
            "SELECT a.snapshot_bytes,a.effect_dispatch_claimed,a.effect_evidence_state,
                    a.effect_lookup,a.effect_confirmed_attempt_id,k.step_key,j.record_bytes,j.attempt_id
             FROM workflow_approvals a
             CROSS JOIN LATERAL (SELECT '_approval:' || encode(sha256(convert_to(a.wait_key,'UTF8')),'hex') AS step_key) k
             LEFT JOIN workflow_local_results j ON j.activation_id=$3 AND j.step_key=k.step_key
             WHERE a.workflow_id=$1 AND a.wait_key=$2",
        )
        .bind(workflow_id).bind(key).bind(&owner.task_id).fetch_optional(tx).await?
        .ok_or(ContractError::NotFound)?;
        let approval = approvals::decode(&row)?;
        if approval.workflow_id != workflow_id
            || approval.key != key
            || approval.scope != owner.scope
        {
            return Err(corrupt("approved effect identity").into());
        }
        if approval.status != ApprovalStatus::Approved
            || approval.resumed_activation_id.as_deref() != Some(&owner.task_id)
        {
            return Err(ContractError::ObsoleteOperation.into());
        }
        let saved_lookup: Option<String> = row.try_get("effect_lookup")?;
        if saved_lookup.as_deref().is_some_and(|saved| saved != lookup) {
            return Err(ContractError::Conflict.into());
        }
        let bytes: Option<Vec<u8>> = row.try_get("record_bytes")?;
        let attempt: Option<String> = row.try_get("attempt_id")?;
        Ok(Self {
            approval,
            step_key: row.try_get("step_key")?,
            claimed: row.try_get("effect_dispatch_claimed")?,
            state: row.try_get("effect_evidence_state")?,
            lookup: saved_lookup,
            confirmed_attempt: row.try_get("effect_confirmed_attempt_id")?,
            prior: bytes.zip(attempt),
        })
    }

    fn confirmed(&self) -> bool {
        matches!(
            self.state.as_deref(),
            Some("confirmed" | "legacy_confirmed")
        )
    }
}

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
        let evidence = Evidence::load(
            &mut tx,
            &workflow,
            owner,
            &command.workflow_id,
            &command.approval_key,
            &command.lookup,
        )
        .await?;
        if !evidence.approval.action.matches(&command.action)?
            || (evidence.prior.is_some() && !evidence.confirmed())
        {
            return Err(ContractError::Conflict.into());
        }

        // An older approved occurrence may already be lookup-only, even when
        // there is no identifiable dispatch attempt. Preserve that provenance.
        if evidence.lookup.is_none() {
            let updated = sqlx::query(
                "UPDATE workflow_approvals
                 SET effect_dispatch_attempt_id=CASE WHEN NOT effect_dispatch_claimed THEN $3 ELSE effect_dispatch_attempt_id END,
                     effect_dispatch_claimed=true,effect_evidence_state='open',effect_step_key=$4,effect_lookup=$5
                 WHERE workflow_id=$1 AND wait_key=$2 AND effect_dispatch_claimed=$6 AND effect_lookup IS NULL",
            )
            .bind(&command.workflow_id).bind(&command.approval_key).bind(&owner.attempt_id)
            .bind(&evidence.step_key).bind(&command.lookup).bind(evidence.claimed).execute(&mut *tx).await?;
            if updated.rows_affected() != 1 {
                return Err(corrupt("approved effect evidence update").into());
            }
        }
        execution::check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
        tx.commit().await?;
        Ok(ApprovedEffectClaimReceipt {
            scope: evidence.approval.scope,
            workflow_id: evidence.approval.workflow_id,
            activation_id: owner.task_id.clone(),
            attempt_id: owner.attempt_id.clone(),
            approval_key: evidence.approval.key,
            action: evidence.approval.action,
            lookup: command.lookup.clone(),
            disposition: if evidence.claimed {
                ApprovedEffectDisposition::LookupOnly
            } else {
                ApprovedEffectDisposition::Dispatch
            },
        })
    }

    pub(super) async fn confirm_approved_effect_once(
        &self,
        command: &ApprovedEffectConfirmCommand,
    ) -> StoreResult<ApprovedEffectConfirmReceipt> {
        command.validate()?;
        let owner = &command.owner;
        self.require_scope(&owner.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = execution::lock_activation(&mut tx, owner).await?;
        let evidence = Evidence::load(
            &mut tx,
            &workflow,
            owner,
            &command.workflow_id,
            &command.approval_key,
            &command.lookup,
        )
        .await?;
        if evidence.lookup.as_deref() != Some(&command.lookup)
            || confirmed_lookup(
                &command.record,
                &evidence.step_key,
                &evidence.approval.action,
            )? != command.lookup
        {
            return Err(ContractError::Conflict.into());
        }

        let already_accepted = evidence.confirmed();
        if already_accepted {
            let (bytes, accepting_attempt) = evidence
                .prior
                .as_ref()
                .ok_or_else(|| corrupt("missing confirmed effect journal"))?;
            if evidence.confirmed_attempt.as_ref() != Some(accepting_attempt) {
                return Err(corrupt("confirmed effect attempt").into());
            }
            let prior: LocalStepRecord = codec::decode(bytes)?;
            if !prior.matches(&command.record)? {
                return Err(ContractError::Conflict.into());
            }
            if accepting_attempt != &owner.attempt_id {
                execution::check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
            }
        } else {
            if evidence.state.as_deref() != Some("open") || evidence.prior.is_some() {
                return Err(ContractError::Conflict.into());
            }
            let now = db::now(&mut tx).await?;
            execution::check_live(&workflow, &authority, owner, now, true)?;
            execution::append_local(
                &mut tx,
                owner,
                &command.record,
                now,
                Some((&command.workflow_id, &command.approval_key)),
            )
            .await?;
            execution::check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
        }
        tx.commit().await?;
        Ok(ApprovedEffectConfirmReceipt {
            scope: owner.scope.clone(),
            workflow_id: command.workflow_id.clone(),
            activation_id: owner.task_id.clone(),
            attempt_id: owner.attempt_id.clone(),
            approval_key: command.approval_key.clone(),
            key: command.record.key.clone(),
            already_accepted,
        })
    }
}
