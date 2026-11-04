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

pub(super) struct Evidence {
    pub(super) approval: ApprovalSnapshot,
    pub(super) approval_bytes: Vec<u8>,
    dispatch_attempt: Option<String>,
    pub(super) step_key: String,
    pub(super) claimed: bool,
    pub(super) state: Option<String>,
    pub(super) lookup: Option<String>,
    pub(super) confirmed_attempt: Option<String>,
    pub(super) prior: Option<(Vec<u8>, String)>,
}

impl Evidence {
    /// Called under the workflow and task locks. Loading the journal here also
    /// prevents a fresh grant if an ordinary approved_local used the occurrence.
    pub(super) async fn load(
        tx: &mut PgConnection,
        workflow: &WorkflowSnapshot,
        owner: &LeaseOwner,
        workflow_id: &str,
        key: &str,
        lookup: &str,
    ) -> StoreResult<Self> {
        Self::load_bound(
            tx,
            workflow,
            &owner.scope,
            &owner.task_id,
            workflow_id,
            key,
            Some(lookup),
            false,
        )
        .await
    }
    pub(super) async fn load_provider(
        tx: &mut PgConnection,
        workflow: &WorkflowSnapshot,
        owner: &LeaseOwner,
        workflow_id: &str,
        key: &str,
    ) -> StoreResult<Self> {
        Self::load_bound(
            tx,
            workflow,
            &owner.scope,
            &owner.task_id,
            workflow_id,
            key,
            None,
            true,
        )
        .await
    }
    /// Only composed admission calls this after validating its retained subject.
    /// The approval's original resumed activation stays immutable across reentry.
    pub(super) async fn load_provider_occurrence(
        tx: &mut PgConnection,
        workflow: &WorkflowSnapshot,
        occurrence: &EffectOccurrence,
    ) -> StoreResult<Self> {
        Self::load_bound(
            tx,
            workflow,
            &occurrence.scope,
            &occurrence.activation_id,
            &occurrence.workflow_id,
            &occurrence.approval_key,
            None,
            true,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn load_bound(
        tx: &mut PgConnection,
        workflow: &WorkflowSnapshot,
        scope: &Scope,
        activation_id: &str,
        workflow_id: &str,
        key: &str,
        lookup: Option<&str>,
        provider: bool,
    ) -> StoreResult<Self> {
        if workflow.workflow_id != workflow_id {
            return Err(ContractError::Conflict.into());
        }
        let row = sqlx::query(
            "SELECT a.snapshot_bytes,a.effect_dispatch_claimed,a.effect_evidence_state,
                    a.effect_lookup,a.effect_confirmed_attempt_id,a.effect_dispatch_attempt_id,k.step_key,j.record_bytes,j.attempt_id
             FROM workflow_approvals a
             CROSS JOIN LATERAL (SELECT '_approval:' || encode(sha256(convert_to(a.wait_key,'UTF8')),'hex') AS step_key) k
             LEFT JOIN workflow_local_results j ON j.activation_id=$3 AND j.step_key=k.step_key
             WHERE a.workflow_id=$1 AND a.wait_key=$2",
        )
        .bind(workflow_id).bind(key).bind(activation_id).fetch_optional(tx).await?
        .ok_or(ContractError::NotFound)?;
        let approval = approvals::decode(&row)?;
        if approval.workflow_id != workflow_id || approval.key != key || &approval.scope != scope {
            return Err(corrupt("approved effect identity").into());
        }
        if approval.status != ApprovalStatus::Approved
            || approval.resumed_activation_id.as_deref() != Some(activation_id)
        {
            return Err(ContractError::ObsoleteOperation.into());
        }
        if (approval.protection == ApprovalProtection::ProviderOperation) != provider {
            return Err(ContractError::Conflict.into());
        }
        let saved_lookup: Option<String> = row.try_get("effect_lookup")?;
        if saved_lookup
            .as_deref()
            .is_some_and(|saved| Some(saved) != lookup)
        {
            return Err(ContractError::Conflict.into());
        }
        let bytes: Option<Vec<u8>> = row.try_get("record_bytes")?;
        let attempt: Option<String> = row.try_get("attempt_id")?;
        Ok(Self {
            approval_bytes: row.try_get("snapshot_bytes")?,
            dispatch_attempt: row.try_get("effect_dispatch_attempt_id")?,
            approval,
            step_key: row.try_get("step_key")?,
            claimed: row.try_get("effect_dispatch_claimed")?,
            state: row.try_get("effect_evidence_state")?,
            lookup: saved_lookup,
            confirmed_attempt: row.try_get("effect_confirmed_attempt_id")?,
            prior: bytes.zip(attempt),
        })
    }

    pub(super) fn confirmed(&self) -> bool {
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

        let budget_grant = if !evidence.claimed {
            match self
                .check_effect_budget(
                    &mut tx,
                    &workflow,
                    &authority,
                    &evidence.approval,
                    command.budget_fence,
                    command,
                )
                .await?
            {
                Ok(grant) => grant,
                Err(reason) => {
                    execution::check_live(
                        &workflow,
                        &authority,
                        owner,
                        db::now(&mut tx).await?,
                        true,
                    )?;
                    tx.commit().await?;
                    return Err(ContractError::RunBudgetDenied(reason).into());
                }
            }
        } else {
            None
        };
        grant(&mut tx, command, &evidence).await?;
        let now = db::now(&mut tx).await?;
        let mut retained = super::effect_retained::ensure(
            &mut tx,
            &command.workflow_id,
            &command.approval_key,
            now,
            None,
        )
        .await?;
        super::effect_retained::legacy_confirmation(&mut tx, &mut retained, now).await?;
        let final_now = db::now(&mut tx).await?;
        execution::check_live(&workflow, &authority, owner, final_now, true)?;
        crate::run_budget::check_grant(budget_grant, final_now)?;
        tx.commit().await?;
        Ok(ApprovedEffectClaimReceipt {
            budget_grant,
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
                Some((
                    &command.workflow_id,
                    &command.approval_key,
                    &evidence.approval_bytes,
                )),
            )
            .await?;
            execution::check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
        }
        let now = db::now(&mut tx).await?;
        let mut retained = super::effect_retained::ensure(
            &mut tx,
            &command.workflow_id,
            &command.approval_key,
            now,
            None,
        )
        .await?;
        super::effect_retained::legacy_confirmation(&mut tx, &mut retained, now).await?;
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

pub(super) async fn grant(
    connection: &mut PgConnection,
    command: &ApprovedEffectClaimCommand,
    evidence: &Evidence,
) -> StoreResult<()> {
    grant_bound(
        connection,
        &command.workflow_id,
        &command.approval_key,
        &command.owner.attempt_id,
        Some(&command.lookup),
        evidence,
    )
    .await
}
pub(super) async fn grant_bound(
    connection: &mut PgConnection,
    workflow_id: &str,
    approval_key: &str,
    attempt_id: &str,
    lookup: Option<&str>,
    evidence: &Evidence,
) -> StoreResult<()> {
    if evidence.lookup.is_none() && (!evidence.claimed || lookup.is_some()) {
        let expected_attempt = if evidence.claimed {
            evidence.dispatch_attempt.as_deref()
        } else {
            Some(attempt_id)
        };
        let updated:Option<bool> = sqlx::query_scalar(
                "UPDATE workflow_approvals
                 SET effect_dispatch_attempt_id=CASE WHEN NOT effect_dispatch_claimed THEN $3 ELSE effect_dispatch_attempt_id END,
                     effect_dispatch_claimed=true,effect_evidence_state='open',effect_step_key=$4,effect_lookup=$5
                 WHERE workflow_id=$1 AND wait_key=$2 AND effect_dispatch_claimed=$6 AND effect_lookup IS NULL
                 RETURNING workflow_id=$1 AND wait_key=$2 AND effect_dispatch_claimed AND effect_evidence_state='open'
                    AND effect_step_key=$4 AND effect_lookup IS NOT DISTINCT FROM $5 AND effect_dispatch_attempt_id IS NOT DISTINCT FROM $7 AND snapshot_bytes=$8",
            )
            .bind(workflow_id).bind(approval_key).bind(attempt_id)
            .bind(&evidence.step_key).bind(lookup).bind(evidence.claimed).bind(expected_attempt).bind(&evidence.approval_bytes)
            .fetch_optional(&mut *connection).await?;
        if updated != Some(true) {
            return Err(corrupt("approved effect evidence update").into());
        }
    }

    Ok(())
}

impl PostgresStore {
    pub(super) async fn check_effect_budget<T: serde::Serialize>(
        &self,
        tx: &mut PgConnection,
        workflow: &WorkflowSnapshot,
        authority: &execution::ActivationAuthority,
        reviewed: &ApprovalSnapshot,
        negotiated: bool,
        command: &T,
    ) -> StoreResult<std::result::Result<Option<RunBudgetGrant>, RunBudgetDenial>> {
        self.check_dispatch_quarantine()?;
        if !authority.has_run_budget {
            return Ok(Ok(None));
        }
        self.admission_configured()?;
        if !negotiated {
            return Err(ContractError::InvalidInput(
                "worker must support run budget dispatch fences".into(),
            )
            .into());
        }
        let member = crate::run_budget::workflow(&workflow.workflow_id);
        let approval = EffectOccurrence {
            scope: reviewed.scope.clone(),
            workflow_id: reviewed.workflow_id.clone(),
            activation_id: reviewed.activation_id.clone(),
            approval_key: reviewed.key.clone(),
        };
        let source_id = crate::run_budget::effect_source(tx, &approval).await?;
        match crate::run_budget::call(tx, &workflow.scope, &member, &source_id, Some(&approval))
            .await?
        {
            Ok((at, deadline)) => Ok(Ok(Some(RunBudgetGrant::new(
                at,
                deadline.unwrap_or(u64::MAX).min(authority.valid_until()),
            )?))),
            Err(reason) => {
                let reason = crate::run_budget::bound_call_refusal(
                    tx,
                    &workflow.scope,
                    &member,
                    &source_id,
                    reason,
                    command,
                    &approval,
                )
                .await?;
                Ok(Err(reason))
            }
        }
    }
}
