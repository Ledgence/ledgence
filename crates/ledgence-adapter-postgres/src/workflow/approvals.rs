//! A review decision is bound to a persisted immutable action. Workflow locking
//! orders proposal creation, review, deadlines, cancellation and logical consume.
use super::*;

impl PostgresStore {
    pub(super) async fn approval_once(
        &self,
        scope: &Scope,
        workflow: &str,
        key: &str,
    ) -> StoreResult<ApprovalSnapshot> {
        self.require_scope(scope)?;
        validate_text(workflow, 128)?;
        validate_text(key, 128)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        require_visible(&mut tx, scope, workflow).await?;
        let approval = load(&mut tx, workflow, key).await?;
        if &approval.scope != scope {
            return Err(corrupt("approval scope").into());
        }
        tx.commit().await?;
        Ok(approval)
    }
    pub(super) async fn list_approvals_once(
        &self,
        scope: &Scope,
        workflow: &str,
        after: Option<&str>,
        limit: u32,
    ) -> StoreResult<ApprovalPage> {
        self.require_scope(scope)?;
        validate_text(workflow, 128)?;
        validate_approval_page(after, limit)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        require_visible(&mut tx, scope, workflow).await?;
        let rows = sqlx::query("SELECT snapshot_bytes FROM workflow_approvals WHERE workflow_id=$1 AND ($2::text IS NULL OR wait_key COLLATE \"C\">$2 COLLATE \"C\") ORDER BY wait_key COLLATE \"C\" LIMIT $3")
            .bind(workflow).bind(after).bind(i64::from(limit) + 1).fetch_all(&mut *tx).await?;
        let mut items = rows.iter().map(decode).collect::<StoreResult<Vec<_>>>()?;
        let more = items.len() > limit as usize;
        items.truncate(limit as usize);
        let next_cursor = more.then(|| items.last().expect("nonempty page").key.clone());
        let page = ApprovalPage { items, next_cursor };
        page.validate().map_err(|_| corrupt("approval page"))?;
        if !page.matches(scope, workflow, after, limit) {
            return Err(corrupt("approval page identity").into());
        }
        tx.commit().await?;
        Ok(page)
    }
    pub(super) async fn decide_approval_once(
        &self,
        command: &ApprovalDecisionCommand,
    ) -> StoreResult<ApprovalDecisionReceipt> {
        if self.instance_access.get().is_some() {
            return Err(ContractError::Forbidden.into());
        }
        Ok(self
            .decide_approval_transaction(command, None)
            .await?
            .receipt)
    }
    pub(crate) async fn decide_approval_audited_once(
        &self,
        principal: &VerifiedPrincipal,
        command: &ApprovalDecisionCommand,
        request: &OperatorRequest,
    ) -> StoreResult<AuditedDecision> {
        self.decide_approval_transaction(command, Some((principal, request)))
            .await
    }
    async fn decide_approval_transaction(
        &self,
        command: &ApprovalDecisionCommand,
        operator: Option<(&VerifiedPrincipal, &OperatorRequest)>,
    ) -> StoreResult<AuditedDecision> {
        command.validate()?;
        self.require_scope(&command.scope)?;
        if let Some((principal, _)) = operator {
            principal.require(AccessCapability::ApprovalDecide)?;
            principal.require(AccessCapability::SensitiveRead)?;
            if command.scope != principal.audience().scope {
                return Err(ContractError::Forbidden.into());
            }
        }
        let bytes = codec::encode(command)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let run=sqlx::query("SELECT state,external_wait_key,wait_activation_id,retiring_at_ms FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR NO KEY UPDATE")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.workflow_id)
            .fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        codec::visible(&run)?;
        // Authority and expiry are evaluated after acquiring workflow ownership,
        // not before a potentially long wait for the row lock.
        let now = db::now(&mut tx).await?;
        if let Some((principal, _)) = operator {
            self.require_operator(principal, AccessCapability::ApprovalDecide, now)?;
        }
        let select = if operator.is_some() {
            "SELECT snapshot_bytes,decision_bytes,decision_audit_event_id FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2"
        } else {
            "SELECT snapshot_bytes,decision_bytes FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2"
        };
        let row = sqlx::query(select)
            .bind(&command.workflow_id)
            .bind(&command.key)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ContractError::NotFound)?;
        let audit_id = if operator.is_some() {
            row.try_get::<Option<String>, _>("decision_audit_event_id")?
        } else {
            None
        };
        let mut approval = decode(&row)?;
        if approval.scope != command.scope
            || approval.workflow_id != command.workflow_id
            || approval.key != command.key
        {
            return Err(corrupt("approval identity").into());
        }
        // Replay uses the immutable original command bytes before testing the
        // current deadline/status, including completed and cancelled workflows.
        if let Some(previous) = row.try_get::<Option<Vec<u8>>, _>("decision_bytes")? {
            if previous != bytes {
                return Err(ContractError::Conflict.into());
            }
            let receipt = ApprovalDecisionReceipt {
                approval,
                already_accepted: true,
            };
            let attribution = if let Some(id) = &audit_id {
                let (principal, _) = operator.ok_or(ContractError::Forbidden)?;
                let event = crate::operator_audit::load_acceptance(&mut tx, id, &bytes).await?;
                if !event.actor.same_actor(principal) {
                    return Err(ContractError::Conflict.into());
                }
                event.require_acceptance(principal, command, &receipt)?;
                DecisionAttribution::Verified {
                    event: Box::new(event),
                }
            } else {
                DecisionAttribution::LegacyUnverified
            };
            let result = AuditedDecision {
                receipt,
                attribution,
            };
            if let Some((principal, _)) = operator {
                result.validate_for(principal, command)?;
            }
            tx.commit().await?;
            return Ok(result);
        }
        if audit_id.is_some() {
            return Err(corrupt("approval audit without decision").into());
        }
        if !approval.matches(command)? {
            return Err(ContractError::Conflict.into());
        }
        if approval.status != ApprovalStatus::Pending
            || now >= approval.deadline
            || run.try_get::<String, _>("state")? != "waiting"
            || run
                .try_get::<Option<String>, _>("external_wait_key")?
                .as_deref()
                != Some(command.key.as_str())
            || run
                .try_get::<Option<String>, _>("wait_activation_id")?
                .as_deref()
                != Some(command.activation_id.as_str())
        {
            return Err(ContractError::ObsoleteOperation.into());
        }
        approval.status = match command.decision {
            ApprovalDecision::Approve => ApprovalStatus::Approved,
            ApprovalDecision::Reject => ApprovalStatus::Rejected,
        };
        approval.decision = Some(ApprovalDecisionRecord {
            decision_id: command.decision_id.clone(),
            decision: command.decision,
            reviewer: command.reviewer.clone(),
            reason: command.reason.clone(),
            decided_at: now,
        });
        approval.validate()?;
        let attribution = if let Some((principal, request)) = operator {
            let event = AuditEvent {
                event_id: db::id(&mut tx, "aud").await?,
                recorded_at: now,
                audience: principal.audience().clone(),
                actor: AuditActor::from_principal(principal),
                operation: AuditOperation::accepted(command),
            };
            crate::operator_audit::insert_event(
                &mut tx,
                &event,
                Some(&bytes),
                request.request_id(),
                false,
            )
            .await?;
            DecisionAttribution::Verified {
                event: Box::new(event),
            }
        } else {
            DecisionAttribution::LegacyUnverified
        };
        let audit_id = match &attribution {
            DecisionAttribution::Verified { event } => Some(event.event_id.as_str()),
            DecisionAttribution::LegacyUnverified => None,
        };
        let affected = if operator.is_some() {
            sqlx::query("UPDATE workflow_approvals SET snapshot_bytes=$3,decision_bytes=$4,decision_audit_event_id=$5 WHERE workflow_id=$1 AND wait_key=$2")
                .bind(&command.workflow_id).bind(&command.key).bind(codec::encode(&approval)?).bind(&bytes).bind(audit_id).execute(&mut *tx).await?.rows_affected()
        } else {
            sqlx::query("UPDATE workflow_approvals SET snapshot_bytes=$3,decision_bytes=$4 WHERE workflow_id=$1 AND wait_key=$2")
                .bind(&command.workflow_id).bind(&command.key).bind(codec::encode(&approval)?).bind(&bytes).execute(&mut *tx).await?.rows_affected()
        };
        if affected != 1 {
            return Err(corrupt("approval decision was not persisted").into());
        }
        // A pending approval always owns a delayed wake. Require exactly one
        // durable wake; triggers or broken storage must never acknowledge loss.
        let affected=sqlx::query("UPDATE workflow_work SET available_at_ms=LEAST(available_at_ms,$2) WHERE id='wait:'||$1 AND processed_at_ms IS NULL")
            .bind(&command.activation_id).bind(codec::ms(now)?).execute(&mut *tx).await?.rows_affected();
        if affected != 1 {
            return Err(corrupt("approval wake was not persisted").into());
        }
        record_history(
            &mut tx,
            &command.workflow_id,
            Some(&command.activation_id),
            now,
            match command.decision {
                ApprovalDecision::Approve => "approval_approved",
                ApprovalDecision::Reject => "approval_rejected",
            },
        )
        .await?;
        let result = AuditedDecision {
            receipt: ApprovalDecisionReceipt {
                approval,
                already_accepted: false,
            },
            attribution,
        };
        if let Some((principal, _)) = operator {
            result.validate_for(principal, command)?;
        }
        tx.commit().await?;
        Ok(result)
    }
}

async fn require_visible(
    connection: &mut PgConnection,
    scope: &Scope,
    workflow: &str,
) -> StoreResult<()> {
    // Share locks permit concurrent inspection but exclude decision/cancellation,
    // resumption and retirement while reading authoritative persisted status.
    // Reads never infer a terminal transition merely from the current clock.
    let row = sqlx::query("SELECT retiring_at_ms FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR SHARE")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(workflow).fetch_optional(connection).await?.ok_or(ContractError::NotFound)?;
    codec::visible(&row)?;
    Ok(())
}
pub(super) fn decode(row: &PgRow) -> StoreResult<ApprovalSnapshot> {
    let approval: ApprovalSnapshot = codec::decode(&row.try_get::<Vec<u8>, _>("snapshot_bytes")?)?;
    approval
        .validate()
        .map_err(|_| corrupt("approval snapshot"))?;
    Ok(approval)
}
pub(super) async fn load(
    connection: &mut PgConnection,
    workflow: &str,
    key: &str,
) -> StoreResult<ApprovalSnapshot> {
    let row = sqlx::query(
        "SELECT snapshot_bytes FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2",
    )
    .bind(workflow)
    .bind(key)
    .fetch_optional(connection)
    .await?
    .ok_or(ContractError::NotFound)?;
    let approval = decode(&row)?;
    if approval.workflow_id != workflow || approval.key != key {
        return Err(corrupt("approval identity").into());
    }
    Ok(approval)
}
pub(super) async fn save(
    connection: &mut PgConnection,
    approval: &ApprovalSnapshot,
) -> StoreResult<()> {
    approval.validate()?;
    sqlx::query(
        "UPDATE workflow_approvals SET snapshot_bytes=$3 WHERE workflow_id=$1 AND wait_key=$2",
    )
    .bind(&approval.workflow_id)
    .bind(&approval.key)
    .bind(codec::encode(approval)?)
    .execute(connection)
    .await?;
    Ok(())
}
fn expire(approval: &mut ApprovalSnapshot, now: u64) {
    if approval.status == ApprovalStatus::Pending && now >= approval.deadline {
        approval.status = ApprovalStatus::Expired;
    }
}
pub(super) async fn install(
    connection: &mut PgConnection,
    run: &RunRecord,
    activation: &str,
    wait: &WorkflowWait,
    now: u64,
) -> StoreResult<()> {
    let WorkflowWait::Approval {
        key,
        action,
        proposed_arguments,
        protection,
        provider_binding,
        ..
    } = wait
    else {
        return Ok(());
    };
    let approval = ApprovalSnapshot {
        scope: run.snapshot.scope.clone(),
        workflow_id: run.snapshot.workflow_id.clone(),
        key: key.clone(),
        activation_id: activation.to_owned(),
        revision: run
            .snapshot
            .revision
            .checked_sub(1)
            .ok_or_else(|| corrupt("approval revision"))?,
        action: action.clone(),
        protection: *protection,
        provider_binding: provider_binding
            .as_ref()
            .map(|b| b.resolve(run.controller.clone()))
            .transpose()?,
        proposed_arguments: proposed_arguments.clone(),
        created_at: now,
        deadline: wait
            .deadline(now)?
            .ok_or_else(|| corrupt("approval deadline"))?,
        status: ApprovalStatus::Pending,
        decision: None,
        resumed_activation_id: None,
    };
    approval.validate()?;
    if let Some(binding) = &approval.provider_binding
        && binding.reconciliation.queue == run.submission.input.queue
    {
        return Err(invalid("provider reconciliation requires a separate queue").into());
    }
    let valid: Option<bool> = sqlx::query_scalar("INSERT INTO workflow_approvals(workflow_id,wait_key,snapshot_bytes,provider_operation) VALUES($1,$2,$3,$4) RETURNING workflow_id=$1 AND wait_key=$2 AND snapshot_bytes=$3 AND provider_operation=$4")
        .bind(&approval.workflow_id).bind(&approval.key).bind(codec::encode(&approval)?)
        .bind(approval.provider_binding.is_some()).fetch_optional(connection).await?;
    if valid != Some(true) {
        return Err(corrupt("approval proposal changed or suppressed").into());
    }
    Ok(())
}
pub(super) async fn select_wake(
    connection: &mut PgConnection,
    workflow: &str,
    key: &str,
    now: u64,
) -> StoreResult<Option<WorkflowWake>> {
    let mut approval = load(connection, workflow, key).await?;
    expire(&mut approval, now);
    if approval.status == ApprovalStatus::Pending {
        return Ok(None);
    }
    if approval.resumed_activation_id.is_some() {
        return Err(corrupt("approval consumed twice").into());
    }
    Ok(Some(WorkflowWake::Approval {
        approval: Box::new(approval),
    }))
}
pub(super) async fn close(
    connection: &mut PgConnection,
    workflow: &str,
    key: &str,
    now: u64,
) -> StoreResult<()> {
    let kind: String =
        sqlx::query_scalar("SELECT kind FROM workflow_waits WHERE workflow_id=$1 AND wait_key=$2")
            .bind(workflow)
            .bind(key)
            .fetch_one(&mut *connection)
            .await?;
    if kind != "approval" {
        return Ok(());
    }
    let mut approval = load(connection, workflow, key).await?;
    expire(&mut approval, now);
    if approval.status == ApprovalStatus::Pending {
        approval.status = ApprovalStatus::Cancelled;
    }
    save(connection, &approval).await
}
