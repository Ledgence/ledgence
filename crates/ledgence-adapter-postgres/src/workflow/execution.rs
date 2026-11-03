use super::*;

impl PostgresStore {
    pub(super) async fn activation_context_once(
        &self,
        owner: &LeaseOwner,
    ) -> StoreResult<WorkflowActivationContext> {
        self.require_scope(&owner.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = lock_activation(&mut tx, owner).await?;
        check_live(&workflow, &authority, owner, db::now(&mut tx).await?, false)?;
        let bytes: Vec<u8> = sqlx::query_scalar("SELECT context_bytes FROM workflow_activations WHERE activation_id=$1 AND workflow_id=$2")
            .bind(&owner.task_id).bind(&workflow.workflow_id).fetch_one(&mut *tx).await?;
        let mut context: WorkflowActivationContext = codec::decode(&bytes)?;
        if context.workflow_id != workflow.workflow_id
            || context.activation_id != owner.task_id
            || context.revision != workflow.revision
            || context.parent_workflow_id != workflow.parent_workflow_id
            || context.root_workflow_id != workflow.root_workflow_id
        {
            return Err(corrupt("activation context identity").into());
        }
        let rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT record_bytes FROM workflow_local_results WHERE activation_id=$1 ORDER BY step_key LIMIT 129")
            .bind(&owner.task_id).fetch_all(&mut *tx).await?;
        context.local_steps = rows
            .iter()
            .map(|bytes| codec::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        context
            .validate()
            .map_err(|_| corrupt("activation context"))?;
        tx.commit().await?;
        Ok(context)
    }

    pub(super) async fn record_local_result_once(
        &self,
        command: &LocalResultCommand,
    ) -> StoreResult<LocalResultReceipt> {
        command.record.validate()?;
        let owner = &command.owner;
        self.require_scope(&command.owner.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = lock_activation(&mut tx, owner).await?;
        let prior = if command.record.key.starts_with("_approval:") {
            reserved_local_prior(&mut tx, &workflow, owner, &command.record).await?
        } else {
            sqlx::query_as("SELECT record_bytes,attempt_id FROM workflow_local_results WHERE activation_id=$1 AND step_key=$2")
                .bind(&owner.task_id).bind(&command.record.key).fetch_optional(&mut *tx).await?
        };
        if let Some((bytes, accepting_attempt)) = prior {
            let record: LocalStepRecord = codec::decode(&bytes)?;
            if !record.matches(&command.record)? {
                return Err(ContractError::Conflict.into());
            }
            if accepting_attempt != owner.attempt_id {
                check_live(&workflow, &authority, owner, db::now(&mut tx).await?, true)?;
            }
            // An exact receipt is durable independently of the current lease,
            // workflow cancellation, or a later successful continuation.
            tx.commit().await?;
            return Ok(LocalResultReceipt {
                key: record.key,
                already_accepted: true,
            });
        }
        let now = db::now(&mut tx).await?;
        check_live(&workflow, &authority, owner, now, true)?;
        append_local(&mut tx, owner, &command.record, now, None).await?;
        tx.commit().await?;
        Ok(LocalResultReceipt {
            key: command.record.key.clone(),
            already_accepted: false,
        })
    }
}

/// Generic commits must never manufacture a protected journal record, including
/// before a worker has claimed the occurrence. Non-reserved local steps use
/// their original single indexed journal lookup.
async fn reserved_local_prior(
    connection: &mut PgConnection,
    workflow: &WorkflowSnapshot,
    owner: &LeaseOwner,
    record: &LocalStepRecord,
) -> StoreResult<Option<(Vec<u8>, String)>> {
    let row = sqlx::query(
        "SELECT j.record_bytes,j.attempt_id,
                EXISTS(SELECT 1 FROM workflow_approvals WHERE workflow_id=$3 AND effect_step_key=$2) AS protected,
                CASE WHEN $4 THEN a.context_bytes END AS context_bytes
         FROM workflow_activations a
         LEFT JOIN workflow_local_results j ON j.activation_id=a.activation_id AND j.step_key=$2
         WHERE a.activation_id=$1",
    )
    .bind(&owner.task_id).bind(&record.key).bind(&workflow.workflow_id)
    .bind(record.input.get("action_json").is_some()).fetch_one(connection).await?;
    if row.try_get::<bool, _>("protected")? {
        return Err(ContractError::Conflict.into());
    }
    if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("context_bytes")? {
        let ctx: WorkflowActivationContext = codec::decode(&bytes)?;
        if let Some(WorkflowWake::Approval { approval }) = ctx.wake
            && approved_effects::confirmed_lookup(record, &record.key, &approval.action).is_ok()
        {
            return Err(ContractError::Conflict.into());
        }
    }
    let bytes: Option<Vec<u8>> = row.try_get("record_bytes")?;
    Ok(bytes.zip(row.try_get("attempt_id")?))
}

/// Insert the bounded local journal, graph discovery row and (only for a typed
/// confirmation) close its retention obligation in the caller's transaction.
pub(super) async fn append_local(
    connection: &mut PgConnection,
    owner: &LeaseOwner,
    record: &LocalStepRecord,
    now: u64,
    effect: Option<(&str, &str)>,
) -> StoreResult<()> {
    let bytes = codec::encode(record)?;
    let (count, total): (i64, i64) = sqlx::query_as("SELECT count(*)::bigint,coalesce(sum(octet_length(record_bytes)),0)::bigint FROM workflow_local_results WHERE activation_id=$1")
        .bind(&owner.task_id).fetch_one(&mut *connection).await?;
    // Canonical array encoding adds two brackets and one comma per existing
    // element. The workflow lock serializes this bound with concurrent steps.
    if count >= WORKFLOW_MAX_LOCAL_STEPS as i64
        || total + bytes.len() as i64 + count + 2 > WORKFLOW_LOCAL_LEDGER_MAX_BYTES as i64
    {
        return Err(invalid("local step ledger exceeds supported limit").into());
    }
    let sql = if effect.is_some() {
        "WITH accepted AS (
            INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms,callable)
            VALUES($1,$2,$3,$4,$5,$6) RETURNING attempt_id)
         UPDATE workflow_approvals SET effect_evidence_state='confirmed',
                effect_confirmed_attempt_id=accepted.attempt_id FROM accepted
         WHERE workflow_id=$7 AND wait_key=$8 AND effect_evidence_state='open'"
    } else {
        "INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms,callable)
         VALUES($1,$2,$3,$4,$5,$6)"
    };
    let mut query = sqlx::query(sql)
        .bind(&owner.task_id)
        .bind(&record.key)
        .bind(bytes)
        .bind(&owner.attempt_id)
        .bind(codec::ms(now)?)
        .bind(&record.callable);
    if let Some((workflow, approval)) = effect {
        query = query.bind(workflow).bind(approval);
    }
    if query.execute(&mut *connection).await?.rows_affected() != 1 {
        return Err(ContractError::Conflict.into());
    }
    explorer::local(connection, owner, record, now).await?;
    Ok(())
}

/// The first membership lookup is immutable and unlocked. All mutable checks
/// happen after workflow -> task locks, so cancellation and attempt replacement
/// cannot race a newly acknowledged result into an obsolete continuation.
pub(super) struct ActivationAuthority {
    task_state: TaskState,
    current_attempt_id: Option<String>,
    cancellation_requested: bool,
    attempt_state: AttemptState,
    settled: bool,
    expires_at: u64,
    authority_deadline: u64,
    execution_deadline: u64,
    execution_may_have_started: bool,
}
pub(super) async fn lock_activation(
    connection: &mut PgConnection,
    owner: &LeaseOwner,
) -> StoreResult<(WorkflowSnapshot, ActivationAuthority)> {
    owner.scope.validate()?;
    validate_text(&owner.task_id, 128)?;
    validate_text(&owner.attempt_id, 128)?;
    let workflow_id: String = sqlx::query_scalar(
        "SELECT workflow_id FROM workflow_task_links WHERE task_id=$1 AND is_activation",
    )
    .bind(&owner.task_id)
    .fetch_optional(&mut *connection)
    .await?
    .ok_or(ContractError::OwnershipLost)?;
    let row = sqlx::query("SELECT retiring_at_ms,workflow_id,parent_workflow_id,root_workflow_id,tenant_id,namespace,state,trunc(revision)::text AS revision_text,current_activation_id,submitted_at_ms,terminal_at_ms,correlation_key FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR NO KEY UPDATE")
        .bind(&owner.scope.tenant_id).bind(&owner.scope.namespace).bind(&workflow_id).fetch_optional(&mut *connection).await?.ok_or(ContractError::NotFound)?;
    let workflow = status_record(&row)?;
    // Only authority metadata is read: local receipts must not decode or transfer
    // task input, frozen inputs, checkpoint, event or accepted report payloads.
    let task = sqlx::query("SELECT workflow_id,workflow_activation_id,state,current_attempt_id,cancel_requested_at_ms FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 FOR NO KEY UPDATE")
        .bind(&owner.scope.tenant_id).bind(&owner.scope.namespace).bind(&owner.task_id)
        .fetch_optional(&mut *connection).await?.ok_or(ContractError::OwnershipLost)?;
    if task
        .try_get::<Option<String>, _>("workflow_activation_id")?
        .as_deref()
        != Some(&owner.task_id)
        || task.try_get::<Option<String>, _>("workflow_id")?.as_deref() != Some(&workflow_id)
    {
        return Err(ContractError::OwnershipLost.into());
    }
    // A READ COMMITTED statement retains its snapshot while waiting for a lock.
    // Read attempt and settlement in a fresh statement after acquiring the task,
    // which serializes renewals, settlements and interactive workflow mutations.
    let row = sqlx::query("SELECT a.state AS attempt_state,a.lease_id,a.generation,a.worker_session_id,a.consumer_id,a.expires_at_ms,a.authority_deadline_ms,a.deadline_ms,a.execution_may_have_started,EXISTS(SELECT 1 FROM accepted_settlements s WHERE s.attempt_id=a.attempt_id) AS settled FROM attempts a WHERE a.task_id=$1 AND a.attempt_id=$2")
        .bind(&owner.task_id).bind(&owner.attempt_id)
        .fetch_optional(&mut *connection).await?.ok_or(ContractError::OwnershipLost)?;
    if row.try_get::<String, _>("lease_id")? != owner.lease_id
        || row.try_get::<i64, _>("generation")? != i64::from(owner.generation)
        || row.try_get::<String, _>("worker_session_id")? != owner.worker_session_id
        || row.try_get::<i64, _>("consumer_id")? != i64::from(owner.consumer_id)
    {
        return Err(ContractError::OwnershipLost.into());
    }
    let timestamp = |name| -> StoreResult<u64> {
        u64::try_from(row.try_get::<i64, _>(name)?).map_err(|_| corrupt(name).into())
    };
    Ok((
        workflow,
        ActivationAuthority {
            task_state: serde_json::from_value(Value::String(task.try_get("state")?))
                .map_err(|_| corrupt("task state"))?,
            current_attempt_id: task.try_get("current_attempt_id")?,
            cancellation_requested: task
                .try_get::<Option<i64>, _>("cancel_requested_at_ms")?
                .is_some(),
            attempt_state: serde_json::from_value(Value::String(row.try_get("attempt_state")?))
                .map_err(|_| corrupt("attempt state"))?,
            settled: row.try_get("settled")?,
            expires_at: timestamp("expires_at_ms")?,
            authority_deadline: timestamp("authority_deadline_ms")?,
            execution_deadline: timestamp("deadline_ms")?,
            execution_may_have_started: row.try_get("execution_may_have_started")?,
        },
    ))
}
pub(super) fn check_live(
    workflow: &WorkflowSnapshot,
    authority: &ActivationAuthority,
    owner: &LeaseOwner,
    now: u64,
    require_dispatch: bool,
) -> StoreResult<()> {
    if workflow.state != WorkflowState::Running
        || workflow.activation_id.as_deref() != Some(&owner.task_id)
        || authority.task_state != TaskState::Active
        || authority.current_attempt_id.as_deref() != Some(&owner.attempt_id)
        || authority.cancellation_requested
        || authority.attempt_state != AttemptState::Active
        || authority.settled
        || now >= authority.expires_at
        || now >= authority.authority_deadline
        || now >= authority.execution_deadline
        || (require_dispatch && !authority.execution_may_have_started)
    {
        return Err(ContractError::OwnershipLost.into());
    }
    Ok(())
}
