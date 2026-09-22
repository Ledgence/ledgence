use super::*;

impl PostgresStore {
    pub(super) async fn activation_context_once(
        &self,
        owner: &LeaseOwner,
    ) -> StoreResult<WorkflowActivationContext> {
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
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = lock_activation(&mut tx, owner).await?;
        let prior: Option<(Vec<u8>, String)> = sqlx::query_as("SELECT record_bytes,attempt_id FROM workflow_local_results WHERE activation_id=$1 AND step_key=$2")
            .bind(&owner.task_id).bind(&command.record.key).fetch_optional(&mut *tx).await?;
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
        let bytes = codec::encode(&command.record)?;
        let (count, total): (i64, i64) = sqlx::query_as("SELECT count(*)::bigint,coalesce(sum(octet_length(record_bytes)),0)::bigint FROM workflow_local_results WHERE activation_id=$1")
            .bind(&owner.task_id).fetch_one(&mut *tx).await?;
        // Canonical array encoding adds two brackets and one comma per existing
        // element. The workflow lock serializes this bound with concurrent steps.
        if count >= WORKFLOW_MAX_LOCAL_STEPS as i64
            || total + bytes.len() as i64 + count + 2 > WORKFLOW_LOCAL_LEDGER_MAX_BYTES as i64
        {
            return Err(invalid("local step ledger exceeds supported limit").into());
        }
        sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms) VALUES($1,$2,$3,$4,$5)")
            .bind(&owner.task_id).bind(&command.record.key).bind(bytes).bind(&owner.attempt_id).bind(codec::ms(now)?)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(LocalResultReceipt {
            key: command.record.key.clone(),
            already_accepted: false,
        })
    }
}

/// The first membership lookup is immutable and unlocked. All mutable checks
/// happen after workflow -> task locks, so cancellation and attempt replacement
/// cannot race a newly acknowledged result into an obsolete continuation.
struct ActivationAuthority {
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
async fn lock_activation(
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
    let row = sqlx::query("SELECT t.workflow_id,t.workflow_activation_id,t.state AS task_state,t.current_attempt_id,t.cancel_requested_at_ms,a.state AS attempt_state,a.lease_id,a.generation,a.worker_session_id,a.consumer_id,a.expires_at_ms,a.authority_deadline_ms,a.deadline_ms,a.execution_may_have_started,EXISTS(SELECT 1 FROM accepted_settlements s WHERE s.attempt_id=a.attempt_id) AS settled FROM tasks t JOIN attempts a ON a.task_id=t.task_id WHERE t.tenant_id=$1 AND t.namespace=$2 AND t.task_id=$3 AND a.attempt_id=$4 FOR NO KEY UPDATE OF t")
        .bind(&owner.scope.tenant_id).bind(&owner.scope.namespace).bind(&owner.task_id).bind(&owner.attempt_id).fetch_optional(&mut *connection).await?.ok_or(ContractError::OwnershipLost)?;
    if row
        .try_get::<Option<String>, _>("workflow_activation_id")?
        .as_deref()
        != Some(&owner.task_id)
        || row.try_get::<Option<String>, _>("workflow_id")?.as_deref() != Some(&workflow_id)
        || row.try_get::<String, _>("lease_id")? != owner.lease_id
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
            task_state: serde_json::from_value(Value::String(row.try_get("task_state")?))
                .map_err(|_| corrupt("task state"))?,
            current_attempt_id: row.try_get("current_attempt_id")?,
            cancellation_requested: row
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
fn check_live(
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
