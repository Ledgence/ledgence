use super::*;

pub(super) struct RunRecord {
    pub snapshot: WorkflowSnapshot,
    pub submission: SubmitCommand,
    pub controller: ProgramDescriptor,
    pub continuation: String,
    pub checkpoint: Value,
    pub wait_activation: Option<String>,
    pub wait_keys: Vec<String>,
    pub external_wait_key: Option<String>,
    pub outcome: Option<WorkflowOutcome>,
}
impl RunRecord {
    pub fn state_terminal(&self) -> bool {
        self.snapshot.state.is_terminal()
    }
}
pub(super) fn corrupt(label: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid workflow persistence: {label}"))
}
pub(super) fn invalid(label: &str) -> ContractError {
    ContractError::InvalidInput(label.into())
}
pub(super) fn command_scope(command: &SubmitCommand) -> Scope {
    Scope {
        tenant_id: command.input.tenant_id.clone(),
        namespace: command.input.namespace.clone(),
    }
}
pub(super) fn status_record(row: &PgRow) -> StoreResult<WorkflowSnapshot> {
    let snapshot = snapshot_record(row)?;
    snapshot.validate().map_err(|_| corrupt("snapshot"))?;
    Ok(snapshot)
}
fn snapshot_record(row: &PgRow) -> StoreResult<WorkflowSnapshot> {
    Ok(WorkflowSnapshot {
        workflow_id: row.try_get("workflow_id")?,
        scope: Scope {
            tenant_id: row.try_get("tenant_id")?,
            namespace: row.try_get("namespace")?,
        },
        state: serde_json::from_value(Value::String(row.try_get("state")?))
            .map_err(|_| corrupt("state"))?,
        revision: row
            .try_get::<String, _>("revision_text")?
            .parse()
            .map_err(|_| corrupt("revision"))?,
        activation_id: row.try_get("current_activation_id")?,
        submitted_at: u64::try_from(row.try_get::<i64, _>("submitted_at_ms")?)
            .map_err(|_| corrupt("submitted timestamp"))?,
        terminal_at: row
            .try_get::<Option<i64>, _>("terminal_at_ms")?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| corrupt("terminal timestamp"))?,
        correlation_key: row.try_get("correlation_key")?,
    })
}
pub(super) fn run_record(row: &PgRow) -> StoreResult<RunRecord> {
    let submission: SubmitCommand = codec::decode(&row.try_get::<Vec<u8>, _>("submission_bytes")?)?;
    let controller: ProgramDescriptor =
        codec::decode(&row.try_get::<Vec<u8>, _>("controller_bytes")?)?;
    core::validate_submission(&submission).map_err(|_| corrupt("submission"))?;
    controller
        .validate()
        .map_err(|_| corrupt("controller descriptor"))?;
    let scope = command_scope(&submission);
    if row.try_get::<String, _>("tenant_id")? != scope.tenant_id
        || row.try_get::<String, _>("namespace")? != scope.namespace
        || row.try_get::<String, _>("idempotency_key")? != submission.idempotency_key
        || controller.program != submission.input.program
        || row.try_get::<Option<String>, _>("correlation_key")? != submission.input.correlation_key
    {
        return Err(corrupt("submission indexes").into());
    }
    let state: WorkflowState = serde_json::from_value(Value::String(row.try_get("state")?))
        .map_err(|_| corrupt("state"))?;
    let terminal_at = row
        .try_get::<Option<i64>, _>("terminal_at_ms")?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| corrupt("terminal timestamp"))?;
    let outcome = row
        .try_get::<Option<Vec<u8>>, _>("outcome_bytes")?
        .map(|b| codec::decode(&b))
        .transpose()?;
    if state.is_terminal() != terminal_at.is_some() {
        return Err(corrupt("terminal state").into());
    }
    let consistent = matches!(
        (&state, &outcome),
        (WorkflowState::Running | WorkflowState::Waiting, None)
            | (
                WorkflowState::Failing | WorkflowState::Failed,
                Some(WorkflowOutcome::Failed { .. })
            )
            | (
                WorkflowState::Cancelling | WorkflowState::Cancelled,
                Some(WorkflowOutcome::Cancelled {})
            )
            | (
                WorkflowState::Succeeded,
                Some(WorkflowOutcome::Succeeded { .. })
            )
    );
    if !consistent {
        return Err(corrupt("workflow outcome").into());
    }
    Ok(RunRecord {
        snapshot: snapshot_record(row)?,
        submission,
        controller,
        continuation: row.try_get("continuation")?,
        checkpoint: codec::decode(&row.try_get::<Vec<u8>, _>("checkpoint_bytes")?)?,
        wait_activation: row.try_get("wait_activation_id")?,
        external_wait_key: row.try_get("external_wait_key")?,
        wait_keys: row
            .try_get::<Option<Vec<u8>>, _>("wait_keys_bytes")?
            .map(|b| codec::decode(&b))
            .transpose()?
            .unwrap_or_default(),
        outcome,
    })
}
pub(super) async fn load_run(
    connection: &mut PgConnection,
    scope: &Scope,
    id: Option<&str>,
    key: Option<&str>,
    locked: bool,
) -> StoreResult<RunRecord> {
    scope.validate()?;
    if let Some(id) = id {
        validate_text(id, 128)?;
    }
    if let Some(key) = key {
        validate_text(key, 255)?;
    }
    let sql = if locked {
        "SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND (($3::text IS NOT NULL AND workflow_id=$3) OR ($3::text IS NULL AND idempotency_key=$4)) FOR NO KEY UPDATE"
    } else {
        "SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND (($3::text IS NOT NULL AND workflow_id=$3) OR ($3::text IS NULL AND idempotency_key=$4))"
    };
    let row = sqlx::query(sql)
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(id)
        .bind(key)
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    run_record(&row)
}
pub(super) fn replay_submission(run: &RunRecord, command: &SubmitCommand) -> StoreResult<()> {
    if run.submission.idempotency_key != command.idempotency_key
        || !run
            .submission
            .input
            .semantically_matches(&command.input)
            .map_err(ContractError::from)?
    {
        return Err(ContractError::Conflict.into());
    }
    Ok(())
}
pub(super) async fn save_run(connection: &mut PgConnection, run: &RunRecord) -> StoreResult<()> {
    let until = run
        .wait_activation
        .as_ref()
        .map(|_| codec::encode(&run.wait_keys))
        .transpose()?;
    sqlx::query("UPDATE workflow_runs SET state=$2,revision=($3::text)::ldg_u64,continuation=$4,checkpoint_bytes=$5,current_activation_id=$6,wait_activation_id=$7,wait_keys_bytes=$8,outcome_bytes=$9,terminal_at_ms=$10,external_wait_key=$11 WHERE workflow_id=$1")
        .bind(&run.snapshot.workflow_id).bind(codec::label(&run.snapshot.state)?).bind(run.snapshot.revision.to_string())
        .bind(&run.continuation).bind(codec::encode(&run.checkpoint)?).bind(&run.snapshot.activation_id).bind(&run.wait_activation)
        .bind(until).bind(run.outcome.as_ref().map(codec::encode).transpose()?).bind(run.snapshot.terminal_at.map(codec::ms).transpose()?).bind(&run.external_wait_key)
        .execute(connection).await?;
    Ok(())
}
pub(super) async fn record_history(
    connection: &mut PgConnection,
    id: &str,
    activation: Option<&str>,
    now: u64,
    reason: &str,
) -> StoreResult<()> {
    let sequence: String = sqlx::query_scalar("UPDATE workflow_runs SET history_sequence=history_sequence+1 WHERE workflow_id=$1 RETURNING trunc(history_sequence)::text")
        .bind(id).fetch_one(&mut *connection).await?;
    sqlx::query("INSERT INTO workflow_history(workflow_id,sequence,activation_id,at_ms,reason) VALUES($1,($2::text)::ldg_u64,$3,$4,$5)")
        .bind(id).bind(sequence).bind(activation).bind(codec::ms(now)?).bind(reason).execute(connection).await?;
    Ok(())
}

pub(super) async fn insert_task(
    connection: &mut PgConnection,
    command: &SubmitCommand,
    descriptor: &ProgramDescriptor,
    task_id: &str,
    workflow_id: &str,
    activation: bool,
    now: u64,
) -> StoreResult<TaskSnapshot> {
    let destination = crate::dispatch_intents::submission_destination(connection, command).await?;
    let run_id = db::id(connection, "run").await?;
    let mut transition = core::submit(command, descriptor, task_id, &run_id, now)?;
    transition.task.workflow_activation_id = activation.then(|| task_id.to_owned());
    transition.task.workflow_id = Some(workflow_id.to_owned());
    let task = &transition.task;
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,correlation_key,input_bytes,descriptor_bytes,origin_trace_bytes,state,submitted_at_ms,available_at_ms,attempt_count,dispatch_destination,workflow_activation_id,workflow_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'queued',$11,$11,0,$12,$13,$14)")
        .bind(&task.task_id).bind(&task.run_id).bind(&task.input.tenant_id).bind(&task.input.namespace).bind(&task.input.queue)
        .bind(&task.idempotency_key).bind(&task.input.correlation_key).bind(codec::encode(&task.input)?).bind(codec::encode(&task.descriptor)?)
        .bind(task.origin_trace.as_ref().map(codec::encode).transpose()?).bind(codec::ms(now)?).bind(destination.as_deref())
        .bind(&task.workflow_activation_id).bind(&task.workflow_id).execute(&mut *connection).await?;
    db::history(connection, &transition.history).await?;
    if destination.is_some() {
        crate::dispatch_intents::sync_intent(connection, task_id).await?;
    }
    Ok(transition.task)
}
pub(super) async fn schedule_activation(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    inputs: BTreeMap<String, WorkflowChildResult>,
    wake: Option<WorkflowWake>,
    now: u64,
) -> StoreResult<TaskSnapshot> {
    let task_id = db::id(connection, "task").await?;
    let context = WorkflowActivationContext {
        v: WORKFLOW_VERSION,
        workflow_id: run.snapshot.workflow_id.clone(),
        activation_id: task_id.clone(),
        revision: run.snapshot.revision,
        continuation: run.continuation.clone(),
        state: run.checkpoint.clone(),
        inputs,
        wake,
        local_steps: Vec::new(),
    };
    context.validate()?;
    let mut command = run.submission.clone();
    command.idempotency_key = format!("workflow:{}:{task_id}", run.snapshot.workflow_id);
    let task = insert_task(
        connection,
        &command,
        &run.controller,
        &task_id,
        &run.snapshot.workflow_id,
        true,
        now,
    )
    .await?;
    sqlx::query("INSERT INTO workflow_activations(activation_id,workflow_id,revision,task_id,context_bytes) VALUES($1,$2,($3::text)::ldg_u64,$1,$4)")
        .bind(&task_id).bind(&run.snapshot.workflow_id).bind(run.snapshot.revision.to_string()).bind(codec::encode(&context)?)
        .execute(&mut *connection).await?;
    link_task(
        connection,
        &task_id,
        &run.snapshot.workflow_id,
        &task_id,
        true,
        "controller",
    )
    .await?;
    run.snapshot.activation_id = Some(task_id);
    run.snapshot.state = WorkflowState::Running;
    run.wait_activation = None;
    run.wait_keys.clear();
    run.external_wait_key = None;
    save_run(connection, run).await?;
    Ok(task)
}
pub(super) async fn link_task(
    connection: &mut PgConnection,
    task: &str,
    workflow: &str,
    activation: &str,
    is_activation: bool,
    key: &str,
) -> StoreResult<()> {
    sqlx::query("INSERT INTO workflow_task_links(task_id,workflow_id,activation_id,is_activation,command_key) VALUES($1,$2,$3,$4,$5)")
        .bind(task).bind(workflow).bind(activation).bind(is_activation).bind(key).execute(connection).await?;
    Ok(())
}
pub(super) async fn task_result(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
) -> StoreResult<TaskResult> {
    let row = sqlx::query(include_str!("../../queries/task_result.sql"))
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(id)
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let task = codec::task(&row)?;
    let attempt = if task.attempt_count == 0 {
        None
    } else {
        Some(codec::attempt_prefixed(&row, &task, "a_")?)
    };
    Ok(core::task_result(&task, attempt.as_ref())?)
}
pub(super) async fn enqueue_drain(
    connection: &mut PgConnection,
    run: &RunRecord,
    now: u64,
) -> StoreResult<()> {
    let task = run
        .snapshot
        .activation_id
        .as_deref()
        .or(run.wait_activation.as_deref())
        .ok_or_else(|| corrupt("missing controller activation"))?;
    sqlx::query("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) VALUES('drain:'||$1,$1,$2,'drain',$3,$3) ON CONFLICT(id) DO UPDATE SET available_at_ms=EXCLUDED.available_at_ms,processed_at_ms=NULL")
        .bind(&run.snapshot.workflow_id).bind(task).bind(codec::ms(now)?).execute(connection).await?;
    Ok(())
}
