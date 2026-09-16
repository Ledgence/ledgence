//! Direct ownership only: terminal children notify parents without a parent
//! mutation lock; parent drain sends durable cancellation work down one edge.
use super::*;

pub(super) async fn create_child(
    connection: &mut PgConnection,
    parent: &RunRecord,
    activation: &str,
    command: &WorkflowChildCommand,
    descriptor: &ProgramDescriptor,
    origin_trace: Option<&TraceContext>,
    now: u64,
) -> StoreResult<TaskSnapshot> {
    if parent.nesting_depth >= WORKFLOW_MAX_DEPTH {
        return Err(invalid("workflow nesting exceeds supported depth").into());
    }
    let live: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM owned_workflow_links WHERE parent_workflow_id=$1 AND NOT terminal",
    )
    .bind(&parent.snapshot.workflow_id)
    .fetch_one(&mut *connection)
    .await?;
    if live >= i64::from(WORKFLOW_MAX_LIVE_SUBWORKFLOWS) {
        return Err(invalid("workflow has too many live owned subworkflows").into());
    }
    let id = db::id(connection, "wf").await?;
    let submission = SubmitCommand {
        input: command.submission(
            &parent.snapshot.scope,
            parent.snapshot.correlation_key.clone(),
        ),
        idempotency_key: format!("owned:{id}"),
        origin_trace: origin_trace.cloned(),
    };
    let root = parent
        .snapshot
        .root_workflow_id
        .as_deref()
        .unwrap_or(&parent.snapshot.workflow_id);
    sqlx::query("INSERT INTO workflow_runs(workflow_id,tenant_id,namespace,idempotency_key,submission_bytes,controller_bytes,state,continuation,checkpoint_bytes,submitted_at_ms,correlation_key,parent_workflow_id,root_workflow_id,nesting_depth) VALUES($1,$2,$3,$4,$5,$6,'running','start',$7,$8,$9,$10,$11,$12)")
        .bind(&id).bind(&submission.input.tenant_id).bind(&submission.input.namespace).bind(&submission.idempotency_key)
        .bind(codec::encode(&submission)?).bind(codec::encode(descriptor)?).bind(codec::encode(&Value::Null)?).bind(codec::ms(now)?)
        .bind(&submission.input.correlation_key).bind(&parent.snapshot.workflow_id).bind(root).bind((parent.nesting_depth + 1) as i32)
        .execute(&mut *connection).await?;
    sqlx::query("INSERT INTO owned_workflow_links(parent_workflow_id,command_key,child_workflow_id,creating_activation_id) VALUES($1,$2,$3,$4)")
        .bind(&parent.snapshot.workflow_id).bind(&command.key).bind(&id).bind(activation).execute(&mut *connection).await?;
    let mut child = load_run(connection, &parent.snapshot.scope, Some(&id), None, false).await?;
    let task = schedule_activation(connection, &mut child, BTreeMap::new(), None, now).await?;
    record_history(connection, &id, Some(&task.task_id), now, "started_owned").await?;
    Ok(task)
}

pub(super) async fn unfinished(connection: &mut PgConnection, workflow: &str) -> StoreResult<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_task_links WHERE workflow_id=$1 AND NOT terminal) OR EXISTS(SELECT 1 FROM owned_workflow_links WHERE parent_workflow_id=$1 AND NOT terminal)")
        .bind(workflow).fetch_one(connection).await?)
}

/// Called in the child's terminal transaction. Do not lock its parent: the
/// immutable FK only takes KEY SHARE, compatible with parent NO KEY UPDATE.
pub(super) async fn terminal_obligation(
    connection: &mut PgConnection,
    child: &WorkflowSnapshot,
) -> StoreResult<()> {
    let at = codec::ms(
        child
            .terminal_at
            .ok_or_else(|| corrupt("child terminal timestamp"))?,
    )?;
    sqlx::query("WITH ended AS (UPDATE owned_workflow_links SET terminal=true WHERE child_workflow_id=$1 AND NOT terminal RETURNING parent_workflow_id,creating_activation_id,child_workflow_id) INSERT INTO workflow_work(id,workflow_id,task_id,kind,child_workflow_id,available_at_ms,created_at_ms) SELECT 'workflow-terminal:'||child_workflow_id,parent_workflow_id,creating_activation_id,'workflow_terminal',child_workflow_id,$2,$2 FROM ended ON CONFLICT DO NOTHING")
        .bind(&child.workflow_id).bind(at).execute(connection).await?;
    Ok(())
}

pub(super) async fn enqueue_cancellations(
    connection: &mut PgConnection,
    parent: &str,
    limit: i64,
    now: u64,
) -> StoreResult<()> {
    if limit == 0 {
        return Ok(());
    }
    let children = sqlx::query("SELECT l.child_workflow_id,a.task_id FROM owned_workflow_links l JOIN workflow_activations a ON a.workflow_id=l.child_workflow_id AND a.revision=0 WHERE l.parent_workflow_id=$1 AND NOT l.terminal AND NOT l.cancel_enqueued ORDER BY l.child_workflow_id LIMIT $2")
        .bind(parent).bind(limit).fetch_all(&mut *connection).await?;
    for child in children {
        let id: String = child.try_get("child_workflow_id")?;
        let task: String = child.try_get("task_id")?;
        sqlx::query("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) VALUES('cancel-owned:'||$1,$1,$2,'cancel_owned',$3,$3) ON CONFLICT DO NOTHING")
            .bind(&id).bind(&task).bind(codec::ms(now)?).execute(&mut *connection).await?;
        sqlx::query("UPDATE owned_workflow_links SET cancel_enqueued=true WHERE parent_workflow_id=$1 AND child_workflow_id=$2")
            .bind(parent).bind(&id).execute(&mut *connection).await?;
    }
    Ok(())
}

pub(super) async fn cancel_owned(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    now: u64,
) -> StoreResult<()> {
    if run.snapshot.parent_workflow_id.is_none() {
        return Err(corrupt("owned cancellation has no parent").into());
    }
    if run.snapshot.state.is_terminal() || run.snapshot.state == WorkflowState::Cancelling {
        return Ok(());
    }
    external::close_external_wait(connection, run, now).await?;
    run.snapshot.state = WorkflowState::Cancelling;
    run.outcome = Some(WorkflowOutcome::Cancelled {});
    save_run(connection, run).await?;
    enqueue_drain(connection, run, now).await?;
    record_history(
        connection,
        &run.snapshot.workflow_id,
        run.snapshot.activation_id.as_deref(),
        now,
        "owner_cancel_requested",
    )
    .await?;
    Ok(())
}

pub(super) async fn result(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
) -> StoreResult<WorkflowChildResult> {
    let row = sqlx::query("SELECT state,outcome_bytes FROM workflow_runs WHERE workflow_id=$1 AND tenant_id=$2 AND namespace=$3 AND terminal_at_ms IS NOT NULL")
        .bind(id).bind(&scope.tenant_id).bind(&scope.namespace).fetch_optional(connection).await?.ok_or_else(|| corrupt("unfinished owned workflow input"))?;
    let result = WorkflowChildResult::Workflow(WorkflowSubworkflowResult {
        kind: WorkflowChildKind::Workflow,
        workflow_id: id.to_owned(),
        state: serde_json::from_value(Value::String(row.try_get("state")?))
            .map_err(|_| corrupt("owned workflow state"))?,
        outcome: codec::decode(&row.try_get::<Vec<u8>, _>("outcome_bytes")?)?,
    });
    result
        .validate()
        .map_err(|_| corrupt("owned workflow outcome"))?;
    Ok(result)
}
