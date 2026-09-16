use super::*;
use std::collections::{BTreeMap, BTreeSet};

const LEASE_MS: u64 = 30_000;
const APPLICATION_DEADLINE_MS: u64 = 24 * 60 * 60 * 1_000;
const DRAIN_BATCH: i64 = 16;

struct WorkRow {
    kind: String,
    task_id: String,
    failures: u32,
    created_at: u64,
}

impl PostgresStore {
    pub(super) async fn claim_work_once(&self, limit: u32) -> StoreResult<Vec<WorkflowWork>> {
        if !(1..=WORKFLOW_MAX_WORK_BATCH).contains(&limit) {
            return Err(invalid("workflow work batch must be 1..=16").into());
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let now = db::now(&mut tx).await?;
        let rows = sqlx::query("WITH due AS (SELECT id FROM workflow_work WHERE processed_at_ms IS NULL AND available_at_ms<=$1 AND (lease_until_ms IS NULL OR lease_until_ms<=$1) ORDER BY available_at_ms,id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE workflow_work w SET lease_token='wflease_'||gen_random_uuid()::text,lease_until_ms=$3 FROM due WHERE w.id=due.id RETURNING w.*")
            .bind(codec::ms(now)?).bind(i64::from(limit)).bind(codec::ms(now + LEASE_MS)?).fetch_all(&mut *tx).await?;
        let mut batch = Vec::with_capacity(rows.len());
        for row in rows {
            let workflow_id: String = row.try_get("workflow_id")?;
            let task_id: String = row.try_get("task_id")?;
            let row_scope =
                sqlx::query("SELECT tenant_id,namespace FROM workflow_runs WHERE workflow_id=$1")
                    .bind(&workflow_id)
                    .fetch_one(&mut *tx)
                    .await?;
            let scope = Scope {
                tenant_id: row_scope.try_get("tenant_id")?,
                namespace: row_scope.try_get("namespace")?,
            };
            let source = work_source(&mut tx, &row).await?;
            let activation = matches!(
                source,
                WorkflowWorkSource::TaskTerminal {
                    activation: true,
                    ..
                }
            );
            let outcome = if activation {
                Some(
                    task_result(&mut tx, &scope, &task_id)
                        .await?
                        .outcome
                        .ok_or_else(|| corrupt("nonterminal workflow obligation"))?,
                )
            } else {
                None
            };
            let mut resolved_children = Vec::new();
            if activation
                && let Some(TaskOutcome::Succeeded { output, .. }) = &outcome
                && let Ok(decision) = WorkflowDecision::decode(output)
            {
                let keys: Vec<_> = decision.commands().iter().map(|c| c.key.clone()).collect();
                if !keys.is_empty() {
                    let accepted = sqlx::query("SELECT l.command_key,t.descriptor_bytes,'task' AS child_kind FROM workflow_task_links l JOIN tasks t USING(task_id) WHERE l.workflow_id=$1 AND NOT l.is_activation AND l.command_key=ANY($2) UNION ALL SELECT l.command_key,w.controller_bytes AS descriptor_bytes,'workflow' AS child_kind FROM owned_workflow_links l JOIN workflow_runs w ON w.workflow_id=l.child_workflow_id WHERE l.parent_workflow_id=$1 AND l.command_key=ANY($2)")
                        .bind(&workflow_id).bind(&keys).fetch_all(&mut *tx).await?;
                    for child in accepted {
                        resolved_children.push(ResolvedWorkflowChild {
                            key: child.try_get("command_key")?,
                            kind: if child.try_get::<String, _>("child_kind")? == "task" {
                                WorkflowChildKind::Task
                            } else {
                                WorkflowChildKind::Workflow
                            },
                            descriptor: codec::decode(
                                &child.try_get::<Vec<u8>, _>("descriptor_bytes")?,
                            )?,
                        });
                    }
                }
            }
            batch.push(WorkflowWork {
                id: row.try_get("id")?,
                token: row.try_get("lease_token")?,
                workflow_id,
                source,
                outcome,
                resolved_children,
            });
        }
        tx.commit().await?;
        Ok(batch)
    }

    pub(super) async fn apply_work_once(
        &self,
        work: &WorkflowWork,
        resolved: &[ResolvedWorkflowChild],
    ) -> StoreResult<WorkflowProgress> {
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let snapshot = load_work_snapshot(&mut tx, &work.workflow_id).await?;
        let Some(row) = lock_work(&mut tx, work).await? else {
            tx.commit().await?;
            return Ok(WorkflowProgress::default());
        };
        let mut now = db::now(&mut tx).await?;
        let mut progress = WorkflowProgress {
            processed: 1,
            ..WorkflowProgress::default()
        };
        let mut wakes = Vec::new();
        if snapshot.state.is_terminal() {
            finish_work(&mut tx, &work.id, now).await?;
        } else if row.kind == "drain" {
            let mut run = load_run(
                &mut tx,
                &snapshot.scope,
                Some(&snapshot.workflow_id),
                None,
                false,
            )
            .await?;
            drain(&mut tx, &mut run, &work.id, now).await?;
        } else if row.kind == "cancel_owned" {
            let mut run = load_run(
                &mut tx,
                &snapshot.scope,
                Some(&snapshot.workflow_id),
                None,
                false,
            )
            .await?;
            owned::cancel_owned(&mut tx, &mut run, now).await?;
            finish_work(&mut tx, &work.id, now).await?;
        } else if matches!(
            snapshot.state,
            WorkflowState::Failing | WorkflowState::Cancelling
        ) {
            let run = load_run(
                &mut tx,
                &snapshot.scope,
                Some(&snapshot.workflow_id),
                None,
                false,
            )
            .await?;
            enqueue_drain(&mut tx, &run, now).await?;
            finish_work(&mut tx, &work.id, now).await?;
        } else if row.kind == "wait" {
            let applied =
                external::apply_external_wait(&mut tx, &snapshot, &row.task_id, now).await?;
            if let Some(task) = applied.task {
                wakes.push(task);
                progress.activations_scheduled += 1;
            }
            if let Some(at) = applied.rearm_at {
                sqlx::query("UPDATE workflow_work SET available_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id=$1")
                    .bind(&work.id).bind(codec::ms(at)?).execute(&mut *tx).await?;
            } else {
                finish_work(&mut tx, &work.id, now).await?;
            }
        } else {
            let (is_activation, terminal): (bool, bool) =
                if let WorkflowWorkSource::WorkflowTerminal { workflow_id: child } = &work.source {
                    (false, sqlx::query_scalar("SELECT terminal FROM owned_workflow_links WHERE parent_workflow_id=$1 AND child_workflow_id=$2")
                    .bind(&work.workflow_id).bind(child).fetch_one(&mut *tx).await?)
                } else {
                    sqlx::query_as("SELECT is_activation,terminal FROM workflow_task_links WHERE workflow_id=$1 AND task_id=$2")
                    .bind(&work.workflow_id).bind(&row.task_id).fetch_one(&mut *tx).await?
                };
            if !terminal {
                return Err(corrupt("unfinished source task").into());
            }
            if is_activation {
                let mut run = load_run(
                    &mut tx,
                    &snapshot.scope,
                    Some(&snapshot.workflow_id),
                    None,
                    false,
                )
                .await?;
                let (result, processing_trace) =
                    task_result_with_trace(&mut tx, &run.snapshot.scope, &row.task_id).await?;
                let outcome = result
                    .outcome
                    .ok_or_else(|| corrupt("unfinished source task"))?;
                match outcome {
                    TaskOutcome::Succeeded { output, .. } => {
                        let decision = WorkflowDecision::decode(&output)?;
                        let revision: String = sqlx::query_scalar("SELECT trunc(revision)::text FROM workflow_activations WHERE activation_id=$1")
                            .bind(&row.task_id).fetch_one(&mut *tx).await?;
                        if decision.activation_id != row.task_id
                            || decision.revision != codec::u64_text(&revision)?
                        {
                            return Err(ContractError::Conflict.into());
                        }
                        if run.snapshot.activation_id.as_deref() != Some(&row.task_id)
                            || decision.revision != run.snapshot.revision
                        {
                            return Err(corrupt(
                                "unprocessed controller obligation differs from current activation",
                            )
                            .into());
                        }
                        let applied = apply_decision(
                            &mut tx,
                            &mut run,
                            &decision,
                            resolved,
                            processing_trace.as_ref(),
                            now,
                            &mut progress,
                        )
                        .await?;
                        wakes.extend(applied.wakes);
                        now = applied.at;
                        sqlx::query("UPDATE workflow_activations SET applied_at_ms=$2 WHERE activation_id=$1")
                            .bind(&row.task_id).bind(codec::ms(now)?).execute(&mut *tx).await?;
                    }
                    TaskOutcome::Failed { failure, .. } => {
                        let error = match failure {
                            TaskFailure::Application { error } => error,
                            _ => ApplicationError {
                                kind: "workflow_activation_failed".into(),
                                message: "workflow activation exhausted its execution attempts"
                                    .into(),
                            },
                        };
                        fail_run(&mut tx, &mut run, &error, now).await?;
                    }
                    TaskOutcome::Cancelled {} => {
                        fail_run(
                            &mut tx,
                            &mut run,
                            &ApplicationError {
                                kind: "workflow_activation_cancelled".into(),
                                message: "workflow activation was cancelled independently".into(),
                            },
                            now,
                        )
                        .await?
                    }
                }
            } else if snapshot.state == WorkflowState::Waiting {
                let bytes: Option<Vec<u8>> = sqlx::query_scalar(
                    "SELECT wait_keys_bytes FROM workflow_runs WHERE workflow_id=$1 AND external_wait_key IS NULL",
                )
                .bind(&snapshot.workflow_id)
                .fetch_optional(&mut *tx)
                .await?;
                let Some(bytes) = bytes else {
                    finish_work(&mut tx, &work.id, now).await?;
                    tx.commit().await?;
                    return Ok(progress);
                };
                let keys: Vec<String> = codec::decode(&bytes)?;
                if let Some(inputs) = wait_inputs(&mut tx, &snapshot, &keys).await? {
                    let mut run = load_run(
                        &mut tx,
                        &snapshot.scope,
                        Some(&snapshot.workflow_id),
                        None,
                        false,
                    )
                    .await?;
                    // A child can commit after the initial clock sample without
                    // taking this parent's lock. Timestamp after observing the join.
                    now = db::now(&mut tx).await?;
                    wakes.push(schedule_activation(&mut tx, &mut run, inputs, None, now).await?);
                    progress.activations_scheduled += 1;
                    record_history(
                        &mut tx,
                        &run.snapshot.workflow_id,
                        run.snapshot.activation_id.as_deref(),
                        now,
                        "resumed",
                    )
                    .await?;
                }
            }
            finish_work(&mut tx, &work.id, now).await?;
        }
        tx.commit().await?;
        self.workflow_wakes(&wakes);
        Ok(progress)
    }

    pub(super) async fn retry_work_once(
        &self,
        work: &WorkflowWork,
        reason: &str,
    ) -> StoreResult<()> {
        let reason: String = reason.chars().take(4096).collect();
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let mut run = load_work_run(&mut tx, &work.workflow_id, true).await?;
        let Some(row) = lock_work(&mut tx, work).await? else {
            tx.commit().await?;
            return Ok(());
        };
        let now = db::now(&mut tx).await?;
        if matches!(row.kind.as_str(), "terminal" | "workflow_terminal")
            && now.saturating_sub(row.created_at) >= APPLICATION_DEADLINE_MS
        {
            fail_run(
                &mut tx,
                &mut run,
                &ApplicationError {
                    kind: "workflow_application_timeout".into(),
                    message: "workflow decision application exceeded its 24-hour recovery deadline"
                        .into(),
                },
                now,
            )
            .await?;
            finish_work(&mut tx, &work.id, now).await?;
        } else {
            let delay = (500_u64 << row.failures.min(6)).min(30_000);
            sqlx::query("UPDATE workflow_work SET available_at_ms=$2,lease_token=NULL,lease_until_ms=NULL,failures=failures+1,last_error=$3 WHERE id=$1")
                .bind(&work.id).bind(codec::ms(now + delay)?).bind(reason).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn reject_work_once(
        &self,
        work: &WorkflowWork,
        error: &ApplicationError,
    ) -> StoreResult<()> {
        validate_workflow_error(error)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let mut run = load_work_run(&mut tx, &work.workflow_id, true).await?;
        let Some(row) = lock_work(&mut tx, work).await? else {
            tx.commit().await?;
            return Ok(());
        };
        if matches!(row.kind.as_str(), "drain" | "cancel_owned") {
            return Err(corrupt("cancellation work cannot be permanently rejected").into());
        }
        let now = db::now(&mut tx).await?;
        fail_run(&mut tx, &mut run, error, now).await?;
        sqlx::query("UPDATE workflow_activations SET applied_at_ms=$2,error_bytes=$3 WHERE activation_id=$1 AND applied_at_ms IS NULL")
            .bind(&row.task_id).bind(codec::ms(now)?).bind(codec::encode(error)?).execute(&mut *tx).await?;
        finish_work(&mut tx, &work.id, now).await?;
        tx.commit().await?;
        Ok(())
    }
}

async fn load_work_snapshot(
    connection: &mut PgConnection,
    id: &str,
) -> StoreResult<WorkflowSnapshot> {
    validate_text(id, 128)?;
    let row = sqlx::query("SELECT workflow_id,parent_workflow_id,root_workflow_id,tenant_id,namespace,state,trunc(revision)::text AS revision_text,current_activation_id,submitted_at_ms,terminal_at_ms,correlation_key FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(id).fetch_optional(connection).await?.ok_or(ContractError::NotFound)?;
    status_record(&row)
}
async fn load_work_run(
    connection: &mut PgConnection,
    id: &str,
    locked: bool,
) -> StoreResult<RunRecord> {
    validate_text(id, 128)?;
    let row = sqlx::query("SELECT tenant_id,namespace FROM workflow_runs WHERE workflow_id=$1")
        .bind(id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let scope = Scope {
        tenant_id: row.try_get("tenant_id")?,
        namespace: row.try_get("namespace")?,
    };
    load_run(connection, &scope, Some(id), None, locked).await
}
async fn lock_work(
    connection: &mut PgConnection,
    work: &WorkflowWork,
) -> StoreResult<Option<WorkRow>> {
    let row = sqlx::query("SELECT * FROM workflow_work WHERE id=$1 FOR UPDATE")
        .bind(&work.id)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    if row.try_get::<String, _>("workflow_id")? != work.workflow_id
        || work_source(connection, &row).await? != work.source
    {
        return Err(ContractError::Conflict.into());
    }
    if row.try_get::<Option<i64>, _>("processed_at_ms")?.is_some() {
        return Ok(None);
    }
    let until = row.try_get::<Option<i64>, _>("lease_until_ms")?;
    if row.try_get::<Option<String>, _>("lease_token")?.as_deref() != Some(&work.token)
        || until.is_none_or(|t| t < 0)
        || db::now(connection).await? >= until.unwrap_or_default() as u64
    {
        return Err(ContractError::OwnershipLost.into());
    }
    Ok(Some(WorkRow {
        kind: row.try_get("kind")?,
        task_id: row.try_get("task_id")?,
        failures: u32::try_from(row.try_get::<i64, _>("failures")?)
            .map_err(|_| corrupt("work retry count"))?,
        created_at: u64::try_from(row.try_get::<i64, _>("created_at_ms")?)
            .map_err(|_| corrupt("work timestamp"))?,
    }))
}
async fn finish_work(connection: &mut PgConnection, id: &str, now: u64) -> StoreResult<()> {
    sqlx::query("UPDATE workflow_work SET processed_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id=$1")
        .bind(id).bind(codec::ms(now)?).execute(connection).await?;
    Ok(())
}
pub(super) struct AppliedDecision {
    pub wakes: Vec<TaskSnapshot>,
    pub at: u64,
}

pub(super) async fn apply_decision(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    decision: &WorkflowDecision,
    resolved: &[ResolvedWorkflowChild],
    processing_trace: Option<&TraceContext>,
    now: u64,
    progress: &mut WorkflowProgress,
) -> StoreResult<AppliedDecision> {
    let mut wakes = Vec::new();
    let unfinished = if matches!(decision.action, WorkflowAction::Complete { .. }) {
        owned::unfinished(connection, &run.snapshot.workflow_id).await?
    } else {
        false
    };
    let plan = core::plan_workflow_decision(&run.snapshot, decision, unfinished)?;
    let mut descriptors = BTreeMap::new();
    for child in resolved {
        child.descriptor.validate().map_err(ContractError::from)?;
        if descriptors.insert(&child.key, child).is_some() {
            return Err(ContractError::Conflict.into());
        }
    }
    let expected: BTreeSet<_> = decision.commands().iter().map(|c| &c.key).collect();
    if descriptors.keys().any(|key| !expected.contains(key)) {
        return Err(ContractError::Conflict.into());
    }
    // Pin causality to the accepted spawning activation, while retaining the
    // workflow's original submission carrier for its own future activations.
    let origin_trace = processing_trace.or(run.submission.origin_trace.as_ref());
    for command in decision.commands() {
        let existing_task: Option<String> = sqlx::query_scalar("SELECT task_id FROM workflow_task_links WHERE workflow_id=$1 AND NOT is_activation AND command_key=$2")
            .bind(&run.snapshot.workflow_id).bind(&command.key).fetch_optional(&mut *connection).await?;
        let existing_workflow: Option<Vec<u8>> = sqlx::query_scalar("SELECT w.submission_bytes FROM owned_workflow_links l JOIN workflow_runs w ON w.workflow_id=l.child_workflow_id WHERE l.parent_workflow_id=$1 AND l.command_key=$2")
            .bind(&run.snapshot.workflow_id).bind(&command.key).fetch_optional(&mut *connection).await?;
        let input = command.submission(&run.snapshot.scope, run.snapshot.correlation_key.clone());
        if existing_task.is_some() && existing_workflow.is_some() {
            return Err(corrupt("ambiguous owned child key").into());
        }
        if let Some(task_id) = existing_task {
            if command.kind != WorkflowChildKind::Task {
                return Err(ContractError::Conflict.into());
            }
            let task = db::load_task(connection, &run.snapshot.scope, &task_id, false).await?;
            if !task
                .input
                .semantically_matches(&input)
                .map_err(ContractError::from)?
            {
                return Err(ContractError::Conflict.into());
            }
            continue;
        }
        if let Some(bytes) = existing_workflow {
            if command.kind != WorkflowChildKind::Workflow {
                return Err(ContractError::Conflict.into());
            }
            let submission: SubmitCommand = codec::decode(&bytes)?;
            if !submission
                .input
                .semantically_matches(&input)
                .map_err(ContractError::from)?
            {
                return Err(ContractError::Conflict.into());
            }
            continue;
        }
        let resolved = descriptors
            .get(&command.key)
            .ok_or_else(|| invalid("missing resolved child binding"))?;
        if resolved.kind != command.kind || resolved.descriptor.program != command.program {
            return Err(ContractError::Conflict.into());
        }
        let task = if command.kind == WorkflowChildKind::Workflow {
            owned::create_child(
                connection,
                run,
                &decision.activation_id,
                command,
                &resolved.descriptor,
                origin_trace,
                now,
            )
            .await?
        } else {
            let id = db::id(connection, "task").await?;
            let submit = SubmitCommand {
                input,
                idempotency_key: format!("workflow:{}:{id}", run.snapshot.workflow_id),
                origin_trace: origin_trace.cloned(),
            };
            let task = insert_task(
                connection,
                &submit,
                &resolved.descriptor,
                &id,
                &run.snapshot,
                false,
                now,
            )
            .await?;
            link_task(
                connection,
                &id,
                &run.snapshot.workflow_id,
                &decision.activation_id,
                false,
                &command.key,
            )
            .await?;
            task
        };
        wakes.push(task);
        progress.children_scheduled += 1;
    }
    let context_bytes: Vec<u8> =
        sqlx::query_scalar("SELECT context_bytes FROM workflow_activations WHERE activation_id=$1")
            .bind(&decision.activation_id)
            .fetch_one(&mut *connection)
            .await?;
    let context: WorkflowActivationContext = codec::decode(&context_bytes)?;
    context
        .validate()
        .map_err(|_| corrupt("activation context"))?;
    if context.workflow_id != run.snapshot.workflow_id
        || context.activation_id != decision.activation_id
        || context.revision != decision.revision
        || context.parent_workflow_id != run.snapshot.parent_workflow_id
        || context.root_workflow_id != run.snapshot.root_workflow_id
    {
        return Err(corrupt("activation context identity").into());
    }
    let consumed_tasks: Vec<_> = context
        .inputs
        .values()
        .filter_map(|input| match input {
            WorkflowChildResult::Task(child) => Some(child.task_id.clone()),
            _ => None,
        })
        .collect();
    let consumed_workflows: Vec<_> = context
        .inputs
        .values()
        .filter_map(|input| match input {
            WorkflowChildResult::Workflow(child) => Some(child.workflow_id.clone()),
            _ => None,
        })
        .collect();
    if !consumed_tasks.is_empty() {
        sqlx::query("UPDATE workflow_task_links SET consumed=true WHERE workflow_id=$1 AND NOT is_activation AND task_id=ANY($2)")
            .bind(&run.snapshot.workflow_id).bind(consumed_tasks).execute(&mut *connection).await?;
    }
    if !consumed_workflows.is_empty() {
        sqlx::query("UPDATE owned_workflow_links SET consumed=true WHERE parent_workflow_id=$1 AND child_workflow_id=ANY($2)")
            .bind(&run.snapshot.workflow_id).bind(consumed_workflows).execute(&mut *connection).await?;
    }
    run.snapshot.revision = plan.revision;
    if let Some(checkpoint) = plan.checkpoint {
        run.checkpoint = checkpoint.state;
        run.continuation = checkpoint.continuation;
    }
    let mut applied_at = now;
    match plan.disposition {
        core::WorkflowDisposition::Wait { members } => {
            run.snapshot.state = WorkflowState::Waiting;
            run.snapshot.activation_id = None;
            run.wait_activation = Some(decision.activation_id.clone());
            run.wait_keys = members;
            save_run(connection, run).await?;
            if let Some(inputs) = wait_inputs(connection, &run.snapshot, &run.wait_keys).await? {
                applied_at = db::now(connection).await?;
                wakes.push(schedule_activation(connection, run, inputs, None, applied_at).await?);
                progress.activations_scheduled += 1;
            }
        }
        core::WorkflowDisposition::ExternalWait { wait } => {
            if let Some(task) = external::install_external_wait(
                connection,
                run,
                &decision.activation_id,
                &wait,
                now,
            )
            .await?
            {
                wakes.push(task);
                progress.activations_scheduled += 1;
            }
        }
        core::WorkflowDisposition::Continue => {
            let inputs = pending_inputs(connection, run).await?;
            if !inputs.is_empty() {
                applied_at = db::now(connection).await?;
            }
            wakes.push(schedule_activation(connection, run, inputs, None, applied_at).await?);
            progress.activations_scheduled += 1;
        }
        core::WorkflowDisposition::Complete { output } => {
            // Owned children can finish after the coordinator's initial time
            // sample without locking this parent. Timestamp the observed join.
            applied_at = db::now(connection).await?;
            external::close_external_wait(connection, run, applied_at).await?;
            run.snapshot.state = WorkflowState::Succeeded;
            run.snapshot.terminal_at = Some(applied_at);
            run.snapshot.activation_id = None;
            run.outcome = Some(WorkflowOutcome::Succeeded { output });
            run.wait_activation = None;
            run.wait_keys.clear();
            run.external_wait_key = None;
            save_run(connection, run).await?;
        }
        core::WorkflowDisposition::Fail { error } => fail_run(connection, run, &error, now).await?,
    }
    record_history(
        connection,
        &run.snapshot.workflow_id,
        Some(&decision.activation_id),
        applied_at,
        "decision_applied",
    )
    .await?;
    Ok(AppliedDecision {
        wakes,
        at: applied_at,
    })
}
async fn child_input(
    connection: &mut PgConnection,
    scope: &Scope,
    row: &PgRow,
) -> StoreResult<WorkflowChildResult> {
    let id: String = row.try_get("child_id")?;
    if row.try_get::<bool, _>("is_workflow")? {
        return owned::result(connection, scope, &id).await;
    }
    let result = task_result(connection, scope, &id).await?;
    Ok(WorkflowChildResult::Task(WorkflowTaskResult {
        task_id: id,
        state: result.task.state,
        outcome: result
            .outcome
            .ok_or_else(|| corrupt("unfinished child input"))?,
    }))
}
async fn wait_inputs(
    connection: &mut PgConnection,
    snapshot: &WorkflowSnapshot,
    keys: &[String],
) -> StoreResult<Option<BTreeMap<String, WorkflowChildResult>>> {
    let rows = sqlx::query("SELECT command_key,task_id AS child_id,terminal,false AS is_workflow FROM workflow_task_links WHERE workflow_id=$1 AND NOT is_activation AND command_key=ANY($2) UNION ALL SELECT command_key,child_workflow_id AS child_id,terminal,true AS is_workflow FROM owned_workflow_links WHERE parent_workflow_id=$1 AND command_key=ANY($2)")
        .bind(&snapshot.workflow_id).bind(keys).fetch_all(&mut *connection).await?;
    if rows.len() != keys.len() {
        return Err(invalid("wait references an unknown child key").into());
    }
    for row in &rows {
        if !row.try_get::<bool, _>("terminal")? {
            return Ok(None);
        }
    }
    let mut inputs = BTreeMap::new();
    for row in rows {
        inputs.insert(
            row.try_get("command_key")?,
            child_input(connection, &snapshot.scope, &row).await?,
        );
    }
    Ok(Some(inputs))
}
async fn pending_inputs(
    connection: &mut PgConnection,
    run: &RunRecord,
) -> StoreResult<BTreeMap<String, WorkflowChildResult>> {
    let rows = sqlx::query("SELECT * FROM (SELECT l.command_key,l.task_id AS child_id,false AS is_workflow,t.terminal_at_ms FROM workflow_task_links l JOIN tasks t USING(task_id) WHERE l.workflow_id=$1 AND NOT l.is_activation AND NOT l.consumed AND l.terminal UNION ALL SELECT l.command_key,l.child_workflow_id AS child_id,true AS is_workflow,w.terminal_at_ms FROM owned_workflow_links l JOIN workflow_runs w ON w.workflow_id=l.child_workflow_id WHERE l.parent_workflow_id=$1 AND l.terminal AND NOT l.consumed) children ORDER BY terminal_at_ms,child_id LIMIT 64")
        .bind(&run.snapshot.workflow_id).fetch_all(&mut *connection).await?;
    let mut inputs = BTreeMap::new();
    for row in rows {
        inputs.insert(
            row.try_get("command_key")?,
            child_input(connection, &run.snapshot.scope, &row).await?,
        );
    }
    Ok(inputs)
}
async fn fail_run(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    error: &ApplicationError,
    now: u64,
) -> StoreResult<()> {
    if run.state_terminal()
        || matches!(
            run.snapshot.state,
            WorkflowState::Cancelling | WorkflowState::Failing
        )
    {
        return Ok(());
    }
    validate_workflow_error(error)?;
    external::close_external_wait(connection, run, now).await?;
    run.snapshot.state = WorkflowState::Failing;
    run.outcome = Some(WorkflowOutcome::Failed {
        error: error.clone(),
    });
    save_run(connection, run).await?;
    enqueue_drain(connection, run, now).await?;
    record_history(
        connection,
        &run.snapshot.workflow_id,
        run.snapshot.activation_id.as_deref(),
        now,
        "failure_requested",
    )
    .await?;
    Ok(())
}
async fn drain(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    work_id: &str,
    now: u64,
) -> StoreResult<()> {
    if !matches!(
        run.snapshot.state,
        WorkflowState::Failing | WorkflowState::Cancelling
    ) {
        return Err(corrupt("drain without terminal intent").into());
    }
    let ids: Vec<String> = sqlx::query_scalar("SELECT t.task_id FROM workflow_task_links l JOIN tasks t USING(task_id) WHERE l.workflow_id=$1 AND NOT l.terminal AND t.cancel_requested_at_ms IS NULL ORDER BY t.task_id LIMIT $2")
        .bind(&run.snapshot.workflow_id).bind(DRAIN_BATCH).fetch_all(&mut *connection).await?;
    let remaining = DRAIN_BATCH - ids.len() as i64;
    for id in ids {
        let task = db::load_task(connection, &run.snapshot.scope, &id, true).await?;
        let attempt = if let Some(id) = &task.current_attempt_id {
            Some(db::load_attempt(connection, &task, id).await?)
        } else {
            None
        };
        let transition = core::cancel(&task, attempt.as_ref(), db::now(connection).await?)?;
        db::apply(connection, &transition).await?;
    }
    owned::enqueue_cancellations(connection, &run.snapshot.workflow_id, remaining, now).await?;
    let unfinished = owned::unfinished(connection, &run.snapshot.workflow_id).await?;
    if unfinished {
        sqlx::query("UPDATE workflow_work SET available_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id=$1")
            .bind(work_id).bind(codec::ms(now + 1_000)?).execute(connection).await?;
    } else {
        // Cancelling tasks may have committed later timestamps than this drain's
        // initial sample. Finalize after the observed owned terminal boundary.
        let now = db::now(connection).await?;
        run.snapshot.state = if run.snapshot.state == WorkflowState::Cancelling {
            WorkflowState::Cancelled
        } else {
            WorkflowState::Failed
        };
        run.snapshot.terminal_at = Some(now);
        run.snapshot.activation_id = None;
        run.wait_activation = None;
        run.wait_keys.clear();
        run.external_wait_key = None;
        save_run(connection, run).await?;
        record_history(
            connection,
            &run.snapshot.workflow_id,
            run.snapshot.activation_id.as_deref(),
            now,
            "terminalized",
        )
        .await?;
        finish_work(connection, work_id, now).await?;
    }
    Ok(())
}

async fn work_source(
    connection: &mut PgConnection,
    row: &PgRow,
) -> StoreResult<WorkflowWorkSource> {
    Ok(match row.try_get::<String, _>("kind")?.as_str() {
        "terminal" => {
            let task_id: String = row.try_get("task_id")?;
            let workflow_id: String = row.try_get("workflow_id")?;
            let activation: bool = sqlx::query_scalar(
                "SELECT is_activation FROM workflow_task_links WHERE workflow_id=$1 AND task_id=$2",
            )
            .bind(workflow_id)
            .bind(&task_id)
            .fetch_one(connection)
            .await?;
            WorkflowWorkSource::TaskTerminal {
                task_id,
                activation,
            }
        }
        "workflow_terminal" => WorkflowWorkSource::WorkflowTerminal {
            workflow_id: row.try_get("child_workflow_id")?,
        },
        "drain" => WorkflowWorkSource::Drain,
        "cancel_owned" => WorkflowWorkSource::CancelOwned,
        "wait" => WorkflowWorkSource::Wait,
        _ => return Err(corrupt("workflow work source").into()),
    })
}
