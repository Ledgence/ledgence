//! Compact read evidence, written atomically with the facts it describes. This
//! projection never drives orchestration and never requires a root mutation lock.
use super::*;
use ledgence_orchestration_api::console::*;

pub(super) async fn entrypoint(
    connection: &mut PgConnection,
    context: &WorkflowActivationContext,
    now: u64,
) -> StoreResult<()> {
    let data = ConsoleExplorerData::Entrypoint {
        state: None,
        availability: ConsoleEvidenceAvailability::Available,
        submitted_at: now,
        terminal_at: None,
        applied_at: None,
        decision_kind: None,
        error: None,
        resumed_activation_id: None,
    };
    sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) VALUES($1,($2::text)::ldg_u64,'entrypoint','',$3,$4,$5)")
        .bind(&context.workflow_id).bind(context.revision.to_string()).bind(&context.activation_id).bind(&context.continuation).bind(codec::encode(&data)?).execute(connection).await?;
    Ok(())
}

pub(super) async fn insert(
    connection: &mut PgConnection,
    activation: &str,
    data: &ConsoleExplorerData,
) -> StoreResult<()> {
    data.validate()?;
    let inserted = sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) SELECT workflow_id,revision,$2,$3,activation_id,entrypoint,$4 FROM workflow_explorer_records WHERE activation_id=$1 AND kind='entrypoint' AND record_key='' ON CONFLICT (activation_id,kind,record_key) DO NOTHING")
        .bind(activation).bind(data.kind()).bind(data.key()).bind(codec::encode(data)?).execute(connection).await?.rows_affected();
    if inserted != 1 {
        return Err(corrupt("missing or duplicate explorer entrypoint record").into());
    }
    Ok(())
}

pub(super) async fn child(
    connection: &mut PgConnection,
    activation: &str,
    command: &WorkflowChildCommand,
    id: &str,
    fork_key: Option<&str>,
    now: u64,
) -> StoreResult<()> {
    insert(
        connection,
        activation,
        &ConsoleExplorerData::Child {
            key: command.key.clone(),
            execution: ConsoleExecutionIdentity {
                kind: match command.kind {
                    WorkflowChildKind::Task => ConsoleExecutionKind::Task,
                    WorkflowChildKind::Workflow => ConsoleExecutionKind::Workflow,
                },
                id: id.to_owned(),
            },
            program: command.program.clone(),
            fork_key: fork_key.map(str::to_owned),
            availability: ConsoleEvidenceAvailability::Available,
            state: None,
            submitted_at: now,
            terminal_at: None,
        },
    )
    .await
}

pub(super) async fn local(
    connection: &mut PgConnection,
    command: &LocalResultCommand,
    now: u64,
) -> StoreResult<()> {
    let data = ConsoleExplorerData::Local {
        key: command.record.key.clone(),
        callable: command.record.callable.clone(),
        accepted_at: Some(now),
        accepting_attempt_id: Some(command.owner.attempt_id.clone()),
        observation: None,
    };
    // Only metadata identifiers and timestamps enter this JSON merge. Application
    // input/output remain exclusively in the accepted-result journal.
    sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) SELECT workflow_id,revision,'local',$2,activation_id,entrypoint,$3 FROM workflow_explorer_records WHERE activation_id=$1 AND kind='entrypoint' ON CONFLICT (activation_id,kind,record_key) DO UPDATE SET metadata_bytes=convert_to((convert_from(EXCLUDED.metadata_bytes,'UTF8')::jsonb || jsonb_build_object('observation',CASE WHEN convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->>'callable'=convert_from(EXCLUDED.metadata_bytes,'UTF8')::jsonb->>'callable' THEN convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->'observation' ELSE NULL END))::text,'UTF8')")
        .bind(&command.owner.task_id).bind(&command.record.key).bind(codec::encode(&data)?).execute(connection).await?;
    Ok(())
}

pub(super) async fn decision(
    connection: &mut PgConnection,
    decision: &WorkflowDecision,
    now: u64,
    resumed: Option<&str>,
) -> StoreResult<()> {
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT metadata_bytes FROM workflow_explorer_records WHERE activation_id=$1 AND kind='entrypoint'")
        .bind(&decision.activation_id).fetch_one(&mut *connection).await?;
    let mut data: ConsoleExplorerData = codec::decode(&bytes)?;
    let ConsoleExplorerData::Entrypoint {
        applied_at,
        decision_kind,
        resumed_activation_id,
        ..
    } = &mut data
    else {
        return Err(corrupt("explorer entrypoint kind").into());
    };
    *applied_at = Some(now);
    *decision_kind = Some(match &decision.action {
        WorkflowAction::Continue { .. } => ConsoleDecisionKind::Continue,
        WorkflowAction::Suspend { .. } => ConsoleDecisionKind::Suspend,
        WorkflowAction::Wait { .. } => ConsoleDecisionKind::Wait,
        WorkflowAction::Complete { .. } => ConsoleDecisionKind::Complete,
        WorkflowAction::Fail { .. } => ConsoleDecisionKind::Fail,
    });
    *resumed_activation_id = resumed.map(str::to_owned);
    data.validate()?;
    sqlx::query("UPDATE workflow_explorer_records SET metadata_bytes=$2 WHERE activation_id=$1 AND kind='entrypoint'")
        .bind(&decision.activation_id).bind(codec::encode(&data)?).execute(&mut *connection).await?;
    if let WorkflowAction::Suspend {
        until,
        continuation,
        ..
    } = &decision.action
    {
        insert(
            connection,
            &decision.activation_id,
            &ConsoleExplorerData::ChildWait {
                member_keys: until.clone(),
                resume: continuation.clone(),
                applied_at: now,
                resumed_activation_id: resumed.map(str::to_owned),
            },
        )
        .await?;
    }
    Ok(())
}

/// A later child/event wake schedules a resume without another parent decision.
pub(super) async fn resumed(
    connection: &mut PgConnection,
    activation: &str,
    task: &str,
    wake: Option<&WorkflowWake>,
    now: u64,
) -> StoreResult<()> {
    let rows = sqlx::query("SELECT kind,record_key,metadata_bytes FROM workflow_explorer_records WHERE activation_id=$1 AND kind IN ('entrypoint','child_wait','external_wait')")
        .bind(activation).fetch_all(&mut *connection).await?;
    for row in rows {
        let mut data: ConsoleExplorerData =
            codec::decode(&row.try_get::<Vec<u8>, _>("metadata_bytes")?)?;
        match &mut data {
            ConsoleExplorerData::Entrypoint {
                resumed_activation_id,
                decision_kind: Some(_),
                ..
            }
            | ConsoleExplorerData::ChildWait {
                resumed_activation_id,
                ..
            } if wake.is_none() => {
                *resumed_activation_id = Some(task.into());
            }
            ConsoleExplorerData::Entrypoint {
                resumed_activation_id,
                decision_kind: Some(ConsoleDecisionKind::Wait),
                ..
            } => {
                *resumed_activation_id = Some(task.into());
            }
            ConsoleExplorerData::ExternalWait {
                key,
                closed_at,
                wake_reason,
                resumed_activation_id,
                ..
            } => {
                let Some(wake) = wake.filter(|wake| wake.key() == key) else {
                    continue;
                };
                *closed_at = Some(now);
                *resumed_activation_id = Some(task.into());
                *wake_reason = Some(match wake {
                    WorkflowWake::Event { .. } => ConsoleWakeReason::Event,
                    WorkflowWake::Timer { .. } => ConsoleWakeReason::Timer,
                    WorkflowWake::Timeout { .. } => ConsoleWakeReason::Timeout,
                });
            }
            _ => continue,
        }
        data.validate()?;
        sqlx::query("UPDATE workflow_explorer_records SET metadata_bytes=$3 WHERE activation_id=$1 AND kind=$2 AND record_key=$4")
            .bind(activation).bind(row.try_get::<String, _>("kind")?).bind(codec::encode(&data)?).bind(row.try_get::<String, _>("record_key")?).execute(&mut *connection).await?;
    }
    Ok(())
}

/// Bounded optional lifecycle evidence from an accepted attempt report. It does
/// not duplicate a replayed logical step or overwrite durable result acceptance.
pub(crate) async fn observe_locals(
    connection: &mut PgConnection,
    task_id: &str,
    attempt_id: &str,
    observations: &ledgence_worker_api::InvocationObservations,
) -> StoreResult<()> {
    observations.validate().map_err(ContractError::from)?;
    if observations.local_steps.is_empty() {
        return Ok(());
    }
    let keys: Vec<String> = observations
        .local_steps
        .iter()
        .map(|step| step.key.clone())
        .collect();
    let records = observations
        .local_steps
        .iter()
        .map(|step| {
            codec::encode(&ConsoleExplorerData::Local {
                key: step.key.clone(),
                callable: step.callable.clone(),
                accepted_at: None,
                accepting_attempt_id: None,
                observation: Some(ConsoleLocalObservation {
                    attempt_id: attempt_id.into(),
                    started_at: step.started_at_ms,
                    elapsed_us: ConsoleU64(step.elapsed_us),
                    state: codec::label(&step.state)?,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // One bounded batch. An ordinary task has no entrypoint and therefore adds no nodes.
    // Replay does not execute the callable: retain an earlier returned interval
    // instead of replacing its runtime with the near-zero cache lookup. The
    // original attempt observations still retain every replay occurrence.
    sqlx::query("INSERT INTO workflow_explorer_records(workflow_id,revision,kind,record_key,activation_id,entrypoint,metadata_bytes) SELECT p.workflow_id,p.revision,'local',r.key,p.activation_id,p.entrypoint,r.bytes FROM workflow_explorer_records p CROSS JOIN unnest($2::text[],$3::bytea[]) r(key,bytes) WHERE p.activation_id=$1 AND p.kind='entrypoint' ON CONFLICT (activation_id,kind,record_key) DO UPDATE SET metadata_bytes=convert_to((convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb || jsonb_build_object('callable',CASE WHEN convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->>'accepted_at' IS NULL THEN convert_from(EXCLUDED.metadata_bytes,'UTF8')::jsonb->>'callable' ELSE convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->>'callable' END,'observation',convert_from(EXCLUDED.metadata_bytes,'UTF8')::jsonb->'observation'))::text,'UTF8') WHERE (convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->>'accepted_at' IS NULL OR convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->>'callable'=convert_from(EXCLUDED.metadata_bytes,'UTF8')::jsonb->>'callable') AND NOT (convert_from(EXCLUDED.metadata_bytes,'UTF8')::jsonb->'observation'->>'state'='replayed' AND coalesce(convert_from(workflow_explorer_records.metadata_bytes,'UTF8')::jsonb->'observation'->>'state'='returned',false))")
        .bind(task_id).bind(keys).bind(records).execute(connection).await?;
    Ok(())
}
