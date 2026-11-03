//! Native rendezvous identity is separate from user event keys. Callers hold the workflow lock.
use super::effect_retained::{self as retained, Retained};
use super::*;
pub(super) async fn load_wait(
    tx: &mut PgConnection,
    workflow: &str,
    parking: &str,
) -> StoreResult<WorkflowWait> {
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT wait_bytes FROM workflow_effect_waits WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL")
        .bind(workflow).bind(parking).fetch_optional(tx).await?.ok_or_else(||corrupt("native wait missing or closed"))?;
    let wait: WorkflowWait = codec::decode(&bytes)?;
    wait.validate()?;
    if !matches!(wait, WorkflowWait::EffectResolution { .. }) {
        return Err(corrupt("native wait kind").into());
    }
    Ok(wait)
}
pub(super) fn eligible(wait: &WorkflowWait, r: &Retained) -> Result<bool> {
    let WorkflowWait::EffectResolution {
        occurrence,
        after_resolution_id,
        ..
    } = wait
    else {
        return Err(corrupt("native wait kind"));
    };
    Ok(occurrence == &r.binding.occurrence
        && r.summary
            .operator_resolution
            .as_ref()
            .is_some_and(|s| Some(&s.reference.resolution_id) != after_resolution_id.as_ref()))
}
pub(super) async fn enqueue(
    tx: &mut PgConnection,
    workflow: &str,
    parking: &str,
    now: u64,
) -> StoreResult<()> {
    let proof:Option<bool>=sqlx::query_scalar("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) VALUES('effect-wait:'||$2,$1,$2,'effect_wait',$3,$3) ON CONFLICT(id) DO UPDATE SET available_at_ms=LEAST(workflow_work.available_at_ms,EXCLUDED.available_at_ms) WHERE workflow_work.processed_at_ms IS NULL RETURNING id='effect-wait:'||$2 AND workflow_id=$1 AND task_id=$2 AND kind='effect_wait' AND processed_at_ms IS NULL")
        .bind(workflow).bind(parking).bind(codec::ms(now)?).fetch_optional(tx).await?;
    if proof != Some(true) {
        return Err(corrupt("native wait obligation suppressed or changed").into());
    }
    Ok(())
}
pub(super) async fn install(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    wait: &WorkflowWait,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    wait.validate()?;
    let WorkflowWait::EffectResolution {
        key,
        occurrence,
        after_resolution_id,
    } = wait
    else {
        return Err(corrupt("native wait kind").into());
    };
    if occurrence.scope != run.snapshot.scope || occurrence.workflow_id != run.snapshot.workflow_id
    {
        return Err(ContractError::Conflict.into());
    }
    let r = retained::load(tx, occurrence)
        .await?
        .ok_or(ContractError::NotFound)?;
    if let Some(id) = after_resolution_id {
        let old = retained::entry(tx, occurrence, "assessment", id)
            .await?
            .ok_or(ContractError::Conflict)?;
        if old.entry.resolution_reference().is_none() {
            return Err(corrupt("native wait predecessor").into());
        }
    }
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_effect_waits WHERE workflow_id=$1 AND logical_key=$2)").bind(&run.snapshot.workflow_id).bind(key).fetch_one(&mut *tx).await?;
    if exists {
        return Err(ContractError::Conflict.into());
    }
    let bytes = codec::encode(wait)?;
    let proof:Option<bool>=sqlx::query_scalar("INSERT INTO workflow_effect_waits(workflow_id,parking_activation_id,logical_key,wait_bytes,registered_at_ms) VALUES($1,$2,$3,$4,$5) RETURNING workflow_id=$1 AND parking_activation_id=$2 AND logical_key=$3 AND wait_bytes=$4 AND registered_at_ms=$5 AND closed_at_ms IS NULL AND wake_bytes IS NULL")
        .bind(&run.snapshot.workflow_id).bind(parking).bind(key).bind(bytes).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
    if proof != Some(true) {
        return Err(corrupt("native wait registration suppressed or changed").into());
    }
    run.snapshot.state = WorkflowState::Waiting;
    run.snapshot.activation_id = None;
    run.wait_activation = Some(parking.into());
    run.wait_keys.clear();
    run.external_wait_key = None;
    run.native_wait_activation = Some(parking.into());
    save_run(tx, run).await?;
    explorer::native_wait(tx, parking, wait, now).await?;
    if eligible(wait, &r)? {
        select(tx, run, parking, wait, &r, now).await.map(Some)
    } else {
        Ok(None)
    }
}
pub(super) async fn apply(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    if run.snapshot.state != WorkflowState::Waiting
        || run.native_wait_activation.as_deref() != Some(parking)
    {
        return Ok(None);
    }
    let wait = load_wait(tx, &run.snapshot.workflow_id, parking).await?;
    let WorkflowWait::EffectResolution { occurrence, .. } = &wait else {
        return Err(corrupt("native wait kind").into());
    };
    let r = retained::load(tx, occurrence)
        .await?
        .ok_or_else(|| corrupt("native wait occurrence"))?;
    if !eligible(&wait, &r)? {
        return Err(corrupt("native wait obligation without eligible assessment").into());
    }
    select(tx, run, parking, &wait, &r, now).await.map(Some)
}
async fn select(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    wait: &WorkflowWait,
    r: &Retained,
    now: u64,
) -> StoreResult<TaskSnapshot> {
    let WorkflowWait::EffectResolution {
        key, occurrence, ..
    } = wait
    else {
        return Err(corrupt("native wait kind").into());
    };
    let resolution = r
        .summary
        .operator_resolution
        .as_ref()
        .ok_or_else(|| corrupt("native assessment missing"))?;
    let EffectEvidenceEntry::Assessment { assessment, .. } = r
        .current_assessment
        .as_ref()
        .ok_or_else(|| corrupt("native assessment missing"))?
    else {
        return Err(corrupt("native assessment kind").into());
    };
    let outcome = if let Some(sequence) = resolution.first_conflicting_entry_sequence {
        EffectResolutionOutcome::Disputed {
            first_conflicting_entry_sequence: sequence,
        }
    } else {
        match assessment {
            EffectResolutionAssessment::RecordSuccess { result } => {
                EffectResolutionOutcome::RecordedSuccess {
                    result: result.clone(),
                }
            }
            EffectResolutionAssessment::RecordFailure { failure } => {
                EffectResolutionOutcome::RecordedFailure {
                    failure: failure.clone(),
                }
            }
            EffectResolutionAssessment::CloseUnknown => EffectResolutionOutcome::ClosedUnknown,
        }
    };
    let task_id = db::id(tx, "task").await?;
    let reference = &resolution.reference;
    // No incomplete public wake is constructed while allocating the next activation.
    let wake = EffectResolutionWake {
        key: key.clone(),
        occurrence: occurrence.clone(),
        parking_activation_id: parking.into(),
        parking_revision: run
            .snapshot
            .revision
            .checked_sub(1)
            .ok_or_else(|| corrupt("native parking revision"))?,
        resumed_activation_id: task_id.clone(),
        resumed_revision: run.snapshot.revision,
        resolution_id: reference.resolution_id.clone(),
        accepted_at: reference.accepted_at,
        accepted_revision: reference.accepted_revision,
        basis_revision: reference.basis_revision,
        selected_at: now.max(reference.accepted_at),
        selected_effect_revision: r.summary.effect_revision,
        outcome,
    };
    wake.validate()?;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_effect_waits SET closed_at_ms=$3,wake_bytes=$4 WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL RETURNING workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms=$3 AND wake_bytes=$4")
        .bind(&run.snapshot.workflow_id).bind(parking).bind(codec::ms(wake.selected_at)?).bind(codec::encode(&wake)?).fetch_optional(&mut *tx).await?;
    if valid != Some(true) {
        return Err(corrupt("native wait selection suppressed or changed").into());
    }
    explorer::native_closed(tx, parking, Some(&wake), wake.selected_at).await?;
    let task = schedule_activation_with_id(
        tx,
        run,
        BTreeMap::new(),
        Some(WorkflowWake::EffectResolution(Box::new(wake))),
        now,
        task_id,
    )
    .await?;
    record_history(
        tx,
        &run.snapshot.workflow_id,
        Some(&task.task_id),
        now,
        "effect_resolution_resumed",
    )
    .await?;
    Ok(task)
}
pub(super) async fn close(tx: &mut PgConnection, run: &mut RunRecord, now: u64) -> StoreResult<()> {
    if let Some(parking) = run.native_wait_activation.take() {
        let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_effect_waits SET closed_at_ms=$3 WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL RETURNING workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms=$3 AND wake_bytes IS NULL")
            .bind(&run.snapshot.workflow_id).bind(&parking).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
        if valid != Some(true) {
            return Err(corrupt("native wait closure suppressed or changed").into());
        }
        sqlx::query("UPDATE workflow_work SET processed_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id='effect-wait:'||$1 AND processed_at_ms IS NULL").bind(&parking).bind(codec::ms(now)?).execute(&mut *tx).await?;
        explorer::native_closed(tx, &parking, None, now).await?;
    }
    Ok(())
}
