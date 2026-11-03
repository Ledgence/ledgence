use super::*;
pub(super) async fn enqueue(
    tx: &mut PgConnection,
    workflow: &str,
    parking: &str,
    now: u64,
) -> StoreResult<()> {
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) VALUES('provider-wait:'||$2,$1,$2,'provider_wait',$3,$3) ON CONFLICT(id) DO UPDATE SET processed_at_ms=NULL,available_at_ms=LEAST(workflow_work.available_at_ms,EXCLUDED.available_at_ms),lease_token=NULL,lease_until_ms=NULL RETURNING id='provider-wait:'||$2 AND workflow_id=$1 AND task_id=$2 AND kind='provider_wait' AND processed_at_ms IS NULL")
      .bind(workflow).bind(parking).bind(codec::ms(now)?).fetch_optional(tx).await?;
    proof(valid, "provider wait obligation")
}
fn eligible(wait: &ProviderWait, r: &Retained) -> bool {
    summary(r).is_ok_and(|s| {
        s.candidate_revision
            .is_some_and(|rev| wait.after_effect_revision.is_none_or(|after| rev > after))
    })
}
pub(super) async fn notify(
    tx: &mut PgConnection,
    run: &RunRecord,
    r: &Retained,
    _state: &State,
    now: u64,
) -> StoreResult<()> {
    if run.snapshot.state == WorkflowState::Waiting
        && let Some(parking) = &run.provider_wait_activation
    {
        let wait = load_wait(tx, &run.snapshot.workflow_id, parking).await?;
        if wait.occurrence == r.binding.occurrence && eligible(&wait, r) {
            enqueue(tx, &run.snapshot.workflow_id, parking, now).await?;
        }
    }
    Ok(())
}
async fn load_wait(
    tx: &mut PgConnection,
    workflow: &str,
    parking: &str,
) -> StoreResult<ProviderWait> {
    let bytes:Vec<u8>=sqlx::query_scalar("SELECT wait_bytes FROM workflow_provider_waits WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL").bind(workflow).bind(parking).fetch_optional(tx).await?.ok_or_else(||corrupt("provider wait missing"))?;
    let w: ProviderWait = codec::decode(&bytes)?;
    w.validate()?;
    Ok(w)
}
pub(in crate::workflow) async fn install_wait(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    wait: &WorkflowWait,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    let WorkflowWait::ProviderOperation {
        key,
        occurrence,
        after_effect_revision,
    } = wait
    else {
        return Err(corrupt("provider wait kind").into());
    };
    let wait = ProviderWait {
        key: key.clone(),
        occurrence: occurrence.clone(),
        after_effect_revision: *after_effect_revision,
    };
    wait.validate()?;
    if occurrence.scope != run.snapshot.scope || occurrence.workflow_id != run.snapshot.workflow_id
    {
        return Err(ContractError::Conflict.into());
    }
    let r = retained::load(tx, occurrence)
        .await?
        .ok_or(ContractError::NotFound)?;
    let state = load(tx, &r).await?;
    if after_effect_revision.is_some_and(|rev| rev > r.summary.effect_revision) {
        return Err(ContractError::Conflict.into());
    }
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_provider_waits WHERE workflow_id=$1 AND logical_key=$2)").bind(&occurrence.workflow_id).bind(key).fetch_one(&mut *tx).await?;
    if exists {
        return Err(ContractError::Conflict.into());
    }
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO workflow_provider_waits(workflow_id,parking_activation_id,logical_key,wait_bytes,registered_at_ms) VALUES($1,$2,$3,$4,$5) RETURNING workflow_id=$1 AND parking_activation_id=$2 AND logical_key=$3 AND wait_bytes=$4 AND registered_at_ms=$5 AND closed_at_ms IS NULL AND wake_bytes IS NULL")
      .bind(&occurrence.workflow_id).bind(parking).bind(key).bind(codec::encode(&wait)?).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
    proof(valid, "provider wait registration")?;
    run.snapshot.state = WorkflowState::Waiting;
    run.snapshot.activation_id = None;
    run.wait_activation = Some(parking.into());
    run.wait_keys.clear();
    run.external_wait_key = None;
    run.native_wait_activation = None;
    run.provider_wait_activation = Some(parking.into());
    save_run(tx, run).await?;
    explorer::provider_wait(tx, parking, &wait, now).await?;
    if eligible(&wait, &r) {
        select(tx, run, parking, &wait, &r, &state, now)
            .await
            .map(Some)
    } else {
        Ok(None)
    }
}
pub(in crate::workflow) async fn apply_wait(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    if run.snapshot.state != WorkflowState::Waiting
        || run.provider_wait_activation.as_deref() != Some(parking)
    {
        return Ok(None);
    }
    let wait = load_wait(tx, &run.snapshot.workflow_id, parking).await?;
    let r = retained::load(tx, &wait.occurrence)
        .await?
        .ok_or_else(|| corrupt("provider wait occurrence"))?;
    let state = load(tx, &r).await?;
    if !eligible(&wait, &r) {
        return Ok(None);
    }
    select(tx, run, parking, &wait, &r, &state, now)
        .await
        .map(Some)
}
async fn select(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    parking: &str,
    wait: &ProviderWait,
    r: &Retained,
    state: &State,
    now: u64,
) -> StoreResult<TaskSnapshot> {
    let task_id = db::id(tx, "task").await?;
    let wake = ProviderWake {
        key: wait.key.clone(),
        occurrence: wait.occurrence.clone(),
        parking_activation_id: parking.into(),
        parking_revision: run
            .snapshot
            .revision
            .checked_sub(1)
            .ok_or_else(|| corrupt("provider parking revision"))?,
        resumed_activation_id: task_id.clone(),
        resumed_revision: run.snapshot.revision,
        selected_at: now.max(r.summary.updated_at),
        selected_effect_revision: r.summary.effect_revision,
        selection: state
            .candidate
            .clone()
            .ok_or_else(|| corrupt("provider candidate missing"))?,
    };
    wake.validate()?;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_provider_waits SET closed_at_ms=$3,wake_bytes=$4 WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL RETURNING workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms=$3 AND wake_bytes=$4").bind(&run.snapshot.workflow_id).bind(parking).bind(codec::ms(wake.selected_at)?).bind(codec::encode(&wake)?).fetch_optional(&mut *tx).await?;
    proof(valid, "provider wake selection")?;
    explorer::provider_closed(tx, parking, Some(&wake), wake.selected_at).await?;
    let task = schedule_activation_with_id(
        tx,
        run,
        BTreeMap::new(),
        Some(WorkflowWake::ProviderOperation(Box::new(wake))),
        now,
        task_id,
    )
    .await?;
    record_history(
        tx,
        &run.snapshot.workflow_id,
        Some(&task.task_id),
        now,
        "provider_operation_resumed",
    )
    .await?;
    Ok(task)
}
pub(in crate::workflow) async fn close_wait(
    tx: &mut PgConnection,
    run: &mut RunRecord,
    now: u64,
) -> StoreResult<()> {
    if let Some(parking) = run.provider_wait_activation.take() {
        let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_provider_waits SET closed_at_ms=$3 WHERE workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms IS NULL RETURNING workflow_id=$1 AND parking_activation_id=$2 AND closed_at_ms=$3 AND wake_bytes IS NULL").bind(&run.snapshot.workflow_id).bind(&parking).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
        proof(valid, "provider wait closure")?;
        sqlx::query("UPDATE workflow_work SET processed_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id='provider-wait:'||$1 AND processed_at_ms IS NULL").bind(&parking).bind(codec::ms(now)?).execute(&mut *tx).await?;
        explorer::provider_closed(tx, &parking, None, now).await?;
    }
    Ok(())
}
