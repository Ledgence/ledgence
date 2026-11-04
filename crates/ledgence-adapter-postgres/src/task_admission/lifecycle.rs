use super::*;
/// Accept or retry a root task. Its queue intent is withheld until an offer is
/// durable; a resource wake, not a worker or broker message, owns the wait.
pub(crate) async fn register(tx: &mut PgConnection, task: &TaskSnapshot) -> StoreResult<()> {
    let Some(requirement) = &task.input.admission else {
        return Ok(());
    };
    if task.workflow_id.is_some()
        || task.workflow_activation_id.is_some()
        || task.provider_probe.is_some()
    {
        return Err(ContractError::InvalidInput(
            "task admission requires a standalone task".into(),
        )
        .into());
    }
    let number = task
        .attempt_count
        .checked_add(1)
        .filter(|n| *n <= MAX_TASK_ATTEMPT_GENERATION)
        .ok_or_else(|| corrupt("attempt generation exhausted"))?;
    let o = TaskAdmissionOccurrence {
        scope: task.scope(),
        task_id: task.task_id.clone(),
        generation: number,
    };
    let mut r = capacity::resource(tx, &o.scope, &requirement.resource, true).await?;
    let now = db::now(tx).await?;
    let snapshot = TaskAdmissionSnapshot {
        occurrence: o.clone(),
        resource: requirement.resource.clone(),
        program: task.descriptor.clone(),
        state: TaskAdmissionState::Waiting,
        revision: 1,
        created_at: now,
        updated_at: now,
        offer: None,
        owner: None,
        source: None,
        close: None,
    };
    snapshot.validate()?;
    let available_at = if task.available_at <= now {
        0
    } else {
        task.available_at
    };
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO task_admission_requests(tenant_id,namespace,task_id,generation,resource_name,state,revision,created_at_ms,updated_at_ms,available_at_ms,snapshot_bytes,origin_queue,fair_group) VALUES($1,$2,$3,$4,$5,'waiting',1,$6,$6,$7,$8,$9,$10) RETURNING snapshot_bytes=$8 AND state='waiting' AND generation=$4 AND available_at_ms=$7 AND origin_queue=$9 AND fair_group IS NOT DISTINCT FROM $10")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).bind(i64::from(number)).bind(&requirement.resource).bind(codec::ms(now)?).bind(codec::ms(available_at)?).bind(codec::encode(&snapshot)?).bind(&task.input.queue).bind(crate::workflow::admission::fair::group(&r,&task.input.queue)).fetch_optional(&mut *tx).await?;
    proof(valid, "waiting registration")?;
    let projected:Option<bool>=sqlx::query_scalar("UPDATE tasks SET admission_resource=$2,admission_ready=false WHERE task_id=$1 RETURNING admission_resource=$2 AND NOT admission_ready")
        .bind(&o.task_id).bind(&requirement.resource).fetch_optional(&mut *tx).await?;
    proof(projected, "waiting task projection")?;
    r.wake(now.max(r.high_water));
    capacity::save_resource(tx, &r).await
}
/// Validate after taking the task lock and before allocating an attempt. An
/// expired unclaimed offer rotates behind other work, with a durable rewait.
pub(crate) async fn validate_candidate(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
    enabled: bool,
) -> StoreResult<bool> {
    let Some(requirement) = &task.input.admission else {
        return Ok(true);
    };
    let o = next_occurrence(task)?;
    let mut r = capacity::resource(tx, &o.scope, &requirement.resource, true).await?;
    let mut a = request(tx, &o).await?;
    let now = db::now(tx).await?;
    if now < r.high_water || now < a.snapshot.updated_at {
        return Err(ContractError::Unavailable(
            "task admission clock has not reached its high-water mark".into(),
        )
        .into());
    }
    if a.snapshot.state == TaskAdmissionState::Waiting {
        return Ok(false);
    }
    if a.snapshot.state != TaskAdmissionState::Offered {
        return Err(corrupt("queued task lacks a recoverable admission offer or wait").into());
    }
    let offer = a
        .snapshot
        .offer
        .as_ref()
        .ok_or_else(|| corrupt("missing candidate offer"))?;
    if offer.policy_revision != r.record.revision
        || !enabled
        || !r.record.policy.enabled
        || now >= offer.expires_at
    {
        repark(tx, &mut a, &mut r, now).await?;
        return Ok(false);
    }
    Ok(true)
}
/// An older broker worker understands Deferred, but cannot execute a governed
/// assignment. Relinquish only an unclaimed offer. The wait owns redelivery;
/// duplicates never advance its backoff or consume an attempt.
pub(crate) async fn defer_legacy_claim(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
) -> StoreResult<u64> {
    let o = next_occurrence(task)?;
    let name = &task
        .input
        .admission
        .as_ref()
        .ok_or_else(|| corrupt("missing broker requirement"))?
        .resource;
    let mut r = capacity::resource(tx, &o.scope, name, true).await?;
    let mut a = request(tx, &o).await?;
    let now = db::now(tx)
        .await?
        .max(r.high_water)
        .max(a.snapshot.updated_at);
    match a.snapshot.state {
        TaskAdmissionState::Offered => repark(tx, &mut a, &mut r, now).await?,
        TaskAdmissionState::Waiting if a.last_rewait_at.is_none() => {
            repark(tx, &mut a, &mut r, now).await?
        }
        TaskAdmissionState::Waiting => {}
        _ => return Err(corrupt("queued task lacks durable broker wait").into()),
    }
    // available_at=0 is an index optimization, not a delivery promise.
    a.last_rewait_at
        .ok_or_else(|| corrupt("missing durable legacy handoff time").into())
}
pub(super) fn next_occurrence(task: &TaskSnapshot) -> StoreResult<TaskAdmissionOccurrence> {
    Ok(TaskAdmissionOccurrence {
        scope: task.scope(),
        task_id: task.task_id.clone(),
        generation: task
            .attempt_count
            .checked_add(1)
            .filter(|n| *n <= MAX_TASK_ATTEMPT_GENERATION)
            .ok_or_else(|| corrupt("attempt generation exhausted"))?,
    })
}
pub(super) async fn repark(
    tx: &mut PgConnection,
    a: &mut Request,
    r: &mut capacity::Resource,
    now: u64,
) -> StoreResult<()> {
    release_offer(a, r, now)?;
    if a.snapshot.state == TaskAdmissionState::Waiting {
        a.available_at = a.available_at.max(
            now.checked_add(TASK_ADMISSION_DEFERRAL_BACKOFF_MS)
                .ok_or_else(|| corrupt("rewait deadline overflow"))?,
        );
        a.last_rewait_at = Some(a.available_at);
        sqlx::query("UPDATE task_admission_requests SET request_seq=nextval('admission_requests_request_seq_seq') WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND generation=$4")
            .bind(&a.snapshot.occurrence.scope.tenant_id).bind(&a.snapshot.occurrence.scope.namespace).bind(&a.snapshot.occurrence.task_id).bind(i64::from(a.snapshot.occurrence.generation)).execute(&mut *tx).await?;
        ready(tx, &a.snapshot.occurrence.task_id, false).await?;
    }
    save(tx, a).await?;
    capacity::save_resource(tx, r).await
}
/// Convert a short unclaimed offer into preparation bounded by the attempt
/// deadline. It continues to reserve occupancy/credit but consumes no rate.
pub(crate) async fn bind_preparation(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
    attempt: &mut AttemptSnapshot,
    session: &WorkerSession,
) -> StoreResult<Option<TaskAdmissionAssignment>> {
    let Some(requirement) = &task.input.admission else {
        return Ok(None);
    };
    if !session.features.iter().any(|v| v == TASK_ADMISSION_FEATURE) {
        return Err(ContractError::Forbidden.into());
    }
    let o = TaskAdmissionOccurrence::from(&attempt.lease.owner);
    let mut r = capacity::resource(tx, &o.scope, &requirement.resource, true).await?;
    let mut a = request(tx, &o).await?;
    let now = db::now(tx).await?;
    let offer = a.snapshot.offer.as_mut().ok_or(ContractError::Conflict)?;
    if a.snapshot.state != TaskAdmissionState::Offered
        || offer.policy_revision != r.record.revision
        || now >= offer.expires_at
        || !r.record.policy.enabled
        || now < r.high_water
        || now < a.snapshot.updated_at
    {
        return Err(ContractError::Conflict.into());
    }
    let worker: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT admission_worker_bytes FROM worker_sessions WHERE session_id=$1",
    )
    .bind(&session.id)
    .fetch_one(&mut *tx)
    .await?;
    let worker: AdmissionWorkerIdentity = codec::decode(&worker.ok_or(ContractError::Forbidden)?)?;
    worker.validate()?;
    let marker = TaskAdmissionAssignment {
        resource: requirement.resource.clone(),
        offer_generation: offer.generation,
    };
    marker.validate()?;
    offer.expires_at = attempt.deadline;
    a.snapshot.owner = Some(attempt.lease.owner.clone());
    a.snapshot.state = TaskAdmissionState::Preparing;
    a.worker = Some(worker);
    a.check_at = Some(attempt.lease.expires_at.min(attempt.deadline));
    a.bump(now)?;
    r.wake(now);
    attempt.task_admission = Some(marker.clone());
    save(tx, &a).await?;
    capacity::save_resource(tx, &r).await?;
    Ok(Some(marker))
}
/// The ordinary lifecycle never touches this ledger. Caller already holds task
/// lock; acquisition and ready-Start perform their own atomic reservation writes.
pub(crate) async fn apply_transition<R>(
    tx: &mut PgConnection,
    transition: &core::Transition<R>,
) -> StoreResult<()> {
    let task = &transition.task;
    if task.input.admission.is_none() || transition.history.is_empty() {
        return Ok(());
    }
    let changed = |reason| transition.history.iter().any(|e| e.reason == reason);
    if changed(TransitionReason::Claimed) || changed(TransitionReason::DispatchAuthorized) {
        return Ok(());
    }
    let at = transition
        .history
        .last()
        .ok_or_else(|| corrupt("empty transition"))?
        .at;
    if let Some(attempt) = &transition.attempt {
        let o = TaskAdmissionOccurrence::from(&attempt.lease.owner);
        let mut r = capacity::resource(
            tx,
            &o.scope,
            &task
                .input
                .admission
                .as_ref()
                .ok_or_else(|| corrupt("requirement"))?
                .resource,
            true,
        )
        .await?;
        let mut a = request(tx, &o).await?;
        let now = db::now(tx).await?.max(at);
        let mut dirty = false;
        if attempt.state != AttemptState::Active
            || task.cancel_requested_at.is_some()
            || (a.snapshot.source.is_none() && attempt.settlement.is_some())
        {
            if a.snapshot.source.is_none() {
                dirty = release_offer(&mut a, &mut r, now)?;
            } else if a.snapshot.state == TaskAdmissionState::PossiblyDispatched {
                a.snapshot.state = TaskAdmissionState::UnknownOccupied;
                a.check_at = None;
                r.unknown = r
                    .unknown
                    .checked_add(1)
                    .ok_or_else(|| corrupt("unknown count overflow"))?;
                a.bump(now)?;
                r.wake(now);
                dirty = true;
            }
        }
        // Ordinary settlement cannot authenticate the admission source actor.
        // Only a source-bound Close (or audited operator release) frees a start.
        if dirty {
            save(tx, &a).await?;
            capacity::save_resource(tx, &r).await?;
        }
    } else if task.state == TaskState::Cancelled {
        let o = next_occurrence(task)?;
        let mut r = capacity::resource(
            tx,
            &o.scope,
            &task
                .input
                .admission
                .as_ref()
                .ok_or_else(|| corrupt("requirement"))?
                .resource,
            true,
        )
        .await?;
        let mut a = request(tx, &o).await?;
        let now = db::now(tx).await?;
        release_offer(&mut a, &mut r, now)?;
        a.snapshot.state = TaskAdmissionState::ClosedBeforeStart;
        a.check_at = None;
        a.bump(now)?;
        r.wake(now);
        save(tx, &a).await?;
        capacity::save_resource(tx, &r).await?;
    }
    if task.state == TaskState::Queued
        && (changed(TransitionReason::RetryScheduled)
            || changed(TransitionReason::AdmissionDeferred))
    {
        register(tx, task).await?;
    }
    Ok(())
}
/// No Start exists and no real preparation failure was already closed. Serialize
/// the tombstone and immutable Deferred attempt with the retry registration.
pub(crate) async fn expire_transition(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    now: u64,
) -> StoreResult<core::Transition<bool>> {
    if task.input.admission.is_none()
        || attempt.execution_may_have_started
        || attempt.settlement.is_some()
        || attempt.state != AttemptState::Active
        || (now < attempt.lease.expires_at && now < attempt.authority_deadline)
    {
        return Ok(core::expire(task, attempt, now)?);
    }
    let o = TaskAdmissionOccurrence::from(&attempt.lease.owner);
    let name = resource_name(tx, &o).await?;
    let _ = capacity::resource(tx, &o.scope, &name, true).await?;
    let a = request(tx, &o).await?;
    if a.snapshot.source.is_some() {
        return Err(corrupt("source without started attempt").into());
    }
    if a.snapshot
        .close
        .as_ref()
        .is_some_and(|close| close.outcome == TaskAdmissionCloseOutcome::Quiescent)
    {
        return Ok(core::expire(task, attempt, now)?);
    }
    let mut t = core::defer_task_admission(
        task,
        attempt,
        &attempt.lease.owner,
        Quiescence::Unconfirmed,
        now,
    )?;
    t.history.insert(
        0,
        HistoryEvent {
            task_id: task.task_id.clone(),
            attempt_id: Some(attempt.lease.owner.attempt_id.clone()),
            at: now,
            reason: TransitionReason::LeaseExpired,
        },
    );
    Ok(core::Transition {
        task: t.task,
        attempt: t.attempt,
        history: t.history,
        reply: true,
    })
}

/// Freeze the same accepted marker in the attempt and the initial wire reply.
/// Historical acquire and broker claim replays reconstruct it from the attempt.
pub(crate) async fn prepare_assignment(
    tx: &mut PgConnection,
    t: &mut core::AcquireTransition,
    session: &WorkerSession,
) -> StoreResult<()> {
    let Some(changes) = &mut t.changes else {
        return Ok(());
    };
    if changes.task.input.admission.is_none() {
        return Ok(());
    }
    let attempt = changes
        .attempt
        .as_mut()
        .ok_or_else(|| corrupt("missing assigned attempt"))?;
    let marker = bind_preparation(tx, &changes.task, attempt, session).await?;
    let AcquireReply::Assigned { assignment, .. } = &mut t.reply else {
        return Err(corrupt("missing assigned reply").into());
    };
    assignment.task_admission = marker;
    Ok(())
}
