use super::*;
use crate::workflow::admission::scheduler::refresh_due;
/// Return at most one row per indexed obligation family. Waiting rows use
/// available_at_ms=0 once eligible, so the FIFO arm seeks without sorting a
/// large delayed population. Both subjects share the same request sequence.
pub(crate) async fn candidate(
    tx: &mut PgConnection,
    r: &capacity::Resource,
    now: u64,
    dispatch: bool,
) -> StoreResult<Option<(i32, i64, TaskAdmissionOccurrence)>> {
    let row = sqlx::query(include_str!("../../queries/task_admission_candidate.sql"))
        .bind(&r.record.scope.tenant_id)
        .bind(&r.record.scope.namespace)
        .bind(&r.record.name)
        .bind(r.record.revision as i64)
        .bind(codec::ms(now)?)
        .bind(
            dispatch
                && r.record.policy.enabled
                && r.started + r.offered < u64::from(r.record.policy.max_occupancy),
        )
        .fetch_optional(&mut *tx)
        .await?;
    row.map(|row| {
        Ok((
            row.try_get("priority")?,
            row.try_get("request_seq")?,
            TaskAdmissionOccurrence {
                scope: r.record.scope.clone(),
                task_id: row.try_get("task_id")?,
                generation: u32::try_from(row.try_get::<i64, _>("generation")?)
                    .map_err(|_| corrupt("generation"))?,
            },
        ))
    })
    .transpose()
}
pub(crate) async fn due(
    tx: &mut PgConnection,
    r: &capacity::Resource,
    now: u64,
) -> StoreResult<(Option<u64>, bool)> {
    let (due, waiting): (Option<i64>, bool) =
        sqlx::query_as(include_str!("../../queries/task_admission_due.sql"))
            .bind(&r.record.scope.tenant_id)
            .bind(&r.record.scope.namespace)
            .bind(&r.record.name)
            .bind(r.record.revision as i64)
            .bind(codec::ms(now)?)
            .fetch_one(tx)
            .await?;
    Ok((due.map(codec::timestamp).transpose()?, waiting))
}
pub(crate) async fn first_ready(
    tx: &mut PgConnection,
    r: &capacity::Resource,
) -> StoreResult<Option<i64>> {
    Ok(
        sqlx::query_scalar(include_str!("../../queries/admission_first_ready.sql"))
            .bind(&r.record.scope.tenant_id)
            .bind(&r.record.scope.namespace)
            .bind(&r.record.name)
            .fetch_one(tx)
            .await?,
    )
}
pub(crate) async fn tick(
    tx: &mut PgConnection,
    o: &TaskAdmissionOccurrence,
    name: &str,
    dispatch: bool,
) -> StoreResult<(Option<TaskSnapshot>, bool, bool)> {
    let row=sqlx::query("SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 FOR NO KEY UPDATE NOWAIT").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).fetch_optional(&mut *tx).await?;
    let task = match row {
        Some(row) if row.try_get::<Option<i64>, _>("retiring_at_ms")?.is_none() => {
            Some(codec::task(&row)?)
        }
        _ => None,
    };
    let row=sqlx::query("SELECT * FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 FOR UPDATE NOWAIT").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(name).fetch_one(&mut *tx).await?;
    let mut r = capacity::resource_row(&row)?;
    let mut a = request(tx, o).await?;
    let now = db::now(tx).await?;
    if now < r.high_water || now < a.snapshot.updated_at {
        r.wake(r.high_water.max(a.snapshot.updated_at));
        capacity::save_resource(tx, &r).await?;
        return Ok((None, false, false));
    }
    r.refill(now)?;
    let mut offered = None;
    let mut expired = false;
    match a.snapshot.state {
        TaskAdmissionState::Offered | TaskAdmissionState::Preparing => {
            let offer = a
                .snapshot
                .offer
                .as_ref()
                .ok_or_else(|| corrupt("scheduled offer"))?;
            if offer.policy_revision != r.record.revision
                || !r.record.policy.enabled
                || now >= offer.expires_at
            {
                lifecycle::repark(tx, &mut a, &mut r, now).await?;
                expired = true;
            } else if a.snapshot.state == TaskAdmissionState::Preparing
                && a.check_at.is_some_and(|at| at <= now)
            {
                let owner = a
                    .snapshot
                    .owner
                    .as_ref()
                    .ok_or_else(|| corrupt("preparation owner"))?;
                let live = if let Some(task) = &task {
                    if task.state == TaskState::Active
                        && task.current_attempt_id.as_ref() == Some(&owner.attempt_id)
                    {
                        let attempt = db::load_attempt(tx, task, &owner.attempt_id).await?;
                        Some(
                            attempt
                                .lease
                                .expires_at
                                .min(attempt.authority_deadline)
                                .min(attempt.deadline),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(next) = live.filter(|next| *next > now) {
                    a.check_at = Some(next);
                    save(tx, &a).await?;
                } else {
                    release_offer(&mut a, &mut r, now)?;
                    save(tx, &a).await?;
                    expired = true;
                }
            }
        }
        TaskAdmissionState::PossiblyDispatched if a.check_at.is_some_and(|at| at <= now) => {
            let owner = a
                .snapshot
                .owner
                .as_ref()
                .ok_or_else(|| corrupt("start owner"))?;
            let live = if let Some(task) = &task {
                if task.state == TaskState::Active
                    && task.current_attempt_id.as_ref() == Some(&owner.attempt_id)
                    && task.cancel_requested_at.is_none()
                {
                    let attempt = db::load_attempt(tx, task, &owner.attempt_id).await?;
                    Some(attempt.lease.expires_at.min(attempt.authority_deadline))
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(next) = live.filter(|next| *next > now) {
                a.check_at = Some(next);
            } else {
                a.check_at = None;
                a.snapshot.state = TaskAdmissionState::UnknownOccupied;
                r.unknown = r
                    .unknown
                    .checked_add(1)
                    .ok_or_else(|| corrupt("unknown count overflow"))?;
                a.bump(now)?;
            }
            save(tx, &a).await?;
        }
        TaskAdmissionState::Waiting => {
            let task = task.ok_or_else(|| corrupt("waiting task is missing"))?;
            if task.state != TaskState::Queued
                || task.cancel_requested_at.is_some()
                || task.attempt_count.checked_add(1) != Some(o.generation)
            {
                a.snapshot.state = TaskAdmissionState::ClosedBeforeStart;
                a.bump(now)?;
                save(tx, &a).await?;
            } else if a.available_at != 0 && a.available_at <= now {
                a.available_at = 0;
                save(tx, &a).await?;
            } else if a.available_at == 0
                && dispatch
                && r.record.policy.enabled
                && r.capacity()? > 0
                && first_ready(tx, &r).await? == Some(a.sequence)
            {
                a.offer_generation = a
                    .offer_generation
                    .checked_add(1)
                    .ok_or_else(|| corrupt("offer generation overflow"))?;
                a.snapshot.offer = Some(TaskAdmissionOffer {
                    generation: a.offer_generation,
                    policy_revision: r.record.revision,
                    expires_at: now
                        .checked_add(u64::from(r.record.policy.offer_timeout_ms))
                        .ok_or_else(|| corrupt("offer expiry overflow"))?,
                });
                a.snapshot.state = TaskAdmissionState::Offered;
                a.bump(now)?;
                r.offered = r
                    .offered
                    .checked_add(1)
                    .ok_or_else(|| corrupt("offer count overflow"))?;
                save(tx, &a).await?;
                ready(tx, &o.task_id, true).await?;
                offered = Some(task);
            }
        }
        _ => {}
    }
    refresh_due(tx, &mut r, now).await?;
    let again = (if dispatch { r.due } else { r.quarantined_due }).is_some_and(|at| at <= now);
    capacity::save_resource(tx, &r).await?;
    Ok((offered, expired, again))
}
