use super::*;
use std::collections::VecDeque;
impl PostgresStore {
    pub(super) async fn advance_admissions_once(
        &self,
        q: &AdmissionMaintenanceQuery,
    ) -> StoreResult<AdmissionMaintenanceProgress> {
        q.validate()?;
        let config = self.admission_configured()?;
        let scope = &config.audience.scope;
        let dispatch = config.dispatch == AdmissionDispatchPolicy::Enabled;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        let now = db::now(&mut tx).await?;
        // A local keyset cursor rotates bounded pages even when every first-page
        // workflow is busy. The database remains authoritative; a restart may
        // revisit a page but cannot lose an obligation.
        let after = self
            .admission_scan_cursor
            .lock()
            .map_err(|_| corrupt("admission scan cursor"))?
            .clone();
        let mut page = due_page(&mut tx, scope, dispatch, now, after.as_ref(), q.limit).await?;
        if page.is_empty() && after.is_some() {
            page = due_page(&mut tx, scope, dispatch, now, None, q.limit).await?;
        }
        *self
            .admission_scan_cursor
            .lock()
            .map_err(|_| corrupt("admission scan cursor"))? = page.last().cloned();
        tx.commit().await?;
        let mut pending: VecDeque<String> = page.into_iter().map(|(_, name)| name).collect();
        let mut progress = AdmissionMaintenanceProgress::default();
        while progress.examined < q.limit {
            let Some(name) = pending.pop_front() else {
                break;
            };
            progress.examined += 1;
            match self.admission_tick(scope, &name, dispatch).await {
                Ok((task, expired, again)) => {
                    if let Some(task) = task {
                        progress.resumed += 1;
                        if task.workflow_activation_id.is_some() {
                            progress.offered += u32::from(!expired);
                        }
                        self.workflow_wakes(&[task]);
                    }
                    progress.expired += u32::from(expired);
                    if again {
                        pending.push_back(name);
                    }
                }
                Err(StoreError::Database(error))
                    if error.as_database_error().and_then(|e| e.code()).as_deref()
                        == Some("55P03") => {}
                Err(error) => return Err(error),
            }
        }
        Ok(progress)
    }
    async fn admission_tick(
        &self,
        scope: &Scope,
        name: &str,
        dispatch: bool,
    ) -> StoreResult<(Option<TaskSnapshot>, bool, bool)> {
        // Pick a bounded candidate before locking. Recheck FIFO and all authority
        // after acquiring workflow -> resource; a hot resource never blocks others.
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let observed = resource(&mut tx, scope, name, false).await?;
        let sampled = db::now(&mut tx).await?;
        let candidate = choose(&mut tx, &observed, sampled, dispatch).await?;
        let mut run = if let Some(o) = &candidate {
            let row=sqlx::query("SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE workflow_id=$1 AND tenant_id=$2 AND namespace=$3 FOR NO KEY UPDATE NOWAIT").bind(&o.workflow_id).bind(&scope.tenant_id).bind(&scope.namespace).fetch_optional(&mut *tx).await?;
            row.as_ref().map(run_record).transpose()?
        } else {
            None
        };
        let row=sqlx::query("SELECT * FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 FOR UPDATE NOWAIT").bind(&scope.tenant_id).bind(&scope.namespace).bind(name).fetch_one(&mut *tx).await?;
        let mut r = resource_row(&row)?;
        let now = db::now(&mut tx).await?;
        // A backward database clock must not create a wake whose offer already
        // predates its monotone selection. Defer authority, retaining all debt.
        if now < r.high_water {
            r.wake(r.high_water);
            save_resource(&mut tx, &r).await?;
            tx.commit().await?;
            return Ok((None, false, false));
        }
        r.refill(now)?;
        let mut task = None;
        let mut expired = false;
        if let Some(o) = candidate {
            let mut a = request(&mut tx, &o).await?;
            if now < a.snapshot.updated_at {
                r.wake(a.snapshot.updated_at);
                save_resource(&mut tx, &r).await?;
                tx.commit().await?;
                return Ok((None, false, false));
            }
            let closed = run.as_ref().is_none_or(|run| {
                run.snapshot.state.is_terminal()
                    || matches!(
                        run.snapshot.state,
                        WorkflowState::Failing | WorkflowState::Cancelling
                    )
            });
            if closed {
                close_request(&mut a, &mut r, now)?;
                a.parking = None;
                save_request(&mut tx, &a).await?;
            } else if invalidate_offer(&mut a, &mut r, now)? {
                expired = true;
                save_request(&mut tx, &a).await?;
            } else if a.snapshot.state == AdmissionState::PossiblyDispatched
                && a.check_at.is_none_or(|at| at <= now)
            {
                let source = a
                    .snapshot
                    .source
                    .as_ref()
                    .ok_or_else(|| corrupt("admission source"))?;
                a.check_at = execution::source_check_at(&mut tx, source, now).await?;
                if a.check_at.is_none() {
                    a.snapshot.state = AdmissionState::UnknownOccupied;
                    r.unknown += 1;
                    a.bump(now)?;
                }
                save_request(&mut tx, &a).await?;
            } else if a.snapshot.state == AdmissionState::Waiting {
                let run = run
                    .as_mut()
                    .ok_or_else(|| corrupt("waiting admission workflow"))?;
                let parking = a
                    .parking
                    .clone()
                    .ok_or_else(|| corrupt("admission parking"))?;
                if run.snapshot.state != WorkflowState::Waiting
                    || run.admission_wait_activation.as_deref() != Some(&parking)
                {
                    return Err(corrupt("orphaned admission wait").into());
                }
                let episode = wait::load(&mut tx, &o.workflow_id, &parking).await?;
                if let Some(selection) = wait::resolved(&a, episode.deadline_at, now) {
                    task = Some(
                        wait::select(
                            &mut tx, run, &parking, &episode, &mut a, &mut r, selection, now, None,
                        )
                        .await?,
                    );
                    expired = true;
                } else if dispatch && r.capacity()? > 0 {
                    let first:Option<i64>=sqlx::query_scalar("SELECT request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting' ORDER BY request_seq LIMIT 1").bind(&scope.tenant_id).bind(&scope.namespace).bind(name).fetch_optional(&mut *tx).await?;
                    let sequence:i64=sqlx::query_scalar("SELECT request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND origin_activation_id=$4 AND logical_key=$5").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.origin_activation_id).bind(&o.key).fetch_one(&mut *tx).await?;
                    if first == Some(sequence) {
                        let task_id = db::id(&mut tx, "task").await?;
                        a.generation = a
                            .generation
                            .checked_add(1)
                            .ok_or_else(|| corrupt("offer generation overflow"))?;
                        let offer = AdmissionOffer {
                            occurrence: o.clone(),
                            generation: a.generation,
                            policy_revision: r.record.revision,
                            activation_id: task_id.clone(),
                            expires_at: now
                                .saturating_add(u64::from(r.record.policy.offer_timeout_ms))
                                .min(a.snapshot.deadline_at)
                                .min(episode.deadline_at),
                            binding_digest: a.snapshot.binding.digest()?,
                        };
                        offer.validate()?;
                        a.snapshot.state = AdmissionState::Offered;
                        a.snapshot.offer = Some(offer.clone());
                        r.offered += 1;
                        task = Some(
                            wait::select(
                                &mut tx,
                                run,
                                &parking,
                                &episode,
                                &mut a,
                                &mut r,
                                AdmissionSelection::Offered { offer },
                                now,
                                Some(task_id),
                            )
                            .await?,
                        );
                    }
                }
            }
        }
        refresh_due(&mut tx, &mut r, now).await?;
        let again =
            (if dispatch { r.due } else { r.quarantined_due }).is_some_and(|due| due <= now);
        save_resource(&mut tx, &r).await?;
        tx.commit().await?;
        Ok((task, expired, again))
    }
}
async fn choose(
    tx: &mut PgConnection,
    r: &Resource,
    now: u64,
    dispatch: bool,
) -> StoreResult<Option<AdmissionOccurrence>> {
    let scope = &r.record.scope;
    let name = &r.record.name;
    // Each UNION arm uses its own resource-local partial index and LIMIT 1.
    let row=sqlx::query("SELECT workflow_id,origin_activation_id,logical_key FROM ((SELECT workflow_id,origin_activation_id,logical_key,0 AS priority,request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='offered' AND offer_policy_revision<$4 ORDER BY offer_policy_revision,request_seq LIMIT 1) UNION ALL (SELECT workflow_id,origin_activation_id,logical_key,1,request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='offered' AND offer_expires_at_ms<=$5 ORDER BY offer_expires_at_ms,request_seq LIMIT 1) UNION ALL (SELECT workflow_id,origin_activation_id,logical_key,2,request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='possibly_dispatched' AND check_at_ms<=$5 ORDER BY check_at_ms,request_seq LIMIT 1) UNION ALL (SELECT workflow_id,origin_activation_id,logical_key,3,request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting' AND deadline_at_ms<=$5 ORDER BY deadline_at_ms,request_seq LIMIT 1) UNION ALL (SELECT workflow_id,origin_activation_id,logical_key,4,request_seq FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting' AND $6 ORDER BY request_seq LIMIT 1)) candidates ORDER BY priority,request_seq LIMIT 1")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(name).bind(r.record.revision as i64).bind(codec::ms(now)?).bind(dispatch&&r.record.policy.enabled&&r.started+r.offered<u64::from(r.record.policy.max_occupancy)).fetch_optional(&mut *tx).await?;
    row.map(|row| {
        Ok(AdmissionOccurrence {
            scope: scope.clone(),
            workflow_id: row.try_get("workflow_id")?,
            origin_activation_id: row.try_get("origin_activation_id")?,
            key: row.try_get("logical_key")?,
        })
    })
    .transpose()
}
pub(super) async fn refresh_due(
    tx: &mut PgConnection,
    r: &mut Resource,
    now: u64,
) -> StoreResult<()> {
    let scope = &r.record.scope;
    let name = &r.record.name;
    let due:Option<i64>=sqlx::query_scalar("SELECT min(due) FROM ((SELECT $5::bigint AS due FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='offered' AND offer_policy_revision<$4 ORDER BY offer_policy_revision,request_seq LIMIT 1) UNION ALL (SELECT offer_expires_at_ms FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='offered' ORDER BY offer_expires_at_ms,request_seq LIMIT 1) UNION ALL (SELECT check_at_ms FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='possibly_dispatched' ORDER BY check_at_ms,request_seq LIMIT 1) UNION ALL (SELECT deadline_at_ms FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting' ORDER BY deadline_at_ms,request_seq LIMIT 1)) deadlines")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(name).bind(r.record.revision as i64).bind(codec::ms(now)?).fetch_one(&mut *tx).await?;
    r.quarantined_due = due.map(|v| v as u64);
    r.due = r.quarantined_due;
    if r.record.policy.enabled {
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting')").bind(&scope.tenant_id).bind(&scope.namespace).bind(name).fetch_one(&mut *tx).await?;
        if waiting {
            let next = if r.capacity()? > 0 {
                Some(now)
            } else {
                core::next_admission_refill(
                    &r.record.policy,
                    r.credit
                        .map(|credit| core::AdmissionBucket {
                            credit,
                            high_water_at: r.high_water,
                        })
                        .as_ref(),
                    r.offered,
                    r.offered + r.started,
                    now,
                )?
            };
            if let Some(next) = next {
                r.due = Some(r.due.map_or(next, |due| due.min(next)));
            }
        }
    }
    Ok(())
}
pub(super) fn close_request(a: &mut Request, r: &mut Resource, now: u64) -> Result<()> {
    match a.snapshot.state {
        AdmissionState::Offered => {
            if a.snapshot
                .offer
                .as_ref()
                .is_some_and(|o| o.policy_revision == r.record.revision)
            {
                r.offered = r
                    .offered
                    .checked_sub(1)
                    .ok_or_else(|| corrupt("offer release underflow"))?;
            }
            a.snapshot.offer = None;
            a.snapshot.state = AdmissionState::ClosedBeforeStart;
        }
        AdmissionState::Waiting | AdmissionState::AwaitingRewait => {
            a.snapshot.state = AdmissionState::ClosedBeforeStart;
        }
        AdmissionState::PossiblyDispatched => {
            a.snapshot.state = AdmissionState::UnknownOccupied;
            a.check_at = None;
            r.unknown += 1;
        }
        _ => return Ok(()),
    }
    a.bump(now)?;
    r.refill(now)?;
    r.wake(now);
    Ok(())
}
/// Cancellation/finalization closes bounded unstarted work, never assumes a lost
/// callback stopped. Unknown occupancy does not keep a workflow in cancelling.
pub(in crate::workflow) async fn drain(
    tx: &mut PgConnection,
    run: &RunRecord,
    now: u64,
) -> StoreResult<bool> {
    if !run.has_admissions {
        return Ok(false);
    }
    let rows=sqlx::query("SELECT tenant_id,namespace,workflow_id,origin_activation_id,logical_key,resource_name FROM admission_requests WHERE workflow_id=$1 AND state IN ('waiting','offered','possibly_dispatched') ORDER BY request_seq LIMIT 33").bind(&run.snapshot.workflow_id).fetch_all(&mut *tx).await?;
    let mut requests = rows
        .iter()
        .take(32)
        .map(|row| {
            Ok((
                row.try_get::<String, _>("resource_name")?,
                AdmissionOccurrence {
                    scope: run.snapshot.scope.clone(),
                    workflow_id: run.snapshot.workflow_id.clone(),
                    origin_activation_id: row.try_get("origin_activation_id")?,
                    key: row.try_get("logical_key")?,
                },
            ))
        })
        .collect::<StoreResult<Vec<_>>>()?;
    // Two workflows may have invoked resources in opposite orders. Cleanup
    // locks its bounded resource set consistently, independent of invocation order.
    requests.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, o) in requests {
        let mut r = resource(tx, &o.scope, &name, true).await?;
        let mut a = request(tx, &o).await?;
        close_request(&mut a, &mut r, now)?;
        a.parking = None;
        save_request(tx, &a).await?;
        save_resource(tx, &r).await?;
    }
    Ok(rows.len() > 32)
}

async fn due_page(
    tx: &mut PgConnection,
    scope: &Scope,
    dispatch: bool,
    now: u64,
    after: Option<&(u64, String)>,
    limit: u32,
) -> StoreResult<Vec<(u64, String)>> {
    let rows: Vec<(i64, String)> = if let Some((at, name)) = after {
        let sql = if dispatch {
            "SELECT next_due_ms,resource_name FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND next_due_ms<=$3 AND (next_due_ms,resource_name)>($4,$5) ORDER BY next_due_ms,resource_name LIMIT $6"
        } else {
            "SELECT quarantined_due_ms,resource_name FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND quarantined_due_ms<=$3 AND (quarantined_due_ms,resource_name)>($4,$5) ORDER BY quarantined_due_ms,resource_name LIMIT $6"
        };
        sqlx::query_as(sql)
            .bind(&scope.tenant_id)
            .bind(&scope.namespace)
            .bind(codec::ms(now)?)
            .bind(codec::ms(*at)?)
            .bind(name)
            .bind(i64::from(limit))
            .fetch_all(&mut *tx)
            .await?
    } else {
        let sql = if dispatch {
            "SELECT next_due_ms,resource_name FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND next_due_ms<=$3 ORDER BY next_due_ms,resource_name LIMIT $4"
        } else {
            "SELECT quarantined_due_ms,resource_name FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND quarantined_due_ms<=$3 ORDER BY quarantined_due_ms,resource_name LIMIT $4"
        };
        sqlx::query_as(sql)
            .bind(&scope.tenant_id)
            .bind(&scope.namespace)
            .bind(codec::ms(now)?)
            .bind(i64::from(limit))
            .fetch_all(&mut *tx)
            .await?
    };
    rows.into_iter()
        .map(|(at, name)| Ok((codec::timestamp(at)?, name)))
        .collect()
}
