use super::*;
impl PostgresStore {
    pub(super) async fn advance_provider_once(
        &self,
        q: &ProviderMaintenanceQuery,
    ) -> StoreResult<ProviderMaintenanceProgress> {
        q.validate()?;
        let config = self.provider_configured()?;
        let scope = &config.audience.scope;
        if q.cursor
            .as_ref()
            .is_some_and(|c| &c.occurrence.scope != scope)
        {
            return Err(ContractError::Forbidden.into());
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let now = db::now(&mut tx).await?;
        let after = q.cursor.as_ref();
        let rows = if let Some(after) = after {
            sqlx::query(DUE_AFTER_SQL)
                .bind(&scope.tenant_id)
                .bind(&scope.namespace)
                .bind(codec::ms(now)?)
                .bind(codec::ms(after.due_at)?)
                .bind(&after.occurrence.workflow_id)
                .bind(&after.occurrence.activation_id)
                .bind(&after.occurrence.approval_key)
                .bind(i64::from(q.limit))
                .fetch_all(&mut *tx)
                .await?
        } else {
            sqlx::query(DUE_SQL)
                .bind(&scope.tenant_id)
                .bind(&scope.namespace)
                .bind(codec::ms(now)?)
                .bind(i64::from(q.limit))
                .fetch_all(&mut *tx)
                .await?
        };
        tx.commit().await?;
        let mut progress = ProviderMaintenanceProgress::default();
        for row in rows {
            let o = EffectOccurrence {
                scope: scope.clone(),
                workflow_id: row.try_get("workflow_id")?,
                activation_id: row.try_get("activation_id")?,
                approval_key: row.try_get("approval_key")?,
            };
            let due = codec::timestamp(row.try_get("next_due_at_ms")?)?;
            progress.examined += 1;
            progress.next_cursor = Some(ProviderMaintenanceCursor {
                due_at: due,
                occurrence: o.clone(),
            });
            match self.advance_provider_item(&o).await {
                Ok((task, stopped)) => {
                    if let Some(task) = task {
                        progress.allocated += 1;
                        self.workflow_wakes(&[task]);
                    }
                    if stopped {
                        progress.stopped += 1;
                    }
                }
                Err(StoreError::Database(error))
                    if error
                        .as_database_error()
                        .is_some_and(|e| e.code().as_deref() == Some("55P03")) =>
                {
                    progress.skipped_busy += 1
                }
                Err(error) => return Err(error),
            }
        }
        Ok(progress)
    }
    async fn advance_provider_item(
        &self,
        o: &EffectOccurrence,
    ) -> StoreResult<(Option<TaskSnapshot>, bool)> {
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let row=sqlx::query("SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND retiring_at_ms IS NULL FOR NO KEY UPDATE NOWAIT").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            // A terminal report and retention may have won after the due shortlist.
            let r = retained::load(&mut tx, o)
                .await?
                .ok_or_else(|| corrupt("scheduled occurrence missing"))?;
            let _state = load(&mut tx, &r).await?;
            if summary(&r)?.reconciliation.next_due_at.is_none() {
                tx.commit().await?;
                return Ok((None, false));
            }
            return Err(corrupt("scheduled provider has no live workflow").into());
        };
        let run = run_record(&row)?;
        let active:Option<String>=sqlx::query_scalar("SELECT active_task_id FROM provider_operations WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5")
            .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).fetch_optional(&mut *tx).await?.flatten();
        if let Some(id) = active {
            sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE NOWAIT")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        }
        sqlx::query("SELECT workflow_id FROM approved_effect_occurrences WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 FOR UPDATE NOWAIT")
            .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).fetch_one(&mut *tx).await?;
        let mut r = retained::load(&mut tx, o)
            .await?
            .ok_or_else(|| corrupt("scheduled occurrence"))?;
        let mut state = load(&mut tx, &r).await?;
        let now = db::now(&mut tx).await?;
        let q = &summary(&r)?.reconciliation;
        if q.next_due_at.is_none_or(|due| due > now) {
            tx.commit().await?;
            return Ok((None, false));
        }
        let reason = if !matches!(
            run.snapshot.state,
            WorkflowState::Running | WorkflowState::Waiting
        ) {
            Some(
                if matches!(
                    run.snapshot.state,
                    WorkflowState::Cancelling | WorkflowState::Cancelled
                ) {
                    ProviderRecoveryStopReason::WorkflowCancelled
                } else {
                    ProviderRecoveryStopReason::WorkflowFailed
                },
            )
        } else if now >= q.deadline_at {
            Some(ProviderRecoveryStopReason::Deadline)
        } else if q.checks_used >= q.max_checks {
            Some(ProviderRecoveryStopReason::BudgetExhausted)
        } else if binding(&r)?.contract.lookup == ProviderLookup::Unavailable
            || (binding(&r)?.contract.lookup == ProviderLookup::ByReference
                && state.provider_reference.is_none())
        {
            Some(ProviderRecoveryStopReason::LookupUnavailable)
        } else {
            None
        };
        if let Some(reason) = reason {
            stop(&mut tx, &mut r, &mut state, reason, now).await?;
            cancel_active(&mut tx, &r, now).await?;
            wait::notify(&mut tx, &run, &r, &state, now).await?;
            tx.commit().await?;
            return Ok((None, true));
        }
        if summary(&r)?.reconciliation.active_task_id.is_some() {
            return Err(corrupt("active provider due before recovery deadline").into());
        }
        let b = binding(&r)?.clone();
        let generation = summary(&r)?.reconciliation.generation + 1;
        let task_id = db::id(&mut tx, "task").await?;
        let probe_id = db::id(&mut tx, "probe").await?;
        let marker = ProviderProbeAssignment {
            occurrence: o.clone(),
            probe_id: probe_id.clone(),
            generation,
        };
        let command = SubmitCommand {
            input: SubmitTask {
                admission: None,
                tenant_id: o.scope.tenant_id.clone(),
                namespace: o.scope.namespace.clone(),
                queue: b.reconciliation.queue.clone(),
                program: b.program.program.clone(),
                correlation_key: run.snapshot.correlation_key.clone(),
                data: Value::Null,
                retry_policy: RetryPolicy {
                    max_attempts: 1,
                    retry_delay_ms: 0,
                },
                attempt_timeout_ms: PROVIDER_ATTEMPT_TIMEOUT_MS,
            },
            idempotency_key: format!("provider:{probe_id}"),
            origin_trace: run.submission.origin_trace.clone(),
        };
        let mut task = insert_task(
            &mut tx,
            &command,
            &b.program,
            &task_id,
            &run.snapshot,
            false,
            now,
        )
        .await?;
        task.provider_probe = Some(marker.clone());
        let valid:Option<bool>=sqlx::query_scalar("UPDATE tasks SET provider_probe_bytes=$2 WHERE task_id=$1 RETURNING task_id=$1 AND provider_probe_bytes=$2 AND workflow_id=$3 AND workflow_activation_id IS NULL").bind(&task_id).bind(codec::encode(&marker)?).bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
        proof(valid, "provider task marker")?;
        let valid:Option<bool>=sqlx::query_scalar("INSERT INTO provider_probe_tasks(tenant_id,namespace,workflow_id,activation_id,approval_key,probe_id,generation,task_id,live_task_id,assignment_bytes,descriptor_bytes,provider_reference,allocated_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7::text::numeric,$8,$8,$9,$10,$11,$12) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND probe_id=$6 AND generation=$7::text::numeric AND task_id=$8 AND live_task_id=$8 AND assignment_bytes=$9 AND descriptor_bytes=$10 AND provider_reference IS NOT DISTINCT FROM $11 AND allocated_at_ms=$12 AND consumed_at_ms IS NULL")
          .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(&probe_id).bind(generation.to_string()).bind(&task_id).bind(codec::encode(&marker)?).bind(codec::encode(&b.program)?).bind(&state.provider_reference).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
        proof(valid, "provider probe allocation")?;
        let s = summary_mut(&mut r)?;
        s.reconciliation.checks_used += 1;
        s.reconciliation.generation = generation;
        s.reconciliation.active_probe_id = Some(probe_id.clone());
        s.reconciliation.active_task_id = Some(task_id.clone());
        s.reconciliation.next_due_at = Some(s.reconciliation.deadline_at);
        let (sequence, revision) = r.next(now)?;
        let e = EffectEvidenceEntry::ProviderProbeScheduled {
            sequence,
            recorded_revision: revision,
            recorded_at: now,
            probe_id,
            generation,
            task_id,
            due_at: now,
        };
        retained::append(&mut tx, &r, &e, "provider", None, None, None).await?;
        save(&mut tx, &r, &state).await?;
        tx.commit().await?;
        Ok((Some(task), false))
    }
}
pub(in crate::workflow) async fn terminal_obligation(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
) -> StoreResult<()> {
    let Some(marker) = &task.provider_probe else {
        return Ok(());
    };
    let at = task
        .terminal_at
        .ok_or_else(|| corrupt("probe terminal timestamp"))?;
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) VALUES('provider-terminal:'||$2,$1,$2,'provider_probe_terminal',$3,$3) ON CONFLICT(id) DO UPDATE SET available_at_ms=LEAST(workflow_work.available_at_ms,EXCLUDED.available_at_ms) RETURNING id='provider-terminal:'||$2 AND workflow_id=$1 AND task_id=$2 AND kind='provider_probe_terminal'").bind(&marker.occurrence.workflow_id).bind(&task.task_id).bind(codec::ms(at)?).fetch_optional(tx).await?;
    proof(valid, "provider terminal obligation")
}
pub(in crate::workflow) async fn finalize(
    tx: &mut PgConnection,
    run: &RunRecord,
    task_id: &str,
    now: u64,
) -> StoreResult<()> {
    let task = db::load_task(tx, &run.snapshot.scope, task_id, true).await?;
    let marker = task
        .provider_probe
        .as_ref()
        .ok_or_else(|| corrupt("probe marker missing"))?;
    if !task.state.is_terminal() {
        return Err(corrupt("probe finalized before task terminal").into());
    }
    let o = &marker.occurrence;
    let mut r = retained::load(tx, o)
        .await?
        .ok_or_else(|| corrupt("probe retained occurrence"))?;
    let mut state = load(tx, &r).await?;
    let row=sqlx::query("SELECT live_task_id,consumed_at_ms FROM provider_probe_tasks WHERE probe_id=$1 AND task_id=$2").bind(&marker.probe_id).bind(task_id).fetch_one(&mut *tx).await?;
    if row.try_get::<Option<i64>, _>("consumed_at_ms")?.is_some() {
        return Ok(());
    }
    let q = &summary(&r)?.reconciliation;
    if q.active_task_id.as_deref() != Some(task_id)
        || q.active_probe_id.as_ref() != Some(&marker.probe_id)
        || q.generation != marker.generation
    {
        return Err(corrupt("probe finalization generation").into());
    }
    let report:Option<Vec<u8>>=sqlx::query_scalar("SELECT e.entry_bytes FROM provider_invocations i JOIN provider_reports p USING(tenant_id,namespace,workflow_id,activation_id,approval_key,source_id) JOIN approved_effect_evidence_entries e ON e.tenant_id=p.tenant_id AND e.namespace=p.namespace AND e.workflow_id=p.workflow_id AND e.activation_id=p.activation_id AND e.approval_key=p.approval_key AND e.sequence=p.entry_sequence WHERE i.probe_id=$1 ORDER BY e.sequence LIMIT 1").bind(&marker.probe_id).fetch_optional(&mut *tx).await?;
    let (completion, permanent_failure) = if let Some(bytes) = report {
        let entry: EffectEvidenceEntry = codec::decode(&bytes)?;
        (
            ProviderProbeCompletion::Reported {
                report: entry
                    .provider_report_reference()
                    .ok_or_else(|| corrupt("probe report kind"))?,
            },
            false,
        )
    } else {
        let (failure, permanent) = classify_failure(tx, &task).await?;
        (ProviderProbeCompletion::Unreported { failure }, permanent)
    };
    let valid:Option<bool>=sqlx::query_scalar("UPDATE provider_probe_tasks SET consumed_at_ms=$3,live_task_id=NULL WHERE probe_id=$1 AND live_task_id=$2 AND consumed_at_ms IS NULL RETURNING probe_id=$1 AND task_id=$2 AND consumed_at_ms=$3 AND live_task_id IS NULL").bind(&marker.probe_id).bind(task_id).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
    proof(valid, "probe finalization")?;
    let q = &mut summary_mut(&mut r)?.reconciliation;
    q.active_probe_id = None;
    q.active_task_id = None;
    let (sequence, revision) = r.next(now)?;
    let e = EffectEvidenceEntry::ProviderProbeFinished {
        sequence,
        recorded_revision: revision,
        recorded_at: now,
        probe_id: marker.probe_id.clone(),
        generation: marker.generation,
        task_id: task_id.into(),
        completion: completion.clone(),
    };
    retained::append(tx, &r, &e, "provider", None, None, None).await?;
    let q = &summary(&r)?.reconciliation;
    if q.stopped_reason.is_none() {
        let failure = permanent_failure.then_some(ProviderRecoveryStopReason::RecoveryFailed);
        let reason = failure
            .or_else(|| (now >= q.deadline_at).then_some(ProviderRecoveryStopReason::Deadline))
            .or_else(|| {
                (q.checks_used >= q.max_checks)
                    .then_some(ProviderRecoveryStopReason::BudgetExhausted)
            });
        if let Some(reason) = reason {
            stop(tx, &mut r, &mut state, reason, now).await?;
        } else {
            let policy = &binding(&r)?.reconciliation;
            let delay = policy
                .interval_ms
                .saturating_mul(
                    1u64 << summary(&r)?
                        .reconciliation
                        .checks_used
                        .saturating_sub(1)
                        .min(20),
                )
                .min(policy.maximum_interval_ms);
            let delay = jittered_delay(
                &r.binding.occurrence,
                summary(&r)?.reconciliation.generation,
                delay,
            );
            let q = &mut summary_mut(&mut r)?.reconciliation;
            q.next_due_at = Some(now.saturating_add(delay).min(q.deadline_at));
        }
    }
    save(tx, &r, &state).await?;
    wait::notify(tx, run, &r, &state, now).await
}
async fn classify_failure(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
) -> StoreResult<(ProviderProbeFailureKind, bool)> {
    use ledgence_worker_api::{ErrorKind as E, Phase as P};
    if task.state == TaskState::Cancelled {
        return Ok((ProviderProbeFailureKind::Cancelled, false));
    }
    let row=sqlx::query("SELECT a.state,s.accepted_command FROM attempts a LEFT JOIN accepted_settlements s USING(attempt_id) WHERE a.task_id=$1 ORDER BY a.generation DESC LIMIT 1").bind(&task.task_id).fetch_optional(tx).await?;
    let Some(row) = row else {
        return Ok((ProviderProbeFailureKind::MissingReport, false));
    };
    if row.try_get::<String, _>("state")? == "lost" {
        return Ok((ProviderProbeFailureKind::WorkerLost, false));
    }
    if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("accepted_command")? {
        let command: SettleCommand = codec::decode(&bytes)?;
        if let AttemptReport::Failed(report) = &command.report {
            let preparation = matches!(report.phase, P::Admission | P::Preparation | P::Startup);
            let definitive_preparation = preparation
                && matches!(
                    report.error.kind,
                    E::NotFound | E::Integrity | E::InvalidInput | E::Protocol | E::Incompatible
                );
            if let Some(failure) = command.provider_probe_failure {
                failure.validate_settlement(&command.report)?;
                let permanent = definitive_preparation
                    || matches!(
                        failure,
                        ProviderProbeFailureKind::IncompatibleWorker
                            | ProviderProbeFailureKind::ContractMismatch
                            | ProviderProbeFailureKind::ContextInvalid
                            | ProviderProbeFailureKind::ResultInvalid
                    );
                return Ok((failure, permanent));
            }
            // Old hosts have no C classification. Only typed host kind/phase is
            // inspected; an application failure string never selects a category.
            let failure = if report.error.kind == E::Incompatible && preparation {
                ProviderProbeFailureKind::IncompatibleWorker
            } else if preparation {
                ProviderProbeFailureKind::PreparationFailed
            } else if report.phase == P::Execution && report.error.kind == E::TimedOut {
                ProviderProbeFailureKind::InvocationTimeout
            } else if report.phase == P::Execution && report.error.kind == E::Protocol {
                ProviderProbeFailureKind::ResultInvalid
            } else {
                ProviderProbeFailureKind::InvocationFailed
            };
            return Ok((
                failure,
                definitive_preparation || failure == ProviderProbeFailureKind::ResultInvalid,
            ));
        }
    }
    Ok((ProviderProbeFailureKind::MissingReport, false))
}
pub(super) async fn stop(
    tx: &mut PgConnection,
    r: &mut Retained,
    state: &mut State,
    reason: ProviderRecoveryStopReason,
    now: u64,
) -> StoreResult<()> {
    if summary(r)?.reconciliation.stopped_reason.is_some() {
        return Ok(());
    }
    let s = summary_mut(r)?;
    s.reconciliation.stopped_reason = Some(reason);
    s.reconciliation.next_due_at = None;
    let (sequence, revision) = r.next(now)?;
    reports::refresh_candidate(r, state, now)?;
    let entry = EffectEvidenceEntry::ProviderRecoveryStopped {
        sequence,
        recorded_revision: revision,
        recorded_at: now,
        reason,
    };
    retained::append(tx, r, &entry, "provider", None, None, None).await?;
    save(tx, r, state).await
}
pub(super) async fn cancel_active(
    tx: &mut PgConnection,
    r: &Retained,
    now: u64,
) -> StoreResult<()> {
    if let Some(id) = &summary(r)?.reconciliation.active_task_id {
        let task = db::load_task(tx, &r.binding.occurrence.scope, id, true).await?;
        if !task.state.is_terminal() && task.cancel_requested_at.is_none() {
            let attempt = if let Some(id) = &task.current_attempt_id {
                Some(db::load_attempt(tx, &task, id).await?)
            } else {
                None
            };
            let transition = core::cancel(&task, attempt.as_ref(), now)?;
            db::apply(tx, &transition).await?;
        }
    }
    Ok(())
}
pub(in crate::workflow) async fn close_for_assessment(
    tx: &mut PgConnection,
    r: &mut Retained,
    mut state: State,
    now: u64,
) -> StoreResult<()> {
    if r.summary.provider.is_none() {
        return Ok(());
    }
    summary_mut(r)?.handling = ProviderHandlingState::OperatorClosed;
    let q = &mut summary_mut(r)?.reconciliation;
    q.stopped_reason = Some(ProviderRecoveryStopReason::OperatorAssessment);
    q.next_due_at = None;
    reports::refresh_candidate(r, &mut state, now)?;
    cancel_active(tx, r, now).await?;
    save(tx, r, &state).await?;
    if let Some(run) = load_live_run(tx, &r.binding.occurrence).await? {
        wait::notify(tx, &run, r, &state, now).await?;
    }
    Ok(())
}
async fn load_live_run(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
) -> StoreResult<Option<RunRecord>> {
    let row=sqlx::query("SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE workflow_id=$1 AND retiring_at_ms IS NULL").bind(&o.workflow_id).fetch_optional(tx).await?;
    row.as_ref().map(run_record).transpose()
}
/// A composed cancellation must acquire active probe task locks before resource
/// locks, then retain those locks while draining provider effect evidence. The
/// workflow lock keeps this bounded candidate set stable between both phases.
pub(in crate::workflow) async fn lock_drain_tasks(
    tx: &mut PgConnection,
    run: &RunRecord,
) -> StoreResult<()> {
    if !run.has_provider_operations {
        return Ok(());
    }
    let ids: Vec<String> = sqlx::query_scalar("SELECT active_task_id FROM (SELECT active_task_id FROM provider_operations WHERE workflow_id=$1 AND (stopped_reason IS NULL OR active_task_id IS NOT NULL) ORDER BY activation_id,approval_key LIMIT 16) p WHERE active_task_id IS NOT NULL ORDER BY active_task_id")
        .bind(&run.snapshot.workflow_id).fetch_all(&mut *tx).await?;
    for id in ids {
        db::load_task(tx, &run.snapshot.scope, &id, true).await?;
    }
    Ok(())
}
pub(in crate::workflow) async fn drain(
    tx: &mut PgConnection,
    run: &RunRecord,
    now: u64,
) -> StoreResult<bool> {
    if !run.has_provider_operations {
        return Ok(false);
    }
    let rows=sqlx::query("SELECT activation_id,approval_key FROM provider_operations WHERE workflow_id=$1 AND (stopped_reason IS NULL OR active_task_id IS NOT NULL) ORDER BY activation_id,approval_key LIMIT 16").bind(&run.snapshot.workflow_id).fetch_all(&mut *tx).await?;
    for row in rows {
        let o = EffectOccurrence {
            scope: run.snapshot.scope.clone(),
            workflow_id: run.snapshot.workflow_id.clone(),
            activation_id: row.try_get("activation_id")?,
            approval_key: row.try_get("approval_key")?,
        };
        lock_active(tx, &o).await?;
        let mut r = retained::load(tx, &o)
            .await?
            .ok_or_else(|| corrupt("provider drain occurrence"))?;
        let mut state = load(tx, &r).await?;
        stop(
            tx,
            &mut r,
            &mut state,
            if matches!(
                run.snapshot.state,
                WorkflowState::Cancelling | WorkflowState::Cancelled
            ) {
                ProviderRecoveryStopReason::WorkflowCancelled
            } else {
                ProviderRecoveryStopReason::WorkflowFailed
            },
            now,
        )
        .await?;
        cancel_active(tx, &r, now).await?;
        if let Some(id) = summary(&r)?.reconciliation.active_task_id.clone() {
            let task = db::load_task(tx, &o.scope, &id, false).await?;
            if task.state.is_terminal() {
                finalize(tx, run, &id, now).await?;
            }
        }
    }
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM provider_operations WHERE workflow_id=$1 AND (stopped_reason IS NULL OR active_task_id IS NOT NULL))").bind(&run.snapshot.workflow_id).fetch_one(tx).await?)
}

// Separate first/seek predicates retain bounded index conditions even when
// PostgreSQL chooses a generic prepared plan.
pub(in crate::workflow) const DUE_SQL: &str = "SELECT workflow_id,activation_id,approval_key,next_due_at_ms FROM provider_operations WHERE tenant_id=$1 AND namespace=$2 AND next_due_at_ms<=$3 ORDER BY next_due_at_ms,workflow_id,activation_id,approval_key LIMIT $4";
pub(in crate::workflow) const DUE_AFTER_SQL: &str = "SELECT workflow_id,activation_id,approval_key,next_due_at_ms FROM provider_operations WHERE tenant_id=$1 AND namespace=$2 AND next_due_at_ms<=$3 AND (next_due_at_ms,workflow_id,activation_id,approval_key)>($4,$5,$6,$7) ORDER BY next_due_at_ms,workflow_id,activation_id,approval_key LIMIT $8";
fn jittered_delay(o: &EffectOccurrence, generation: u64, capped_delay: u64) -> u64 {
    // FNV-1a is only a deterministic spread, not a security primitive. The
    // accepted due time is persisted once; no RNG or runtime-specific hasher.
    let mut hash = 0xcbf29ce484222325u64;
    for value in [
        &o.scope.tenant_id,
        &o.scope.namespace,
        &o.workflow_id,
        &o.activation_id,
        &o.approval_key,
    ] {
        for byte in value.bytes().chain([0]) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    for byte in generation.to_le_bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
    capped_delay
        .saturating_sub(hash % (capped_delay / 5 + 1))
        .max(1000)
        .min(capped_delay)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn jitter_is_stable_bounded_and_spreads_generations() {
        let o = EffectOccurrence {
            scope: Scope {
                tenant_id: "t".into(),
                namespace: "n".into(),
            },
            workflow_id: "wf".into(),
            activation_id: "activation".into(),
            approval_key: "key".into(),
        };
        let delays: std::collections::BTreeSet<_> = (1..=256)
            .map(|n| {
                let d = jittered_delay(&o, n, 300000);
                assert!((240000..=300000).contains(&d));
                assert_eq!(d, jittered_delay(&o, n, 300000));
                d
            })
            .collect();
        assert!(delays.len() > 200);
        assert_eq!(jittered_delay(&o, 1, 1000), 1000);
    }
}
