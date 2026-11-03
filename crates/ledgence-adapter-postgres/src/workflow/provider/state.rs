use super::*;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    pub source_id: String,
    pub report_id: String,
    pub ticket: ProviderOperationTicket,
    pub worker: EffectWorkerIdentity,
    pub source: ProviderSourceReference,
    pub owner: LeaseOwner,
    pub context: Option<ProviderProbeContext>,
}
pub(in crate::workflow) struct State {
    pub candidate: Option<ProviderSelection>,
    pub current_report: Option<EffectEvidenceEntry>,
    pub provider_reference: Option<String>,
}
pub(in crate::workflow) async fn load(tx: &mut PgConnection, r: &Retained) -> StoreResult<State> {
    let o = &r.binding.occurrence;
    let row=sqlx::query("SELECT *,trunc(generation)::text AS generation_text,trunc(candidate_revision)::text AS candidate_text FROM provider_operations WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5")
      .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).fetch_optional(tx).await?.ok_or_else(||corrupt("provider operation missing"))?;
    let s = summary(r)?;
    let q = &s.reconciliation;
    let stored: ProviderEffectSummary = codec::decode(&row.try_get::<Vec<u8>, _>("state_bytes")?)?;
    if stored != *s
        || row.try_get::<String, _>("binding_digest")? != s.binding_digest.0.as_str()
        || codec::enum_value::<ProviderKnowledgeKind>(row.try_get("knowledge")?)? != s.knowledge
        || codec::enum_value::<ProviderHandlingState>(row.try_get("handling")?)? != s.handling
        || row.try_get::<i32, _>("checks_used")? != q.checks_used as i32
        || codec::u64_text(&row.try_get::<String, _>("generation_text")?)? != q.generation
        || row.try_get::<Option<String>, _>("active_task_id")? != q.active_task_id
        || row.try_get::<Option<String>, _>("active_probe_id")? != q.active_probe_id
        || row.try_get::<i64, _>("first_due_at_ms")? != codec::ms(q.first_due_at)?
        || row.try_get::<i64, _>("deadline_at_ms")? != codec::ms(q.deadline_at)?
        || row.try_get::<Option<i64>, _>("next_due_at_ms")?
            != q.next_due_at.map(codec::ms).transpose()?
        || row
            .try_get::<Option<String>, _>("stopped_reason")?
            .as_deref()
            != q.stopped_reason.map(|v| v.as_str())
        || row
            .try_get::<Option<String>, _>("candidate_text")?
            .map(|v| codec::u64_text(&v))
            .transpose()?
            != s.candidate_revision
    {
        return Err(corrupt("provider scalar projection").into());
    }
    let state = State {
        candidate: row
            .try_get::<Option<Vec<u8>>, _>("candidate_bytes")?
            .map(|b| codec::decode(&b))
            .transpose()?,
        current_report: row
            .try_get::<Option<Vec<u8>>, _>("current_report_bytes")?
            .map(|b| codec::decode(&b))
            .transpose()?,
        provider_reference: row.try_get("provider_reference")?,
    };
    if state.candidate.is_some() != s.candidate_revision.is_some() {
        return Err(corrupt("provider candidate revision").into());
    }
    if let Some(e) = &state.current_report {
        e.validate(&o.scope)?;
        if e.provider_report_reference() != s.selected_report {
            return Err(corrupt("provider current report").into());
        }
    }
    Ok(state)
}
pub(in crate::workflow) async fn save(
    tx: &mut PgConnection,
    r: &Retained,
    state: &State,
) -> StoreResult<()> {
    let o = &r.binding.occurrence;
    let s = summary(r)?;
    let q = &s.reconciliation;
    s.validate(r.summary.entry_count, r.summary.effect_revision)?;
    let candidate = state.candidate.as_ref().map(codec::encode).transpose()?;
    let report = state
        .current_report
        .as_ref()
        .map(codec::encode)
        .transpose()?;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE provider_operations SET knowledge=$6,handling=$7,checks_used=$8,generation=$9::text::numeric,active_probe_id=$10,active_task_id=$11,next_due_at_ms=$12,stopped_reason=$13,candidate_revision=$14::text::numeric,candidate_bytes=$15,state_bytes=$16,current_report_bytes=$17,provider_reference=$18,needs_review=$19 WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND knowledge=$6 AND handling=$7 AND checks_used=$8 AND generation=$9::text::numeric AND active_probe_id IS NOT DISTINCT FROM $10 AND active_task_id IS NOT DISTINCT FROM $11 AND next_due_at_ms IS NOT DISTINCT FROM $12 AND stopped_reason IS NOT DISTINCT FROM $13 AND candidate_revision IS NOT DISTINCT FROM $14::text::numeric AND candidate_bytes IS NOT DISTINCT FROM $15 AND state_bytes=$16 AND current_report_bytes IS NOT DISTINCT FROM $17 AND provider_reference IS NOT DISTINCT FROM $18 AND needs_review=$19")
      .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key)
      .bind(codec::label(&s.knowledge)?).bind(codec::label(&s.handling)?).bind(q.checks_used as i32).bind(q.generation.to_string()).bind(&q.active_probe_id).bind(&q.active_task_id).bind(q.next_due_at.map(codec::ms).transpose()?).bind(q.stopped_reason.map(|v|v.as_str())).bind(s.candidate_revision.map(|v|v.to_string())).bind(candidate).bind(codec::encode(s)?).bind(report).bind(&state.provider_reference).bind(s.needs_review(r.summary.operator_resolution.as_ref().is_some_and(|v|v.first_conflicting_entry_sequence.is_some()))).fetch_optional(&mut *tx).await?;
    proof(valid, "provider projection update suppressed or changed")?;
    retained::save(tx, r).await
}
pub(super) fn select_candidate(
    r: &mut Retained,
    state: &mut State,
    candidate: Option<ProviderSelection>,
) -> Result<()> {
    if canonical(&state.candidate)? != canonical(&candidate)? {
        state.candidate = candidate;
        let revision = r.summary.effect_revision;
        summary_mut(r)?.candidate_revision = state.candidate.as_ref().map(|_| revision);
    }
    Ok(())
}
pub(super) async fn source(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
    id: &str,
) -> StoreResult<Option<Source>> {
    let row=sqlx::query("SELECT source_bytes,source_id,report_id,owner_bytes FROM provider_invocations WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND source_id=$6")
      .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(id).fetch_optional(tx).await?;
    row.map(|row| {
        let s: Source = codec::decode(&row.try_get::<Vec<u8>, _>("source_bytes")?)?;
        s.ticket.validate()?;
        s.worker.validate()?;
        s.owner.scope.validate()?;
        if s.ticket.occurrence != *o
            || s.source_id != row.try_get::<String, _>("source_id")?
            || s.report_id != row.try_get::<String, _>("report_id")?
            || codec::encode(&s.owner)? != row.try_get::<Vec<u8>, _>("owner_bytes")?
        {
            return Err(corrupt("provider source identity").into());
        }
        Ok(s)
    })
    .transpose()
}
pub(super) async fn insert_source<T: serde::Serialize>(
    tx: &mut PgConnection,
    s: &Source,
    command: &T,
) -> StoreResult<()> {
    let o = &s.ticket.occurrence;
    let (kind, claim, probe) = match &s.source {
        ProviderSourceReference::Dispatch { claim_id } => ("dispatch", Some(claim_id), None),
        ProviderSourceReference::Probe { probe_id, .. } => ("probe", None, Some(probe_id)),
    };
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO provider_invocations(tenant_id,namespace,workflow_id,activation_id,approval_key,source_id,report_id,source_kind,claim_id,probe_id,task_id,attempt_id,source_bytes,owner_bytes,command_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND source_id=$6 AND report_id=$7 AND source_kind=$8 AND claim_id IS NOT DISTINCT FROM $9 AND probe_id IS NOT DISTINCT FROM $10 AND task_id=$11 AND attempt_id=$12 AND source_bytes=$13 AND owner_bytes=$14 AND command_bytes=$15")
      .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(&s.source_id).bind(&s.report_id).bind(kind).bind(claim).bind(probe).bind(&s.owner.task_id).bind(&s.owner.attempt_id).bind(codec::encode(s)?).bind(codec::encode(&s.owner)?).bind(canonical(command)?).fetch_optional(tx).await?;
    proof(valid, "provider source suppressed or changed")
}
/// Workflow first, then its one active probe task, before any occurrence lock.
pub(super) async fn lock_live(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
) -> StoreResult<Option<RunRecord>> {
    let row=sqlx::query("SELECT *,trunc(revision)::text AS revision_text FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND retiring_at_ms IS NULL FOR NO KEY UPDATE")
      .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
    let run = row.as_ref().map(run_record).transpose()?;
    if run.is_some() {
        lock_active(tx, o).await?;
    }
    Ok(run)
}
pub(in crate::workflow) async fn lock_active(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
) -> StoreResult<()> {
    let id:Option<String>=sqlx::query_scalar("SELECT active_task_id FROM provider_operations WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5")
      .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).fetch_optional(&mut *tx).await?.flatten();
    if let Some(id) = id {
        db::load_task(tx, &o.scope, &id, true).await?;
    }
    Ok(())
}
pub(super) async fn check_owner(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
    owner: &LeaseOwner,
    now: u64,
    require_dispatch: bool,
) -> StoreResult<()> {
    let task = db::load_task(tx, &o.scope, &owner.task_id, true).await?;
    let attempt = db::load_attempt(tx, &task, &owner.attempt_id).await?;
    if task.workflow_id.as_deref() != Some(&o.workflow_id)
        || task.state != TaskState::Active
        || task.current_attempt_id.as_deref() != Some(&owner.attempt_id)
        || codec::encode(&attempt.lease.owner)? != codec::encode(owner)?
        || task.cancel_requested_at.is_some()
        || attempt.state != AttemptState::Active
        || now >= attempt.lease.expires_at
        || now >= attempt.authority_deadline
        || now >= attempt.deadline
        || attempt.settlement.is_some()
        || (require_dispatch && !attempt.execution_may_have_started)
    {
        return Err(ContractError::ObsoleteOperation.into());
    }
    Ok(())
}
