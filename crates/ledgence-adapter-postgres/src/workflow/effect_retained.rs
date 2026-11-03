//! Independent evidence projection. Callers hold workflow -> task -> occurrence locks.
use super::*;

pub(crate) struct Retained {
    pub binding: EffectEvidenceBinding,
    pub summary: EffectEvidenceSummary,
    pub reference: Option<Vec<u8>>,
    pub journal: Option<Vec<u8>>,
    pub current_assessment: Option<EffectEvidenceEntry>,
    pub assessment_result: Option<Vec<u8>>,
}
impl Retained {
    pub fn next(&mut self, now: u64) -> Result<(u64, u64)> {
        self.summary.entry_count = self
            .summary
            .entry_count
            .checked_add(1)
            .ok_or_else(|| corrupt("effect sequence overflow"))?;
        self.summary.effect_revision = self
            .summary
            .effect_revision
            .checked_add(1)
            .ok_or_else(|| corrupt("effect revision overflow"))?;
        self.summary.updated_at = now.max(self.summary.updated_at);
        Ok((self.summary.entry_count, self.summary.effect_revision))
    }
    pub fn report(&mut self, output: &Value) -> Result<()> {
        let bytes = canonical_json_bytes(output)?;
        if let Some(resolution) = &mut self.summary.operator_resolution
            && resolution.first_conflicting_entry_sequence.is_none()
            && self
                .assessment_result
                .as_ref()
                .is_some_and(|result| result != &bytes)
        {
            resolution.first_conflicting_entry_sequence = Some(
                self.summary
                    .entry_count
                    .checked_add(1)
                    .ok_or_else(|| corrupt("effect sequence overflow"))?,
            );
        }
        if let Some(reference) = &self.reference {
            if reference != &bytes {
                self.summary.has_discrepancy = true;
            }
        } else {
            self.reference = Some(bytes);
        }
        Ok(())
    }
}
fn from_row(row: &PgRow, occurrence: &EffectOccurrence) -> Result<Retained> {
    let binding: EffectEvidenceBinding = codec::decode(
        &row.try_get::<Vec<u8>, _>("binding_bytes")
            .map_err(database_error)?,
    )?;
    let summary: EffectEvidenceSummary = codec::decode(
        &row.try_get::<Vec<u8>, _>("summary_bytes")
            .map_err(database_error)?,
    )?;
    binding
        .validate()
        .map_err(|_| corrupt("retained effect binding"))?;
    summary
        .validate()
        .map_err(|_| corrupt("retained effect summary"))?;
    if &binding.occurrence != occurrence
        || &summary.occurrence != occurrence
        || summary.program != binding.program.program
        || summary.callable != binding.approval.action.name
        || summary.action_version != binding.approval.action.version
    {
        return Err(corrupt("retained effect identity"));
    }
    let revision: u64 = row
        .try_get::<String, _>("effect_revision_text")
        .map_err(database_error)?
        .parse()
        .map_err(|_| corrupt("effect revision"))?;
    let count: u64 = row
        .try_get::<String, _>("entry_count_text")
        .map_err(database_error)?
        .parse()
        .map_err(|_| corrupt("effect entry count"))?;
    if summary.effect_revision != revision
        || summary.entry_count != count
        || summary.journal_state
            != codec::enum_value::<EffectJournalState>(
                row.try_get("journal_state").map_err(database_error)?,
            )?
        || summary.has_discrepancy
            != row
                .try_get::<bool, _>("has_discrepancy")
                .map_err(database_error)?
        || summary.created_at
            != codec::timestamp(row.try_get("created_at_ms").map_err(database_error)?)?
        || summary.updated_at
            != codec::timestamp(row.try_get("updated_at_ms").map_err(database_error)?)?
    {
        return Err(corrupt("retained effect projection"));
    }
    let current_assessment: Option<EffectEvidenceEntry> = row
        .try_get::<Option<Vec<u8>>, _>("current_assessment_bytes")
        .map_err(database_error)?
        .map(|b| codec::decode(&b))
        .transpose()?;
    let resolution_id: Option<String> = row.try_get("resolution_id").map_err(database_error)?;
    let disputed: bool = row.try_get("disputed").map_err(database_error)?;
    match (&summary.operator_resolution, &current_assessment) {
        (None, None) if resolution_id.is_none() && !disputed => {}
        (Some(r), Some(e))
            if e.resolution_reference().as_ref() == Some(&r.reference)
                && resolution_id.as_deref() == Some(&r.reference.resolution_id)
                && disputed == r.first_conflicting_entry_sequence.is_some() =>
        {
            e.validate(&occurrence.scope)?;
        }
        _ => return Err(corrupt("effect assessment projection")),
    }
    let assessment_result: Option<Vec<u8>> = row
        .try_get("assessment_result_bytes")
        .map_err(database_error)?;
    let expected = match &current_assessment {
        Some(EffectEvidenceEntry::Assessment {
            assessment: EffectResolutionAssessment::RecordSuccess { result },
            ..
        }) => Some(canonical_json_bytes(result)?),
        _ => None,
    };
    if assessment_result != expected {
        return Err(corrupt("assessment result projection"));
    }
    Ok(Retained {
        current_assessment,
        assessment_result,
        binding,
        summary,
        reference: row.try_get("reference_bytes").map_err(database_error)?,
        journal: row.try_get("journal_bytes").map_err(database_error)?,
    })
}
pub(super) async fn load(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
) -> StoreResult<Option<Retained>> {
    let row=sqlx::query("SELECT current_assessment_bytes,assessment_result_bytes,resolution_id,disputed,binding_bytes,summary_bytes,reference_bytes,journal_bytes,journal_state,has_discrepancy,trunc(effect_revision)::text AS effect_revision_text,trunc(entry_count)::text AS entry_count_text,created_at_ms,updated_at_ms FROM approved_effect_occurrences WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 FOR UPDATE")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).fetch_optional(tx).await?;
    row.as_ref()
        .map(|r| from_row(r, o))
        .transpose()
        .map_err(Into::into)
}
/// Copy only authoritative saved bytes. Import no invented worker or dispatch claim.
pub(crate) async fn ensure(
    tx: &mut PgConnection,
    workflow_id: &str,
    key: &str,
    now: u64,
    verified_dispatch: Option<&str>,
) -> StoreResult<Retained> {
    let row=sqlx::query("SELECT a.snapshot_bytes,a.effect_dispatch_claimed,a.effect_dispatch_attempt_id,a.effect_lookup,a.effect_evidence_state,a.decision_audit_event_id,w.tenant_id,w.namespace FROM workflow_approvals a JOIN workflow_runs w USING(workflow_id) WHERE a.workflow_id=$1 AND a.wait_key=$2")
        .bind(workflow_id).bind(key).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
    let approval_bytes: Vec<u8> = row.try_get("snapshot_bytes")?;
    let approval: ApprovalSnapshot = codec::decode(&approval_bytes)?;
    let activation_id = approval
        .resumed_activation_id
        .clone()
        .ok_or_else(|| corrupt("effect missing resumed activation"))?;
    let occurrence = EffectOccurrence {
        scope: approval.scope.clone(),
        workflow_id: workflow_id.into(),
        activation_id,
        approval_key: key.into(),
    };
    if approval.scope.tenant_id != row.try_get::<String, _>("tenant_id")?
        || approval.scope.namespace != row.try_get::<String, _>("namespace")?
    {
        return Err(corrupt("effect scope").into());
    }
    if let Some(mut saved) = load(tx, &occurrence).await? {
        let lookup: Option<String> = row.try_get("effect_lookup")?;
        if saved.binding.lookup.is_some() && lookup != saved.binding.lookup {
            return Err(ContractError::Conflict.into());
        }
        if saved.binding.lookup.is_none() && lookup.is_some() {
            saved.binding.lookup = lookup;
            save(tx, &saved).await?;
        }
        return Ok(saved);
    }
    if !row.try_get::<bool, _>("effect_dispatch_claimed")? {
        return Err(corrupt("unclaimed retained effect").into());
    }
    // Immutable program payload is fetched only for the first retained copy.
    let program_bytes: Vec<u8> =
        sqlx::query_scalar("SELECT controller_bytes FROM workflow_runs WHERE workflow_id=$1")
            .bind(workflow_id)
            .fetch_one(&mut *tx)
            .await?;
    let program: ProgramDescriptor = codec::decode(&program_bytes)?;
    let dispatch_attempt: Option<String> = row.try_get("effect_dispatch_attempt_id")?;
    let provenance = if let Some(id) = verified_dispatch {
        EffectDispatchProvenance::VerifiedClaim {
            claim_id: id.into(),
        }
    } else if let Some(attempt_id) = dispatch_attempt {
        EffectDispatchProvenance::LegacyAttempt { attempt_id }
    } else {
        EffectDispatchProvenance::LegacyFence
    };
    let binding = EffectEvidenceBinding {
        occurrence: occurrence.clone(),
        key: approved_effect_step_key(key),
        lookup: row.try_get("effect_lookup")?,
        approval_acceptance_event_id: row.try_get("decision_audit_event_id")?,
        program: program.clone(),
        approval,
    };
    binding.validate()?;
    let summary = EffectEvidenceSummary {
        operator_resolution: None,
        occurrence: occurrence.clone(),
        program: program.program,
        callable: binding.approval.action.name.clone(),
        action_version: binding.approval.action.version.clone(),
        dispatch_provenance: provenance,
        journal_state: EffectJournalState::Unconfirmed,
        has_discrepancy: false,
        entry_count: 0,
        effect_revision: 1,
        created_at: now,
        updated_at: now,
    };
    summary.validate()?;
    let binding_bytes = codec::encode(&binding)?;
    let summary_bytes = codec::encode(&summary)?;
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO approved_effect_occurrences(tenant_id,namespace,workflow_id,activation_id,approval_key,journal_state,has_discrepancy,effect_revision,entry_count,created_at_ms,updated_at_ms,binding_bytes,summary_bytes,approval_bytes,program_bytes) VALUES($1,$2,$3,$4,$5,'unconfirmed',false,1,0,$6,$6,$7,$8,$9,$10) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND binding_bytes=$7 AND summary_bytes=$8 AND approval_bytes=$9 AND program_bytes=$10 AND journal_state='unconfirmed' AND NOT has_discrepancy AND effect_revision=1 AND entry_count=0 AND created_at_ms=$6 AND updated_at_ms=$6 AND reference_bytes IS NULL AND journal_bytes IS NULL")
        .bind(&occurrence.scope.tenant_id).bind(&occurrence.scope.namespace).bind(&occurrence.workflow_id).bind(&occurrence.activation_id).bind(&occurrence.approval_key).bind(codec::ms(now)?).bind(binding_bytes).bind(summary_bytes).bind(approval_bytes).bind(program_bytes).fetch_optional(&mut *tx).await?;
    if valid != Some(true) {
        return Err(corrupt("retained effect insert suppressed or changed").into());
    }
    Ok(Retained {
        binding,
        summary,
        reference: None,
        journal: None,
        current_assessment: None,
        assessment_result: None,
    })
}
pub(super) async fn save(tx: &mut PgConnection, r: &Retained) -> StoreResult<()> {
    r.binding.validate()?;
    r.summary.validate()?;
    let o = &r.binding.occurrence;
    let state = match r.summary.journal_state {
        EffectJournalState::Unconfirmed => "unconfirmed",
        EffectJournalState::Confirmed => "confirmed",
    };
    let valid:Option<bool>=sqlx::query_scalar("UPDATE approved_effect_occurrences SET binding_bytes=$6,summary_bytes=$7,journal_state=$8,has_discrepancy=$9,effect_revision=$10::text::numeric,entry_count=$11::text::numeric,updated_at_ms=$12,reference_bytes=$13,journal_bytes=$14,current_assessment_bytes=$16,assessment_result_bytes=$17,resolution_id=$18,disputed=$19 WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND binding_bytes=$6 AND summary_bytes=$7 AND journal_state=$8 AND has_discrepancy=$9 AND effect_revision=$10::text::numeric AND entry_count=$11::text::numeric AND updated_at_ms=$12 AND reference_bytes IS NOT DISTINCT FROM $13 AND journal_bytes IS NOT DISTINCT FROM $14 AND created_at_ms=$15 AND current_assessment_bytes IS NOT DISTINCT FROM $16 AND assessment_result_bytes IS NOT DISTINCT FROM $17 AND resolution_id IS NOT DISTINCT FROM $18 AND disputed=$19")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(codec::encode(&r.binding)?).bind(codec::encode(&r.summary)?).bind(state).bind(r.summary.has_discrepancy).bind(r.summary.effect_revision.to_string()).bind(r.summary.entry_count.to_string()).bind(codec::ms(r.summary.updated_at)?).bind(&r.reference).bind(&r.journal).bind(codec::ms(r.summary.created_at)?).bind(r.current_assessment.as_ref().map(codec::encode).transpose()?).bind(&r.assessment_result).bind(r.summary.operator_resolution.as_ref().map(|v|&v.reference.resolution_id)).bind(r.summary.operator_resolution.as_ref().is_some_and(|v|v.first_conflicting_entry_sequence.is_some())).fetch_optional(tx).await?;
    if valid != Some(true) {
        return Err(corrupt("retained effect update suppressed or changed").into());
    }
    Ok(())
}
pub(super) struct StoredEntry {
    pub entry: EffectEvidenceEntry,
    pub command: Option<Vec<u8>>,
    pub receipt: Option<Vec<u8>>,
    pub operation: String,
}
pub(super) async fn entry(
    tx: &mut PgConnection,
    o: &EffectOccurrence,
    kind: &str,
    id: &str,
) -> StoreResult<Option<StoredEntry>> {
    let row=sqlx::query("SELECT entry_bytes,command_bytes,receipt_bytes,operation FROM approved_effect_evidence_entries WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND entry_kind=$6 AND command_id=$7")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(kind).bind(id).fetch_optional(tx).await?;
    row.map(|r| {
        let e: EffectEvidenceEntry = codec::decode(&r.try_get::<Vec<u8>, _>("entry_bytes")?)?;
        e.validate(&o.scope)?;
        if e.kind() != kind || e.command_id() != id {
            return Err(corrupt("effect entry identity").into());
        }
        Ok(StoredEntry {
            entry: e,
            command: r.try_get("command_bytes")?,
            receipt: r.try_get("receipt_bytes")?,
            operation: r.try_get("operation")?,
        })
    })
    .transpose()
}
pub(super) async fn append(
    tx: &mut PgConnection,
    r: &Retained,
    e: &EffectEvidenceEntry,
    operation: &str,
    claim_sequence: Option<u64>,
    command: Option<Vec<u8>>,
    receipt: Option<Vec<u8>>,
) -> StoreResult<()> {
    let o = &r.binding.occurrence;
    e.validate(&o.scope)?;
    if e.sequence() != r.summary.entry_count || e.recorded_revision() != r.summary.effect_revision {
        return Err(corrupt("effect append revision").into());
    }
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO approved_effect_evidence_entries(tenant_id,namespace,workflow_id,activation_id,approval_key,sequence,entry_kind,command_id,operation,claim_sequence,claim_kind,entry_bytes,command_bytes,receipt_bytes,acceptance_event_id) VALUES($1,$2,$3,$4,$5,$6::text::numeric,$7,$8,$9,$10::text::numeric,CASE WHEN $10::text IS NULL THEN NULL WHEN $9='resolve' THEN 'assessment' ELSE 'claim' END,$11,$12,$13,$14) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND sequence=$6::text::numeric AND entry_kind=$7 AND command_id=$8 AND operation=$9 AND claim_sequence IS NOT DISTINCT FROM $10::text::numeric AND entry_bytes=$11 AND command_bytes IS NOT DISTINCT FROM $12 AND receipt_bytes IS NOT DISTINCT FROM $13 AND acceptance_event_id IS NOT DISTINCT FROM $14")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(e.sequence().to_string()).bind(e.kind()).bind(e.command_id()).bind(operation).bind(claim_sequence.map(|v|v.to_string())).bind(codec::encode(e)?).bind(command).bind(receipt).bind(e.resolution_reference().map(|r|r.acceptance_event_id)).fetch_optional(&mut *tx).await?;
    if valid != Some(true) {
        return Err(corrupt("effect entry insert suppressed or changed").into());
    }
    save(tx, r).await
}
/// Mirror only an actual validated v1 journal; repeated reads create no new fact.
pub(crate) async fn legacy_confirmation(
    tx: &mut PgConnection,
    r: &mut Retained,
    now: u64,
) -> StoreResult<()> {
    if r.journal.is_some() {
        return Ok(());
    }
    let o = &r.binding.occurrence;
    let row=sqlx::query("SELECT j.record_bytes,j.attempt_id,j.accepted_at_ms,a.effect_evidence_state,a.effect_confirmed_attempt_id FROM workflow_approvals a LEFT JOIN workflow_local_results j ON j.activation_id=$3 AND j.step_key=$4 WHERE a.workflow_id=$1 AND a.wait_key=$2")
        .bind(&o.workflow_id).bind(&o.approval_key).bind(&o.activation_id).bind(&r.binding.key).fetch_optional(&mut *tx).await?.ok_or_else(||corrupt("retained approval missing"))?;
    let state: Option<String> = row.try_get("effect_evidence_state")?;
    if !matches!(state.as_deref(), Some("confirmed" | "legacy_confirmed")) {
        return Ok(());
    }
    let bytes: Vec<u8> = row
        .try_get::<Option<Vec<u8>>, _>("record_bytes")?
        .ok_or_else(|| {
            corrupt("confirmed effect journal missing; restore pre-retirement backup")
        })?;
    let record: LocalStepRecord = codec::decode(&bytes)?;
    let lookup =
        approved_effects::confirmed_lookup(&record, &r.binding.key, &r.binding.approval.action)?;
    if r.binding.lookup.as_deref() != Some(lookup) {
        return Err(corrupt("confirmed effect lookup").into());
    }
    let attempt_id: String = row.try_get("attempt_id")?;
    if row
        .try_get::<Option<String>, _>("effect_confirmed_attempt_id")?
        .as_ref()
        != Some(&attempt_id)
    {
        return Err(corrupt("confirmed effect source").into());
    }
    let accepted: i64 = row.try_get("accepted_at_ms")?;
    let journal_accepted_at = u64::try_from(accepted).map_err(|_| corrupt("journal timestamp"))?;
    let observation_id = format!(
        "legacy_{}",
        approved_effect_step_key(&attempt_id).trim_start_matches("_approval:")
    );
    r.journal = Some(bytes);
    r.summary.journal_state = EffectJournalState::Confirmed;
    r.report(&record.output)?;
    let (sequence, recorded_revision) = r.next(now)?;
    let e = EffectEvidenceEntry::Observation {
        sequence,
        recorded_at: now,
        recorded_revision,
        observation_id,
        source: EffectObservationSource::LegacyJournal {
            attempt_id,
            journal_accepted_at,
        },
        output: record.output,
    };
    append(tx, r, &e, "legacy_confirm", None, None, None).await
}
