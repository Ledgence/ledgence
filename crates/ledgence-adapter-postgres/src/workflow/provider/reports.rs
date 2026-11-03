use super::*;
impl PostgresStore {
    pub(super) async fn provider_report_once<C: ProviderReportedCommand>(
        &self,
        p: &VerifiedPrincipal,
        c: &C,
        owner: Option<&LeaseOwner>,
    ) -> StoreResult<ProviderReportReply> {
        c.validate()?;
        self.provider_configured()?;
        let o = &c.ticket().occurrence;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let run = lock_live(&mut tx, o).await?;
        // A live dispatch owner is not the active probe; lock it before occurrence.
        if let Some(owner) = owner
            && run.is_some()
        {
            db::load_task(&mut tx, &o.scope, &owner.task_id, true).await?;
        }
        let reply = self
            .provider_report_tx(&mut tx, p, c, owner, run.as_ref(), false)
            .await?;
        tx.commit().await?;
        Ok(reply)
    }
    /// Append business evidence inside the caller's transaction. Resource admission
    /// callers lock their resource first, including when execution history retired.
    pub(in crate::workflow) async fn provider_report_tx<C: ProviderReportedCommand>(
        &self,
        tx: &mut PgConnection,
        p: &VerifiedPrincipal,
        c: &C,
        owner: Option<&LeaseOwner>,
        run: Option<&RunRecord>,
        composed: bool,
    ) -> StoreResult<ProviderReportReply> {
        let o = &c.ticket().occurrence;
        let mut r = retained::load(tx, o)
            .await?
            .ok_or(ContractError::NotFound)?;
        let mut state = load(tx, &r).await?;
        let now = db::now(tx).await?;
        let worker = self.require_effect_worker(p, &o.scope, now)?;
        let source = source(tx, o, c.source_id())
            .await?
            .ok_or(ContractError::NotFound)?;
        if !composed
            && matches!(source.source, ProviderSourceReference::Dispatch { .. })
            && binding(&r)?.admission.is_some()
        {
            return Err(invalid("governed dispatch reports require the composed source").into());
        }
        if !source.worker.same_worker(&worker) {
            return Err(ContractError::Forbidden.into());
        }
        if source.ticket != *c.ticket()
            || source.report_id != c.report_id()
            || ticket(&r)? != *c.ticket()
            || !c.source_matches(&source.source)
            || c.report().reported_target != binding(&r)?.target
        {
            return Err(ContractError::Conflict.into());
        }
        if let Some(owner) = owner
            && canonical(owner)? != canonical(&source.owner)?
        {
            return Err(ContractError::Conflict.into());
        }
        let existing=sqlx::query("SELECT report_bytes,receipt_bytes FROM provider_reports WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND source_id=$6")
          .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(c.source_id()).fetch_optional(&mut *tx).await?;
        if let Some(row) = existing {
            if row.try_get::<Vec<u8>, _>("report_bytes")? != canonical(c.report())? {
                return Err(ContractError::Conflict.into());
            }
            let mut receipt: ProviderReportReceipt =
                codec::decode(&row.try_get::<Vec<u8>, _>("receipt_bytes")?)?;
            receipt.already_accepted = true;
            let reply = if receipt.accepted_via == c.ingress() {
                ProviderReportReply::Accepted { receipt }
            } else {
                ProviderReportReply::ExistingReport {
                    original_receipt: receipt,
                }
            };
            reply.validate_for(p, c)?;
            return Ok(reply);
        }
        if let Some(owner) = owner {
            let run = run.as_ref().ok_or(ContractError::ObsoleteOperation)?;
            if !matches!(
                run.snapshot.state,
                WorkflowState::Running | WorkflowState::Waiting
            ) {
                return Err(ContractError::ObsoleteOperation.into());
            }
            check_owner(tx, o, owner, now, true).await?;
            if let ProviderSourceReference::Probe {
                probe_id,
                generation,
                ..
            } = &source.source
            {
                let q = &summary(&r)?.reconciliation;
                if q.active_probe_id.as_ref() != Some(probe_id)
                    || q.generation != *generation
                    || q.stopped_reason.is_some()
                {
                    return Err(ContractError::ObsoleteOperation.into());
                }
            }
        }
        let (sequence, revision) = r.next(now)?;
        let entry = EffectEvidenceEntry::ProviderReport {
            sequence,
            recorded_at: now,
            recorded_revision: revision,
            source_id: c.source_id().into(),
            source: source.source.clone(),
            report_id: c.report_id().into(),
            worker: source.worker.clone(),
            ingress: c.ingress(),
            report: Box::new(c.report().clone()),
        };
        let receipt = ProviderReportReceipt {
            ticket: c.ticket().clone(),
            source_id: c.source_id().into(),
            source: source.source.clone(),
            report_id: c.report_id().into(),
            report_digest: c.report().digest()?,
            worker: source.worker,
            accepted_at: now,
            recorded_revision: revision,
            accepted_via: c.ingress(),
            already_accepted: false,
        };
        aggregate(tx, &mut r, &mut state, &entry, now).await?;
        retained::append(
            tx,
            &r,
            &entry,
            "provider",
            None,
            Some(canonical(c)?),
            Some(codec::encode(&receipt)?),
        )
        .await?;
        let valid:Option<bool>=sqlx::query_scalar("INSERT INTO provider_reports(tenant_id,namespace,workflow_id,activation_id,approval_key,source_id,report_id,report_bytes,receipt_bytes,entry_sequence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::text::numeric) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND source_id=$6 AND report_id=$7 AND report_bytes=$8 AND receipt_bytes=$9 AND entry_sequence=$10::text::numeric")
          .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(c.source_id()).bind(c.report_id()).bind(canonical(c.report())?).bind(codec::encode(&receipt)?).bind(sequence.to_string()).fetch_optional(&mut *tx).await?;
        proof(valid, "provider report changed or suppressed")?;
        save(tx, &r, &state).await?;
        if run.is_some()
            && c.ingress() != ProviderReportIngress::LiveProbe
            && summary(&r)?.reconciliation.stopped_reason.is_some()
        {
            scheduler::cancel_active(tx, &r, now).await?;
        }
        if let Some(run) = run {
            wait::notify(tx, run, &r, &state, now).await?;
        }
        let now = db::now(tx).await?;
        self.require_effect_worker(p, &o.scope, now)?;
        if let Some(owner) = owner {
            check_owner(tx, o, owner, now, true).await?;
        }
        let reply = ProviderReportReply::Accepted { receipt };
        reply.validate_for(p, c)?;
        Ok(reply)
    }
    pub(super) async fn provider_context_once(
        &self,
        p: &VerifiedPrincipal,
        c: &ProviderProbeContextCommand,
    ) -> StoreResult<ProviderProbeContext> {
        c.validate()?;
        self.provider_configured()?;
        let o = &c.probe.occurrence;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let run = lock_live(&mut tx, o)
            .await?
            .ok_or(ContractError::ObsoleteOperation)?;
        let mut r = retained::load(&mut tx, o)
            .await?
            .ok_or(ContractError::NotFound)?;
        let state = load(&mut tx, &r).await?;
        let now = db::now(&mut tx).await?;
        let worker = self.require_effect_worker(p, &o.scope, now)?;
        if !matches!(
            run.snapshot.state,
            WorkflowState::Running | WorkflowState::Waiting
        ) {
            return Err(ContractError::ObsoleteOperation.into());
        }
        let q = &summary(&r)?.reconciliation;
        if q.active_probe_id.as_ref() != Some(&c.probe.probe_id)
            || q.active_task_id.as_ref() != Some(&c.owner.task_id)
            || q.generation != c.probe.generation
            || q.stopped_reason.is_some()
        {
            return Err(ContractError::ObsoleteOperation.into());
        }
        check_owner(&mut tx, o, &c.owner, now, false).await?;
        let task = db::load_task(&mut tx, &o.scope, &c.owner.task_id, false).await?;
        if task.provider_probe.as_ref() != Some(&c.probe) {
            return Err(ContractError::Conflict.into());
        }
        let row=sqlx::query("SELECT assignment_bytes,descriptor_bytes,provider_reference FROM provider_probe_tasks WHERE probe_id=$1 AND live_task_id=$2 AND consumed_at_ms IS NULL").bind(&c.probe.probe_id).bind(&c.owner.task_id).fetch_optional(&mut *tx).await?.ok_or(ContractError::ObsoleteOperation)?;
        if row.try_get::<Vec<u8>, _>("assignment_bytes")? != codec::encode(&c.probe)?
            || row.try_get::<Vec<u8>, _>("descriptor_bytes")?
                != codec::encode(&binding(&r)?.program)?
        {
            return Err(corrupt("probe immutable binding").into());
        }
        let old: Option<String> = sqlx::query_scalar(
            "SELECT source_id FROM provider_invocations WHERE probe_id=$1 AND attempt_id=$2",
        )
        .bind(&c.probe.probe_id)
        .bind(&c.owner.attempt_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(id) = old {
            let source = source(&mut tx, o, &id)
                .await?
                .ok_or_else(|| corrupt("probe source"))?;
            if !source.worker.same_worker(&worker)
                || canonical(&source.owner)? != canonical(&c.owner)?
            {
                return Err(ContractError::Conflict.into());
            }
            let ctx = source.context.ok_or_else(|| corrupt("probe context"))?;
            ctx.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(ctx);
        }
        let b = binding(&r)?.clone();
        let context = ProviderProbeContext {
            ticket: ticket(&r)?,
            binding: b.clone(),
            action: r.binding.approval.action.clone(),
            probe_id: c.probe.probe_id.clone(),
            generation: c.probe.generation,
            task_id: c.owner.task_id.clone(),
            attempt_id: c.owner.attempt_id.clone(),
            source_id: db::id(&mut tx, "psrc").await?,
            report_id: db::id(&mut tx, "prep").await?,
            worker: worker.clone(),
            provider_reference: row.try_get("provider_reference")?,
            check_timeout_ms: b.reconciliation.check_timeout_ms,
        };
        context.validate_for(p, c)?;
        let source = Source {
            source_id: context.source_id.clone(),
            report_id: context.report_id.clone(),
            ticket: context.ticket.clone(),
            worker: worker.clone(),
            source: ProviderSourceReference::Probe {
                probe_id: c.probe.probe_id.clone(),
                generation: c.probe.generation,
                task_id: c.owner.task_id.clone(),
                attempt_id: c.owner.attempt_id.clone(),
            },
            owner: c.owner.clone(),
            context: Some(context.clone()),
        };
        insert_source(&mut tx, &source, c).await?;
        let (sequence, revision) = r.next(now)?;
        let e = EffectEvidenceEntry::ProviderProbeSource {
            sequence,
            recorded_revision: revision,
            recorded_at: now,
            probe_id: c.probe.probe_id.clone(),
            generation: c.probe.generation,
            task_id: c.owner.task_id.clone(),
            attempt_id: c.owner.attempt_id.clone(),
            source_id: context.source_id.clone(),
            report_id: context.report_id.clone(),
            worker,
        };
        retained::append(
            &mut tx,
            &r,
            &e,
            "provider",
            None,
            Some(canonical(c)?),
            Some(codec::encode(&context)?),
        )
        .await?;
        save(&mut tx, &r, &state).await?;
        let now = db::now(&mut tx).await?;
        self.require_effect_worker(p, &o.scope, now)?;
        check_owner(&mut tx, o, &c.owner, now, false).await?;
        tx.commit().await?;
        Ok(context)
    }
}
pub(super) async fn aggregate(
    tx: &mut PgConnection,
    r: &mut Retained,
    state: &mut State,
    entry: &EffectEvidenceEntry,
    now: u64,
) -> StoreResult<()> {
    let EffectEvidenceEntry::ProviderReport { report, .. } = entry else {
        return Err(corrupt("provider aggregate kind").into());
    };
    let reference = entry
        .provider_report_reference()
        .ok_or_else(|| corrupt("report reference"))?;
    if let Some(value) = &report.provider_reference
        && state
            .provider_reference
            .as_ref()
            .is_none_or(|old| old == value)
    {
        state.provider_reference = Some(value.clone());
    }
    let (new_reference, reference_facts) = if let Some(value) = &report.provider_reference {
        record_fact(
            tx,
            r,
            "reference",
            &canonical(&("reference", value))?,
            &reference,
        )
        .await?
    } else {
        (false, Vec::new())
    };
    let reference_conflict = reference_facts.len() == 2;
    let mut first_new_terminal = false;
    if report.knowledge.is_terminal() {
        let bytes = canonical(&("terminal", &report.knowledge, &report.provider_reference))?;
        let (fresh, facts) = record_fact(tx, r, "terminal", &bytes, &reference).await?;
        first_new_terminal = fresh;
        if facts.len() == 2 {
            dispute(r, state, facts)?;
            if summary(r)?.handling != ProviderHandlingState::OperatorClosed {
                summary_mut(r)?.handling = ProviderHandlingState::ProviderTerminal;
            }
        } else {
            let s = summary_mut(r)?;
            if !matches!(
                s.knowledge,
                ProviderKnowledgeKind::Succeeded
                    | ProviderKnowledgeKind::Failed
                    | ProviderKnowledgeKind::Cancelled
                    | ProviderKnowledgeKind::Disputed
            ) {
                s.knowledge = report.knowledge.kind();
                s.selected_report = Some(reference.clone());
                state.current_report = Some(entry.clone());
            }
            if s.handling != ProviderHandlingState::OperatorClosed {
                s.handling = ProviderHandlingState::ProviderTerminal;
            }
        }
    } else if nonterminal_rank(report.knowledge.kind()) > nonterminal_rank(summary(r)?.knowledge)
        || summary(r)?.knowledge == ProviderKnowledgeKind::Unknown
            && summary(r)?.selected_report.is_none()
    {
        let s = summary_mut(r)?;
        s.knowledge = report.knowledge.kind();
        s.selected_report = Some(reference.clone());
        state.current_report = Some(entry.clone());
    }
    if reference_conflict {
        dispute(r, state, reference_facts)?;
    }
    // Repeated facts covered by an assessment never fabricate a new conflict.
    if (first_new_terminal || new_reference && reference_conflict)
        && r.summary
            .operator_resolution
            .as_ref()
            .is_some_and(|v| v.first_conflicting_entry_sequence.is_none())
    {
        let compatible = match (&r.current_assessment, &report.knowledge) {
            (
                Some(EffectEvidenceEntry::Assessment {
                    assessment: EffectResolutionAssessment::CloseUnknown,
                    ..
                }),
                _,
            ) => true,
            (
                Some(EffectEvidenceEntry::Assessment {
                    assessment: EffectResolutionAssessment::RecordSuccess { result },
                    ..
                }),
                ProviderKnowledge::Succeeded { result: actual },
            ) => !(new_reference && reference_conflict) && canonical(result)? == canonical(actual)?,
            (
                Some(EffectEvidenceEntry::Assessment {
                    assessment: EffectResolutionAssessment::RecordFailure { failure },
                    ..
                }),
                ProviderKnowledge::Failed { failure: actual },
            ) => {
                !(new_reference && reference_conflict) && canonical(failure)? == canonical(actual)?
            }
            _ => false,
        };
        if !compatible {
            r.summary
                .operator_resolution
                .as_mut()
                .expect("present")
                .first_conflicting_entry_sequence = Some(reference.sequence);
        }
    }
    refresh_candidate(r, state, now)?;
    Ok(())
}
pub(super) fn refresh_candidate(r: &mut Retained, state: &mut State, _now: u64) -> Result<()> {
    let operator = r.summary.operator_resolution.clone();
    let candidate = if let Some(resolution) = operator {
        let outcome = if let Some(sequence) = resolution.first_conflicting_entry_sequence {
            EffectResolutionOutcome::Disputed {
                first_conflicting_entry_sequence: sequence,
            }
        } else {
            match r.current_assessment.as_ref() {
                Some(EffectEvidenceEntry::Assessment { assessment, .. }) => match assessment {
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
                    EffectResolutionAssessment::CloseUnknown => {
                        EffectResolutionOutcome::ClosedUnknown
                    }
                },
                _ => return Err(corrupt("provider assessment missing")),
            }
        };
        Some(ProviderSelection::OperatorAssessment {
            reference: Box::new(resolution.reference),
            outcome,
        })
    } else if summary(r)?.knowledge == ProviderKnowledgeKind::Disputed {
        Some(ProviderSelection::Disputed {
            first_conflicting_entry_sequence: summary(r)?.conflicting_reports[1].sequence,
        })
    } else if let Some(EffectEvidenceEntry::ProviderReport { report, .. }) = &state.current_report {
        let reference = summary(r)?
            .selected_report
            .clone()
            .ok_or_else(|| corrupt("selected provider reference"))?;
        match &report.knowledge {
            ProviderKnowledge::Succeeded { result } => Some(ProviderSelection::ProviderSucceeded {
                report_ref: reference,
                result: result.clone(),
            }),
            ProviderKnowledge::Failed { failure } => Some(ProviderSelection::ProviderFailed {
                report_ref: reference,
                failure: failure.clone(),
            }),
            ProviderKnowledge::Cancelled { reason } => Some(ProviderSelection::ProviderCancelled {
                report_ref: reference,
                reason: reason.clone(),
            }),
            _ => None,
        }
    } else {
        None
    };
    let stop = if operator_present(r) {
        Some(ProviderRecoveryStopReason::OperatorAssessment)
    } else {
        match summary(r)?.knowledge {
            ProviderKnowledgeKind::Succeeded
            | ProviderKnowledgeKind::Failed
            | ProviderKnowledgeKind::Cancelled => {
                Some(ProviderRecoveryStopReason::ProviderTerminal)
            }
            ProviderKnowledgeKind::Disputed => Some(ProviderRecoveryStopReason::Disputed),
            ProviderKnowledgeKind::RequiresAction => {
                Some(ProviderRecoveryStopReason::ActionRequired)
            }
            _ => None,
        }
    };
    if let Some(reason) = stop {
        let q = &mut summary_mut(r)?.reconciliation;
        q.stopped_reason = Some(reason);
        q.next_due_at = None;
    }
    let candidate = candidate.or_else(|| {
        let q = &summary(r).ok()?.reconciliation;
        q.stopped_reason
            .filter(|reason| {
                !matches!(
                    reason,
                    ProviderRecoveryStopReason::ProviderTerminal
                        | ProviderRecoveryStopReason::OperatorAssessment
                )
            })
            .map(|reason| ProviderSelection::NeedsReview {
                reason,
                checks_used: q.checks_used,
                deadline_at: q.deadline_at,
            })
    });
    select_candidate(r, state, candidate)
}
fn operator_present(r: &Retained) -> bool {
    r.summary.operator_resolution.is_some()
}

// A later less informative response remains evidence without erasing knowledge.
// Equal-ranked reports also preserve the original selected source deterministically.
fn nonterminal_rank(kind: ProviderKnowledgeKind) -> u8 {
    match kind {
        ProviderKnowledgeKind::Unknown => 0,
        ProviderKnowledgeKind::Pending => 1,
        ProviderKnowledgeKind::RequiresAction => 2,
        ProviderKnowledgeKind::Succeeded
        | ProviderKnowledgeKind::Failed
        | ProviderKnowledgeKind::Cancelled
        | ProviderKnowledgeKind::Disputed => 3,
    }
}

async fn record_fact(
    tx: &mut PgConnection,
    r: &Retained,
    kind: &str,
    bytes: &[u8],
    reference: &ProviderReportReference,
) -> StoreResult<(bool, Vec<ProviderReportReference>)> {
    let o = &r.binding.occurrence;
    let old:Option<Vec<u8>>=sqlx::query_scalar("SELECT fact_bytes FROM provider_terminal_facts WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND fact_digest=sha256($6::bytea)").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(bytes).fetch_optional(&mut *tx).await?;
    let fresh = old.is_none();
    if let Some(old) = old {
        if old != bytes {
            return Err(corrupt("provider fact digest collision").into());
        }
    } else {
        let valid:Option<bool>=sqlx::query_scalar("INSERT INTO provider_terminal_facts(tenant_id,namespace,workflow_id,activation_id,approval_key,fact_digest,fact_bytes,first_seen_revision,first_report_bytes,fact_kind) VALUES($1,$2,$3,$4,$5,sha256($6::bytea),$6,$7::text::numeric,$8,$9) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND fact_digest=sha256($6::bytea) AND fact_bytes=$6 AND first_seen_revision=$7::text::numeric AND first_report_bytes=$8 AND fact_kind=$9")
          .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(bytes).bind(r.summary.effect_revision.to_string()).bind(codec::encode(reference)?).bind(kind).fetch_optional(&mut *tx).await?;
        proof(valid, "provider fact insert changed or suppressed")?;
    }
    let rows:Vec<Vec<u8>>=sqlx::query_scalar("SELECT first_report_bytes FROM provider_terminal_facts WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND fact_kind=$6 ORDER BY first_seen_revision LIMIT 2").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(kind).fetch_all(tx).await?;
    Ok((
        fresh,
        rows.iter()
            .map(|b| codec::decode(b))
            .collect::<Result<_>>()?,
    ))
}
fn dispute(
    r: &mut Retained,
    state: &mut State,
    mut references: Vec<ProviderReportReference>,
) -> Result<()> {
    if references.len() != 2 {
        return Err(corrupt("provider conflict references"));
    }
    references.sort_by_key(|r| r.sequence);
    let s = summary_mut(r)?;
    // Preserve the earliest pair establishing disagreement, not the latest arrival.
    if s.conflicting_reports.len() != 2
        || (references[1].sequence, references[0].sequence)
            < (
                s.conflicting_reports[1].sequence,
                s.conflicting_reports[0].sequence,
            )
    {
        s.conflicting_reports = references;
    }
    s.knowledge = ProviderKnowledgeKind::Disputed;
    s.selected_report = None;
    state.current_report = None;
    r.summary.has_discrepancy = true;
    Ok(())
}
