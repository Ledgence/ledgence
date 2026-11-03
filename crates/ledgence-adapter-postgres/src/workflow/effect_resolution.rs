//! Verified append-only assessments. No assessment creates execution authority.
use super::effect_retained as retained;
use super::*;
impl EffectResolution for PostgresStore {
    fn resolve_effect<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, AuditedEffectResolution> {
        Box::pin(self.run(move || self.resolve_effect_once(p, c)))
    }
}
impl EffectResolutionStore for PostgresStore {
    fn apply_effect_work<'a>(
        &'a self,
        w: &'a WorkflowWork,
        r: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress> {
        Box::pin(self.run(move || self.apply_work_once(w, r)))
    }
}
impl PostgresStore {
    async fn resolve_effect_once(
        &self,
        p: &VerifiedPrincipal,
        c: &EffectResolutionCommand,
    ) -> StoreResult<AuditedEffectResolution> {
        c.validate()?;
        self.require_scope(&c.occurrence.scope)?;
        let bytes = canonical_json_bytes(
            &serde_json::to_value(c).map_err(|_| invalid("effect resolution encoding"))?,
        )
        .map_err(ContractError::from)?;
        let o = &c.occurrence;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        // Collection may have removed the run. Retained assessment/replay remains legal.
        let row=sqlx::query("SELECT state,native_wait_activation_id,retiring_at_ms FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR NO KEY UPDATE")
            .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
        let mut retained = retained::load(&mut tx, o)
            .await?
            .ok_or(ContractError::NotFound)?;
        let now = db::now(&mut tx).await?;
        self.require_operator(p, AccessCapability::EffectResolve, now)?;
        if p.audience().scope != o.scope {
            return Err(ContractError::Forbidden.into());
        }
        if let Some(old) = retained::entry(&mut tx, o, "assessment", &c.resolution_id).await? {
            if old.operation != "resolve" || old.command.as_deref() != Some(&bytes) {
                return Err(ContractError::Conflict.into());
            }
            let mut receipt: EffectResolutionReceipt = codec::decode(
                old.receipt
                    .as_deref()
                    .ok_or_else(|| corrupt("resolution receipt"))?,
            )?;
            let event = crate::operator_audit::load_acceptance(
                &mut tx,
                &receipt.acceptance_event_id,
                &bytes,
            )
            .await?;
            if !event.actor.same_actor(p) || event.audience != *p.audience() {
                return Err(ContractError::Conflict.into());
            }
            receipt.already_accepted = true;
            let proof = AuditedEffectResolution {
                receipt,
                acceptance_event: event,
            };
            proof.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(proof);
        }
        if retained.summary.effect_revision != c.expected_effect_revision
            || retained
                .summary
                .operator_resolution
                .as_ref()
                .map(|r| &r.reference.resolution_id)
                != c.supersedes_resolution_id.as_ref()
            || !retained.binding.approval.action.matches(&c.action)?
        {
            return Err(ContractError::Conflict.into());
        }
        let previous = retained
            .summary
            .operator_resolution
            .as_ref()
            .map(|r| r.reference.sequence);
        let (sequence, revision) = retained.next(now)?;
        let event_id = db::id(&mut tx, "audit").await?;
        let receipt = EffectResolutionReceipt {
            occurrence: o.clone(),
            resolution_id: c.resolution_id.clone(),
            accepted_at: now,
            accepted_revision: revision,
            basis_revision: c.expected_effect_revision,
            supersedes_resolution_id: c.supersedes_resolution_id.clone(),
            assessment_kind: c.assessment.kind(),
            acceptance_event_id: event_id.clone(),
            already_accepted: false,
        };
        let event = AuditEvent {
            event_id: event_id.clone(),
            recorded_at: now,
            audience: p.audience().clone(),
            actor: AuditActor::from_principal(p),
            operation: receipt.audit_operation(),
        };
        let entry = EffectEvidenceEntry::Assessment {
            sequence,
            recorded_at: now,
            recorded_revision: revision,
            resolution_id: c.resolution_id.clone(),
            basis_revision: c.expected_effect_revision,
            supersedes_resolution_id: c.supersedes_resolution_id.clone(),
            assessment: c.assessment.clone(),
            reason: c.reason.clone(),
            evidence_references: c.evidence_references.clone(),
            actor: event.actor.clone(),
            acceptance_event_id: event_id,
        };
        entry.validate(&o.scope)?;
        retained.summary.operator_resolution = Some(EffectResolutionSummary {
            reference: entry
                .resolution_reference()
                .ok_or_else(|| corrupt("assessment reference"))?,
            first_conflicting_entry_sequence: None,
        });
        retained.assessment_result = match &c.assessment {
            EffectResolutionAssessment::RecordSuccess { result } => {
                Some(canonical_json_bytes(result).map_err(ContractError::from)?)
            }
            _ => None,
        };
        retained.current_assessment = Some(entry.clone());
        crate::operator_audit::insert_event(&mut tx, &event, Some(&bytes), None, false).await?;
        retained::append(
            &mut tx,
            &retained,
            &entry,
            "resolve",
            previous,
            Some(bytes),
            Some(codec::encode(&receipt)?),
        )
        .await?;
        if let Some(row) = row
            && row.try_get::<Option<i64>, _>("retiring_at_ms")?.is_none()
            && row.try_get::<String, _>("state")? == "waiting"
            && let Some(parking) = row.try_get::<Option<String>, _>("native_wait_activation_id")?
        {
            let wait = effect_wait::load_wait(&mut tx, &o.workflow_id, &parking).await?;
            if effect_wait::eligible(&wait, &retained)? {
                effect_wait::enqueue(&mut tx, &o.workflow_id, &parking, now).await?;
            }
        }
        let proof = AuditedEffectResolution {
            receipt,
            acceptance_event: event,
        };
        proof.validate_for(p, c)?;
        tx.commit().await?;
        Ok(proof)
    }
}
