//! Authenticated claim provenance and independent evidence. Never re-grant on replay.
use super::effect_retained as retained;
use super::*;

impl ApprovedEffectEvidence for PostgresStore {
    fn claim_v2<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        Box::pin(self.run(move || self.claim_effect_v2_once(p, c)))
    }
    fn confirm_v2<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        Box::pin(self.run(move || self.confirm_effect_v2_once(p, c)))
    }
    fn observe<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        Box::pin(self.run(move || self.observe_effect_once(p, c)))
    }
}
impl PostgresStore {
    fn require_effect_worker(
        &self,
        p: &VerifiedPrincipal,
        scope: &Scope,
        now: u64,
    ) -> Result<EffectWorkerIdentity> {
        self.require_scope(scope)?;
        p.require_at(self.audit_audience()?, now)?;
        if &p.audience().scope != scope {
            return Err(ContractError::Forbidden);
        }
        EffectWorkerIdentity::from_principal(p)
    }
    async fn claim_effect_v2_once(
        &self,
        p: &VerifiedPrincipal,
        c: &EffectClaimV2Command,
    ) -> StoreResult<EffectClaimV2Receipt> {
        c.validate()?;
        self.require_scope(&c.owner.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = execution::lock_activation(&mut tx, &c.owner).await?;
        let now = db::now(&mut tx).await?;
        let worker = self.require_effect_worker(p, &c.owner.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        let evidence = approved_effects::Evidence::load(
            &mut tx,
            &workflow,
            &c.owner,
            &c.workflow_id,
            &c.approval_key,
            &c.lookup,
        )
        .await?;
        if !evidence.approval.action.matches(&c.action)?
            || (evidence.prior.is_some() && !evidence.confirmed())
        {
            return Err(ContractError::Conflict.into());
        }
        let occurrence = c.occurrence();
        let existing = retained::load(&mut tx, &occurrence).await?;
        let old = if existing.is_some() {
            retained::entry(&mut tx, &occurrence, "claim", &c.claim_id).await?
        } else {
            None
        };
        if let Some(old) = &old {
            validate_claim(old, p, c)?;
        }
        // Even a fresh claim ID must recover the original accepted journal without
        // inventing a worker callback/claim fact after a context-read race.
        let receipt = if let Some((bytes, _)) = &evidence.prior {
            let record: LocalStepRecord = codec::decode(bytes)?;
            if approved_effects::confirmed_lookup(&record, &evidence.step_key, &c.action)?
                != c.lookup
            {
                return Err(ContractError::Conflict.into());
            }
            EffectClaimV2Receipt::Replay {
                audience: worker.audience.clone(),
                occurrence,
                record,
            }
        } else if let Some(old) = old {
            let mut claim: EffectClaimBinding = codec::decode(
                old.receipt
                    .as_deref()
                    .ok_or_else(|| corrupt("claim receipt missing"))?,
            )?;
            claim.already_accepted = true;
            EffectClaimV2Receipt::LookupOnly { claim }
        } else {
            approved_effects::grant(&mut tx, &c.legacy(), &evidence).await?;
            let permission = if evidence.claimed {
                ApprovedEffectDisposition::LookupOnly
            } else {
                ApprovedEffectDisposition::Dispatch
            };
            let mut r = retained::ensure(
                &mut tx,
                &c.workflow_id,
                &c.approval_key,
                now,
                (!evidence.claimed).then_some(c.claim_id.as_str()),
            )
            .await?;
            let (sequence, revision) = r.next(now)?;
            let claim = EffectClaimBinding {
                scope: c.owner.scope.clone(),
                workflow_id: c.workflow_id.clone(),
                activation_id: c.owner.task_id.clone(),
                attempt_id: c.owner.attempt_id.clone(),
                approval_key: c.approval_key.clone(),
                action: c.action.clone(),
                lookup: c.lookup.clone(),
                claim_id: c.claim_id.clone(),
                worker: worker.clone(),
                claimed_at: now,
                initial_permission: permission,
                recorded_revision: revision,
                already_accepted: false,
            };
            claim.validate()?;
            let entry = EffectEvidenceEntry::Claim {
                sequence,
                recorded_at: now,
                recorded_revision: revision,
                claim_id: c.claim_id.clone(),
                attempt_id: c.owner.attempt_id.clone(),
                worker,
                initial_permission: permission,
            };
            retained::append(
                &mut tx,
                &r,
                &entry,
                "claim",
                None,
                Some(codec::encode(c)?),
                Some(codec::encode(&claim)?),
            )
            .await?;
            match permission {
                ApprovedEffectDisposition::Dispatch => EffectClaimV2Receipt::Dispatch { claim },
                ApprovedEffectDisposition::LookupOnly => EffectClaimV2Receipt::LookupOnly { claim },
            }
        };
        let now = db::now(&mut tx).await?;
        self.require_effect_worker(p, &c.owner.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        receipt.validate_for(p, c)?;
        tx.commit().await?;
        Ok(receipt)
    }
    async fn confirm_effect_v2_once(
        &self,
        p: &VerifiedPrincipal,
        c: &EffectConfirmV2Command,
    ) -> StoreResult<EffectConfirmV2Receipt> {
        c.validate()?;
        self.require_scope(&c.owner.scope)?;
        let o = c.occurrence();
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        lock_existing_execution(&mut tx, &o).await?;
        let mut r = retained::load(&mut tx, &o)
            .await?
            .ok_or(ContractError::NotFound)?;
        let now = db::now(&mut tx).await?;
        let worker = self.require_effect_worker(p, &o.scope, now)?;
        let claim = retained::entry(&mut tx, &o, "claim", &c.claim_id)
            .await?
            .ok_or(ContractError::NotFound)?;
        let frozen = claim_command(&claim, p, &o, &c.claim_id, c.reported_callback)?;
        if codec::encode(&frozen.owner)? != codec::encode(&c.owner)?
            || frozen.lookup != c.lookup
            || !effect_record_matches(&c.record, &frozen.action, &frozen.lookup)?
            || c.record.key != r.binding.key
        {
            return Err(ContractError::Conflict.into());
        }
        if let Some(old) = retained::entry(&mut tx, &o, "observation", &c.observation_id).await? {
            exact_observation(&old, "confirm", &codec::encode(c)?, p)?;
            let mut receipt: EffectConfirmV2Receipt = codec::decode(
                old.receipt
                    .as_deref()
                    .ok_or_else(|| corrupt("effect confirmation receipt missing"))?,
            )?;
            receipt.already_accepted = true;
            receipt.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        let (workflow, authority) = execution::lock_activation(&mut tx, &c.owner).await?;
        let now = db::now(&mut tx).await?;
        self.require_effect_worker(p, &o.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        let evidence = approved_effects::Evidence::load(
            &mut tx,
            &workflow,
            &c.owner,
            &c.workflow_id,
            &c.approval_key,
            &c.lookup,
        )
        .await?;
        if evidence.lookup.as_deref() != Some(&c.lookup)
            || approved_effects::confirmed_lookup(
                &c.record,
                &evidence.step_key,
                &evidence.approval.action,
            )? != c.lookup
        {
            return Err(ContractError::Conflict.into());
        }
        if let Some((bytes, attempt)) = &evidence.prior {
            if !evidence.confirmed()
                || evidence.confirmed_attempt.as_ref() != Some(attempt)
                || !codec::decode::<LocalStepRecord>(bytes)?.matches(&c.record)?
            {
                return Err(ContractError::Conflict.into());
            }
            r.journal = Some(bytes.clone());
        } else {
            if evidence.state.as_deref() != Some("open") {
                return Err(ContractError::Conflict.into());
            }
            execution::append_local(
                &mut tx,
                &c.owner,
                &c.record,
                now,
                Some((&c.workflow_id, &c.approval_key, &evidence.approval_bytes)),
            )
            .await?;
            r.journal = Some(codec::encode(&c.record)?);
        }
        r.summary.journal_state = EffectJournalState::Confirmed;
        r.report(&c.record.output)?;
        let (sequence, recorded_revision) = r.next(now)?;
        let receipt = EffectConfirmV2Receipt {
            scope: o.scope.clone(),
            workflow_id: o.workflow_id.clone(),
            activation_id: o.activation_id.clone(),
            attempt_id: c.owner.attempt_id.clone(),
            approval_key: o.approval_key.clone(),
            key: c.record.key.clone(),
            already_accepted: false,
            claim_id: c.claim_id.clone(),
            observation_id: c.observation_id.clone(),
            worker: worker.clone(),
            recorded_at: now,
            recorded_revision,
        };
        let entry = EffectEvidenceEntry::Observation {
            sequence,
            recorded_at: now,
            recorded_revision,
            observation_id: c.observation_id.clone(),
            source: EffectObservationSource::VerifiedWorkerClaim {
                claim_id: c.claim_id.clone(),
                worker,
                reported_callback: c.reported_callback,
                ingress: EffectObservationIngress::LiveConfirm,
            },
            output: c.record.output.clone(),
        };
        retained::append(
            &mut tx,
            &r,
            &entry,
            "confirm",
            Some(claim.entry.sequence()),
            Some(codec::encode(c)?),
            Some(codec::encode(&receipt)?),
        )
        .await?;
        let now = db::now(&mut tx).await?;
        self.require_effect_worker(p, &o.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        receipt.validate_for(p, c)?;
        tx.commit().await?;
        Ok(receipt)
    }
    async fn observe_effect_once(
        &self,
        p: &VerifiedPrincipal,
        c: &EffectObservationCommand,
    ) -> StoreResult<EffectObservationReceipt> {
        c.validate()?;
        let o = &c.occurrence;
        self.require_scope(&o.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        lock_existing_execution(&mut tx, o).await?;
        let mut r = retained::load(&mut tx, o)
            .await?
            .ok_or(ContractError::NotFound)?;
        let now = db::now(&mut tx).await?;
        let worker = self.require_effect_worker(p, &o.scope, now)?;
        let claim = retained::entry(&mut tx, o, "claim", &c.claim_id)
            .await?
            .ok_or(ContractError::NotFound)?;
        claim_command(&claim, p, o, &c.claim_id, c.reported_callback)?;
        if let Some(old) = retained::entry(&mut tx, o, "observation", &c.observation_id).await? {
            exact_observation(&old, "observe", &codec::encode(c)?, p)?;
            let mut receipt: EffectObservationReceipt = codec::decode(
                old.receipt
                    .as_deref()
                    .ok_or_else(|| corrupt("effect observation receipt missing"))?,
            )?;
            receipt.already_accepted = true;
            receipt.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        r.report(&c.output)?;
        let (sequence, recorded_revision) = r.next(now)?;
        let receipt = EffectObservationReceipt {
            occurrence: o.clone(),
            claim_id: c.claim_id.clone(),
            observation_id: c.observation_id.clone(),
            worker: worker.clone(),
            recorded_at: now,
            recorded_revision,
            already_accepted: false,
            disposition: EffectObservationDisposition::EvidenceOnly,
        };
        let entry = EffectEvidenceEntry::Observation {
            sequence,
            recorded_at: now,
            recorded_revision,
            observation_id: c.observation_id.clone(),
            source: EffectObservationSource::VerifiedWorkerClaim {
                claim_id: c.claim_id.clone(),
                worker,
                reported_callback: c.reported_callback,
                ingress: EffectObservationIngress::LateObserve,
            },
            output: c.output.clone(),
        };
        retained::append(
            &mut tx,
            &r,
            &entry,
            "observe",
            Some(claim.entry.sequence()),
            Some(codec::encode(c)?),
            Some(codec::encode(&receipt)?),
        )
        .await?;
        self.require_effect_worker(p, &o.scope, db::now(&mut tx).await?)?;
        receipt.validate_for(p, c)?;
        tx.commit().await?;
        Ok(receipt)
    }
}
fn validate_claim(
    entry: &retained::StoredEntry,
    p: &VerifiedPrincipal,
    c: &EffectClaimV2Command,
) -> Result<()> {
    if entry.operation != "claim" || entry.command.as_deref() != Some(codec::encode(c)?.as_slice())
    {
        return Err(ContractError::Conflict);
    }
    match &entry.entry {
        EffectEvidenceEntry::Claim { worker, .. } if worker.matches_principal(p) => Ok(()),
        _ => Err(ContractError::Conflict),
    }
}
fn claim_command(
    entry: &retained::StoredEntry,
    p: &VerifiedPrincipal,
    o: &EffectOccurrence,
    id: &str,
    mode: EffectReportedCallback,
) -> Result<EffectClaimV2Command> {
    let c: EffectClaimV2Command = codec::decode(
        entry
            .command
            .as_deref()
            .ok_or_else(|| corrupt("missing effect claim command"))?,
    )?;
    c.validate()?;
    if c.occurrence() != *o || c.claim_id != id {
        return Err(ContractError::Conflict);
    }
    validate_claim(entry, p, &c)?;
    let EffectEvidenceEntry::Claim {
        initial_permission, ..
    } = &entry.entry
    else {
        return Err(corrupt("effect claim type"));
    };
    if mode == EffectReportedCallback::Dispatch
        && *initial_permission != ApprovedEffectDisposition::Dispatch
    {
        return Err(ContractError::Conflict);
    }
    Ok(c)
}
fn exact_observation(
    entry: &retained::StoredEntry,
    operation: &str,
    bytes: &[u8],
    p: &VerifiedPrincipal,
) -> Result<()> {
    if entry.operation != operation || entry.command.as_deref() != Some(bytes) {
        return Err(ContractError::Conflict);
    }
    match &entry.entry {
        EffectEvidenceEntry::Observation {
            source: EffectObservationSource::VerifiedWorkerClaim { worker, .. },
            ..
        } if worker.matches_principal(p) => Ok(()),
        _ => Err(ContractError::Conflict),
    }
}
/// Retained receipt replay must still work when execution rows have been collected.
/// Existing rows lock in the same order as v1 and retention; no authority is granted here.
async fn lock_existing_execution(tx: &mut PgConnection, o: &EffectOccurrence) -> StoreResult<()> {
    let workflow:Option<String>=sqlx::query_scalar("SELECT workflow_id FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR NO KEY UPDATE")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
    if workflow.is_some() {
        sqlx::query("SELECT task_id FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 FOR NO KEY UPDATE").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.activation_id).fetch_optional(tx).await?;
    }
    Ok(())
}
