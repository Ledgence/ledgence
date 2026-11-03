use super::*;
impl PostgresStore {
    pub(super) async fn start_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &AdmissionStartCommand,
    ) -> StoreResult<AdmissionStartReceipt> {
        c.validate()?;
        if self.admission_configured()?.dispatch != AdmissionDispatchPolicy::Enabled {
            return Err(ContractError::Unavailable(
                "resource admission dispatch is quarantined".into(),
            )
            .into());
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.admission_worker(p, &c.occurrence.scope, db::now(&mut tx).await?)?;
        let name = request_resource(&mut tx, &c.occurrence).await?;
        let started:bool=sqlx::query_scalar("SELECT start_command_bytes IS NOT NULL FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND origin_activation_id=$4 AND logical_key=$5")
            .bind(&c.occurrence.scope.tenant_id).bind(&c.occurrence.scope.namespace).bind(&c.occurrence.workflow_id).bind(&c.occurrence.origin_activation_id).bind(&c.occurrence.key).fetch_one(&mut *tx).await?;
        if started {
            let _resource = resource(&mut tx, &c.occurrence.scope, &name, true).await?;
            self.admission_worker(p, &c.occurrence.scope, db::now(&mut tx).await?)?;
            let a = request(&mut tx, &c.occurrence).await?;
            require_operation(&a)?;
            check_replay(&a, c)?;
            tx.commit().await?;
            return Ok(AdmissionStartReceipt::Existing {
                snapshot: Box::new(a.snapshot),
            });
        }
        let (workflow, authority) =
            super::super::execution::lock_activation(&mut tx, &c.owner).await?;
        if workflow.workflow_id != c.occurrence.workflow_id {
            return Err(ContractError::Conflict.into());
        }
        let mut r = resource(&mut tx, &c.occurrence.scope, &name, true).await?;
        let now = db::now(&mut tx).await?;
        let worker = self.admission_worker(p, &c.occurrence.scope, now)?;
        let mut a = request(&mut tx, &c.occurrence).await?;
        require_operation(&a)?;
        if a.start_bytes.is_some() {
            check_replay(&a, c)?;
            tx.commit().await?;
            return Ok(AdmissionStartReceipt::Existing {
                snapshot: Box::new(a.snapshot),
            });
        }
        if now < r.high_water || now < a.snapshot.updated_at {
            return Err(ContractError::Unavailable(
                "resource admission clock has not reached its high-water mark".into(),
            )
            .into());
        }
        let invalidated = invalidate_offer(&mut a, &mut r, now)?;
        let eligible = a.snapshot.offer.as_ref().is_some_and(|o| {
            o.generation == c.offer_generation
                && o.activation_id == c.owner.task_id
                && o.policy_revision == r.record.revision
                && now < o.expires_at
        }) && r.record.policy.enabled
            && now < a.snapshot.deadline_at;
        if !eligible {
            if invalidated {
                save_request(&mut tx, &a).await?;
                save_resource(&mut tx, &r).await?;
            }
            tx.commit().await?;
            return Ok(AdmissionStartReceipt::Existing {
                snapshot: Box::new(a.snapshot),
            });
        }
        super::super::execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        // Refill after lock and spend once. Offers reserve shadow credit, never rate tokens.
        r.refill(now)?;
        if let Some(rate) = &r.record.policy.rate {
            let b = core::spend_admission_token(
                &core::AdmissionBucket {
                    credit: r.credit.ok_or_else(|| corrupt("admission rate credit"))?,
                    high_water_at: r.high_water,
                },
                rate,
            )?;
            r.credit = Some(b.credit);
            r.high_water = b.high_water_at;
        }
        if r.offered == 0 || r.started >= u64::from(r.record.policy.max_occupancy) {
            return Err(corrupt("offer occupancy authority").into());
        }
        let start_deadline_at = now
            .checked_add(u64::from(r.record.policy.start_timeout_ms))
            .ok_or_else(|| corrupt("admission start deadline"))?
            .min(authority.valid_until());
        let source = AdmissionSource {
            occurrence: c.occurrence.clone(),
            source_id: db::id(&mut tx, "admsrc").await?,
            start_id: c.start_id.clone(),
            finish_id: c.finish_id.clone(),
            generation: c.offer_generation,
            binding_digest: a.snapshot.binding.digest()?,
            owner: c.owner.clone(),
            worker,
            accepted_at: now,
            start_deadline_at,
        };
        source.validate()?;
        a.snapshot.state = AdmissionState::PossiblyDispatched;
        a.snapshot.offer = None;
        a.snapshot.source = Some(source.clone());
        a.start_bytes = Some(canonical(c)?);
        a.check_at = source_check_at(&mut tx, &source, now).await?;
        a.bump(now)?;
        r.offered -= 1;
        r.started += 1;
        r.wake(now);
        save_request(&mut tx, &a).await?;
        save_resource(&mut tx, &r).await?;
        tx.commit().await?;
        Ok(AdmissionStartReceipt::Granted {
            source: Box::new(source),
        })
    }
    pub(super) async fn finish_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &AdmissionFinishCommand,
    ) -> StoreResult<AdmissionFinishReceipt> {
        c.validate()?;
        self.admission_configured()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.admission_worker(p, &c.source.occurrence.scope, db::now(&mut tx).await?)?;
        if !c.source.worker.matches_principal(p) {
            return Err(ContractError::Forbidden.into());
        }
        lock_retained_workflow(&mut tx, &c.source.occurrence).await?;
        let name = request_resource(&mut tx, &c.source.occurrence).await?;
        let mut r = resource(&mut tx, &c.source.occurrence.scope, &name, true).await?;
        let now = db::now(&mut tx).await?;
        self.admission_worker(p, &c.source.occurrence.scope, now)?;
        let mut a = request(&mut tx, &c.source.occurrence).await?;
        require_operation(&a)?;
        if a.snapshot.source.as_ref() != Some(&c.source) {
            return Err(ContractError::Conflict.into());
        }
        if let Some(bytes) = &a.finish_bytes {
            if bytes != &canonical(c)? {
                return Err(ContractError::Conflict.into());
            }
            let mut receipt = a
                .snapshot
                .finish
                .ok_or_else(|| corrupt("missing admission result"))?;
            receipt.already_accepted = true;
            tx.commit().await?;
            return Ok(receipt);
        }
        if !matches!(
            a.snapshot.state,
            AdmissionState::PossiblyDispatched
                | AdmissionState::UnknownOccupied
                | AdmissionState::AdministrativelyReleased
        ) {
            return Err(ContractError::Conflict.into());
        }
        let receipt = AdmissionFinishReceipt {
            source: c.source.clone(),
            output: c.output.clone(),
            accepted_at: now.max(c.source.accepted_at).max(a.snapshot.updated_at),
            already_accepted: false,
        };
        receipt.validate()?;
        if a.snapshot.state != AdmissionState::AdministrativelyReleased {
            r.started = r
                .started
                .checked_sub(1)
                .ok_or_else(|| corrupt("admission occupancy underflow"))?;
            if a.snapshot.state == AdmissionState::UnknownOccupied {
                r.unknown = r
                    .unknown
                    .checked_sub(1)
                    .ok_or_else(|| corrupt("admission unknown underflow"))?;
            }
            a.snapshot.state = AdmissionState::Finished;
        }
        a.snapshot.finish = Some(receipt.clone());
        a.finish_bytes = Some(canonical(c)?);
        a.check_at = None;
        a.bump(now)?;
        r.refill(now)?;
        r.wake(now);
        save_request(&mut tx, &a).await?;
        save_resource(&mut tx, &r).await?;
        wait::notify(&mut tx, &a, now).await?;
        explorer::admission_finished(&mut tx, &a.snapshot, &receipt).await?;
        tx.commit().await?;
        Ok(receipt)
    }
}
fn check_replay(a: &Request, c: &AdmissionStartCommand) -> Result<()> {
    if let Some(source) = &a.snapshot.source
        && (source.start_id == c.start_id || source.finish_id == c.finish_id)
        && a.start_bytes.as_ref() != Some(&canonical(c)?)
    {
        return Err(ContractError::Conflict);
    }
    Ok(())
}
/// This observation never releases occupancy. A missing/settled owner becomes unknown.
pub(in crate::workflow) async fn source_check_at(
    tx: &mut PgConnection,
    s: &AdmissionSource,
    now: u64,
) -> StoreResult<Option<u64>> {
    let row=sqlx::query("SELECT a.state,a.expires_at_ms,a.authority_deadline_ms,a.deadline_ms,t.current_attempt_id,t.state AS task_state,t.cancel_requested_at_ms,EXISTS(SELECT 1 FROM accepted_settlements x WHERE x.attempt_id=a.attempt_id) AS settled FROM attempts a JOIN tasks t ON t.task_id=a.task_id WHERE a.task_id=$1 AND a.attempt_id=$2")
        .bind(&s.owner.task_id).bind(&s.owner.attempt_id).fetch_optional(&mut *tx).await?;
    let Some(row) = row else { return Ok(None) };
    let due = [
        row.try_get::<i64, _>("expires_at_ms")?,
        row.try_get::<i64, _>("authority_deadline_ms")?,
        row.try_get::<i64, _>("deadline_ms")?,
    ]
    .into_iter()
    .min()
    .ok_or_else(|| corrupt("admission source deadline"))? as u64;
    if row.try_get::<String, _>("state")? != "active"
        || row.try_get::<String, _>("task_state")? != "active"
        || row
            .try_get::<Option<String>, _>("current_attempt_id")?
            .as_deref()
            != Some(&s.owner.attempt_id)
        || row
            .try_get::<Option<i64>, _>("cancel_requested_at_ms")?
            .is_some()
        || row.try_get::<bool, _>("settled")?
        || now >= due
    {
        return Ok(None);
    }
    Ok(Some(due))
}

fn require_operation(a: &Request) -> Result<()> {
    if a.snapshot.binding.operation().is_none() {
        return Err(invalid(
            "protected provider admission requires the composed operation",
        ));
    }
    Ok(())
}
