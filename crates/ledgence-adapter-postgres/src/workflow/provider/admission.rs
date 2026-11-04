//! One transaction owns the protected grant, resource debit, and recovery record.
//! Lock order: workflow -> active tasks -> resource -> admission occurrence ->
//! sorted provider uniqueness keys -> retained effect. No path takes a resource
//! lock after a retained effect lock, including after workflow history retirement.
use super::*;
use crate::workflow::admission::{
    execution::source_check_at, state as capacity, wait as admission_wait,
};

impl ProviderAdmissionOperations for PostgresStore {
    fn start_provider_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt> {
        Box::pin(self.run(move || self.start_provider_admission_once(p, c)))
    }
    fn report_provider_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt> {
        Box::pin(self.run(move || self.report_provider_admission_once(p, c)))
    }
}
impl ProviderAdmissionStore for PostgresStore {}

impl PostgresStore {
    pub(in crate::workflow) async fn start_provider_admission_once(
        &self,
        p: &VerifiedPrincipal,
        command: &ProviderAdmissionStartCommand,
    ) -> StoreResult<ProviderAdmissionStartReceipt> {
        command.validate()?;
        self.provider_configured()?;
        self.admission_configured()?;
        let c = &command.admission;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.admission_worker(p, &c.occurrence.scope, db::now(&mut tx).await?)?;
        let name = capacity::request_resource(&mut tx, &c.occurrence).await?;
        let started: bool = sqlx::query_scalar("SELECT start_command_bytes IS NOT NULL FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND origin_activation_id=$4 AND logical_key=$5")
            .bind(&c.occurrence.scope.tenant_id).bind(&c.occurrence.scope.namespace).bind(&c.occurrence.workflow_id).bind(&c.occurrence.origin_activation_id).bind(&c.occurrence.key).fetch_one(&mut *tx).await?;
        // A replay observes authority; it cannot grant authority again, and remains
        // readable after its owner and even execution history have disappeared.
        if started {
            capacity::resource(&mut tx, &c.occurrence.scope, &name, true).await?;
            self.admission_worker(p, &c.occurrence.scope, db::now(&mut tx).await?)?;
            let a = capacity::request(&mut tx, &c.occurrence).await?;
            require_provider(&a)?;
            check_start_replay(&a, command)?;
            let receipt = existing(p, a.snapshot);
            receipt.validate_for(p, command)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        let (workflow, authority) = execution::lock_activation(&mut tx, &c.owner).await?;
        if workflow.workflow_id != c.occurrence.workflow_id {
            return Err(ContractError::Conflict.into());
        }
        let mut r = capacity::resource(&mut tx, &c.occurrence.scope, &name, true).await?;
        let now = db::now(&mut tx).await?;
        let admission_worker = self.admission_worker(p, &c.occurrence.scope, now)?;
        let worker = self.require_effect_worker(p, &c.occurrence.scope, now)?;
        let mut a = capacity::request(&mut tx, &c.occurrence).await?;
        let subject = require_provider(&a)?.clone();
        if a.start_bytes.is_some() {
            check_start_replay(&a, command)?;
            let receipt = existing(p, a.snapshot);
            receipt.validate_for(p, command)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        if self.admission_configured()?.dispatch != AdmissionDispatchPolicy::Enabled
            || self.provider_configured()?.dispatch != ProviderDispatchPolicy::Enabled
        {
            return Err(ContractError::Unavailable(
                "protected provider dispatch is quarantined".into(),
            )
            .into());
        }
        if now < r.high_water || now < a.snapshot.updated_at {
            return Err(ContractError::Unavailable(
                "resource admission clock has not reached its high-water mark".into(),
            )
            .into());
        }
        let invalidated = capacity::invalidate_offer(&mut a, &mut r, now)?;
        let eligible = a.snapshot.offer.as_ref().is_some_and(|o| {
            o.generation == c.offer_generation
                && o.activation_id == c.owner.task_id
                && o.policy_revision == r.record.revision
                && now < o.expires_at
        }) && r.record.policy.enabled
            && now < a.snapshot.deadline_at;
        if !eligible {
            if invalidated {
                capacity::save_request(&mut tx, &a).await?;
                capacity::save_resource(&mut tx, &r).await?;
            }
            let receipt = existing(p, a.snapshot);
            receipt.validate_for(p, command)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        let evidence = approved_effects::Evidence::load_provider_occurrence(
            &mut tx,
            &workflow,
            &subject.ticket.occurrence,
        )
        .await?;
        let binding = subject
            .binding
            .resolve(a.snapshot.binding.program().clone())?;
        if evidence.prior.is_some()
            || evidence.approval.provider_binding.as_ref() != Some(&binding)
            || !evidence.approval.action.matches(&subject.action)?
        {
            return Err(ContractError::Conflict.into());
        }
        let linked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM provider_admission_links WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND origin_activation_id=$6 AND logical_key=$7)")
            .bind(&c.occurrence.scope.tenant_id).bind(&c.occurrence.scope.namespace).bind(&c.occurrence.workflow_id)
            .bind(&subject.ticket.occurrence.activation_id).bind(&subject.ticket.occurrence.approval_key)
            .bind(&c.occurrence.origin_activation_id).bind(&c.occurrence.key).fetch_one(&mut *tx).await?;
        if !linked {
            return Err(corrupt("missing protected admission link").into());
        }
        let claim = ProviderClaimCommand {
            budget_fence: true,
            owner: c.owner.clone(),
            workflow_id: workflow.workflow_id.clone(),
            approval_key: subject.ticket.occurrence.approval_key.clone(),
            action: subject.action.clone(),
            binding,
            claim_id: c.start_id.clone(),
            report_id: c.finish_id.clone(),
        };
        // The helper preserves the original occurrence while the lease belongs to
        // the admission-resumed activation. The generic Claim endpoint cannot do this.
        let capacity_deadline = a
            .snapshot
            .offer
            .as_ref()
            .ok_or_else(|| corrupt("missing protected offer"))?
            .expires_at
            .min(a.snapshot.deadline_at);
        // Retain the workflow/task/resource/request locks if capacity expires.
        // All tentative provider authority, recovery and accounting are inside
        // this savepoint; an expiry returns an observation, never another grant.
        sqlx::query("SAVEPOINT admission_grant")
            .execute(&mut *tx)
            .await?;
        let provider = self
            .claim_provider_tx(
                &mut tx,
                &worker,
                &evidence,
                &claim,
                &subject.ticket.occurrence,
                now,
                Some(capacity_deadline),
                authority.has_run_budget,
                authority.valid_until(),
            )
            .await?;
        let provider = match provider {
            Ok(receipt) => receipt,
            Err(claim::ClaimRefusal::Budget(reason)) => {
                let key = crate::run_budget::denial_id(
                    &mut tx,
                    &("provider", &subject.ticket.occurrence),
                )
                .await?;
                a.start_bytes = Some(canonical(command)?);
                crate::workflow::admission::execution::close_budget_offer(
                    &mut tx, &mut a, &mut r, reason, &key,
                )
                .await?;
                let at = db::now(&mut tx).await?;
                self.admission_worker(p, &c.occurrence.scope, at)?;
                self.require_effect_worker(p, &c.occurrence.scope, at)?;
                execution::check_live(&workflow, &authority, &c.owner, at, true)?;
                let receipt = existing(p, a.snapshot);
                receipt.validate_for(p, command)?;
                tx.commit().await?;
                return Ok(receipt);
            }
            Err(claim::ClaimRefusal::Expired { observed_at }) => {
                let snapshot = self
                    .recover_expired_admission(&mut tx, p, c, &name, observed_at)
                    .await?;
                let receipt = existing(p, snapshot);
                receipt.validate_for(p, command)?;
                tx.commit().await?;
                return Ok(receipt);
            }
        };
        let now = match &provider {
            ProviderClaimReceipt::Dispatch { source, .. } => source.accepted_at,
            _ => db::now(&mut tx).await?,
        };
        // A still-valid capacity offer cannot extend the task lease or worker
        // credential. Reject these before constructing a source from fresh time.
        self.admission_worker(p, &c.occurrence.scope, now)?;
        self.require_effect_worker(p, &c.occurrence.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        if r.offered == 0 || r.started >= u64::from(r.record.policy.max_occupancy) {
            return Err(corrupt("offer occupancy authority").into());
        }
        r.offered -= 1;
        a.snapshot.offer = None;
        a.start_bytes = Some(canonical(command)?);
        let receipt = match provider {
            ProviderClaimReceipt::Dispatch {
                ticket,
                source: provider,
            } => {
                r.refill(now)?;
                if let Some(rate) = &r.record.policy.rate {
                    let bucket = core::spend_admission_token(
                        &core::AdmissionBucket {
                            credit: r.credit.ok_or_else(|| corrupt("admission rate credit"))?,
                            high_water_at: r.high_water,
                        },
                        rate,
                    )?;
                    r.credit = Some(bucket.credit);
                    r.high_water = bucket.high_water_at;
                }
                let deadline = if authority.has_run_budget {
                    crate::run_budget::lock(
                        &mut tx,
                        &c.occurrence.scope,
                        &crate::run_budget::workflow(&c.occurrence.workflow_id),
                    )
                    .await?
                    .ok_or_else(|| corrupt("missing run budget"))?
                    .policy
                    .deadline_at_ms
                } else {
                    None
                };
                let source = ProviderAdmissionSource {
                    admission: AdmissionSource {
                        occurrence: c.occurrence.clone(),
                        source_id: db::id(&mut tx, "admsrc").await?,
                        start_id: c.start_id.clone(),
                        finish_id: c.finish_id.clone(),
                        generation: c.offer_generation,
                        binding_digest: a.snapshot.binding.digest()?,
                        owner: c.owner.clone(),
                        worker: admission_worker,
                        accepted_at: now,
                        start_deadline_at: now
                            .checked_add(u64::from(r.record.policy.start_timeout_ms))
                            .ok_or_else(|| corrupt("admission start deadline"))?
                            .min(authority.valid_until())
                            .min(deadline.unwrap_or(u64::MAX)),
                    },
                    provider: *provider,
                };
                source.validate()?;
                a.snapshot.state = AdmissionState::PossiblyDispatched;
                a.snapshot.source = Some(source.admission.clone());
                a.snapshot.provider = Some(ProviderAdmissionRecord {
                    ticket,
                    source: Some(source.clone()),
                    report: None,
                });
                a.check_at = source_check_at(&mut tx, &source.admission, now).await?;
                r.started += 1;
                ProviderAdmissionStartReceipt::Granted {
                    source: Box::new(source),
                }
            }
            ProviderClaimReceipt::Existing { ticket, .. }
            | ProviderClaimReceipt::ExistingOperation { ticket, .. } => {
                // An existing business operation consumes neither a token nor a
                // dispatch slot. The unused offer is closed in this same commit.
                a.snapshot.state = AdmissionState::ClosedBeforeStart;
                a.snapshot.provider = Some(ProviderAdmissionRecord {
                    ticket,
                    source: None,
                    report: None,
                });
                a.check_at = None;
                a.bump(now)?;
                existing(p, a.snapshot.clone())
            }
        };
        if matches!(receipt, ProviderAdmissionStartReceipt::Granted { .. }) {
            a.bump(now)?;
        }
        r.wake(now);
        capacity::save_request(&mut tx, &a).await?;
        capacity::save_resource(&mut tx, &r).await?;
        admission_wait::notify(&mut tx, &a, now).await?;
        let final_now = db::now(&mut tx).await?;
        if final_now < now {
            return Err(ContractError::Unavailable(
                "provider admission clock moved backwards".into(),
            )
            .into());
        }
        if final_now >= capacity_deadline {
            let snapshot = self
                .recover_expired_admission(&mut tx, p, c, &name, final_now)
                .await?;
            let receipt = existing(p, snapshot);
            receipt.validate_for(p, command)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        self.admission_worker(p, &c.occurrence.scope, final_now)?;
        self.require_effect_worker(p, &c.occurrence.scope, final_now)?;
        execution::check_live(&workflow, &authority, &c.owner, final_now, true)?;
        if let ProviderAdmissionStartReceipt::Granted { source } = &receipt
            && final_now >= source.admission.start_deadline_at
        {
            return Err(ContractError::OwnershipLost.into());
        }
        receipt.validate_for(p, command)?;
        tx.commit().await?;
        Ok(receipt)
    }

    async fn report_provider_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &ProviderAdmissionReportCommand,
    ) -> StoreResult<ProviderAdmissionReportReceipt> {
        c.validate()?;
        self.provider_configured()?;
        self.admission_configured()?;
        let o = &c.source.admission.occurrence;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.admission_worker(p, &o.scope, db::now(&mut tx).await?)?;
        if !c.source.admission.worker.matches_principal(p) {
            return Err(ContractError::Forbidden.into());
        }
        capacity::lock_retained_workflow(&mut tx, o).await?;
        let run = lock_live(&mut tx, &c.source.provider.ticket.occurrence).await?;
        let name = capacity::request_resource(&mut tx, o).await?;
        let mut r = capacity::resource(&mut tx, &o.scope, &name, true).await?;
        let mut a = capacity::request(&mut tx, o).await?;
        require_provider(&a)?;
        if a.snapshot.provider.as_ref().and_then(|v| v.source.as_ref()) != Some(&c.source) {
            return Err(ContractError::Conflict.into());
        }
        if let Some(bytes) = &a.finish_bytes {
            if bytes != &canonical(c)? {
                return Err(ContractError::Conflict.into());
            }
            let mut receipt = a
                .snapshot
                .provider
                .and_then(|v| v.report)
                .ok_or_else(|| corrupt("missing provider admission report"))?;
            self.admission_worker(p, &o.scope, db::now(&mut tx).await?)?;
            receipt.receipt.already_accepted = true;
            receipt.validate_for(p, c)?;
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
        // No live-owner authority is required for a late, immutable report from
        // the original source. Business evidence and occupancy settle together.
        let reply = self
            .provider_report_tx(&mut tx, p, c, None, run.as_ref(), true)
            .await?;
        let receipt = ProviderAdmissionReportReceipt {
            source: c.source.clone(),
            call_completion: c.outcome.call_completion,
            receipt: reply.receipt().clone(),
        };
        receipt.validate_for(p, c)?;
        if a.snapshot.state != AdmissionState::AdministrativelyReleased {
            match c.outcome.call_completion {
                ProviderCallCompletion::Completed => {
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
                ProviderCallCompletion::Unknown => {
                    if a.snapshot.state != AdmissionState::UnknownOccupied {
                        r.unknown += 1;
                    }
                    a.snapshot.state = AdmissionState::UnknownOccupied;
                }
            }
        }
        a.snapshot
            .provider
            .as_mut()
            .ok_or_else(|| corrupt("missing provider admission record"))?
            .report = Some(receipt.clone());
        a.finish_bytes = Some(canonical(c)?);
        a.check_at = None;
        let now = db::now(&mut tx)
            .await?
            .max(receipt.receipt.accepted_at)
            .max(a.snapshot.updated_at);
        self.admission_worker(p, &o.scope, now)?;
        a.bump(now)?;
        r.refill(now)?;
        r.wake(now);
        capacity::save_request(&mut tx, &a).await?;
        capacity::save_resource(&mut tx, &r).await?;
        admission_wait::notify(&mut tx, &a, now).await?;
        tx.commit().await?;
        Ok(receipt)
    }
}
fn require_provider(a: &capacity::Request) -> Result<&ProviderAdmissionSubject> {
    a.snapshot
        .binding
        .provider()
        .ok_or_else(|| invalid("operation admission requires generic Start/Finish"))
}
fn existing(p: &VerifiedPrincipal, snapshot: AdmissionSnapshot) -> ProviderAdmissionStartReceipt {
    ProviderAdmissionStartReceipt::Existing {
        audience: p.audience().clone(),
        snapshot: Box::new(snapshot),
    }
}
fn check_start_replay(a: &capacity::Request, c: &ProviderAdmissionStartCommand) -> Result<()> {
    if let Some(bytes) = &a.start_bytes {
        let old: ProviderAdmissionStartCommand = codec::decode(bytes)?;
        if (old.admission.start_id == c.admission.start_id
            || old.admission.finish_id == c.admission.finish_id)
            && bytes != &canonical(c)?
        {
            return Err(ContractError::Conflict);
        }
    }
    Ok(())
}
