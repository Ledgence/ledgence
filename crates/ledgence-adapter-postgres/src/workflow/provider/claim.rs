use super::*;

/// Capacity expiry is recoverable by parking the same admission occurrence.
/// It is not a worker ownership error and must never trigger a new grant here.
pub(super) enum ClaimRefusal {
    Expired { observed_at: u64 },
    Budget(RunBudgetDenial),
}
impl PostgresStore {
    pub(super) async fn claim_provider_once(
        &self,
        p: &VerifiedPrincipal,
        c: &ProviderClaimCommand,
    ) -> StoreResult<ProviderClaimReceipt> {
        c.validate()?;
        if c.binding.admission.is_some() {
            return Err(invalid("governed provider dispatch requires atomic admission").into());
        }
        self.provider_configured()?;
        let o = c.occurrence();
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let (workflow, authority) = execution::lock_activation(&mut tx, &c.owner).await?;
        let now = db::now(&mut tx).await?;
        let worker = self.require_effect_worker(p, &o.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        let evidence = approved_effects::Evidence::load_provider(
            &mut tx,
            &workflow,
            &c.owner,
            &c.workflow_id,
            &c.approval_key,
        )
        .await?;
        if evidence.prior.is_some()
            || canonical(
                &evidence
                    .approval
                    .provider_binding
                    .as_ref()
                    .ok_or(ContractError::Conflict)?,
            )? != canonical(&c.binding)?
            || !evidence.approval.action.matches(&c.action)?
        {
            return Err(ContractError::Conflict.into());
        }
        let receipt = match self
            .claim_provider_tx(
                &mut tx,
                &worker,
                &evidence,
                c,
                &o,
                now,
                None,
                authority.has_run_budget,
                authority.valid_until(),
            )
            .await?
        {
            Ok(receipt) => receipt,
            Err(ClaimRefusal::Expired { .. }) => {
                return Err(corrupt("ungoverned provider claim expired admission").into());
            }
            Err(ClaimRefusal::Budget(reason)) => {
                let key = crate::run_budget::denial_id(&mut tx, &("provider", &o)).await?;
                let reason = crate::run_budget::bound_call_refusal(
                    &mut tx,
                    &o.scope,
                    &crate::run_budget::workflow(&o.workflow_id),
                    &key,
                    reason,
                    c,
                    &o,
                )
                .await?;
                let at = db::now(&mut tx).await?;
                self.require_effect_worker(p, &o.scope, at)?;
                execution::check_live(&workflow, &authority, &c.owner, at, true)?;
                tx.commit().await?;
                return Err(ContractError::RunBudgetDenied(reason).into());
            }
        };
        let now = db::now(&mut tx).await?;
        self.require_effect_worker(p, &o.scope, now)?;
        execution::check_live(&workflow, &authority, &c.owner, now, true)?;
        if let ProviderClaimReceipt::Dispatch { source, .. } = &receipt {
            crate::run_budget::check_grant(source.budget_grant, now)?;
        }
        receipt.validate_for(p, c)?;
        tx.commit().await?;
        Ok(receipt)
    }
    /// Caller holds workflow/task and, for composed admission, the resource lock.
    /// The original occurrence comes from verified approval, never a replacement owner.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn claim_provider_tx(
        &self,
        tx: &mut PgConnection,
        worker: &EffectWorkerIdentity,
        evidence: &approved_effects::Evidence,
        c: &ProviderClaimCommand,
        o: &EffectOccurrence,
        now: u64,
        admission_deadline: Option<u64>,
        has_run_budget: bool,
        lease_until: u64,
    ) -> StoreResult<std::result::Result<ProviderClaimReceipt, ClaimRefusal>> {
        // Sorted advisory keys serialize only competing provider/business identities.
        // Unique indexes remain the authoritative guard; full bytes detect collisions.
        let business = canonical(&(
            worker.audience.instance_id.as_str(),
            &o.scope,
            &c.binding.business,
        ))?;
        let domain = c
            .binding
            .deduplication
            .as_ref()
            .map(|d| canonical(&d.domain))
            .transpose()?;
        let domain_digest: Option<Vec<u8>> = if let Some(bytes) = &domain {
            Some(
                sqlx::query_scalar("SELECT sha256($1::bytea)")
                    .bind(bytes)
                    .fetch_one(&mut *tx)
                    .await?,
            )
        } else {
            None
        };
        let mut locks = vec![business];
        if let Some(d) = &c.binding.deduplication {
            locks.push(canonical(&(
                worker.audience.instance_id.as_str(),
                &d.domain,
                &d.key,
            ))?);
        }
        locks.sort();
        locks.dedup();
        for key in locks {
            sqlx::query(
                "SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea,'hex'),1))",
            )
            .bind(key)
            .execute(&mut *tx)
            .await?;
        }
        // Only composed admission has expiring resource offers. Advisory locks
        // may have waited: never backdate a new grant using the pre-lock clock.
        // Ungoverned provider calls retain their original SQL path.
        let now = if let Some(deadline) = admission_deadline {
            let fresh = db::now(tx).await?;
            if fresh < now {
                return Err(ContractError::Unavailable(
                    "provider admission clock moved backwards".into(),
                )
                .into());
            }
            if fresh >= deadline {
                return Ok(Err(ClaimRefusal::Expired { observed_at: fresh }));
            }
            fresh
        } else {
            now
        };
        let rows=sqlx::query("SELECT tenant_id,namespace,workflow_id,activation_id,approval_key,binding_digest,provider_domain_bytes FROM provider_operations WHERE (tenant_id=$1 AND namespace=$2 AND business_namespace=$3 AND business_key=$4) OR (instance_id=$5 AND provider_domain_digest=$6 AND provider_key=$7)")
          .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&c.binding.business.namespace).bind(&c.binding.business.key).bind(&worker.audience.instance_id).bind(&domain_digest).bind(c.binding.deduplication.as_ref().map(|d|&d.key)).fetch_all(&mut *tx).await?;
        if rows.len() > 1 {
            return Err(ContractError::Conflict.into());
        }
        let receipt = if let Some(row) = rows.first() {
            let existing = EffectOccurrence {
                scope: Scope {
                    tenant_id: row.try_get("tenant_id")?,
                    namespace: row.try_get("namespace")?,
                },
                workflow_id: row.try_get("workflow_id")?,
                activation_id: row.try_get("activation_id")?,
                approval_key: row.try_get("approval_key")?,
            };
            if existing.scope != o.scope {
                return Err(ContractError::Conflict.into());
            }
            let saved = retained::load(tx, &existing)
                .await?
                .ok_or_else(|| corrupt("provider uniqueness target"))?;
            if canonical(binding(&saved)?)? != canonical(&c.binding)?
                || !saved.binding.approval.action.matches(&c.action)?
                || row.try_get::<String, _>("binding_digest")?
                    != c.binding.digest(&c.action)?.0.as_str()
                || row.try_get::<Option<Vec<u8>>, _>("provider_domain_bytes")? != domain
            {
                return Err(ContractError::Conflict.into());
            }
            // Reusing a recorded command ID cannot hide a changed request/actor.
            if let Some(old) =
                retained::entry(tx, &existing, "provider_dispatch", &c.claim_id).await?
            {
                let prior: ProviderClaimCommand = codec::decode(
                    old.command
                        .as_deref()
                        .ok_or_else(|| corrupt("dispatch command"))?,
                )?;
                if canonical(&prior)? != canonical(c)? {
                    return Err(ContractError::Conflict.into());
                }
                let EffectEvidenceEntry::ProviderDispatch {
                    worker: original, ..
                } = old.entry
                else {
                    return Err(corrupt("dispatch entry").into());
                };
                if !original.same_worker(worker) {
                    return Err(ContractError::Conflict.into());
                }
            }
            if existing == *o {
                ProviderClaimReceipt::Existing {
                    audience: worker.audience.clone(),
                    ticket: ticket(&saved)?,
                }
            } else {
                ProviderClaimReceipt::ExistingOperation {
                    audience: worker.audience.clone(),
                    ticket: ticket(&saved)?,
                }
            }
        } else {
            self.check_dispatch_quarantine()?;
            if self.provider_configured()?.dispatch != ProviderDispatchPolicy::Enabled {
                return Err(ContractError::Forbidden.into());
            }
            if evidence.claimed {
                return Err(corrupt("provider grant without retained operation").into());
            }
            let source_id = db::id(tx, "psrc").await?;
            let (now, budget_grant) = if has_run_budget {
                self.admission_configured()?;
                if !c.budget_fence {
                    return Err(ContractError::InvalidInput(
                        "worker must support run budget dispatch fences".into(),
                    )
                    .into());
                }
                match crate::run_budget::call(
                    tx,
                    &o.scope,
                    &crate::run_budget::workflow(&o.workflow_id),
                    &source_id,
                    Some(o),
                )
                .await?
                {
                    Err(reason) => return Ok(Err(ClaimRefusal::Budget(reason))),
                    Ok((at, deadline)) => {
                        if admission_deadline.is_some_and(|end| at >= end) {
                            return Ok(Err(ClaimRefusal::Expired { observed_at: at }));
                        }
                        let bound = deadline.unwrap_or(u64::MAX).min(lease_until);
                        let grant = if admission_deadline.is_some() {
                            None
                        } else {
                            Some(RunBudgetGrant::new(at, bound)?)
                        };
                        (at, grant)
                    }
                }
            } else {
                (now, None)
            };
            approved_effects::grant_bound(
                tx,
                &c.workflow_id,
                &c.approval_key,
                &c.owner.attempt_id,
                None,
                evidence,
            )
            .await?;
            let mut r =
                retained::ensure(tx, &c.workflow_id, &c.approval_key, now, Some(&c.claim_id))
                    .await?;
            let first_due = now
                .checked_add(c.binding.reconciliation.initial_delay_ms)
                .ok_or_else(|| invalid("provider deadline overflow"))?;
            let deadline = now
                .checked_add(c.binding.reconciliation.max_elapsed_ms)
                .ok_or_else(|| invalid("provider deadline overflow"))?;
            let s = ProviderEffectSummary {
                binding_digest: c.binding.digest(&c.action)?,
                knowledge: ProviderKnowledgeKind::Unknown,
                selected_report: None,
                conflicting_reports: vec![],
                handling: ProviderHandlingState::Unresolved,
                candidate_revision: None,
                reconciliation: ProviderReconciliationSummary {
                    queue: c.binding.reconciliation.queue.clone(),
                    checks_used: 0,
                    max_checks: c.binding.reconciliation.max_checks,
                    generation: 0,
                    active_probe_id: None,
                    active_task_id: None,
                    first_due_at: first_due,
                    next_due_at: Some(first_due),
                    deadline_at: deadline,
                    stopped_reason: None,
                },
            };
            r.summary.provider = Some(s.clone());
            let valid:Option<bool>=sqlx::query_scalar("INSERT INTO provider_operations(tenant_id,namespace,workflow_id,activation_id,approval_key,instance_id,binding_digest,business_namespace,business_key,provider_domain_digest,provider_domain_bytes,provider_key,first_due_at_ms,deadline_at_ms,next_due_at_ms,knowledge,handling,state_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$13,'unknown','unresolved',$15) RETURNING tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND instance_id=$6 AND binding_digest=$7 AND business_namespace=$8 AND business_key=$9 AND provider_domain_digest IS NOT DISTINCT FROM $10 AND provider_domain_bytes IS NOT DISTINCT FROM $11 AND provider_key IS NOT DISTINCT FROM $12 AND first_due_at_ms=$13 AND deadline_at_ms=$14 AND next_due_at_ms=$13 AND state_bytes=$15 AND checks_used=0 AND generation=0 AND knowledge='unknown' AND handling='unresolved' AND active_task_id IS NULL AND candidate_bytes IS NULL")
              .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(&worker.audience.instance_id).bind(s.binding_digest.0.as_str()).bind(&c.binding.business.namespace).bind(&c.binding.business.key).bind(&domain_digest).bind(&domain).bind(c.binding.deduplication.as_ref().map(|d|&d.key)).bind(codec::ms(first_due)?).bind(codec::ms(deadline)?).bind(codec::encode(&s)?).fetch_optional(&mut *tx).await?;
            proof(valid, "provider uniqueness insert changed or suppressed")?;

            let source = ProviderDispatchSource {
                budget_grant,
                source_id: source_id.clone(),
                claim_id: c.claim_id.clone(),
                report_id: c.report_id.clone(),
                ticket: ticket(&r)?,
                worker: worker.clone(),
                accepted_at: now,
                first_possible_dispatch_at: now,
                reconciliation_deadline: deadline,
            };
            let saved = Source {
                source_id: source_id.clone(),
                report_id: c.report_id.clone(),
                ticket: source.ticket.clone(),
                worker: worker.clone(),
                source: ProviderSourceReference::Dispatch {
                    claim_id: c.claim_id.clone(),
                },
                owner: c.owner.clone(),
                context: None,
            };
            insert_source(tx, &saved, c).await?;
            let (sequence, revision) = r.next(now)?;
            let entry = EffectEvidenceEntry::ProviderDispatch {
                sequence,
                recorded_revision: revision,
                recorded_at: now,
                claim_id: c.claim_id.clone(),
                source_id,
                report_id: c.report_id.clone(),
                worker: worker.clone(),
            };
            retained::append(
                tx,
                &r,
                &entry,
                "provider",
                None,
                Some(canonical(c)?),
                Some(codec::encode(&source)?),
            )
            .await?;
            let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_runs SET has_provider_operations=true WHERE workflow_id=$1 RETURNING workflow_id=$1 AND has_provider_operations").bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
            proof(valid, "provider workflow marker")?;
            ProviderClaimReceipt::Dispatch {
                ticket: ticket(&r)?,
                source: Box::new(source),
            }
        };
        Ok(Ok(receipt))
    }
}
