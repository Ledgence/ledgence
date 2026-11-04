use super::*;
use crate::workflow::admission::policy;
impl PostgresStore {
    pub(super) async fn open_task_admission_session_once(
        &self,
        p: &VerifiedPrincipal,
        c: &TaskAdmissionSessionCommand,
    ) -> StoreResult<WorkerSession> {
        c.validate()?;
        self.admission_configured()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let now = db::now(&mut tx).await?;
        let worker = self.admission_worker(p, &c.scope, now)?;
        let mut session = core::open_session(
            &db::id(&mut tx, "ws").await?,
            c.scope.clone(),
            &c.queue,
            c.concurrency,
            now,
        )?;
        session.features = vec![TASK_ADMISSION_FEATURE.into()];
        sqlx::query("INSERT INTO worker_sessions(session_id,tenant_id,namespace,queue,concurrency,expires_at_ms,features_bytes,admission_worker_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(&session.id).bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(&c.queue).bind(i64::from(c.concurrency)).bind(codec::ms(session.expires_at)?).bind(codec::encode(&session.features)?).bind(codec::encode(&worker)?).execute(&mut *tx).await?;
        self.admission_worker(p, &c.scope, db::now(&mut tx).await?)?;
        tx.commit().await?;
        Ok(session)
    }
    pub(super) async fn start_task_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &TaskAdmissionStartCommand,
    ) -> StoreResult<TaskAdmissionStartReceipt> {
        c.validate()?;
        self.admission_configured()?;
        let o = TaskAdmissionOccurrence::from(&c.owner);
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.admission_worker(p, &o.scope, db::now(&mut tx).await?)?;
        // A replay can observe retained evidence after the session/task is gone.
        let prior:bool=sqlx::query_scalar("SELECT start_command_bytes IS NOT NULL OR close_command_bytes IS NOT NULL FROM task_admission_requests WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND generation=$4")
            .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).bind(i64::from(o.generation)).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        let session = if prior {
            None
        } else {
            Some(
                self.lock_session(&mut tx, &c.owner.worker_session_id)
                    .await?,
            )
        };
        let task = lock_task(&mut tx, &o).await?;
        let name = resource_name(&mut tx, &o).await?;
        let mut r = capacity::resource(&mut tx, &o.scope, &name, true).await?;
        let mut a = request(&mut tx, &o).await?;
        let now = db::now(&mut tx).await?;
        self.admission_worker(p, &o.scope, now)?;
        check_actor(&a, p, &c.owner, c.offer_generation)?;
        if let Some(bytes) = &a.start_bytes {
            if bytes != &canonical(c)? {
                return Err(ContractError::Conflict.into());
            }
            let receipt = TaskAdmissionStartReceipt::Existing {
                snapshot: Box::new(a.snapshot),
            };
            receipt.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        if a.snapshot
            .close
            .as_ref()
            .is_some_and(|close| close.start_id != c.start_id)
        {
            return Err(ContractError::Conflict.into());
        }
        if a.snapshot.state != TaskAdmissionState::Preparing || a.close_bytes.is_some() {
            let receipt = TaskAdmissionStartReceipt::Existing {
                snapshot: Box::new(a.snapshot),
            };
            receipt.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        if self.admission_configured()?.dispatch != AdmissionDispatchPolicy::Enabled {
            return Err(ContractError::Unavailable(
                "task admission dispatch is quarantined".into(),
            )
            .into());
        }
        if now < r.high_water || now < a.snapshot.updated_at {
            return Err(ContractError::Unavailable(
                "task admission clock has not reached its high-water mark".into(),
            )
            .into());
        }
        let offer = a
            .snapshot
            .offer
            .as_ref()
            .ok_or_else(|| corrupt("preparation offer"))?;
        if offer.policy_revision != r.record.revision
            || !r.record.policy.enabled
            || now >= offer.expires_at
        {
            release_offer(&mut a, &mut r, now)?;
            save(&mut tx, &a).await?;
            capacity::save_resource(&mut tx, &r).await?;
            let receipt = TaskAdmissionStartReceipt::Existing {
                snapshot: Box::new(a.snapshot),
            };
            receipt.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        let task = task.ok_or(ContractError::OwnershipLost)?;
        let session = session.ok_or(ContractError::OwnershipLost)?;
        if !session.features.iter().any(|v| v == TASK_ADMISSION_FEATURE)
            || now >= session.expires_at
        {
            return Err(ContractError::OwnershipLost.into());
        }
        let attempt = db::load_attempt(&mut tx, &task, &c.owner.attempt_id).await?;
        let member = crate::run_budget::task(&o.task_id);
        let source_id = db::id(&mut tx, "tadmsrc").await?;
        let budget_deadline = if task.input.budget.is_some() {
            match crate::run_budget::call(&mut tx, &o.scope, &member, &source_id, None).await? {
                Ok((_, deadline)) => deadline,
                Err(reason) => {
                    let key =
                        crate::run_budget::denial_id(&mut tx, &("task", &o, c.offer_generation))
                            .await?;
                    crate::run_budget::record_denial(
                        &mut tx,
                        &o.scope,
                        &member,
                        RunBudgetAction::Call,
                        &key,
                        reason,
                        None,
                    )
                    .await?;
                    let now = db::now(&mut tx).await?;
                    self.admission_worker(p, &o.scope, now)?;
                    let transition =
                        core::deny_task_admission(&task, &attempt, &c.owner, reason, now)?;
                    release_offer(&mut a, &mut r, now)?;
                    a.snapshot.budget_denial = Some(reason);
                    a.start_bytes = Some(canonical(c)?);
                    a.check_at = None;
                    a.bump(now)?;
                    r.wake(now);
                    save(&mut tx, &a).await?;
                    capacity::save_resource(&mut tx, &r).await?;
                    db::apply(&mut tx, &transition).await?;
                    let receipt = TaskAdmissionStartReceipt::Existing {
                        snapshot: Box::new(a.snapshot),
                    };
                    receipt.validate_for(p, c)?;
                    tx.commit().await?;
                    return Ok(receipt);
                }
            }
        } else {
            None
        };
        let now = if task.input.budget.is_some() {
            db::now(&mut tx).await?
        } else {
            now
        };
        self.admission_worker(p, &o.scope, now)?;
        if now >= offer.expires_at || now >= session.expires_at {
            return Err(ContractError::OwnershipLost.into());
        }
        let transition = core::start_task_admission(&task, &attempt, &c.owner, now)?;
        r.refill(now)?;
        if r.offered == 0 {
            return Err(corrupt("missing preparation occupancy").into());
        }
        if let Some(rate) = &r.record.policy.rate {
            let bucket = core::spend_admission_token(
                &core::AdmissionBucket {
                    credit: r.credit.ok_or_else(|| corrupt("rate credit"))?,
                    high_water_at: r.high_water,
                },
                rate,
            )?;
            r.credit = Some(bucket.credit);
            r.high_water = bucket.high_water_at;
        }
        let source = TaskAdmissionSource {
            owner: c.owner.clone(),
            offer_generation: c.offer_generation,
            resource: name,
            binding_digest: a.snapshot.binding().digest()?,
            start_id: c.start_id.clone(),
            source_id,
            worker: a
                .worker
                .clone()
                .ok_or_else(|| corrupt("preparation worker"))?,
            accepted_at: now,
            start_deadline_at: now
                .checked_add(u64::from(r.record.policy.start_timeout_ms))
                .ok_or_else(|| corrupt("start time overflow"))?
                .min(attempt.deadline)
                .min(attempt.lease.expires_at)
                .min(attempt.authority_deadline)
                .min(budget_deadline.unwrap_or(u64::MAX)),
        };
        source.validate()?;
        r.offered -= 1;
        r.started = r
            .started
            .checked_add(1)
            .ok_or_else(|| corrupt("started count overflow"))?;
        a.snapshot.source = Some(source.clone());
        a.snapshot.state = TaskAdmissionState::PossiblyDispatched;
        a.check_at = Some(attempt.lease.expires_at.min(attempt.authority_deadline));
        a.start_bytes = Some(canonical(c)?);
        a.bump(now)?;
        r.wake(now);
        db::apply(&mut tx, &transition).await?;
        save(&mut tx, &a).await?;
        capacity::save_resource(&mut tx, &r).await?;
        // Sample fresh time and prove the invocation fence in one statement.
        // Even a trigger that rewrites or suppresses a transition cannot issue
        // a grant with an unmarked attempt.
        let (at,marked):(i64,bool)=sqlx::query_as("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint, EXISTS(SELECT 1 FROM attempts a JOIN tasks t ON t.task_id=a.task_id WHERE a.attempt_id=$1 AND a.task_id=$2 AND a.execution_may_have_started AND a.state='active' AND t.state='active' AND t.current_attempt_id=a.attempt_id AND t.cancel_requested_at_ms IS NULL)").bind(&c.owner.attempt_id).bind(&c.owner.task_id).fetch_one(&mut *tx).await?;
        proof(Some(marked), "task invocation fence")?;
        let final_now = codec::timestamp(at)?;
        self.admission_worker(p, &o.scope, final_now)?;
        if final_now < now
            || final_now >= source.start_deadline_at
            || final_now >= session.expires_at
        {
            return Err(ContractError::OwnershipLost.into());
        }
        let mut authority = transition.reply;
        authority.remaining_ms = authority.expires_at.saturating_sub(final_now);
        authority.execution_remaining_ms = attempt.deadline.saturating_sub(final_now);
        let receipt = TaskAdmissionStartReceipt::Granted {
            source: Box::new(source),
            authority,
        };
        receipt.validate_for(p, c)?;
        tx.commit().await?;
        Ok(receipt)
    }
    pub(super) async fn close_task_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &TaskAdmissionCloseCommand,
    ) -> StoreResult<TaskAdmissionCloseReceipt> {
        c.validate()?;
        self.admission_configured()?;
        let o = TaskAdmissionOccurrence::from(&c.owner);
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.admission_worker(p, &o.scope, db::now(&mut tx).await?)?;
        let task = lock_task(&mut tx, &o).await?;
        let name = resource_name(&mut tx, &o).await?;
        let mut r = capacity::resource(&mut tx, &o.scope, &name, true).await?;
        let mut a = request(&mut tx, &o).await?;
        let sampled = db::now(&mut tx).await?;
        self.admission_worker(p, &o.scope, sampled)?;
        // Cleanup is evidence, not new permission. Keep its timestamp monotone
        // even after clock rollback; authenticate against the actual DB clock.
        let now = sampled.max(a.snapshot.updated_at).max(r.high_water);
        check_actor(&a, p, &c.owner, c.offer_generation)?;
        if let Some(bytes) = &a.close_bytes {
            if bytes != &canonical(c)? {
                return Err(ContractError::Conflict.into());
            }
            let mut receipt = a.snapshot.close.ok_or_else(|| corrupt("close replay"))?;
            receipt.already_accepted = true;
            receipt.validate_for(p, c)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        if a.snapshot
            .source
            .as_ref()
            .is_some_and(|s| s.start_id != c.start_id)
        {
            return Err(ContractError::Conflict.into());
        }
        let mut transition = None;
        let mut disposition = TaskAdmissionCloseDisposition::Closed;
        if a.snapshot.source.is_some() {
            let released = release_started(&mut a, &mut r, now)?;
            if released {
                a.snapshot.state = TaskAdmissionState::Finished;
            }
            disposition = if released {
                TaskAdmissionCloseDisposition::Released
            } else {
                TaskAdmissionCloseDisposition::AlreadyReleased
            };
            if c.outcome == TaskAdmissionCloseOutcome::NotInvoked {
                if let Some(task) = &task {
                    let attempt = db::load_attempt(&mut tx, task, &c.owner.attempt_id).await?;
                    transition = Some(core::stop_started_task_admission(
                        task, &attempt, &c.owner, now,
                    )?);
                }
                if released {
                    disposition = TaskAdmissionCloseDisposition::ChargedNotInvoked;
                }
            }
        } else {
            release_offer(&mut a, &mut r, now)?;
            a.snapshot.state = TaskAdmissionState::ClosedBeforeStart;
            a.check_at = None;
            if a.snapshot.budget_denial.is_some() {
                let original: TaskAdmissionStartCommand = codec::decode(
                    a.start_bytes
                        .as_deref()
                        .ok_or_else(|| corrupt("missing budget denial command"))?,
                )?;
                if original.start_id != c.start_id {
                    return Err(ContractError::Conflict.into());
                }
                if let Some(task) = &task {
                    let attempt = db::load_attempt(&mut tx, task, &c.owner.attempt_id).await?;
                    transition = Some(core::confirm_denied_task_quiescence(
                        task, &attempt, &c.owner, now,
                    )?);
                }
            } else if c.outcome == TaskAdmissionCloseOutcome::NotInvoked
                && let Some(task) = &task
            {
                let attempt = db::load_attempt(&mut tx, task, &c.owner.attempt_id).await?;
                if task.state == TaskState::Active
                    && task.current_attempt_id.as_deref() == Some(&c.owner.attempt_id)
                    && attempt.settlement.is_none()
                {
                    let deferred = core::defer_task_admission(
                        task,
                        &attempt,
                        &c.owner,
                        Quiescence::Confirmed,
                        now,
                    )?;
                    if deferred
                        .attempt
                        .as_ref()
                        .is_some_and(|v| v.state == AttemptState::Deferred)
                    {
                        disposition = TaskAdmissionCloseDisposition::Deferred;
                    }
                    transition = Some(deferred);
                }
            } else if c.outcome == TaskAdmissionCloseOutcome::Quiescent
                && let Some(task) = &task
                && task.state == TaskState::Active
                && task.current_attempt_id.as_deref() == Some(&c.owner.attempt_id)
            {
                // A real preparation failure may have already settled while
                // cleanup was outstanding. Its accepted bytes remain immutable.
                let attempt = db::load_attempt(&mut tx, task, &c.owner.attempt_id).await?;
                if attempt.settlement.is_some()
                    && attempt.quiescence == Quiescence::Unconfirmed
                    && sampled < attempt.lease.expires_at.min(attempt.authority_deadline)
                {
                    transition = Some(core::confirm_quiescence(task, &attempt, &c.owner, sampled)?);
                }
            }
        }
        let receipt = TaskAdmissionCloseReceipt {
            owner: c.owner.clone(),
            offer_generation: c.offer_generation,
            start_id: c.start_id.clone(),
            operation_id: c.operation_id.clone(),
            outcome: c.outcome,
            source: a.snapshot.source.clone(),
            disposition,
            accepted_at: now,
            already_accepted: false,
        };
        receipt.validate_for(p, c)?;
        a.close_bytes = Some(canonical(c)?);
        a.snapshot.close = Some(receipt.clone());
        a.bump(now)?;
        r.wake(now);
        // Persist the tombstone before applying a requeue in the same transaction.
        save(&mut tx, &a).await?;
        capacity::save_resource(&mut tx, &r).await?;
        if let Some(transition) = &transition {
            db::apply(&mut tx, transition).await?;
        }
        self.admission_worker(p, &o.scope, db::now(&mut tx).await?)?;
        tx.commit().await?;
        if let Some(transition) = &transition {
            self.wake_queued_transition(transition);
        }
        Ok(receipt)
    }
    pub(super) async fn inspect_task_admission_once(
        &self,
        p: &VerifiedPrincipal,
        q: &TaskAdmissionQuery,
    ) -> StoreResult<TaskAdmissionSnapshot> {
        q.validate()?;
        self.admission_configured()?;
        self.require_scope(&q.occurrence.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        self.require_operator(p, AccessCapability::SensitiveRead, db::now(&mut tx).await?)?;
        let o = &q.occurrence;
        let row=sqlx::query("SELECT * FROM task_admission_requests WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND generation=$4")
            .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).bind(i64::from(o.generation)).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        let snapshot = from_row(&row)?.snapshot;
        self.require_operator(p, AccessCapability::SensitiveRead, db::now(&mut tx).await?)?;
        tx.commit().await?;
        Ok(snapshot)
    }
    pub(super) async fn release_task_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &TaskAdmissionReleaseCommand,
    ) -> StoreResult<TaskAdmissionReleaseReceipt> {
        c.validate()?;
        self.admission_configured()?;
        self.require_scope(&c.occurrence.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.require_operator(
            p,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        lock_task(&mut tx, &c.occurrence).await?;
        let name = resource_name(&mut tx, &c.occurrence).await?;
        let mut r = capacity::resource(&mut tx, &c.occurrence.scope, &name, true).await?;
        policy::lock_receipt(&mut tx, &c.occurrence.scope, &c.request_id).await?;
        if let Some(mut receipt) = policy::receipt::<TaskAdmissionReleaseReceipt, _>(
            &mut tx,
            p,
            &c.occurrence.scope,
            &c.request_id,
            "task_release",
            c,
        )
        .await?
        {
            receipt.already_accepted = true;
            self.require_operator(
                p,
                AccessCapability::ResourceControl,
                db::now(&mut tx).await?,
            )?;
            tx.commit().await?;
            return Ok(receipt);
        }
        let mut a = request(&mut tx, &c.occurrence).await?;
        if a.snapshot.revision != c.expected_revision
            || a.snapshot.source.as_ref().map(|s| &s.source_id) != Some(&c.source_id)
            || !matches!(
                a.snapshot.state,
                TaskAdmissionState::PossiblyDispatched | TaskAdmissionState::UnknownOccupied
            )
        {
            return Err(ContractError::Conflict.into());
        }
        let sampled = db::now(&mut tx).await?;
        self.require_operator(p, AccessCapability::ResourceControl, sampled)?;
        let now = sampled.max(a.snapshot.updated_at).max(r.high_water);
        release_started(&mut a, &mut r, now)?;
        a.snapshot.state = TaskAdmissionState::AdministrativelyReleased;
        a.bump(now)?;
        save(&mut tx, &a).await?;
        capacity::save_resource(&mut tx, &r).await?;
        let receipt = TaskAdmissionReleaseReceipt {
            snapshot: a.snapshot,
            already_accepted: false,
        };
        receipt.validate()?;
        policy::record_receipt(
            &mut tx,
            p,
            &c.occurrence.scope,
            &c.request_id,
            "task_release",
            c,
            &receipt,
            AuditOperation::TaskAdmissionReleased {
                occurrence: c.occurrence.clone(),
                source_id: c.source_id.clone(),
                request_id: c.request_id.clone(),
                revision: receipt.snapshot.revision,
                reason: c.reason.clone(),
            },
            now,
        )
        .await?;
        self.require_operator(
            p,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        Ok(receipt)
    }
}
pub(super) async fn lock_task(
    tx: &mut PgConnection,
    o: &TaskAdmissionOccurrence,
) -> StoreResult<Option<TaskSnapshot>> {
    let row = sqlx::query(
        "SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 FOR NO KEY UPDATE",
    )
    .bind(&o.scope.tenant_id)
    .bind(&o.scope.namespace)
    .bind(&o.task_id)
    .fetch_optional(tx)
    .await?;
    match row {
        Some(row) if row.try_get::<Option<i64>, _>("retiring_at_ms")?.is_none() => {
            Ok(Some(codec::task(&row)?))
        }
        _ => Ok(None),
    }
}
pub(super) fn check_actor(
    a: &Request,
    p: &VerifiedPrincipal,
    owner: &LeaseOwner,
    generation: u64,
) -> StoreResult<()> {
    if a.snapshot.owner.as_ref() != Some(owner)
        || a.snapshot
            .offer
            .as_ref()
            .is_none_or(|o| o.generation != generation)
    {
        return Err(ContractError::Conflict.into());
    }
    if a.worker.as_ref().is_none_or(|w| !w.matches_principal(p)) {
        return Err(ContractError::Forbidden.into());
    }
    Ok(())
}
