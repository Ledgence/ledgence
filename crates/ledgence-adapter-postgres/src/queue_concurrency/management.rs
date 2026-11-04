use super::*;

impl PostgresStore {
    pub(super) fn queue_operator(
        &self,
        p: &VerifiedPrincipal,
        s: &Scope,
        cap: AccessCapability,
        at: u64,
    ) -> Result<()> {
        self.require_scope(s)?;
        self.require_operator(p, cap, at)?;
        if &p.audience().scope != s {
            return Err(ContractError::Forbidden);
        }
        Ok(())
    }

    pub(super) async fn put_queue_once(
        &self,
        p: &VerifiedPrincipal,
        c: &QueuePolicyPut,
    ) -> StoreResult<QueuePolicyReceipt> {
        c.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        self.queue_operator(
            p,
            &c.scope,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        sqlx::query("SELECT pg_advisory_xact_lock(ldg_queue_lock_key($1,$2,$3))")
            .bind(&c.scope.tenant_id)
            .bind(&c.scope.namespace)
            .bind(&c.queue)
            .execute(&mut *tx)
            .await?;
        let existing = get_policy(&mut tx, &c.scope, &c.queue, true).await?;
        lock_receipt(&mut tx, &c.scope, &c.request_id).await?;
        let now = db::now(&mut tx).await?;
        self.queue_operator(p, &c.scope, AccessCapability::ResourceControl, now)?;
        if let Some(mut r) =
            receipt::<QueuePolicyReceipt, _>(&mut tx, p, &c.scope, &c.request_id, "queue_policy", c)
                .await?
        {
            r.already_accepted = true;
            r.validate_for(c)?;
            tx.commit().await?;
            return Ok(r);
        }
        let previous = existing.as_ref().map(revision).transpose()?;
        if previous != c.expected_revision {
            return Err(ContractError::Conflict.into());
        }
        let timeout = c.policy.timeout.map(|v| i64::from(v.after_ms));
        let grace = c.policy.timeout.and_then(|v| match v.release {
            QueueTimeoutRelease::Confirm => None,
            QueueTimeoutRelease::AfterGrace { grace_ms } => Some(i64::from(grace_ms)),
        });
        let row=sqlx::query("INSERT INTO queue_concurrency_policies(tenant_id,namespace,queue,revision,updated_at_ms,maximum,timeout_ms,grace_ms) VALUES($1,$2,$3,1,$4,$5,$6,$7) ON CONFLICT(tenant_id,namespace,queue) DO UPDATE SET revision=queue_concurrency_policies.revision+1,updated_at_ms=EXCLUDED.updated_at_ms,maximum=EXCLUDED.maximum,timeout_ms=EXCLUDED.timeout_ms,grace_ms=EXCLUDED.grace_ms RETURNING *,trunc(revision)::text AS rev")
            .bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(&c.queue).bind(codec::ms(now)?).bind(c.policy.max_concurrency.map(i64::from)).bind(timeout).bind(grace).fetch_optional(&mut *tx).await?.ok_or_else(||invalid("suppressed policy update"))?;
        let record = snapshot(&row)?.record;
        let r = QueuePolicyReceipt {
            record,
            already_accepted: false,
        };
        r.validate_for(c)?;
        if previous.is_none() {
            // The exclusive cutover lock prevents racing a new assignment or
            // a source transition. This one-time scan uses retained attempt evidence.
            let (expected, occupied, uncertain): (i64, i64, i64) =
                sqlx::query_as(include_str!("adopt.sql"))
                    .bind(&c.scope.tenant_id)
                    .bind(&c.scope.namespace)
                    .bind(&c.queue)
                    .bind(codec::ms(now)?)
                    .bind(timeout)
                    .bind(grace)
                    .fetch_one(&mut *tx)
                    .await?;
            let saved:Option<bool>=sqlx::query_scalar("UPDATE queue_concurrency_policies SET occupied=$4,uncertain=$5 WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 RETURNING occupied=$4 AND uncertain=$5")
                .bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(&c.queue).bind(occupied).bind(uncertain).fetch_optional(&mut *tx).await?;
            if expected != occupied || saved != Some(true) {
                return Err(invalid("incomplete adoption").into());
            }
        }
        record_receipt(
            &mut tx,
            p,
            &c.scope,
            &c.request_id,
            "queue_policy",
            c,
            &r,
            AuditOperation::QueuePolicyAccepted {
                queue: c.queue.clone(),
                request_id: c.request_id.clone(),
                revision: r.record.revision,
                policy_digest: c.policy.digest()?,
                reason: c.reason.clone(),
            },
            now,
        )
        .await?;
        self.queue_operator(
            p,
            &c.scope,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        self.wake_capacity(&c.scope, &c.queue);
        Ok(r)
    }

    pub(super) fn wake_capacity(&self, s: &Scope, q: &str) {
        self.acquisition_wake
            .publish(AcquisitionHint::QueueChanged(AcquisitionQueue {
                scope: s.clone(),
                queue: q.to_string(),
            }));
    }

    pub(super) async fn inspect_queue_once(
        &self,
        p: &VerifiedPrincipal,
        q: &QueuePolicyQuery,
    ) -> StoreResult<QueuePolicySnapshot> {
        q.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_read().await?;
        self.queue_operator(
            p,
            &q.scope,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        let result = snapshot(
            &get_policy(&mut tx, &q.scope, &q.queue, false)
                .await?
                .ok_or(ContractError::NotFound)?,
        )?;
        result.validate_for(q)?;
        self.queue_operator(
            p,
            &q.scope,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        Ok(result)
    }

    pub(super) async fn list_queues_once(
        &self,
        p: &VerifiedPrincipal,
        q: &QueuePolicyList,
    ) -> StoreResult<QueuePolicyPage> {
        q.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_read().await?;
        self.queue_operator(
            p,
            &q.scope,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        let rows=sqlx::query("SELECT *,trunc(revision)::text AS rev FROM queue_concurrency_policies WHERE tenant_id=$1 AND namespace=$2 AND queue>$3 ORDER BY queue LIMIT $4")
            .bind(&q.scope.tenant_id).bind(&q.scope.namespace).bind(q.after.as_deref().unwrap_or("")).bind(i64::from(q.limit)+1).fetch_all(&mut *tx).await?;
        let more = rows.len() > q.limit as usize;
        let items = rows
            .iter()
            .take(q.limit as usize)
            .map(snapshot)
            .collect::<StoreResult<Vec<_>>>()?;
        let next = if more {
            items.last().map(|v| v.record.queue.clone())
        } else {
            None
        };
        let page = QueuePolicyPage { items, next };
        page.validate_for(q)?;
        self.queue_operator(
            p,
            &q.scope,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        Ok(page)
    }

    pub(super) async fn list_holds_once(
        &self,
        p: &VerifiedPrincipal,
        q: &QueueHoldList,
    ) -> StoreResult<QueueHoldPage> {
        q.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_read().await?;
        self.queue_operator(
            p,
            &q.scope,
            AccessCapability::SensitiveRead,
            db::now(&mut tx).await?,
        )?;
        if get_policy(&mut tx, &q.scope, &q.queue, false)
            .await?
            .is_none()
        {
            return Err(ContractError::NotFound.into());
        }
        let query = if q.include_released {
            "SELECT *,trunc(revision)::text AS rev,trunc(policy_revision)::text AS policy_rev FROM queue_concurrency_holds WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND attempt_id>$4 ORDER BY attempt_id LIMIT $5"
        } else {
            "SELECT *,trunc(revision)::text AS rev,trunc(policy_revision)::text AS policy_rev FROM queue_concurrency_holds WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND attempt_id>$4 AND state<>'released' ORDER BY attempt_id LIMIT $5"
        };
        let rows = sqlx::query(query)
            .bind(&q.scope.tenant_id)
            .bind(&q.scope.namespace)
            .bind(&q.queue)
            .bind(q.after.as_deref().unwrap_or(""))
            .bind(i64::from(q.limit) + 1)
            .fetch_all(&mut *tx)
            .await?;
        let more = rows.len() > q.limit as usize;
        let items = rows
            .iter()
            .take(q.limit as usize)
            .map(hold)
            .collect::<StoreResult<Vec<_>>>()?;
        let next = if more {
            items.last().map(|v| v.attempt_id.clone())
        } else {
            None
        };
        let page = QueueHoldPage { items, next };
        page.validate_for(q)?;
        self.queue_operator(
            p,
            &q.scope,
            AccessCapability::SensitiveRead,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        Ok(page)
    }
}
