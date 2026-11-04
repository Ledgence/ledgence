use super::*;

/// Preserve the normal attempt/task -> policy -> source lock order. A retained
/// hold can be closed after the task itself was collected.
async fn lock_source(
    tx: &mut PgConnection,
    s: &Scope,
    q: &str,
    task: &str,
    attempt: &str,
    nonblocking: bool,
) -> StoreResult<PgRow> {
    let task_lock = if nonblocking {
        "SELECT task_id FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND task_id=$4 FOR NO KEY UPDATE NOWAIT"
    } else {
        "SELECT task_id FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND task_id=$4 FOR NO KEY UPDATE"
    };
    let policy_lock = if nonblocking {
        "SELECT 1 FROM queue_concurrency_policies WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR UPDATE NOWAIT"
    } else {
        "SELECT 1 FROM queue_concurrency_policies WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR UPDATE"
    };
    sqlx::query(task_lock)
        .bind(&s.tenant_id)
        .bind(&s.namespace)
        .bind(q)
        .bind(task)
        .fetch_optional(&mut *tx)
        .await?;
    if sqlx::query(policy_lock)
        .bind(&s.tenant_id)
        .bind(&s.namespace)
        .bind(q)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        return Err(ContractError::NotFound.into());
    }
    let row = get_hold(tx, s, q, attempt).await?;
    if row.try_get::<String, _>("task_id")? != task {
        return Err(ContractError::NotFound.into());
    }
    Ok(row)
}

/// Execution deadline is independent of the longer cleanup/settlement lease.
/// Fencing the actual attempt also protects generic replays from an older server.
async fn fence(tx: &mut PgConnection, task: &str, attempt: &str, at: u64) -> StoreResult<()> {
    let row:Option<bool>=sqlx::query_scalar("UPDATE attempts SET deadline_ms=LEAST(deadline_ms,$3) WHERE task_id=$1 AND attempt_id=$2 RETURNING deadline_ms<=$3")
        .bind(task).bind(attempt).bind(codec::ms(at)?).fetch_optional(&mut *tx).await?;
    if row != Some(true) {
        let retained: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE task_id=$1 AND attempt_id=$2)",
        )
        .bind(task)
        .bind(attempt)
        .fetch_one(tx)
        .await?;
        if retained {
            return Err(invalid("attempt stop fence not persisted").into());
        }
    }
    Ok(())
}
async fn move_hold(
    tx: &mut PgConnection,
    id: &str,
    state: &str,
    kind: Option<&str>,
    at: u64,
) -> StoreResult<()> {
    let changed: bool = sqlx::query_scalar("SELECT ldg_queue_transition($1,$2,$3,$4)")
        .bind(id)
        .bind(state)
        .bind(kind)
        .bind(codec::ms(at)?)
        .fetch_one(tx)
        .await?;
    if !changed {
        return Err(invalid("hold transition suppressed").into());
    }
    Ok(())
}
impl PostgresStore {
    pub(super) async fn close_hold_once(
        &self,
        p: &VerifiedPrincipal,
        c: &QueueHoldClose,
    ) -> StoreResult<QueueHoldCloseReceipt> {
        c.validate()?;
        let o = &c.owner;
        self.require_scope(&o.scope)?;
        if p.class() != PrincipalClass::Worker || p.audience().scope != o.scope {
            return Err(ContractError::Forbidden.into());
        }
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        p.require_at(self.audit_audience()?, db::now(&mut tx).await?)?;
        p.require(AccessCapability::WorkerExecute)?;
        let row = lock_source(
            &mut tx,
            &o.scope,
            &c.queue,
            &o.task_id,
            &o.attempt_id,
            false,
        )
        .await?;
        let mut h = hold(&row)?;
        if h.worker_session_id != o.worker_session_id
            || h.consumer_id != o.consumer_id
            || row.try_get::<String, _>("lease_id")? != o.lease_id
            || row.try_get::<i64, _>("generation")? != i64::from(o.generation)
        {
            return Err(ContractError::Forbidden.into());
        }
        if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("worker_identity_bytes")? {
            let identity: AdmissionWorkerIdentity = codec::decode(&bytes)?;
            identity.validate()?;
            if !identity.matches_principal(p) {
                return Err(ContractError::Forbidden.into());
            }
        }
        let now = db::now(&mut tx).await?;
        p.require_at(self.audit_audience()?, now)?;
        let already_closed = h.state == QueueHoldState::Released;
        if !already_closed {
            fence(&mut tx, &o.task_id, &o.attempt_id, now).await?;
            move_hold(&mut tx, &o.attempt_id, "released", Some("confirmed"), now).await?;
            h = hold(&get_hold(&mut tx, &o.scope, &c.queue, &o.attempt_id).await?)?;
        }
        let r = QueueHoldCloseReceipt::new(
            c,
            h.revision,
            h.release_kind.ok_or_else(|| invalid("source not closed"))?,
            already_closed,
        )?;
        p.require_at(self.audit_audience()?, db::now(&mut tx).await?)?;
        tx.commit().await?;
        self.wake_capacity(&o.scope, &c.queue);
        Ok(r)
    }

    pub(super) async fn release_hold_once(
        &self,
        p: &VerifiedPrincipal,
        c: &QueueHoldRelease,
    ) -> StoreResult<QueueHoldReceipt> {
        c.validate()?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        self.queue_operator(
            p,
            &c.scope,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        let row = lock_source(
            &mut tx,
            &c.scope,
            &c.queue,
            &c.task_id,
            &c.attempt_id,
            false,
        )
        .await?;
        lock_receipt(&mut tx, &c.scope, &c.request_id).await?;
        let now = db::now(&mut tx).await?;
        self.queue_operator(p, &c.scope, AccessCapability::ResourceControl, now)?;
        if let Some(mut r) =
            receipt::<QueueHoldReceipt, _>(&mut tx, p, &c.scope, &c.request_id, "queue_release", c)
                .await?
        {
            r.already_accepted = true;
            r.validate_for(c)?;
            tx.commit().await?;
            return Ok(r);
        }
        let h = hold(&row)?;
        if h.revision != c.expected_revision || h.state != QueueHoldState::Uncertain {
            return Err(ContractError::Conflict.into());
        }
        fence(&mut tx, &c.task_id, &c.attempt_id, now).await?;
        move_hold(&mut tx, &c.attempt_id, "released", Some("operator"), now).await?;
        let hold = hold(&get_hold(&mut tx, &c.scope, &c.queue, &c.attempt_id).await?)?;
        let revision = hold.revision;
        let result = QueueHoldReceipt {
            hold,
            already_accepted: false,
        };
        result.validate_for(c)?;
        record_receipt(
            &mut tx,
            p,
            &c.scope,
            &c.request_id,
            "queue_release",
            c,
            &result,
            AuditOperation::QueueHoldReleased {
                queue: c.queue.clone(),
                task_id: c.task_id.clone(),
                attempt_id: c.attempt_id.clone(),
                request_id: c.request_id.clone(),
                revision,
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
        Ok(result)
    }

    pub(crate) async fn advance_queue_timeouts(&self, limit: u32) -> Result<()> {
        // A contended source is skipped without consuming the next sweep's head.
        // The cursor is a local fairness hint, never part of execution authority.
        let cursor = self
            .queue_timeout_cursor
            .lock()
            .map_err(|_| invalid("timeout scan cursor"))?
            .clone();
        let mut candidates = self.queue_timeout_page(limit, &cursor).await?;
        if candidates.is_empty() && cursor.0 > 0 {
            candidates = self.queue_timeout_page(limit, &(0, String::new())).await?;
        }
        let next = if candidates.len() == limit as usize {
            candidates
                .last()
                .map(|r| -> std::result::Result<_, sqlx::Error> {
                    Ok((r.try_get("due")?, r.try_get("attempt_id")?))
                })
                .transpose()
                .map_err(database_error)?
                .unwrap_or_default()
        } else {
            (0, String::new())
        };
        *self
            .queue_timeout_cursor
            .lock()
            .map_err(|_| invalid("timeout scan cursor"))? = next;
        for r in candidates {
            let scope = Scope {
                tenant_id: r.try_get("tenant_id").map_err(database_error)?,
                namespace: r.try_get("namespace").map_err(database_error)?,
            };
            let queue: String = r.try_get("queue").map_err(database_error)?;
            let task: String = r.try_get("task_id").map_err(database_error)?;
            let attempt: String = r.try_get("attempt_id").map_err(database_error)?;
            self.run(|| self.tick_timeout(&scope, &queue, &task, &attempt))
                .await?;
        }
        Ok(())
    }

    async fn queue_timeout_page(&self, limit: u32, cursor: &(i64, String)) -> Result<Vec<PgRow>> {
        let scope = self.instance_scope.get();
        self.run(|| async {
            Ok(sqlx::query(include_str!("due.sql"))
                .bind(scope.map(|s| s.tenant_id.as_str()))
                .bind(scope.map(|s| s.namespace.as_str()))
                .bind(i64::from(limit))
                .bind(cursor.0)
                .bind(&cursor.1)
                .fetch_all(&self.pool)
                .await?)
        })
        .await
    }

    async fn tick_timeout(&self, s: &Scope, q: &str, task: &str, attempt: &str) -> StoreResult<()> {
        self.require_scope(s)?;
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = match lock_source(&mut tx, s, q, task, attempt, true).await {
            Ok(row) => row,
            Err(StoreError::Database(e))
                if e.as_database_error().and_then(|e| e.code()).as_deref() == Some("55P03") =>
            {
                // Another replica/request owns this source or its queue. Do not
                // block recovery of unrelated queues or ordinary lease expiry.
                tx.rollback().await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let h = hold(&row)?;
        let now = db::now(&mut tx).await?;
        if h.state != QueueHoldState::Released
            && h.stop_requested_at.is_none()
            && h.deadline.is_some_and(|v| v <= now)
        {
            fence(&mut tx, task, attempt, now).await?;
            let stopped:Option<bool>=sqlx::query_scalar("UPDATE queue_concurrency_holds SET stop_requested_at_ms=$2,force_at_ms=$2+grace_ms, revision=revision+1 WHERE attempt_id=$1 AND stop_requested_at_ms IS NULL AND state<>'released' RETURNING stop_requested_at_ms=$2 AND force_at_ms IS NOT DISTINCT FROM ($2+grace_ms)")
                .bind(attempt).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
            if stopped != Some(true) {
                return Err(invalid("stop intent not persisted").into());
            }
            record_system(&mut tx, attempt, "stop_requested", now).await?;
            if h.state == QueueHoldState::Reserved {
                // No dispatch crossed the fenced source. The row updates are
                // serialized with acquisition, renewal and ready-to-invoke Start.
                move_hold(&mut tx, attempt, "released", Some("never_started"), now).await?;
            } else if h.state != QueueHoldState::Uncertain {
                move_hold(&mut tx, attempt, "uncertain", None, now).await?;
            }
        } else if h.state != QueueHoldState::Released
            && h.stop_requested_at.is_some()
            && optional_time(&row, "force_at_ms")?.is_some_and(|v| v <= now)
        {
            fence(&mut tx, task, attempt, now).await?;
            move_hold(&mut tx, attempt, "released", Some("timeout"), now).await?;
            record_system(&mut tx, attempt, "timeout_released", now).await?;
        }
        tx.commit().await?;
        self.wake_capacity(s, q);
        Ok(())
    }
}

async fn record_system(
    tx: &mut PgConnection,
    attempt: &str,
    kind: &str,
    at: u64,
) -> StoreResult<()> {
    let saved:Option<bool>=sqlx::query_scalar("INSERT INTO queue_concurrency_events(attempt_id,event_kind,at_ms) VALUES($1,$2,$3) RETURNING attempt_id=$1 AND event_kind=$2 AND at_ms=$3")
        .bind(attempt).bind(kind).bind(codec::ms(at)?).fetch_optional(tx).await?;
    if saved != Some(true) {
        return Err(invalid("system event not persisted").into());
    }
    Ok(())
}
