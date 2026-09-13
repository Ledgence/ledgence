use crate::{persistence as db, *};
use ledgence_orchestration_core as core;

impl PostgresStore {
    pub(crate) async fn acquire_once(&self, command: &AcquireCommand) -> StoreResult<AcquireReply> {
        command.scope.validate()?;
        validate_text(&command.worker_session_id, 128)?;
        validate_text(&command.queue, 128)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let session = self
            .lock_session(&mut tx, &command.worker_session_id)
            .await?;
        if session.scope != command.scope
            || session.queue != command.queue
            || command.consumer_id >= session.concurrency
        {
            return Err(ContractError::InvalidInput(
                "acquisition does not match the registered consumer".into(),
            )
            .into());
        }
        let consumer = i64::from(command.consumer_id);
        sqlx::query!("INSERT INTO consumer_cursors(session_id,consumer_id) VALUES($1,$2) ON CONFLICT(session_id,consumer_id) DO NOTHING",session.id,consumer).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT *,trunc(sequence)::text AS sequence_text FROM consumer_cursors WHERE session_id=$1 AND consumer_id=$2 FOR UPDATE")
            .bind(&session.id).bind(consumer).fetch_one(&mut *tx).await?;
        let cursor = codec::cursor(&row, &session)?;
        let mut previous = match cursor.as_ref().and_then(|c| c.assignment.as_ref()) {
            Some(reference) => Some(db::previous(&mut tx, &command.scope, reference).await?),
            None => None,
        };
        let replay = cursor
            .as_ref()
            .is_some_and(|c| c.command.sequence == command.sequence);
        let before = db::now(&mut tx).await?;
        // Validate sequence and prior ownership before selecting any candidate.
        // This proposed Empty is not persisted unless the final scan is empty.
        let checked = core::acquire(
            core::Acquisition {
                session: Some(&session),
                cursor: cursor.as_ref(),
                previous: previous.as_ref().map(|(t, a)| (t, a)),
                candidate: None,
                ids: None,
            },
            command,
            before,
        )?;
        if replay {
            tx.commit().await?;
            return Ok(checked.reply);
        }
        let before_ms = codec::ms(before)?;
        let row = sqlx::query("SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND state='queued' AND cancel_requested_at_ms IS NULL AND available_at_ms <= $4 ORDER BY available_at_ms,submitted_at_ms,task_id LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.queue).bind(before_ms).fetch_optional(&mut *tx).await?;
        let candidate = row.as_ref().map(codec::task).transpose()?;
        if let (Some(candidate), Some((old_task, old_attempt))) = (&candidate, &mut previous)
            && candidate.task_id == old_task.task_id
        {
            *old_task = candidate.clone();
            *old_attempt =
                db::load_attempt(&mut tx, candidate, &old_attempt.lease.owner.attempt_id).await?;
        }
        let ids = if let Some(candidate) = &candidate {
            Some(core::AttemptIds {
                attempt_id: db::id(&mut tx, "att").await?,
                lease_id: db::id(&mut tx, "lease").await?,
                event_id: db::id(&mut tx, "evt").await?,
                trace: candidate.origin_trace.clone(),
            })
        } else {
            None
        };
        let transition = core::acquire(
            core::Acquisition {
                session: Some(&session),
                cursor: cursor.as_ref(),
                previous: previous.as_ref().map(|(t, a)| (t, a)),
                candidate: candidate.as_ref(),
                ids: ids.as_ref(),
            },
            command,
            db::now(&mut tx).await?,
        )?;
        if let Some(changes) = &transition.changes {
            let attempt = changes.attempt.as_ref().ok_or_else(|| {
                ContractError::Unavailable("claim transition omitted its attempt".into())
            })?;
            db::insert_attempt(&mut tx, attempt).await?;
            db::apply(&mut tx, changes).await?;
        }
        let sequence = command.sequence.to_string();
        let task = transition
            .cursor
            .assignment
            .as_ref()
            .map(|a| a.task_id.as_str());
        let attempt = transition
            .cursor
            .assignment
            .as_ref()
            .map(|a| a.attempt_id.as_str());
        sqlx::query!("UPDATE consumer_cursors SET sequence=($3::text)::ldg_u64,task_id=$4,attempt_id=$5 WHERE session_id=$1 AND consumer_id=$2",session.id,consumer,sequence,task,attempt).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(transition.reply)
    }

    pub(crate) async fn renew_once(&self, command: &RenewCommand) -> StoreResult<Authority> {
        let owner = &command.owner;
        owner.scope.validate()?;
        validate_text(&owner.worker_session_id, 128)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let session = self.lock_session(&mut tx, &owner.worker_session_id).await?;
        let row = sqlx::query("SELECT *,trunc(sequence)::text AS sequence_text FROM consumer_cursors WHERE session_id=$1 AND consumer_id=$2 FOR UPDATE")
            .bind(&session.id).bind(i64::from(owner.consumer_id)).fetch_optional(&mut *tx).await?.ok_or(ContractError::OwnershipLost)?;
        let cursor = codec::cursor(&row, &session)?.ok_or(ContractError::OwnershipLost)?;
        if cursor.assignment.as_ref()
            != Some(&AttemptRef {
                task_id: owner.task_id.clone(),
                attempt_id: owner.attempt_id.clone(),
            })
        {
            return Err(ContractError::OwnershipLost.into());
        }
        let task = db::load_task(&mut tx, &owner.scope, &owner.task_id, true).await?;
        let attempt = db::load_attempt(&mut tx, &task, &owner.attempt_id).await?;
        let transition = core::renew(
            &task,
            &attempt,
            Some(&session),
            command,
            db::now(&mut tx).await?,
        )?;
        db::apply(&mut tx, &transition).await?;
        tx.commit().await?;
        Ok(transition.reply)
    }

    pub(crate) async fn settle_once(&self, command: &SettleCommand) -> StoreResult<SettleReply> {
        let owner = &command.owner;
        owner.scope.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let task = db::load_task(&mut tx, &owner.scope, &owner.task_id, true).await?;
        let attempt = db::load_attempt(&mut tx, &task, &owner.attempt_id).await?;
        let transition = core::settle(&task, &attempt, command, db::now(&mut tx).await?)?;
        db::apply(&mut tx, &transition).await?;
        tx.commit().await?;
        Ok(transition.reply)
    }

    async fn lock_session(
        &self,
        tx: &mut sqlx::PgConnection,
        id: &str,
    ) -> StoreResult<WorkerSession> {
        let row = sqlx::query("SELECT * FROM worker_sessions WHERE session_id=$1 FOR SHARE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ContractError::UnknownSession)?;
        let session = codec::session(&row)?;
        if db::now(tx).await? >= session.expires_at {
            return Err(ContractError::SessionExpired.into());
        }
        Ok(session)
    }

    async fn expire_one(&self, id: &str) -> StoreResult<bool> {
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let row = sqlx::query(
            "SELECT * FROM tasks WHERE task_id=$1 AND state='active' FOR NO KEY UPDATE SKIP LOCKED",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(false);
        };
        let task = codec::task(&row)?;
        let id = task
            .current_attempt_id
            .as_ref()
            .ok_or_else(|| ContractError::Unavailable("active task has no attempt".into()))?;
        let attempt = db::load_attempt(&mut tx, &task, id).await?;
        let transition = core::expire(&task, &attempt, db::now(&mut tx).await?)?;
        if transition.reply {
            db::apply(&mut tx, &transition).await?;
        }
        tx.commit().await?;
        Ok(transition.reply)
    }
}
impl RecoveryStore for PostgresStore {
    fn expire_batch(&self, limit: u32) -> ContractFuture<'_, RecoveryProgress> {
        Box::pin(async move {
            let work = async {
                if !(1..=MAX_RECOVERY_BATCH).contains(&limit) {
                    return Err(ContractError::InvalidInput(
                        "expiry batch limit must be 1..=100".into(),
                    ));
                }
                // Each operation is bounded independently. Already committed tasks
                // remain recovered if a later candidate encounters an unavailable DB.
                let ids = self
                    .run(|| async {
                        let limit = i64::from(limit);
                        Ok(sqlx::query_file!("queries/expiry_candidates.sql", limit)
                            .fetch_all(&self.pool)
                            .await?
                            .into_iter()
                            .map(|row| row.task_id)
                            .collect::<Vec<_>>())
                    })
                    .await?;
                let mut progress = RecoveryProgress {
                    examined: 0,
                    expired: 0,
                };
                for id in ids {
                    progress.examined += 1;
                    progress.expired += u32::from(self.run(|| self.expire_one(&id)).await?);
                }
                Ok(progress)
            };
            tokio::time::timeout(self.operation_timeout, work)
                .await
                .unwrap_or_else(|_| {
                    Err(ContractError::Unavailable(
                        "expiry batch timed out; committed progress remains durable".into(),
                    ))
                })
        })
    }
}
