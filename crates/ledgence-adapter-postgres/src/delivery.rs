use crate::{persistence as db, *};
use ledgence_orchestration_core as core;
use tracing::Instrument;

impl PostgresStore {
    pub(crate) async fn probe_acquisition_once(
        &self,
        command: &AcquireCommand,
        finish_empty: bool,
    ) -> StoreResult<AcquisitionProbe> {
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
            let targeted: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatch_claim_receipts WHERE session_id=$1 AND consumer_id=$2 AND sequence=($3::text)::ldg_u64)")
                .bind(&session.id).bind(consumer).bind(command.sequence.to_string()).fetch_one(&mut *tx).await?;
            if targeted {
                return Err(ContractError::Conflict.into());
            }
            tx.commit().await?;
            return Ok(AcquisitionProbe::Completed {
                reply: checked.reply,
                kind: AcquisitionCompletion::Replayed,
            });
        }
        let external: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatch_routes WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND destination IS NOT NULL)")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.queue)
            .fetch_one(&mut *tx).await?;
        if external {
            return Err(ContractError::ExternalDispatchRequired.into());
        }
        let before_ms = codec::ms(before)?;
        let row = sqlx::query("SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND state='queued' AND cancel_requested_at_ms IS NULL AND dispatch_destination IS NULL AND available_at_ms <= $4 ORDER BY available_at_ms,submitted_at_ms,task_id LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.queue).bind(before_ms).fetch_optional(&mut *tx).await?;
        let candidate = row.as_ref().map(codec::task).transpose()?;
        if candidate.is_none() && !finish_empty {
            // A first-sequence placeholder participates in serialization, but
            // remains uncommitted. Explicit rollback releases it and all locks.
            let now = db::now(&mut tx).await?;
            core::acquire(
                core::Acquisition {
                    session: Some(&session),
                    cursor: cursor.as_ref(),
                    previous: previous.as_ref().map(|(t, a)| (t, a)),
                    candidate: None,
                    ids: None,
                },
                command,
                now,
            )?;
            let session_remaining_ms = session.expires_at.saturating_sub(now);
            tx.rollback().await?;
            return Ok(AcquisitionProbe::Pending {
                session_remaining_ms,
            });
        }

        if let (Some(candidate), Some((old_task, old_attempt))) = (&candidate, &mut previous)
            && candidate.task_id == old_task.task_id
        {
            *old_task = candidate.clone();
            *old_attempt =
                db::load_attempt(&mut tx, candidate, &old_attempt.lease.owner.attempt_id).await?;
        }
        let mut ids = if let Some(candidate) = &candidate {
            Some(core::AttemptIds {
                attempt_id: db::id(&mut tx, "att").await?,
                lease_id: db::id(&mut tx, "lease").await?,
                event_id: db::id(&mut tx, "evt").await?,
                trace: candidate.origin_trace.clone(),
            })
        } else {
            None
        };
        // This point is reached only after the durable response replay check.
        // Parent/link must be supplied before asking the bridge for a context:
        // materialization can run the sampler, including for nonrecording spans.
        let producer = candidate.as_ref().zip(ids.as_mut()).map(|(task, ids)| {
            let transport = self.trace_bridge.context(&tracing::Span::current());
            let span = invocation_span(task, command, ids);
            self.trace_bridge
                .set_parent(&span, task.origin_trace.as_ref());
            if let Some(transport) = &transport {
                self.trace_bridge.add_link(&span, transport);
            }
            ids.trace = self
                .trace_bridge
                .context(&span)
                .or_else(|| task.origin_trace.clone());
            span
        });
        let operation_span = producer.clone().unwrap_or_else(tracing::Span::none);
        async {
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
            if let Some(span) = &producer {
                // If cancellation or transport failure interrupts COMMIT, retain an
                // uncertain outcome instead of claiming publication or rollback.
                span.record("ledgence.publication.outcome", "commit_unconfirmed");
            }
            let committed = tx.commit().await;
            if let Some(span) = &producer {
                match &committed {
                    Ok(()) => {
                        span.record("ledgence.publication.outcome", "committed");
                        span.record("otel.status_code", "OK");
                    }
                    Err(error) if error.as_database_error().and_then(|error| error.code()).as_deref().is_some_and(known_commit_rollback) => {
                        span.record("ledgence.publication.outcome", "rolled_back");
                    }
                    Err(_) => {}
                }
            }
            committed?;
            self.acquisition_wake.publish(AcquisitionHint::AcquisitionCompleted(command.into()));
            Ok(AcquisitionProbe::Completed {
                reply: transition.reply,
                kind: if candidate.is_some() { AcquisitionCompletion::Claimed } else { AcquisitionCompletion::FinalizedEmpty },
            })
        }
        .instrument(operation_span)
        .await
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
        self.wake_queued_transition(&transition);
        Ok(transition.reply)
    }

    pub(crate) async fn lock_session(
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
        self.wake_queued_transition(&transition);
        if transition.reply {
            tracing::info!(
                ledgence.tenant.id = task.input.tenant_id,
                ledgence.namespace = task.input.namespace,
                ledgence.task.id = task.task_id,
                ledgence.run.id = task.run_id,
                ledgence.attempt.id = id,
                "recovery committed attempt expiry"
            );
        }
        Ok(transition.reply)
    }
}

// SQLSTATE 40003 explicitly means completion is unknown. Connection loss,
// shutdown, and unfamiliar failures also retain uncertainty; only recognized
// transaction rollback and deferred-constraint rejections establish rollback.
fn known_commit_rollback(code: &str) -> bool {
    matches!(code, "40000" | "40001" | "40002" | "40P01") || code.starts_with("23")
}

pub(crate) fn invocation_span(
    task: &TaskSnapshot,
    command: &AcquireCommand,
    ids: &core::AttemptIds,
) -> tracing::Span {
    tracing::info_span!(
        parent: None,
        "ledgence.invocation.create",
        otel.kind = "producer",
        ledgence.tenant.id = task.input.tenant_id,
        ledgence.namespace = task.input.namespace,
        ledgence.run.id = task.run_id,
        ledgence.task.id = task.task_id,
        ledgence.attempt.id = ids.attempt_id,
        ledgence.attempt.number = i64::from(task.attempt_count.saturating_add(1)),
        ledgence.worker.session.id = command.worker_session_id,
        ledgence.consumer.id = i64::from(command.consumer_id),
        ledgence.program.id = task.descriptor.program.id,
        ledgence.program.version = task.descriptor.program.version,
        ledgence.program.digest = task.descriptor.digest.0,
        ledgence.business.correlation_key = task.input.correlation_key.as_deref(),
        cloudevents.event_id = ids.event_id,
        cloudevents.event_source = "urn:ledgence:orchestrator",
        cloudevents.event_spec_version = "1.0",
        cloudevents.event_type = "com.ledgence.task.invocation.requested.v1",
        ledgence.publication.outcome = "not_committed",
        otel.status_code = "ERROR",
    )
}

impl RecoveryStore for PostgresStore {
    fn expire_batch(&self, limit: u32) -> ContractFuture<'_, RecoveryProgress> {
        Box::pin(async move {
            let span = tracing::info_span!(
                parent: None,
                "ledgence.recovery.scan",
                otel.kind = "internal",
                ledgence.recovery.examined = tracing::field::Empty,
                ledgence.recovery.expired = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
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
            let result =
                tokio::time::timeout(self.operation_timeout, work.instrument(span.clone()))
                    .await
                    .unwrap_or_else(|_| {
                        Err(ContractError::Unavailable(
                            "expiry batch timed out; committed progress remains durable".into(),
                        ))
                    });
            match &result {
                Ok(progress) => {
                    span.record("ledgence.recovery.examined", i64::from(progress.examined));
                    span.record("ledgence.recovery.expired", i64::from(progress.expired));
                }
                Err(_) => {
                    span.record("otel.status_code", "ERROR");
                }
            }
            result
        })
    }
}

#[cfg(test)]
mod trace_outcome_tests {
    use super::known_commit_rollback;

    #[test]
    fn ambiguous_server_errors_do_not_claim_rollback() {
        for code in ["40003", "08007", "08006", "57P01", "58030", "XX000"] {
            assert!(!known_commit_rollback(code), "{code}");
        }
        for code in ["40001", "40P01", "23503", "23505", "23514"] {
            assert!(known_commit_rollback(code), "{code}");
        }
    }
}
