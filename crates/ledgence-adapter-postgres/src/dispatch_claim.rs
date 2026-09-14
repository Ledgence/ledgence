use crate::{persistence as db, *};
use ledgence_orchestration_core as core;
use sqlx::Row;

impl PostgresStore {
    pub(crate) async fn claim_dispatch_once(
        &self,
        command: &ClaimCommand,
    ) -> StoreResult<ClaimReply> {
        command.validate()?;
        let acquire = &command.acquisition;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        // Immutable receipt lookup precedes generic cursor replay. A sequence
        // used for another dispatch can never acknowledge this received record.
        if let Some(bytes) = sqlx::query_scalar::<_, Vec<u8>>("SELECT reply_bytes FROM dispatch_claim_receipts WHERE session_id=$1 AND consumer_id=$2 AND sequence=($3::text)::ldg_u64")
            .bind(&acquire.worker_session_id).bind(i64::from(acquire.consumer_id)).bind(acquire.sequence.to_string())
            .fetch_optional(&mut *tx).await?
        {
            let mut saved: ClaimReply = decode_unique_json(&bytes, DISPATCH_MAX_BYTES).map_err(|_| ContractError::Unavailable("invalid stored claim receipt".into()))?;
            if saved.command != *command { return Err(ContractError::Conflict.into()); }
            saved.validate_reply_against(command)?;
            if matches!(saved.disposition, ClaimDisposition::Claimed { .. }) {
                self.refresh_claim_replay(&mut tx, command, &mut saved).await?;
            }
            tx.commit().await?;
            return Ok(saved);
        }
        let session = self
            .lock_session(&mut tx, &acquire.worker_session_id)
            .await?;
        if session.scope != acquire.scope
            || session.queue != acquire.queue
            || acquire.consumer_id >= session.concurrency
        {
            return Err(ContractError::InvalidInput(
                "claim does not match registered consumer".into(),
            )
            .into());
        }
        let consumer = i64::from(acquire.consumer_id);
        sqlx::query("INSERT INTO consumer_cursors(session_id,consumer_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
            .bind(&session.id).bind(consumer).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT *,trunc(sequence)::text AS sequence_text FROM consumer_cursors WHERE session_id=$1 AND consumer_id=$2 FOR UPDATE")
            .bind(&session.id).bind(consumer).fetch_one(&mut *tx).await?;
        let cursor = codec::cursor(&row, &session)?;
        // A competing claim may have committed after the first receipt lookup,
        // and its client may already have advanced the cursor. Recheck under
        // cursor serialization before rejecting any consumed sequence; fresh
        // claims need no additional receipt query.
        if let Some(cursor) = cursor
            .as_ref()
            .filter(|c| c.command.sequence >= acquire.sequence)
        {
            let receipt: Option<Vec<u8>> = sqlx::query_scalar("SELECT reply_bytes FROM dispatch_claim_receipts WHERE session_id=$1 AND consumer_id=$2 AND sequence=($3::text)::ldg_u64")
                .bind(&session.id).bind(consumer).bind(acquire.sequence.to_string()).fetch_optional(&mut *tx).await?;
            if let Some(bytes) = receipt {
                let mut saved: ClaimReply = decode_unique_json(&bytes, DISPATCH_MAX_BYTES)
                    .map_err(|_| {
                        ContractError::Unavailable("invalid stored claim receipt".into())
                    })?;
                if saved.command != *command {
                    return Err(ContractError::Conflict.into());
                }
                saved.validate_reply_against(command)?;
                if matches!(saved.disposition, ClaimDisposition::Claimed { .. }) {
                    self.refresh_claim_replay(&mut tx, command, &mut saved)
                        .await?;
                }
                tx.commit().await?;
                return Ok(saved);
            }
            if cursor.command.sequence == acquire.sequence {
                return Err(ContractError::Conflict.into());
            }
            // An older operation without a receipt follows the existing core
            // validation, including session expiry and ObsoleteOperation.
        }
        let mut previous = match cursor.as_ref().and_then(|c| c.assignment.as_ref()) {
            Some(reference) => Some(db::previous(&mut tx, &acquire.scope, reference).await?),
            None => None,
        };
        // Reject sequence gaps or a still-owned prior task before claiming a
        // target. This validation is not persisted on any error.
        core::acquire(
            core::Acquisition {
                session: Some(&session),
                cursor: cursor.as_ref(),
                previous: previous.as_ref().map(|(t, a)| (t, a)),
                candidate: None,
                ids: None,
            },
            acquire,
            db::now(&mut tx).await?,
        )?;
        let row = sqlx::query("SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 FOR NO KEY UPDATE")
            .bind(&command.dispatch.scope.tenant_id).bind(&command.dispatch.scope.namespace).bind(&command.dispatch.task_id)
            .fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        let task = codec::task(&row)?;
        let destination: Option<String> = row.try_get("dispatch_destination")?;
        if destination.is_none() {
            return Err(
                ContractError::InvalidInput("task does not use external dispatch".into()).into(),
            );
        }
        let now = db::now(&mut tx).await?;
        let future = if task.state == TaskState::Queued && task.available_at > now {
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatch_intents WHERE task_id=$1 AND generation=$2 AND destination=$3 AND available_at_ms=$4)")
                .bind(&task.task_id).bind(i64::from(command.dispatch.generation)).bind(destination.as_deref()).bind(codec::ms(task.available_at)?)
                .fetch_one(&mut *tx).await?
        } else {
            false
        };
        let mut producer = None;
        let (transition, disposition) =
            match core::classify_dispatch(&task, &command.dispatch, now, future)? {
                core::DispatchDecision::Handled(disposition) => {
                    // Task-lock contention can outlive the session even when no
                    // execution authority is returned. Revalidate before writing
                    // the nonauthority receipt and consuming its sequence.
                    let transition = core::acquire(
                        core::Acquisition {
                            session: Some(&session),
                            cursor: cursor.as_ref(),
                            previous: previous.as_ref().map(|(t, a)| (t, a)),
                            candidate: None,
                            ids: None,
                        },
                        acquire,
                        db::now(&mut tx).await?,
                    )?;
                    (transition, disposition)
                }
                core::DispatchDecision::Ready => {
                    if let Some((old_task, old_attempt)) = &mut previous
                        && old_task.task_id == task.task_id
                    {
                        *old_task = task.clone();
                        *old_attempt =
                            db::load_attempt(&mut tx, &task, &old_attempt.lease.owner.attempt_id)
                                .await?;
                    }
                    let mut ids = core::AttemptIds {
                        attempt_id: db::id(&mut tx, "att").await?,
                        lease_id: db::id(&mut tx, "lease").await?,
                        event_id: db::id(&mut tx, "evt").await?,
                        trace: task.origin_trace.clone(),
                    };
                    let transport = self.trace_bridge.context(&tracing::Span::current());
                    let span = crate::delivery::invocation_span(&task, acquire, &ids);
                    self.trace_bridge
                        .set_parent(&span, task.origin_trace.as_ref());
                    if let Some(transport) = &transport {
                        self.trace_bridge.add_link(&span, transport);
                    }
                    ids.trace = self
                        .trace_bridge
                        .context(&span)
                        .or_else(|| task.origin_trace.clone());
                    let transition = core::acquire(
                        core::Acquisition {
                            session: Some(&session),
                            cursor: cursor.as_ref(),
                            previous: previous.as_ref().map(|(t, a)| (t, a)),
                            candidate: Some(&task),
                            ids: Some(&ids),
                        },
                        acquire,
                        db::now(&mut tx).await?,
                    )?;
                    let disposition = ClaimDisposition::Claimed {
                        reply: transition.reply.clone(),
                    };
                    producer = Some(span);
                    (transition, disposition)
                }
            };
        if let Some(changes) = &transition.changes {
            let attempt = changes.attempt.as_ref().ok_or_else(|| {
                ContractError::Unavailable("claim transition omitted attempt".into())
            })?;
            db::insert_attempt(&mut tx, attempt).await?;
            db::apply(&mut tx, changes).await?;
        }
        let assignment = transition.cursor.assignment.as_ref();
        sqlx::query("UPDATE consumer_cursors SET sequence=($3::text)::ldg_u64,task_id=$4,attempt_id=$5 WHERE session_id=$1 AND consumer_id=$2")
            .bind(&session.id).bind(consumer).bind(acquire.sequence.to_string())
            .bind(assignment.map(|a|a.task_id.as_str())).bind(assignment.map(|a|a.attempt_id.as_str())).execute(&mut *tx).await?;
        let reply = ClaimReply {
            command: command.clone(),
            disposition,
        };
        reply.validate_reply_against(command)?;
        let mut saved = reply.clone();
        if let ClaimDisposition::Claimed {
            reply:
                AcquireReply::Assigned {
                    sequence,
                    assignment,
                },
        } = &reply.disposition
        {
            saved.disposition = ClaimDisposition::Claimed {
                reply: AcquireReply::OwnershipLost {
                    sequence: *sequence,
                    assignment: AttemptRef {
                        task_id: assignment.lease.owner.task_id.clone(),
                        attempt_id: assignment.lease.owner.attempt_id.clone(),
                    },
                },
            };
        }
        let bytes = serde_json::to_vec(&saved)
            .map_err(|_| ContractError::Unavailable("claim receipt encoding failed".into()))?;
        if bytes.len() > DISPATCH_MAX_BYTES {
            return Err(
                ContractError::Unavailable("claim receipt exceeds storage bound".into()).into(),
            );
        }
        sqlx::query("INSERT INTO dispatch_claim_receipts(session_id,consumer_id,sequence,task_id,reply_bytes) VALUES($1,$2,($3::text)::ldg_u64,$4,$5)")
            .bind(&session.id).bind(consumer).bind(acquire.sequence.to_string()).bind(&task.task_id).bind(bytes).execute(&mut *tx).await?;
        if let Some(span) = &producer {
            span.record("ledgence.publication.outcome", "commit_unconfirmed");
        }
        tx.commit().await?;
        if let Some(span) = &producer {
            span.record("ledgence.publication.outcome", "committed");
            span.record("otel.status_code", "OK");
        }
        self.acquisition_wake
            .publish(AcquisitionHint::AcquisitionCompleted(acquire.into()));
        Ok(reply)
    }

    async fn refresh_claim_replay(
        &self,
        tx: &mut sqlx::PgConnection,
        command: &ClaimCommand,
        saved: &mut ClaimReply,
    ) -> StoreResult<()> {
        let acquire = &command.acquisition;
        let row = sqlx::query("SELECT * FROM worker_sessions WHERE session_id=$1 FOR SHARE")
            .bind(&acquire.worker_session_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            return Ok(());
        };
        let session = codec::session(&row)?;
        if session.expires_at <= db::now(tx).await? {
            return Ok(());
        }
        let row = sqlx::query("SELECT *,trunc(sequence)::text AS sequence_text FROM consumer_cursors WHERE session_id=$1 AND consumer_id=$2 FOR UPDATE")
            .bind(&session.id).bind(i64::from(acquire.consumer_id)).fetch_optional(&mut *tx).await?;
        let cursor = row
            .as_ref()
            .map(|row| codec::cursor(row, &session))
            .transpose()?
            .flatten();
        let Some(cursor) = cursor else {
            return Ok(());
        };
        if cursor.command != *acquire {
            return Ok(());
        }
        let reference = match &saved.disposition {
            ClaimDisposition::Claimed {
                reply: AcquireReply::OwnershipLost { assignment, .. },
            } => assignment,
            _ => {
                return Err(ContractError::Unavailable(
                    "stored claim receipt contained cached authority".into(),
                )
                .into());
            }
        };
        if cursor.assignment.as_ref() != Some(reference) {
            return Ok(());
        }
        let (task, attempt) = db::previous(tx, &acquire.scope, reference).await?;
        let transition = core::acquire(
            core::Acquisition {
                session: Some(&session),
                cursor: Some(&cursor),
                previous: Some((&task, &attempt)),
                candidate: None,
                ids: None,
            },
            acquire,
            db::now(tx).await?,
        )?;
        saved.disposition = ClaimDisposition::Claimed {
            reply: transition.reply,
        };
        saved.validate_reply_against(command)?;
        Ok(())
    }
}
