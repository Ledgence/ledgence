//! Latest-only worker observations. Never changes sessions, leases or tasks.

use crate::{persistence as db, *};
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::WorkerObservationDetailState;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, postgres::PgRow};
use std::collections::HashMap;

const SUMMARY_COLUMNS: &str = "s.session_id,s.tenant_id,s.namespace,s.queue,s.concurrency,s.expires_at_ms,\
    trunc(o.snapshot_sequence)::text AS sequence_text,o.received_at_ms,o.display_name,o.accepting,\
    o.active_consumers,o.occupied_process_slots,o.detail_state";

impl WorkerObservationStore for PostgresStore {
    fn record_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt> {
        Box::pin(async move {
            command.validate()?;
            self.require_session_scope(&command.scope)?;
            // Normalize before taking session/database locks, including retries.
            let bytes = command.normalized_bytes()?;
            self.run(|| record(self, command, &bytes)).await
        })
    }

    fn query_workers<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a WorkerObservationQuery,
    ) -> ContractFuture<'a, WorkerObservationReply> {
        Box::pin(self.run(|| read_workers(self, scope, query)))
    }
}

async fn record(
    store: &PostgresStore,
    command: &WorkerObservationCommand,
    bytes: &[u8],
) -> StoreResult<WorkerObservationReceipt> {
    let mut connection = store.transaction_connection().await?;
    let mut tx = connection.begin_write().await?;
    let row = sqlx::query("SELECT * FROM worker_sessions WHERE session_id=$1 FOR UPDATE")
        .bind(&command.worker_session_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ContractError::UnknownSession)?;
    let session = codec::session(&row)?;
    store.require_session_scope(&session.scope)?;
    if session.scope != command.scope {
        return Err(ContractError::UnknownSession.into());
    }
    let now = db::now(&mut tx).await?;
    if now >= session.expires_at {
        return Err(ContractError::UnknownSession.into());
    }
    if command.snapshot.configured_concurrency != session.concurrency as usize {
        return Err(ContractError::InvalidInput(
            "worker observation capacity differs from session".into(),
        )
        .into());
    }
    let prior = sqlx::query("SELECT trunc(snapshot_sequence)::text AS sequence_text,received_at_ms,snapshot_bytes FROM worker_observations WHERE session_id=$1")
        .bind(&session.id).fetch_optional(&mut *tx).await?;
    if let Some(prior) = prior {
        let sequence = counter(prior.try_get("sequence_text")?)?;
        if command.sequence.0 < sequence {
            return Err(ContractError::Conflict.into());
        }
        if command.sequence.0 == sequence {
            if prior.try_get::<Vec<u8>, _>("snapshot_bytes")? != bytes {
                return Err(ContractError::Conflict.into());
            }
            let received_at = time(prior.try_get("received_at_ms")?)?;
            tx.commit().await?;
            return Ok(WorkerObservationReceipt {
                worker_session_id: session.id,
                sequence: command.sequence,
                received_at,
                already_received: true,
            });
        }
    }
    let detail = codec::label(&command.snapshot.detail_state)?;
    sqlx::query(concat!(
        "INSERT INTO worker_observations(session_id,snapshot_sequence,received_at_ms,display_name,",
        "accepting,active_consumers,occupied_process_slots,detail_state,snapshot_bytes) ",
        "VALUES($1,($2::text)::ldg_u64,$3,$4,$5,$6,$7,$8,$9) ",
        "ON CONFLICT(session_id) DO UPDATE SET snapshot_sequence=excluded.snapshot_sequence,",
        "received_at_ms=excluded.received_at_ms,display_name=excluded.display_name,",
        "accepting=excluded.accepting,active_consumers=excluded.active_consumers,",
        "occupied_process_slots=excluded.occupied_process_slots,",
        "detail_state=excluded.detail_state,snapshot_bytes=excluded.snapshot_bytes"
    ))
    .bind(&session.id)
    .bind(command.sequence.0.to_string())
    .bind(codec::ms(now)?)
    .bind(&command.display_name)
    .bind(command.snapshot.accepting)
    .bind(command.snapshot.active_consumers as i64)
    .bind(command.snapshot.occupied_process_slots as i64)
    .bind(detail)
    .bind(bytes)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(WorkerObservationReceipt {
        worker_session_id: session.id,
        sequence: command.sequence,
        received_at: now,
        already_received: false,
    })
}

async fn read_workers(
    store: &PostgresStore,
    scope: &Scope,
    query: &WorkerObservationQuery,
) -> StoreResult<WorkerObservationReply> {
    store.require_scope(scope)?;
    let position = query.validate(scope)?;
    let binding = query.binding(scope)?;
    let mut connection = store.transaction_connection().await?;
    // Summary, snapshot and validated attempt links come from one database view.
    let mut tx = connection.begin_read().await?;
    let now = db::now(&mut tx).await?;
    let reply = match query {
        WorkerObservationQuery::Workers { queue, page } => {
            let mut sql = QueryBuilder::<Postgres>::new("SELECT ");
            sql.push(SUMMARY_COLUMNS)
                .push(" FROM worker_sessions s LEFT JOIN worker_observations o ON o.session_id=s.session_id WHERE s.tenant_id=")
                .push_bind(&scope.tenant_id)
                .push(" AND s.namespace=").push_bind(&scope.namespace);
            if let Some(queue) = queue {
                sql.push(" AND s.queue=").push_bind(queue);
            }
            if let Some(position) = &position {
                let [ConsoleKey::Text(id)] = position.as_slice() else {
                    return Err(corrupt().into());
                };
                sql.push(" AND s.session_id>").push_bind(id);
            }
            sql.push(" ORDER BY s.session_id LIMIT ")
                .push_bind(i64::from(page.limit) + 1);
            let rows = sql.build().fetch_all(&mut *tx).await?;
            let items = rows
                .iter()
                .map(|row| summary(row, now))
                .collect::<StoreResult<Vec<_>>>()?;
            WorkerObservationReply::Workers(make_page(items, page, &binding, now)?)
        }
        WorkerObservationQuery::Inspect {
            worker_session_id,
            page,
        } => {
            let mut sql = QueryBuilder::<Postgres>::new("SELECT ");
            sql.push(SUMMARY_COLUMNS)
                .push(",o.snapshot_bytes FROM worker_sessions s LEFT JOIN worker_observations o ON o.session_id=s.session_id WHERE s.tenant_id=")
                .push_bind(&scope.tenant_id)
                .push(" AND s.namespace=").push_bind(&scope.namespace)
                .push(" AND s.session_id=").push_bind(worker_session_id);
            let row = sql
                .build()
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(ContractError::NotFound)?;
            let worker = summary(&row, now)?;
            let after = if let Some(position) = position {
                let [ConsoleKey::Number(id)] = position.as_slice() else {
                    return Err(corrupt().into());
                };
                Some(id.0)
            } else {
                None
            };
            let items = if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("snapshot_bytes")? {
                let command: WorkerObservationCommand = codec::decode(&bytes)?;
                validate_snapshot(&command, scope, &worker)?;
                let mut selected = command
                    .snapshot
                    .slots
                    .into_iter()
                    .filter(|slot| after.is_none_or(|after| (slot.slot_id as u64) > after))
                    .collect::<Vec<_>>();
                selected.sort_by_key(|slot| slot.slot_id);
                selected.truncate(page.limit as usize + 1);
                let links = validated_links(&mut tx, scope, worker_session_id, &selected).await?;
                selected
                    .into_iter()
                    .map(|slot| public_slot(slot, &links))
                    .collect()
            } else {
                Vec::new()
            };
            WorkerObservationReply::Inspect(Box::new(ConsoleWorkerDetail {
                worker,
                slots: make_page(items, page, &binding, now)?,
            }))
        }
    };
    reply.validate(scope, query)?;
    tx.commit().await?;
    Ok(reply)
}

fn validate_snapshot(
    command: &WorkerObservationCommand,
    scope: &Scope,
    worker: &ConsoleWorkerSummary,
) -> Result<()> {
    command.validate().map_err(|_| corrupt())?;
    if command.worker_session_id != worker.worker_session_id
        || command.scope != *scope
        || command.snapshot.configured_concurrency != worker.capacity as usize
        || Some(command.sequence) != worker.snapshot_sequence
        || Some(command.snapshot.accepting) != worker.accepting
        || Some(command.snapshot.active_consumers as u32) != worker.active_consumers
        || Some(command.snapshot.occupied_process_slots as u32) != worker.occupied_process_slots
        || Some(command.snapshot.detail_state) != worker.detail_state
        || command.display_name != worker.display_name
    {
        return Err(corrupt());
    }
    Ok(())
}

fn public_slot(
    slot: ledgence_worker_api::SlotObservation,
    links: &HashMap<String, (String, u32)>,
) -> ConsoleWorkerSlot {
    let mut task_id = None;
    let mut attempt_id = None;
    let mut consumer_id = None;
    let mut link_diagnostic = None;
    if let Some(invocation) = slot.invocation {
        match links.get(&invocation.attempt_id) {
            Some((task, consumer)) if task == &invocation.task_id => {
                task_id = Some(invocation.task_id);
                attempt_id = Some(invocation.attempt_id);
                consumer_id = Some(*consumer);
            }
            _ => link_diagnostic = Some(WorkerLinkDiagnostic::AuthorityMismatch),
        }
    }
    ConsoleWorkerSlot {
        slot_id: slot.slot_id as u32,
        state: slot.state,
        process_instance_id: slot.process_instance_id,
        process_id: slot.process_id,
        program: slot.program,
        digest: slot.digest,
        task_id,
        attempt_id,
        consumer_id,
        link_diagnostic,
    }
}

fn make_page<T: ConsoleRecord>(
    mut items: Vec<T>,
    page: &ConsolePagination,
    binding: &ConsoleCursorBinding,
    now: u64,
) -> Result<ConsolePage<T>> {
    let more = items.len() > page.limit as usize;
    items.truncate(page.limit as usize);
    let next_cursor = if more {
        Some(page.next_cursor(binding, &items.last().ok_or_else(corrupt)?.position())?)
    } else {
        None
    };
    Ok(ConsolePage {
        items,
        next_cursor,
        observed_at: now,
    })
}
fn corrupt() -> ContractError {
    ContractError::Unavailable("invalid worker observation record".into())
}
fn counter(value: String) -> Result<u64> {
    value.parse().map_err(|_| corrupt())
}
fn time(value: i64) -> Result<u64> {
    let value = u64::try_from(value).map_err(|_| corrupt())?;
    if value > CONSOLE_MAX_TIMESTAMP {
        return Err(corrupt());
    }
    Ok(value)
}
fn count(value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|_| corrupt())
}
fn summary(row: &PgRow, now: u64) -> StoreResult<ConsoleWorkerSummary> {
    let received_at = row
        .try_get::<Option<i64>, _>("received_at_ms")?
        .map(time)
        .transpose()?;
    let session_expires_at = time(row.try_get("expires_at_ms")?)?;
    let freshness = match received_at {
        None => WorkerObservationFreshness::Unavailable,
        Some(at) => match now.checked_sub(at).ok_or_else(corrupt)? {
            0..=15000 => WorkerObservationFreshness::Fresh,
            15001..=60000 => WorkerObservationFreshness::Stale,
            _ => WorkerObservationFreshness::NoRecentReport,
        },
    };
    let detail_state = row
        .try_get::<Option<String>, _>("detail_state")?
        .map(|value| {
            serde_json::from_value::<WorkerObservationDetailState>(serde_json::Value::String(value))
                .map_err(|_| corrupt())
        })
        .transpose()?;
    let worker = ConsoleWorkerSummary {
        worker_session_id: row.try_get("session_id")?,
        display_name: row.try_get("display_name")?,
        queue: row.try_get("queue")?,
        capacity: count(row.try_get("concurrency")?)?,
        session_expires_at,
        session_expired: session_expires_at <= now,
        snapshot_sequence: row
            .try_get::<Option<String>, _>("sequence_text")?
            .map(counter)
            .transpose()?
            .map(ConsoleU64),
        received_at,
        accepting: row.try_get("accepting")?,
        active_consumers: row
            .try_get::<Option<i64>, _>("active_consumers")?
            .map(count)
            .transpose()?,
        occupied_process_slots: row
            .try_get::<Option<i64>, _>("occupied_process_slots")?
            .map(count)
            .transpose()?,
        detail_state,
        freshness,
    };
    worker.validate_at(now).map_err(|_| corrupt())?;
    Ok(worker)
}
async fn validated_links(
    connection: &mut PgConnection,
    scope: &Scope,
    session: &str,
    slots: &[ledgence_worker_api::SlotObservation],
) -> StoreResult<HashMap<String, (String, u32)>> {
    let attempts = slots
        .iter()
        .filter_map(|slot| slot.invocation.as_ref().map(|v| v.attempt_id.clone()))
        .collect::<Vec<_>>();
    if attempts.is_empty() {
        return Ok(HashMap::new());
    }
    let rows=sqlx::query("SELECT a.attempt_id,a.task_id,a.consumer_id FROM attempts a JOIN tasks t ON t.task_id=a.task_id WHERE a.attempt_id=ANY($1) AND a.worker_session_id=$2 AND t.tenant_id=$3 AND t.namespace=$4 AND t.retiring_at_ms IS NULL")
        .bind(attempts).bind(session).bind(&scope.tenant_id).bind(&scope.namespace).fetch_all(connection).await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("attempt_id")?,
                (row.try_get("task_id")?, count(row.try_get("consumer_id")?)?),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests;
