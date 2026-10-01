//! Consistent bounded console reads. Never mutates authority or scheduling.
mod executions;
mod explorer;
mod observations;
mod tasks;
#[cfg(test)]
mod tests;
mod workflows;
use crate::{persistence as db, *};
use ledgence_orchestration_api::console::*;
use serde::de::DeserializeOwned;
use sqlx::{PgConnection, Row, postgres::PgRow};

impl ConsoleQueryStore for PostgresStore {
    fn query_console<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(self.run(|| async {
            self.require_scope(scope)?;
            let position = query.validate(scope)?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_read().await?;
            let observed_at = db::now(&mut tx).await?;
            let reply = match query {
                ConsoleQuery::Explorer { .. }
                | ConsoleQuery::WorkflowInput { .. }
                | ConsoleQuery::Ancestry { .. } => {
                    explorer::query(&mut tx, scope, query, position.as_ref(), observed_at).await?
                }
                ConsoleQuery::AttemptObservations { attempt_id } => {
                    ConsoleQueryReply::AttemptObservations(
                        observations::attempt(&mut tx, scope, attempt_id, observed_at).await?,
                    )
                }
                ConsoleQuery::Executions { filters, page } => ConsoleQueryReply::Executions(
                    executions::list(
                        &mut tx,
                        scope,
                        query,
                        filters,
                        page,
                        position.as_ref(),
                        observed_at,
                    )
                    .await?,
                ),
                ConsoleQuery::Tasks { filters, page } => ConsoleQueryReply::Tasks(
                    tasks::list(
                        &mut tx,
                        scope,
                        query,
                        filters,
                        page,
                        position.as_ref(),
                        observed_at,
                    )
                    .await?,
                ),
                ConsoleQuery::Attempts { task_id, page } => ConsoleQueryReply::Attempts(
                    tasks::attempts(
                        &mut tx,
                        scope,
                        query,
                        task_id,
                        page,
                        position.as_ref(),
                        observed_at,
                    )
                    .await?,
                ),
                ConsoleQuery::Attempt { attempt_id } => ConsoleQueryReply::Attempt(
                    tasks::attempt(&mut tx, scope, attempt_id, observed_at).await?,
                ),
                _ => {
                    workflows::query(&mut tx, scope, query, position.as_ref(), observed_at).await?
                }
            };
            reply.validate(scope, query)?;
            tx.commit().await?;
            Ok(reply)
        }))
    }
}
fn corrupt(label: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid console persistence: {label}"))
}
fn number(row: &PgRow, key: &str) -> StoreResult<u64> {
    u64::try_from(row.try_get::<i64, _>(key)?).map_err(|_| corrupt(key).into())
}
fn optional_number(row: &PgRow, key: &str) -> StoreResult<Option<u64>> {
    row.try_get::<Option<i64>, _>(key)?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| corrupt(key).into())
}
fn unsigned(row: &PgRow, key: &str) -> StoreResult<ConsoleU64> {
    Ok(ConsoleU64(
        row.try_get::<String, _>(key)?
            .parse()
            .map_err(|_| corrupt(key))?,
    ))
}
fn enumeration<T: DeserializeOwned>(row: &PgRow, key: &str) -> StoreResult<T> {
    serde_json::from_value(serde_json::Value::String(row.try_get(key)?))
        .map_err(|_| corrupt(key).into())
}
fn descriptor(row: &PgRow, key: &str) -> StoreResult<ConsoleProgramDescriptor> {
    let bytes = row
        .try_get::<Option<Vec<u8>>, _>(key)?
        .ok_or_else(|| corrupt("descriptor size"))?;
    let value: ledgence_worker_api::ProgramDescriptor =
        decode_unique_json(&bytes, 4096).map_err(|_| corrupt("descriptor"))?;
    value.validate().map_err(|_| corrupt("descriptor"))?;
    Ok(value.into())
}
fn position_number(position: &ConsolePosition, index: usize) -> Result<u64> {
    match position.get(index) {
        Some(ConsoleKey::Number(n)) => Ok(n.0),
        _ => Err(ContractError::InvalidInput(
            "invalid numeric cursor position".into(),
        )),
    }
}
fn position_text(position: &ConsolePosition, index: usize) -> Result<String> {
    match position.get(index) {
        Some(ConsoleKey::Text(s)) => Ok(s.clone()),
        _ => Err(ContractError::InvalidInput(
            "invalid text cursor position".into(),
        )),
    }
}
fn position_time(position: &ConsolePosition, index: usize) -> Result<i64> {
    let n = position_number(position, index)?;
    if n > CONSOLE_MAX_TIMESTAMP {
        return Err(ContractError::InvalidInput(
            "cursor timestamp outside supported range".into(),
        ));
    }
    Ok(n as i64)
}
fn page<T: ConsoleRecord>(
    mut items: Vec<T>,
    scope: &Scope,
    query: &ConsoleQuery,
    request: &ConsolePagination,
    observed_at: u64,
) -> StoreResult<ConsolePage<T>> {
    let more = items.len() > request.limit as usize;
    items.truncate(request.limit as usize);
    let next_cursor = if more {
        Some(
            request.next_cursor(
                &query.binding(scope)?,
                &items
                    .last()
                    .ok_or_else(|| corrupt("empty page"))?
                    .position(),
            )?,
        )
    } else {
        None
    };
    Ok(ConsolePage {
        items,
        next_cursor,
        observed_at,
    })
}
async fn require_task(connection: &mut PgConnection, scope: &Scope, id: &str) -> StoreResult<()> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND retiring_at_ms IS NULL)")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_one(connection).await?;
    if !exists {
        return Err(ContractError::NotFound.into());
    }
    Ok(())
}
