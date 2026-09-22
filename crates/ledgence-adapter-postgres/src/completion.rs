//! Opt-in terminal notifications. Target mutation -> subscription is the only
//! lock direction; dispatchers never acquire a task or workflow mutation lock.
use crate::{persistence as db, *};
use ledgence_orchestration_core as core;
use sqlx::{PgConnection, Row, postgres::PgRow};

impl CompletionStore for PostgresStore {
    fn configure_completion_destination<'a>(
        &'a self,
        destination: &'a CompletionDestination,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run(move || self.configure_completion_destination_once(destination)))
    }

    fn subscribe_completion<'a>(
        &'a self,
        command: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(self.run(move || self.subscribe_completion_once(command)))
    }

    fn completion_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(self.run(move || async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let row = sqlx::query("SELECT * FROM completion_subscriptions WHERE tenant_id=$1 AND namespace=$2 AND subscription_id=$3")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(id)
                .fetch_optional(&self.pool).await?.ok_or(ContractError::NotFound)?;
            subscription(&row)
        }))
    }

    fn retry_completion<'a>(
        &'a self,
        command: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(self.run(move || self.retry_completion_once(command)))
    }

    fn lease_completions<'a>(
        &'a self,
        destination: &'a CompletionDestination,
        limit: u32,
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<CompletionLease>> {
        Box::pin(self.run_until(deadline, move || {
            self.lease_completions_once(destination, limit)
        }))
    }

    fn complete_deliveries<'a>(
        &'a self,
        results: &'a [CompletionDeliveryResult],
        deadline: Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run_until(deadline, move || self.complete_deliveries_once(results)))
    }
}

impl PostgresStore {
    async fn configure_completion_destination_once(
        &self,
        destination: &CompletionDestination,
    ) -> StoreResult<()> {
        destination.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        sqlx::query("INSERT INTO completion_destinations(tenant_id,namespace,destination,binding) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(&destination.scope.tenant_id).bind(&destination.scope.namespace)
            .bind(&destination.destination).bind(&destination.binding)
            .execute(&mut *tx).await?;
        check_destination(&mut tx, destination).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn subscribe_completion_once(
        &self,
        command: &CompletionSubscribeCommand,
    ) -> StoreResult<CompletionSubscription> {
        command.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        // Serialize registration with terminalization, including registrations
        // after completion. This also makes the per-execution limit race-free.
        let event = match &command.target {
            CompletionTarget::Task { id } => {
                let task = db::load_task(&mut tx, &command.scope, id, true).await?;
                task.state
                    .is_terminal()
                    .then(|| core::task_completion_event(&task))
                    .transpose()?
            }
            CompletionTarget::Workflow { id } => {
                let (snapshot, trace) =
                    crate::workflow::lock_completion_context(&mut tx, &command.scope, id).await?;
                snapshot
                    .state
                    .is_terminal()
                    .then(|| core::workflow_completion_event(&snapshot, trace.as_ref()))
                    .transpose()?
            }
        };
        let (task, workflow) = target_columns(&command.target);
        let lookup = match command.target {
            CompletionTarget::Task { .. } => {
                "SELECT * FROM completion_subscriptions WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND idempotency_key=$4"
            }
            CompletionTarget::Workflow { .. } => {
                "SELECT * FROM completion_subscriptions WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND idempotency_key=$4"
            }
        };
        let existing = sqlx::query(lookup)
            .bind(&command.scope.tenant_id)
            .bind(&command.scope.namespace)
            .bind(command.target.id())
            .bind(&command.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(row) = existing {
            let accepted = subscription(&row)?;
            if accepted.command != *command {
                return Err(ContractError::Conflict.into());
            }
            tx.commit().await?;
            return Ok(accepted);
        }
        let destination_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM completion_destinations WHERE tenant_id=$1 AND namespace=$2 AND destination=$3)")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.destination)
            .fetch_one(&mut *tx).await?;
        if !destination_exists {
            return Err(ContractError::InvalidInput(
                "completion destination is not configured".into(),
            )
            .into());
        }
        let count_query = match command.target {
            CompletionTarget::Task { .. } => {
                "SELECT count(*) FROM completion_subscriptions WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3"
            }
            CompletionTarget::Workflow { .. } => {
                "SELECT count(*) FROM completion_subscriptions WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3"
            }
        };
        let count: i64 = sqlx::query_scalar(count_query)
            .bind(&command.scope.tenant_id)
            .bind(&command.scope.namespace)
            .bind(command.target.id())
            .fetch_one(&mut *tx)
            .await?;
        if count >= i64::from(MAX_COMPLETION_SUBSCRIPTIONS) {
            return Err(ContractError::InvalidInput(
                "execution has too many completion subscriptions".into(),
            )
            .into());
        }
        let id = db::id(&mut tx, "sub").await?;
        let now = db::now(&mut tx).await?;
        let row = sqlx::query("INSERT INTO completion_subscriptions(subscription_id,tenant_id,namespace,task_id,workflow_id,destination,idempotency_key,command_bytes,state,created_at_ms,activated_at_ms,next_attempt_at_ms,event_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11,$12) RETURNING *")
            .bind(&id).bind(&command.scope.tenant_id).bind(&command.scope.namespace)
            .bind(task).bind(workflow).bind(&command.destination).bind(&command.idempotency_key)
            .bind(codec::encode(command)?).bind(if event.is_some() { "pending" } else { "waiting" })
            .bind(codec::ms(now)?).bind(event.as_ref().map(|_| codec::ms(now)).transpose()?)
            .bind(event.as_ref().map(codec::encode).transpose()?)
            .fetch_one(&mut *tx).await?;
        let result = subscription(&row)?;
        tx.commit().await?;
        Ok(result)
    }

    async fn retry_completion_once(
        &self,
        command: &CompletionRetryCommand,
    ) -> StoreResult<CompletionSubscription> {
        command.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let row = sqlx::query("SELECT * FROM completion_subscriptions WHERE tenant_id=$1 AND namespace=$2 AND subscription_id=$3 FOR UPDATE")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.subscription_id)
            .fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        let current = subscription(&row)?;
        if command.expected_generation < current.generation {
            tx.commit().await?;
            return Ok(current);
        }
        if command.expected_generation != current.generation
            || current.state != CompletionState::Exhausted
            || current.generation >= 1000
        {
            return Err(ContractError::Conflict.into());
        }
        let now = db::now(&mut tx).await?;
        let row = sqlx::query("UPDATE completion_subscriptions SET state='pending',generation=generation+1,attempts=0,next_attempt_at_ms=$2,exhausted_at_ms=NULL,last_failure=NULL WHERE subscription_id=$1 RETURNING *")
            .bind(&command.subscription_id).bind(codec::ms(now)?).fetch_one(&mut *tx).await?;
        let result = subscription(&row)?;
        tx.commit().await?;
        Ok(result)
    }

    async fn lease_completions_once(
        &self,
        destination: &CompletionDestination,
        limit: u32,
    ) -> StoreResult<Vec<CompletionLease>> {
        destination.validate()?;
        if !(1..=MAX_COMPLETION_BATCH).contains(&limit) {
            return Err(
                ContractError::InvalidInput("completion batch must be 1..=16".into()).into(),
            );
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        check_destination(&mut tx, destination).await?;
        let now = db::now(&mut tx).await?;
        let until = codec::ms(
            now.checked_add(COMPLETION_LEASE_MS)
                .ok_or_else(|| corrupt("lease overflow"))?,
        )?;
        // Only delivery rows are locked, never target rows. Expired last leases
        // exhaust durably instead of becoming permanently stuck in delivering.
        let rows = sqlx::query(include_str!("../queries/completion_leases.sql"))
            .bind(&destination.scope.tenant_id)
            .bind(&destination.scope.namespace)
            .bind(&destination.destination)
            .bind(codec::ms(now)?)
            .bind(i64::from(limit))
            .bind(until)
            .fetch_all(&mut *tx)
            .await?;
        let mut leases = Vec::with_capacity(rows.len());
        for row in rows {
            let record = subscription(&row)?;
            if record.state == CompletionState::Delivering {
                leases.push(CompletionLease {
                    subscription: record,
                    lease_token: row.try_get("lease_token")?,
                    event_bytes: row.try_get("event_bytes")?,
                });
            }
        }
        tx.commit().await?;
        Ok(leases)
    }

    async fn complete_deliveries_once(
        &self,
        results: &[CompletionDeliveryResult],
    ) -> StoreResult<()> {
        if results.len() > MAX_COMPLETION_BATCH as usize {
            return Err(ContractError::InvalidInput("completion batch exceeds 16".into()).into());
        }
        let mut ids = std::collections::HashSet::new();
        for result in results {
            result.validate()?;
            if !ids.insert(&result.subscription_id) {
                return Err(ContractError::InvalidInput(
                    "duplicate completion delivery outcome".into(),
                )
                .into());
            }
        }
        if results.is_empty() {
            return Ok(());
        }
        let mut ordered: Vec<_> = results.iter().collect();
        ordered.sort_by(|a, b| a.subscription_id.cmp(&b.subscription_id));
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        for result in ordered {
            let row = sqlx::query(
                "SELECT * FROM completion_subscriptions WHERE subscription_id=$1 FOR UPDATE",
            )
            .bind(&result.subscription_id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(row) = row else {
                continue;
            };
            let now = db::now(&mut tx).await?;
            if row.try_get::<String, _>("state")? != "delivering"
                || row.try_get::<i64, _>("generation")? != i64::from(result.generation)
                || row.try_get::<Option<String>, _>("lease_token")?.as_deref()
                    != Some(&result.lease_token)
                || row
                    .try_get::<Option<i64>, _>("lease_until_ms")?
                    .is_none_or(|at| at <= now as i64)
            {
                continue;
            }
            let attempts = row.try_get::<i64, _>("attempts")?;
            let (state, next, delivered, exhausted, failure) = match &result.outcome {
                CompletionDeliveryOutcome::Confirmed => {
                    ("delivered", None, Some(codec::ms(now)?), None, None)
                }
                CompletionDeliveryOutcome::Retry {
                    reason,
                    retry_after_ms,
                } => {
                    if attempts >= i64::from(COMPLETION_MAX_ATTEMPTS) {
                        (
                            "exhausted",
                            None,
                            None,
                            Some(codec::ms(now)?),
                            Some(reason.as_str()),
                        )
                    } else {
                        let delay = core::completion_retry_delay_ms(
                            &result.subscription_id,
                            attempts as u32,
                        )
                        .max(
                            retry_after_ms
                                .unwrap_or_default()
                                .min(COMPLETION_MAX_RETRY_DELAY_MS),
                        );
                        let next = codec::ms(
                            now.checked_add(delay)
                                .ok_or_else(|| corrupt("retry timestamp overflow"))?,
                        )?;
                        ("retrying", Some(next), None, None, Some(reason.as_str()))
                    }
                }
            };
            sqlx::query("UPDATE completion_subscriptions SET state=$2,next_attempt_at_ms=$3,lease_token=NULL,lease_until_ms=NULL,delivered_at_ms=$4,exhausted_at_ms=$5,last_failure=$6 WHERE subscription_id=$1")
                .bind(&result.subscription_id).bind(state).bind(next).bind(delivered).bind(exhausted).bind(failure)
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

async fn check_destination(
    connection: &mut PgConnection,
    destination: &CompletionDestination,
) -> StoreResult<()> {
    let binding: Option<String> = sqlx::query_scalar("SELECT binding FROM completion_destinations WHERE tenant_id=$1 AND namespace=$2 AND destination=$3")
        .bind(&destination.scope.tenant_id).bind(&destination.scope.namespace).bind(&destination.destination)
        .fetch_optional(connection).await?;
    if binding.as_deref() != Some(&destination.binding) {
        return Err(ContractError::Conflict.into());
    }
    Ok(())
}

fn target_columns(target: &CompletionTarget) -> (Option<&str>, Option<&str>) {
    match target {
        CompletionTarget::Task { id } => (Some(id), None),
        CompletionTarget::Workflow { id } => (None, Some(id)),
    }
}

fn corrupt(label: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid completion persistence: {label}"))
}

fn timestamp(row: &PgRow, name: &str) -> StoreResult<Option<u64>> {
    row.try_get::<Option<i64>, _>(name)?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| corrupt("negative timestamp").into())
}

fn subscription(row: &PgRow) -> StoreResult<CompletionSubscription> {
    let command: CompletionSubscribeCommand =
        codec::decode(&row.try_get::<Vec<u8>, _>("command_bytes")?)?;
    let (task, workflow) = target_columns(&command.target);
    if row.try_get::<String, _>("tenant_id")? != command.scope.tenant_id
        || row.try_get::<String, _>("namespace")? != command.scope.namespace
        || row.try_get::<Option<String>, _>("task_id")?.as_deref() != task
        || row.try_get::<Option<String>, _>("workflow_id")?.as_deref() != workflow
        || row.try_get::<String, _>("destination")? != command.destination
        || row.try_get::<String, _>("idempotency_key")? != command.idempotency_key
    {
        return Err(corrupt("subscription identity").into());
    }
    let value = CompletionSubscription {
        subscription_id: row.try_get("subscription_id")?,
        command,
        state: serde_json::from_value(serde_json::Value::String(row.try_get("state")?))
            .map_err(|_| corrupt("state"))?,
        generation: u32::try_from(row.try_get::<i64, _>("generation")?)
            .map_err(|_| corrupt("generation"))?,
        attempts: u32::try_from(row.try_get::<i64, _>("attempts")?)
            .map_err(|_| corrupt("attempt count"))?,
        total_attempts: u64::try_from(row.try_get::<i64, _>("total_attempts")?)
            .map_err(|_| corrupt("total attempts"))?,
        created_at: timestamp(row, "created_at_ms")?.ok_or_else(|| corrupt("created timestamp"))?,
        activated_at: timestamp(row, "activated_at_ms")?,
        next_attempt_at: timestamp(row, "next_attempt_at_ms")?,
        lease_expires_at: timestamp(row, "lease_until_ms")?,
        delivered_at: timestamp(row, "delivered_at_ms")?,
        exhausted_at: timestamp(row, "exhausted_at_ms")?,
        last_failure: row.try_get("last_failure")?,
        event: row
            .try_get::<Option<Vec<u8>>, _>("event_bytes")?
            .map(|bytes| codec::decode(&bytes))
            .transpose()?,
    };
    value.validate().map_err(|_| corrupt("subscription"))?;
    Ok(value)
}

/// Called while the task mutation lock is held, inside its terminal transaction.
pub(crate) async fn task_terminal(
    connection: &mut PgConnection,
    task: &TaskSnapshot,
) -> StoreResult<()> {
    let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM completion_subscriptions WHERE task_id=$1 AND state='waiting')")
        .bind(&task.task_id).fetch_one(&mut *connection).await?;
    if waiting {
        activate(
            connection,
            &CompletionTarget::Task {
                id: task.task_id.clone(),
            },
            &core::task_completion_event(task)?,
        )
        .await?;
    }
    Ok(())
}

/// Workflow terminalization is distinct from completion of its controller task.
pub(crate) async fn workflow_terminal(
    connection: &mut PgConnection,
    snapshot: &WorkflowSnapshot,
    origin_trace: Option<&TraceContext>,
) -> StoreResult<()> {
    let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM completion_subscriptions WHERE workflow_id=$1 AND state='waiting')")
        .bind(&snapshot.workflow_id).fetch_one(&mut *connection).await?;
    if waiting {
        activate(
            connection,
            &CompletionTarget::Workflow {
                id: snapshot.workflow_id.clone(),
            },
            &core::workflow_completion_event(snapshot, origin_trace)?,
        )
        .await?;
    }
    Ok(())
}

async fn activate(
    connection: &mut PgConnection,
    target: &CompletionTarget,
    event: &CompletionEvent,
) -> StoreResult<()> {
    let sql = match target {
        CompletionTarget::Task { .. } => {
            "UPDATE completion_subscriptions SET state='pending',activated_at_ms=$2,next_attempt_at_ms=$2,event_bytes=$3 WHERE task_id=$1 AND state='waiting'"
        }
        CompletionTarget::Workflow { .. } => {
            "UPDATE completion_subscriptions SET state='pending',activated_at_ms=$2,next_attempt_at_ms=$2,event_bytes=$3 WHERE workflow_id=$1 AND state='waiting'"
        }
    };
    let now = db::now(connection).await?;
    // A resource has at most 16 subscriptions, enforced under its target lock.
    // Terminal replays leave previously activated/delivered obligations intact.
    sqlx::query(sql)
        .bind(target.id())
        .bind(codec::ms(now)?)
        .bind(codec::encode(event)?)
        .execute(connection)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
