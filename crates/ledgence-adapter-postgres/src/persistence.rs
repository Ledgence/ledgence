use crate::*;
use ledgence_orchestration_core::Transition;
use sqlx::PgConnection;

pub(crate) async fn now(connection: &mut PgConnection) -> StoreResult<u64> {
    let row = sqlx::query!(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS \"now!\""
    )
    .fetch_one(connection)
    .await?;
    u64::try_from(row.now).map_err(|_| {
        ContractError::Unavailable("database clock is outside supported range".into()).into()
    })
}
pub(crate) async fn id(connection: &mut PgConnection, prefix: &str) -> StoreResult<String> {
    let row = sqlx::query!("SELECT gen_random_uuid()::text AS \"id!\"")
        .fetch_one(connection)
        .await?;
    Ok(format!("{prefix}_{}", row.id))
}
pub(crate) async fn load_task(
    connection: &mut PgConnection,
    scope: &Scope,
    task_id: &str,
    locked: bool,
) -> StoreResult<TaskSnapshot> {
    let sql = if locked {
        "SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 FOR NO KEY UPDATE"
    } else {
        "SELECT * FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3"
    };
    let row = sqlx::query(sql)
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(task_id)
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    Ok(codec::task(&row)?)
}
pub(crate) async fn load_attempt(
    connection: &mut PgConnection,
    task: &TaskSnapshot,
    attempt_id: &str,
) -> StoreResult<AttemptSnapshot> {
    let row = sqlx::query(include_str!("../queries/attempt.sql"))
        .bind(&task.task_id)
        .bind(attempt_id)
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    Ok(codec::attempt(&row, task)?)
}

/// Old cursor context is one MVCC snapshot, never a write source. Column aliases
/// keep both records independently decodable without JSON numeric conversion.
pub(crate) async fn previous(
    connection: &mut PgConnection,
    scope: &Scope,
    reference: &AttemptRef,
) -> StoreResult<(TaskSnapshot, AttemptSnapshot)> {
    // Separate aliases preserve both records in one statement snapshot.
    let row = sqlx::query(include_str!("../queries/previous_assignment.sql"))
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(&reference.task_id)
        .bind(&reference.attempt_id)
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let task = codec::task(&row)?;
    let attempt = codec::attempt_prefixed(&row, &task, "a_")?;
    Ok((task, attempt))
}

pub(crate) async fn insert_attempt(
    connection: &mut PgConnection,
    a: &AttemptSnapshot,
) -> StoreResult<()> {
    let o = &a.lease.owner;
    let event = codec::encode(&a.event)?;
    let expires = codec::ms(a.lease.expires_at)?;
    let deadline = codec::ms(a.deadline)?;
    let horizon = codec::ms(a.authority_deadline)?;
    let generation = i64::from(o.generation);
    let consumer = i64::from(o.consumer_id);
    let source = a.event.value()["source"]
        .as_str()
        .ok_or_else(|| ContractError::Unavailable("stored event source missing".into()))?;
    let event_id = a.event.value()["id"]
        .as_str()
        .ok_or_else(|| ContractError::Unavailable("stored event ID missing".into()))?;
    sqlx::query!("INSERT INTO attempts(attempt_id,task_id,generation,lease_id,worker_session_id,consumer_id,event_source,event_id,event_bytes,expires_at_ms,deadline_ms,authority_deadline_ms,state,execution_may_have_started,quiescence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'active',false,'unconfirmed')",
        o.attempt_id,o.task_id,generation,o.lease_id,o.worker_session_id,consumer,source,event_id,event,expires,deadline,horizon)
        .execute(connection).await?;
    Ok(())
}

/// Caller holds the task lock; immutable fields and FK identity keys are never
/// rewritten. Historical receipt replay must preserve the current attempt index.
pub(crate) async fn apply<R>(
    connection: &mut PgConnection,
    transition: &Transition<R>,
) -> StoreResult<()> {
    if let Some(a) = &transition.attempt {
        let expires = codec::ms(a.lease.expires_at)?;
        let horizon = codec::ms(a.authority_deadline)?;
        let state = codec::label(&a.state)?;
        let quiet = codec::label(&a.quiescence)?;
        let finished = a.finished_at.map(codec::ms).transpose()?;
        let sequence = a.last_renewal.as_ref().map(|r| r.sequence.to_string());
        let intent = a
            .last_renewal
            .as_ref()
            .map(|r| codec::label(&r.intent))
            .transpose()?;
        sqlx::query!("UPDATE attempts SET expires_at_ms=$2, authority_deadline_ms=$3, state=$4, execution_may_have_started=$5, last_renew_sequence=($6::text)::ldg_u64, last_renew_intent=$7, quiescence=$8, finished_at_ms=$9 WHERE attempt_id=$1",a.lease.owner.attempt_id,expires,horizon,state,a.execution_may_have_started,sequence,intent,quiet,finished)
            .execute(&mut *connection).await?;
        if let Some(accepted) = &a.settlement {
            let bytes = codec::encode(&accepted.command)?;
            let at = codec::ms(accepted.receipt.accepted_at)?;
            sqlx::query!("INSERT INTO accepted_settlements(attempt_id,operation_id,accepted_command,accepted_at) VALUES($1,$2,$3,$4) ON CONFLICT(attempt_id) DO NOTHING",a.lease.owner.attempt_id,accepted.receipt.operation_id,bytes,at)
                .execute(&mut *connection).await?;
        }
    }
    let t = &transition.task;
    let state = codec::label(&t.state)?;
    let available = codec::ms(t.available_at)?;
    let terminal = t.terminal_at.map(codec::ms).transpose()?;
    let cancelled = t.cancel_requested_at.map(codec::ms).transpose()?;
    let count = i64::from(t.attempt_count);
    sqlx::query!("UPDATE tasks SET state=$2,available_at_ms=$3,terminal_at_ms=$4,current_attempt_id=$5,attempt_count=$6,cancel_requested_at_ms=$7,next_expiry_ms=(SELECT LEAST(expires_at_ms,authority_deadline_ms) FROM attempts WHERE attempt_id=$5 AND task_id=$1) WHERE task_id=$1",t.task_id,state,available,terminal,t.current_attempt_id,count,cancelled)
        .execute(&mut *connection).await?;
    history(connection, &transition.history).await
}

pub(crate) async fn history(
    connection: &mut PgConnection,
    events: &[HistoryEvent],
) -> StoreResult<()> {
    for e in events {
        let row = sqlx::query!("UPDATE tasks SET history_sequence=history_sequence+1 WHERE task_id=$1 RETURNING trunc(history_sequence)::text AS \"sequence!\"",e.task_id).fetch_one(&mut *connection).await?;
        let at = codec::ms(e.at)?;
        let reason = codec::label(&e.reason)?;
        sqlx::query!("INSERT INTO task_history(task_id,sequence,attempt_id,at_ms,reason) VALUES($1,($2::text)::ldg_u64,$3,$4,$5)",e.task_id,row.sequence,e.attempt_id,at,reason)
            .execute(&mut *connection).await?;
    }
    Ok(())
}

pub(crate) async fn read_snapshot(connection: &mut PgConnection) -> StoreResult<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(connection)
        .await?;
    Ok(())
}
