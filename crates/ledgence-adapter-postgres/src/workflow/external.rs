//! One-shot inbox receipts and delayed work share the workflow mutation lock.
//! Neither early events nor dormant timers hold a connection or a worker slot.
use super::*;

impl PostgresStore {
    pub(super) async fn send_workflow_event_once(
        &self,
        command: &WorkflowEventCommand,
    ) -> StoreResult<WorkflowEventReceipt> {
        command.validate()?;
        let bytes = codec::encode(&command.event)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        // Read only coordination metadata, not the workflow's program, input or
        // checkpoint. This lock linearizes acceptance with wait installation,
        // winner selection, cancellation and other senders for this workflow.
        let row = sqlx::query("SELECT state,external_wait_key,wait_activation_id,pending_event_count,pending_event_bytes,retiring_at_ms FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR NO KEY UPDATE")
            .bind(&command.scope.tenant_id).bind(&command.scope.namespace).bind(&command.workflow_id)
            .fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        codec::visible(&row)?;
        let previous = sqlx::query("SELECT event_key,event_source,event_id,event_bytes,accepted_at_ms FROM workflow_events WHERE workflow_id=$1 AND (event_key=$2 OR (event_source=$3 AND event_id=$4))")
            .bind(&command.workflow_id).bind(&command.key).bind(command.event.source()).bind(command.event.id())
            .fetch_all(&mut *tx).await?;
        if !previous.is_empty() {
            if previous.len() != 1 {
                return Err(ContractError::Conflict.into());
            }
            let receipt = &previous[0];
            if receipt.try_get::<String, _>("event_key")? != command.key
                || receipt.try_get::<String, _>("event_source")? != command.event.source()
                || receipt.try_get::<String, _>("event_id")? != command.event.id()
                || receipt.try_get::<Vec<u8>, _>("event_bytes")? != bytes
            {
                return Err(ContractError::Conflict.into());
            }
            let at = timestamp(receipt.try_get("accepted_at_ms")?)?;
            tx.commit().await?;
            return Ok(event_receipt(command, at, true));
        }
        if !matches!(
            row.try_get::<String, _>("state")?.as_str(),
            "running" | "waiting"
        ) {
            return Err(ContractError::ObsoleteOperation.into());
        }
        // Store time is sampled after obtaining authority. At the exact
        // persisted deadline timeout wins, even if its coordinator is delayed.
        let now = db::now(&mut tx).await?;
        let wait = sqlx::query("SELECT kind,deadline_ms,closed_at_ms FROM workflow_waits WHERE workflow_id=$1 AND wait_key=$2")
            .bind(&command.workflow_id).bind(&command.key).fetch_optional(&mut *tx).await?;
        if let Some(wait) = wait {
            let deadline = wait
                .try_get::<Option<i64>, _>("deadline_ms")?
                .map(timestamp)
                .transpose()?;
            if wait.try_get::<String, _>("kind")? != "event"
                || wait.try_get::<Option<i64>, _>("closed_at_ms")?.is_some()
                || deadline.is_some_and(|deadline| now >= deadline)
            {
                return Err(ContractError::ObsoleteOperation.into());
            }
        }
        if row.try_get::<i32, _>("pending_event_count")? as usize >= WORKFLOW_MAX_PENDING_EVENTS
            || row.try_get::<i32, _>("pending_event_bytes")? as usize + bytes.len()
                > WORKFLOW_PENDING_EVENTS_MAX_BYTES
        {
            return Err(ContractError::Busy.into());
        }
        sqlx::query("INSERT INTO workflow_events(workflow_id,event_key,event_source,event_id,event_bytes,accepted_at_ms) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(&command.workflow_id).bind(&command.key).bind(command.event.source()).bind(command.event.id())
            .bind(&bytes).bind(codec::ms(now)?).execute(&mut *tx).await?;
        sqlx::query("UPDATE workflow_runs SET pending_event_count=pending_event_count+1,pending_event_bytes=pending_event_bytes+$2 WHERE workflow_id=$1")
            .bind(&command.workflow_id).bind(i32::try_from(bytes.len()).map_err(|_| invalid("event size"))?).execute(&mut *tx).await?;
        if row
            .try_get::<Option<String>, _>("external_wait_key")?
            .as_deref()
            == Some(&command.key)
        {
            let activation: String = row.try_get("wait_activation_id")?;
            enqueue_wait(&mut tx, &command.workflow_id, &activation, now, now).await?;
        }
        tx.commit().await?;
        Ok(event_receipt(command, now, false))
    }
}

fn timestamp(at: i64) -> StoreResult<u64> {
    u64::try_from(at).map_err(|_| corrupt("external wait timestamp").into())
}
fn event_receipt(command: &WorkflowEventCommand, at: u64, replay: bool) -> WorkflowEventReceipt {
    WorkflowEventReceipt {
        scope: command.scope.clone(),
        workflow_id: command.workflow_id.clone(),
        key: command.key.clone(),
        event_id: command.event.id().to_owned(),
        event_source: command.event.source().to_owned(),
        accepted_at: at,
        already_accepted: replay,
    }
}

pub(super) async fn install_external_wait(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    activation: &str,
    wait: &WorkflowWait,
    now: u64,
) -> StoreResult<Option<TaskSnapshot>> {
    let deadline = wait.deadline(now)?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workflow_waits WHERE workflow_id=$1 AND wait_key=$2)",
    )
    .bind(&run.snapshot.workflow_id)
    .bind(wait.key())
    .fetch_one(&mut *connection)
    .await?;
    if exists {
        return Err(ContractError::Conflict.into());
    }
    let kind = match wait {
        WorkflowWait::Event { .. } => "event",
        WorkflowWait::Timer { .. } => {
            let event: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_events WHERE workflow_id=$1 AND event_key=$2)")
                .bind(&run.snapshot.workflow_id).bind(wait.key()).fetch_one(&mut *connection).await?;
            if event {
                return Err(ContractError::Conflict.into());
            }
            "timer"
        }
    };
    sqlx::query("INSERT INTO workflow_waits(workflow_id,wait_key,activation_id,kind,deadline_ms,registered_at_ms) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(&run.snapshot.workflow_id).bind(wait.key()).bind(activation).bind(kind)
        .bind(deadline.map(codec::ms).transpose()?).bind(codec::ms(now)?).execute(&mut *connection).await?;
    run.snapshot.state = WorkflowState::Waiting;
    run.snapshot.activation_id = None;
    run.wait_activation = Some(activation.to_owned());
    run.wait_keys.clear();
    run.external_wait_key = Some(wait.key().to_owned());
    save_run(connection, run).await?;
    if let Some(wake) = select_wake(
        connection,
        &run.snapshot.workflow_id,
        wait.key(),
        kind,
        deadline,
        now,
    )
    .await?
    {
        return resume(connection, run, wake, now).await.map(Some);
    }
    if let Some(deadline) = deadline {
        enqueue_wait(
            connection,
            &run.snapshot.workflow_id,
            activation,
            deadline,
            now,
        )
        .await?;
    }
    Ok(None)
}

/// A clock adjustment can make a previously due deadline future again. Return
/// its original deadline so the obligation is rearmed, never acknowledged away.
pub(super) struct WaitApplication {
    pub task: Option<TaskSnapshot>,
    pub rearm_at: Option<u64>,
}
pub(super) async fn apply_external_wait(
    connection: &mut PgConnection,
    snapshot: &WorkflowSnapshot,
    activation: &str,
    now: u64,
) -> StoreResult<WaitApplication> {
    let row = sqlx::query("SELECT w.wait_key,w.kind,w.deadline_ms FROM workflow_runs r JOIN workflow_waits w ON w.workflow_id=r.workflow_id AND w.wait_key=r.external_wait_key WHERE r.workflow_id=$1 AND r.state='waiting' AND r.wait_activation_id=$2 AND w.activation_id=$2 AND w.closed_at_ms IS NULL")
        .bind(&snapshot.workflow_id).bind(activation).fetch_optional(&mut *connection).await?;
    let Some(row) = row else {
        return Ok(WaitApplication {
            task: None,
            rearm_at: None,
        });
    };
    let key: String = row.try_get("wait_key")?;
    let kind: String = row.try_get("kind")?;
    let deadline = row
        .try_get::<Option<i64>, _>("deadline_ms")?
        .map(timestamp)
        .transpose()?;
    let Some(wake) = select_wake(
        connection,
        &snapshot.workflow_id,
        &key,
        &kind,
        deadline,
        now,
    )
    .await?
    else {
        return Ok(WaitApplication {
            task: None,
            rearm_at: Some(deadline.ok_or_else(|| corrupt("event wake without event"))?),
        });
    };
    let mut run = load_run(
        connection,
        &snapshot.scope,
        Some(&snapshot.workflow_id),
        None,
        false,
    )
    .await?;
    Ok(WaitApplication {
        task: Some(resume(connection, &mut run, wake, now).await?),
        rearm_at: None,
    })
}

async fn select_wake(
    connection: &mut PgConnection,
    workflow: &str,
    key: &str,
    kind: &str,
    deadline: Option<u64>,
    now: u64,
) -> StoreResult<Option<WorkflowWake>> {
    if kind == "event" {
        let event = sqlx::query("SELECT event_bytes,accepted_at_ms FROM workflow_events WHERE workflow_id=$1 AND event_key=$2 AND pending")
            .bind(workflow).bind(key).fetch_optional(&mut *connection).await?;
        if let Some(event) = event {
            let accepted_at = timestamp(event.try_get("accepted_at_ms")?)?;
            if deadline.is_none_or(|deadline| accepted_at < deadline) {
                return Ok(Some(WorkflowWake::Event {
                    key: key.to_owned(),
                    event: codec::decode(&event.try_get::<Vec<u8>, _>("event_bytes")?)?,
                    accepted_at,
                }));
            }
        }
    }
    Ok(deadline
        .filter(|deadline| now >= *deadline)
        .map(|deadline| {
            if kind == "event" {
                WorkflowWake::Timeout {
                    key: key.to_owned(),
                    deadline,
                }
            } else {
                WorkflowWake::Timer {
                    key: key.to_owned(),
                    deadline,
                }
            }
        }))
}
async fn resume(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    wake: WorkflowWake,
    now: u64,
) -> StoreResult<TaskSnapshot> {
    sqlx::query("UPDATE workflow_waits SET closed_at_ms=$3 WHERE workflow_id=$1 AND wait_key=$2 AND closed_at_ms IS NULL")
        .bind(&run.snapshot.workflow_id).bind(wake.key()).bind(codec::ms(now)?).execute(&mut *connection).await?;
    // One pending receipt can be retired here, including a receipt whose exact
    // deadline equality selected timeout. Retain its bytes for lost-ACK replay.
    sqlx::query("WITH retired AS (UPDATE workflow_events SET pending=false WHERE workflow_id=$1 AND event_key=$2 AND pending RETURNING octet_length(event_bytes) AS bytes) UPDATE workflow_runs SET pending_event_count=pending_event_count-(SELECT count(*)::integer FROM retired),pending_event_bytes=pending_event_bytes-(SELECT coalesce(sum(bytes),0)::integer FROM retired) WHERE workflow_id=$1")
        .bind(&run.snapshot.workflow_id).bind(wake.key()).execute(&mut *connection).await?;
    let task = schedule_activation(connection, run, BTreeMap::new(), Some(wake), now).await?;
    record_history(
        connection,
        &run.snapshot.workflow_id,
        Some(&task.task_id),
        now,
        "external_wait_resumed",
    )
    .await?;
    Ok(task)
}
async fn enqueue_wait(
    connection: &mut PgConnection,
    workflow: &str,
    activation: &str,
    at: u64,
    now: u64,
) -> StoreResult<()> {
    // An event may make a delayed timer ready sooner. Preserve any current
    // processing lease; the workflow lock serializes its eventual application.
    sqlx::query("INSERT INTO workflow_work(id,workflow_id,task_id,kind,available_at_ms,created_at_ms) VALUES('wait:'||$2,$1,$2,'wait',$3,$4) ON CONFLICT(id) DO UPDATE SET available_at_ms=LEAST(workflow_work.available_at_ms,EXCLUDED.available_at_ms) WHERE workflow_work.processed_at_ms IS NULL")
        .bind(workflow).bind(activation).bind(codec::ms(at)?).bind(codec::ms(now)?).execute(connection).await?;
    Ok(())
}

/// Cancellation/failure closes authority before draining children. Pending
/// inbox cleanup is bounded by the enforced 128-entry cap, never full history.
pub(super) async fn close_external_wait(
    connection: &mut PgConnection,
    run: &mut RunRecord,
    now: u64,
) -> StoreResult<()> {
    if let Some(key) = run.external_wait_key.take() {
        sqlx::query("UPDATE workflow_waits SET closed_at_ms=$3 WHERE workflow_id=$1 AND wait_key=$2 AND closed_at_ms IS NULL")
            .bind(&run.snapshot.workflow_id).bind(&key).bind(codec::ms(now)?).execute(&mut *connection).await?;
        if let Some(activation) = &run.wait_activation {
            sqlx::query("UPDATE workflow_work SET processed_at_ms=$2,lease_token=NULL,lease_until_ms=NULL WHERE id='wait:'||$1 AND processed_at_ms IS NULL")
                .bind(activation).bind(codec::ms(now)?).execute(&mut *connection).await?;
        }
    }
    sqlx::query("UPDATE workflow_events SET pending=false WHERE workflow_id=$1 AND pending")
        .bind(&run.snapshot.workflow_id)
        .execute(&mut *connection)
        .await?;
    sqlx::query(
        "UPDATE workflow_runs SET pending_event_count=0,pending_event_bytes=0 WHERE workflow_id=$1",
    )
    .bind(&run.snapshot.workflow_id)
    .execute(connection)
    .await?;
    Ok(())
}
