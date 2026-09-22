//! Bounded, crash-resumable collection. Retention never runs on execution reads.
//! The tiny scan lock serializes cooperating collectors, not execution traffic.
//! Target locks precede subscription locks; no session lock follows a task lock.
use crate::{persistence as db, *};
use sqlx::{AssertSqlSafe, PgConnection, Row};

impl RetentionStore for PostgresStore {
    fn retention_preview<'a>(
        &'a self,
        scope: &'a Scope,
        policy: &'a RetentionPolicy,
        deadline: Instant,
    ) -> ContractFuture<'a, RetentionPreview> {
        Box::pin(self.run_until(deadline, move || async move {
            scope.validate()?; policy.validate()?;
            let mut connection=self.transaction_connection().await?;
            let mut tx=connection.begin_read().await?;
            let now=db::now(&mut tx).await?;
            let cutoff=retention_cutoff(now,policy.retain_for_ms);
            let task_candidates=sqlx::query_scalar("SELECT task_id FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND terminal_at_ms<=$3 AND retiring_at_ms IS NULL ORDER BY terminal_at_ms,task_id LIMIT $4")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(cutoff).bind(i64::from(policy.batch_size)).fetch_all(&mut *tx).await?;
            let workflow_candidates=sqlx::query_scalar("SELECT workflow_id FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND terminal_at_ms<=$3 AND retiring_at_ms IS NULL ORDER BY terminal_at_ms,workflow_id LIMIT $4")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(cutoff).bind(i64::from(policy.batch_size)).fetch_all(&mut *tx).await?;
            let retiring_tasks=sqlx::query_scalar("SELECT task_id FROM tasks WHERE tenant_id=$1 AND namespace=$2 AND retiring_at_ms IS NOT NULL ORDER BY retiring_at_ms,task_id LIMIT $3")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(i64::from(policy.batch_size)).fetch_all(&mut *tx).await?;
            let retiring_workflows=sqlx::query_scalar("SELECT workflow_id FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND retiring_at_ms IS NOT NULL ORDER BY retiring_at_ms,workflow_id LIMIT $3")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(i64::from(policy.batch_size)).fetch_all(&mut *tx).await?;
            let expired_session_candidates=sqlx::query_scalar("SELECT session_id FROM worker_sessions WHERE tenant_id=$1 AND namespace=$2 AND expires_at_ms<=$3 ORDER BY expires_at_ms,session_id LIMIT $4")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(codec::ms(now)?).bind(i64::from(policy.batch_size)).fetch_all(&mut *tx).await?;
            tx.commit().await?;
            Ok(RetentionPreview {task_candidates,workflow_candidates,retiring_tasks,retiring_workflows,expired_session_candidates})
        }))
    }

    fn retain_batch<'a>(
        &'a self,
        scope: &'a Scope,
        policy: &'a RetentionPolicy,
        deadline: Instant,
    ) -> ContractFuture<'a, RetentionProgress> {
        Box::pin(self.run_until(deadline, move || self.retain_once(scope, policy)))
    }
}

impl PostgresStore {
    async fn retain_once(
        &self,
        scope: &Scope,
        policy: &RetentionPolicy,
    ) -> StoreResult<RetentionProgress> {
        scope.validate()?;
        policy.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        sqlx::query("INSERT INTO retention_turn(tenant_id,namespace,lane) VALUES($1,$2,0) ON CONFLICT DO NOTHING").bind(&scope.tenant_id).bind(&scope.namespace).execute(&mut *tx).await?;
        let Some(lane): Option<i16> = sqlx::query_scalar("SELECT lane FROM retention_turn WHERE tenant_id=$1 AND namespace=$2 FOR UPDATE SKIP LOCKED")
            .bind(&scope.tenant_id).bind(&scope.namespace).fetch_optional(&mut *tx).await? else { return Ok(RetentionProgress::default()); };
        sqlx::query("UPDATE retention_turn SET lane=$3 WHERE tenant_id=$1 AND namespace=$2")
            .bind(&scope.tenant_id)
            .bind(&scope.namespace)
            .bind((lane + 1) % 3)
            .execute(&mut *tx)
            .await?;
        let now = db::now(&mut tx).await?;
        let cutoff = now.saturating_sub(policy.retain_for_ms);
        let kind = match lane {
            0 => "task",
            1 => "workflow",
            _ => "session",
        };
        let mut progress = RetentionProgress::default();
        if let Some((id, retiring)) = candidate(
            &mut tx,
            scope,
            kind,
            if kind == "session" {
                codec::ms(now)?
            } else {
                retention_cutoff(now, policy.retain_for_ms)
            },
        )
        .await?
        {
            progress.examined = 1;
            match kind {
                "task" => {
                    if retiring {
                        collect_task(&mut tx, &id, policy.batch_size, &mut progress).await?;
                    } else {
                        retire_task(&mut tx, &id, cutoff, now, &mut progress).await?;
                    }
                }
                "workflow" => {
                    if retiring {
                        collect_workflow(&mut tx, &id, policy.batch_size, &mut progress).await?;
                    } else {
                        retire_workflow(&mut tx, &id, cutoff, now, &mut progress).await?;
                    }
                }
                _ => collect_session(&mut tx, &id, now, policy.batch_size, &mut progress).await?,
            }
        }
        tx.commit().await?;
        Ok(progress)
    }
}

fn retention_cutoff(now: u64, retain_for_ms: u64) -> i64 {
    now.checked_sub(retain_for_ms)
        .map_or(-1, |value| value as i64)
}

/// Range seeks use two independent durable cursors. Alternating discovery and
/// collection lets blocked terminal trees coexist with unrelated progress.
async fn candidate(
    connection: &mut PgConnection,
    scope: &Scope,
    kind: &str,
    cutoff: i64,
) -> StoreResult<Option<(String, bool)>> {
    sqlx::query("INSERT INTO retention_scans(tenant_id,namespace,lane) VALUES($1,$2,$3) ON CONFLICT DO NOTHING").bind(&scope.tenant_id).bind(&scope.namespace).bind(kind).execute(&mut *connection).await?;
    let row = sqlx::query(
        "SELECT * FROM retention_scans WHERE tenant_id=$1 AND namespace=$2 AND lane=$3",
    )
    .bind(&scope.tenant_id)
    .bind(&scope.namespace)
    .bind(kind)
    .fetch_one(&mut *connection)
    .await?;
    let job: bool = kind != "session" && row.try_get("process_retiring")?;
    sqlx::query("UPDATE retention_scans SET process_retiring=NOT process_retiring WHERE tenant_id=$1 AND namespace=$2 AND lane=$3").bind(&scope.tenant_id).bind(&scope.namespace).bind(kind).execute(&mut *connection).await?;
    let (table, id, time) = match (kind, job) {
        ("task", false) => ("tasks", "task_id", "terminal_at_ms"),
        ("task", true) => ("tasks", "task_id", "retiring_at_ms"),
        ("workflow", false) => ("workflow_runs", "workflow_id", "terminal_at_ms"),
        ("workflow", true) => ("workflow_runs", "workflow_id", "retiring_at_ms"),
        _ => ("worker_sessions", "session_id", "expires_at_ms"),
    };
    let (at_column, id_column) = if job {
        ("retiring_after_ms", "retiring_after_id")
    } else {
        ("after_ms", "after_id")
    };
    let at: i64 = row.try_get(at_column)?;
    let after: String = row.try_get(id_column)?;
    let filter = if kind == "session" {
        ""
    } else if job {
        " AND retiring_at_ms IS NOT NULL"
    } else {
        " AND terminal_at_ms IS NOT NULL AND retiring_at_ms IS NULL"
    };
    // Every identifier is selected from the fixed constants above.
    let query = candidate_sql(table, id, time, filter);
    let selected = sqlx::query(AssertSqlSafe(query))
        .bind(at)
        .bind(after)
        .bind(if job { i64::MAX } else { cutoff })
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .fetch_optional(&mut *connection)
        .await?;
    let position = selected
        .as_ref()
        .map(|r| Ok::<_, sqlx::Error>((r.try_get::<i64, _>("at")?, r.try_get::<String, _>("id")?)))
        .transpose()?;
    let (next_at, next_id) = position.clone().unwrap_or((0, String::new()));
    sqlx::query(AssertSqlSafe(format!("UPDATE retention_scans SET {at_column}=$2,{id_column}=$3 WHERE lane=$1 AND tenant_id=$4 AND namespace=$5")))
        .bind(kind).bind(next_at).bind(next_id).bind(&scope.tenant_id).bind(&scope.namespace).execute(&mut *connection).await?;
    Ok(position.map(|(_, id)| (id, job)))
}

fn candidate_sql(table: &str, id: &str, time: &str, filter: &str) -> String {
    format!(
        "SELECT {id} AS id,{time} AS at FROM {table} WHERE tenant_id=$4 AND namespace=$5 AND ({time},{id})>($1,$2){filter} AND {time}<=$3 ORDER BY {time},{id} LIMIT 1 FOR UPDATE SKIP LOCKED"
    )
}

/// Locks the bounded subscriber set before checking final delivery ages. A
/// concurrent rearm either commits first and protects the target, or sees the
/// removed receipt; it cannot report success after retirement was accepted.
async fn expire_subscriptions(
    connection: &mut PgConnection,
    kind: &str,
    id: &str,
    cutoff: u64,
) -> StoreResult<Option<u32>> {
    let field = if kind == "task" {
        "task_id"
    } else {
        "workflow_id"
    };
    let rows = sqlx::query(AssertSqlSafe(format!("SELECT subscription_id,state,delivered_at_ms,exhausted_at_ms FROM completion_subscriptions WHERE {field}=$1 ORDER BY subscription_id LIMIT 17 FOR UPDATE")))
        .bind(id).fetch_all(&mut *connection).await?;
    if rows.len() > MAX_COMPLETION_SUBSCRIPTIONS as usize {
        return Err(ContractError::Unavailable(
            "completion subscription limit violated during retention".into(),
        )
        .into());
    }
    for row in &rows {
        let state: String = row.try_get("state")?;
        let at: Option<i64> = match state.as_str() {
            "delivered" => row.try_get("delivered_at_ms")?,
            "exhausted" => row.try_get("exhausted_at_ms")?,
            _ => return Ok(None),
        };
        if at.is_none_or(|at| at < 0 || at as u64 > cutoff) {
            return Ok(None);
        }
    }
    let removed = sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM completion_subscriptions WHERE {field}=$1"
    )))
    .bind(id)
    .execute(&mut *connection)
    .await?
    .rows_affected();
    Ok(Some(removed as u32))
}

async fn retire_task(
    connection: &mut PgConnection,
    id: &str,
    cutoff: u64,
    now: u64,
    progress: &mut RetentionProgress,
) -> StoreResult<()> {
    let safe: bool = sqlx::query_scalar(include_str!("../queries/retention_task_safe.sql"))
        .bind(id)
        .fetch_one(&mut *connection)
        .await?;
    if !safe {
        return Ok(());
    }
    let Some(removed) = expire_subscriptions(connection, "task", id, cutoff).await? else {
        return Ok(());
    };
    sqlx::query("UPDATE tasks SET retiring_at_ms=$2,workflow_activation_id=NULL WHERE task_id=$1")
        .bind(id)
        .bind(codec::ms(now)?)
        .execute(connection)
        .await?;
    progress.retired = 1;
    progress.deleted_rows = removed;
    Ok(())
}

async fn retire_workflow(
    connection: &mut PgConnection,
    id: &str,
    cutoff: u64,
    now: u64,
    progress: &mut RetentionProgress,
) -> StoreResult<()> {
    // Leaf first. A terminal root never authorizes collection of an active child.
    let safe: bool = sqlx::query_scalar(include_str!("../queries/retention_workflow_safe.sql"))
        .bind(id)
        .bind(codec::ms(cutoff)?)
        .fetch_one(&mut *connection)
        .await?;
    if !safe {
        return Ok(());
    }
    let Some(removed) = expire_subscriptions(connection, "workflow", id, cutoff).await? else {
        return Ok(());
    };
    sqlx::query("UPDATE workflow_runs SET retiring_at_ms=$2,current_activation_id=NULL,wait_activation_id=NULL,wait_keys_bytes=NULL,external_wait_key=NULL WHERE workflow_id=$1")
        .bind(id).bind(codec::ms(now)?).execute(connection).await?;
    progress.retired = 1;
    progress.deleted_rows = removed;
    Ok(())
}

/// Each call deletes at most one bounded page from one dependent table, or the
/// final constant-size identity records after all dependent pages are gone.
async fn collect_task(
    connection: &mut PgConnection,
    id: &str,
    limit: u32,
    progress: &mut RetentionProgress,
) -> StoreResult<()> {
    for sql in [
        "DELETE FROM dispatch_claim_receipts WHERE ctid IN (SELECT ctid FROM dispatch_claim_receipts WHERE task_id=$1 LIMIT $2)",
        "DELETE FROM task_history WHERE ctid IN (SELECT ctid FROM task_history WHERE task_id=$1 LIMIT $2)",
        "DELETE FROM accepted_settlements WHERE attempt_id IN (SELECT s.attempt_id FROM accepted_settlements s JOIN attempts a ON a.attempt_id=s.attempt_id WHERE a.task_id=$1 LIMIT $2)",
        "DELETE FROM attempts WHERE attempt_id IN (SELECT attempt_id FROM attempts WHERE task_id=$1 LIMIT $2)",
    ] {
        let removed = sqlx::query(sql)
            .bind(id)
            .bind(i64::from(limit))
            .execute(&mut *connection)
            .await?
            .rows_affected();
        if removed > 0 {
            progress.deleted_rows = removed as u32;
            return Ok(());
        }
    }
    // At admission all external references were absent. Only this run's terminal
    // task link and its own activation can remain, and neither is mutable now.
    progress.deleted_rows += sqlx::query("DELETE FROM workflow_task_links WHERE task_id=$1")
        .bind(id)
        .execute(&mut *connection)
        .await?
        .rows_affected() as u32;
    progress.deleted_rows += sqlx::query("DELETE FROM workflow_activations WHERE task_id=$1")
        .bind(id)
        .execute(&mut *connection)
        .await?
        .rows_affected() as u32;
    progress.deleted_rows += sqlx::query("DELETE FROM tasks WHERE task_id=$1")
        .bind(id)
        .execute(connection)
        .await?
        .rows_affected() as u32;
    progress.deleted_executions = 1;
    Ok(())
}

async fn collect_workflow(
    connection: &mut PgConnection,
    id: &str,
    limit: u32,
    progress: &mut RetentionProgress,
) -> StoreResult<()> {
    for sql in [
        "DELETE FROM workflow_history WHERE ctid IN (SELECT ctid FROM workflow_history WHERE workflow_id=$1 LIMIT $2)",
        "DELETE FROM workflow_events WHERE ctid IN (SELECT ctid FROM workflow_events WHERE workflow_id=$1 LIMIT $2)",
        "DELETE FROM workflow_waits WHERE ctid IN (SELECT ctid FROM workflow_waits WHERE workflow_id=$1 LIMIT $2)",
        "DELETE FROM workflow_local_results WHERE ctid IN (SELECT r.ctid FROM workflow_local_results r JOIN workflow_activations a ON a.activation_id=r.activation_id WHERE a.workflow_id=$1 LIMIT $2)",
        "DELETE FROM workflow_work WHERE id IN (SELECT id FROM workflow_work WHERE workflow_id=$1 AND processed_at_ms IS NOT NULL LIMIT $2)",
        "DELETE FROM workflow_work WHERE id IN (SELECT id FROM workflow_work WHERE child_workflow_id=$1 AND processed_at_ms IS NOT NULL LIMIT $2)",
    ] {
        let removed = sqlx::query(sql)
            .bind(id)
            .bind(i64::from(limit))
            .execute(&mut *connection)
            .await?
            .rows_affected();
        if removed > 0 {
            progress.deleted_rows = removed as u32;
            return Ok(());
        }
    }
    progress.deleted_rows +=
        sqlx::query("DELETE FROM owned_workflow_links WHERE child_workflow_id=$1")
            .bind(id)
            .execute(&mut *connection)
            .await?
            .rows_affected() as u32;
    let tasks: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE workflow_id=$1)")
        .bind(id)
        .fetch_one(&mut *connection)
        .await?;
    if tasks {
        return Ok(());
    }
    progress.deleted_rows += sqlx::query("DELETE FROM workflow_runs WHERE workflow_id=$1")
        .bind(id)
        .execute(connection)
        .await?
        .rows_affected() as u32;
    progress.deleted_executions = 1;
    Ok(())
}

async fn collect_session(
    connection: &mut PgConnection,
    id: &str,
    now: u64,
    limit: u32,
    progress: &mut RetentionProgress,
) -> StoreResult<()> {
    let safe: bool=sqlx::query_scalar("SELECT expires_at_ms<=$2 AND NOT EXISTS(SELECT 1 FROM attempts WHERE worker_session_id=$1 AND state='active') FROM worker_sessions WHERE session_id=$1")
        .bind(id).bind(codec::ms(now)?).fetch_one(&mut *connection).await?;
    if !safe {
        return Ok(());
    }
    let removed=sqlx::query("DELETE FROM consumer_cursors WHERE ctid IN (SELECT ctid FROM consumer_cursors WHERE session_id=$1 LIMIT $2)")
        .bind(id).bind(i64::from(limit)).execute(&mut *connection).await?.rows_affected();
    progress.deleted_rows = removed as u32;
    if removed > 0 {
        return Ok(());
    }
    // Claim receipts retain the target's full retention window independently of
    // session expiry. Target collection removes only those that have expired.
    let receipts: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM dispatch_claim_receipts WHERE session_id=$1)",
    )
    .bind(id)
    .fetch_one(&mut *connection)
    .await?;
    if !receipts {
        progress.deleted_rows += sqlx::query("DELETE FROM worker_sessions WHERE session_id=$1")
            .bind(id)
            .execute(connection)
            .await?
            .rows_affected() as u32;
        progress.deleted_sessions = 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
