//! Queue concurrency is a cluster-wide limit on assigned attempts. Positive
//! source closure is distinct from a task result and from releasing unknown risk.
mod authority;
mod lifecycle;
mod management;
#[cfg(test)]
mod tests;
use crate::workflow::admission::policy::{lock_receipt, receipt, record_receipt};
use crate::{persistence as db, *};
pub(crate) use authority::{defer_broker, gate, prepare, replay};
use ledgence_orchestration_core as core;
use sqlx::{PgConnection, Row, postgres::PgRow};
#[cfg(test)]
pub(crate) use tests::crash_fixture;

fn invalid(s: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid queue capacity authority: {s}"))
}
fn optional_u32(row: &PgRow, col: &str) -> StoreResult<Option<u32>> {
    row.try_get::<Option<i64>, _>(col)?
        .map(|v| u32::try_from(v).map_err(|_| invalid(col).into()))
        .transpose()
}
fn time(row: &PgRow, col: &str) -> StoreResult<u64> {
    u64::try_from(row.try_get::<i64, _>(col)?).map_err(|_| invalid(col).into())
}
fn optional_time(row: &PgRow, col: &str) -> StoreResult<Option<u64>> {
    row.try_get::<Option<i64>, _>(col)?
        .map(|v| u64::try_from(v).map_err(|_| invalid(col).into()))
        .transpose()
}
fn revision(row: &PgRow) -> StoreResult<u64> {
    row.try_get::<String, _>("rev")?
        .parse()
        .map_err(|_| invalid("revision").into())
}
fn snapshot(row: &PgRow) -> StoreResult<QueuePolicySnapshot> {
    let timeout = optional_u32(row, "timeout_ms")?
        .map(|after_ms| -> StoreResult<_> {
            Ok(QueueHoldTimeout {
                after_ms,
                release: optional_u32(row, "grace_ms")?
                    .map_or(QueueTimeoutRelease::Confirm, |grace_ms| {
                        QueueTimeoutRelease::AfterGrace { grace_ms }
                    }),
            })
        })
        .transpose()?;
    let result = QueuePolicySnapshot {
        record: QueuePolicyRecord {
            scope: Scope {
                tenant_id: row.try_get("tenant_id")?,
                namespace: row.try_get("namespace")?,
            },
            queue: row.try_get("queue")?,
            revision: revision(row)?,
            policy: QueueConcurrencyPolicy {
                max_concurrency: optional_u32(row, "maximum")?,
                timeout,
            },
            updated_at: time(row, "updated_at_ms")?,
        },
        occupied: time(row, "occupied")?,
        uncertain: time(row, "uncertain")?,
    };
    result.validate()?;
    Ok(result)
}
fn hold(row: &PgRow) -> StoreResult<QueueHoldSnapshot> {
    let state = codec::enum_value(row.try_get("state")?)?;
    let release_kind = row
        .try_get::<Option<String>, _>("release_kind")?
        .map(codec::enum_value)
        .transpose()?;
    let h = QueueHoldSnapshot {
        scope: Scope {
            tenant_id: row.try_get("tenant_id")?,
            namespace: row.try_get("namespace")?,
        },
        queue: row.try_get("queue")?,
        task_id: row.try_get("task_id")?,
        attempt_id: row.try_get("attempt_id")?,
        worker_session_id: row.try_get("worker_session_id")?,
        consumer_id: u32::try_from(row.try_get::<i64, _>("consumer_id")?)
            .map_err(|_| invalid("consumer"))?,
        revision: revision(row)?,
        policy_revision: row
            .try_get::<String, _>("policy_rev")?
            .parse()
            .map_err(|_| invalid("policy revision"))?,
        grace_ms: optional_u32(row, "grace_ms")?,
        force_at: optional_time(row, "force_at_ms")?,
        state,
        acquired_at: time(row, "acquired_at_ms")?,
        deadline: optional_time(row, "deadline_ms")?,
        stop_requested_at: optional_time(row, "stop_requested_at_ms")?,
        released_at: optional_time(row, "released_at_ms")?,
        release_kind,
    };
    h.validate()?;
    Ok(h)
}
async fn get_policy(
    tx: &mut PgConnection,
    scope: &Scope,
    queue: &str,
    locked: bool,
) -> StoreResult<Option<PgRow>> {
    let sql = if locked {
        "SELECT *,trunc(revision)::text AS rev FROM queue_concurrency_policies WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR UPDATE"
    } else {
        "SELECT *,trunc(revision)::text AS rev FROM queue_concurrency_policies WHERE tenant_id=$1 AND namespace=$2 AND queue=$3"
    };
    Ok(sqlx::query(sql)
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(queue)
        .fetch_optional(tx)
        .await?)
}
async fn get_hold(
    tx: &mut PgConnection,
    scope: &Scope,
    queue: &str,
    id: &str,
) -> StoreResult<PgRow> {
    sqlx::query("SELECT *,trunc(revision)::text AS rev,trunc(policy_revision)::text AS policy_rev FROM queue_concurrency_holds WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 AND attempt_id=$4 FOR UPDATE").bind(&scope.tenant_id).bind(&scope.namespace).bind(queue).bind(id).fetch_optional(tx).await?.ok_or(ContractError::NotFound.into())
}
impl QueuePolicyStore for PostgresStore {}
impl QueuePolicyOperations for PostgresStore {
    fn put_queue_policy<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a QueuePolicyPut,
    ) -> ContractFuture<'a, QueuePolicyReceipt> {
        Box::pin(self.run(move || self.put_queue_once(p, c)))
    }
    fn inspect_queue_policy<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a QueuePolicyQuery,
    ) -> ContractFuture<'a, QueuePolicySnapshot> {
        Box::pin(self.run(move || self.inspect_queue_once(p, c)))
    }
    fn list_queue_policies<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a QueuePolicyList,
    ) -> ContractFuture<'a, QueuePolicyPage> {
        Box::pin(self.run(move || self.list_queues_once(p, c)))
    }
    fn list_queue_holds<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a QueueHoldList,
    ) -> ContractFuture<'a, QueueHoldPage> {
        Box::pin(self.run(move || self.list_holds_once(p, c)))
    }
    fn release_queue_hold<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a QueueHoldRelease,
    ) -> ContractFuture<'a, QueueHoldReceipt> {
        Box::pin(self.run(move || self.release_hold_once(p, c)))
    }
    fn close_queue_hold<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a QueueHoldClose,
    ) -> ContractFuture<'a, QueueHoldCloseReceipt> {
        Box::pin(self.run(move || self.close_hold_once(p, c)))
    }
}
