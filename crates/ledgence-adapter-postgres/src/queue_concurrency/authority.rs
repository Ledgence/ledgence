//! All assignment paths take the same per-queue cutover lock after finding a
//! candidate. Only configured queues take an exclusive policy row lock.
use super::*;

pub(crate) struct Gate {
    pub(crate) full: bool,
    timeout_ms: Option<u32>,
}

pub(crate) async fn gate(
    tx: &mut PgConnection,
    scope: &Scope,
    queue: &str,
) -> StoreResult<Option<Gate>> {
    shared(tx, scope, queue).await?;
    let row=sqlx::query("SELECT maximum,occupied,timeout_ms FROM queue_concurrency_policies WHERE tenant_id=$1 AND namespace=$2 AND queue=$3 FOR UPDATE")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(queue).fetch_optional(tx).await?;
    row.map(|r| {
        let ceiling: Option<i64> = r.try_get("maximum")?;
        let occupied: i64 = r.try_get("occupied")?;
        Ok(Gate {
            full: ceiling.is_some_and(|n| occupied >= n),
            timeout_ms: optional_u32(&r, "timeout_ms")?,
        })
    })
    .transpose()
}
async fn shared(tx: &mut PgConnection, scope: &Scope, queue: &str) -> StoreResult<()> {
    sqlx::query("SELECT pg_advisory_xact_lock_shared(ldg_queue_lock_key($1,$2,$3))")
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(queue)
        .execute(tx)
        .await?;
    Ok(())
}

pub(crate) async fn prepare(
    tx: &mut PgConnection,
    t: &mut core::AcquireTransition,
    gate: Option<&Gate>,
    at: u64,
) -> StoreResult<()> {
    let Some(g) = gate else { return Ok(()) };
    let Some(attempt) = t.changes.as_mut().and_then(|c| c.attempt.as_mut()) else {
        return Ok(());
    };
    let AcquireReply::Assigned { assignment, .. } = &mut t.reply else {
        return Err(invalid("assignment lost queue authority").into());
    };
    if g.full {
        return Err(invalid("overbooked queue assignment").into());
    }
    if let Some(ms) = g.timeout_ms {
        let deadline = at
            .checked_add(u64::from(ms))
            .ok_or_else(|| invalid("queue deadline overflow"))?;
        attempt.deadline = attempt.deadline.min(deadline);
        assignment.attempt_deadline = attempt.deadline;
        assignment.authority.execution_remaining_ms = assignment
            .authority
            .execution_remaining_ms
            .min(attempt.deadline.saturating_sub(at));
    }
    assignment.queue_concurrency = true;
    sqlx::query("SELECT set_config('ledgence.queue_claim_attempt',$1,true),set_config('ledgence.queue_claim_time',$2,true)")
        .bind(&attempt.lease.owner.attempt_id).bind(at.to_string()).execute(tx).await?;
    Ok(())
}

pub(crate) async fn replay(tx: &mut PgConnection, reply: &mut AcquireReply) -> StoreResult<()> {
    if let AcquireReply::Assigned { assignment, .. } = reply {
        assignment.queue_concurrency = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM queue_concurrency_holds WHERE attempt_id=$1)",
        )
        .bind(&assignment.lease.owner.attempt_id)
        .fetch_one(tx)
        .await?;
    }
    Ok(())
}

/// The broker may delete the original only after a future route publication
/// exists in the same transaction as Deferred. Duplicates do not postpone the
/// next publication. An already leased/uncertain send is harmless and is fenced.
pub(crate) async fn defer_broker(
    tx: &mut PgConnection,
    task: &TaskSnapshot,
    destination: &str,
    generation: u32,
) -> StoreResult<u64> {
    let at = codec::ms(db::now(tx).await?)?;
    let next = at
        .checked_add(1000)
        .ok_or_else(|| invalid("broker delay overflow"))?;
    let due: Option<i64> = sqlx::query_scalar(include_str!("defer.sql"))
        .bind(&task.task_id)
        .bind(i64::from(generation))
        .bind(destination)
        .bind(at)
        .bind(next)
        .fetch_optional(tx)
        .await?;
    let due = due
        .filter(|v| *v > at && *v <= next)
        .ok_or_else(|| invalid("missing durable broker deferral"))?;
    Ok(u64::try_from(due).map_err(|_| invalid("broker delay"))?)
}
