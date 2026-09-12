//! Deterministic orchestration decisions over transaction-loaded records.
//!
//! Nothing here acquires a database lock, reads a clock, generates an ID, or
//! acknowledges a network call. A storage adapter must load related records
//! under the required locks, obtain fresh authoritative time, call a transition,
//! persist ALL returned records/history, and commit before returning its reply.

mod acquisition;
mod authority;
mod lifecycle;
pub use acquisition::*;
pub use authority::*;
pub use lifecycle::*;

use ledgence_orchestration_api::*;
use ledgence_worker_api::{CloudEvent, ProgramDescriptor};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Proposed writes, not a durable receipt. Input snapshots are never modified.
#[derive(Debug, Clone)]
pub struct Transition<R> {
    pub task: TaskSnapshot,
    pub attempt: Option<AttemptSnapshot>,
    pub history: Vec<HistoryEvent>,
    pub reply: R,
}

/// IDs supplied by the adapter's allocator. Store uniqueness is still required.
#[derive(Debug, Clone)]
pub struct AttemptIds {
    pub attempt_id: String,
    pub lease_id: String,
    pub event_id: String,
    pub trace: Option<TraceContext>,
}

/// Bind a validated submission to immutable bytes before making it claimable.
pub fn submit(
    command: &SubmitCommand,
    descriptor: &ProgramDescriptor,
    task_id: &str,
    run_id: &str,
    now: Timestamp,
) -> Result<Transition<()>> {
    command.input.validate()?;
    validate_text(&command.idempotency_key, 255)?;
    validate_text(task_id, 128)?;
    validate_text(run_id, 128)?;
    descriptor.validate()?;
    validate_trace(command.origin_trace.as_ref())?;
    timestamp(now)?;
    if descriptor.program != command.input.program {
        return Err(ContractError::Conflict);
    }
    let task = TaskSnapshot {
        task_id: task_id.into(),
        run_id: run_id.into(),
        idempotency_key: command.idempotency_key.clone(),
        input: command.input.clone(),
        descriptor: descriptor.clone(),
        origin_trace: command.origin_trace.clone(),
        state: TaskState::Queued,
        submitted_at: now,
        available_at: now,
        terminal_at: None,
        current_attempt_id: None,
        attempt_count: 0,
        cancel_requested_at: None,
    };
    Ok(Transition {
        history: vec![history(&task, None, now, TransitionReason::Submitted)],
        task,
        attempt: None,
        reply: (),
    })
}

/// Call before external program resolution. The originally bound digest wins;
/// transport tracing on a retry does not replace the accepted origin context.
pub fn replay_submission(task: &TaskSnapshot, command: &SubmitCommand) -> Result<()> {
    command.input.validate()?;
    validate_text(&command.idempotency_key, 255)?;
    validate_trace(command.origin_trace.as_ref())?;
    if task.idempotency_key != command.idempotency_key
        || !task.input.semantically_matches(&command.input)?
    {
        return Err(ContractError::Conflict);
    }
    Ok(())
}

/// Register a newly allocated, never-reused session identity. The store checks
/// uniqueness. Acquisition never implicitly registers an unknown identity.
pub fn open_session(
    id: &str,
    scope: Scope,
    queue: &str,
    concurrency: u32,
    now: Timestamp,
) -> Result<WorkerSession> {
    validate_text(id, 128)?;
    scope.validate()?;
    validate_text(queue, 128)?;
    if concurrency == 0 {
        return Err(invalid("concurrency must be positive"));
    }
    Ok(WorkerSession {
        id: id.into(),
        scope,
        queue: queue.into(),
        concurrency,
        expires_at: add_time(now, SESSION_VALIDITY_MS)?,
    })
}

/// Session presence may extend a live session, but never revives one that expired.
/// Existing attempt leases still require their own renewal.
pub fn extend_session(session: &WorkerSession, now: Timestamp) -> Result<WorkerSession> {
    check_session(session, now)?;
    let mut next = session.clone();
    next.expires_at = add_time(now, SESSION_VALIDITY_MS)?;
    Ok(next)
}

fn check_session(session: &WorkerSession, now: Timestamp) -> Result<()> {
    validate_text(&session.id, 128)?;
    session.scope.validate()?;
    validate_text(&session.queue, 128)?;
    if session.concurrency == 0 {
        return Err(invalid("invalid session concurrency"));
    }
    if now >= session.expires_at {
        return Err(ContractError::SessionExpired);
    }
    Ok(())
}

fn validate_trace(trace: Option<&TraceContext>) -> Result<()> {
    if let Some(trace) = trace {
        let mut value = json!({"specversion":"1.0","id":"validation","source":"urn:ledgence:orchestrator",
            "type":"com.ledgence.task.invocation.requested.v1","datacontenttype":"application/json",
            "ldgtenantid":"validation","ldgnamespace":"validation","ldgrunid":"validation",
            "ldgtaskid":"validation","ldgattemptid":"validation","ldgattemptno":1,"data":null});
        insert_trace(&mut value, trace);
        CloudEvent::new(value)?;
    }
    Ok(())
}
fn insert_trace(value: &mut Value, trace: &TraceContext) {
    value["traceparent"] = json!(trace.traceparent);
    if let Some(state) = &trace.tracestate {
        value["tracestate"] = json!(state);
    }
}
fn timestamp(now: Timestamp) -> Result<String> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(now) * 1_000_000)
        .map_err(|_| invalid("timestamp outside RFC3339 range"))?
        .format(&Rfc3339)
        .map_err(|_| invalid("timestamp outside RFC3339 range"))
}
fn add_time(now: Timestamp, milliseconds: u64) -> Result<Timestamp> {
    let next = now
        .checked_add(milliseconds)
        .ok_or_else(|| invalid("timestamp overflow"))?;
    timestamp(next)?;
    Ok(next)
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn history(
    task: &TaskSnapshot,
    attempt: Option<&AttemptSnapshot>,
    at: Timestamp,
    reason: TransitionReason,
) -> HistoryEvent {
    HistoryEvent {
        task_id: task.task_id.clone(),
        attempt_id: attempt.map(|a| a.lease.owner.attempt_id.clone()),
        at,
        reason,
    }
}
fn unchanged<R>(task: &TaskSnapshot, attempt: Option<&AttemptSnapshot>, reply: R) -> Transition<R> {
    Transition {
        task: task.clone(),
        attempt: attempt.cloned(),
        history: Vec::new(),
        reply,
    }
}

fn validate_attempt(task: &TaskSnapshot, attempt: &AttemptSnapshot) -> Result<()> {
    let owner = &attempt.lease.owner;
    if owner.scope != task.scope()
        || owner.task_id != task.task_id
        || owner.attempt_id != attempt.event.attempt_id()
        || owner.generation == 0
        || owner.generation > task.attempt_count
        || attempt.event.value()["ldgattemptno"].as_u64() != Some(u64::from(owner.generation))
        || attempt.event.task_id() != task.task_id
        || attempt.event.value()["ldgrunid"].as_str() != Some(&task.run_id)
        || attempt.event.tenant_id() != task.input.tenant_id
        || attempt.event.namespace() != task.input.namespace
        || attempt.descriptor != task.descriptor
    {
        return Err(invalid("attempt snapshot does not belong to task"));
    }
    Ok(())
}
fn live(task: &TaskSnapshot, attempt: &AttemptSnapshot, now: Timestamp) -> bool {
    task.state == TaskState::Active
        && attempt.state == AttemptState::Active
        && task.current_attempt_id.as_deref() == Some(&attempt.lease.owner.attempt_id)
        && now < attempt.lease.expires_at
        && now < attempt.authority_deadline
}
fn check_owner(task: &TaskSnapshot, attempt: &AttemptSnapshot, owner: &LeaseOwner) -> Result<()> {
    validate_attempt(task, attempt)?;
    if &attempt.lease.owner != owner {
        return Err(ContractError::OwnershipLost);
    }
    Ok(())
}
fn authority(task: &TaskSnapshot, attempt: &AttemptSnapshot, now: Timestamp) -> Authority {
    let valid = live(task, attempt, now);
    let cancellation = task.cancel_requested_at.is_some() || now >= attempt.deadline;
    Authority {
        owner: attempt.lease.owner.clone(),
        expires_at: attempt.lease.expires_at,
        remaining_ms: if valid {
            attempt.lease.expires_at.saturating_sub(now)
        } else {
            0
        },
        execution_remaining_ms: if valid && !cancellation {
            attempt.deadline.saturating_sub(now)
        } else {
            0
        },
        renew_sequence: attempt
            .last_renewal
            .as_ref()
            .map_or(0, |renewal| renewal.sequence),
        cancel_requested: cancellation,
        dispatch_allowed: valid
            && !cancellation
            && attempt.settlement.is_none()
            && attempt
                .last_renewal
                .as_ref()
                .is_some_and(|renewal| renewal.intent == RenewIntent::Dispatch),
    }
}

#[cfg(test)]
mod tests;
