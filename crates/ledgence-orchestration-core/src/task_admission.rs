//! Pure task-admission transitions. The adapter proves and serializes the
//! corresponding retained Start/tombstone before persisting these changes.
use super::*;

/// Assignment generations are immutable; only proven no-Start deferrals are free.
pub fn charged_attempts(task: &TaskSnapshot) -> Result<u32> {
    task.charged_attempts()
}

/// Ready-to-invoke authority; this deliberately does not consume a renewal
/// sequence. The adapter commits the resource charge and retained source too.
pub fn start_task_admission(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    owner: &LeaseOwner,
    now: Timestamp,
) -> Result<Transition<Authority>> {
    check_owner(task, attempt, owner)?;
    if task.input.admission.is_none() || task.workflow_id.is_some() || task.provider_probe.is_some()
    {
        return Err(invalid("task admission requires a standalone task"));
    }
    if !live(task, attempt, now)
        || task.cancel_requested_at.is_some()
        || now >= attempt.deadline
        || attempt.settlement.is_some()
        || attempt.execution_may_have_started
    {
        return Err(ContractError::OwnershipLost);
    }
    let mut next = attempt.clone();
    next.execution_may_have_started = true;
    // Generic acquire/replay/renew never authorize a governed invocation.
    // Only this fresh one-shot Start transition is allowed to open the gate.
    let mut grant = authority(task, &next, now);
    grant.dispatch_allowed = true;
    Ok(Transition {
        task: task.clone(),
        history: vec![history(
            task,
            Some(&next),
            now,
            TransitionReason::DispatchAuthorized,
        )],
        reply: grant,
        attempt: Some(next),
    })
}

/// A permanent budget refusal before Start is terminal, independently of retry policy.
pub fn deny_task_admission(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    owner: &LeaseOwner,
    reason: RunBudgetDenial,
    now: Timestamp,
) -> Result<Transition<TaskState>> {
    check_owner(task, attempt, owner)?;
    timestamp(now)?;
    if task.input.admission.is_none()
        || !live(task, attempt, now)
        || task.current_attempt_id.as_deref() != Some(&owner.attempt_id)
        || task.cancel_requested_at.is_some()
        || now >= attempt.deadline
        || attempt.execution_may_have_started
        || attempt.settlement.is_some()
    {
        return Err(ContractError::OwnershipLost);
    }
    let mut next = attempt.clone();
    next.state = AttemptState::Failed;
    next.budget_denial = Some(reason);
    next.quiescence = Quiescence::Unconfirmed;
    next.finished_at = Some(now);
    let mut updated = task.clone();
    updated.state = TaskState::Failed;
    updated.current_attempt_id = None;
    updated.terminal_at = Some(now);
    let event = history(task, Some(&next), now, TransitionReason::Failed);
    Ok(Transition {
        task: updated,
        attempt: Some(next),
        history: vec![event],
        reply: TaskState::Failed,
    })
}

/// Only an authenticated close by the original worker proves local cleanup after refusal.
pub fn confirm_denied_task_quiescence(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    owner: &LeaseOwner,
    now: Timestamp,
) -> Result<Transition<TaskState>> {
    check_owner(task, attempt, owner)?;
    timestamp(now)?;
    if attempt.budget_denial.is_none()
        || task.state != TaskState::Failed
        || attempt.state != AttemptState::Failed
        || task.current_attempt_id.is_some()
        || attempt.execution_may_have_started
        || attempt.settlement.is_some()
        || attempt
            .finished_at
            .is_none_or(|finished| finished > now || Some(finished) != task.terminal_at)
    {
        return Err(ContractError::Conflict);
    }
    if attempt.quiescence == Quiescence::Confirmed {
        return Ok(unchanged(task, Some(attempt), task.state));
    }
    let mut next = attempt.clone();
    next.quiescence = Quiescence::Confirmed;
    let event = history(task, Some(&next), now, TransitionReason::CleanupConfirmed);
    Ok(Transition {
        task: task.clone(),
        attempt: Some(next),
        history: vec![event],
        reply: task.state,
    })
}

/// The retained tombstone proves this attempt can never acquire a Start. This
/// can also close an expired preparation lease without claiming process cleanup.
pub fn defer_task_admission(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    owner: &LeaseOwner,
    quiescence: Quiescence,
    now: Timestamp,
) -> Result<Transition<TaskState>> {
    check_owner(task, attempt, owner)?;
    timestamp(now)?;
    if task.input.admission.is_none() {
        return Err(invalid("ordinary task cannot defer resource admission"));
    }
    if attempt.state == AttemptState::Deferred {
        return Ok(unchanged(task, Some(attempt), task.state));
    }
    if task.state != TaskState::Active
        || attempt.state != AttemptState::Active
        || task.current_attempt_id.as_deref() != Some(&owner.attempt_id)
        || attempt.execution_may_have_started
        || attempt.settlement.is_some()
    {
        return Err(ContractError::OwnershipLost);
    }
    let mut next = attempt.clone();
    next.finished_at = Some(now);
    next.quiescence = quiescence;
    let mut updated = task.clone();
    updated.current_attempt_id = None;
    let reason = if task.cancel_requested_at.is_some() {
        next.state = AttemptState::Cancelled;
        updated.state = TaskState::Cancelled;
        updated.terminal_at = Some(now);
        TransitionReason::Cancelled
    } else {
        next.state = AttemptState::Deferred;
        updated.admission_deferrals = updated
            .admission_deferrals
            .checked_add(1)
            .ok_or_else(|| invalid("admission deferral count overflow"))?;
        charged_attempts(&updated)?;
        if task.attempt_count == MAX_TASK_ATTEMPT_GENERATION {
            updated.state = TaskState::Failed;
            updated.terminal_at = Some(now);
        } else {
            updated.state = TaskState::Queued;
            updated.available_at = add_time(now, TASK_ADMISSION_DEFERRAL_BACKOFF_MS)?;
        }
        TransitionReason::AdmissionDeferred
    };
    let mut events = vec![history(task, Some(&next), now, reason)];
    if updated.state == TaskState::Failed {
        events.push(history(task, Some(&next), now, TransitionReason::Failed));
    }
    Ok(Transition {
        reply: updated.state,
        task: updated,
        history: events,
        attempt: Some(next),
    })
}

/// Start was accepted, but its owning runtime confirms it did not invoke. The
/// accepted authorization remains charged; no application report is invented.
pub fn stop_started_task_admission(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    owner: &LeaseOwner,
    now: Timestamp,
) -> Result<Transition<TaskState>> {
    check_owner(task, attempt, owner)?;
    if task.input.admission.is_none() || !attempt.execution_may_have_started {
        return Err(invalid("task has no accepted admission Start"));
    }
    if task.state != TaskState::Active || attempt.state != AttemptState::Active {
        return Ok(unchanged(task, Some(attempt), task.state));
    }
    if task.current_attempt_id.as_deref() != Some(&owner.attempt_id) || attempt.settlement.is_some()
    {
        return Err(ContractError::Conflict);
    }
    let mut next = attempt.clone();
    next.quiescence = Quiescence::Confirmed;
    let mut updated = task.clone();
    let mut events = vec![history(
        task,
        Some(&next),
        now,
        TransitionReason::AdmissionStopped,
    )];
    lifecycle::finish(&mut updated, &mut next, now, &mut events)?;
    Ok(Transition {
        reply: updated.state,
        task: updated,
        attempt: Some(next),
        history: events,
    })
}
