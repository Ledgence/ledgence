use super::*;
use ledgence_worker_api::{ErrorKind, InvocationIdentity, ProgramOutcome, validate_wire_value};

/// Accept one ordered renewal. Duplicate sequences never extend the lease twice.
pub fn renew(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    session: Option<&WorkerSession>,
    command: &RenewCommand,
    now: Timestamp,
) -> Result<Transition<Authority>> {
    let session = session.ok_or(ContractError::UnknownSession)?;
    check_session(session, now)?;
    check_owner(task, attempt, &command.owner)?;
    if session.id != command.owner.worker_session_id
        || session.scope != command.owner.scope
        || session.queue != task.input.queue
        || command.owner.consumer_id >= session.concurrency
        || !live(task, attempt, now)
    {
        return Err(ContractError::OwnershipLost);
    }
    let last_sequence = attempt
        .last_renewal
        .as_ref()
        .map_or(0, |renewal| renewal.sequence);
    if command.sequence == 0 || command.sequence > last_sequence.saturating_add(1) {
        return Err(ContractError::OutOfOrder);
    }
    if command.sequence < last_sequence {
        return Err(ContractError::ObsoleteOperation);
    }
    if command.sequence == last_sequence {
        if attempt.last_renewal.as_ref() != Some(command) {
            return Err(ContractError::Conflict);
        }
        return Ok(unchanged(
            task,
            Some(attempt),
            authority(task, attempt, now),
        ));
    }
    let mut next = attempt.clone();
    next.lease.expires_at = add_time(now, LEASE_DURATION_MS)?
        .min(next.authority_deadline)
        .min(session.expires_at);
    next.last_renewal = Some(command.clone());
    let mut events = Vec::new();
    if command.intent == RenewIntent::Dispatch
        && task.cancel_requested_at.is_none()
        && now < next.deadline
        && next.settlement.is_none()
        && !next.execution_may_have_started
    {
        next.execution_may_have_started = true;
        events.push(history(
            task,
            Some(&next),
            now,
            TransitionReason::DispatchAuthorized,
        ));
    }
    let reply = authority(task, &next, now);
    Ok(Transition {
        task: task.clone(),
        attempt: Some(next),
        history: events,
        reply,
    })
}

/// Store the immutable report first. Confirmed quiescence finalizes immediately;
/// otherwise confirmation or lease expiry completes the scheduling transition.
pub fn settle(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    command: &SettleCommand,
    now: Timestamp,
) -> Result<Transition<SettleReply>> {
    check_owner(task, attempt, &command.owner)?;
    validate_report(attempt, command)?;
    if let Some(accepted) = &attempt.settlement {
        if canonical_json_bytes(
            &serde_json::to_value(&accepted.command)
                .map_err(|_| invalid("report serialization failed"))?,
        )? != canonical_json_bytes(
            &serde_json::to_value(command).map_err(|_| invalid("report serialization failed"))?,
        )? {
            return Err(ContractError::Conflict);
        }
        return Ok(unchanged(
            task,
            Some(attempt),
            SettleReply {
                receipt: accepted.receipt.clone(),
                already_accepted: true,
                task_state: task.state,
            },
        ));
    }
    if !live(task, attempt, now) {
        return Err(ContractError::OwnershipLost);
    }
    let receipt = SettlementReceipt {
        operation_id: command.operation_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id: command.owner.attempt_id.clone(),
        accepted_at: now,
    };
    let mut next = attempt.clone();
    next.quiescence = command.quiescence;
    next.settlement = Some(AcceptedSettlement {
        command: command.clone(),
        receipt: receipt.clone(),
    });
    next.execution_may_have_started |= match &command.report {
        AttemptReport::Completed(_) => true,
        AttemptReport::Failed(report) => report.execution_may_have_started,
    };
    let mut updated_task = task.clone();
    let mut events = vec![history(
        task,
        Some(&next),
        now,
        TransitionReason::ReportAccepted,
    )];
    if command.quiescence == Quiescence::Confirmed {
        finish(&mut updated_task, &mut next, now, &mut events)?;
    }
    let reply = SettleReply {
        receipt,
        already_accepted: false,
        task_state: updated_task.state,
    };
    Ok(Transition {
        task: updated_task,
        attempt: Some(next),
        history: events,
        reply,
    })
}

/// A separate monotone acknowledgement, so cleanup progress never rewrites an
/// accepted report. Replays are valid after finalization; new stale confirmations
/// are rejected and cannot change a newer task attempt.
pub fn confirm_quiescence(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    owner: &LeaseOwner,
    now: Timestamp,
) -> Result<Transition<TaskState>> {
    check_owner(task, attempt, owner)?;
    if attempt.quiescence == Quiescence::Confirmed {
        return Ok(unchanged(task, Some(attempt), task.state));
    }
    if !live(task, attempt, now) {
        return Err(ContractError::OwnershipLost);
    }
    if attempt.settlement.is_none() {
        return Err(invalid("a report must precede cleanup confirmation"));
    }
    let mut next = attempt.clone();
    next.quiescence = Quiescence::Confirmed;
    let mut updated_task = task.clone();
    let mut events = vec![history(
        task,
        Some(&next),
        now,
        TransitionReason::CleanupConfirmed,
    )];
    finish(&mut updated_task, &mut next, now, &mut events)?;
    let reply = updated_task.state;
    Ok(Transition {
        task: updated_task,
        attempt: Some(next),
        history: events,
        reply,
    })
}

/// Cancellation is a persisted scheduling decision. Queued work is terminal
/// immediately; active work is awaited until cleanup confirmation or expiry.
pub fn cancel(
    task: &TaskSnapshot,
    attempt: Option<&AttemptSnapshot>,
    now: Timestamp,
) -> Result<Transition<TaskState>> {
    timestamp(now)?;
    if task.state.is_terminal() || task.cancel_requested_at.is_some() {
        return Ok(unchanged(task, attempt, task.state));
    }
    let mut next = task.clone();
    next.cancel_requested_at = Some(now);
    let mut events = vec![history(
        task,
        attempt,
        now,
        TransitionReason::CancelRequested,
    )];
    if task.state == TaskState::Queued {
        if task.current_attempt_id.is_some() {
            return Err(invalid("queued task owns an attempt"));
        }
        next.state = TaskState::Cancelled;
        next.terminal_at = Some(now);
        events.push(history(&next, None, now, TransitionReason::Cancelled));
        return Ok(Transition {
            reply: next.state,
            task: next,
            attempt: None,
            history: events,
        });
    }
    let attempt = attempt.ok_or_else(|| invalid("active task needs its attempt"))?;
    validate_attempt(task, attempt)?;
    if task.current_attempt_id.as_deref() != Some(&attempt.lease.owner.attempt_id) {
        return Err(invalid("not the current attempt"));
    }
    if now >= attempt.lease.expires_at || now >= attempt.authority_deadline {
        let mut expired = expire(&next, attempt, now)?;
        events.append(&mut expired.history);
        return Ok(Transition {
            task: expired.task,
            attempt: expired.attempt,
            history: events,
            reply: TaskState::Cancelled,
        });
    }
    let mut cancelled_attempt = attempt.clone();
    cancelled_attempt.authority_deadline = cancelled_attempt
        .authority_deadline
        .min(add_time(now, CLEANUP_GRACE_MS)?);
    cancelled_attempt.lease.expires_at = cancelled_attempt
        .lease
        .expires_at
        .min(cancelled_attempt.authority_deadline);
    Ok(Transition {
        task: next,
        attempt: Some(cancelled_attempt),
        history: events,
        reply: TaskState::Active,
    })
}

/// Idempotent expiry scan. An already recorded report remains authoritative;
/// without one the attempt is lost. Cancellation stops further retries.
pub fn expire(
    task: &TaskSnapshot,
    attempt: &AttemptSnapshot,
    now: Timestamp,
) -> Result<Transition<bool>> {
    validate_attempt(task, attempt)?;
    if attempt.state != AttemptState::Active {
        return Ok(unchanged(task, Some(attempt), false));
    }
    if task.current_attempt_id.as_deref() != Some(&attempt.lease.owner.attempt_id)
        || task.state != TaskState::Active
    {
        return Err(invalid("active attempt is not current"));
    }
    if now < attempt.lease.expires_at && now < attempt.authority_deadline {
        return Ok(unchanged(task, Some(attempt), false));
    }
    let mut next = attempt.clone();
    let mut updated_task = task.clone();
    let mut events = vec![history(
        task,
        Some(attempt),
        now,
        TransitionReason::LeaseExpired,
    )];
    finish(&mut updated_task, &mut next, now, &mut events)?;
    Ok(Transition {
        task: updated_task,
        attempt: Some(next),
        history: events,
        reply: true,
    })
}

/// Validate report payload and tracing against an immutable attempt.
/// This does not establish current ownership or durable acceptance.
pub fn validate_report(attempt: &AttemptSnapshot, command: &SettleCommand) -> Result<()> {
    validate_text(&command.operation_id, 128)?;
    validate_trace(command.processing_trace.as_ref())?;
    let context = match &command.report {
        AttemptReport::Completed(report) => {
            if let ProgramOutcome::Success { output } = &report.outcome {
                validate_wire_value(output)?;
            }
            &report.context
        }
        AttemptReport::Failed(report) => &report.context,
    };
    if context.identity != InvocationIdentity::from(&attempt.event)
        || context.program != attempt.descriptor.program
        || context.digest != attempt.descriptor.digest
    {
        return Err(ContractError::Conflict);
    }
    let bytes = canonical_json_bytes(
        &serde_json::to_value(command).map_err(|_| invalid("report serialization failed"))?,
    )?;
    if bytes.len() > SETTLEMENT_MAX_BYTES {
        return Err(invalid("settlement exceeds 8 MiB"));
    }
    Ok(())
}

fn finish(
    task: &mut TaskSnapshot,
    attempt: &mut AttemptSnapshot,
    now: Timestamp,
    events: &mut Vec<HistoryEvent>,
) -> Result<()> {
    let (state, retryable) = if task.cancel_requested_at.is_some() {
        (
            if attempt.quiescence == Quiescence::Confirmed {
                AttemptState::Cancelled
            } else {
                AttemptState::Lost
            },
            false,
        )
    } else {
        match attempt
            .settlement
            .as_ref()
            .map(|accepted| &accepted.command.report)
        {
            Some(AttemptReport::Completed(report)) => match report.outcome {
                ProgramOutcome::Success { .. } => (AttemptState::Succeeded, false),
                ProgramOutcome::Failure { .. } => (AttemptState::Failed, false),
            },
            Some(AttemptReport::Failed(report)) => {
                (AttemptState::Failed, retryable_failure(report.error.kind))
            }
            None => (AttemptState::Lost, true),
        }
    };
    let retry_at = if retryable && task.attempt_count < task.input.retry_policy.max_attempts {
        Some(add_time(now, task.input.retry_policy.retry_delay_ms)?)
    } else {
        None
    };
    attempt.state = state;
    attempt.finished_at = Some(now);
    task.current_attempt_id = None;
    if task.cancel_requested_at.is_some() {
        task.state = TaskState::Cancelled;
        events.push(history(
            task,
            Some(attempt),
            now,
            TransitionReason::Cancelled,
        ));
    } else if state == AttemptState::Succeeded {
        task.state = TaskState::Succeeded;
        events.push(history(
            task,
            Some(attempt),
            now,
            TransitionReason::Succeeded,
        ));
    } else if let Some(available_at) = retry_at {
        task.state = TaskState::Queued;
        task.available_at = available_at;
        events.push(history(
            task,
            Some(attempt),
            now,
            TransitionReason::RetryScheduled,
        ));
    } else {
        task.state = TaskState::Failed;
        events.push(history(task, Some(attempt), now, TransitionReason::Failed));
    }
    if task.state.is_terminal() {
        task.terminal_at = Some(now);
    }
    Ok(())
}

/// One classification shared by scheduling and read-model consistency checks.
pub(crate) fn retryable_failure(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::Unavailable
            | ErrorKind::Io
            | ErrorKind::Capacity
            | ErrorKind::Runtime
            | ErrorKind::TimedOut
            | ErrorKind::Cancelled
    )
}
