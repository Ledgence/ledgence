//! Pure projection of durable logical outcomes; never a lifecycle transition.

use super::*;
use ledgence_worker_api::ProgramOutcome;

/// Project validated records selected from the same durable snapshot.
/// `latest` must be generation `task.attempt_count`, including for queued retries.
pub fn task_status(task: &TaskSnapshot, latest: Option<&AttemptSnapshot>) -> Result<TaskStatus> {
    let status = TaskStatus {
        scope: task.scope(),
        task_id: task.task_id.clone(),
        run_id: task.run_id.clone(),
        workflow_id: task.workflow_id.clone(),
        workflow_activation_id: task.workflow_activation_id.clone(),
        queue: task.input.queue.clone(),
        correlation_key: task.input.correlation_key.clone(),
        state: task.state,
        attempt_count: task.attempt_count,
        current_attempt_id: task.current_attempt_id.clone(),
        latest_attempt_id: latest.map(|attempt| attempt.lease.owner.attempt_id.clone()),
        submitted_at: task.submitted_at,
        available_at: task.available_at,
        terminal_at: task.terminal_at,
        cancel_requested_at: task.cancel_requested_at,
    };
    status.validate()?;
    if let Some(attempt) = latest {
        validate_attempt(task, attempt).map_err(|_| corrupt())?;
        if attempt.lease.owner.generation != task.attempt_count
            || (attempt.state == AttemptState::Active) != (task.state == TaskState::Active)
            || (attempt.state == AttemptState::Active) != attempt.finished_at.is_none()
            || (attempt.quiescence == Quiescence::Confirmed && attempt.settlement.is_none())
            || (attempt.state == AttemptState::Lost && attempt.quiescence == Quiescence::Confirmed)
            || (matches!(task.state, TaskState::Succeeded | TaskState::Failed)
                && task.terminal_at != attempt.finished_at)
            || (task.state == TaskState::Queued
                && !matches!(attempt.state, AttemptState::Failed | AttemptState::Lost))
            || (task.state == TaskState::Cancelled
                && !matches!(
                    attempt.state,
                    AttemptState::Cancelled | AttemptState::Failed | AttemptState::Lost
                ))
        {
            return Err(corrupt());
        }
        if task.state == TaskState::Active && attempt.quiescence == Quiescence::Confirmed {
            return Err(corrupt());
        }
        if matches!(task.state, TaskState::Queued | TaskState::Failed) {
            let retryable = match (
                attempt.state,
                attempt.settlement.as_ref().map(|s| &s.command.report),
            ) {
                (AttemptState::Lost, None) => true,
                (AttemptState::Failed, Some(AttemptReport::Failed(report))) => {
                    retryable_failure(report.error.kind)
                }
                (AttemptState::Failed, Some(AttemptReport::Completed(report)))
                    if matches!(report.outcome, ProgramOutcome::Failure { .. }) =>
                {
                    false
                }
                _ => return Err(corrupt()),
            };
            let retry_due = retryable && task.attempt_count < task.input.retry_policy.max_attempts;
            if (task.state == TaskState::Queued) != retry_due {
                return Err(corrupt());
            }
        }
        if let Some(accepted) = &attempt.settlement {
            validate_report(attempt, &accepted.command).map_err(|_| corrupt())?;
            if accepted.command.owner != attempt.lease.owner
                || accepted.receipt.task_id != task.task_id
                || accepted.receipt.attempt_id != attempt.lease.owner.attempt_id
                || accepted.receipt.operation_id != accepted.command.operation_id
                || (accepted.command.quiescence == Quiescence::Confirmed
                    && attempt.quiescence != Quiescence::Confirmed)
                || (matches!(accepted.command.report, AttemptReport::Completed(_))
                    && !attempt.execution_may_have_started)
                || matches!(&accepted.command.report, AttemptReport::Failed(report) if report.execution_may_have_started && !attempt.execution_may_have_started)
            {
                return Err(corrupt());
            }
        }
    }
    Ok(status)
}

/// A stored report is visible as a logical outcome only after task finalization.
pub fn task_result(task: &TaskSnapshot, latest: Option<&AttemptSnapshot>) -> Result<TaskResult> {
    let status = task_status(task, latest)?;
    let outcome = match task.state {
        TaskState::Queued | TaskState::Active => None,
        TaskState::Cancelled => Some(TaskOutcome::Cancelled {}),
        TaskState::Succeeded | TaskState::Failed => {
            let attempt = latest.ok_or_else(corrupt)?;
            let attempt_id = attempt.lease.owner.attempt_id.clone();
            let quiescence = attempt.quiescence;
            let execution_may_have_started = attempt.execution_may_have_started;
            let report = attempt
                .settlement
                .as_ref()
                .map(|value| &value.command.report);
            match (task.state, attempt.state, report) {
                (
                    TaskState::Succeeded,
                    AttemptState::Succeeded,
                    Some(AttemptReport::Completed(report)),
                ) => {
                    let ProgramOutcome::Success { output } = &report.outcome else {
                        return Err(corrupt());
                    };
                    Some(TaskOutcome::Succeeded {
                        attempt_id,
                        quiescence,
                        execution_may_have_started,
                        output: output.clone(),
                    })
                }
                (
                    TaskState::Failed,
                    AttemptState::Failed,
                    Some(AttemptReport::Completed(report)),
                ) => {
                    let ProgramOutcome::Failure { kind, message } = &report.outcome else {
                        return Err(corrupt());
                    };
                    Some(TaskOutcome::Failed {
                        attempt_id,
                        quiescence,
                        execution_may_have_started,
                        failure: TaskFailure::Application {
                            error: ApplicationError {
                                kind: kind.clone(),
                                message: message.clone(),
                            },
                        },
                    })
                }
                (TaskState::Failed, AttemptState::Failed, Some(AttemptReport::Failed(report))) => {
                    Some(TaskOutcome::Failed {
                        attempt_id,
                        quiescence,
                        execution_may_have_started,
                        failure: TaskFailure::Execution {
                            error: report.error.clone(),
                            phase: report.phase,
                            cleanup_error: report.cleanup_error.clone(),
                        },
                    })
                }
                (TaskState::Failed, AttemptState::Lost, None) => Some(TaskOutcome::Failed {
                    attempt_id,
                    quiescence,
                    execution_may_have_started,
                    failure: TaskFailure::AttemptLost {},
                }),
                _ => return Err(corrupt()),
            }
        }
    };
    let result = TaskResult {
        task: status,
        outcome,
    };
    result.validate()?;
    Ok(result)
}

fn corrupt() -> ContractError {
    ContractError::Unavailable("inconsistent stored task observation".into())
}
