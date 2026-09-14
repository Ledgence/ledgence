use super::*;

fn read(transition: &Transition<impl Sized>) -> TaskResult {
    task_result(&transition.task, transition.attempt.as_ref()).unwrap()
}

#[test]
fn pending_and_successful_null_remain_distinct_through_cleanup_and_expiry() {
    assert!(task_result(&queued(), None).unwrap().outcome.is_none());
    for confirm in [true, false] {
        let current = claimed();
        assert!(
            task_result(&current.task, Some(&current.attempt))
                .unwrap()
                .outcome
                .is_none()
        );
        let mut command = success(&current.attempt, Quiescence::Unconfirmed);
        let AttemptReport::Completed(report) = &mut command.report else {
            unreachable!()
        };
        report.outcome = ProgramOutcome::Success {
            output: Value::Null,
        };
        let accepted = settle(&current.task, &current.attempt, &command, NOW + 1).unwrap();
        let before = snapshot(&accepted.task, accepted.attempt.as_ref());
        assert!(read(&accepted).outcome.is_none());
        assert_eq!(
            before,
            snapshot(&accepted.task, accepted.attempt.as_ref()),
            "reads must not mutate lifecycle"
        );
        let attempt = accepted.attempt.as_ref().unwrap();
        let result = if confirm {
            read(
                &confirm_quiescence(&accepted.task, attempt, &attempt.lease.owner, NOW + 2)
                    .unwrap(),
            )
        } else {
            // Observing already expired authority does not itself run recovery.
            assert!(read(&accepted).outcome.is_none());
            read(&expire(&accepted.task, attempt, attempt.lease.expires_at).unwrap())
        };
        assert_eq!(
            result.outcome,
            Some(TaskOutcome::Succeeded {
                attempt_id: current.attempt.lease.owner.attempt_id,
                quiescence: if confirm {
                    Quiescence::Confirmed
                } else {
                    Quiescence::Unconfirmed
                },
                execution_may_have_started: true,
                output: Value::Null,
            })
        );
        assert_eq!(
            attempt.settlement.as_ref().unwrap().command.quiescence,
            Quiescence::Unconfirmed
        );
    }
}

#[test]
fn cancellation_never_exposes_success_as_logical_output_or_invents_attempt_provenance() {
    let initial = cancel(&queued(), None, NOW).unwrap();
    assert_eq!(read(&initial).outcome, Some(TaskOutcome::Cancelled {}));
    assert!(read(&initial).task.latest_attempt_id.is_none());
    for confirm in [true, false] {
        let current = claimed();
        let accepted = settle(
            &current.task,
            &current.attempt,
            &success(&current.attempt, Quiescence::Unconfirmed),
            NOW,
        )
        .unwrap();
        let attempt = accepted.attempt.as_ref().unwrap();
        let requested = cancel(&accepted.task, Some(attempt), NOW).unwrap();
        assert!(read(&requested).outcome.is_none());
        let attempt = requested.attempt.as_ref().unwrap();
        let result = if confirm {
            read(&confirm_quiescence(&requested.task, attempt, &attempt.lease.owner, NOW).unwrap())
        } else {
            read(&expire(&requested.task, attempt, attempt.lease.expires_at).unwrap())
        };
        assert_eq!(result.outcome, Some(TaskOutcome::Cancelled {}));
        assert_eq!(
            serde_json::to_value(&result).unwrap()["outcome"],
            json!({"kind":"cancelled"})
        );
        assert_eq!(result.task.latest_attempt_id.as_deref(), Some("attempt_1"));
    }
    let current = claimed();
    let completed = settle(
        &current.task,
        &current.attempt,
        &success(&current.attempt, Quiescence::Confirmed),
        NOW,
    )
    .unwrap();
    let later = cancel(&completed.task, completed.attempt.as_ref(), NOW).unwrap();
    assert_eq!(read(&completed), read(&later));

    let current = claimed();
    let retry = settle(
        &current.task,
        &current.attempt,
        &failure(&current.attempt, ErrorKind::Io, Quiescence::Confirmed),
        NOW,
    )
    .unwrap();
    assert!(read(&retry).outcome.is_none());
    // Same-millisecond earlier failure and queued cancellation remain distinct.
    let cancelled = cancel(&retry.task, None, NOW).unwrap();
    let result = task_result(&cancelled.task, retry.attempt.as_ref()).unwrap();
    assert_eq!(result.outcome, Some(TaskOutcome::Cancelled {}));
    assert_eq!(
        serde_json::to_value(&result).unwrap()["outcome"],
        json!({"kind":"cancelled"})
    );
    assert_eq!(result.task.latest_attempt_id.as_deref(), Some("attempt_1"));
}

#[test]
fn application_execution_and_lost_failures_keep_distinct_evidence() {
    let current = claimed();
    let mut command = success(&current.attempt, Quiescence::Confirmed);
    let AttemptReport::Completed(report) = &mut command.report else {
        unreachable!()
    };
    report.outcome = ProgramOutcome::Failure {
        kind: "InvoiceError".into(),
        message: "invalid invoice".into(),
    };
    let result = read(&settle(&current.task, &current.attempt, &command, NOW).unwrap());
    assert!(
        matches!(result.outcome, Some(TaskOutcome::Failed { failure: TaskFailure::Application { error }, execution_may_have_started: true, .. }) if error.kind == "InvoiceError")
    );

    let mut task = queued();
    task.input.retry_policy.max_attempts = 1;
    let current = claim_task(task);
    let mut command = failure(&current.attempt, ErrorKind::Runtime, Quiescence::Confirmed);
    let cleanup = Error::new(ErrorKind::Io, "cleanup failed");
    let AttemptReport::Failed(report) = &mut command.report else {
        unreachable!()
    };
    report.cleanup_error = Some(cleanup.clone());
    let result = read(&settle(&current.task, &current.attempt, &command, NOW).unwrap());
    assert!(
        matches!(result.outcome, Some(TaskOutcome::Failed { failure: TaskFailure::Execution { error, phase: Phase::Preparation, cleanup_error }, execution_may_have_started: false, .. }) if error.kind == ErrorKind::Runtime && cleanup_error == Some(cleanup))
    );

    for started in [false, true] {
        let mut task = queued();
        task.input.retry_policy.max_attempts = 1;
        let current = claim_task(task);
        let attempt = if started {
            renew(
                &current.task,
                &current.attempt,
                Some(&current.session),
                &renewal(&current.attempt, 1, RenewIntent::Dispatch),
                NOW,
            )
            .unwrap()
            .attempt
            .unwrap()
        } else {
            current.attempt
        };
        let expired = expire(&current.task, &attempt, attempt.lease.expires_at).unwrap();
        assert_eq!(
            read(&expired).outcome,
            Some(TaskOutcome::Failed {
                attempt_id: "attempt_1".into(),
                quiescence: Quiescence::Unconfirmed,
                execution_may_have_started: started,
                failure: TaskFailure::AttemptLost {}
            })
        );
    }
}

#[test]
fn retries_select_latest_generation_and_reject_old_or_missing_attempts() {
    let current = claimed();
    let retry = settle(
        &current.task,
        &current.attempt,
        &failure(&current.attempt, ErrorKind::Io, Quiescence::Confirmed),
        NOW,
    )
    .unwrap();
    let old = retry.attempt.as_ref().unwrap();
    let next = claimed_from(
        next_acquisition(&current, &retry.task, old, retry.task.available_at).unwrap(),
        &current.session,
    );
    assert!(task_result(&next.task, Some(old)).is_err());
    assert!(task_result(&next.task, None).is_err());
    let done = settle(
        &next.task,
        &next.attempt,
        &success(&next.attempt, Quiescence::Confirmed),
        retry.task.available_at,
    )
    .unwrap();
    let result = read(&done);
    assert_eq!(result.task.attempt_count, 2);
    assert!(
        matches!(result.outcome, Some(TaskOutcome::Succeeded { attempt_id, .. }) if attempt_id == "attempt_2")
    );
}

#[test]
fn contradictory_terminal_reports_and_metadata_are_unavailable() {
    let current = claimed();
    let done = settle(
        &current.task,
        &current.attempt,
        &success(&current.attempt, Quiescence::Confirmed),
        NOW,
    )
    .unwrap();
    let mut corrupt_attempts = Vec::new();
    let original = done.attempt.unwrap();
    let mut a = original.clone();
    a.settlement = None;
    corrupt_attempts.push(a);
    let mut a = original.clone();
    a.execution_may_have_started = false;
    corrupt_attempts.push(a);
    let mut a = original.clone();
    a.lease.owner.generation = 2;
    corrupt_attempts.push(a);
    let mut a = original.clone();
    a.finished_at = Some(NOW + 1);
    corrupt_attempts.push(a);
    let mut a = original.clone();
    a.settlement.as_mut().unwrap().receipt.task_id = "other".into();
    corrupt_attempts.push(a);
    let mut a = original.clone();
    a.state = AttemptState::Failed;
    corrupt_attempts.push(a);
    for a in corrupt_attempts {
        assert!(matches!(
            task_result(&done.task, Some(&a)),
            Err(ContractError::Unavailable(_))
        ));
    }
    let mut task = done.task;
    task.cancel_requested_at = Some(NOW);
    assert!(matches!(
        task_result(&task, Some(&original)),
        Err(ContractError::Unavailable(_))
    ));

    let current = claimed();
    let retry = settle(
        &current.task,
        &current.attempt,
        &failure(&current.attempt, ErrorKind::Io, Quiescence::Confirmed),
        NOW,
    )
    .unwrap();
    let mut task = retry.task;
    task.state = TaskState::Failed;
    task.terminal_at = Some(NOW);
    assert!(
        matches!(
            task_result(&task, retry.attempt.as_ref()),
            Err(ContractError::Unavailable(_))
        ),
        "retryable work with remaining attempts cannot already be failed"
    );
}
