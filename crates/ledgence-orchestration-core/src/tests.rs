mod completion;
use super::*;
use ledgence_worker_api::{
    Digest, Error, ErrorKind, ExecutionContext, ExecutionFailure, ExecutionReport,
    InvocationIdentity, Phase, ProgramOutcome,
};

const NOW: Timestamp = 1_000_000;

fn command() -> SubmitCommand {
    SubmitCommand {
        idempotency_key: "invoice:1042".into(),
        input: SubmitTask::decode(
            br#"{
            "tenant_id":"acme","namespace":"billing","queue":"python",
            "program":{"id":"invoice","version":"1.0.0"},
            "data":{"amount":9007199254740993,"details":[null,"left\u0000right",-0.0]}
        }"#,
        )
        .unwrap(),
        origin_trace: Some(trace("1111111111111111")),
    }
}

fn trace(span: &str) -> TraceContext {
    TraceContext {
        traceparent: format!("00-0af7651916cd43dd8448eb211c80319c-{span}-01"),
        tracestate: None,
    }
}

fn descriptor(command: &SubmitCommand) -> ProgramDescriptor {
    ProgramDescriptor {
        program: command.input.program.clone(),
        digest: Digest(format!("sha256:{}", "a".repeat(64))),
        size: 1234,
    }
}

fn queued() -> TaskSnapshot {
    let command = command();
    submit(
        &command,
        &descriptor(&command),
        "task_1042",
        "run_1042",
        NOW,
    )
    .unwrap()
    .task
}

fn session(task: &TaskSnapshot) -> WorkerSession {
    open_session("worker_boot_1", task.scope(), &task.input.queue, 2, NOW).unwrap()
}

fn acquire_command(session: &WorkerSession, sequence: u64) -> AcquireCommand {
    AcquireCommand {
        scope: session.scope.clone(),
        queue: session.queue.clone(),
        worker_session_id: session.id.clone(),
        consumer_id: 0,
        sequence,
    }
}

fn ids(number: u32) -> AttemptIds {
    AttemptIds {
        attempt_id: format!("attempt_{number}"),
        lease_id: format!("lease_{number}"),
        event_id: format!("event_{number}"),
        trace: Some(trace(&format!("{number:016x}"))),
    }
}

#[derive(Clone)]
struct Claimed {
    task: TaskSnapshot,
    attempt: AttemptSnapshot,
    cursor: ConsumerCursor,
    session: WorkerSession,
}

fn claimed_from(change: AcquireTransition, session: &WorkerSession) -> Claimed {
    assert!(matches!(change.reply, AcquireReply::Assigned { .. }));
    let changes = change.changes.unwrap();
    assert_eq!(changes.history.len(), 1);
    assert_eq!(changes.history[0].reason, TransitionReason::Claimed);
    Claimed {
        task: changes.task,
        attempt: changes.attempt.unwrap(),
        cursor: change.cursor,
        session: session.clone(),
    }
}

fn claim_task(task: TaskSnapshot) -> Claimed {
    let session = session(&task);
    let change = acquire(
        Acquisition {
            session: Some(&session),
            cursor: None,
            previous: None,
            candidate: Some(&task),
            ids: Some(&ids(1)),
        },
        &acquire_command(&session, 1),
        NOW,
    )
    .unwrap();
    claimed_from(change, &session)
}

fn claimed() -> Claimed {
    claim_task(queued())
}

fn next_acquisition(
    current: &Claimed,
    task: &TaskSnapshot,
    previous: &AttemptSnapshot,
    now: Timestamp,
) -> Result<AcquireTransition> {
    acquire(
        Acquisition {
            session: Some(&current.session),
            cursor: Some(&current.cursor),
            previous: Some((task, previous)),
            candidate: Some(task),
            ids: Some(&ids(task.attempt_count + 1)),
        },
        &acquire_command(&current.session, current.cursor.command.sequence + 1),
        now,
    )
}

fn context(attempt: &AttemptSnapshot) -> Box<ExecutionContext> {
    Box::new(ExecutionContext {
        identity: InvocationIdentity::from(&attempt.event),
        program: attempt.descriptor.program.clone(),
        digest: attempt.descriptor.digest.clone(),
    })
}

fn success(attempt: &AttemptSnapshot, quiescence: Quiescence) -> SettleCommand {
    SettleCommand {
        owner: attempt.lease.owner.clone(),
        operation_id: format!("settle_{}", attempt.lease.owner.attempt_id),
        report: AttemptReport::Completed(ExecutionReport {
            context: context(attempt),
            process_id: 42,
            reused_process: true,
            outcome: ProgramOutcome::Success {
                output: json!({"count":1,"zero":0.0}),
            },
            elapsed_ms: 20,
        }),
        quiescence,
        processing_trace: Some(trace("3333333333333333")),
    }
}

fn failure(attempt: &AttemptSnapshot, kind: ErrorKind, quiescence: Quiescence) -> SettleCommand {
    SettleCommand {
        report: AttemptReport::Failed(ExecutionFailure {
            context: context(attempt),
            error: Error::new(kind, "adapter failure"),
            cleanup_error: None,
            phase: Phase::Preparation,
            execution_may_have_started: false,
        }),
        ..success(attempt, quiescence)
    }
}

fn snapshot(task: &TaskSnapshot, attempt: Option<&AttemptSnapshot>) -> Vec<u8> {
    serde_json::to_vec(&(task, attempt)).unwrap()
}

fn renewal(attempt: &AttemptSnapshot, sequence: u64, intent: RenewIntent) -> RenewCommand {
    RenewCommand {
        owner: attempt.lease.owner.clone(),
        sequence,
        intent,
    }
}

#[test]
fn submission_replay_preserves_binding_and_origin_while_ignoring_transport_trace() {
    let original = command();
    let bound = submit(
        &original,
        &descriptor(&original),
        "task_1042",
        "run_1042",
        NOW,
    )
    .unwrap();
    assert_eq!(bound.task.state, TaskState::Queued);
    assert_eq!(bound.history[0].reason, TransitionReason::Submitted);
    let before = snapshot(&bound.task, None);
    let mut replay = original.clone();
    replay.origin_trace = Some(trace("2222222222222222"));
    replay.input.data = ledgence_worker_api::decode_json(
        br#"{
        "details":[null,"left\u0000right",-0e0],"amount":9007199254740993
    }"#,
    )
    .unwrap();
    assert!(replay_submission(&bound.task, &replay).is_ok());
    assert_eq!(snapshot(&bound.task, None), before);
    assert_eq!(bound.task.origin_trace, original.origin_trace);
    assert_eq!(bound.task.descriptor, descriptor(&original));
}

#[test]
fn changed_submission_or_descriptor_conflicts_without_changing_accepted_input() {
    let original = command();
    let task = queued();
    let before = snapshot(&task, None);
    let mut changed = original.clone();
    changed.input.program.version = "2.0.0".into();
    assert_eq!(
        replay_submission(&task, &changed).unwrap_err(),
        ContractError::Conflict
    );
    assert_eq!(
        submit(&changed, &descriptor(&original), "other", "run", NOW).unwrap_err(),
        ContractError::Conflict
    );
    changed = original.clone();
    changed.input.retry_policy.max_attempts = 2;
    assert_eq!(
        replay_submission(&task, &changed).unwrap_err(),
        ContractError::Conflict
    );
    changed = original.clone();
    changed.input.correlation_key = Some("other-business-reference".into());
    assert_eq!(
        replay_submission(&task, &changed).unwrap_err(),
        ContractError::Conflict
    );
    changed = original.clone();
    changed.idempotency_key = "different-operation".into();
    assert_eq!(
        replay_submission(&task, &changed).unwrap_err(),
        ContractError::Conflict
    );
    assert_eq!(snapshot(&task, None), before);
}

#[test]
fn retry_gets_new_attempt_and_event_but_preserves_task_data_and_pinned_program() {
    let first = claimed();
    let original_event = serde_json::to_vec(first.attempt.event.value()).unwrap();
    let completed = settle(
        &first.task,
        &first.attempt,
        &failure(
            &first.attempt,
            ErrorKind::Unavailable,
            Quiescence::Confirmed,
        ),
        NOW + 10,
    )
    .unwrap();
    assert_eq!(completed.task.state, TaskState::Queued);
    assert_eq!(completed.task.available_at, NOW + 10 + 5_000);
    let prior = completed.attempt.unwrap();
    assert_eq!(prior.state, AttemptState::Failed);
    assert_eq!(
        next_acquisition(
            &first,
            &completed.task,
            &prior,
            completed.task.available_at - 1
        )
        .unwrap_err(),
        ContractError::Busy
    );
    let second = claimed_from(
        next_acquisition(&first, &completed.task, &prior, completed.task.available_at).unwrap(),
        &first.session,
    );
    assert_eq!(second.task.task_id, first.task.task_id);
    assert_eq!(second.task.run_id, first.task.run_id);
    assert_eq!(second.task.attempt_count, 2);
    assert_eq!(second.attempt.lease.owner.generation, 2);
    assert_ne!(
        second.attempt.event.attempt_id(),
        first.attempt.event.attempt_id()
    );
    assert_ne!(
        second.attempt.event.value()["id"],
        first.attempt.event.value()["id"]
    );
    assert_eq!(second.attempt.event.value()["ldgattemptno"], json!(2));
    assert_eq!(
        canonical_json_bytes(&second.attempt.event.value()["data"]).unwrap(),
        canonical_json_bytes(&first.task.input.data).unwrap()
    );
    assert_eq!(second.attempt.descriptor, first.attempt.descriptor);
    assert_eq!(
        second.task.input.canonical_bytes().unwrap(),
        first.task.input.canonical_bytes().unwrap()
    );
    assert_eq!(
        serde_json::to_vec(first.attempt.event.value()).unwrap(),
        original_event
    );
}

#[test]
fn empty_acquisition_replay_stays_empty_and_only_the_next_sequence_can_claim() {
    let task = queued();
    let session = session(&task);
    let poll = acquire_command(&session, 1);
    let empty = acquire(
        Acquisition {
            session: Some(&session),
            cursor: None,
            previous: None,
            candidate: None,
            ids: None,
        },
        &poll,
        NOW,
    )
    .unwrap();
    assert!(matches!(empty.reply, AcquireReply::Empty { sequence: 1 }));
    let replay = acquire(
        Acquisition {
            session: Some(&session),
            cursor: Some(&empty.cursor),
            previous: None,
            candidate: Some(&task),
            ids: Some(&ids(1)),
        },
        &poll,
        NOW + 1,
    )
    .unwrap();
    assert!(matches!(replay.reply, AcquireReply::Empty { sequence: 1 }));
    assert!(replay.changes.is_none());
    let next = acquire(
        Acquisition {
            session: Some(&session),
            cursor: Some(&empty.cursor),
            previous: None,
            candidate: Some(&task),
            ids: Some(&ids(1)),
        },
        &acquire_command(&session, 2),
        NOW + 2,
    )
    .unwrap();
    assert!(matches!(
        next.reply,
        AcquireReply::Assigned { sequence: 2, .. }
    ));
    let old = acquire(
        Acquisition {
            session: Some(&session),
            cursor: Some(&next.cursor),
            previous: None,
            candidate: None,
            ids: None,
        },
        &poll,
        NOW + 3,
    )
    .unwrap_err();
    assert_eq!(old, ContractError::ObsoleteOperation);
}

#[test]
fn assigned_acquisition_replay_returns_fresh_remaining_authority_and_blocks_overtaking() {
    let first = claimed();
    let before = snapshot(&first.task, Some(&first.attempt));
    let command = acquire_command(&first.session, 1);
    let replay = acquire(
        Acquisition {
            session: Some(&first.session),
            cursor: Some(&first.cursor),
            previous: Some((&first.task, &first.attempt)),
            candidate: None,
            ids: None,
        },
        &command,
        NOW + 7_000,
    )
    .unwrap();
    let AcquireReply::Assigned { assignment, .. } = replay.reply else {
        panic!("expected recovered assignment")
    };
    assert_eq!(
        assignment.event.value()["id"],
        first.attempt.event.value()["id"]
    );
    assert_eq!(assignment.lease, first.attempt.lease);
    assert_eq!(assignment.authority.remaining_ms, LEASE_DURATION_MS - 7_000);
    assert!(!assignment.authority.dispatch_allowed);
    assert!(replay.changes.is_none());
    assert_eq!(
        next_acquisition(&first, &first.task, &first.attempt, NOW + 7_000).unwrap_err(),
        ContractError::Busy
    );
    assert_eq!(snapshot(&first.task, Some(&first.attempt)), before);
    let expired = acquire(
        Acquisition {
            session: Some(&first.session),
            cursor: Some(&first.cursor),
            previous: Some((&first.task, &first.attempt)),
            candidate: None,
            ids: None,
        },
        &command,
        first.attempt.lease.expires_at,
    )
    .unwrap();
    assert!(matches!(
        expired.reply,
        AcquireReply::OwnershipLost { sequence: 1, .. }
    ));
}

#[test]
fn acquisition_sequences_and_registered_consumer_identity_are_enforced() {
    let task = queued();
    let session = session(&task);
    for sequence in [0, 2] {
        assert_eq!(
            acquire(
                Acquisition {
                    session: Some(&session),
                    cursor: None,
                    previous: None,
                    candidate: None,
                    ids: None
                },
                &acquire_command(&session, sequence),
                NOW
            )
            .unwrap_err(),
            ContractError::OutOfOrder
        );
    }
    let first = claimed();
    assert_eq!(
        acquire(
            Acquisition {
                session: Some(&first.session),
                cursor: Some(&first.cursor),
                previous: None,
                candidate: None,
                ids: None
            },
            &acquire_command(&first.session, 3),
            NOW
        )
        .unwrap_err(),
        ContractError::OutOfOrder
    );
    let mut invalid = acquire_command(&session, 1);
    invalid.consumer_id = session.concurrency;
    assert!(matches!(
        acquire(
            Acquisition {
                session: Some(&session),
                cursor: None,
                previous: None,
                candidate: None,
                ids: None
            },
            &invalid,
            NOW
        ),
        Err(ContractError::InvalidInput(_))
    ));
    invalid.consumer_id = 0;
    invalid.queue = "another-queue".into();
    assert!(matches!(
        acquire(
            Acquisition {
                session: Some(&session),
                cursor: None,
                previous: None,
                candidate: None,
                ids: None
            },
            &invalid,
            NOW
        ),
        Err(ContractError::InvalidInput(_))
    ));
}

#[test]
fn unknown_or_expired_sessions_cannot_be_recreated_by_acquisition_or_renewal() {
    let current = claimed();
    let command = acquire_command(&current.session, 1);
    assert_eq!(
        acquire(
            Acquisition {
                session: None,
                cursor: None,
                previous: None,
                candidate: None,
                ids: None
            },
            &command,
            NOW
        )
        .unwrap_err(),
        ContractError::UnknownSession
    );
    assert_eq!(
        acquire(
            Acquisition {
                session: Some(&current.session),
                cursor: None,
                previous: None,
                candidate: None,
                ids: None
            },
            &command,
            current.session.expires_at
        )
        .unwrap_err(),
        ContractError::SessionExpired
    );
    let renewal = renewal(&current.attempt, 1, RenewIntent::KeepAlive);
    assert_eq!(
        renew(&current.task, &current.attempt, None, &renewal, NOW + 1).unwrap_err(),
        ContractError::UnknownSession
    );
    assert_eq!(
        renew(
            &current.task,
            &current.attempt,
            Some(&current.session),
            &renewal,
            current.session.expires_at
        )
        .unwrap_err(),
        ContractError::SessionExpired
    );
    assert_eq!(
        extend_session(&current.session, current.session.expires_at).unwrap_err(),
        ContractError::SessionExpired
    );
    let extended = extend_session(&current.session, NOW + 10).unwrap();
    assert_eq!(extended.expires_at, current.session.expires_at + 10);
    assert_eq!(current.attempt.lease.expires_at, NOW + LEASE_DURATION_MS);
}

#[test]
fn duplicate_renewal_does_not_extend_lease_and_changed_or_out_of_order_replays_fail() {
    let current = claimed();
    let first_command = renewal(&current.attempt, 1, RenewIntent::KeepAlive);
    let first = renew(
        &current.task,
        &current.attempt,
        Some(&current.session),
        &first_command,
        NOW + 10_000,
    )
    .unwrap();
    let attempt = first.attempt.unwrap();
    assert_eq!(attempt.lease.expires_at, NOW + 70_000);
    let replay = renew(
        &first.task,
        &attempt,
        Some(&current.session),
        &first_command,
        NOW + 15_000,
    )
    .unwrap();
    assert_eq!(
        replay.attempt.as_ref().unwrap().lease.expires_at,
        NOW + 70_000
    );
    assert_eq!(replay.reply.remaining_ms, 55_000);
    assert!(replay.history.is_empty());
    let changed = renewal(&attempt, 1, RenewIntent::Dispatch);
    assert_eq!(
        renew(
            &first.task,
            &attempt,
            Some(&current.session),
            &changed,
            NOW + 15_000
        )
        .unwrap_err(),
        ContractError::Conflict
    );
    for sequence in [0, 3] {
        assert_eq!(
            renew(
                &first.task,
                &attempt,
                Some(&current.session),
                &renewal(&attempt, sequence, RenewIntent::KeepAlive),
                NOW + 15_000
            )
            .unwrap_err(),
            ContractError::OutOfOrder
        );
    }
    let second = renew(
        &first.task,
        &attempt,
        Some(&current.session),
        &renewal(&attempt, 2, RenewIntent::KeepAlive),
        NOW + 20_000,
    )
    .unwrap();
    assert_eq!(
        renew(
            &second.task,
            second.attempt.as_ref().unwrap(),
            Some(&current.session),
            &first_command,
            NOW + 21_000
        )
        .unwrap_err(),
        ContractError::ObsoleteOperation
    );
}

#[test]
fn exact_lease_expiry_rejects_renewal_and_completion_and_expires_only_once() {
    let current = claimed();
    let at = current.attempt.lease.expires_at;
    let early = expire(&current.task, &current.attempt, at - 1).unwrap();
    assert!(!early.reply);
    assert!(early.history.is_empty());
    assert_eq!(
        renew(
            &current.task,
            &current.attempt,
            Some(&current.session),
            &renewal(&current.attempt, 1, RenewIntent::KeepAlive),
            at
        )
        .unwrap_err(),
        ContractError::OwnershipLost
    );
    assert_eq!(
        settle(
            &current.task,
            &current.attempt,
            &success(&current.attempt, Quiescence::Confirmed),
            at
        )
        .unwrap_err(),
        ContractError::OwnershipLost
    );
    let expired = expire(&current.task, &current.attempt, at).unwrap();
    assert!(expired.reply);
    let attempt = expired.attempt.unwrap();
    assert_eq!(attempt.state, AttemptState::Lost);
    assert_eq!(expired.task.state, TaskState::Queued);
    assert_eq!(attempt.finished_at, Some(at));
    let replay = expire(&expired.task, &attempt, at + 1).unwrap();
    assert!(!replay.reply);
    assert!(replay.history.is_empty());
}

#[test]
fn dispatch_authorization_is_monotone_and_deadline_blocks_further_start() {
    let current = claimed();
    assert!(!current.attempt.execution_may_have_started);
    let first = renew(
        &current.task,
        &current.attempt,
        Some(&current.session),
        &renewal(&current.attempt, 1, RenewIntent::Dispatch),
        NOW + 1,
    )
    .unwrap();
    let attempt = first.attempt.unwrap();
    assert!(attempt.execution_may_have_started);
    assert!(first.reply.dispatch_allowed);
    assert_eq!(
        first.history[0].reason,
        TransitionReason::DispatchAuthorized
    );
    let next = renew(
        &first.task,
        &attempt,
        Some(&current.session),
        &renewal(&attempt, 2, RenewIntent::Dispatch),
        NOW + 2,
    )
    .unwrap();
    assert!(next.history.is_empty());
    assert!(next.attempt.unwrap().execution_may_have_started);
    let mut input = command();
    input.input.attempt_timeout_ms = 60_000;
    let task = submit(&input, &descriptor(&input), "short_task", "short_run", NOW)
        .unwrap()
        .task;
    let short = claim_task(task);
    let kept_alive = renew(
        &short.task,
        &short.attempt,
        Some(&short.session),
        &renewal(&short.attempt, 1, RenewIntent::KeepAlive),
        NOW + 30_000,
    )
    .unwrap();
    let attempt = kept_alive.attempt.unwrap();
    let deadline = renew(
        &kept_alive.task,
        &attempt,
        Some(&short.session),
        &renewal(&attempt, 2, RenewIntent::Dispatch),
        NOW + 60_000,
    )
    .unwrap();
    assert!(!deadline.reply.dispatch_allowed);
    assert!(deadline.reply.cancel_requested);
    assert!(
        !deadline
            .attempt
            .as_ref()
            .unwrap()
            .execution_may_have_started
    );
    assert_eq!(deadline.reply.expires_at, NOW + 60_000 + CLEANUP_GRACE_MS);
}

#[test]
fn accepted_failure_receipt_replays_after_retry_without_mutating_the_new_attempt() {
    let first = claimed();
    let report = failure(&first.attempt, ErrorKind::Io, Quiescence::Confirmed);
    let accepted = settle(&first.task, &first.attempt, &report, NOW + 1).unwrap();
    let old = accepted.attempt.unwrap();
    let second = claimed_from(
        next_acquisition(&first, &accepted.task, &old, accepted.task.available_at).unwrap(),
        &first.session,
    );
    let before = snapshot(&second.task, Some(&second.attempt));
    let replay = settle(
        &second.task,
        &old,
        &report,
        second.attempt.lease.expires_at + 1,
    )
    .unwrap();
    assert!(replay.reply.already_accepted);
    assert_eq!(replay.reply.receipt.accepted_at, NOW + 1);
    assert_eq!(replay.reply.task_state, TaskState::Active);
    assert_eq!(
        replay.task.current_attempt_id,
        Some(second.attempt.lease.owner.attempt_id.clone())
    );
    assert!(replay.history.is_empty());
    assert_eq!(snapshot(&second.task, Some(&second.attempt)), before);
}

#[test]
fn unaccepted_stale_completion_cannot_replace_a_new_attempt() {
    let first = claimed();
    let expired = expire(&first.task, &first.attempt, first.attempt.lease.expires_at).unwrap();
    let old = expired.attempt.unwrap();
    let second = claimed_from(
        next_acquisition(&first, &expired.task, &old, expired.task.available_at).unwrap(),
        &first.session,
    );
    let before = snapshot(&second.task, Some(&second.attempt));
    assert_eq!(
        settle(
            &second.task,
            &old,
            &success(&old, Quiescence::Confirmed),
            expired.task.available_at + 1
        )
        .unwrap_err(),
        ContractError::OwnershipLost
    );
    assert_eq!(snapshot(&second.task, Some(&second.attempt)), before);
}

#[test]
fn accepted_success_replays_exactly_but_changed_report_number_classes_conflict() {
    let current = claimed();
    let report = success(&current.attempt, Quiescence::Confirmed);
    let accepted = settle(&current.task, &current.attempt, &report, NOW + 20).unwrap();
    let attempt = accepted.attempt.unwrap();
    let replay = settle(&accepted.task, &attempt, &report, NOW + SESSION_VALIDITY_MS).unwrap();
    assert!(replay.reply.already_accepted);
    assert_eq!(replay.reply.receipt.accepted_at, NOW + 20);
    assert!(replay.history.is_empty());
    for output in [
        json!({"count":1.0,"zero":0.0}),
        json!({"count":1,"zero":-0.0}),
    ] {
        let mut changed = report.clone();
        let AttemptReport::Completed(ref mut completed) = changed.report else {
            unreachable!()
        };
        completed.outcome = ProgramOutcome::Success { output };
        assert_eq!(
            settle(&accepted.task, &attempt, &changed, NOW + 21).unwrap_err(),
            ContractError::Conflict
        );
    }
    let mut changed = report.clone();
    changed.operation_id = "another-settlement".into();
    assert_eq!(
        settle(&accepted.task, &attempt, &changed, NOW + 21).unwrap_err(),
        ContractError::Conflict
    );
}

#[test]
fn queued_cancellation_is_terminal_and_a_terminal_attempt_does_not_block_retry_cancellation() {
    let task = queued();
    let cancelled = cancel(&task, None, NOW + 1).unwrap();
    assert_eq!(cancelled.task.state, TaskState::Cancelled);
    assert_eq!(cancelled.task.terminal_at, Some(NOW + 1));
    assert!(cancelled.attempt.is_none());
    let session = session(&task);
    assert_eq!(
        acquire(
            Acquisition {
                session: Some(&session),
                cursor: None,
                previous: None,
                candidate: Some(&cancelled.task),
                ids: Some(&ids(1))
            },
            &acquire_command(&session, 1),
            NOW + 2
        )
        .unwrap_err(),
        ContractError::Busy
    );
    let duplicate = cancel(&cancelled.task, None, NOW + 3).unwrap();
    assert!(duplicate.history.is_empty());
    assert_eq!(duplicate.task.cancel_requested_at, Some(NOW + 1));
    let current = claimed();
    let failed = settle(
        &current.task,
        &current.attempt,
        &failure(
            &current.attempt,
            ErrorKind::Unavailable,
            Quiescence::Confirmed,
        ),
        NOW + 1,
    )
    .unwrap();
    assert_eq!(failed.attempt.as_ref().unwrap().state, AttemptState::Failed);
    assert_eq!(failed.task.state, TaskState::Queued);
    let cancelled_retry = cancel(&failed.task, None, NOW + 2).unwrap();
    assert_eq!(cancelled_retry.task.state, TaskState::Cancelled);
}

#[test]
fn cancellation_and_success_follow_task_transaction_order_and_keep_observed_output() {
    let current = claimed();
    let cancelled = cancel(&current.task, Some(&current.attempt), NOW + 1).unwrap();
    assert_eq!(cancelled.task.state, TaskState::Active);
    let attempt = cancelled.attempt.unwrap();
    let control = renew(
        &cancelled.task,
        &attempt,
        Some(&current.session),
        &renewal(&attempt, 1, RenewIntent::Dispatch),
        NOW + 2,
    )
    .unwrap();
    assert!(control.reply.cancel_requested);
    assert!(!control.reply.dispatch_allowed);
    let attempt = control.attempt.unwrap();
    assert!(!attempt.execution_may_have_started);
    let completed = settle(
        &control.task,
        &attempt,
        &success(&attempt, Quiescence::Confirmed),
        NOW + 3,
    )
    .unwrap();
    assert_eq!(completed.task.state, TaskState::Cancelled);
    let finished = completed.attempt.unwrap();
    assert_eq!(finished.state, AttemptState::Cancelled);
    assert!(matches!(
        finished.settlement.unwrap().command.report,
        AttemptReport::Completed(ExecutionReport {
            outcome: ProgramOutcome::Success { .. },
            ..
        })
    ));
    let current = claimed();
    let successful = settle(
        &current.task,
        &current.attempt,
        &success(&current.attempt, Quiescence::Confirmed),
        NOW + 1,
    )
    .unwrap();
    let later_cancel = cancel(&successful.task, successful.attempt.as_ref(), NOW + 2).unwrap();
    assert_eq!(later_cancel.task.state, TaskState::Succeeded);
    assert_eq!(later_cancel.task.cancel_requested_at, None);
    assert!(later_cancel.history.is_empty());
}

#[test]
fn unconfirmed_report_holds_attempt_until_separate_monotone_cleanup_confirmation() {
    let current = claimed();
    let report = success(&current.attempt, Quiescence::Unconfirmed);
    let accepted = settle(&current.task, &current.attempt, &report, NOW + 1).unwrap();
    let attempt = accepted.attempt.unwrap();
    assert_eq!(accepted.task.state, TaskState::Active);
    assert_eq!(attempt.state, AttemptState::Active);
    assert!(attempt.finished_at.is_none());
    assert_eq!(
        next_acquisition(&current, &accepted.task, &attempt, NOW + 2).unwrap_err(),
        ContractError::Busy
    );
    let renewed = renew(
        &accepted.task,
        &attempt,
        Some(&current.session),
        &renewal(&attempt, 1, RenewIntent::Dispatch),
        NOW + 2,
    )
    .unwrap();
    assert!(!renewed.reply.dispatch_allowed);
    let mut changed = report.clone();
    changed.quiescence = Quiescence::Confirmed;
    assert_eq!(
        settle(&accepted.task, &attempt, &changed, NOW + 2).unwrap_err(),
        ContractError::Conflict
    );
    let confirmed =
        confirm_quiescence(&accepted.task, &attempt, &attempt.lease.owner, NOW + 3).unwrap();
    let finished = confirmed.attempt.unwrap();
    assert_eq!(confirmed.task.state, TaskState::Succeeded);
    assert_eq!(finished.quiescence, Quiescence::Confirmed);
    assert_eq!(
        finished.settlement.as_ref().unwrap().command.quiescence,
        Quiescence::Unconfirmed
    );
    assert_eq!(
        finished.settlement.as_ref().unwrap().receipt.accepted_at,
        NOW + 1
    );
    let replay = confirm_quiescence(
        &confirmed.task,
        &finished,
        &finished.lease.owner,
        NOW + SESSION_VALIDITY_MS,
    )
    .unwrap();
    assert_eq!(replay.reply, TaskState::Succeeded);
    assert!(replay.history.is_empty());
}

#[test]
fn unconfirmed_report_expiry_preserves_known_result_and_exposes_unknown_cleanup() {
    for cancelled in [false, true] {
        let current = claimed();
        let report = success(&current.attempt, Quiescence::Unconfirmed);
        let accepted = settle(&current.task, &current.attempt, &report, NOW + 1).unwrap();
        let attempt = accepted.attempt.unwrap();
        let task = if cancelled {
            cancel(&accepted.task, Some(&attempt), NOW + 2)
                .unwrap()
                .task
        } else {
            accepted.task
        };
        let at = attempt.lease.expires_at;
        let expired = expire(&task, &attempt, at).unwrap();
        let finished = expired.attempt.unwrap();
        assert_eq!(finished.quiescence, Quiescence::Unconfirmed);
        assert_eq!(
            finished.settlement.as_ref().unwrap().receipt.accepted_at,
            NOW + 1
        );
        assert_eq!(
            expired.task.state,
            if cancelled {
                TaskState::Cancelled
            } else {
                TaskState::Succeeded
            }
        );
        assert_eq!(
            finished.state,
            if cancelled {
                AttemptState::Lost
            } else {
                AttemptState::Succeeded
            }
        );
        assert_eq!(
            confirm_quiescence(&expired.task, &finished, &finished.lease.owner, at + 1)
                .unwrap_err(),
            ContractError::OwnershipLost
        );
    }
}

#[test]
fn retries_follow_failure_classification_and_stop_at_the_total_attempt_budget() {
    for (kind, retryable) in [
        (ErrorKind::Unavailable, true),
        (ErrorKind::Integrity, false),
        (ErrorKind::InvalidInput, false),
    ] {
        let current = claimed();
        let completed = settle(
            &current.task,
            &current.attempt,
            &failure(&current.attempt, kind, Quiescence::Confirmed),
            NOW + 1,
        )
        .unwrap();
        assert_eq!(
            completed.task.state,
            if retryable {
                TaskState::Queued
            } else {
                TaskState::Failed
            }
        );
    }
    let current = claimed();
    let mut application_failure = success(&current.attempt, Quiescence::Confirmed);
    let AttemptReport::Completed(ref mut report) = application_failure.report else {
        unreachable!()
    };
    report.outcome = ProgramOutcome::Failure {
        kind: "business_rejection".into(),
        message: "cannot invoice".into(),
    };
    assert_eq!(
        settle(
            &current.task,
            &current.attempt,
            &application_failure,
            NOW + 1
        )
        .unwrap()
        .task
        .state,
        TaskState::Failed
    );
    let mut input = command();
    input.input.retry_policy.max_attempts = 2;
    let task = submit(
        &input,
        &descriptor(&input),
        "task_budget",
        "run_budget",
        NOW,
    )
    .unwrap()
    .task;
    let first = claim_task(task);
    let expired = expire(&first.task, &first.attempt, first.attempt.lease.expires_at).unwrap();
    let old = expired.attempt.unwrap();
    let second = claimed_from(
        next_acquisition(&first, &expired.task, &old, expired.task.available_at).unwrap(),
        &first.session,
    );
    let exhausted = expire(
        &second.task,
        &second.attempt,
        second.attempt.lease.expires_at,
    )
    .unwrap();
    assert_eq!(exhausted.task.state, TaskState::Failed);
    assert_eq!(exhausted.task.attempt_count, 2);
    assert_eq!(exhausted.attempt.unwrap().state, AttemptState::Lost);
    assert!(exhausted.task.terminal_at.is_some());
    assert!(exhausted.task.current_attempt_id.is_none());
}

#[test]
fn rejected_commands_leave_input_snapshots_unchanged_and_do_not_grant_authority() {
    let current = claimed();
    let before = snapshot(&current.task, Some(&current.attempt));
    let mut wrong_owner = renewal(&current.attempt, 1, RenewIntent::Dispatch);
    wrong_owner.owner.worker_session_id = "another_boot".into();
    assert_eq!(
        renew(
            &current.task,
            &current.attempt,
            Some(&current.session),
            &wrong_owner,
            NOW + 1
        )
        .unwrap_err(),
        ContractError::OwnershipLost
    );
    let mut wrong_report = success(&current.attempt, Quiescence::Confirmed);
    let AttemptReport::Completed(ref mut report) = wrong_report.report else {
        unreachable!()
    };
    report.context.digest = Digest(format!("sha256:{}", "b".repeat(64)));
    assert_eq!(
        settle(&current.task, &current.attempt, &wrong_report, NOW + 1).unwrap_err(),
        ContractError::Conflict
    );
    assert!(matches!(
        confirm_quiescence(
            &current.task,
            &current.attempt,
            &current.attempt.lease.owner,
            NOW + 1
        ),
        Err(ContractError::InvalidInput(_))
    ));
    assert!(matches!(
        cancel(&current.task, None, NOW + 1),
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(snapshot(&current.task, Some(&current.attempt)), before);
    assert!(!current.attempt.execution_may_have_started);
    assert!(current.attempt.settlement.is_none());
}

#[path = "observation_tests.rs"]
mod observation;

#[test]
fn workflow_decision_envelopes_preserve_application_depth_without_widening_ordinary_outputs() {
    let mut value = Value::Null;
    for _ in 0..64 {
        value = json!([value]);
    }
    let mut task = queued();
    task.workflow_id = Some("workflow_depth".into());
    task.workflow_activation_id = Some(task.task_id.clone());
    let current = claim_task(task);
    let decision = json!({"v":1,"activation_id":current.task.task_id,"revision":0,"kind":"complete","output":value});
    WorkflowDecision::decode(&decision).unwrap();
    let mut command = success(&current.attempt, Quiescence::Confirmed);
    let AttemptReport::Completed(ref mut report) = command.report else {
        unreachable!()
    };
    report.outcome = ProgramOutcome::Success {
        output: decision.clone(),
    };
    SettleCommand::decode(&serde_json::to_vec(&command).unwrap()).unwrap();
    let accepted = settle(&current.task, &current.attempt, &command, NOW + 1).unwrap();
    let result = task_result(&accepted.task, accepted.attempt.as_ref()).unwrap();
    result.validate().unwrap();
    assert_eq!(result.task.workflow_id.as_deref(), Some("workflow_depth"));
    assert_eq!(
        result.task.workflow_activation_id.as_deref(),
        Some(current.task.task_id.as_str())
    );
    assert!(matches!(result.outcome,Some(TaskOutcome::Succeeded{output,..}) if output == decision));

    let ordinary = claimed();
    let mut ordinary_command = success(&ordinary.attempt, Quiescence::Confirmed);
    let AttemptReport::Completed(ref mut report) = ordinary_command.report else {
        unreachable!()
    };
    report.outcome = ProgramOutcome::Success { output: decision };
    assert!(SettleCommand::decode(&serde_json::to_vec(&ordinary_command).unwrap()).is_err());
    report_marker(&mut ordinary_command);
    assert_eq!(
        settle(
            &ordinary.task,
            &ordinary.attempt,
            &ordinary_command,
            NOW + 1
        )
        .unwrap_err(),
        ContractError::Conflict
    );

    fn report_marker(command: &mut SettleCommand) {
        let AttemptReport::Completed(ref mut report) = command.report else {
            unreachable!()
        };
        report.context.identity.workflow_id = Some("workflow_depth".into());
        report.context.identity.activation_id = Some(report.context.identity.task_id.clone());
    }
}

#[test]
fn nested_workflow_ancestry_is_bound_to_each_attempt_and_its_report() {
    let mut task = queued();
    task.workflow_id = Some("child".into());
    task.parent_workflow_id = Some("parent".into());
    task.root_workflow_id = Some("root".into());
    let current = claim_task(task);
    assert_eq!(
        current.attempt.event.value()["ldgparentworkflowid"],
        "parent"
    );
    assert_eq!(current.attempt.event.value()["ldgrootworkflowid"], "root");
    let command = success(&current.attempt, Quiescence::Confirmed);
    let accepted = settle(&current.task, &current.attempt, &command, NOW + 1).unwrap();
    task_result(&accepted.task, accepted.attempt.as_ref()).unwrap();
    for field in ["parent", "root", "workflow"] {
        let mut changed = current.task.clone();
        match field {
            "parent" => changed.parent_workflow_id = Some("other".into()),
            "root" => changed.root_workflow_id = Some("other".into()),
            _ => changed.workflow_id = Some("other".into()),
        }
        assert!(settle(&changed, &current.attempt, &command, NOW + 1).is_err());
        assert!(task_status(&changed, Some(&current.attempt)).is_err());
    }
    let mut changed = command;
    let AttemptReport::Completed(ref mut report) = changed.report else {
        unreachable!()
    };
    report.context.identity.parent_workflow_id = Some("other".into());
    assert_eq!(
        settle(&current.task, &current.attempt, &changed, NOW + 1).unwrap_err(),
        ContractError::Conflict
    );
}
