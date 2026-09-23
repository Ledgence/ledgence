//! Cross-component wire budgets: real submission, acquisition, Python, settlement.

use super::*;
use ledgence_orchestration_api::{
    AcquireCommand, AcquireReply, Assignment, AttemptReport, AttemptSnapshot, Quiescence,
    RenewCommand, RenewIntent, RetryPolicy, SETTLEMENT_MAX_BYTES, SUBMISSION_DATA_MAX_BYTES,
    SettleCommand, SubmitCommand, SubmitTask, TaskSnapshot, TaskState, TraceContext,
    canonical_json_bytes,
};
use ledgence_orchestration_core::{
    Acquisition, AttemptIds, acquire, open_session, renew, settle, submit,
};
use ledgence_worker_api::{
    APPLICATION_INPUT_MAX_BYTES, DEFAULT_RUNTIME_FRAME_MAX_BYTES, ExecutionContext,
    ExecutionReport, InvocationIdentity, ProgramDescriptor,
};

const NOW: u64 = 1_000_001; // Includes the longest millisecond timestamp suffix.

/// All 128 bytes need JSON escaping, with distinct values for each ID/scope.
fn maximum_text(index: usize) -> String {
    format!("{}{}", "\\".repeat(index), "\"".repeat(128 - index))
}

fn maximum_trace() -> TraceContext {
    let tracestate = format!("a={},b={}", "\"".repeat(256), "\\".repeat(251));
    assert_eq!(tracestate.len(), 512);
    TraceContext {
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
        tracestate: Some(tracestate),
    }
}

fn delivery_fixture(source: &str) -> (TempDir, PreparedArtifact, SubprocessRuntime) {
    let (dir, artifact, runtime) = fixture(source);
    let manifest = ProgramManifest {
        program: ProgramRef {
            id: "p".repeat(128),
            version: "v".repeat(128),
        },
        ..artifact.manifest().clone()
    };
    let artifact = PreparedArtifact::new(
        artifact.root().to_owned(),
        manifest,
        artifact.digest().clone(),
        Arc::new(()),
    );
    (dir, artifact, runtime)
}

fn submission(artifact: &PreparedArtifact, data: Value) -> SubmitCommand {
    SubmitCommand {
        idempotency_key: "\"".repeat(255),
        input: SubmitTask {
            tenant_id: maximum_text(0),
            namespace: maximum_text(1),
            queue: maximum_text(2),
            program: artifact.manifest().program.clone(),
            correlation_key: Some("\"".repeat(512)),
            data,
            retry_policy: RetryPolicy {
                max_attempts: 1_000,
                ..RetryPolicy::default()
            },
            attempt_timeout_ms: 300_000,
        },
        origin_trace: Some(maximum_trace()),
    }
}

struct Claimed {
    task: TaskSnapshot,
    attempt: AttemptSnapshot,
    assignment: Box<Assignment>,
}

fn claim(artifact: &PreparedArtifact, data: Value, identity_offset: usize) -> Claimed {
    let mut command = submission(artifact, data);
    command.idempotency_key = format!(
        "{}{}",
        "\\".repeat(identity_offset),
        "\"".repeat(255 - identity_offset)
    );
    let command = SubmitCommand::decode(&serde_json::to_vec(&command).unwrap()).unwrap();
    let descriptor = ProgramDescriptor {
        program: artifact.manifest().program.clone(),
        digest: artifact.digest().clone(),
        size: 123,
    };
    let queued = submit(
        &command,
        &descriptor,
        &maximum_text(3 + identity_offset),
        &maximum_text(4 + identity_offset),
        NOW,
    )
    .unwrap()
    .task;
    let session = open_session(
        &maximum_text(8 + identity_offset),
        queued.scope(),
        &queued.input.queue,
        1,
        NOW,
    )
    .unwrap();
    let acquired = acquire(
        Acquisition {
            session: Some(&session),
            cursor: None,
            previous: None,
            candidate: Some(&queued),
            ids: Some(&AttemptIds {
                attempt_id: maximum_text(5 + identity_offset),
                lease_id: maximum_text(6 + identity_offset),
                event_id: maximum_text(7 + identity_offset),
                trace: Some(maximum_trace()),
            }),
        },
        &AcquireCommand {
            scope: session.scope.clone(),
            queue: session.queue.clone(),
            worker_session_id: session.id.clone(),
            consumer_id: 0,
            sequence: 1,
        },
        NOW,
    )
    .unwrap();
    let AcquireReply::Assigned { assignment, .. } = acquired.reply else {
        panic!("valid submission must be assigned");
    };
    let claimed = acquired.changes.unwrap();
    let attempt = claimed.attempt.unwrap();
    let authorized = renew(
        &claimed.task,
        &attempt,
        Some(&session),
        &RenewCommand {
            owner: attempt.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        },
        NOW,
    )
    .unwrap();
    assert!(authorized.reply.dispatch_allowed);
    Claimed {
        task: authorized.task,
        attempt: authorized.attempt.unwrap(),
        assignment,
    }
}

fn settle_outcome(claimed: &Claimed, outcome: ProgramOutcome, pid: u32) -> usize {
    let command = SettleCommand {
        owner: claimed.attempt.lease.owner.clone(),
        operation_id: maximum_text(9),
        report: AttemptReport::Completed(ExecutionReport {
            context: Box::new(ExecutionContext {
                identity: InvocationIdentity::from(&claimed.assignment.event),
                program: claimed.assignment.descriptor.program.clone(),
                digest: claimed.assignment.descriptor.digest.clone(),
            }),
            process_id: pid,
            reused_process: false,
            outcome,
            elapsed_ms: 1,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: Some(maximum_trace()),
    };
    let expected_state = match &command.report {
        AttemptReport::Completed(ExecutionReport {
            outcome: ProgramOutcome::Success { .. },
            ..
        }) => TaskState::Succeeded,
        _ => TaskState::Failed,
    };
    let encoded = canonical_json_bytes(&serde_json::to_value(&command).unwrap()).unwrap();
    assert!(encoded.len() <= SETTLEMENT_MAX_BYTES);
    let decoded = SettleCommand::decode(&encoded).unwrap();
    let accepted = settle(&claimed.task, &claimed.attempt, &decoded, NOW + 1).unwrap();
    assert_eq!(accepted.reply.task_state, expected_state);
    assert!(!accepted.reply.already_accepted);
    assert!(accepted.attempt.unwrap().settlement.is_some());
    encoded.len()
}

#[tokio::test]
async fn maximum_submission_with_escaped_metadata_executes_and_settles_with_defaults() {
    let (_dir, artifact, runtime) = delivery_fixture("def handle(event): return event\n");
    assert_eq!(SUBMISSION_DATA_MAX_BYTES, APPLICATION_INPUT_MAX_BYTES);
    assert_eq!(
        SubprocessConfig::default().max_frame_bytes,
        DEFAULT_RUNTIME_FRAME_MAX_BYTES
    );
    let data = json!("a".repeat(SUBMISSION_DATA_MAX_BYTES - 2));
    assert_eq!(
        serde_json::to_vec(&data).unwrap().len(),
        SUBMISSION_DATA_MAX_BYTES
    );

    let mut oversized = submission(&artifact, data.clone());
    oversized.input.data = json!("a".repeat(SUBMISSION_DATA_MAX_BYTES - 1));
    assert_eq!(
        serde_json::to_vec(&oversized.input.data).unwrap().len(),
        SUBMISSION_DATA_MAX_BYTES + 1
    );
    assert!(SubmitCommand::decode(&serde_json::to_vec(&oversized).unwrap()).is_err());
    assert!(oversized.input.validate().is_err());

    let claimed = claim(&artifact, data, 0);
    let event = &claimed.assignment.event;
    // The initial assignment has attempt number 1; also include the largest
    // supported retry number when proving generated envelope/wrapper headroom.
    let mut largest_metadata = event.value().clone();
    largest_metadata["ldgattemptno"] = json!(claimed.task.input.retry_policy.max_attempts);
    let frame_bytes = serde_json::to_vec(&json!({
        "v": 1, "type": "invoke", "event_id": event.id(),
        "attempt_id": event.attempt_id(), "event": largest_metadata,
    }))
    .unwrap()
    .len()
        + 1;
    assert!(frame_bytes > SUBMISSION_DATA_MAX_BYTES);
    assert!(frame_bytes <= DEFAULT_RUNTIME_FRAME_MAX_BYTES);

    let mut session = ready(runtime.start(artifact, control()).await);
    let result = session
        .execute(
            event.clone().into(),
            RunControl::new(Duration::from_secs(30)),
        )
        .await;
    session.close().await.unwrap();
    let outcome = result.expect("the complete accepted input must fit the default runtime");
    assert_eq!(output(outcome.clone()), *event.value());
    let settlement_bytes = settle_outcome(&claimed, outcome, session.pid());
    eprintln!(
        "maximum submission: data={SUBMISSION_DATA_MAX_BYTES}, full frame={frame_bytes}, settlement={settlement_bytes}"
    );
}

#[tokio::test]
async fn default_result_frame_boundary_settles_and_one_byte_over_is_bounded_failure() {
    // Count the actual Python encoder's complete frame, including its newline.
    // Only the returned string's ASCII bytes vary, giving exact +0/+1 cases.
    let source = format!(
        r#"import json
from ledgence.worker.bootstrap import DEFAULT_RUNTIME_FRAME_MAX_BYTES
assert DEFAULT_RUNTIME_FRAME_MAX_BYTES == {DEFAULT_RUNTIME_FRAME_MAX_BYTES}
def handle(event):
    response = {{'v': 1, 'type': 'result', 'event_id': event['id'],
                'attempt_id': event['ldgattemptid'], 'status': 'success', 'output': ''}}
    encoded_size = len(json.dumps(response, ensure_ascii=False, allow_nan=False,
                                  separators=(',', ':')).encode('utf-8')) + 1
    return 'x' * ({DEFAULT_RUNTIME_FRAME_MAX_BYTES} - encoded_size + event['data'])
"#
    );
    let (_dir, artifact, runtime) = delivery_fixture(&source);
    let mut session = ready(runtime.start(artifact.clone(), control()).await);
    let pid = session.pid();
    for extra_byte in [0, 1] {
        let claimed = claim(&artifact, json!(extra_byte), extra_byte * 16);
        let event = &claimed.assignment.event;
        let outcome = session
            .execute(
                event.clone().into(),
                RunControl::new(Duration::from_secs(30)),
            )
            .await
            .unwrap();
        if extra_byte == 0 {
            let output = output(outcome.clone());
            let frame_bytes = serde_json::to_vec(&json!({
                "v": 1, "type": "result", "event_id": event.id(),
                "attempt_id": event.attempt_id(), "status": "success", "output": output,
            }))
            .unwrap()
            .len()
                + 1;
            assert_eq!(frame_bytes, DEFAULT_RUNTIME_FRAME_MAX_BYTES);
        } else {
            assert!(
                matches!(&outcome, ProgramOutcome::Failure { kind, .. } if kind == "invalid_output")
            );
        }
        let settlement_bytes = settle_outcome(&claimed, outcome, pid);
        assert_eq!(session.pid(), pid);
        eprintln!(
            "default result boundary: extra={extra_byte}, frame limit={DEFAULT_RUNTIME_FRAME_MAX_BYTES}, settlement={settlement_bytes}"
        );
    }
    session.close().await.unwrap();
}
