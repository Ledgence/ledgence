use ledgence_orchestration_api::*;
use ledgence_worker_api::*;
use serde_json::{Value, json};

fn command(output: Value) -> SettleCommand {
    let event = CloudEvent::new(json!({
        "specversion":"1.0","id":"event","source":"urn:ledgence:orchestrator",
        "type":"com.ledgence.task.invocation.requested.v1","datacontenttype":"application/json",
        "ldgtenantid":"tenant","ldgnamespace":"billing","ldgrunid":"run",
        "ldgtaskid":"task","ldgattemptid":"attempt","ldgattemptno":1,"data":null
    }))
    .unwrap();
    let descriptor = ProgramDescriptor {
        program: ProgramRef {
            id: "program".into(),
            version: "1.0.0".into(),
        },
        digest: Digest(format!("sha256:{}", "a".repeat(64))),
        size: 12,
    };
    let context = ExecutionContext::from(&ExecutionRequest { descriptor, event });
    SettleCommand {
        owner: LeaseOwner {
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "billing".into(),
            },
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        },
        operation_id: "settle".into(),
        report: AttemptReport::Completed(ExecutionReport {
            context: Box::new(context),
            process_id: 42,
            reused_process: true,
            outcome: ProgramOutcome::Success { output },
            elapsed_ms: 10,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    }
}

#[test]
fn complete_settlement_roundtrip_preserves_user_result_and_report_context() {
    let output:Value = decode_json(br#"[18446744073709551615,-9223372036854775808,9007199254740993,1.0,-0.0,2.291712365432881e-09,{"nul":"\u0000"}]"#).unwrap();
    let original = command(output);
    let bytes = serde_json::to_vec(&original).unwrap();
    let decoded = SettleCommand::decode(&bytes).unwrap();
    assert_eq!(
        canonical_json_bytes(&serde_json::to_value(original).unwrap()).unwrap(),
        canonical_json_bytes(&serde_json::to_value(decoded).unwrap()).unwrap()
    );
}

#[test]
fn full_settlement_rejects_duplicate_user_keys_and_out_of_range_integer_tokens() {
    let base =
        String::from_utf8(serde_json::to_vec(&command(json!("REPLACE_OUTPUT"))).unwrap()).unwrap();
    for replacement in [r#"{"invoice":1,"invoice":2}"#, "18446744073709551616"] {
        let malformed = base.replace("\"REPLACE_OUTPUT\"", replacement);
        assert!(SettleCommand::decode(malformed.as_bytes()).is_err());
    }
    let mut unknown = serde_json::to_value(command(Value::Null)).unwrap();
    unknown["unexpected"] = json!(true);
    assert!(SettleCommand::decode(&serde_json::to_vec(&unknown).unwrap()).is_err());
}

#[test]
fn report_envelope_does_not_reduce_supported_output_depth() {
    let mut output = Value::Null;
    for _ in 0..64 {
        output = json!([output]);
    }
    assert!(SettleCommand::decode(&serde_json::to_vec(&command(output.clone())).unwrap()).is_ok());
    assert!(
        SettleCommand::decode(&serde_json::to_vec(&command(json!([output]))).unwrap()).is_err()
    );
}
