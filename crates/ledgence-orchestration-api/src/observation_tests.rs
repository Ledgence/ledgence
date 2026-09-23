use super::*;
use serde_json::json;

fn result() -> Value {
    json!({"task":{"scope":{"tenant_id":"acme","namespace":"billing"},"task_id":"task","run_id":"run","queue":"queue","correlation_key":null,"state":"succeeded","attempt_count":1,"current_attempt_id":null,"latest_attempt_id":"attempt","submitted_at":1,"available_at":1,"terminal_at":2,"cancel_requested_at":null},"outcome":{"kind":"succeeded","attempt_id":"attempt","quiescence":"confirmed","execution_may_have_started":true,"output":null}})
}

#[test]
fn missing_required_nullable_fields_and_success_output_are_rejected() {
    let value = result();
    let decoded: TaskResult = serde_json::from_value(value.clone()).unwrap();
    decoded.validate().unwrap();
    let roundtrip: TaskResult =
        decode_unique_json(&serde_json::to_vec(&decoded).unwrap(), 4096).unwrap();
    roundtrip.validate().unwrap();
    assert_eq!(roundtrip, decoded);
    for field in [
        "correlation_key",
        "current_attempt_id",
        "latest_attempt_id",
        "terminal_at",
        "cancel_requested_at",
    ] {
        let mut changed = value.clone();
        changed["task"].as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<TaskResult>(changed).is_err(),
            "missing {field}"
        );
    }
    let mut changed = value.clone();
    changed.as_object_mut().unwrap().remove("outcome");
    assert!(serde_json::from_value::<TaskResult>(changed).is_err());
    let mut changed = value;
    changed["outcome"].as_object_mut().unwrap().remove("output");
    assert!(serde_json::from_value::<TaskResult>(changed).is_err());
}

#[test]
fn malformed_variants_and_contradictory_result_identity_are_rejected() {
    for replacement in [
        json!({"kind":"unknown"}),
        json!({"kind":"cancelled","attempt_id":"old"}),
    ] {
        let mut value = result();
        value["outcome"] = replacement;
        assert!(serde_json::from_value::<TaskResult>(value).is_err());
    }
    for (path, value) in [
        ("state", json!("active")),
        ("latest_attempt_id", json!("other")),
        ("attempt_count", json!(0)),
        ("cancel_requested_at", json!(1)),
        ("terminal_at", Value::Null),
        ("submitted_at", json!(u64::MAX)),
    ] {
        let mut changed = result();
        changed["task"][path] = value;
        assert!(
            serde_json::from_value::<TaskResult>(changed)
                .unwrap()
                .validate()
                .is_err(),
            "{path}"
        );
    }
    let mut changed = result();
    changed["outcome"]["execution_may_have_started"] = json!(false);
    assert!(
        serde_json::from_value::<TaskResult>(changed)
            .unwrap()
            .validate()
            .is_err()
    );
}

#[test]
fn compact_outcome_preserves_json_profile_and_enforces_settlement_bound() {
    let mut value = result();
    let payload: Value = decode_unique_json(
        br#"[null,false,0,-0,18446744073709551615,-9223372036854775808,1.0,"x\u0000y"]"#,
        1000,
    )
    .unwrap();
    value["outcome"]["output"] = payload.clone();
    let bytes = serde_json::to_vec(&value).unwrap();
    let decoded: TaskResult = decode_unique_json(&bytes, 4096).unwrap();
    decoded.validate().unwrap();
    let Some(TaskOutcome::Succeeded { output, .. }) = decoded.outcome else {
        panic!()
    };
    assert_eq!(
        canonical_json_bytes(&output).unwrap(),
        canonical_json_bytes(&payload).unwrap()
    );
    value["outcome"]["output"] = json!("x".repeat(SETTLEMENT_MAX_BYTES));
    assert!(
        serde_json::from_value::<TaskResult>(value)
            .unwrap()
            .validate()
            .is_err()
    );
}

#[test]
fn correlation_metadata_preserves_the_existing_submission_profile() {
    for correlation in ["", "invoice:1", "\u{ffff}"] {
        let mut value = result();
        value["task"]["correlation_key"] = json!(correlation);
        serde_json::from_value::<TaskResult>(value)
            .unwrap()
            .validate()
            .unwrap();
    }
    for correlation in ["x\n".to_owned(), "x".repeat(513)] {
        let mut value = result();
        value["task"]["correlation_key"] = json!(correlation);
        assert!(
            serde_json::from_value::<TaskResult>(value)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}

#[test]
fn nested_worker_errors_and_empty_failure_variants_reject_unknown_fields() {
    for failure in [
        json!({"kind":"attempt_lost","error":{"kind":"runtime","message":"fabricated"}}),
        json!({"kind":"execution","error":{"kind":"runtime","message":"failure","unknown":true},"phase":"execution","cleanup_error":null}),
        json!({"kind":"execution","error":{"kind":"runtime","message":"failure"},"phase":"execution","cleanup_error":{"kind":"io","message":"cleanup","unknown":true}}),
        json!({"kind":"execution","error":{"kind":"runtime","message":"failure"},"phase":"execution"}),
    ] {
        assert!(serde_json::from_value::<TaskFailure>(failure).is_err());
    }
}
