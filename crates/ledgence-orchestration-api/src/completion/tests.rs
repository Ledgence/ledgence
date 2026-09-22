use super::*;
use serde_json::json;
fn command() -> CompletionSubscribeCommand {
    CompletionSubscribeCommand {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        },
        target: CompletionTarget::Task { id: "task".into() },
        destination: "billing".into(),
        idempotency_key: "notify".into(),
    }
}
fn event() -> Value {
    json!({"specversion":"1.0","id":"evt_task_completed_task","source":"urn:ledgence:orchestrator",
    "type":"com.ledgence.task.completed.v1","subject":"tasks/task","time":"2026-09-22T00:00:00Z",
    "ldgtenantid":"acme","ldgnamespace":"billing","ldgstate":"succeeded","ldgtaskid":"task","ldgrunid":"run",
    "ldgresultref":"/v1/tasks/result?tenant_id=acme&namespace=billing&task_id=task"})
}
fn waiting() -> CompletionSubscription {
    CompletionSubscription {
        subscription_id: "sub".into(),
        command: command(),
        state: CompletionState::Waiting,
        generation: 1,
        attempts: 0,
        total_attempts: 0,
        created_at: 1,
        activated_at: None,
        next_attempt_at: None,
        lease_expires_at: None,
        delivered_at: None,
        exhausted_at: None,
        last_failure: None,
        event: None,
    }
}
#[test]
fn envelope_rejects_payload_and_mismatched_reference_or_controller_identity() {
    CompletionEvent::new(event()).unwrap();
    for (key, value) in [
        ("data", json!({})),
        ("id", json!("random")),
        ("subject", json!("tasks/other")),
        ("ldgresultref", json!("https://example.com/other")),
        ("ldgstate", json!("running")),
        ("ldgparentworkflowid", json!("parent")),
        ("ldgactivationid", json!("different")),
        ("ldgtenantid", json!(true)),
    ] {
        let mut value0 = event();
        value0[key] = value;
        assert!(CompletionEvent::new(value0).is_err(), "{key}");
    }
    let mut value = event();
    value["type"] = json!("com.ledgence.workflow.completed.v1");
    assert!(CompletionEvent::new(value).is_err());
}
#[test]
fn strict_wire_decoding_rejects_duplicates_unknown_fields_and_missing_nullable_fields() {
    let bytes = serde_json::to_vec(&command()).unwrap();
    CompletionSubscribeCommand::decode(&bytes).unwrap();
    let mut value = serde_json::to_value(command()).unwrap();
    value["extra"] = json!(true);
    assert!(CompletionSubscribeCommand::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut duplicate = bytes[..bytes.len() - 1].to_vec();
    duplicate.extend_from_slice(br#", "destination":"other"}"#);
    assert!(CompletionSubscribeCommand::decode(&duplicate).is_err());
    let mut value = serde_json::to_value(waiting()).unwrap();
    value.as_object_mut().unwrap().remove("event");
    assert!(serde_json::from_value::<CompletionSubscription>(value).is_err());
}
#[test]
fn percent_encoding_is_canonical_utf8_and_does_not_reinterpret_plain_business_keys() {
    let key = "reference\u{10ffff}";
    assert_eq!(
        decode_completion_correlation(&encode_completion_correlation(key)).unwrap(),
        key
    );
    for key in ["%", "%GG", "%FF", "%00", "%2f", "%41", "é"] {
        assert!(decode_completion_correlation(key).is_err(), "{key}");
    }
    let mut value = event();
    value["ldgcorrelationkey"] = json!("literal%20value");
    CompletionEvent::new(value.clone()).unwrap();
    value["ldgcorrelationkeyencoding"] = json!("base64");
    assert!(CompletionEvent::new(value).is_err());
    let target = CompletionTarget::Task {
        id: "a +/é".into()
    };
    assert!(completion_result_ref(&command().scope, &target).ends_with("task_id=a%20%2B%2F%C3%A9"));
}
#[test]
fn observations_require_consistent_states_and_bind_leases_to_the_committed_event() {
    let original = waiting();
    original.validate().unwrap();
    let mut bad = original.clone();
    bad.state = CompletionState::Pending;
    assert!(bad.validate().is_err());
    let mut active = original;
    active.state = CompletionState::Delivering;
    active.attempts = 1;
    active.total_attempts = 1;
    active.activated_at = Some(2);
    active.lease_expires_at = Some(30);
    active.event = Some(CompletionEvent::new(event()).unwrap());
    active.validate().unwrap();
    let mut lease = CompletionLease {
        subscription: active.clone(),
        lease_token: "token".into(),
        event_bytes: serde_json::to_vec(&event()).unwrap(),
    };
    lease.validate().unwrap();
    let mut other = event();
    other["ldgrunid"] = json!("other");
    lease.event_bytes = serde_json::to_vec(&other).unwrap();
    assert!(lease.validate().is_err());
    active.state = CompletionState::Exhausted;
    active.lease_expires_at = None;
    active.exhausted_at = Some(4);
    assert!(active.validate().is_err());
    active.attempts = 8;
    active.total_attempts = 8;
    active.validate().unwrap();
    active.command.target = CompletionTarget::Task { id: "other".into() };
    assert!(active.validate().is_err());
}
#[test]
fn retry_commands_and_outcomes_have_finite_bounds() {
    let mut command = CompletionRetryCommand {
        scope: command().scope,
        subscription_id: "sub".into(),
        expected_generation: 1,
    };
    command.validate().unwrap();
    for generation in [0, 1000, u32::MAX] {
        command.expected_generation = generation;
        assert!(command.validate().is_err());
    }
    let mut result = CompletionDeliveryResult {
        subscription_id: "sub".into(),
        generation: 1,
        lease_token: "token".into(),
        outcome: CompletionDeliveryOutcome::Retry {
            reason: "receiver unavailable".into(),
            retry_after_ms: Some(300_000),
        },
    };
    result.validate().unwrap();
    result.outcome = CompletionDeliveryOutcome::Retry {
        reason: "receiver unavailable".into(),
        retry_after_ms: Some(300_001),
    };
    assert!(result.validate().is_err());
}

#[test]
fn completion_lineage_rejects_self_ancestry_and_unowned_activation() {
    for extra in [
        json!({"ldgactivationid":"task"}),
        json!({"ldgworkflowid":"flow", "ldgparentworkflowid":"flow", "ldgrootworkflowid":"ancestor"}),
        json!({"ldgworkflowid":"flow", "ldgparentworkflowid":"parent", "ldgrootworkflowid":"flow"}),
    ] {
        let mut invalid = event();
        invalid
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(CompletionEvent::new(invalid).is_err());
    }
    let mut root_activation = event();
    root_activation["ldgworkflowid"] = json!("root");
    root_activation["ldgactivationid"] = json!("task");
    CompletionEvent::new(root_activation.clone()).unwrap();
    root_activation["ldgworkflowid"] = json!("child");
    root_activation["ldgparentworkflowid"] = json!("root");
    root_activation["ldgrootworkflowid"] = json!("root");
    CompletionEvent::new(root_activation).unwrap();
}
