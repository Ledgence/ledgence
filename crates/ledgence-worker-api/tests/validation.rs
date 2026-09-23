use ledgence_worker_api::{CloudEvent, ErrorKind, RunControl};
use serde_json::{Value, json};
use std::time::Duration;

fn event() -> Value {
    json!({
        "specversion": "1.0", "id": "invocation-1", "source": "urn:ledgence:worker:test",
        "type": "io.ledgence.invocation", "datacontenttype": "application/json",
        "ldgtenantid": "tenant-1", "ldgnamespace": "default", "ldgrunid": "run-1",
        "ldgtaskid": "task-1", "ldgattemptid": "attempt-1", "ldgattemptno": 1,
        "data": {"businessid": "invoice-17"}
    })
}
fn with_field(name: &str, value: Value) -> Value {
    let mut event = event();
    event[name] = value;
    event
}
fn trace() -> &'static str {
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
}

#[test]
fn user_owned_json_is_unchanged_and_not_subject_to_context_rules() {
    for data in [
        Value::Null,
        json!(true),
        json!(2.5),
        json!(u64::MAX),
        json!("line\n\u{0}\u{fffe}"),
        json!([1, null, {"Trace-ID": ["a", "b"]}]),
        json!({"traceparent": {"anything": true}, "UPPER_CASE": [1.5], "": null}),
    ] {
        let original = with_field("data", data);
        let validated = CloudEvent::new(original.clone()).unwrap();
        assert_eq!(validated.value(), &original);
        let encoded = serde_json::to_string(&validated).unwrap();
        let decoded: CloudEvent = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.into_value(), original);
    }
}

#[test]
fn context_names_obey_required_rules_without_enforcing_length_recommendations() {
    for name in ["", "Traceparent", "business_id", "business-id", "näme"] {
        assert!(
            CloudEvent::new(with_field(name, json!("value"))).is_err(),
            "{name}"
        );
    }
    for name in [
        "businessid",
        "7identifier",
        "thisextensionislongerthantwentycharacters",
    ] {
        CloudEvent::new(with_field(name, json!("value"))).unwrap();
    }
}

#[test]
fn context_values_are_scalars_with_signed_32_bit_integers() {
    for value in [
        json!({}),
        json!([]),
        Value::Null,
        json!(1.5),
        json!(1.0),
        json!(i64::from(i32::MAX) + 1),
        json!(i64::from(i32::MIN) - 1),
    ] {
        assert!(
            CloudEvent::new(with_field("custom", value.clone())).is_err(),
            "{value}"
        );
    }
    for value in [
        json!(true),
        json!(false),
        json!(""),
        json!("客户-17"),
        json!(i32::MIN),
        json!(i32::MAX),
    ] {
        CloudEvent::new(with_field("custom", value)).unwrap();
    }
    for value in [
        json!(0),
        json!(-1),
        json!(i64::from(i32::MAX) + 1),
        json!("1"),
        json!(true),
    ] {
        assert!(CloudEvent::new(with_field("ldgattemptno", value)).is_err());
    }
    CloudEvent::new(with_field("ldgattemptno", json!(i32::MAX))).unwrap();
}

#[test]
fn context_strings_reject_control_and_noncharacter_codepoints() {
    for text in [
        "a\nb",
        "a\tb",
        "a\u{7f}b",
        "a\u{85}b",
        "a\u{fdd0}b",
        "a\u{10ffff}b",
    ] {
        assert!(
            CloudEvent::new(with_field("custom", json!(text))).is_err(),
            "{text:?}"
        );
    }
    CloudEvent::new(with_field("subject", json!("invoice/客户-17"))).unwrap();
}

#[test]
fn source_accepts_uri_references_and_rejects_invalid_encoding() {
    for source in [
        "urn:ledgence:worker:test",
        "https://example.com/events",
        "/relative/events",
        "1-555-123-4567",
        "../events?x=1#part",
    ] {
        CloudEvent::new(with_field("source", json!(source))).unwrap();
    }
    for source in [
        "",
        "two words",
        "https://example.com/%",
        "https://example.com/%xx",
        "http://[invalid]/",
        "https://example.com/客户",
    ] {
        assert!(
            CloudEvent::new(with_field("source", json!(source))).is_err(),
            "{source}"
        );
    }
}

#[test]
fn optional_standard_context_attributes_retain_their_declared_types() {
    for field in ["subject", "time", "dataschema", "tracestate"] {
        for value in [json!(17), json!(true), Value::Null] {
            assert!(
                CloudEvent::new(with_field(field, value)).is_err(),
                "{field}"
            );
        }
    }
    assert!(CloudEvent::new(with_field("subject", json!(""))).is_err());
    for schema in ["https://example.com/schema/1", "urn:example:schema:1"] {
        CloudEvent::new(with_field("dataschema", json!(schema))).unwrap();
    }
    for schema in [
        "",
        "/schema/1",
        "schema.json",
        "https://example.com/schema#part",
        "https://example.com/%xy",
    ] {
        assert!(
            CloudEvent::new(with_field("dataschema", json!(schema))).is_err(),
            "{schema}"
        );
    }
}

#[test]
fn timestamp_checks_calendar_and_offset_without_reformatting() {
    for timestamp in [
        "2024-02-29T23:59:59Z",
        "2026-09-12T10:11:12.123456789-04:00",
        "2026-09-12t10:11:12z",
    ] {
        let original = with_field("time", json!(timestamp));
        assert_eq!(
            CloudEvent::new(original.clone()).unwrap().value(),
            &original
        );
    }
    for timestamp in [
        "2025-02-29T00:00:00Z",
        "2026-06-31T00:00:00Z",
        "2026-09-12",
        "2026-09-12T10:11:12",
        "2026-09-12T10:11:12+24:00",
        "2026-09-12T25:11:12Z",
        "2026-09-12T10:11:12Z trailing",
    ] {
        assert!(
            CloudEvent::new(with_field("time", json!(timestamp))).is_err(),
            "{timestamp}"
        );
    }
}

#[test]
fn traceparent_v00_profile_validates_identity_and_preserves_flags() {
    CloudEvent::new(with_field("traceparent", json!(trace()))).unwrap();
    CloudEvent::new(with_field(
        "traceparent",
        json!(trace().replace("-01", "-ff")),
    ))
    .unwrap();
    for invalid in [
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        "00-4BF92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
    ] {
        assert!(CloudEvent::new(with_field("traceparent", json!(invalid))).is_err());
    }
}

#[test]
fn tracestate_checks_grammar_duplicates_and_profile_limits() {
    for state in [
        "",
        " , ",
        "rojo=abc,congo=def",
        "1tenant@system=opaque",
        "vendor= value with spaces ",
        "a=b,,c=d",
    ] {
        let mut original = with_field("traceparent", json!(trace()));
        original["tracestate"] = json!(state);
        assert_eq!(
            CloudEvent::new(original.clone()).unwrap().value(),
            &original
        );
    }
    for invalid in [
        "a=",
        "a=b,a=c",
        "A=b",
        "1tenant=b",
        "a=b=c",
        "a =b",
        "tenant@1system=value",
        "vendor=é",
    ] {
        let mut input = with_field("traceparent", json!(trace()));
        input["tracestate"] = json!(invalid);
        assert!(CloudEvent::new(input).is_err(), "{invalid}");
    }
    for oversized in [
        format!("a={}", "x".repeat(257)),
        ",".repeat(32),
        format!("a={},b={}", "x".repeat(255), "y".repeat(255)),
    ] {
        let mut input = with_field("traceparent", json!(trace()));
        input["tracestate"] = json!(oversized);
        assert!(CloudEvent::new(input).is_err());
    }
    assert!(CloudEvent::new(with_field("tracestate", json!("vendor=value"))).is_err());
}

#[test]
fn decoding_rejects_duplicate_context_attributes() {
    let encoded = serde_json::to_string(&event()).unwrap();
    let duplicate = format!("{},\"id\":\"other\"}}", &encoded[..encoded.len() - 1]);
    assert!(serde_json::from_str::<CloudEvent>(&duplicate).is_err());
}

#[test]
fn required_json_profile_fields_are_enforced() {
    for field in [
        "specversion",
        "id",
        "source",
        "type",
        "datacontenttype",
        "ldgtenantid",
        "ldgnamespace",
        "ldgrunid",
        "ldgtaskid",
        "ldgattemptid",
        "ldgattemptno",
        "data",
    ] {
        let mut input = event();
        input.as_object_mut().unwrap().remove(field);
        assert!(CloudEvent::new(input).is_err(), "{field}");
    }
    assert!(CloudEvent::new(with_field("data_base64", json!("YWJj"))).is_err());
}

#[test]
fn timeout_overflow_fails_closed_and_clones_share_cancellation() {
    assert_eq!(
        RunControl::try_new(Duration::MAX).unwrap_err().kind,
        ErrorKind::InvalidInput
    );
    assert_eq!(
        RunControl::new(Duration::MAX).check().unwrap_err().kind,
        ErrorKind::TimedOut
    );
    assert_eq!(
        RunControl::new(Duration::ZERO).check().unwrap_err().kind,
        ErrorKind::TimedOut
    );
    let control = RunControl::try_new(Duration::from_secs(5)).unwrap();
    assert!(control.check().is_ok());
    control.clone().cancel();
    assert_eq!(control.check().unwrap_err().kind, ErrorKind::Cancelled);
}

#[test]
fn invocation_identity_preserves_scope_and_optional_trace_without_user_data() {
    use ledgence_worker_api::InvocationIdentity;
    let mut value = event();
    value["traceparent"] = trace().into();
    value["tracestate"] = "vendor=state".into();
    value["ldgworkflowid"] = "workflow-1".into();
    value["ldgactivationid"] = "task-1".into();
    value["ldgparentworkflowid"] = "parent-1".into();
    value["ldgrootworkflowid"] = "root-1".into();
    value["data"] = json!({"source": "do not read this", "ldgrunid": "wrong"});
    let identity = InvocationIdentity::from(&CloudEvent::new(value).unwrap());
    let serialized = serde_json::to_value(&identity).unwrap();
    assert_eq!(serialized["source"], "urn:ledgence:worker:test");
    assert_eq!(serialized["tenant_id"], "tenant-1");
    assert_eq!(serialized["namespace"], "default");
    assert_eq!(serialized["run_id"], "run-1");
    assert_eq!(serialized["workflow_id"], "workflow-1");
    assert_eq!(serialized["activation_id"], "task-1");
    assert_eq!(serialized["parent_workflow_id"], "parent-1");
    assert_eq!(serialized["root_workflow_id"], "root-1");
    assert_eq!(serialized["event_id"], "invocation-1");
    assert_eq!(serialized["task_id"], "task-1");
    assert_eq!(serialized["attempt_id"], "attempt-1");
    assert_eq!(serialized["attempt_no"], 1);
    assert_eq!(serialized["traceparent"], trace());
    assert_eq!(serialized["tracestate"], "vendor=state");
    assert!(serialized.get("data").is_none());
    let untraced =
        serde_json::to_value(InvocationIdentity::from(&CloudEvent::new(event()).unwrap())).unwrap();
    assert!(untraced.get("traceparent").is_none());
    assert!(untraced.get("tracestate").is_none());
    assert!(untraced.get("workflow_id").is_none());
    assert!(untraced.get("activation_id").is_none());
    assert!(untraced.get("parent_workflow_id").is_none());
    assert!(untraced.get("root_workflow_id").is_none());
}

#[test]
fn wire_result_depth_accepts_the_boundary_and_rejects_the_next_container() {
    use ledgence_worker_api::{MAX_WIRE_VALUE_DEPTH, validate_wire_value};
    let mut nested = Value::Null;
    for depth in 1..=MAX_WIRE_VALUE_DEPTH {
        nested = if depth % 2 == 0 {
            json!({"child":nested})
        } else {
            json!([nested])
        };
        validate_wire_value(&nested).unwrap();
    }
    assert_eq!(
        validate_wire_value(&json!([nested])).unwrap_err().kind,
        ErrorKind::Protocol
    );
    for value in [
        json!(i64::MIN),
        json!(u64::MAX),
        json!(f64::MAX),
        json!("\u{0}😀"),
    ] {
        validate_wire_value(&value).unwrap();
    }
}

#[test]
fn runtime_extension_is_additive_and_does_not_change_user_data() {
    use ledgence_worker_api::{RuntimeExtension, RuntimeInvocation, RuntimeRequest};
    let original = event();
    let mut invocation: RuntimeInvocation =
        serde_json::from_value(json!({"event": original, "processing_context": null})).unwrap();
    assert!(invocation.extension.is_none());
    let extension = RuntimeExtension {
        schema: "test.activation.v1".into(),
        payload: json!({"checkpoint": 1}),
    };
    extension.validate().unwrap();
    invocation.extension = Some(extension.clone());
    assert_eq!(invocation.event.value(), &original);
    let restored: RuntimeInvocation =
        serde_json::from_value(serde_json::to_value(invocation).unwrap()).unwrap();
    assert_eq!(restored.extension, Some(extension));
    assert_eq!(restored.event.value(), &original);
    assert!(
        RuntimeRequest {
            id: 0,
            operation: "test.commit".into(),
            payload: Value::Null
        }
        .validate()
        .is_err()
    );
    assert!(
        RuntimeExtension {
            schema: "invalid\nschema".into(),
            payload: Value::Null
        }
        .validate()
        .is_err()
    );
    assert!(
        serde_json::from_value::<RuntimeRequest>(
            json!({"id": 1, "operation": "test.commit", "payload": {}, "authority": "forged"})
        )
        .is_err()
    );
}

#[test]
fn interactive_envelopes_preserve_full_application_depth_and_bound_encoded_bytes() {
    use ledgence_worker_api::{
        MAX_RUNTIME_VALUE_DEPTH, RUNTIME_EXTENSION_MAX_BYTES, RuntimeExtension,
        validate_runtime_payload, validate_wire_value,
    };
    let mut application = json!(0);
    for _ in 0..64 {
        application = json!([application]);
    }
    validate_wire_value(&application).unwrap();
    let envelope = json!({"local_steps": [{"output": application}]});
    assert!(validate_wire_value(&envelope).is_err());
    RuntimeExtension {
        schema: "test.activation.v1".into(),
        payload: envelope,
    }
    .validate()
    .unwrap();
    let mut nested = json!(0);
    for _ in 0..MAX_RUNTIME_VALUE_DEPTH {
        nested = json!([nested]);
    }
    validate_runtime_payload(&nested, RUNTIME_EXTENSION_MAX_BYTES).unwrap();
    assert!(validate_runtime_payload(&json!([nested]), RUNTIME_EXTENSION_MAX_BYTES).is_err());
    validate_runtime_payload(&json!("1234"), 6).unwrap();
    assert!(validate_runtime_payload(&json!("1234"), 5).is_err());
    assert!(validate_runtime_payload(&json!("\u{0000}"), 7).is_err());
    assert!(
        RuntimeExtension {
            schema: "test.activation.v1".into(),
            payload: Value::String("x".repeat(RUNTIME_EXTENSION_MAX_BYTES))
        }
        .validate()
        .is_err()
    );
}

#[test]
fn workflow_context_ids_are_optional_but_have_consistent_identity_when_present() {
    let mut value = event();
    value["ldgworkflowid"] = json!("workflow-1");
    CloudEvent::new(value.clone()).unwrap();
    value["ldgactivationid"] = value["ldgtaskid"].clone();
    CloudEvent::new(value.clone()).unwrap();
    for key in ["ldgworkflowid", "ldgactivationid"] {
        for invalid in [json!(true), json!(1), json!(""), json!("x".repeat(129))] {
            let mut bad = value.clone();
            bad[key] = invalid;
            assert!(CloudEvent::new(bad).is_err(), "{key}");
        }
    }
    let mut bad = value.clone();
    bad["ldgactivationid"] = json!("other-task");
    assert!(CloudEvent::new(bad).is_err());
    value.as_object_mut().unwrap().remove("ldgworkflowid");
    assert!(CloudEvent::new(value).is_err());
}

#[test]
fn nested_invocations_reject_partial_or_self_ancestry() {
    for (workflow, parent, root) in [
        (None, Some("parent"), Some("root")),
        (Some("child"), None, Some("root")),
        (Some("child"), Some("parent"), None),
        (Some("child"), Some("child"), Some("root")),
        (Some("child"), Some("parent"), Some("child")),
    ] {
        let mut value = event();
        for (key, id) in [
            ("ldgworkflowid", workflow),
            ("ldgparentworkflowid", parent),
            ("ldgrootworkflowid", root),
        ] {
            if let Some(id) = id {
                value[key] = json!(id);
            }
        }
        assert!(CloudEvent::new(value).is_err());
    }
}
