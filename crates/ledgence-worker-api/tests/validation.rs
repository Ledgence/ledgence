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
