use ledgence_orchestration_api::{LocalStepRecord, console::*};
use serde_json::{Value, json};

fn detail(callable: &str, input: Value) -> ConsoleLocalStepDetail {
    let step = ConsoleLocalStep {
        workflow_id: "workflow".into(),
        activation_id: "activation".into(),
        step_key: "key".into(),
        callable: callable.into(),
        attempt_id: "accepted-attempt".into(),
        accepted_at: 100,
    };
    let record = LocalStepRecord {
        key: step.step_key.clone(),
        callable: step.callable.clone(),
        input,
        output: json!({"count":u64::MAX,"zero":-0.0}),
    };
    ConsoleLocalStepDetail::from_record(step, record).unwrap()
}
#[test]
fn classifies_only_complete_version_one_operation_bindings() {
    for (kind, expected) in [
        ("tool", ConsoleOperationKind::Tool),
        ("model", ConsoleOperationKind::Model),
    ] {
        let args =
            json!({"key":"effective", "unicode":"☂", "value":1.0, "integer":9007199254740993_u64});
        let input = json!({"v":1,"kind":kind,"version":"β3","arguments":args});
        let entity = detail(&format!("{kind}:example:application"), input.clone());
        assert_eq!(entity.kind, expected);
        assert_eq!(entity.version.as_deref(), Some("β3"));
        assert_eq!(entity.arguments, args);
        assert_eq!(entity.result["count"].as_u64(), Some(u64::MAX));
        assert!(entity.result["zero"].as_f64().unwrap().is_sign_negative());
        let incompatible = if kind == "model" { "tool" } else { "model" };
        for (callable, input) in [
            (format!("{kind}:lookup"), input.clone()),
            (format!("{incompatible}:example:application"), input.clone()),
            ("demo:application".into(), input.clone()),
            (
                format!("{kind}:example:application"),
                json!({"v":2,"kind":kind,"version":"v","arguments":{}}),
            ),
            (
                format!("{kind}:example:application"),
                json!({"v":1,"kind":kind,"version":"v","arguments":{},"extra":1}),
            ),
            (
                format!("{kind}:example:application"),
                json!({"v":1,"kind":kind,"version":"v","arguments":null}),
            ),
        ] {
            let local = detail(&callable, input.clone());
            assert_eq!(local.kind, ConsoleOperationKind::Local);
            assert!(local.version.is_none());
            assert_eq!(local.arguments, input);
        }
    }
}
#[test]
fn local_detail_preserves_null_and_enforces_identity() {
    let local = detail("example:run", Value::Null);
    assert_eq!(local.arguments, Value::Null);
    let mut request = ConsoleLocalStepLookup {
        workflow_id: "workflow".into(),
        activation_id: "activation".into(),
        step_key: "key".into(),
    };
    request.validate_reply(&local).unwrap();
    request.activation_id = "unrelated".into();
    assert!(request.validate_reply(&local).is_err());
    let mut response = serde_json::to_value(local).unwrap();
    response.as_object_mut().unwrap().remove("arguments");
    assert!(serde_json::from_value::<ConsoleLocalStepDetail>(response).is_err());
}
