use ledgence_orchestration_api::*;
use serde_json::{Value, json};

fn accepted(value: &Value) -> bool {
    serde_json::from_value::<RunBudgetPolicy>(value.clone()).is_ok_and(|p| p.validate().is_ok())
}
#[test]
fn budget_is_part_of_root_submission_identity_without_altering_legacy_bytes() {
    let legacy = json!({"tenant_id":"acme","namespace":"billing","queue":"support","program":{"id":"agent","version":"1"},"data":{}});
    let input = SubmitTask::decode(&serde_json::to_vec(&legacy).unwrap()).unwrap();
    let canonical = input.canonical_bytes().unwrap();
    let mut raw: Value = serde_json::from_slice(&canonical).unwrap();
    assert!(raw.get("budget").is_none());
    raw["budget"] = json!({"max_calls":1});
    let governed = SubmitTask::decode(&serde_json::to_vec(&raw).unwrap()).unwrap();
    assert!(!input.semantically_matches(&governed).unwrap());
    assert_eq!(
        serde_json::to_value(&governed).unwrap()["budget"],
        json!({"max_calls":1})
    );
    raw["budget"] = Value::Null;
    assert!(SubmitTask::decode(&serde_json::to_vec(&raw).unwrap()).is_err());
    for bad in [
        json!({}),
        json!({"max_calls":null}),
        json!({"max_calls":1000001}),
        json!({"max_delegations":-1}),
        json!({"deadline_at_ms":253402300800000_u64}),
        json!({"root":"caller"}),
    ] {
        assert!(!accepted(&bad), "{bad}");
    }
    assert!(accepted(&json!({"max_calls":0,"max_delegations":1})));
    assert!(accepted(&json!({"deadline_at_ms":0})));
}
#[test]
fn journal_declines_foreign_receipts_invalid_usage_and_cursor_rebinding() {
    let data: Value = serde_json::from_str(include_str!("fixtures/console-v13.json")).unwrap();
    let mut page: RunBudgetPage =
        serde_json::from_value(data["run_budget_entries"].clone()).unwrap();
    let mut query = RunBudgetEntriesQuery {
        scope: page.scope.clone(),
        subject: page.subject.clone(),
        before: None,
        limit: 1,
    };
    page.validate_for(&query).unwrap();
    query.before = page.next;
    assert!(page.validate_for(&query).is_err());
    let row = &mut page.items[0];
    row.outcome = RunBudgetOutcome::Denied;
    row.reason = Some(RunBudgetDenial::CallsExhausted);
    assert!(
        row.validate().is_err(),
        "denied calls cannot report spent tokens"
    );
    row.usage = RunBudgetUsage::Unknown {};
    row.validate().unwrap();
    row.reason = Some(RunBudgetDenial::DelegationsExhausted);
    assert!(row.validate().is_err());
    for usage in [
        json!({"kind":"estimated"}),
        json!({"kind":"reported","output_tokens":9007199254740992_u64}),
        json!({"kind":"unknown","input_tokens":0}),
    ] {
        assert!(
            !serde_json::from_value::<RunBudgetUsage>(usage).is_ok_and(|u| u.validate().is_ok())
        );
    }
    assert!(
        serde_json::from_value::<RunBudgetUsage>(json!({"kind":"reported","input_tokens":0}))
            .unwrap()
            .validate()
            .is_ok()
    );
}

#[test]
fn budget_counters_round_trip_u64_without_loosening_policy_bounds_or_wire_types() {
    let data: Value = serde_json::from_str(include_str!("fixtures/console-v13.json")).unwrap();
    let raw = &data["run_budget_large_usage"];
    assert_eq!(raw["calls"], "9007199254740993");
    assert_eq!(raw["delegations"], "18446744073709551615");
    let mut snapshot: RunBudgetSnapshot = serde_json::from_value(raw.clone()).unwrap();
    snapshot.validate().unwrap();
    assert_eq!(snapshot.delegations, u64::MAX);
    assert_eq!(&serde_json::to_value(&snapshot).unwrap(), raw);
    snapshot.policy.max_calls = Some(1_000_000);
    assert!(snapshot.validate().is_err());
    for bad in [json!(42), json!("07"), json!("18446744073709551616")] {
        let mut malformed = raw.clone();
        malformed["calls"] = bad;
        assert!(serde_json::from_value::<RunBudgetSnapshot>(malformed).is_err());
    }
}

#[test]
fn generic_effect_link_is_optional_and_can_only_reference_the_call_owners_approval() {
    let data: Value = serde_json::from_str(include_str!("fixtures/console-v13.json")).unwrap();
    let raw = &data["run_budget_entries"]["items"][0];
    let entry: RunBudgetEntry = serde_json::from_value(raw.clone()).unwrap();
    entry.validate().unwrap();
    assert!(entry.approval.is_some());
    let mut old = raw.clone();
    old.as_object_mut().unwrap().remove("approval");
    let legacy: RunBudgetEntry = serde_json::from_value(old.clone()).unwrap();
    legacy.validate().unwrap();
    assert_eq!(serde_json::to_value(legacy).unwrap(), old);
    let mut absent = raw.clone();
    absent["approval"] = Value::Null;
    assert!(serde_json::from_value::<RunBudgetEntry>(absent).is_err());
    for field in ["scope", "workflow_id"] {
        let mut bad = raw.clone();
        bad["approval"][field] = if field == "scope" {
            json!({"tenant_id":"other","namespace":"demo"})
        } else {
            json!("other")
        };
        let bad: RunBudgetEntry = serde_json::from_value(bad).unwrap();
        assert!(bad.validate().is_err());
    }
    let mut bad = entry.clone();
    bad.subject = RunBudgetSubject::Task {
        task_id: "other".into(),
    };
    assert!(bad.validate().is_err());
    let mut delegation = entry;
    delegation.action = RunBudgetAction::Delegation;
    delegation.usage = RunBudgetUsage::Unknown {};
    assert!(delegation.validate().is_err());
}

#[test]
fn inherited_cancellation_has_distinct_receipt_and_wake_reasons() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/console-v13.json")).unwrap();
    let mut row: RunBudgetEntry =
        serde_json::from_value(fixture["run_budget_entries"]["items"][0].clone()).unwrap();
    row.approval = None;
    row.usage = RunBudgetUsage::Unknown {};
    row.outcome = RunBudgetOutcome::Denied;
    row.reason = Some(RunBudgetDenial::Cancelled);
    for action in [RunBudgetAction::Call, RunBudgetAction::Delegation] {
        row.action = action;
        row.validate().unwrap();
        assert_eq!(serde_json::to_value(&row).unwrap()["reason"], "cancelled");
    }
    assert_eq!(RunBudgetDenial::Cancelled.code(), "budget_cancelled");
    assert_eq!(
        serde_json::to_value(AdmissionClosedReason::from(RunBudgetDenial::Cancelled)).unwrap(),
        "budget_cancelled"
    );
    assert_eq!(
        serde_json::to_value(AdmissionClosedReason::Cancelled).unwrap(),
        "cancelled"
    );
    assert_eq!(
        serde_json::to_value(ContractError::RunBudgetDenied(RunBudgetDenial::Cancelled)).unwrap(),
        json!({"code":"run_budget_denied","message":"cancelled"})
    );
}
