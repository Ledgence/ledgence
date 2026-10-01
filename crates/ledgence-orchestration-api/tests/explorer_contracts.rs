use ledgence_orchestration_api::{console::*, *};
use ledgence_worker_api::ProgramRef;
use serde_json::json;

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
fn entrypoint() -> ConsoleExplorerData {
    ConsoleExplorerData::Entrypoint {
        state: Some(TaskState::Succeeded),
        availability: ConsoleEvidenceAvailability::Available,
        submitted_at: 10,
        terminal_at: Some(20),
        applied_at: None,
        decision_kind: None,
        error: None,
        resumed_activation_id: None,
    }
}
fn node(revision: u64, data: ConsoleExplorerData) -> ConsoleExplorerNode {
    let activation_id = format!("activation:{revision}");
    let mut node = ConsoleExplorerNode {
        id: serde_json::to_string(&(data.kind(), "workflow:1", &activation_id, data.key()))
            .unwrap(),
        activation_id,
        revision: ConsoleU64(revision),
        entrypoint: "validate".into(),
        relations: Vec::new(),
        data,
    };
    node.relations = node.derive_relations("workflow:1").unwrap();
    node
}

#[test]
fn singleton_entrypoint_and_child_wait_have_valid_ordered_cursor_boundaries() {
    let request = ConsolePagination {
        limit: 1,
        cursor: None,
    };
    let query = ConsoleQuery::Explorer {
        workflow_id: "workflow:1".into(),
        page: request.clone(),
    };
    let binding = query.binding(&scope()).unwrap();
    let records = [
        node(
            0,
            ConsoleExplorerData::ChildWait {
                member_keys: vec!["review:0".into()],
                resume: "collect".into(),
                applied_at: 20,
                resumed_activation_id: None,
            },
        ),
        node(0, entrypoint()),
        node(1, entrypoint()),
    ];
    let mut request = request;
    for record in records {
        let next_cursor = Some(request.next_cursor(&binding, &record.position()).unwrap());
        let result = ConsolePage {
            items: vec![record.clone()],
            next_cursor: next_cursor.clone(),
            observed_at: 30,
        };
        result.validate(&request, &binding).unwrap();
        request.cursor = next_cursor;
        assert_eq!(request.validate(&binding).unwrap(), Some(record.position()));
    }
    let wrong_parent = ConsoleQuery::Explorer {
        workflow_id: "other".into(),
        page: request,
    };
    assert!(wrong_parent.validate(&scope()).is_err());
}

#[test]
fn accepted_and_rejected_decisions_do_not_imply_applied_effects() {
    let mut record = entrypoint();
    record.validate().unwrap(); // Task success alone does not apply the decision.
    let ConsoleExplorerData::Entrypoint {
        applied_at, error, ..
    } = &mut record
    else {
        unreachable!()
    };
    *applied_at = Some(21);
    *error = Some(ApplicationError {
        kind: "rejected".into(),
        message: "Program binding conflicted".into(),
    });
    record.validate().unwrap(); // A rejection may also carry applied_at.
    let ConsoleExplorerData::Entrypoint { decision_kind, .. } = &mut record else {
        unreachable!()
    };
    *decision_kind = Some(ConsoleDecisionKind::Continue);
    assert!(record.validate().is_err());
    let ConsoleExplorerData::Entrypoint {
        error,
        resumed_activation_id,
        ..
    } = &mut record
    else {
        unreachable!()
    };
    *error = None;
    *resumed_activation_id = Some("next".into());
    record.validate().unwrap();
    let ConsoleExplorerData::Entrypoint { applied_at, .. } = &mut record else {
        unreachable!()
    };
    *applied_at = None;
    assert!(record.validate().is_err());
}

#[test]
fn explorer_preserves_valid_workflow_error_messages() {
    for message in ["", "first line\nsecond line\twith detail"] {
        let mut record = entrypoint();
        let ConsoleExplorerData::Entrypoint {
            applied_at, error, ..
        } = &mut record
        else {
            unreachable!()
        };
        *applied_at = Some(21);
        *error = Some(ApplicationError {
            kind: "rejected".into(),
            message: message.into(),
        });
        record.validate().unwrap();
    }
}

#[test]
fn closed_waits_do_not_require_a_wake_but_claimed_wakes_require_evidence() {
    let mut wait = ConsoleExplorerData::ExternalWait {
        key: "approval:0".into(),
        wait_kind: ConsoleWaitKind::Event,
        deadline: Some(30),
        registered_at: 10,
        closed_at: Some(20),
        wake_reason: None,
        resumed_activation_id: None,
    };
    wait.validate().unwrap(); // Cancellation/failure closes without scheduling.
    let ConsoleExplorerData::ExternalWait { wake_reason, .. } = &mut wait else {
        unreachable!()
    };
    *wake_reason = Some(ConsoleWakeReason::Timeout);
    assert!(wait.validate().is_err());
    let ConsoleExplorerData::ExternalWait {
        resumed_activation_id,
        ..
    } = &mut wait
    else {
        unreachable!()
    };
    *resumed_activation_id = Some("next".into());
    wait.validate().unwrap();
    let ConsoleExplorerData::ExternalWait { wake_reason, .. } = &mut wait else {
        unreachable!()
    };
    *wake_reason = Some(ConsoleWakeReason::Timer);
    assert!(wait.validate().is_err());
}

#[test]
fn local_result_acceptance_and_optional_callable_observation_are_independent() {
    let mut local = ConsoleExplorerData::Local {
        key: "tests:0".into(),
        callable: "program:tests".into(),
        accepted_at: None,
        accepting_attempt_id: None,
        observation: None,
    };
    assert!(local.validate().is_err()); // No evidence is not "not started".
    let ConsoleExplorerData::Local { observation, .. } = &mut local else {
        unreachable!()
    };
    *observation = Some(ConsoleLocalObservation {
        attempt_id: "attempt:1".into(),
        started_at: 10,
        elapsed_us: ConsoleU64(1200),
        state: "failed".into(),
    });
    local.validate().unwrap();
    let ConsoleExplorerData::Local {
        observation,
        accepted_at,
        accepting_attempt_id,
        ..
    } = &mut local
    else {
        unreachable!()
    };
    *observation = None;
    *accepted_at = Some(20);
    *accepting_attempt_id = Some("attempt:1".into());
    local.validate().unwrap(); // Older workers can have accepted-result evidence only.
    let ConsoleExplorerData::Local {
        accepting_attempt_id,
        ..
    } = &mut local
    else {
        unreachable!()
    };
    *accepting_attempt_id = None;
    assert!(local.validate().is_err());
}

#[test]
fn child_kind_state_and_unavailability_remain_explicit() {
    let mut child = ConsoleExplorerData::Child {
        key: "review:0".into(),
        execution: ConsoleExecutionIdentity {
            kind: ConsoleExecutionKind::Workflow,
            id: "wf:child".into(),
        },
        program: ProgramRef {
            id: "review".into(),
            version: "1.0.0".into(),
        },
        fork_key: Some("validate:0".into()),
        availability: ConsoleEvidenceAvailability::Available,
        state: Some(ConsoleExecutionState::Failing),
        submitted_at: 10,
        terminal_at: None,
    };
    child.validate().unwrap();
    let ConsoleExplorerData::Child { state, .. } = &mut child else {
        unreachable!()
    };
    *state = Some(ConsoleExecutionState::Queued);
    assert!(child.validate().is_err());
    let ConsoleExplorerData::Child {
        state,
        availability,
        ..
    } = &mut child
    else {
        unreachable!()
    };
    *state = None;
    *availability = ConsoleEvidenceAvailability::Unavailable;
    child.validate().unwrap();
}

#[test]
fn explorer_wire_shape_is_flat_and_does_not_include_application_payloads() {
    let record = node(0, entrypoint());
    let encoded = serde_json::to_value(&record).unwrap();
    assert_eq!(encoded["kind"], "entrypoint");
    assert_eq!(encoded["revision"], "0");
    assert!(encoded.get("data").is_none());
    let decoded: ConsoleExplorerNode = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    let mut extra = encoded;
    extra["input"] = json!({"private": true});
    assert!(serde_json::from_value::<ConsoleExplorerNode>(extra).is_err());
}

#[test]
fn console_three_rejects_legacy_explorer_kind_without_changing_failure_stage() {
    let mut encoded = serde_json::to_value(node(0, entrypoint())).unwrap();
    assert_eq!(encoded["kind"], "entrypoint");
    encoded["kind"] = json!("phase");
    assert!(serde_json::from_value::<ConsoleExplorerNode>(encoded).is_err());
    // Worker failure stage is a separate protocol and keeps its original enum.
    assert_eq!(
        serde_json::to_value(ledgence_worker_api::Phase::Execution).unwrap(),
        "execution"
    );
}

#[test]
fn every_legacy_explorer_cursor_is_rejected_including_unchanged_node_kinds() {
    let query = ConsoleQuery::Explorer {
        workflow_id: "workflow:1".into(),
        page: Default::default(),
    };
    let current = query.binding(&scope()).unwrap();
    assert_eq!(current.endpoint, "workflows/explorer/v4");
    let mut old = current.clone();
    for endpoint in ["workflows/explorer", "workflows/explorer/v3"] {
        old.endpoint = endpoint;
        for kind in [
            "entrypoint",
            "phase",
            "child",
            "fork",
            "local",
            "child_wait",
            "external_wait",
        ] {
            let position = vec![
                ConsoleKey::Number(ConsoleU64(0)),
                ConsoleKey::Text(kind.into()),
                ConsoleKey::Text(
                    if matches!(kind, "phase" | "child_wait") {
                        kind
                    } else {
                        "record:0"
                    }
                    .into(),
                ),
            ];
            let request = ConsolePagination {
                limit: 2,
                cursor: Some(
                    ConsolePagination::default()
                        .next_cursor(&old, &position)
                        .unwrap(),
                ),
            };
            assert!(request.validate(&old).is_ok());
            assert!(
                request.validate(&current).is_err(),
                "legacy {kind} cursor must restart under C4"
            );
        }
    }
    // Endpoints whose ordering did not change keep their independent bindings.
    let other = ConsoleQuery::Attempts {
        task_id: "activation:0".into(),
        page: Default::default(),
    };
    let binding = other.binding(&scope()).unwrap();
    assert_eq!(binding.endpoint, "tasks/attempts");
    let position = vec![
        ConsoleKey::Number(ConsoleU64(2)),
        ConsoleKey::Text("attempt:2".into()),
    ];
    let request = ConsolePagination {
        limit: 1,
        cursor: Some(
            ConsolePagination::default()
                .next_cursor(&binding, &position)
                .unwrap(),
        ),
    };
    assert_eq!(request.validate(&binding).unwrap(), Some(position));
}
