use ledgence_orchestration_api::{console::*, *};
use ledgence_worker_api::ProgramRef;
use serde_json::json;

fn node(activation: &str, data: ConsoleExplorerData) -> ConsoleExplorerNode {
    let mut node = ConsoleExplorerNode {
        id: serde_json::to_string(&(data.kind(), "workflow:1", activation, data.key())).unwrap(),
        activation_id: activation.into(),
        revision: ConsoleU64(0),
        entrypoint: "review".into(),
        relations: Vec::new(),
        data,
    };
    node.relations = node.derive_relations("workflow:1").unwrap();
    node.validate().unwrap();
    node
}
fn local(accepted: bool, attempt: &str) -> ConsoleExplorerData {
    ConsoleExplorerData::Local {
        key: "summary".into(),
        callable: "program:summary".into(),
        accepted_at: accepted.then_some(20),
        accepting_attempt_id: accepted.then(|| attempt.into()),
        observation: (!accepted).then(|| ConsoleLocalObservation {
            attempt_id: attempt.into(),
            started_at: 10,
            elapsed_us: ConsoleU64(5),
            state: "failed".into(),
        }),
    }
}
fn child(key: &str) -> ConsoleExplorerData {
    ConsoleExplorerData::Child {
        key: key.into(),
        execution: ConsoleExecutionIdentity {
            kind: ConsoleExecutionKind::Workflow,
            id: "child_workflow".into(),
        },
        program: ProgramRef {
            id: "review".into(),
            version: "1".into(),
        },
        fork_key: Some("fork:0".into()),
        availability: ConsoleEvidenceAvailability::Unavailable,
        state: None,
        submitted_at: 10,
        terminal_at: Some(20),
    }
}

#[test]
fn fork_and_child_supply_the_same_membership_without_loaded_endpoints() {
    let key = "review:\"東京\\0";
    let branch = node("activation:0", child(key));
    let fork = node(
        "activation:0",
        ConsoleExplorerData::Fork {
            key: "fork:0".into(),
            branch_keys: vec![key.into()],
            accepted_at: 10,
            accepting_attempt_id: "attempt:1".into(),
        },
    );
    assert_eq!(branch.relations.len(), 2);
    assert_eq!(
        branch.relations[0].kind,
        ConsoleExplorerRelationKind::Invokes
    );
    assert_eq!(branch.relations[1], fork.relations[1]);
    assert_eq!(
        fork.relations[0].kind,
        ConsoleExplorerRelationKind::Registers
    );
    assert_eq!(
        branch.relations[1].target,
        ConsoleExplorerReference::Child { key: key.into() }
    );
    let id: serde_json::Value = serde_json::from_str(&branch.relations[1].id).unwrap();
    assert_eq!(
        id,
        json!(["workflow:1", "branch", {"kind":"fork","key":"fork:0"}, {"kind":"child","key":key}])
    );
}

#[test]
fn local_relations_use_activation_scope_not_attempt_or_observation_timing() {
    let accepted = node("activation:0", local(true, "attempt:1"));
    let retry = node("activation:0", local(true, "attempt:2"));
    let failed = node("activation:0", local(false, "attempt:3"));
    let next = node("activation:1", local(true, "attempt:4"));
    assert_eq!(accepted.relations, retry.relations);
    assert_eq!(accepted.relations, failed.relations);
    assert_ne!(accepted.relations, next.relations);
    assert_eq!(accepted.relations.len(), 1);
    assert_eq!(
        accepted.relations[0].source,
        ConsoleExplorerReference::Entrypoint {
            activation_id: "activation:0".into()
        }
    );
    assert_eq!(
        accepted.relations[0].target,
        ConsoleExplorerReference::Local {
            activation_id: "activation:0".into(),
            key: "summary".into()
        }
    );
}

#[test]
fn later_join_preserves_workflow_scoped_members_and_terminal_policy_at_maximum_size() {
    let member_keys: Vec<_> = (0..WORKFLOW_MAX_COMMANDS)
        .map(|index| format!("child:{index}"))
        .collect();
    let join = node(
        "activation:later",
        ConsoleExplorerData::ChildWait {
            member_keys: member_keys.clone(),
            resume: "review".into(),
            applied_at: 30,
            resumed_activation_id: Some("activation:next".into()),
        },
    );
    assert_eq!(join.relations.len(), CONSOLE_EXPLORER_MAX_RELATIONS);
    for (relation, key) in join.relations[1..=WORKFLOW_MAX_COMMANDS]
        .iter()
        .zip(member_keys)
    {
        assert_eq!(relation.kind, ConsoleExplorerRelationKind::AwaitsTerminal);
        assert_eq!(relation.source, ConsoleExplorerReference::Child { key });
        assert_eq!(
            relation.target,
            ConsoleExplorerReference::ChildWait {
                activation_id: "activation:later".into()
            }
        );
    }
    assert_eq!(
        join.relations.last().unwrap().kind,
        ConsoleExplorerRelationKind::Resumes
    );
}

#[test]
fn entrypoint_resume_requires_continue_and_wait_resume_requires_recorded_wake() {
    for decision in [
        ConsoleDecisionKind::Continue,
        ConsoleDecisionKind::Suspend,
        ConsoleDecisionKind::Wait,
    ] {
        let entrypoint = node(
            "activation:0",
            ConsoleExplorerData::Entrypoint {
                state: Some(TaskState::Succeeded),
                availability: ConsoleEvidenceAvailability::Available,
                submitted_at: 10,
                terminal_at: Some(20),
                applied_at: Some(21),
                decision_kind: Some(decision),
                error: None,
                resumed_activation_id: Some("activation:next".into()),
            },
        );
        assert_eq!(
            entrypoint.relations.len(),
            usize::from(decision == ConsoleDecisionKind::Continue)
        );
    }
    for wake in [
        None,
        Some(ConsoleWakeReason::Event),
        Some(ConsoleWakeReason::Timeout),
    ] {
        let wait = node(
            "activation:0",
            ConsoleExplorerData::ExternalWait {
                key: "approval".into(),
                wait_kind: ConsoleWaitKind::Event,
                deadline: Some(30),
                registered_at: 10,
                closed_at: Some(30),
                wake_reason: wake,
                resumed_activation_id: wake.map(|_| "activation:next".into()),
            },
        );
        assert_eq!(wait.relations.len(), 1 + usize::from(wake.is_some()));
    }
    let rejected = node(
        "activation:0",
        ConsoleExplorerData::Entrypoint {
            state: Some(TaskState::Succeeded),
            availability: ConsoleEvidenceAvailability::Available,
            submitted_at: 10,
            terminal_at: Some(20),
            applied_at: Some(21),
            decision_kind: None,
            error: Some(ApplicationError {
                kind: "rejected".into(),
                message: "Conflict".into(),
            }),
            resumed_activation_id: None,
        },
    );
    assert!(rejected.relations.is_empty());
}

#[test]
fn missing_duplicate_fabricated_or_cross_scope_relation_evidence_is_rejected() {
    let valid = node("activation:0", local(true, "attempt:1"));
    let mut missing = serde_json::to_value(&valid).unwrap();
    missing.as_object_mut().unwrap().remove("relations");
    assert!(serde_json::from_value::<ConsoleExplorerNode>(missing).is_err());
    for modification in 0..7 {
        let mut invalid = valid.clone();
        match modification {
            0 => invalid.relations.clear(),
            1 => invalid.relations.push(invalid.relations[0].clone()),
            2 => invalid.relations[0].kind = ConsoleExplorerRelationKind::AwaitsTerminal,
            3 => {
                invalid.relations[0].source = ConsoleExplorerReference::Entrypoint {
                    activation_id: "other".into(),
                }
            }
            4 => {
                invalid.relations[0].id =
                    invalid.relations[0].id.replace("workflow:1", "workflow:2")
            }
            5 => {
                invalid.relations[0].target = ConsoleExplorerReference::Child {
                    key: "summary".into(),
                }
            }
            6 => {
                invalid.relations =
                    vec![invalid.relations[0].clone(); CONSOLE_EXPLORER_MAX_RELATIONS + 1]
            }
            _ => unreachable!(),
        }
        assert!(invalid.validate().is_err(), "modification {modification}");
    }
    for unknown in ["workflow_id", "attempt_id", "parent_id"] {
        let mut invalid = serde_json::to_value(&valid).unwrap();
        invalid["relations"][0]["target"][unknown] = json!("unsupported");
        assert!(serde_json::from_value::<ConsoleExplorerNode>(invalid).is_err());
    }
}
