use super::*;
use serde_json::json;
fn principal(credential: &str) -> VerifiedPrincipal {
    let audience = AccessAudience {
        authority_id: "authority".into(),
        instance_id: "instance".into(),
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "ns".into(),
        },
    };
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience.issuer(),
            subject: "worker".into(),
        },
        audience,
        credential.into(),
        PrincipalClass::Worker,
        1000,
        vec![AccessCapability::WorkerExecute],
    )
    .unwrap()
}
fn command() -> EffectClaimV2Command {
    EffectClaimV2Command {
        owner: LeaseOwner {
            scope: principal("one").audience().scope.clone(),
            task_id: "activation".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        },
        workflow_id: "workflow".into(),
        approval_key: "review\\u0000".into(),
        action: ApprovalAction {
            name: "send".into(),
            version: "1".into(),
            arguments: json!({"n":1,"nul":"\0","text":"\\u0000"}),
        },
        lookup: "lookup".into(),
        claim_id: "claim".into(),
    }
}
fn record(c: &EffectClaimV2Command) -> LocalStepRecord {
    LocalStepRecord {
        key: approved_effect_step_key(&c.approval_key),
        callable: c.action.name.clone(),
        input: json!({"v":1,"action_json":serde_json::to_string(&c.action).unwrap(),"lookup":c.lookup}),
        output: json!(null),
    }
}
#[test]
fn replay_has_no_fabricated_worker_and_binds_exact_reserved_key_and_canonical_action() {
    let c = command();
    let p = principal("one");
    let r = EffectClaimV2Receipt::Replay {
        audience: p.audience().clone(),
        occurrence: c.occurrence(),
        record: record(&c),
    };
    r.validate_for(&p, &c).unwrap();
    let value = serde_json::to_value(&r).unwrap();
    assert!(value.get("worker").is_none());
    assert!(value.get("claim").is_none());
    for changed in 0..4 {
        let mut wrong = record(&c);
        match changed {
            0 => wrong.key = approved_effect_step_key("other"),
            1 => {
                let mut a = c.action.clone();
                a.arguments["n"] = json!(1.0);
                wrong.input["action_json"] = json!(serde_json::to_string(&a).unwrap());
            }
            2 => wrong.input["v"] = json!(1.0),
            _ => wrong.input["source"] = json!("invented"),
        };
        let wrong = EffectClaimV2Receipt::Replay {
            audience: p.audience().clone(),
            occurrence: c.occurrence(),
            record: wrong,
        };
        assert!(!wrong.matches(&c).unwrap());
        assert!(matches!(
            wrong.validate_for(&p, &c),
            Err(ContractError::Unavailable(_))
        ));
    }
}
#[test]
fn stable_worker_rotation_preserves_source_and_cannot_be_deserialized_as_an_operator() {
    let first = EffectWorkerIdentity::from_principal(&principal("one")).unwrap();
    let second = EffectWorkerIdentity::from_principal(&principal("two")).unwrap();
    assert!(first.same_worker(&second));
    assert_ne!(first.credential_id, second.credential_id);
    let mut value = serde_json::to_value(&first).unwrap();
    value["class"] = json!("operator");
    assert!(serde_json::from_value::<EffectWorkerIdentity>(value).is_err());
    let mut different = second.clone();
    different.subject = "another".into();
    assert!(!first.same_worker(&different));
    different = second;
    different.audience.instance_id = "another".into();
    assert!(!first.same_worker(&different));
}
#[test]
fn claim_replay_permission_is_lookup_only_and_runtime_context_fields_are_not_accepted() {
    let c = command();
    let p = principal("one");
    let claim = EffectClaimBinding {
        scope: c.owner.scope.clone(),
        workflow_id: c.workflow_id.clone(),
        activation_id: c.owner.task_id.clone(),
        attempt_id: c.owner.attempt_id.clone(),
        approval_key: c.approval_key.clone(),
        action: c.action.clone(),
        lookup: c.lookup.clone(),
        claim_id: c.claim_id.clone(),
        worker: EffectWorkerIdentity::from_principal(&p).unwrap(),
        claimed_at: 0,
        initial_permission: ApprovedEffectDisposition::Dispatch,
        recorded_revision: 2,
        already_accepted: true,
    };
    assert!(
        EffectClaimV2Receipt::Dispatch {
            claim: claim.clone()
        }
        .validate()
        .is_err()
    );
    EffectClaimV2Receipt::LookupOnly {
        claim: claim.clone(),
    }
    .validate_for(&p, &c)
    .unwrap();
    let mut malformed = claim;
    malformed.already_accepted = false;
    assert!(
        EffectClaimV2Receipt::LookupOnly { claim: malformed }
            .validate()
            .is_err()
    );
    let mut v = serde_json::to_value(&c).unwrap();
    v["worker"] = json!({"subject":"fake"});
    assert!(serde_json::from_value::<EffectClaimV2Command>(v).is_err());
    let mut v = serde_json::to_value(&c).unwrap();
    v["claim_id"] = Value::Null;
    assert!(serde_json::from_value::<EffectClaimV2Command>(v).is_err());
}
#[test]
fn inspection_cursor_binds_full_occurrence_but_not_page_size() {
    let c = command();
    let q = EffectInspectQuery {
        occurrence: c.occurrence(),
        cursor: None,
        limit: 1,
    };
    let cursor = q.next_cursor(3).unwrap();
    let mut next = EffectInspectQuery {
        cursor: Some(cursor),
        limit: 4,
        ..q
    };
    assert_eq!(next.validate().unwrap(), Some(3));
    next.occurrence.approval_key = "other".into();
    assert!(next.validate().is_err());
    for limit in [0, 5, u32::MAX] {
        assert!(
            EffectInspectQuery {
                occurrence: c.occurrence(),
                cursor: None,
                limit
            }
            .validate()
            .is_err()
        );
    }
}
#[test]
fn observation_is_evidence_only_and_bounded_without_a_lease_owner() {
    let c = command();
    let o = EffectObservationCommand {
        occurrence: c.occurrence(),
        claim_id: c.claim_id.clone(),
        observation_id: "observation".into(),
        reported_callback: EffectReportedCallback::Lookup,
        output: json!({"n":1.0,"nul":"\0"}),
    };
    o.validate().unwrap();
    let mut value = serde_json::to_value(&o).unwrap();
    value["owner"] = serde_json::to_value(&c.owner).unwrap();
    assert!(serde_json::from_value::<EffectObservationCommand>(value).is_err());
    let mut missing = serde_json::to_value(&o).unwrap();
    missing.as_object_mut().unwrap().remove("output");
    assert!(serde_json::from_value::<EffectObservationCommand>(missing).is_err());
    let mut explicit_null = o.clone();
    explicit_null.output = Value::Null;
    explicit_null.validate().unwrap();
    let mut large = o;
    large.output = json!("x".repeat(WORKFLOW_LOCAL_RECORD_MAX_BYTES));
    assert!(large.validate().is_err());
}

#[test]
fn immutable_detail_cannot_skip_or_hide_a_remaining_entry() {
    use ledgence_worker_api::{Digest, ProgramDescriptor, ProgramRef};
    let c = command();
    let o = c.occurrence();
    let approval = ApprovalSnapshot {
        scope: o.scope.clone(),
        workflow_id: o.workflow_id.clone(),
        key: o.approval_key.clone(),
        activation_id: "request-activation".into(),
        revision: 0,
        action: c.action.clone(),
        protection: ApprovalProtection::ExternalEffect,
        proposed_arguments: None,
        created_at: 0,
        deadline: 10,
        status: ApprovalStatus::Approved,
        decision: Some(ApprovalDecisionRecord {
            decision_id: "decision".into(),
            decision: ApprovalDecision::Approve,
            reviewer: "label".into(),
            reason: None,
            decided_at: 1,
        }),
        resumed_activation_id: Some(o.activation_id.clone()),
    };
    let program = ProgramRef {
        id: "example".into(),
        version: "1".into(),
    };
    let binding = EffectEvidenceBinding {
        occurrence: o.clone(),
        approval,
        program: ProgramDescriptor {
            program: program.clone(),
            digest: Digest(format!("sha256:{}", "a".repeat(64))),
            size: 1,
        },
        key: approved_effect_step_key(&c.approval_key),
        lookup: Some(c.lookup.clone()),
        approval_acceptance_event_id: None,
    };
    let summary = EffectEvidenceSummary {
        occurrence: o.clone(),
        program,
        callable: c.action.name.clone(),
        action_version: c.action.version.clone(),
        dispatch_provenance: EffectDispatchProvenance::VerifiedClaim {
            claim_id: c.claim_id.clone(),
        },
        journal_state: EffectJournalState::Unconfirmed,
        has_discrepancy: false,
        entry_count: 2,
        effect_revision: 3,
        created_at: 0,
        updated_at: 1,
    };
    let entry = EffectEvidenceEntry::Claim {
        sequence: 1,
        recorded_at: 1,
        recorded_revision: 2,
        claim_id: c.claim_id,
        attempt_id: c.owner.attempt_id,
        worker: EffectWorkerIdentity::from_principal(&principal("one")).unwrap(),
        initial_permission: ApprovedEffectDisposition::Dispatch,
    };
    let q = EffectInspectQuery {
        occurrence: o,
        cursor: None,
        limit: 4,
    };
    let mut page = EffectEvidenceDetail {
        binding,
        summary,
        entries: vec![entry],
        next_cursor: Some(q.next_cursor(1).unwrap()),
    };
    page.validate_for(&q).unwrap();
    page.next_cursor = None;
    assert!(page.validate_for(&q).is_err());
    page.next_cursor = Some(q.next_cursor(1).unwrap());
    if let EffectEvidenceEntry::Claim { sequence, .. } = &mut page.entries[0] {
        *sequence = 2;
    }
    assert!(page.validate_for(&q).is_err());
    page.entries.clear();
    assert!(page.validate_for(&q).is_err());
}
