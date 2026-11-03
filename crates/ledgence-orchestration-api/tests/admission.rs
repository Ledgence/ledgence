use ledgence_orchestration_api::*;
use ledgence_worker_api::{Digest, ProgramDescriptor, ProgramRef};
use serde_json::{Value, json};

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
fn occurrence() -> AdmissionOccurrence {
    AdmissionOccurrence {
        scope: scope(),
        workflow_id: "workflow".into(),
        origin_activation_id: "origin".into(),
        key: "call:0".into(),
    }
}
fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "authority".into(),
        instance_id: "instance".into(),
        scope: scope(),
    }
}
fn operation() -> AdmissionOperation {
    AdmissionOperation {
        callable: "app:summarize".into(),
        kind: AdmissionOperationKind::Model,
        version: "1".into(),
        arguments: json!({"text":"hello","temperature":1.0}),
    }
}
fn binding() -> AdmissionBinding {
    AdmissionBinding {
        resource: "models.shared".into(),
        program: ProgramDescriptor {
            program: ProgramRef {
                id: "app".into(),
                version: "1.0.0".into(),
            },
            digest: Digest(format!("sha256:{}", "a".repeat(64))),
            size: 1,
        },
        operation: operation(),
    }
}
fn owner() -> LeaseOwner {
    LeaseOwner {
        scope: scope(),
        task_id: "resumed".into(),
        attempt_id: "attempt".into(),
        lease_id: "lease".into(),
        generation: 1,
        worker_session_id: "session".into(),
        consumer_id: 0,
    }
}
fn principal(credential: &str) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: "issuer".into(),
            subject: "worker".into(),
        },
        audience(),
        credential.into(),
        PrincipalClass::Worker,
        100_000,
        vec![AccessCapability::WorkerExecute],
    )
    .unwrap()
}
fn source() -> AdmissionSource {
    AdmissionSource {
        occurrence: occurrence(),
        source_id: "source".into(),
        start_id: "start".into(),
        finish_id: "finish".into(),
        generation: 1,
        binding_digest: binding().digest().unwrap(),
        owner: owner(),
        worker: AdmissionWorkerIdentity::from_principal(&principal("credential")).unwrap(),
        accepted_at: 10,
        start_deadline_at: 1010,
    }
}
fn offer() -> AdmissionOffer {
    AdmissionOffer {
        occurrence: occurrence(),
        generation: 1,
        policy_revision: 1,
        activation_id: "resumed".into(),
        expires_at: 100,
        binding_digest: binding().digest().unwrap(),
    }
}
fn receipt() -> AdmissionFinishReceipt {
    AdmissionFinishReceipt {
        source: source(),
        output: json!({"answer":"yes"}),
        accepted_at: 11,
        already_accepted: false,
    }
}
fn wake() -> AdmissionWake {
    AdmissionWake {
        key: "wait:0".into(),
        occurrence: occurrence(),
        waiting_activation_id: "origin".into(),
        resumed_activation_id: "resumed".into(),
        resumed_revision: 1,
        binding: binding(),
        selected_at: 1,
        selection: AdmissionSelection::Offered { offer: offer() },
    }
}
fn snapshot() -> AdmissionSnapshot {
    AdmissionSnapshot {
        occurrence: occurrence(),
        binding: binding(),
        state: AdmissionState::Finished,
        revision: 3,
        created_at: 0,
        updated_at: 11,
        deadline_at: 1000,
        offer: None,
        source: Some(source()),
        finish: Some(receipt()),
    }
}
fn policy() -> ResourcePolicy {
    ResourcePolicy {
        enabled: true,
        max_occupancy: 2,
        rate: Some(ResourceRate {
            numerator: 1,
            period_ms: 1000,
            burst: 2,
        }),
        offer_timeout_ms: 1000,
        start_timeout_ms: 1000,
    }
}

#[test]
fn operation_binding_freezes_kind_version_program_and_exact_numeric_arguments() {
    let a = binding();
    let mut b = a.clone();
    b.operation.arguments = json!({"temperature":1.0,"text":"hello"});
    assert_eq!(a.digest().unwrap(), b.digest().unwrap());
    b.operation.arguments["temperature"] = json!(1);
    assert_ne!(a.digest().unwrap(), b.digest().unwrap());
    b = a.clone();
    b.operation.version = "2".into();
    assert_ne!(a.digest().unwrap(), b.digest().unwrap());
    b = a.clone();
    b.program.digest = Digest(format!("sha256:{}", "b".repeat(64)));
    assert_ne!(a.digest().unwrap(), b.digest().unwrap());
    for callable in [
        "app",
        "app:<lambda>",
        "app:outer.<locals>.inner",
        "app:method:other",
    ] {
        let mut o = operation();
        o.callable = callable.into();
        assert!(o.validate().is_err());
    }
    let mut o = operation();
    o.arguments = Value::Null;
    assert!(o.validate().is_err());
    o.arguments = json!({"text":"x".repeat(ADMISSION_ARGUMENTS_MAX_BYTES)});
    assert!(o.validate().is_err());
}
#[test]
fn wake_and_preloaded_result_preserve_occurrence_offer_and_acceptance_provenance() {
    let w = wake();
    w.validate().unwrap();
    w.validate_result(&receipt()).unwrap();
    for changed in ["occurrence", "digest", "generation", "owner"] {
        let mut r = receipt();
        match changed {
            "occurrence" => r.source.occurrence.key = "other".into(),
            "digest" => r.source.binding_digest = Digest(format!("sha256:{}", "b".repeat(64))),
            "generation" => r.source.generation = 2,
            _ => r.source.owner.task_id = "other".into(),
        };
        assert!(w.validate_result(&r).is_err(), "{changed}");
    }
    let mut context = WorkflowActivationContext {
        admission_result: Some(receipt()),
        parent_workflow_id: None,
        root_workflow_id: None,
        v: 1,
        workflow_id: "workflow".into(),
        activation_id: "resumed".into(),
        revision: 1,
        continuation: "finish".into(),
        state: Value::Null,
        inputs: Default::default(),
        local_steps: vec![],
        wake: Some(WorkflowWake::ResourceAdmission(Box::new(w.clone()))),
    };
    context.validate_for_owner(&owner()).unwrap();
    context.revision = 2;
    assert!(context.validate().is_err());
    context.revision = 1;
    context.wake = None;
    assert!(context.validate().is_err());
    let mut review = w.clone();
    review.selection = AdmissionSelection::NeedsReview {
        source_id: "source".into(),
    };
    review.validate_result(&receipt()).unwrap();
    review.selection = AdmissionSelection::NeedsReview {
        source_id: "different-source".into(),
    };
    assert!(review.validate_result(&receipt()).is_err());
    let mut closed = w;
    closed.selection = AdmissionSelection::Closed {
        reason: AdmissionClosedReason::AdministrativelyReleased,
    };
    closed.validate().unwrap();
    assert!(closed.validate_result(&receipt()).is_err());
}
#[test]
fn start_permission_and_finish_receipts_bind_worker_source_and_exact_facts() {
    let c = AdmissionStartCommand {
        owner: owner(),
        occurrence: occurrence(),
        offer_generation: 1,
        start_id: "start".into(),
        finish_id: "finish".into(),
    };
    let granted = AdmissionStartReceipt::Granted {
        source: Box::new(source()),
    };
    granted.validate_for(&principal("rotated"), &c).unwrap();
    let mut wrong = c.clone();
    wrong.start_id = "different".into();
    assert!(granted.validate_for(&principal("rotated"), &wrong).is_err());
    let existing = AdmissionStartReceipt::Existing {
        snapshot: Box::new(snapshot()),
    };
    existing
        .validate_for(&principal("rotated"), &wrong)
        .unwrap();
    assert!(!matches!(existing, AdmissionStartReceipt::Granted { .. }));
    let command = AdmissionFinishCommand {
        source: source(),
        output: receipt().output,
    };
    receipt()
        .validate_for(&principal("rotated"), &command)
        .unwrap();
    let mut wrong = command;
    wrong.output = json!({"answer":"changed"});
    assert!(
        receipt()
            .validate_for(&principal("rotated"), &wrong)
            .is_err()
    );
}
#[test]
fn snapshot_cannot_invent_a_result_or_hide_still_occupied_source() {
    let mut stale = snapshot();
    stale.updated_at = 9;
    assert!(stale.validate().is_err());
    stale.updated_at = 10;
    assert!(stale.validate().is_err());
    snapshot().validate().unwrap();
    let mut s = snapshot();
    s.state = AdmissionState::Waiting;
    assert!(s.validate().is_err());
    s = snapshot();
    s.source = None;
    assert!(s.validate().is_err());
    s = snapshot();
    s.finish.as_mut().unwrap().source.source_id = "foreign".into();
    assert!(s.validate().is_err());
    s = snapshot();
    s.state = AdmissionState::AdministrativelyReleased;
    s.validate().unwrap();
    s.finish = None;
    s.validate().unwrap();
}
#[test]
fn rates_policy_updates_pages_and_audit_retain_precise_bounds() {
    policy().validate().unwrap();
    let mut p = policy();
    p.max_occupancy = 0;
    assert!(p.validate().is_err());
    let mut rate = policy().rate.unwrap();
    rate.burst = 1_000_001;
    assert!(rate.validate().is_err());
    let c = ResourcePutCommand {
        scope: scope(),
        name: "models.shared".into(),
        request_id: "put".into(),
        expected_revision: None,
        policy: policy(),
        reason: "Shared model quota".into(),
    };
    c.validate().unwrap();
    let r = ResourcePutReceipt {
        resource: ResourceRecord {
            scope: scope(),
            name: c.name.clone(),
            revision: 1,
            policy: policy(),
            updated_at: 1,
        },
        already_accepted: false,
    };
    assert!(r.matches(&c));
    let page = ResourcePage {
        items: vec![ResourceSnapshot {
            resource: r.resource,
            offered: 0,
            occupied: 4,
            unknown: 4,
            quarantined: false,
        }],
        next: None,
    };
    page.validate_for(&ResourceListQuery {
        scope: scope(),
        after: None,
        limit: 1,
    })
    .unwrap();
    let audit = AuditEvent {
        event_id: "event".into(),
        recorded_at: 10,
        audience: audience(),
        actor: AuditActor::from_principal(
            &VerifiedPrincipal::new(
                PrincipalIdentity {
                    issuer: "issuer".into(),
                    subject: "operator".into(),
                },
                audience(),
                "credential".into(),
                PrincipalClass::Operator,
                100_000,
                vec![AccessCapability::ResourceControl],
            )
            .unwrap(),
        ),
        operation: AuditOperation::AdmissionReleased {
            occurrence: occurrence(),
            source_id: "source".into(),
            request_id: "release".into(),
            revision: 4,
            reason: "Provider shutdown confirmed".into(),
        },
    };
    audit.validate().unwrap();
    assert_eq!(audit.operation.target().0, AuditResourceKind::Workflow);
    for class in [PrincipalClass::Worker, PrincipalClass::Service] {
        assert!(
            class
                .validate_capabilities(&[AccessCapability::ResourceControl])
                .is_err()
        );
    }
}
#[test]
fn strict_shapes_reject_null_and_unknown_fields_without_implicit_defaults() {
    let mut v = serde_json::to_value(wake()).unwrap();
    v["unknown"] = json!(true);
    assert!(serde_json::from_value::<AdmissionWake>(v).is_err());
    let mut v = serde_json::to_value(policy()).unwrap();
    v.as_object_mut().unwrap().remove("rate");
    assert!(serde_json::from_value::<ResourcePolicy>(v).is_err());
    let mut v = serde_json::to_value(operation()).unwrap();
    v.as_object_mut().unwrap().remove("arguments");
    assert!(serde_json::from_value::<AdmissionOperation>(v).is_err());
    let wait = WorkflowWait::ResourceAdmission {
        key: "wait".into(),
        request: AdmissionWaitRequest::New {
            resource: "model".into(),
            operation: operation(),
        },
        timeout_ms: 5000,
    };
    assert_eq!(wait.deadline(5).unwrap(), Some(5005));
    let config = AdmissionConfig {
        v: 1,
        audience: audience(),
        dispatch: AdmissionDispatchPolicy::Quarantined,
    };
    config.validate_for(&audience()).unwrap();
}
