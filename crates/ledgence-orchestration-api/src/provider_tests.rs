use crate::*;
use ledgence_worker_api::{Digest, ProgramDescriptor, ProgramRef};
use serde_json::{Value, json};
fn request_value() -> Value {
    json!({"v":1,"business":{"namespace":"exports","key":"export:1042"},"contract":{"adapter_id":"example.export","version":"1","digest":format!("sha256:{}","a".repeat(64)),"completion_criterion":"Artifact available or definitively failed","dispatch_callable":"app:start_export","reconcile_callable":"app:inspect_export","lookup":"by_binding"},"target":{"provider":"example","environment":"local","account":"demo","api_variant":"v1","operation":"export","endpoint_identity":"loopback-export","dimensions":{}},"deduplication":{"domain":{"provider":"example","namespace":"account","dimensions":{"account":"demo"}},"key":"export:1042","horizon":{"kind":"unknown"}},"credential_reference":"local-export","reconciliation":{"queue":"recovery","initial_delay_ms":30000,"interval_ms":5000,"maximum_interval_ms":300000,"max_checks":12,"max_elapsed_ms":3600000,"check_timeout_ms":10000}})
}
fn request() -> ProviderBindingRequest {
    serde_json::from_value(request_value()).unwrap()
}
fn descriptor() -> ProgramDescriptor {
    ProgramDescriptor {
        program: ProgramRef {
            id: "exports".into(),
            version: "1.0.0".into(),
        },
        digest: Digest(format!("sha256:{}", "b".repeat(64))),
        size: 1,
    }
}
fn action() -> ApprovalAction {
    ApprovalAction {
        name: "app:start_export".into(),
        version: "1".into(),
        arguments: json!({"dataset":"sales","amount":1}),
    }
}
fn occurrence() -> EffectOccurrence {
    EffectOccurrence {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "demo".into(),
        },
        workflow_id: "workflow".into(),
        activation_id: "dispatch".into(),
        approval_key: "export:0".into(),
    }
}
fn worker() -> EffectWorkerIdentity {
    EffectWorkerIdentity {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: occurrence().scope,
        },
        issuer: "issuer".into(),
        subject: "worker".into(),
        credential_id: "credential".into(),
    }
}
fn report() -> ProviderReport {
    ProviderReport {
        reported_target: request().target,
        provider_reference: Some("export-1042".into()),
        provider_request_id: None,
        knowledge: ProviderKnowledge::Succeeded {
            result: Value::Null,
        },
    }
}
#[test]
fn binding_pins_actual_program_and_canonical_effective_request() {
    let req = request();
    req.validate().unwrap();
    let binding = req.resolve(descriptor()).unwrap();
    let before = binding.digest(&action()).unwrap();
    let mut changed = action();
    changed.arguments = json!({"dataset":"sales","amount":1.0});
    assert_ne!(before, binding.digest(&changed).unwrap());
    let mut changed_binding = binding.clone();
    changed_binding.program.digest = Digest(format!("sha256:{}", "c".repeat(64)));
    assert_ne!(before, changed_binding.digest(&action()).unwrap());
    let mut changed = request_value();
    changed["program"] = serde_json::to_value(descriptor()).unwrap();
    assert!(serde_json::from_value::<ProviderBindingRequest>(changed).is_err());
}
#[test]
fn no_lookup_is_explicit_and_cannot_hide_a_callback() {
    let mut value = request_value();
    value["contract"]["lookup"] = json!("unavailable");
    let req: ProviderBindingRequest = serde_json::from_value(value.clone()).unwrap();
    assert!(req.validate().is_err());
    value["contract"]["reconcile_callable"] = Value::Null;
    serde_json::from_value::<ProviderBindingRequest>(value.clone())
        .unwrap()
        .validate()
        .unwrap();
    value["contract"]
        .as_object_mut()
        .unwrap()
        .remove("reconcile_callable");
    assert!(serde_json::from_value::<ProviderBindingRequest>(value).is_err());
}
#[test]
fn new_policy_is_bounded_without_lowering_ordinary_task_minimum() {
    let mut req = request();
    for timeout in [0, 999, 30001, u64::MAX] {
        req.reconciliation.check_timeout_ms = timeout;
        assert!(req.validate().is_err());
    }
    req.reconciliation.check_timeout_ms = 30000;
    req.validate().unwrap();
    req.reconciliation.initial_delay_ms = req.reconciliation.max_elapsed_ms;
    assert!(req.validate().is_err());
    assert_eq!(PROVIDER_ATTEMPT_TIMEOUT_MS, 60000);
}
#[test]
fn provider_approval_requires_exact_binding_and_legacy_omission_survives() {
    let ordinary = json!({"kind":"approval","key":"approve","action":action(),"timeout_ms":1000});
    let legacy: WorkflowWait = serde_json::from_value(ordinary.clone()).unwrap();
    legacy.validate().unwrap();
    assert_eq!(serde_json::to_value(legacy).unwrap(), ordinary);
    let mut provider = ordinary.clone();
    provider["protection"] = json!("provider_operation");
    assert!(
        serde_json::from_value::<WorkflowWait>(provider.clone())
            .unwrap()
            .validate()
            .is_err()
    );
    provider["provider_binding"] = request_value();
    serde_json::from_value::<WorkflowWait>(provider.clone())
        .unwrap()
        .validate()
        .unwrap();
    provider["provider_binding"] = Value::Null;
    assert!(serde_json::from_value::<WorkflowWait>(provider).is_err());
    let mut unexpected = ordinary;
    unexpected["provider_binding"] = request_value();
    assert!(
        serde_json::from_value::<WorkflowWait>(unexpected)
            .unwrap()
            .validate()
            .is_err()
    );
}
#[test]
fn successful_null_is_distinct_from_unknown_and_report_body_is_bounded() {
    let report = report();
    report.validate().unwrap();
    let encoded = serde_json::to_value(&report).unwrap();
    assert_eq!(
        encoded["knowledge"],
        json!({"kind":"succeeded","result":null})
    );
    let mut omitted = encoded;
    omitted["knowledge"]
        .as_object_mut()
        .unwrap()
        .remove("result");
    assert!(serde_json::from_value::<ProviderReport>(omitted).is_err());
    let mut large = report;
    large.knowledge = ProviderKnowledge::Succeeded {
        result: json!("x".repeat(PROVIDER_RESULT_MAX_BYTES)),
    };
    assert!(large.validate().is_err());
}
#[test]
fn crossing_ingress_preserves_evidence_only_without_a_live_acknowledgement() {
    let binding = request().resolve(descriptor()).unwrap();
    let ticket = ProviderOperationTicket {
        occurrence: occurrence(),
        binding_digest: binding.digest(&action()).unwrap(),
    };
    let command = ProviderObservationCommand {
        ticket: ticket.clone(),
        source_id: "source".into(),
        report_id: "report".into(),
        report: report(),
    };
    let receipt = ProviderReportReceipt {
        ticket,
        source_id: "source".into(),
        source: ProviderSourceReference::Dispatch {
            claim_id: "claim".into(),
        },
        report_id: "report".into(),
        report_digest: command.report.digest().unwrap(),
        worker: worker(),
        accepted_at: 10,
        recorded_revision: 3,
        accepted_via: ProviderReportIngress::EvidenceOnly,
        already_accepted: false,
    };
    assert!(
        ProviderReportReply::Accepted {
            receipt: receipt.clone()
        }
        .matches(&command)
        .unwrap()
    );
    assert!(
        !ProviderReportReply::ExistingReport {
            original_receipt: receipt.clone()
        }
        .matches(&command)
        .unwrap()
    );
    let mut changed = command.clone();
    changed.report.provider_request_id = Some("different".into());
    assert!(!receipt.matches(&changed).unwrap());
    let mut receipt = receipt;
    receipt.accepted_via = ProviderReportIngress::LiveDispatch;
    assert!(
        ProviderReportReply::ExistingReport {
            original_receipt: receipt
        }
        .matches(&command)
        .unwrap()
    );
}
#[test]
fn probe_envelope_never_interprets_application_failure_spelling() {
    let bad = json!({"v":1,"kind":"rejected","failure":"failed"});
    assert!(serde_json::from_value::<ProviderProbeRuntimeResult>(bad).is_err());
    let genuine: ProviderProbeRuntimeResult =
        serde_json::from_value(json!({"v":1,"kind":"rejected","failure":"contract_mismatch"}))
            .unwrap();
    genuine.validate().unwrap();
    let mut unknown = request_value();
    unknown["contract"]["lookup"] = json!("retry_post");
    assert!(serde_json::from_value::<ProviderBindingRequest>(unknown).is_err());
}
#[test]
fn provider_wait_preserves_full_width_and_requires_nullable_cursor() {
    let value = json!({"kind":"provider_operation","key":"provider:0","occurrence":occurrence(),"after_effect_revision":9007199254740993_u64});
    let wait: WorkflowWait = serde_json::from_value(value.clone()).unwrap();
    wait.validate().unwrap();
    assert_eq!(serde_json::to_value(wait).unwrap(), value);
    let mut missing = value;
    missing
        .as_object_mut()
        .unwrap()
        .remove("after_effect_revision");
    assert!(serde_json::from_value::<WorkflowWait>(missing).is_err());
}
#[test]
fn external_policy_is_bound_to_the_authenticated_installation() {
    let audience = worker().audience;
    let config = ProviderOperationConfig {
        v: 1,
        audience: audience.clone(),
        dispatch: ProviderDispatchPolicy::DeclaredRestoreQuarantine,
    };
    config.validate_for(&audience).unwrap();
    let mut changed = audience;
    changed.instance_id = "different".into();
    assert_eq!(config.validate_for(&changed), Err(ContractError::Conflict));
}

#[test]
fn provider_summary_keeps_handling_and_active_pending_filter_consistent() {
    let mut value = json!({"occurrence":occurrence(),"program":descriptor().program,"callable":"app:start_export","action_version":"1","dispatch_provenance":{"kind":"verified_claim","claim_id":"claim"},"journal_state":"unconfirmed","has_discrepancy":false,"entry_count":1,"effect_revision":2,"created_at":0,"updated_at":1,"provider":{"binding_digest":request().resolve(descriptor()).unwrap().digest(&action()).unwrap(),"knowledge":"unknown","selected_report":null,"conflicting_reports":[],"reconciliation":{"queue":"recovery","checks_used":0,"max_checks":2,"generation":0,"active_probe_id":null,"active_task_id":null,"first_due_at":1000,"next_due_at":1000,"deadline_at":60000,"stopped_reason":null},"handling":"unresolved","candidate_revision":null}});
    let active: EffectEvidenceSummary = serde_json::from_value(value.clone()).unwrap();
    active.validate().unwrap();
    assert!(ProviderInboxFilter::Pending.matches(&active));
    value["provider"]["reconciliation"]["next_due_at"] = Value::Null;
    value["provider"]["reconciliation"]["stopped_reason"] = json!("workflow_cancelled");
    let stopped: EffectEvidenceSummary = serde_json::from_value(value.clone()).unwrap();
    stopped.validate().unwrap();
    assert!(!ProviderInboxFilter::Pending.matches(&stopped));
    value["provider"]["handling"] = json!("operator_closed");
    assert!(
        serde_json::from_value::<EffectEvidenceSummary>(value.clone())
            .unwrap()
            .validate()
            .is_err()
    );
    value["provider"]["handling"] = json!("provider_terminal");
    assert!(
        serde_json::from_value::<EffectEvidenceSummary>(value.clone())
            .unwrap()
            .validate()
            .is_err()
    );
    value["provider"]["handling"] = json!("unresolved");
    value["journal_state"] = json!("confirmed");
    assert!(
        serde_json::from_value::<EffectEvidenceSummary>(value)
            .unwrap()
            .validate()
            .is_err()
    );
}

#[test]
fn provider_operator_dispute_requires_a_new_fact_after_an_assertive_assessment() {
    let reference = EffectResolutionReference {
        resolution_id: "resolution".into(),
        sequence: 4,
        accepted_at: 10,
        accepted_revision: 5,
        basis_revision: 4,
        supersedes_resolution_id: None,
        assessment_kind: EffectAssessmentKind::RecordSuccess,
        acceptance_event_id: "audit".into(),
    };
    for kind in [
        EffectAssessmentKind::RecordSuccess,
        EffectAssessmentKind::RecordFailure,
        EffectAssessmentKind::CloseUnknown,
    ] {
        for conflict in [3, 4, 5, 7, 8] {
            let mut reference = reference.clone();
            reference.assessment_kind = kind;
            let selection = ProviderSelection::OperatorAssessment {
                reference: Box::new(reference),
                outcome: EffectResolutionOutcome::Disputed {
                    first_conflicting_entry_sequence: conflict,
                },
            };
            assert_eq!(
                selection.validate(8, 10).is_ok(),
                kind != EffectAssessmentKind::CloseUnknown && (5..8).contains(&conflict)
            );
        }
    }
}

#[test]
fn governed_binding_is_versioned_without_changing_v1_bytes() {
    let legacy = request();
    assert_eq!(serde_json::to_value(&legacy).unwrap(), request_value());
    let resolved = legacy.resolve(descriptor()).unwrap();
    let mut old_resolved = request_value();
    old_resolved["program"] = serde_json::to_value(descriptor()).unwrap();
    assert_eq!(serde_json::to_value(&resolved).unwrap(), old_resolved);
    assert_eq!(
        resolved.digest(&action()).unwrap(),
        crate::provider_binding::provider_digest(&(old_resolved, action())).unwrap()
    );
    let mut governed = legacy.clone();
    governed.admission = Some(ProviderAdmissionRequirement {
        resource: "exports.shared".into(),
    });
    assert!(governed.validate().is_err());
    governed.v = 2;
    governed.validate().unwrap();
    assert_ne!(
        resolved.digest(&action()).unwrap(),
        governed
            .resolve(descriptor())
            .unwrap()
            .digest(&action())
            .unwrap()
    );
    governed.admission = None;
    assert!(governed.validate().is_err());
    let mut explicit_null = request_value();
    explicit_null["admission"] = Value::Null;
    assert!(serde_json::from_value::<ProviderBindingRequest>(explicit_null).is_err());
}
fn governed_admission_binding() -> AdmissionBinding {
    let mut binding = request();
    binding.v = 2;
    binding.admission = Some(ProviderAdmissionRequirement {
        resource: "exports.shared".into(),
    });
    let ticket = ProviderOperationTicket {
        occurrence: occurrence(),
        binding_digest: binding
            .resolve(descriptor())
            .unwrap()
            .digest(&action())
            .unwrap(),
    };
    AdmissionBinding::Provider(Box::new(ProviderAdmissionBinding {
        resource: "exports.shared".into(),
        program: descriptor(),
        provider: ProviderAdmissionSubject {
            ticket,
            binding,
            action: action(),
        },
    }))
}
#[test]
fn admission_subjects_are_disjoint_and_provider_intent_is_pinned() {
    let binding = governed_admission_binding();
    binding.validate().unwrap();
    let value = serde_json::to_value(&binding).unwrap();
    let mut mixed = value.clone();
    mixed["operation"] = json!({"callable":"app:run","kind":"tool","version":"1","arguments":{}});
    assert!(serde_json::from_value::<AdmissionBinding>(mixed).is_err());
    for (pointer, replacement) in [
        ("/resource", json!("other")),
        ("/provider/action/arguments", json!({"amount":2})),
        (
            "/program/digest",
            json!(format!("sha256:{}", "c".repeat(64))),
        ),
    ] {
        let mut altered = value.clone();
        *altered.pointer_mut(pointer).unwrap() = replacement;
        assert!(
            serde_json::from_value::<AdmissionBinding>(altered)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut null = value;
    null["provider"] = Value::Null;
    assert!(serde_json::from_value::<AdmissionBinding>(null).is_err());
}
#[test]
fn governed_report_retains_provider_size_and_explicit_call_completion() {
    let mut report = report();
    report.knowledge = ProviderKnowledge::Succeeded {
        result: json!("r".repeat(100 * 1024)),
    };
    report.validate().unwrap();
    let outcome = ProviderDispatchOutcome {
        call_completion: ProviderCallCompletion::Unknown,
        report,
    };
    outcome.validate().unwrap();
    assert!(serde_json::to_vec(&outcome).unwrap().len() > ADMISSION_OUTPUT_MAX_BYTES);
    let mut raw = serde_json::to_value(outcome).unwrap();
    raw.as_object_mut().unwrap().remove("call_completion");
    assert!(serde_json::from_value::<ProviderDispatchOutcome>(raw).is_err());
}

fn governed_source(binding: &AdmissionBinding) -> ProviderAdmissionSource {
    let origin = occurrence();
    let worker = worker();
    let admission = AdmissionSource {
        occurrence: AdmissionOccurrence {
            scope: origin.scope.clone(),
            workflow_id: origin.workflow_id.clone(),
            origin_activation_id: origin.activation_id.clone(),
            key: "capacity:0".into(),
        },
        source_id: "capacity-source".into(),
        start_id: "claim".into(),
        finish_id: "report".into(),
        generation: 1,
        binding_digest: binding.digest().unwrap(),
        owner: LeaseOwner {
            scope: origin.scope.clone(),
            task_id: "capacity-resumed".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        },
        worker: AdmissionWorkerIdentity {
            audience: worker.audience.clone(),
            issuer: worker.issuer.clone(),
            subject: worker.subject.clone(),
            credential_id: worker.credential_id.clone(),
        },
        accepted_at: 100,
        start_deadline_at: 1100,
    };
    ProviderAdmissionSource {
        admission,
        provider: ProviderDispatchSource {
            source_id: "provider-source".into(),
            claim_id: "claim".into(),
            report_id: "report".into(),
            ticket: binding.provider().unwrap().ticket.clone(),
            worker,
            accepted_at: 100,
            first_possible_dispatch_at: 100,
            reconciliation_deadline: 3_600_100,
        },
    }
}
#[test]
fn composed_sources_preserve_original_approval_without_reusing_current_owner() {
    let binding = governed_admission_binding();
    let source = governed_source(&binding);
    source.validate().unwrap();
    let AdmissionBinding::Provider(provider) = binding else {
        panic!("provider")
    };
    assert!(source.matches_binding(&provider).unwrap());
    assert_ne!(
        source.provider.ticket.occurrence.activation_id,
        source.admission.owner.task_id
    );
    for pointer in [
        "/provider/ticket/occurrence/activation_id",
        "/provider/worker/subject",
        "/provider/worker/audience/instance_id",
        "/provider/claim_id",
        "/provider/report_id",
    ] {
        let mut raw = serde_json::to_value(&source).unwrap();
        *raw.pointer_mut(pointer).unwrap() = json!("foreign");
        assert!(
            serde_json::from_value::<ProviderAdmissionSource>(raw)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}
#[test]
fn composed_reports_bind_completion_separately_and_replay_cannot_become_permission() {
    let binding = governed_admission_binding();
    let source = governed_source(&binding);
    let command = ProviderAdmissionReportCommand {
        source: source.clone(),
        outcome: ProviderDispatchOutcome {
            call_completion: ProviderCallCompletion::Unknown,
            report: report(),
        },
    };
    let receipt = ProviderAdmissionReportReceipt {
        source: source.clone(),
        call_completion: ProviderCallCompletion::Unknown,
        receipt: ProviderReportReceipt {
            ticket: source.provider.ticket.clone(),
            source_id: source.provider.source_id.clone(),
            source: ProviderSourceReference::Dispatch {
                claim_id: source.provider.claim_id.clone(),
            },
            report_id: source.provider.report_id.clone(),
            report_digest: command.outcome.report.digest().unwrap(),
            worker: source.provider.worker.clone(),
            accepted_at: 120,
            recorded_revision: 3,
            accepted_via: ProviderReportIngress::EvidenceOnly,
            already_accepted: false,
        },
    };
    assert!(receipt.matches(&command).unwrap());
    let mut changed = command.clone();
    changed.outcome.call_completion = ProviderCallCompletion::Completed;
    assert!(!receipt.matches(&changed).unwrap());
    let record = ProviderAdmissionRecord {
        ticket: source.provider.ticket.clone(),
        source: Some(source.clone()),
        report: Some(receipt),
    };
    let snapshot = AdmissionSnapshot {
        occurrence: source.admission.occurrence.clone(),
        binding: binding.clone(),
        state: AdmissionState::UnknownOccupied,
        revision: 3,
        created_at: 0,
        updated_at: 120,
        deadline_at: 5000,
        offer: None,
        source: Some(source.admission.clone()),
        finish: None,
        provider: Some(record.clone()),
    };
    snapshot.validate().unwrap();
    let mut wrong = snapshot.clone();
    wrong.state = AdmissionState::Finished;
    assert!(wrong.validate().is_err());
    let replay = ProviderAdmissionStartReceipt::Existing {
        audience: worker().audience,
        snapshot: Box::new(snapshot),
    };
    replay.validate().unwrap();
    assert_eq!(serde_json::to_value(replay).unwrap()["kind"], "existing");
    let wake = AdmissionWake {
        key: "capacity:0".into(),
        occurrence: source.admission.occurrence.clone(),
        waiting_activation_id: occurrence().activation_id,
        resumed_activation_id: source.admission.owner.task_id.clone(),
        resumed_revision: 1,
        binding,
        selected_at: 90,
        selection: AdmissionSelection::Offered {
            offer: AdmissionOffer {
                occurrence: source.admission.occurrence.clone(),
                generation: 1,
                policy_revision: 1,
                activation_id: source.admission.owner.task_id.clone(),
                expires_at: 1000,
                binding_digest: source.admission.binding_digest.clone(),
            },
        },
    };
    wake.validate_provider_record(&record).unwrap();
    let mut foreign_wake = wake.clone();
    foreign_wake.occurrence.origin_activation_id = "foreign-approval".into();
    if let AdmissionSelection::Offered { offer } = &mut foreign_wake.selection {
        offer.occurrence = foreign_wake.occurrence.clone();
    }
    assert!(foreign_wake.validate().is_err());
    let mut foreign = record.clone();
    foreign.source.as_mut().unwrap().admission.generation += 1;
    foreign.report = None;
    assert!(wake.validate_provider_record(&foreign).is_err());
}
