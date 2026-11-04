use super::*;
use serde_json::{Value, json};

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "demo".into(),
    }
}
fn owner() -> LeaseOwner {
    LeaseOwner {
        scope: scope(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        lease_id: "lease".into(),
        generation: 1,
        worker_session_id: "session".into(),
        consumer_id: 0,
    }
}
fn program() -> ProgramDescriptor {
    serde_json::from_value(json!({"program":{"id":"hello","version":"1.0.0"},"digest":format!("sha256:{}", "a".repeat(64)),"size":100})).unwrap()
}
fn principal(subject: &str, credential: &str) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: "issuer".into(),
            subject: subject.into(),
        },
        AccessAudience {
            instance_id: "instance".into(),
            authority_id: "authority".into(),
            scope: scope(),
        },
        credential.into(),
        PrincipalClass::Worker,
        60_000,
        vec![AccessCapability::WorkerExecute],
    )
    .unwrap()
}
fn binding() -> TaskAdmissionBinding {
    TaskAdmissionBinding {
        occurrence: (&owner()).into(),
        resource: "model.requests".into(),
        program: program(),
    }
}
fn command() -> TaskAdmissionStartCommand {
    TaskAdmissionStartCommand {
        owner: owner(),
        offer_generation: 1,
        start_id: "start".into(),
    }
}
fn source() -> TaskAdmissionSource {
    TaskAdmissionSource {
        owner: owner(),
        offer_generation: 1,
        resource: binding().resource,
        binding_digest: binding().digest().unwrap(),
        start_id: "start".into(),
        source_id: "source".into(),
        worker: AdmissionWorkerIdentity::from_principal(&principal("worker", "credential"))
            .unwrap(),
        accepted_at: 1_000,
        start_deadline_at: 2_000,
    }
}
fn snapshot() -> TaskAdmissionSnapshot {
    TaskAdmissionSnapshot {
        occurrence: (&owner()).into(),
        resource: binding().resource,
        program: program(),
        state: TaskAdmissionState::PossiblyDispatched,
        revision: 3,
        created_at: 0,
        updated_at: 1_000,
        offer: Some(TaskAdmissionOffer {
            generation: 1,
            policy_revision: 1,
            expires_at: 300_000,
        }),
        owner: Some(owner()),
        source: Some(source()),
        close: None,
    }
}
fn close() -> TaskAdmissionCloseReceipt {
    TaskAdmissionCloseReceipt {
        owner: owner(),
        offer_generation: 1,
        start_id: "start".into(),
        operation_id: "close".into(),
        outcome: TaskAdmissionCloseOutcome::Quiescent,
        source: Some(source()),
        disposition: TaskAdmissionCloseDisposition::Released,
        accepted_at: 2_000,
        already_accepted: false,
    }
}
fn status() -> Value {
    json!({"scope":scope(),"task_id":"task","run_id":"run","queue":"queue","correlation_key":null,"state":"queued","attempt_count":1001,"current_attempt_id":null,"latest_attempt_id":"attempt","submitted_at":0,"available_at":1,"terminal_at":null,"cancel_requested_at":null,"admission":{"resource":"model.requests"},"admission_deferrals":1000})
}

#[test]
fn submission_requirement_is_strict_immutable_and_omission_preserves_wire() {
    let old = br#"{"tenant_id":"acme","namespace":"demo","queue":"q","program":{"id":"hello","version":"1.0.0"},"data":null}"#;
    let input = SubmitTask::decode(old).unwrap();
    let baseline = input.canonical_bytes().unwrap();
    assert!(
        !String::from_utf8(baseline.clone())
            .unwrap()
            .contains("admission")
    );
    let mut configured = serde_json::to_value(&input).unwrap();
    configured["admission"] = json!({"resource":"model.requests"});
    let governed = SubmitTask::decode(&serde_json::to_vec(&configured).unwrap()).unwrap();
    assert!(!governed.semantically_matches(&input).unwrap());
    let mut other = governed.clone();
    other.admission.as_mut().unwrap().resource = "other".into();
    assert!(!governed.semantically_matches(&other).unwrap());
    for invalid in [
        Value::Null,
        json!({}),
        json!({"resource":""}),
        json!({"resource":"a b"}),
        json!({"resource":"valid","units":2}),
    ] {
        configured["admission"] = invalid;
        assert!(SubmitTask::decode(&serde_json::to_vec(&configured).unwrap()).is_err());
    }
    assert_eq!(
        SubmitTask::decode(old).unwrap().canonical_bytes().unwrap(),
        baseline
    );
}

#[test]
fn generations_preserve_cloudevents_integer_range_without_spending_retry_allowance() {
    let mut occurrence: TaskAdmissionOccurrence = (&owner()).into();
    for generation in [1, 1001, MAX_TASK_ATTEMPT_GENERATION] {
        occurrence.generation = generation;
        occurrence.validate().unwrap();
        DispatchRef {
            scope: scope(),
            queue: "queue".into(),
            task_id: "task".into(),
            generation,
        }
        .validate()
        .unwrap();
    }
    for generation in [0, MAX_TASK_ATTEMPT_GENERATION + 1, u32::MAX] {
        occurrence.generation = generation;
        assert!(occurrence.validate().is_err());
    }
    assert_eq!(task_retry_attempts(1001, 1000, true).unwrap(), 1);
    assert!(task_retry_attempts(1, 2, true).is_err());
    assert!(task_retry_attempts(1, 1, false).is_err());
    assert!(task_retry_attempts(u32::MAX, u32::MAX - 1, true).is_err());
}

#[test]
fn governed_status_counters_do_not_loosen_ordinary_task_validation() {
    let good: TaskStatus = serde_json::from_value(status()).unwrap();
    good.validate().unwrap();
    for mutation in [
        |v: &mut Value| {
            v.as_object_mut().unwrap().remove("admission");
        },
        |v: &mut Value| {
            v["admission_deferrals"] = json!(1002);
        },
        |v: &mut Value| {
            v["admission_deferrals"] = json!(0);
        },
        |v: &mut Value| {
            v["workflow_id"] = json!("workflow");
        },
        |v: &mut Value| {
            v["attempt_count"] = json!(u32::MAX);
            v["admission_deferrals"] = json!(u32::MAX - 1);
        },
    ] {
        let mut bad = status();
        mutation(&mut bad);
        assert!(
            serde_json::from_value::<TaskStatus>(bad)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut ordinary = status();
    ordinary.as_object_mut().unwrap().remove("admission");
    ordinary
        .as_object_mut()
        .unwrap()
        .remove("admission_deferrals");
    ordinary["attempt_count"] = json!(1);
    let ordinary: TaskStatus = serde_json::from_value(ordinary).unwrap();
    ordinary.validate().unwrap();
    let wire = serde_json::to_value(ordinary).unwrap();
    assert!(wire.get("admission").is_none());
    assert!(wire.get("admission_deferrals").is_none());
}

#[test]
fn grant_matches_original_worker_and_binding_not_later_actor_or_prepared_program() {
    let receipt = TaskAdmissionStartReceipt::Granted {
        source: Box::new(source()),
        authority: Authority {
            owner: owner(),
            expires_at: 31_000,
            remaining_ms: 30_000,
            execution_remaining_ms: 300_000,
            renew_sequence: 0,
            cancel_requested: false,
            dispatch_allowed: true,
        },
    };
    receipt
        .validate_for(&principal("worker", "rotated"), &command())
        .unwrap();
    assert!(
        receipt
            .validate_for(&principal("other", "credential"), &command())
            .is_err()
    );
    assert!(source().matches_binding(&binding()).unwrap());
    let mut changed = binding();
    changed.program.digest = Digest(format!("sha256:{}", "b".repeat(64)));
    assert!(!source().matches_binding(&changed).unwrap());
    let mut changed = command();
    changed.owner.lease_id = "new-lease".into();
    assert!(!receipt.matches(&changed).unwrap());
    let mut changed = command();
    changed.offer_generation = 2;
    assert!(!receipt.matches(&changed).unwrap());
}

#[test]
fn grant_requires_live_one_shot_invocation_authority() {
    let valid = TaskAdmissionStartReceipt::Granted {
        source: Box::new(source()),
        authority: Authority {
            owner: owner(),
            expires_at: 31_000,
            remaining_ms: 30_000,
            execution_remaining_ms: 300_000,
            renew_sequence: 0,
            cancel_requested: false,
            dispatch_allowed: true,
        },
    };
    for mutate in [
        |a: &mut Authority| a.dispatch_allowed = false,
        |a: &mut Authority| a.cancel_requested = true,
        |a: &mut Authority| a.remaining_ms = 0,
        |a: &mut Authority| a.execution_remaining_ms = 0,
        |a: &mut Authority| a.expires_at = 1_500,
        |a: &mut Authority| a.owner.consumer_id = 1,
    ] {
        let mut bad = valid.clone();
        let TaskAdmissionStartReceipt::Granted { authority, .. } = &mut bad else {
            unreachable!()
        };
        mutate(authority);
        assert!(bad.validate().is_err());
    }
    let replay = TaskAdmissionStartReceipt::Existing {
        snapshot: Box::new(snapshot()),
    };
    replay
        .validate_for(&principal("worker", "rotated"), &command())
        .unwrap();
    let mut different_start = command();
    different_start.start_id = "other-start".into();
    assert!(!replay.matches(&different_start).unwrap());
    let mut different_offer = command();
    different_offer.offer_generation += 1;
    assert!(!replay.matches(&different_offer).unwrap());
    let wire = serde_json::to_value(replay).unwrap();
    assert!(wire.get("authority").is_none());
}

#[test]
fn snapshot_binds_retained_source_owner_offer_and_program() {
    snapshot().validate().unwrap();
    for mutate in [
        |s: &mut TaskAdmissionSnapshot| s.occurrence.generation = 2,
        |s: &mut TaskAdmissionSnapshot| s.owner.as_mut().unwrap().lease_id = "other".into(),
        |s: &mut TaskAdmissionSnapshot| s.offer.as_mut().unwrap().generation = 2,
        |s: &mut TaskAdmissionSnapshot| s.program.program.version = "2.0.0".into(),
        |s: &mut TaskAdmissionSnapshot| s.state = TaskAdmissionState::Preparing,
        |s: &mut TaskAdmissionSnapshot| s.updated_at = 999,
    ] {
        let mut bad = snapshot();
        mutate(&mut bad);
        assert!(bad.validate().is_err());
    }
    let mut waiting = snapshot();
    waiting.state = TaskAdmissionState::Waiting;
    waiting.source = None;
    waiting.owner = None;
    waiting.offer = None;
    waiting.validate().unwrap();
}

#[test]
fn retained_state_requires_its_original_capacity_evidence() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/task-admission-v1.json")).unwrap();
    let rows = &fixture["snapshots"];
    for (name, field, invalid_value) in [
        ("waiting", "/offer", rows["offered"]["offer"].clone()),
        ("finished", "/close", json!(null)),
        ("finished", "/close/disposition", json!("already_released")),
        ("finished", "/close/already_accepted", json!(true)),
        ("released", "/close", rows["finished"]["close"].clone()),
        ("deferred", "/offer", json!(null)),
        ("deferred", "/owner", json!(null)),
    ] {
        let mut value = rows[name].clone();
        *value.pointer_mut(field).unwrap() = invalid_value;
        let observed: TaskAdmissionSnapshot = serde_json::from_value(value).unwrap();
        assert!(
            observed.validate().is_err(),
            "invalid evidence accepted for {name}"
        );
    }
}

#[test]
fn close_cannot_refund_started_source_or_release_another_lease() {
    close().validate().unwrap();
    let mut deferred = close();
    deferred.outcome = TaskAdmissionCloseOutcome::NotInvoked;
    deferred.disposition = TaskAdmissionCloseDisposition::Deferred;
    assert!(deferred.validate().is_err());
    deferred.source = None;
    deferred.validate().unwrap();
    let mut bad = close();
    bad.owner.lease_id = "other".into();
    assert!(bad.validate().is_err());
    let mut bad = close();
    bad.accepted_at = 999;
    assert!(bad.validate().is_err());
    let mut released = snapshot();
    released.state = TaskAdmissionState::Finished;
    released.updated_at = 2_000;
    released.close = Some(close());
    released.validate().unwrap();
    let mut bad = released;
    bad.close.as_mut().unwrap().offer_generation = 2;
    assert!(bad.validate().is_err());
}

#[test]
fn release_receipt_binds_source_and_optimistic_revision() {
    let mut released = snapshot();
    released.state = TaskAdmissionState::AdministrativelyReleased;
    released.revision = 4;
    let receipt = TaskAdmissionReleaseReceipt {
        snapshot: released,
        already_accepted: false,
    };
    receipt.validate().unwrap();
    let command = TaskAdmissionReleaseCommand {
        occurrence: (&owner()).into(),
        source_id: "source".into(),
        expected_revision: 3,
        request_id: "release".into(),
        reason: "Verified worker stopped".into(),
    };
    command.validate().unwrap();
    assert!(receipt.matches(&command));
    let mut other = command.clone();
    other.source_id = "new-source".into();
    assert!(!receipt.matches(&other));
    other = command;
    other.expected_revision = 4;
    assert!(!receipt.matches(&other));
}

#[test]
fn task_admission_audit_scope_is_bound_to_task_not_fabricated_workflow() {
    let occurrence: TaskAdmissionOccurrence = (&owner()).into();
    let operation = AuditOperation::TaskAdmissionReleased {
        occurrence: occurrence.clone(),
        source_id: "source".into(),
        request_id: "release".into(),
        revision: 2,
        reason: "Verified stopped".into(),
    };
    operation.validate().unwrap();
    assert_eq!(operation.target(), (AuditResourceKind::Task, "task"));
    let resource = AuditResource::TaskAdmission { occurrence };
    resource.validate().unwrap();
    assert_eq!(resource.target(), (AuditResourceKind::Task, "task"));
}

#[test]
fn required_nullable_ledger_fields_and_unknown_command_fields_are_strict() {
    let valid = serde_json::to_value(snapshot()).unwrap();
    for key in ["offer", "owner", "source", "close"] {
        let mut bad = valid.clone();
        bad.as_object_mut().unwrap().remove(key);
        assert!(serde_json::from_value::<TaskAdmissionSnapshot>(bad).is_err());
    }
    let mut bad = serde_json::to_value(command()).unwrap();
    bad["permission"] = json!(true);
    assert!(serde_json::from_value::<TaskAdmissionStartCommand>(bad).is_err());
}

#[test]
fn canonical_task_admission_fixture_tracks_rust_wire_contract() {
    let mut waiting = snapshot();
    waiting.state = TaskAdmissionState::Waiting;
    waiting.revision = 1;
    waiting.updated_at = 0;
    waiting.offer = None;
    waiting.owner = None;
    waiting.source = None;
    let mut offered = waiting.clone();
    offered.state = TaskAdmissionState::Offered;
    offered.revision = 2;
    offered.updated_at = 100;
    offered.offer = Some(TaskAdmissionOffer {
        generation: 1,
        policy_revision: 1,
        expires_at: 30_100,
    });
    let mut preparing = offered.clone();
    preparing.state = TaskAdmissionState::Preparing;
    preparing.owner = Some(owner());
    preparing.offer.as_mut().unwrap().expires_at = 300_100;
    preparing.updated_at = 200;
    preparing.revision = 3;
    let mut started = snapshot();
    started.revision = 4;
    let mut unknown = started.clone();
    unknown.state = TaskAdmissionState::UnknownOccupied;
    unknown.updated_at = 2_000;
    unknown.revision = 5;
    let mut finished = started.clone();
    finished.state = TaskAdmissionState::Finished;
    finished.close = Some(close());
    finished.updated_at = 2_000;
    finished.revision = 5;
    let mut deferred = preparing.clone();
    deferred.state = TaskAdmissionState::ClosedBeforeStart;
    deferred.updated_at = 2_000;
    deferred.revision = 4;
    let mut unused = close();
    unused.source = None;
    unused.outcome = TaskAdmissionCloseOutcome::NotInvoked;
    unused.disposition = TaskAdmissionCloseDisposition::Deferred;
    deferred.close = Some(unused.clone());
    let mut released = unknown.clone();
    released.state = TaskAdmissionState::AdministrativelyReleased;
    released.updated_at = 3_000;
    released.revision = 6;
    let cases = [
        ("waiting", waiting),
        ("offered", offered),
        ("preparing", preparing),
        ("started", started),
        ("unknown", unknown),
        ("finished", finished),
        ("deferred", deferred),
        ("released", released),
    ];
    let mut snapshots = serde_json::Map::new();
    for (name, snapshot) in cases {
        snapshot.validate().unwrap();
        snapshots.insert(name.into(), serde_json::to_value(snapshot).unwrap());
    }
    let grant = TaskAdmissionStartReceipt::Granted {
        source: Box::new(source()),
        authority: Authority {
            owner: owner(),
            expires_at: 31_000,
            remaining_ms: 30_000,
            execution_remaining_ms: 300_000,
            renew_sequence: 0,
            cancel_requested: false,
            dispatch_allowed: true,
        },
    };
    grant.validate().unwrap();
    let fixture = json!({"v":1,"source":source(),"start_command":command(),"start_granted":grant,"close_released":close(),"close_deferred":unused,"snapshots":snapshots});
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/task-admission-v1.json");
    if std::env::var_os("LEDGENCE_REFRESH_TASK_ADMISSION_FIXTURE").is_some() {
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&fixture).unwrap()),
        )
        .unwrap();
    }
    let committed: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        committed, fixture,
        "refresh the shared fixture only with an intentional contract change"
    );
}

#[test]
fn generation_exhaustion_is_a_system_outcome_without_invented_invocation_or_cleanup() {
    let mut task = status();
    task["state"] = json!("failed");
    task["terminal_at"] = json!(2);
    task["attempt_count"] = json!(MAX_TASK_ATTEMPT_GENERATION);
    task["admission_deferrals"] = json!(MAX_TASK_ATTEMPT_GENERATION - 1);
    let value = json!({"task":task,"outcome":{"kind":"failed","attempt_id":"attempt","quiescence":"unconfirmed","execution_may_have_started":false,"failure":{"kind":"admission_generation_exhausted"}}});
    let outcome: TaskResult = serde_json::from_value(value.clone()).unwrap();
    outcome.validate().unwrap();
    for mutate in [
        |v: &mut Value| v["outcome"]["execution_may_have_started"] = json!(true),
        |v: &mut Value| {
            v["task"]["attempt_count"] = json!(10);
            v["task"]["admission_deferrals"] = json!(9);
        },
        |v: &mut Value| {
            v["task"].as_object_mut().unwrap().remove("admission");
        },
    ] {
        let mut bad = value.clone();
        mutate(&mut bad);
        assert!(
            serde_json::from_value::<TaskResult>(bad)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}

#[test]
fn maintenance_distinguishes_task_offers_from_workflow_resumes() {
    let legacy = json!({"examined":1,"offered":1,"expired":0,"resumed":1});
    let mut progress: AdmissionMaintenanceProgress =
        serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(progress.task_offered, 0);
    assert_eq!(serde_json::to_value(progress).unwrap(), legacy);
    progress.offered = 0;
    progress.resumed = 0;
    progress.task_offered = 1;
    assert_eq!(
        serde_json::to_value(progress).unwrap(),
        json!({"examined":1,"offered":0,"task_offered":1,"expired":0,"resumed":0})
    );
}
