use super::*;
impl ApprovedEffectEvidence for Mock {
    fn claim_v2<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        command: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        Box::pin(async move { self.reply("claim_v2", command) })
    }
    fn confirm_v2<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        command: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        Box::pin(async move { self.reply("confirm_v2", command) })
    }
    fn observe<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        command: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        Box::pin(async move { self.reply("observe_v2", command) })
    }
}
impl EffectEvidenceReader for Mock {
    fn effect_inbox<'a>(
        &'a self,
        query: &'a EffectInboxQuery,
    ) -> ContractFuture<'a, EffectInboxPage> {
        Box::pin(async move { self.reply("effect_inbox", query) })
    }
    fn inspect_effect<'a>(
        &'a self,
        query: &'a EffectInspectQuery,
    ) -> ContractFuture<'a, EffectEvidenceDetail> {
        Box::pin(async move { self.reply("effect_inspect", query) })
    }
}

#[tokio::test]
async fn claim_and_confirm_validate_full_actor_binding_after_possible_commit() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(3, Ordering::SeqCst);
    let command = EffectClaimV2Command {
        owner: owner(),
        workflow_id: "workflow".into(),
        approval_key: "approval".into(),
        action: ApprovalAction {
            name: "transfer".into(),
            version: "1".into(),
            arguments: json!({"amount":1.0}),
        },
        lookup: "find_transfer".into(),
        claim_id: "claim".into(),
    };
    let claim = EffectClaimBinding {
        scope: command.owner.scope.clone(),
        workflow_id: command.workflow_id.clone(),
        activation_id: command.owner.task_id.clone(),
        attempt_id: command.owner.attempt_id.clone(),
        approval_key: command.approval_key.clone(),
        action: command.action.clone(),
        lookup: command.lookup.clone(),
        claim_id: command.claim_id.clone(),
        worker: identity(),
        claimed_at: 22,
        initial_permission: ApprovedEffectDisposition::Dispatch,
        recorded_revision: 2,
        already_accepted: false,
    };
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    f.mock.set(
        "claim_v2",
        Ok(EffectClaimV2Receipt::Dispatch {
            claim: claim.clone(),
        }),
    );
    assert!(matches!(
        EffectEvidenceClient::claim_v2(&client, &command)
            .await
            .unwrap(),
        EffectClaimV2Receipt::Dispatch { .. }
    ));
    let mut foreign = claim;
    foreign.worker.audience.authority_id = "other".into();
    f.mock.set(
        "claim_v2",
        Ok(EffectClaimV2Receipt::Dispatch { claim: foreign }),
    );
    assert!(matches!(
        EffectEvidenceClient::claim_v2(&client, &command).await,
        Err(ContractError::Unavailable(_))
    ));
    let record = LocalStepRecord {
        key: approved_effect_step_key("approval"),
        callable: "transfer".into(),
        input: json!({"v":1,"lookup":"find_transfer","action_json":serde_json::to_string(&command.action).unwrap()}),
        output: Value::Null,
    };
    let confirmation = EffectConfirmV2Command {
        owner: command.owner.clone(),
        workflow_id: command.workflow_id,
        approval_key: command.approval_key,
        lookup: command.lookup,
        record,
        claim_id: command.claim_id,
        observation_id: "observation".into(),
        reported_callback: EffectReportedCallback::Dispatch,
    };
    let receipt = EffectConfirmV2Receipt {
        scope: confirmation.owner.scope.clone(),
        workflow_id: confirmation.workflow_id.clone(),
        activation_id: confirmation.owner.task_id.clone(),
        attempt_id: confirmation.owner.attempt_id.clone(),
        approval_key: confirmation.approval_key.clone(),
        key: confirmation.record.key.clone(),
        already_accepted: false,
        claim_id: confirmation.claim_id.clone(),
        observation_id: confirmation.observation_id.clone(),
        worker: identity(),
        recorded_at: 23,
        recorded_revision: 3,
    };
    f.mock.set("confirm_v2", Ok(receipt.clone()));
    assert_eq!(
        EffectEvidenceClient::confirm_v2(&client, &confirmation)
            .await
            .unwrap()
            .recorded_revision,
        3
    );
    let mut wrong = receipt;
    wrong.observation_id = "other".into();
    f.mock.set("confirm_v2", Ok(wrong));
    assert!(matches!(
        EffectEvidenceClient::confirm_v2(&client, &confirmation).await,
        Err(ContractError::Unavailable(_))
    ));
    let calls = f.mock.calls.lock().unwrap();
    assert_eq!(
        calls.iter().filter(|(name, _)| *name == "claim_v2").count(),
        2
    );
    assert_eq!(
        calls
            .iter()
            .filter(|(name, _)| *name == "confirm_v2")
            .count(),
        2
    );
    drop(calls);
    assert!(f.auditor.observations.lock().unwrap().is_empty());
}
fn identity() -> EffectWorkerIdentity {
    EffectWorkerIdentity {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "audit".into(),
            scope: scope(),
        },
        issuer: "urn:ledgence:authority:authority".into(),
        subject: "worker".into(),
        credential_id: "credential".into(),
    }
}
pub(super) fn occurrence() -> EffectOccurrence {
    EffectOccurrence {
        scope: scope(),
        workflow_id: "workflow".into(),
        activation_id: "effect-activation".into(),
        approval_key: "approval".into(),
    }
}
fn detail() -> EffectEvidenceDetail {
    let mut approved = approval();
    approved.scope = scope();
    approved.workflow_id = "workflow".into();
    approved.key = "approval".into();
    approved.status = ApprovalStatus::Approved;
    approved.resumed_activation_id = Some("effect-activation".into());
    approved.decision = Some(ApprovalDecisionRecord {
        decision_id: "decision".into(),
        decision: ApprovalDecision::Approve,
        reviewer: "operator".into(),
        reason: None,
        decided_at: 20,
    });
    let program = task(Value::Null).descriptor;
    EffectEvidenceDetail {
        current_assessment: None,
        summary: EffectEvidenceSummary {
            operator_resolution: None,
            occurrence: occurrence(),
            program: program.program.clone(),
            callable: approved.action.name.clone(),
            action_version: approved.action.version.clone(),
            dispatch_provenance: EffectDispatchProvenance::VerifiedClaim {
                claim_id: "claim".into(),
            },
            journal_state: EffectJournalState::Unconfirmed,
            has_discrepancy: false,
            entry_count: 1,
            effect_revision: 2,
            created_at: 21,
            updated_at: 22,
        },
        binding: EffectEvidenceBinding {
            occurrence: occurrence(),
            approval: approved,
            program,
            key: approved_effect_step_key("approval"),
            lookup: Some("find_transfer".into()),
            approval_acceptance_event_id: None,
        },
        entries: vec![EffectEvidenceEntry::Observation {
            sequence: 1,
            recorded_at: 22,
            recorded_revision: 2,
            observation_id: "observation".into(),
            source: EffectObservationSource::VerifiedWorkerClaim {
                claim_id: "claim".into(),
                worker: identity(),
                reported_callback: EffectReportedCallback::Lookup,
                ingress: EffectObservationIngress::LateObserve,
            },
            output: json!(CANARY),
        }],
        next_cursor: None,
    }
}

#[tokio::test]
async fn metadata_inbox_does_not_audit_but_new_sensitive_routes_never_use_legacy_trust() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(3, Ordering::SeqCst);
    let query = EffectInboxQuery {
        scope: scope(),
        filter: EffectInboxFilter::All,
        workflow_id: None,
        cursor: None,
        limit: 10,
    };
    f.mock.set(
        "effect_inbox",
        Ok(EffectInboxPage {
            items: vec![detail().summary],
            next_cursor: None,
        }),
    );
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("reader")
        .unwrap();
    let page = client.effect_inbox(&query).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(!serde_json::to_string(&page).unwrap().contains(CANARY));
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    let mock = Arc::new(Mock::default());
    mock.evidence_capabilities.store(3, Ordering::SeqCst);
    mock.set(
        "effect_inbox",
        Ok(EffectInboxPage {
            items: vec![],
            next_cursor: None,
        }),
    );
    let running = start(server::router_with_workflows(
        mock.clone(),
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(ledgence_worker_api::NoopTraceBridge),
    ))
    .await;
    let client = HttpTaskService::new(&running.url).unwrap();
    assert!(client.effect_inbox(&query).await.unwrap().items.is_empty());
    let inspect = EffectInspectQuery {
        occurrence: occurrence(),
        cursor: None,
        limit: 4,
    };
    assert!(matches!(
        client.inspect_effect(&inspect).await,
        Err(ContractError::Unavailable(_))
    ));
    let command = EffectObservationCommand {
        occurrence: occurrence(),
        claim_id: "claim".into(),
        observation_id: "observation".into(),
        reported_callback: EffectReportedCallback::Lookup,
        output: Value::Null,
    };
    assert_eq!(
        EffectEvidenceClient::observe(&client, &command)
            .await
            .unwrap_err(),
        ContractError::Unauthenticated
    );
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        1,
        "only metadata inbox dispatched"
    );
}

#[tokio::test]
async fn console_projection_preserves_large_revisions_and_rejects_oversized_evidence_before_audit()
{
    let f = setup().await;
    f.mock.evidence_capabilities.store(3, Ordering::SeqCst);
    let sequence = (1u64 << 53) + 1;
    let mut query = EffectInspectQuery {
        occurrence: occurrence(),
        cursor: None,
        limit: 4,
    };
    query.cursor = Some(query.next_cursor(sequence - 1).unwrap());
    let mut reply = detail();
    reply.summary.entry_count = sequence;
    reply.summary.effect_revision = sequence + 1;
    if let EffectEvidenceEntry::Observation {
        sequence: s,
        recorded_revision,
        ..
    } = &mut reply.entries[0]
    {
        *s = sequence;
        *recorded_revision = sequence + 1;
    }
    reply.validate_for(&query).unwrap();
    f.mock.set("effect_inspect", Ok(reply));
    let body = json!({"workflow_id":"workflow","activation_id":"effect-activation","approval_key":"approval","cursor":query.cursor,"limit":4});
    let (status, reply) = request(
        &f,
        "/v1/console/workflows/approved-effects/inspect",
        Some(body),
        "operator",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(reply["summary"]["entry_count"], sequence.to_string());
    assert_eq!(reply["entries"][0]["sequence"], sequence.to_string());
    assert_eq!(
        reply["entries"][0]["recorded_revision"],
        (sequence + 1).to_string()
    );
    let mut too_large = detail();
    if let EffectEvidenceEntry::Observation { output, .. } = &mut too_large.entries[0] {
        *output = json!("x".repeat(WORKFLOW_LOCAL_RECORD_MAX_BYTES));
    }
    f.mock.set("effect_inspect", Ok(too_large));
    let before = f.auditor.observations.lock().unwrap().len();
    let query = EffectInspectQuery {
        occurrence: occurrence(),
        cursor: None,
        limit: 4,
    };
    let (status, _) = request(
        &f,
        "/v1/workflows/approved-effects/inspect",
        Some(serde_json::to_value(query).unwrap()),
        "operator",
    )
    .await;
    assert_eq!(status, 503);
    assert_eq!(f.auditor.observations.lock().unwrap().len(), before);
}
async fn access(f: &Fixture, route: &str, token: &str) -> (u16, Value) {
    let response = reqwest::Client::new()
        .get(format!("{}{route}", f.running.url))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    (
        response.status().as_u16(),
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap(),
    )
}
#[tokio::test]
async fn discovery_advertises_only_complete_verified_worker_composition() {
    let f = setup().await;
    for bits in 0..4 {
        f.mock.evidence_capabilities.store(bits, Ordering::SeqCst);
        let (status, value) = access(&f, "/v1/access", "worker").await;
        assert_eq!(status, 200);
        assert_eq!(value.get("runtime_capabilities").is_some(), bits == 3);
        assert_eq!(value.get("authority_id").is_some(), bits == 3);
        if bits == 3 {
            assert_eq!(
                value["runtime_capabilities"],
                json!({"approved_effect_evidence_v2":true})
            );
        }
        let (status, value) = access(&f, "/v1/access", "operator").await;
        assert_eq!(status, 200);
        assert!(value.get("runtime_capabilities").is_none());
        let (status, value) = access(&f, "/v1/console/access", "operator").await;
        assert_eq!(status, 200);
        assert!(value.get("authority_id").is_none());
    }
    assert!(f.mock.calls.lock().unwrap().is_empty());
    drop(f);
}
#[tokio::test]
async fn both_inspection_aliases_validate_identity_and_audit_before_payload_release() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(3, Ordering::SeqCst);
    let query = EffectInspectQuery {
        occurrence: occurrence(),
        cursor: None,
        limit: 4,
    };
    let value = detail();
    value.validate_for(&query).unwrap();
    for console in [false, true] {
        let path = if console {
            "/v1/console/workflows/approved-effects/inspect"
        } else {
            "/v1/workflows/approved-effects/inspect"
        };
        let body = if console {
            json!({"workflow_id":"workflow","activation_id":"effect-activation","approval_key":"approval","cursor":null,"limit":4})
        } else {
            serde_json::to_value(&query).unwrap()
        };
        f.mock.set("effect_inspect", Ok(detail()));
        let before = f.mock.calls.lock().unwrap().len();
        let (status, denied) = request(&f, path, Some(body.clone()), "reader").await;
        assert_eq!(status, 403);
        assert!(!denied.to_string().contains(CANARY));
        assert_eq!(f.mock.calls.lock().unwrap().len(), before);
        for mode in [1, 2, 0] {
            f.auditor.mode.store(mode, Ordering::SeqCst);
            let (status, reply) = request(&f, path, Some(body.clone()), "operator").await;
            if mode == 0 {
                assert_eq!(status, 200);
                assert!(reply.to_string().contains(CANARY));
                assert_eq!(
                    reply["summary"]["effect_revision"],
                    if console { json!("2") } else { json!(2) }
                );
                assert_eq!(
                    reply["entries"][0]["sequence"],
                    if console { json!("1") } else { json!(1) }
                );
                assert!(reply["entries"][0].get("owner").is_none());
            } else {
                assert_eq!(status, 503);
                assert!(!reply.to_string().contains(CANARY));
            }
        }
        let before = f.auditor.observations.lock().unwrap().len();
        let mut wrong = detail();
        wrong.summary.occurrence.workflow_id = "other".into();
        f.mock.set("effect_inspect", Ok(wrong));
        let (status, reply) = request(&f, path, Some(body), "operator").await;
        assert_eq!(status, 503);
        assert!(!reply.to_string().contains(CANARY));
        assert_eq!(f.auditor.observations.lock().unwrap().len(), before);
    }
    let observations = f.auditor.observations.lock().unwrap();
    assert!(observations.iter().all(|o|o.read==AuditReadKind::EffectInspect&&matches!(&o.resource,AuditResource::Effect{activation_id,..} if activation_id=="effect-activation")));
    drop(observations);
    drop(f);
}
#[tokio::test]
async fn standalone_observation_checks_worker_scope_and_postcommit_reply_without_journal_ack() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(3, Ordering::SeqCst);
    let command = EffectObservationCommand {
        occurrence: occurrence(),
        claim_id: "claim".into(),
        observation_id: "observation".into(),
        reported_callback: EffectReportedCallback::Lookup,
        output: Value::Null,
    };
    let receipt = EffectObservationReceipt {
        occurrence: occurrence(),
        claim_id: "claim".into(),
        observation_id: "observation".into(),
        worker: identity(),
        recorded_at: 22,
        recorded_revision: 3,
        already_accepted: false,
        disposition: EffectObservationDisposition::EvidenceOnly,
    };
    f.mock.set("observe_v2", Ok(receipt.clone()));
    let path = "/v1/workflows/approved-effects/v2/observe";
    let (status, _) = request(
        &f,
        path,
        Some(serde_json::to_value(&command).unwrap()),
        "operator",
    )
    .await;
    assert_eq!(status, 403);
    assert!(f.mock.calls.lock().unwrap().is_empty());
    let mut wrong = command.clone();
    wrong.occurrence.scope.namespace = "other".into();
    let (status, _) = request(
        &f,
        path,
        Some(serde_json::to_value(wrong).unwrap()),
        "worker",
    )
    .await;
    assert_eq!(status, 404);
    assert!(f.mock.calls.lock().unwrap().is_empty());
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    let reply = EffectEvidenceClient::observe(&client, &command)
        .await
        .unwrap();
    assert_eq!(
        reply.disposition,
        EffectObservationDisposition::EvidenceOnly
    );
    assert!(
        !serde_json::to_value(reply)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("committed")
    );
    let mut wrong = receipt;
    wrong.worker.subject = "other".into();
    f.mock.set("observe_v2", Ok(wrong));
    let (status, _) = request(
        &f,
        path,
        Some(serde_json::to_value(command).unwrap()),
        "worker",
    )
    .await;
    assert_eq!(status, 503);
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    assert!(
        f.mock
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(operation, _)| *operation == "observe_v2")
    );
    drop(f);
}
