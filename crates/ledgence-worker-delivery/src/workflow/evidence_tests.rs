use super::*;
fn worker() -> EffectWorkerIdentity {
    EffectWorkerIdentity {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: owner().scope,
        },
        issuer: "urn:worker".into(),
        subject: "worker-one".into(),
        credential_id: "credential".into(),
    }
}
fn fixture() -> (
    WorkflowOperations,
    mpsc::UnboundedReceiver<EffectClaimV2>,
    mpsc::UnboundedReceiver<EffectConfirmV2>,
) {
    let (mut service, _, _) = super::fixture();
    let (claims, requests) = mpsc::unbounded_channel();
    let (confirms, confirmations) = mpsc::unbounded_channel();
    let mock = Arc::get_mut(&mut service).unwrap();
    mock.claims_v2 = Some(claims);
    mock.confirms_v2 = Some(confirms);
    let mut handler = journal(service);
    let worker = worker();
    handler.effect_access = Some(Arc::new(EffectAccess {
        audience: worker.audience,
        identity: PrincipalIdentity {
            issuer: worker.issuer,
            subject: worker.subject,
        },
    }));
    (handler, requests, confirmations)
}
fn claim() -> RuntimeRequest {
    RuntimeRequest {
        id: 3,
        operation: "approved_effect.claim.v2".into(),
        payload: json!({
            "approval_key":"approve","claim_id":"claim-one","lookup":"find_transfer",
            "action":{"name":"transfer","version":"1","arguments":{"amount":1.0}}
        }),
    }
}
fn binding(command: &EffectClaimV2Command) -> EffectClaimBinding {
    EffectClaimBinding {
        budget_grant: None,
        scope: command.owner.scope.clone(),
        workflow_id: command.workflow_id.clone(),
        activation_id: command.owner.task_id.clone(),
        attempt_id: command.owner.attempt_id.clone(),
        approval_key: command.approval_key.clone(),
        action: command.action.clone(),
        lookup: command.lookup.clone(),
        claim_id: command.claim_id.clone(),
        worker: worker(),
        claimed_at: 1,
        initial_permission: ApprovedEffectDisposition::Dispatch,
        recorded_revision: 2,
        already_accepted: false,
    }
}
fn record() -> LocalStepRecord {
    LocalStepRecord {
        key: approved_effect_step_key("approve"),
        callable: "transfer".into(),
        input: json!({"v":1,"lookup":"find_transfer","action_json":claim().payload["action"].to_string()}),
        output: Value::Null,
    }
}

#[tokio::test]
async fn unnegotiated_or_forged_runtime_authority_is_rejected_without_a_request() {
    let (mut handler, mut requests, _) = fixture();
    for field in [
        "owner",
        "workflow_id",
        "activation_id",
        "audience",
        "worker",
    ] {
        let mut command = claim();
        command.payload[field] = json!("forged");
        assert_eq!(
            handler
                .handle(command, RunControl::new(Duration::from_secs(1)))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Protocol
        );
    }
    handler.effect_access = None;
    assert!(!handler.capabilities().approved_effect_evidence_v2);
    assert_eq!(
        handler
            .handle(claim(), RunControl::new(Duration::from_secs(1)))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    assert!(requests.try_recv().is_err());
}
#[tokio::test]
async fn uncertain_or_unsupported_v2_grant_never_retries_or_falls_back() {
    for error in [
        ContractError::Unavailable("lost receipt".into()),
        ContractError::NotFound,
    ] {
        let (handler, mut requests, _) = fixture();
        let exchange = handler.handle(claim(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (_, reply) = requests.recv().await.unwrap();
            reply.send(Err(error)).unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        assert!(result.is_err());
        assert!(requests.try_recv().is_err());
    }
}
#[tokio::test]
async fn every_claim_receipt_is_bound_to_command_and_negotiated_stable_identity() {
    for field in [
        "authority",
        "instance",
        "subject",
        "issuer",
        "attempt",
        "claim",
        "action",
        "credential",
        "none",
    ] {
        let (handler, mut requests, _) = fixture();
        assert!(handler.capabilities().approved_effect_evidence_v2);
        let exchange = handler.handle(claim(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (command, reply) = requests.recv().await.unwrap();
            let mut claim = binding(&command);
            match field {
                "authority" => claim.worker.audience.authority_id = "other".into(),
                "instance" => claim.worker.audience.instance_id = "other".into(),
                "subject" => claim.worker.subject = "other".into(),
                "issuer" => claim.worker.issuer = "other".into(),
                "attempt" => claim.attempt_id = "other".into(),
                "claim" => claim.claim_id = "other".into(),
                "action" => claim.action.arguments = json!({"amount":2}),
                "credential" => claim.worker.credential_id = "rotated-original".into(),
                _ => {}
            }
            reply
                .send(Ok(EffectClaimV2Receipt::Dispatch { claim }))
                .unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        if ["credential", "none"].contains(&field) {
            let value = result.unwrap().result;
            assert_eq!(value["kind"], "dispatch");
            assert!(value["claim"].get("worker").is_none());
            assert!(value["claim"].get("attempt_id").is_none());
        } else {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol, "{field}");
        }
    }
}
#[tokio::test]
async fn replay_preserves_original_record_and_rejects_foreign_audience_or_key() {
    for field in ["authority", "key", "none"] {
        let (handler, mut requests, mut confirmations) = fixture();
        let exchange = handler.handle(claim(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (command, reply) = requests.recv().await.unwrap();
            let mut audience = worker().audience;
            let mut record = record();
            if field == "authority" {
                audience.authority_id = "other".into();
            }
            if field == "key" {
                record.key = approved_effect_step_key("other");
            }
            reply
                .send(Ok(EffectClaimV2Receipt::Replay {
                    audience,
                    occurrence: command.occurrence(),
                    record,
                }))
                .unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        if field == "none" {
            let value = result.unwrap().result;
            assert_eq!(value["kind"], "replay");
            assert_eq!(value["record"], serde_json::to_value(record()).unwrap());
            assert!(value.get("audience").is_none());
        } else {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol);
        }
        assert!(confirmations.try_recv().is_err());
    }
}
#[tokio::test]
async fn confirmation_reconciles_identical_evidence_and_checks_source_before_acknowledging() {
    for foreign in [false, true] {
        let (handler, _, mut confirmations) = fixture();
        let request = RuntimeRequest {
            id: 4,
            operation: "approved_effect.confirm.v2".into(),
            payload: json!({
                "approval_key":"approve","lookup":"find_transfer","record":record(),"claim_id":"claim-one",
                "observation_id":"observation-one","reported_callback":"dispatch"
            }),
        };
        let exchange = handler.handle(request, RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (first, reply) = confirmations.recv().await.unwrap();
            reply
                .send(Err(ContractError::Unavailable("lost reply".into())))
                .unwrap();
            let (command, reply) = confirmations.recv().await.unwrap();
            assert_eq!(
                serde_json::to_value(first).unwrap(),
                serde_json::to_value(&command).unwrap()
            );
            let mut source = worker();
            if foreign {
                source.subject = "other".into();
            }
            reply
                .send(Ok(EffectConfirmV2Receipt {
                    scope: command.owner.scope,
                    workflow_id: command.workflow_id,
                    activation_id: command.owner.task_id,
                    attempt_id: command.owner.attempt_id,
                    approval_key: command.approval_key,
                    key: command.record.key,
                    already_accepted: true,
                    claim_id: command.claim_id,
                    observation_id: command.observation_id,
                    worker: source,
                    recorded_at: 2,
                    recorded_revision: 3,
                }))
                .unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        if foreign {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol);
        } else {
            let value = result.unwrap().result;
            assert_eq!(value["committed"], true);
            assert_eq!(value["observation_id"], "observation-one");
            assert!(value.get("worker").is_none());
        }
    }
}
