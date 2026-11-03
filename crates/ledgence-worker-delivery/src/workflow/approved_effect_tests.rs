use super::*;

fn fixture() -> (WorkflowOperations, mpsc::UnboundedReceiver<EffectClaim>) {
    let (mut service, _, _) = super::fixture();
    let (sender, receiver) = mpsc::unbounded_channel();
    Arc::get_mut(&mut service).unwrap().effect_claims = Some(sender);
    (journal(service), receiver)
}

fn claim() -> RuntimeRequest {
    RuntimeRequest {
        id: 3,
        operation: "approved_effect.claim".into(),
        payload: json!({
            "approval_key": "approve-payment",
            "lookup": "find_transfer",
            "action": {"name": "transfer", "version": "1", "arguments": {"amount": 1.0}}
        }),
    }
}

fn receipt(
    command: &ApprovedEffectClaimCommand,
    disposition: ApprovedEffectDisposition,
) -> ApprovedEffectClaimReceipt {
    ApprovedEffectClaimReceipt {
        scope: command.owner.scope.clone(),
        attempt_id: command.owner.attempt_id.clone(),
        workflow_id: command.workflow_id.clone(),
        activation_id: command.owner.task_id.clone(),
        approval_key: command.approval_key.clone(),
        action: command.action.clone(),
        lookup: command.lookup.clone(),
        disposition,
    }
}

#[tokio::test]
async fn runtime_cannot_supply_effect_authority_or_dispatch_on_an_uncertain_claim() {
    let (handler, mut requests) = fixture();
    for field in [
        "owner",
        "workflow_id",
        "activation_id",
        "attempt_id",
        "disposition",
    ] {
        let mut request = claim();
        request.payload[field] = json!("forged");
        assert_eq!(
            handler
                .handle(request, RunControl::new(Duration::from_secs(1)))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Protocol,
            "{field}"
        );
    }
    assert!(requests.try_recv().is_err());

    for (service_error, expected) in [
        (
            ContractError::Unavailable("first grant persisted; reply lost".into()),
            ErrorKind::Unavailable,
        ),
        (ContractError::Conflict, ErrorKind::Protocol),
        (ContractError::ObsoleteOperation, ErrorKind::Protocol),
        (ContractError::OwnershipLost, ErrorKind::Cancelled),
        (ContractError::NotFound, ErrorKind::Protocol),
    ] {
        let request = handler.handle(claim(), RunControl::new(Duration::from_secs(1)));
        let service = async {
            let (command, reply) = requests.recv().await.unwrap();
            assert_eq!(command.owner, owner());
            assert_eq!(command.workflow_id, context().workflow_id);
            reply.send(Err(service_error)).unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(request, service) }).await;
        assert_eq!(result.unwrap_err().kind, expected);
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn runtime_only_forwards_matching_receipt_and_preserves_lookup_only() {
    let (handler, mut requests) = fixture();
    for disposition in [
        ApprovedEffectDisposition::Dispatch,
        ApprovedEffectDisposition::LookupOnly,
    ] {
        let request = handler.handle(claim(), RunControl::new(Duration::from_secs(1)));
        let service = async {
            let (command, reply) = requests.recv().await.unwrap();
            reply.send(Ok(receipt(&command, disposition))).unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(request, service) }).await;
        let result = result.unwrap();
        assert_eq!(result.id, claim().id);
        assert_eq!(
            result.result,
            json!({
                "workflow_id": context().workflow_id,
                "activation_id": owner().task_id,
                "approval_key": claim().payload["approval_key"],
                "action": claim().payload["action"],
                "lookup": claim().payload["lookup"],
                "disposition": disposition,
            })
        );
    }
    for field in [
        "attempt",
        "workflow",
        "scope",
        "activation",
        "key",
        "number_type",
        "lookup",
    ] {
        let request = handler.handle(claim(), RunControl::new(Duration::from_secs(1)));
        let service = async {
            let (command, reply) = requests.recv().await.unwrap();
            let mut wrong = receipt(&command, ApprovedEffectDisposition::Dispatch);
            match field {
                "attempt" => wrong.attempt_id.push('x'),
                "workflow" => wrong.workflow_id.push('x'),
                "scope" => wrong.scope.namespace.push('x'),
                "activation" => wrong.activation_id.push('x'),
                "key" => wrong.approval_key.push('x'),
                "lookup" => wrong.lookup.push('x'),
                "number_type" => wrong.action.arguments["amount"] = json!(1),
                _ => unreachable!(),
            }
            reply.send(Ok(wrong)).unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(request, service) }).await;
        assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol, "{field}");
    }
}

#[tokio::test]
async fn confirmation_retries_only_the_same_evidence_and_rejects_foreign_receipts() {
    let (mut service, _, _) = super::fixture();
    let (sender, mut confirmations) = mpsc::unbounded_channel();
    Arc::get_mut(&mut service).unwrap().effect_confirms = Some(sender);
    let handler = journal(service);
    let key = "_approval:7afb504880cca849b5be71fedd9090a4281e402002e6a4df486051cc3a1c310c";
    let request = RuntimeRequest {
        id: 4,
        operation: "approved_effect.confirm".into(),
        payload: json!({"approval_key": "approve-payment", "lookup": "find_transfer",
            "record": {"key": key, "callable": "transfer", "input": {
                "v": 1, "lookup": "find_transfer", "action_json": claim().payload["action"].to_string()
            }, "output": {"receipt": "confirmed"}}}),
    };
    for foreign in [true, false] {
        let exchange = handler.handle(request.clone(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (first, reply) = confirmations.recv().await.unwrap();
            assert_eq!(first.owner, owner());
            assert_eq!(first.workflow_id, context().workflow_id);
            reply
                .send(Err(ContractError::Unavailable("reply lost".into())))
                .unwrap();
            let (repeat, reply) = confirmations.recv().await.unwrap();
            assert_eq!(
                serde_json::to_value(&first).unwrap(),
                serde_json::to_value(&repeat).unwrap()
            );
            reply
                .send(Ok(ApprovedEffectConfirmReceipt {
                    scope: repeat.owner.scope,
                    workflow_id: repeat.workflow_id,
                    activation_id: repeat.owner.task_id,
                    attempt_id: if foreign {
                        "other-attempt".into()
                    } else {
                        repeat.owner.attempt_id
                    },
                    approval_key: repeat.approval_key,
                    key: repeat.record.key,
                    already_accepted: true,
                }))
                .unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        if foreign {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol);
        } else {
            assert_eq!(result.unwrap().result, json!({"committed": true}));
        }
    }
}

#[tokio::test]
async fn unsupported_effect_capability_rejects_the_claim_before_dispatch() {
    let (service, _, _) = super::fixture();
    let handler = journal(service);
    let result = handler
        .handle(claim(), RunControl::new(Duration::from_secs(1)))
        .await;
    assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol);
}
