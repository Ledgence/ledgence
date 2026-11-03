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
                "number_type" => wrong.action.arguments["amount"] = json!(1),
                _ => unreachable!(),
            }
            reply.send(Ok(wrong)).unwrap();
        };
        let (result, ()) = bounded(async { tokio::join!(request, service) }).await;
        assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol, "{field}");
    }
}
