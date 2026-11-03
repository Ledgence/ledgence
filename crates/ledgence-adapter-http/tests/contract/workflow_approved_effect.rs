use super::*;

const PATH: &str = "/v1/workflows/approved-effects/claim";

fn command() -> ApprovedEffectClaimCommand {
    ApprovedEffectClaimCommand {
        owner: owner(),
        workflow_id: snapshot().workflow_id,
        approval_key: "approve-payment".into(),
        lookup: "inspect-transfer".into(),
        action: ApprovalAction {
            name: "transfer".into(),
            version: "1".into(),
            arguments: json!({"amount": 1.0}),
        },
    }
}

fn receipt(disposition: ApprovedEffectDisposition) -> ApprovedEffectClaimReceipt {
    let command = command();
    ApprovedEffectClaimReceipt {
        scope: command.owner.scope,
        attempt_id: command.owner.attempt_id,
        workflow_id: command.workflow_id,
        activation_id: command.owner.task_id,
        approval_key: command.approval_key,
        lookup: command.lookup,
        action: command.action,
        disposition,
    }
}

#[tokio::test]
async fn approved_effect_http_preserves_fence_action_disposition_and_conflict() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for disposition in [
        ApprovedEffectDisposition::Dispatch,
        ApprovedEffectDisposition::LookupOnly,
    ] {
        mock.set("effect_claim", Ok(receipt(disposition)));
        let reply = client.claim_approved_effect(&command()).await.unwrap();
        assert_eq!(reply.disposition, disposition);
        assert!(reply.matches(&command()).unwrap());
    }
    mock.set::<ApprovedEffectClaimReceipt>("effect_claim", Err(ContractError::Conflict));
    assert_eq!(
        client.claim_approved_effect(&command()).await.unwrap_err(),
        ContractError::Conflict
    );

    let mut wrong = receipt(ApprovedEffectDisposition::Dispatch);
    wrong.attempt_id.push('x');
    mock.set("effect_claim", Ok(wrong));
    let rejected_grant = reqwest::Client::new()
        .post(format!("{}{PATH}", running.url))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&command()).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(rejected_grant.status(), 503);
    assert_eq!(rejected_grant.headers()["cache-control"], "no-store");
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert!(calls.iter().all(|(method, body)| *method == "effect_claim"
        && *body == serde_json::to_value(command()).unwrap()));
}

#[tokio::test]
async fn ambiguous_or_unbound_effect_receipts_from_an_independent_peer_never_dispatch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let peer = start(axum::Router::new().route(
        PATH,
        axum::routing::post(move || {
            let index = count.fetch_add(1, Ordering::SeqCst);
            async move {
                let mut reply =
                    serde_json::to_value(receipt(ApprovedEffectDisposition::Dispatch)).unwrap();
                let encoded = match index {
                    // The peer consumed the grant but the response body was cut short.
                    0 => "{".to_owned(),
                    1 => {
                        reply["action"]["arguments"]["amount"] = json!(1);
                        reply.to_string()
                    }
                    2 => {
                        reply["attempt_id"] = json!("previous-attempt");
                        reply.to_string()
                    }
                    3 => {
                        reply["disposition"] = json!("unknown");
                        reply.to_string()
                    }
                    4 => {
                        reply["lookup"] = json!("other-lookup");
                        reply.to_string()
                    }
                    5 => {
                        reply.as_object_mut().unwrap().remove("lookup");
                        reply.to_string()
                    }
                    _ => serde_json::to_string(&receipt(ApprovedEffectDisposition::LookupOnly))
                        .unwrap(),
                };
                ([("content-type", "application/json")], encoded)
            }
        }),
    ))
    .await;
    let client = HttpTaskService::new(&peer.url).unwrap();
    for expected_calls in 1..=6 {
        assert!(matches!(
            client.claim_approved_effect(&command()).await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
    }
    let recovered = client.claim_approved_effect(&command()).await.unwrap();
    assert_eq!(recovered.disposition, ApprovedEffectDisposition::LookupOnly);
    assert_eq!(calls.load(Ordering::SeqCst), 7);
}

#[tokio::test]
async fn malformed_or_oversized_effect_claim_never_reaches_service() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = reqwest::Client::new();
    let encoded = serde_json::to_string(&command()).unwrap();
    let duplicate = encoded.replace(r#""amount":1.0"#, r#""amount":1.0,"amount":1"#);
    assert_ne!(duplicate, encoded);
    let mut legacy = serde_json::to_value(command()).unwrap();
    legacy.as_object_mut().unwrap().remove("lookup");
    for (body, expected) in [
        (duplicate, 400),
        (legacy.to_string(), 400),
        (encoded.replace("inspect-transfer", "transfer"), 400),
        (" ".repeat(APPROVAL_SNAPSHOT_MAX_BYTES + 1), 413),
    ] {
        let reply = client
            .post(format!("{}{PATH}", running.url))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), expected);
    }
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_workflow_service_without_protected_writes_never_receives_a_claim() {
    let mock = Arc::new(Mock::default());
    mock.protected_writes_disabled.store(true, Ordering::SeqCst);
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let port = client.approved_effects().unwrap();
    assert!(matches!(
        port.claim_approved_effect(&command()).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert!(mock.calls.lock().unwrap().is_empty());
}
