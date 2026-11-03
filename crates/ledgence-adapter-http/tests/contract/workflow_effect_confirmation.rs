use super::*;

const PATH: &str = "/v1/workflows/approved-effects/confirm";

fn command() -> ApprovedEffectConfirmCommand {
    ApprovedEffectConfirmCommand {
        owner: owner(),
        workflow_id: snapshot().workflow_id,
        approval_key: "approve-payment".into(),
        lookup: "inspect-transfer".into(),
        record: LocalStepRecord {
            key: "_approval:7afb504880cca849b5be71fedd9090a4281e402002e6a4df486051cc3a1c310c"
                .into(),
            callable: "transfer".into(),
            input: json!({"v":1,"lookup":"inspect-transfer","action_json":r#"{"arguments":{"amount":1.0},"name":"transfer","version":"1"}"#}),
            output: json!({"id":"receipt","amount":1.0}),
        },
    }
}

fn receipt(
    command: &ApprovedEffectConfirmCommand,
    already_accepted: bool,
) -> ApprovedEffectConfirmReceipt {
    ApprovedEffectConfirmReceipt {
        scope: command.owner.scope.clone(),
        workflow_id: command.workflow_id.clone(),
        activation_id: command.owner.task_id.clone(),
        attempt_id: command.owner.attempt_id.clone(),
        approval_key: command.approval_key.clone(),
        key: command.record.key.clone(),
        already_accepted,
    }
}

#[tokio::test]
async fn confirmation_preserves_large_journal_record_and_exact_acceptance_or_replay() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let mut command = command();
    command.record.output["evidence"] = json!("x".repeat(APPROVAL_SNAPSHOT_MAX_BYTES));
    command.validate().unwrap();
    for replay in [false, true] {
        let expected = receipt(&command, replay);
        mock.set("effect_confirm", Ok(expected.clone()));
        assert_eq!(
            client.confirm_approved_effect(&command).await.unwrap(),
            expected
        );
    }
    mock.set::<ApprovedEffectConfirmReceipt>("effect_confirm", Err(ContractError::Conflict));
    assert_eq!(
        client.confirm_approved_effect(&command).await.unwrap_err(),
        ContractError::Conflict
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let expected = serde_json::to_value(command).unwrap();
    assert!(
        calls
            .iter()
            .all(|(name, body)| *name == "effect_confirm" && body == &expected)
    );
    assert!(calls[0].1["record"]["output"]["amount"].is_f64());
}

#[tokio::test]
async fn confirmation_server_rejects_invalid_commands_and_an_unbound_store_receipt() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = reqwest::Client::new();
    let url = format!("{}{PATH}", running.url);
    let command = command();
    let mut invalid = serde_json::to_value(&command).unwrap();
    invalid["lookup"] = json!(&command.record.callable);
    for (body, status) in [
        (invalid.to_string(), 400),
        (" ".repeat(APPROVED_EFFECT_CONFIRM_MAX_BYTES + 1), 413),
    ] {
        assert_eq!(
            client
                .post(&url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            status
        );
    }
    assert!(mock.calls.lock().unwrap().is_empty());
    let mut wrong = receipt(&command, true);
    wrong.attempt_id.push('x');
    mock.set("effect_confirm", Ok(wrong));
    assert_eq!(
        client
            .post(url)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&command).unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn independent_peer_cannot_substitute_an_acknowledgement() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let peer = start(axum::Router::new().route(
        PATH,
        axum::routing::post(move || {
            let index = count.fetch_add(1, Ordering::SeqCst);
            async move {
                let mut value = serde_json::to_value(receipt(&command(), true)).unwrap();
                let body = match index {
                    0 => "{".into(), // Outcome is unknown, even if the server committed.
                    1 => json!({"key":command().record.key,"already_accepted":true}).to_string(),
                    2 => {
                        value["scope"]["namespace"] = json!("different");
                        value.to_string()
                    }
                    3 => {
                        value["activation_id"] = json!("different");
                        value.to_string()
                    }
                    4 => {
                        value["approval_key"] = json!("different");
                        value.to_string()
                    }
                    5 => {
                        value["key"] = json!(format!("_approval:{}", "0".repeat(64)));
                        value.to_string()
                    }
                    _ => value.to_string(),
                };
                ([("content-type", "application/json")], body)
            }
        }),
    ))
    .await;
    let client = HttpTaskService::new(&peer.url).unwrap();
    for expected in 1..=6 {
        assert!(matches!(
            client.confirm_approved_effect(&command()).await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), expected);
    }
    assert!(
        client
            .confirm_approved_effect(&command())
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(calls.load(Ordering::SeqCst), 7);
}

#[tokio::test]
async fn an_old_server_cannot_confirm_through_its_ordinary_journal_endpoint() {
    let legacy_writes = Arc::new(AtomicUsize::new(0));
    let count = legacy_writes.clone();
    let peer = start(axum::Router::new().route(
        "/v1/workflows/local-results",
        axum::routing::post(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async {
                (
                    [("content-type", "application/json")],
                    r#"{"already_accepted":true}"#,
                )
            }
        }),
    ))
    .await;
    let client = HttpTaskService::new(&peer.url).unwrap();
    assert!(client.confirm_approved_effect(&command()).await.is_err());
    assert_eq!(legacy_writes.load(Ordering::SeqCst), 0);
}
