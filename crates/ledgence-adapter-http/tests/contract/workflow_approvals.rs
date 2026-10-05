use super::*;
fn proposal() -> ApprovalSnapshot {
    ApprovalSnapshot {
        scope: scope(),
        workflow_id: snapshot().workflow_id,
        key: "refund:1 / é".into(),
        activation_id: "activation:1".into(),
        revision: u64::MAX,
        action: ApprovalAction {
            name: "app:refund".into(),
            version: "1".into(),
            arguments: json!({"amount": 50, "values": [1,1.0,-0.0,18446744073709551615u64]}),
        },
        proposed_arguments: Some(json!({"amount":100})),
        created_at: 10,
        deadline: 1000,
        status: ApprovalStatus::Pending,
        decision: None,
        resumed_activation_id: None,
    }
}
fn command() -> ApprovalDecisionCommand {
    let p = proposal();
    ApprovalDecisionCommand {
        scope: p.scope,
        workflow_id: p.workflow_id,
        key: p.key,
        activation_id: p.activation_id,
        revision: p.revision,
        action: p.action,
        decision_id: "decision:1".into(),
        decision: ApprovalDecision::Approve,
        reviewer: "operator".into(),
        reason: Some("Reviewed effective amount".into()),
    }
}
fn receipt() -> ApprovalDecisionReceipt {
    let c = command();
    let mut p = proposal();
    p.status = ApprovalStatus::Approved;
    p.decision = Some(ApprovalDecisionRecord {
        decision_id: c.decision_id,
        decision: c.decision,
        reviewer: c.reviewer,
        reason: c.reason,
        decided_at: 50,
    });
    ApprovalDecisionReceipt {
        approval: p,
        already_accepted: false,
    }
}
#[tokio::test]
async fn approvals_roundtrip_exact_arguments_and_explicit_retries() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let p = proposal();
    mock.set("approval", Ok(p.clone()));
    let actual = client
        .approval(&p.scope, &p.workflow_id, &p.key)
        .await
        .unwrap();
    assert!(actual.action.matches(&p.action).unwrap());
    assert_eq!(actual.revision, u64::MAX);
    mock.set(
        "approvals",
        Ok(ApprovalPage {
            items: vec![p.clone()],
            next_cursor: None,
        }),
    );
    assert_eq!(
        client
            .list_approvals(&p.scope, &p.workflow_id, None, 1)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    for replay in [false, true] {
        let mut r = receipt();
        r.already_accepted = replay;
        mock.set("decide_approval", Ok(r));
        assert_eq!(
            client
                .decide_approval(&command())
                .await
                .unwrap()
                .already_accepted,
            replay
        );
    }
    mock.set::<ApprovalDecisionReceipt>("decide_approval", Err(ContractError::Conflict));
    assert_eq!(
        client.decide_approval(&command()).await.unwrap_err(),
        ContractError::Conflict
    );
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        5,
        "transport does not retry mutations"
    );
}
#[tokio::test]
async fn altered_receipts_are_uncertain_on_server_and_client_boundaries() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    for field in [
        "scope",
        "workflow",
        "key",
        "activation",
        "revision",
        "action",
        "decision",
        "reviewer",
        "reason",
    ] {
        let mut wrong = receipt();
        match field {
            "scope" => wrong.approval.scope.namespace = "other".into(),
            "workflow" => wrong.approval.workflow_id = "other".into(),
            "key" => wrong.approval.key = "other".into(),
            "activation" => wrong.approval.activation_id = "other".into(),
            "revision" => wrong.approval.revision = 1,
            "action" => wrong.approval.action.arguments["amount"] = json!(50.0),
            "decision" => wrong.approval.decision.as_mut().unwrap().decision_id = "other".into(),
            "reviewer" => wrong.approval.decision.as_mut().unwrap().reviewer = "other".into(),
            _ => wrong.approval.decision.as_mut().unwrap().reason = None,
        }
        mock.set("decide_approval", Ok(wrong.clone()));
        let response = reqwest::Client::new()
            .post(format!("{}/v1/workflows/approvals/decide", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&command()).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503, "{field}");
        let peer = start(axum::Router::new().route(
            "/v1/workflows/approvals/decide",
            axum::routing::post(move || async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    serde_json::to_vec(&wrong).unwrap(),
                )
            }),
        ))
        .await;
        assert!(
            matches!(
                HttpTaskService::new(&peer.url)
                    .unwrap()
                    .decide_approval(&command())
                    .await,
                Err(ContractError::Unavailable(_))
            ),
            "{field}"
        );
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 9);
}
#[tokio::test]
async fn reads_reject_wrong_identity_and_pagination() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let p = proposal();
    let mut wrong = p.clone();
    wrong.scope.namespace = "other".into();
    mock.set("approval", Ok(wrong.clone()));
    assert!(matches!(
        client.approval(&p.scope, &p.workflow_id, &p.key).await,
        Err(ContractError::Unavailable(_))
    ));
    mock.set(
        "approvals",
        Ok(ApprovalPage {
            items: vec![wrong],
            next_cursor: None,
        }),
    );
    assert!(matches!(
        client
            .list_approvals(&p.scope, &p.workflow_id, None, 1)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    mock.set(
        "approvals",
        Ok(ApprovalPage {
            items: vec![p.clone()],
            next_cursor: None,
        }),
    );
    assert!(matches!(
        client
            .list_approvals(&p.scope, &p.workflow_id, Some(&p.key), 1)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    let before = mock.calls.lock().unwrap().len();
    assert!(matches!(
        client
            .list_approvals(&p.scope, &p.workflow_id, None, 11)
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(mock.calls.lock().unwrap().len(), before);
}
#[tokio::test]
async fn console_binds_scope_preserves_numbers_and_checks_origin() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(super::super::console::Queries::default());
    let running = super::super::console::setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    mock.set("approval", Ok(proposal()));
    mock.set("decide_approval", Ok(receipt()));
    let response = client
        .post(format!("{}/v1/console/approvals/inspect", running.url))
        .header("content-type", "application/json")
        .body(json!({"workflow_id":proposal().workflow_id,"key":proposal().key}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert!(body.get("scope").is_none());
    assert_eq!(body["revision"], u64::MAX.to_string());
    assert_eq!(
        body["action"]["arguments"].to_string(),
        proposal().action.arguments.to_string()
    );
    let mut c = serde_json::to_value(command()).unwrap();
    c.as_object_mut().unwrap().remove("scope");
    c["revision"] = json!(u64::MAX.to_string());
    let url = format!("{}/v1/console/approvals/decide", running.url);
    let before = mock.calls.lock().unwrap().len();
    assert_eq!(
        client
            .post(&url)
            .header("origin", "https://other.example")
            .header("content-type", "application/json")
            .body(c.to_string())
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let mut scoped = c.clone();
    scoped["scope"] = json!({"tenant_id":"other","namespace":"other"});
    assert_eq!(
        client
            .post(&url)
            .header("content-type", "application/json")
            .body(scoped.to_string())
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(mock.calls.lock().unwrap().len(), before);
    let response = client
        .post(&url)
        .header("origin", "https://console.example")
        .header("content-type", "application/json")
        .body(c.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let r: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert!(r["approval"].get("scope").is_none());
    assert_eq!(
        mock.calls.lock().unwrap().last().unwrap().1,
        serde_json::to_value(command()).unwrap()
    );
}
#[tokio::test]
async fn nullable_decision_reason_is_required_on_console_and_public_routes() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(super::super::console::Queries::default());
    let running = super::super::console::setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    let mut accepted = receipt();
    accepted.approval.decision.as_mut().unwrap().reason = None;
    mock.set("decide_approval", Ok(accepted));
    for console in [false, true] {
        let mut body = serde_json::to_value(command()).unwrap();
        if console {
            body.as_object_mut().unwrap().remove("scope");
            body["revision"] = json!(u64::MAX.to_string());
        }
        let route = if console { "console" } else { "workflows" };
        let url = format!("{}/v1/{route}/approvals/decide", running.url);
        body.as_object_mut().unwrap().remove("reason");
        let before = mock.calls.lock().unwrap().len();
        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(mock.calls.lock().unwrap().len(), before);
        body["reason"] = Value::Null;
        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let receipt: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert!(receipt["approval"]["decision"]["reason"].is_null());
        assert_eq!(mock.calls.lock().unwrap().len(), before + 1);
    }
}

#[tokio::test]
async fn duplicate_fields_and_invalid_actions_never_reach_service() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let mut invalid = serde_json::to_value(command()).unwrap();
    invalid["action"]["arguments"] = json!([]);
    let body = serde_json::to_string(&command()).unwrap();
    for bytes in [
        invalid.to_string(),
        body.replacen("{", "{\"key\":\"different\",", 1),
    ] {
        let response = reqwest::Client::new()
            .post(format!("{}/v1/workflows/approvals/decide", running.url))
            .header("content-type", "application/json")
            .body(bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn activation_context_rejects_approval_from_another_scope_on_both_boundaries() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let mut p = receipt().approval;
    p.revision = 0;
    p.scope.namespace = "different-scope".into();
    p.resumed_activation_id = Some(owner().task_id.clone());
    let mut c = context();
    c.revision = 1;
    c.wake = Some(WorkflowWake::Approval {
        approval: Box::new(p),
    });
    c.validate().unwrap();
    mock.set("wf_context", Ok(c.clone()));
    let response = reqwest::Client::new()
        .post(format!("{}/v1/workflows/activations/context", running.url))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&owner()).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let peer = start(axum::Router::new().route(
        "/v1/workflows/activations/context",
        axum::routing::post(move || async move {
            (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                serde_json::to_vec(&c).unwrap(),
            )
        }),
    ))
    .await;
    assert!(matches!(
        HttpTaskService::new(&peer.url)
            .unwrap()
            .activation_context(&owner())
            .await,
        Err(ContractError::Unavailable(_))
    ));
}
