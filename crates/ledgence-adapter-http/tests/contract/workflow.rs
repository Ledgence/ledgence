use super::*;
impl WorkflowService for Mock {
    fn submit_workflow<'a>(&'a self, c: &'a SubmitCommand) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move { self.reply("wf_submit", c) })
    }
    fn workflow_status<'a>(
        &'a self,
        s: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move { self.reply("wf_status", json!([s, id])) })
    }
    fn workflow_result<'a>(
        &'a self,
        s: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowResult> {
        Box::pin(async move { self.reply("wf_result", json!([s, id])) })
    }
    fn activation_context<'a>(
        &'a self,
        o: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        Box::pin(async move { self.reply("wf_context", o) })
    }
    fn record_local_result<'a>(
        &'a self,
        c: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        Box::pin(async move { self.reply("wf_local", c) })
    }
    fn cancel_workflow<'a>(
        &'a self,
        s: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move { self.reply("wf_cancel", json!([s, id])) })
    }
}
fn snapshot() -> WorkflowSnapshot {
    WorkflowSnapshot {
        workflow_id: "workflow +%/é".into(),
        scope: scope(),
        state: WorkflowState::Running,
        revision: 0,
        activation_id: Some(owner().task_id),
        submitted_at: 10,
        terminal_at: None,
        correlation_key: Some("invoice:42".into()),
    }
}
fn context() -> WorkflowActivationContext {
    WorkflowActivationContext {
        v: 1,
        workflow_id: snapshot().workflow_id,
        activation_id: owner().task_id,
        revision: 0,
        continuation: "start".into(),
        state: Value::Null,
        inputs: Default::default(),
        local_steps: vec![],
    }
}
async fn setup(mock: &Arc<Mock>) -> Running {
    start(server::router_with_workflows(
        mock.clone(),
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(ledgence_worker_api::NoopTraceBridge),
    ))
    .await
}

#[tokio::test]
async fn workflow_operations_preserve_identity_payload_and_pending_vs_null() {
    let mock = Arc::new(Mock::default());
    mock.set("wf_submit", Ok(snapshot()));
    mock.set("wf_status", Ok(snapshot()));
    mock.set(
        "wf_result",
        Ok(WorkflowResult {
            workflow: snapshot(),
            outcome: None,
        }),
    );
    mock.set("wf_context", Ok(context()));
    mock.set("wf_cancel", Ok(snapshot()));
    mock.set(
        "wf_local",
        Ok(LocalResultReceipt {
            key: "local".into(),
            already_accepted: false,
        }),
    );
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let command = submit(json!({"unowned": [null,-0.0,18446744073709551615u64]}));
    let accepted = client.submit_workflow(&command).await.unwrap();
    let id = &accepted.workflow_id;
    assert_eq!(
        client
            .workflow_status(&scope(), id)
            .await
            .unwrap()
            .workflow_id,
        *id
    );
    assert!(
        client
            .workflow_result(&scope(), id)
            .await
            .unwrap()
            .outcome
            .is_none()
    );
    assert_eq!(
        client
            .activation_context(&owner())
            .await
            .unwrap()
            .activation_id,
        owner().task_id
    );
    let local = LocalResultCommand {
        owner: owner(),
        record: LocalStepRecord {
            key: "local".into(),
            callable: "app:fetch".into(),
            input: json!({"x":1.0}),
            output: Value::Null,
        },
    };
    assert!(
        !client
            .record_local_result(&local)
            .await
            .unwrap()
            .already_accepted
    );
    client.cancel_workflow(&scope(), id).await.unwrap();
    let mut terminal = snapshot();
    terminal.state = WorkflowState::Succeeded;
    terminal.activation_id = None;
    terminal.terminal_at = Some(20);
    mock.set(
        "wf_result",
        Ok(WorkflowResult {
            workflow: terminal,
            outcome: Some(WorkflowOutcome::Succeeded {
                output: Value::Null,
            }),
        }),
    );
    assert!(matches!(
        client.workflow_result(&scope(), id).await.unwrap().outcome,
        Some(WorkflowOutcome::Succeeded {
            output: Value::Null
        })
    ));
    let calls = mock.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .find(|(name, _)| *name == "wf_submit")
            .unwrap()
            .1,
        serde_json::to_value(command).unwrap()
    );
    assert_eq!(
        calls
            .iter()
            .find(|(name, _)| *name == "wf_local")
            .unwrap()
            .1,
        serde_json::to_value(local).unwrap()
    );
}

#[tokio::test]
async fn workflow_identity_and_invalid_context_replies_cannot_be_accepted() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let mut wrong = snapshot();
    wrong.workflow_id = "another".into();
    mock.set("wf_status", Ok(wrong));
    assert!(matches!(
        client
            .workflow_status(&scope(), &snapshot().workflow_id)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    let mut invalid = context();
    invalid.v = 2;
    mock.set("wf_context", Ok(invalid));
    assert!(client.activation_context(&owner()).await.is_err());
    mock.set(
        "wf_local",
        Ok(LocalResultReceipt {
            key: "wrong".into(),
            already_accepted: true,
        }),
    );
    let local = LocalResultCommand {
        owner: owner(),
        record: LocalStepRecord {
            key: "local".into(),
            callable: "app:fetch".into(),
            input: Value::Null,
            output: Value::Null,
        },
    };
    assert!(matches!(
        client.record_local_result(&local).await,
        Err(ContractError::Unavailable(_))
    ));
}

#[tokio::test]
async fn workflow_routes_reject_malformed_commands_and_unconfigured_capability() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let malformed = raw
        .post(format!("{}/v1/workflows/local-results", running.url))
        .header("content-type", "application/json")
        .body(r#"{"owner":{},"record":{"key":"a","key":"b"}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    assert!(mock.calls.lock().unwrap().is_empty());
    for query in [
        "tenant_id=t&namespace=n&workflow_id=w&task_id=t",
        "tenant_id=t&namespace=n&workflow_id=w&workflow_id=x",
        "tenant_id=t&namespace=n",
    ] {
        assert_eq!(
            raw.get(format!("{}/v1/workflows/status?{query}", running.url))
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    let disabled = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&disabled.url).unwrap();
    assert!(matches!(
        client.submit_workflow(&submit(Value::Null)).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_post_mutation_workflow_replies_remain_uncertain() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let mut malformed = snapshot();
    malformed.activation_id = None;
    mock.set("wf_submit", Ok(malformed.clone()));
    mock.set("wf_cancel", Ok(malformed));
    for (path, body) in [
        (
            "/v1/workflows",
            serde_json::to_value(submit(Value::Null)).unwrap(),
        ),
        (
            "/v1/workflows/cancel",
            json!({"scope":scope(),"workflow_id":snapshot().workflow_id}),
        ),
    ] {
        let reply = raw
            .post(format!("{}{path}", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(
            reply.status(),
            503,
            "post-mutation validation must retain uncertainty"
        );
    }
    let client = HttpTaskService::new(&running.url).unwrap();
    for changed in ["scope", "correlation"] {
        let mut wrong = snapshot();
        if changed == "scope" {
            wrong.scope.namespace = "another".into();
        } else {
            wrong.correlation_key = Some("another".into());
        }
        mock.set("wf_submit", Ok(wrong));
        assert!(matches!(
            client.submit_workflow(&submit(Value::Null)).await,
            Err(ContractError::Unavailable(_))
        ));
    }
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        4,
        "the accepted service calls were not reissued"
    );
}
