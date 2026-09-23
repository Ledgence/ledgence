use super::*;

impl CompletionService for Mock {
    fn subscribe_completion<'a>(
        &'a self,
        command: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move { self.reply("completion_subscribe", command) })
    }
    fn completion_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move { self.reply("completion_status", json!([scope, id])) })
    }
    fn retry_completion<'a>(
        &'a self,
        command: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move { self.reply("completion_retry", command) })
    }
}
fn command() -> CompletionSubscribeCommand {
    CompletionSubscribeCommand {
        scope: scope(),
        target: CompletionTarget::Task {
            id: "task +%/é".into(),
        },
        destination: "billing-results".into(),
        idempotency_key: "notify +/é".into(),
    }
}
fn snapshot() -> CompletionSubscription {
    CompletionSubscription {
        subscription_id: "subscription +%/é".into(),
        command: command(),
        state: CompletionState::Waiting,
        generation: 1,
        attempts: 0,
        total_attempts: 0,
        created_at: 10,
        activated_at: None,
        next_attempt_at: None,
        lease_expires_at: None,
        delivered_at: None,
        exhausted_at: None,
        last_failure: None,
        event: None,
    }
}
fn pending(mut subscription: CompletionSubscription) -> CompletionSubscription {
    let command = &subscription.command;
    let mut event = json!({"specversion":"1.0", "id":completion_event_id(&command.target),
        "source":"urn:ledgence:orchestrator", "type":format!("com.ledgence.{}.completed.v1", command.target.kind()),
        "subject":format!("{}s/{}", command.target.kind(), command.target.id()), "time":"2026-09-22T00:00:00Z",
        "ldgtenantid":command.scope.tenant_id, "ldgnamespace":command.scope.namespace,
        "ldgstate":"succeeded", "ldgresultref":completion_result_ref(&command.scope, &command.target)});
    match &command.target {
        CompletionTarget::Task { id } => {
            event["ldgtaskid"] = json!(id);
            event["ldgrunid"] = json!("run");
        }
        CompletionTarget::Workflow { id } => {
            event["ldgworkflowid"] = json!(id);
        }
    }
    subscription.event = Some(CompletionEvent::new(event).unwrap());
    subscription.activated_at = Some(20);
    subscription.next_attempt_at = Some(20);
    subscription.state = CompletionState::Pending;
    subscription
}
async fn setup(mock: &Arc<Mock>) -> Running {
    start(server::router_with_completions(
        mock.clone(),
        mock.clone(),
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(ledgence_worker_api::NoopTraceBridge),
    ))
    .await
}

#[tokio::test]
async fn completion_roundtrip_preserves_binding_scope_and_reference_event() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for kind in [
        CompletionTarget::Task {
            id: "task +%/é".into(),
        },
        CompletionTarget::Workflow {
            id: "workflow".into(),
        },
    ] {
        let mut expected = snapshot();
        expected.command.target = kind;
        mock.set("completion_subscribe", Ok(expected.clone()));
        assert_eq!(
            client
                .subscribe_completion(&expected.command)
                .await
                .unwrap(),
            expected
        );
        expected = pending(expected);
        mock.set("completion_status", Ok(expected.clone()));
        assert_eq!(
            client
                .completion_status(&scope(), &expected.subscription_id)
                .await
                .unwrap(),
            expected
        );
        assert!(expected.event.unwrap().value().get("data").is_none());
    }
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[1].1, json!([scope(), snapshot().subscription_id]));
}

#[tokio::test]
async fn completion_retry_acknowledges_exact_generation_and_never_retries_http() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let mut expected = pending(snapshot());
    expected.generation = 2;
    expected.total_attempts = 8;
    let retry = CompletionRetryCommand {
        scope: scope(),
        subscription_id: expected.subscription_id.clone(),
        expected_generation: 1,
    };
    mock.set("completion_retry", Ok(expected.clone()));
    assert_eq!(client.retry_completion(&retry).await.unwrap(), expected);
    mock.set::<CompletionSubscription>(
        "completion_retry",
        Err(ContractError::Unavailable("lost acknowledgment".into())),
    );
    assert!(matches!(
        client.retry_completion(&retry).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
    expected.generation = 1;
    expected.total_attempts = 0;
    mock.set("completion_retry", Ok(expected));
    assert!(matches!(
        client.retry_completion(&retry).await,
        Err(ContractError::Unavailable(_))
    ));
}

#[tokio::test]
async fn completion_server_rejects_invalid_bindings_and_inconsistent_state() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    for change in ["scope", "target", "destination", "key", "state"] {
        let mut wrong = snapshot();
        match change {
            "scope" => wrong.command.scope.namespace = "other".into(),
            "target" => {
                wrong.command.target = CompletionTarget::Workflow {
                    id: "another".into(),
                }
            }
            "destination" => wrong.command.destination = "other".into(),
            "key" => wrong.command.idempotency_key = "other".into(),
            _ => wrong.state = CompletionState::Delivered,
        }
        mock.set("completion_subscribe", Ok(wrong));
        let reply = raw
            .post(format!("{}/v1/completion-subscriptions", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&command()).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), 503, "{change}");
        assert!(reply.headers().contains_key("request-id"));
        assert_eq!(reply.headers()["cache-control"], "no-store");
    }
}

#[tokio::test]
async fn completion_client_rejects_forged_identity_and_oversized_observation() {
    for change in ["scope", "id", "event", "state"] {
        let mut value = serde_json::to_value(pending(snapshot())).unwrap();
        match change {
            "scope" => value["command"]["scope"]["tenant_id"] = json!("other"),
            "id" => value["subscription_id"] = json!("other"),
            "event" => value["event"]["ldgstate"] = json!("running"),
            _ => value["lease_expires_at"] = json!(30),
        }
        let (running, _) = raw_response(
            response(
                200,
                "application/json",
                &serde_json::to_vec(&value).unwrap(),
            ),
            None,
        )
        .await;
        let client = HttpTaskService::new(&running.url).unwrap();
        assert!(
            matches!(
                client
                    .completion_status(&scope(), &snapshot().subscription_id)
                    .await,
                Err(ContractError::Unavailable(_))
            ),
            "{change}"
        );
    }
    let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", COMPLETION_STATUS_MAX_BYTES + 1).into_bytes();
    let (running, _) = raw_response(reply, None).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    assert!(matches!(
        client.subscribe_completion(&command()).await,
        Err(ContractError::Unavailable(_))
    ));
}

#[tokio::test]
async fn completion_rejects_malformed_queries_commands_and_wrong_methods_before_store() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    for query in [
        "tenant_id=t&namespace=n",
        "tenant_id=t&namespace=n&subscription_id=x&subscription_id=y",
        "tenant_id=t&namespace=n&subscription_id=x&task_id=t",
        "tenant_id=t&namespace=n&subscription_id=%xx",
    ] {
        let reply = raw
            .get(format!(
                "{}/v1/completion-subscriptions/status?{query}",
                running.url
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), 400);
    }
    let body = serde_json::to_string(&command()).unwrap();
    for invalid in [
        body.replacen("{", "{\"unknown\":1,", 1),
        body.replacen("{", "{\"destination\":\"duplicate\",", 1),
    ] {
        let reply = raw
            .post(format!("{}/v1/completion-subscriptions", running.url))
            .header("content-type", "application/json")
            .body(invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), 400);
    }
    let reply = raw
        .post(format!("{}/v1/completion-subscriptions", running.url))
        .header("content-type", "application/json")
        .body(" ".repeat(COMPLETION_COMMAND_MAX_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 413);
    let reply = raw
        .get(format!("{}/v1/completion-subscriptions", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 405);
    assert_eq!(reply.headers()["allow"], "POST");
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn completion_not_found_and_conflict_are_definitive() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for error in [
        ContractError::Conflict,
        ContractError::NotFound,
        ContractError::ObsoleteOperation,
    ] {
        mock.set::<CompletionSubscription>("completion_subscribe", Err(error.clone()));
        assert_eq!(
            client.subscribe_completion(&command()).await.unwrap_err(),
            error
        );
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 3);
}
