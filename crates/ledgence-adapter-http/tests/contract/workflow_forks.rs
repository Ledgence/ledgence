use super::*;

fn command() -> WorkflowForkCommand {
    WorkflowForkCommand {
        owner: owner(),
        processing_trace: Some(ledgence_worker_api::TraceContext {
            traceparent: "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-00".into(),
            tracestate: Some("vendor=unsampled".into()),
        }),
        fork: WorkflowForkRequest {
            key: "fanout".into(),
            branches: ["first", "second"]
                .into_iter()
                .map(|key| WorkflowBranch {
                    key: key.into(),
                    entrypoint: "evaluate".into(),
                    queue: "agents".into(),
                    data: json!([null, 1, 1.0, -0.0, u64::MAX]),
                    retry_policy: RetryPolicy::default(),
                    attempt_timeout_ms: 60_000,
                })
                .collect(),
        },
    }
}

fn receipt() -> WorkflowForkReceipt {
    WorkflowForkReceipt {
        key: "fanout".into(),
        branch_keys: vec!["first".into(), "second".into()],
        already_accepted: false,
    }
}

#[tokio::test]
async fn fork_codec_preserves_fence_trace_numeric_shapes_and_explicit_replay() {
    let mock = Arc::new(Mock::default());
    mock.set("wf_fork", Ok(receipt()));
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let command = command();
    assert_eq!(client.fork_workflow(&command).await.unwrap(), receipt());
    let mut replay = receipt();
    replay.already_accepted = true;
    mock.set("wf_fork", Ok(replay.clone()));
    assert_eq!(client.fork_workflow(&command).await.unwrap(), replay);
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for (name, received) in calls.iter() {
        assert_eq!(*name, "wf_fork");
        assert_eq!(
            serde_json::to_string(received).unwrap(),
            serde_json::to_string(&serde_json::to_value(&command).unwrap()).unwrap()
        );
        assert!(received["fork"]["branches"][0]["data"][2].is_f64());
        assert_eq!(
            received["processing_trace"]["traceparent"],
            command.processing_trace.as_ref().unwrap().traceparent
        );
    }
}

#[tokio::test]
async fn fork_receipts_round_trip_maximum_escaped_keys_after_acceptance_and_replay() {
    let mut command = command();
    command.fork.key = "\"".repeat(128);
    command.fork.branches = (0..WORKFLOW_MAX_COMMANDS)
        .map(|index| WorkflowBranch {
            key: format!("{}{index:02x}", "\"".repeat(126)),
            entrypoint: "evaluate".into(),
            queue: "agents".into(),
            data: Value::Null,
            retry_policy: RetryPolicy::default(),
            attempt_timeout_ms: 60_000,
        })
        .collect();
    command.validate().unwrap();
    let expected = WorkflowForkReceipt {
        key: command.fork.key.clone(),
        branch_keys: command
            .fork
            .branches
            .iter()
            .map(|branch| branch.key.clone())
            .collect(),
        already_accepted: false,
    };
    expected.validate().unwrap();
    assert!(serde_json::to_vec(&expected).unwrap().len() > TASK_STATUS_MAX_BYTES);
    assert!(
        serde_json::to_vec(&expected).unwrap().len()
            <= ledgence_adapter_http::WORKFLOW_FORK_RECEIPT_MAX_BYTES
    );
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for already_accepted in [false, true] {
        let expected = WorkflowForkReceipt {
            already_accepted,
            ..expected.clone()
        };
        mock.set("wf_fork", Ok(expected.clone()));
        assert_eq!(client.fork_workflow(&command).await.unwrap(), expected);
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn malformed_fork_commands_never_reach_service() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    for defect in [
        "duplicate",
        "empty",
        "owner",
        "generation",
        "trace",
        "missing_data",
        "unknown",
        "too_large",
    ] {
        let mut body = serde_json::to_value(command()).unwrap();
        match defect {
            "duplicate" => body["fork"]["branches"][1]["key"] = json!("first"),
            "empty" => body["fork"]["branches"] = json!([]),
            "owner" => body["owner"]["lease_id"] = json!(""),
            "generation" => body["owner"]["generation"] = json!(0),
            "trace" => body["processing_trace"]["traceparent"] = json!("invalid"),
            "missing_data" => {
                body["fork"]["branches"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("data");
            }
            "unknown" => body["fork"]["branches"][0]["program"] = json!("untrusted"),
            "too_large" => {
                body["fork"]["branches"][0]["data"] = json!("x".repeat(WORKFLOW_FORK_MAX_BYTES))
            }
            _ => unreachable!(),
        }
        let response = raw
            .post(format!("{}/v1/workflows/forks", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "{defect}");
    }
    let bytes = serde_json::to_string(&command())
        .unwrap()
        .replace("\"fanout\"", "\"fanout\",\"key\":\"duplicate\"");
    let response = raw
        .post(format!("{}/v1/workflows/forks", running.url))
        .header("content-type", "application/json")
        .body(bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn service_fork_receipt_mismatch_remains_uncertain_without_automatic_retry() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    for defect in ["key", "order", "duplicate", "missing", "empty"] {
        let mut value = receipt();
        match defect {
            "key" => value.key = "other".into(),
            "order" => value.branch_keys.reverse(),
            "duplicate" => value.branch_keys[1] = "first".into(),
            "missing" => {
                value.branch_keys.pop();
            }
            "empty" => value.branch_keys.clear(),
            _ => unreachable!(),
        }
        mock.set("wf_fork", Ok(value));
        let response = raw
            .post(format!("{}/v1/workflows/forks", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&command()).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503, "{defect}");
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn client_checks_independent_fork_peer_shape_identity_order_and_duplicates() {
    for defect in [
        "key",
        "order",
        "duplicate",
        "missing",
        "empty",
        "unknown",
        "duplicate_field",
    ] {
        let mut value = serde_json::to_value(receipt()).unwrap();
        match defect {
            "key" => value["key"] = json!("other"),
            "order" => value["branch_keys"] = json!(["second", "first"]),
            "duplicate" => value["branch_keys"] = json!(["first", "first"]),
            "missing" => value["branch_keys"] = json!(["first"]),
            "empty" => value["branch_keys"] = json!([]),
            "unknown" => value["new_field"] = json!(true),
            "duplicate_field" => {}
            _ => unreachable!(),
        }
        let mut encoded = serde_json::to_string(&value).unwrap();
        if defect == "duplicate_field" {
            encoded = encoded.replace("\"fanout\"", "\"fanout\",\"key\":\"fanout\"");
        }
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let peer = start(axum::Router::new().route(
            "/v1/workflows/forks",
            axum::routing::post(move || {
                let encoded = encoded.clone();
                count.fetch_add(1, Ordering::SeqCst);
                async move { ([("content-type", "application/json")], encoded) }
            }),
        ))
        .await;
        let client = HttpTaskService::new(&peer.url).unwrap();
        assert!(
            matches!(
                client.fork_workflow(&command()).await,
                Err(ContractError::Unavailable(_))
            ),
            "{defect}"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn fork_http_timeout_is_uncertain_and_never_reissues_mutation() {
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let peer = start(axum::Router::new().route(
        "/v1/workflows/forks",
        axum::routing::post(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                (
                    [("content-type", "application/json")],
                    serde_json::to_vec(&receipt()).unwrap(),
                )
            }
        }),
    ))
    .await;
    let client = HttpTaskService::with_timeout(&peer.url, Duration::from_millis(50)).unwrap();
    assert!(matches!(
        client.fork_workflow(&command()).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fork_capability_method_and_body_limit_follow_control_routes() {
    let mock = Arc::new(Mock::default());
    let disabled = start(server::router(mock.clone())).await;
    assert!(matches!(
        HttpTaskService::new(&disabled.url)
            .unwrap()
            .fork_workflow(&command())
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let wrong_method = raw
        .get(format!("{}/v1/workflows/forks", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_method.status(), 405);
    assert_eq!(wrong_method.headers()["allow"], "POST");
    let oversized = raw
        .post(format!("{}/v1/workflows/forks", running.url))
        .header("content-type", "application/json")
        .body(vec![b' '; WORKFLOW_FORK_COMMAND_MAX_BYTES + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), 413);
    assert!(mock.calls.lock().unwrap().is_empty());
}
