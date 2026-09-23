use super::*;

fn command() -> WorkflowEventCommand {
    serde_json::from_value(json!({
        "scope": scope(), "workflow_id": snapshot().workflow_id, "key": "approval:round:1",
        "event": {
            "specversion": "1.0", "id": "evt_1", "source": "urn:test:billing",
            "type": "billing.approved.v1", "datacontenttype": "application/json",
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
            "tracestate": "origin=original", "businessid": "INV-42",
            "data": {"approved": true, "values": [null, 1, 1.0, -0.0, 18446744073709551615u64]}
        }
    }))
    .unwrap()
}
fn receipt(command: &WorkflowEventCommand) -> WorkflowEventReceipt {
    WorkflowEventReceipt {
        scope: command.scope.clone(),
        workflow_id: command.workflow_id.clone(),
        key: command.key.clone(),
        event_id: command.event.id().into(),
        event_source: command.event.source().into(),
        accepted_at: 42,
        already_accepted: false,
    }
}

#[tokio::test]
async fn event_receipts_preserve_cloud_event_binding_and_definitive_errors() {
    let mock = Arc::new(Mock::default());
    let command = command();
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for replay in [false, true] {
        let mut reply = receipt(&command);
        reply.already_accepted = replay;
        mock.set("wf_event", Ok(reply));
        let accepted = client.send_workflow_event(&command).await.unwrap();
        assert_eq!(accepted.already_accepted, replay);
        assert_eq!(accepted.accepted_at, 42);
        assert_eq!(accepted.key, command.key);
    }
    for error in [
        ContractError::Conflict,
        ContractError::ObsoleteOperation,
        ContractError::NotFound,
    ] {
        mock.set::<WorkflowEventReceipt>("wf_event", Err(error.clone()));
        assert_eq!(
            client.send_workflow_event(&command).await.unwrap_err(),
            error
        );
    }
    let calls = mock.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        5,
        "transport must not retry event mutations itself"
    );
    for (name, body) in calls.iter() {
        assert_eq!(*name, "wf_event");
        assert_eq!(
            serde_json::to_string(body).unwrap(),
            serde_json::to_string(&serde_json::to_value(&command).unwrap()).unwrap()
        );
    }
}

#[tokio::test]
async fn event_receipt_identity_is_checked_on_both_http_boundaries() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let command = command();
    for field in ["tenant", "namespace", "workflow", "key", "source", "id"] {
        let mut wrong = receipt(&command);
        match field {
            "tenant" => wrong.scope.tenant_id = "another".into(),
            "namespace" => wrong.scope.namespace = "another".into(),
            "workflow" => wrong.workflow_id = "another".into(),
            "key" => wrong.key = "another".into(),
            "source" => wrong.event_source = "urn:another".into(),
            "id" => wrong.event_id = "another".into(),
            _ => unreachable!(),
        }
        mock.set("wf_event", Ok(wrong.clone()));
        let response = raw
            .post(format!("{}/v1/workflows/events", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&command).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            503,
            "post-mutation {field} mismatch remains uncertain"
        );
        // Bypass our server to prove the Rust client rejects a nonconforming peer.
        let peer = start(axum::Router::new().route(
            "/v1/workflows/events",
            axum::routing::post(move || async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    serde_json::to_vec(&wrong).unwrap(),
                )
            }),
        ))
        .await;
        let observed = Arc::new(AtomicUsize::new(0));
        let captured = observed.clone();
        let client = HttpTaskService::new(&peer.url)
            .unwrap()
            .with_observer(move |_| {
                captured.fetch_add(1, Ordering::Relaxed);
            });
        assert!(
            matches!(
                client.send_workflow_event(&command).await,
                Err(ContractError::Unavailable(_))
            ),
            "client accepted {field} mismatch"
        );
        assert_eq!(observed.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn events_reject_invalid_json_and_bounded_payloads_before_service_execution() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let command = serde_json::to_value(command()).unwrap();
    let mut missing = command.clone();
    missing["event"].as_object_mut().unwrap().remove("source");
    let mut unknown = command.clone();
    unknown["extra"] = json!(true);
    let mut invalid_trace = command.clone();
    invalid_trace["event"]["traceparent"] = json!("invalid");
    let mut oversized_event = command.clone();
    oversized_event["event"]["data"] = json!("x".repeat(64 * 1024));
    let duplicate = serde_json::to_string(&command)
        .unwrap()
        .replace("\"id\":\"evt_1\"", "\"id\":\"evt_1\",\"id\":\"evt_2\"");
    let mut cases = [missing, unknown, invalid_trace, oversized_event]
        .into_iter()
        .map(|value| (serde_json::to_vec(&value).unwrap(), 400))
        .collect::<Vec<_>>();
    cases.push((duplicate.into_bytes(), 400));
    cases.push((vec![b' '; WORKFLOW_EVENT_COMMAND_MAX_BYTES + 1], 413));
    for (body, status) in cases {
        let response = raw
            .post(format!("{}/v1/workflows/events", running.url))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
    }
    let mut invalid = serde_json::from_value::<WorkflowEventCommand>(command).unwrap();
    invalid.key.clear();
    assert!(matches!(
        HttpTaskService::new(&running.url)
            .unwrap()
            .send_workflow_event(&invalid)
            .await,
        Err(ContractError::InvalidInput(_))
    ));
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[derive(Default)]
struct EventTraceBridge {
    parents: Mutex<Vec<Option<TraceContext>>>,
    links: Mutex<Vec<TraceContext>>,
    link_targets: Mutex<Vec<String>>,
    entered: Arc<Mutex<Vec<tracing::span::Id>>>,
}
impl ledgence_worker_api::TraceBridge for EventTraceBridge {
    fn set_parent(&self, _: &tracing::Span, parent: Option<&TraceContext>) {
        self.parents.lock().unwrap().push(parent.cloned());
    }
    fn add_link(&self, span: &tracing::Span, origin: &TraceContext) {
        let id = span.id().expect("acceptance span must be enabled");
        assert!(
            !self.entered.lock().unwrap().contains(&id),
            "origin link added after span entry"
        );
        self.link_targets
            .lock()
            .unwrap()
            .push(span.metadata().unwrap().name().to_owned());
        self.links.lock().unwrap().push(origin.clone());
    }
    fn context(&self, _: &tracing::Span) -> Option<TraceContext> {
        None
    }
}
#[tokio::test]
async fn event_origin_is_linked_without_replacing_transport_parent_or_event_payload() {
    let mock = Arc::new(Mock::default());
    let command = command();
    mock.set("wf_event", Ok(receipt(&command)));
    let bridge = Arc::new(EventTraceBridge::default());
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    // Keep both traced and untraced dispatchers registered. Otherwise tracing's
    // single-dispatch fast path may let a concurrent untraced HTTP test cache
    // `never` when it first encounters the shared server callsite. This test
    // uses a scoped subscriber and must not mutate the process global default.
    let _untraced = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(EnteredSpans(bridge.entered.clone())),
    );
    let router = server::router_with_workflows(
        mock.clone(),
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        bridge.clone(),
    )
    .layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let dispatch = dispatch.clone();
            async move { next.run(request).with_subscriber(dispatch).await }
        },
    ));
    let running = start(router).await;
    let transport = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
    let response = reqwest::Client::new()
        .post(format!("{}/v1/workflows/events", running.url))
        .header("traceparent", transport)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&command).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        bridge.parents.lock().unwrap()[0]
            .as_ref()
            .unwrap()
            .traceparent,
        transport
    );
    let links = bridge.links.lock().unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(
        *bridge.link_targets.lock().unwrap(),
        ["ledgence.workflow.event.accept"]
    );
    assert_eq!(
        links[0].traceparent,
        command.event.value()["traceparent"].as_str().unwrap()
    );
    assert_eq!(links[0].tracestate.as_deref(), Some("origin=original"));
    assert_eq!(
        mock.calls.lock().unwrap()[0].1["event"],
        *command.event.value()
    );
}

#[tokio::test]
async fn malformed_event_receipts_remain_uncertain_after_acceptance() {
    let mock = Arc::new(Mock::default());
    let command = command();
    let running = setup(&mock).await;
    let mut invalid = receipt(&command);
    invalid.accepted_at = u64::MAX;
    mock.set("wf_event", Ok(invalid));
    let response = reqwest::Client::new()
        .post(format!("{}/v1/workflows/events", running.url))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&command).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    for missing in ["accepted_at", "already_accepted", "event_source"] {
        let mut value = serde_json::to_value(receipt(&command)).unwrap();
        value.as_object_mut().unwrap().remove(missing);
        let peer = start(axum::Router::new().route(
            "/v1/workflows/events",
            axum::routing::post(move || async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    serde_json::to_vec(&value).unwrap(),
                )
            }),
        ))
        .await;
        assert!(
            matches!(
                HttpTaskService::new(&peer.url)
                    .unwrap()
                    .send_workflow_event(&command)
                    .await,
                Err(ContractError::Unavailable(_))
            ),
            "client accepted absent {missing}"
        );
    }
}

struct EnteredSpans(Arc<Mutex<Vec<tracing::span::Id>>>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EnteredSpans {
    fn on_enter(&self, id: &tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        self.0.lock().unwrap().push(id.clone());
    }
}
