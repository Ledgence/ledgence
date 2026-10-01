//! Console's browser boundary is additive to the legacy client contracts.
use super::*;
use ledgence_orchestration_api::console::*;

#[derive(Default)]
struct Queries {
    calls: Mutex<Vec<ConsoleQuery>>,
    reply: Mutex<Option<ConsoleQueryReply>>,
}
impl ConsoleQueryService for Queries {
    fn query_console<'a>(
        &'a self,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(query.clone());
            if let Some(reply) = self.reply.lock().unwrap().clone() {
                return Ok(reply);
            }
            match query {
                ConsoleQuery::Tasks { .. } => Ok(ConsoleQueryReply::Tasks(ConsolePage {
                    items: vec![],
                    next_cursor: None,
                    observed_at: 100,
                })),
                _ => Err(ContractError::NotFound),
            }
        })
    }
}
#[derive(Default)]
struct Catalog {
    calls: Mutex<Vec<ProgramCatalogQuery>>,
    reply: Mutex<Option<ProgramCatalogReply>>,
}
impl ProgramCatalogService for Catalog {
    fn register_program<'a>(
        &'a self,
        _: &'a RegisterProgram,
    ) -> ContractFuture<'a, RegisterProgramReply> {
        Box::pin(async { panic!("catalog read must not register a program") })
    }
    fn query_programs<'a>(
        &'a self,
        query: &'a ProgramCatalogQuery,
    ) -> ContractFuture<'a, ProgramCatalogReply> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(query.clone());
            self.reply
                .lock()
                .unwrap()
                .clone()
                .ok_or(ContractError::NotFound)
        })
    }
}
fn config() -> SelfHostedInstanceConfig {
    SelfHostedInstanceConfig {
        instance_id: "local-console".into(),
        name: "Local Console".into(),
        scope: scope(),
        suggested_queues: vec!["queue".into()],
        allowed_origins: vec!["https://console.example".into()],
    }
}
async fn setup(mock: &Arc<Mock>, queries: &Arc<Queries>) -> Running {
    setup_catalog(mock, queries, None).await
}
async fn setup_catalog(
    mock: &Arc<Mock>,
    queries: &Arc<Queries>,
    catalog: Option<Arc<dyn ProgramCatalogService>>,
) -> Running {
    let console = server::console::ConsoleServices::new(
        config(),
        "0.1.1".into(),
        queries.clone(),
        catalog,
        None,
    )
    .unwrap();
    start(
        server::router_with_console(
            mock.clone(),
            mock.clone(),
            mock.clone(),
            console,
            Arc::new(AtomicBool::new(false)),
            Arc::new(ledgence_worker_api::NoopTraceBridge),
        )
        .unwrap(),
    )
    .await
}
fn query_url(running: &Running, path: &str, parameters: &[(&str, &str)]) -> reqwest::Url {
    let mut url = reqwest::Url::parse(&format!("{}{path}", running.url)).unwrap();
    url.query_pairs_mut()
        .extend_pairs(parameters.iter().copied());
    url
}
fn assert_headers(response: &reqwest::Response) {
    assert_eq!(response.headers()["ledgence-instance-id"], "local-console");
    assert_eq!(response.headers()["ledgence-console-contract"], "4");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert!(response.headers().contains_key("request-id"));
}
fn submission(data: Value) -> Value {
    let command = submit(data);
    json!({"idempotency_key":command.idempotency_key,"input":{
        "program":command.input.program,"queue":command.input.queue,
        "correlation_key":command.input.correlation_key,"data":command.input.data,
        "retry_policy":command.input.retry_policy,"attempt_timeout_ms":command.input.attempt_timeout_ms,
    }})
}
fn workflow_snapshot() -> WorkflowSnapshot {
    WorkflowSnapshot {
        workflow_id: "wf +/%é".into(),
        parent_workflow_id: None,
        root_workflow_id: None,
        scope: scope(),
        state: WorkflowState::Running,
        revision: 0,
        activation_id: Some("controller".into()),
        submitted_at: 10,
        terminal_at: None,
        correlation_key: Some("invoice:42".into()),
    }
}

#[tokio::test]
async fn explorer_http_preserves_canonical_c4_entrypoints_and_relationship_evidence() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/console-v4.json"
    ))
    .unwrap();
    let explorer: ConsoleWorkflowExplorer =
        serde_json::from_value(fixtures["explorer"].clone()).unwrap();
    let workflow_id = explorer.workflow.summary.workflow.workflow_id.clone();
    assert!(
        explorer
            .page
            .items
            .iter()
            .any(|node| { matches!(node.data, ConsoleExplorerData::Entrypoint { .. }) })
    );
    *queries.reply.lock().unwrap() = Some(ConsoleQueryReply::Explorer(explorer));
    let running = setup(&mock, &queries).await;
    let response = reqwest::Client::new()
        .get(query_url(
            &running,
            "/v1/console/workflows/explorer",
            &[("workflow_id", &workflow_id)],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_headers(&response);
    let actual: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(actual, fixtures["explorer"]);
    assert!(
        actual["page"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["kind"] != "phase")
    );
}

#[tokio::test]
async fn explorer_http_rejects_every_old_cursor_kind_before_querying_the_store() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let page = ConsolePagination::default();
    let mut old_binding = ConsoleQuery::Explorer {
        workflow_id: "wf_old".into(),
        page: page.clone(),
    }
    .binding(&scope())
    .unwrap();
    for endpoint in ["workflows/explorer", "workflows/explorer/v3"] {
        old_binding.endpoint = endpoint;
        for (kind, key) in [
            ("phase", "phase"),
            ("entrypoint", "entrypoint"),
            ("child", "child:0"),
            ("fork", "fork:0"),
            ("local", "local:0"),
            ("child_wait", "child_wait"),
            ("external_wait", "wait:0"),
        ] {
            let cursor = page
                .next_cursor(
                    &old_binding,
                    &vec![
                        ConsoleKey::Number(ConsoleU64(0)),
                        ConsoleKey::Text(kind.into()),
                        ConsoleKey::Text(key.into()),
                    ],
                )
                .unwrap();
            let response = reqwest::Client::new()
                .get(query_url(
                    &running,
                    "/v1/console/workflows/explorer",
                    &[("workflow_id", "wf_old"), ("cursor", &cursor)],
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "old {kind} cursor");
            assert_headers(&response);
        }
    }
    assert!(queries.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn explorer_http_rejects_missing_or_fabricated_relations_from_query_adapters() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/console-v4.json"
    ))
    .unwrap();
    let explorer: ConsoleWorkflowExplorer =
        serde_json::from_value(fixtures["explorer_cases"]["mixed_local"].clone()).unwrap();
    for fabricate in [false, true] {
        let mut invalid = explorer.clone();
        let local = invalid
            .page
            .items
            .iter_mut()
            .find(|node| matches!(node.data, ConsoleExplorerData::Local { .. }))
            .unwrap();
        if fabricate {
            local.relations[0].kind = ConsoleExplorerRelationKind::AwaitsTerminal;
        } else {
            local.relations.clear();
        }
        *queries.reply.lock().unwrap() = Some(ConsoleQueryReply::Explorer(invalid));
        let response = reqwest::Client::new()
            .get(query_url(
                &running,
                "/v1/console/workflows/explorer",
                &[(
                    "workflow_id",
                    &explorer.workflow.summary.workflow.workflow_id,
                )],
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        assert_headers(&response);
    }
}

#[tokio::test]
async fn unified_execution_endpoint_decodes_filters_and_preserves_typed_identity() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/execution-discovery-v1.json"
    ))
    .unwrap();
    *queries.reply.lock().unwrap() = Some(ConsoleQueryReply::Executions(
        serde_json::from_value(fixtures["executions"].clone()).unwrap(),
    ));
    let running = setup(&mock, &queries).await;
    let response = reqwest::Client::new()
        .get(query_url(
            &running,
            "/v1/console/executions",
            &[
                ("execution_id", "shared-id"),
                ("program_id", "mixed-program"),
                ("submitted_from", "100"),
                ("submitted_until", "102"),
            ],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_headers(&response);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.bytes().await.unwrap()).unwrap(),
        fixtures["executions"]
    );
    let calls = queries.calls.lock().unwrap();
    let ConsoleQuery::Executions { filters, .. } = &calls[0] else {
        panic!()
    };
    assert!(filters.includes_children());
    assert!(!filters.include_children);
    assert_eq!(filters.execution_id.as_deref(), Some("shared-id"));
}

#[tokio::test]
async fn unified_execution_endpoint_rejects_incompatible_or_unknown_filters_before_service() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    for fields in [
        vec![("kind", "task"), ("state", "waiting")],
        vec![("version", "1")],
        vec![("include_children", "yes")],
        vec![("kind", "local")],
        vec![("tenant_id", "other")],
        vec![("submitted_from", "20"), ("submitted_until", "10")],
    ] {
        let response = reqwest::Client::new()
            .get(query_url(&running, "/v1/console/executions", &fields))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "{fields:?}");
    }
    assert!(queries.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn catalog_endpoint_filters_any_version_kind_and_binds_its_cursor() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let catalog = Arc::new(Catalog::default());
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/execution-discovery-v1.json"
    ))
    .unwrap();
    *catalog.reply.lock().unwrap() = Some(ProgramCatalogReply::Catalog(
        serde_json::from_value(fixture["catalog"].clone()).unwrap(),
    ));
    let running = setup_catalog(&mock, &queries, Some(catalog.clone())).await;
    let client = reqwest::Client::new();
    // The fixture's editable summary says task, but registered versions include
    // a workflow. The endpoint must validate membership, not summary metadata.
    let response = client
        .get(query_url(
            &running,
            "/v1/console/programs/catalog",
            &[("kind", "workflow")],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_headers(&response);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.bytes().await.unwrap()).unwrap(),
        fixture["catalog"]
    );
    let binding = ProgramCatalogQuery::Catalog {
        kind: Some(ConsoleProgramKind::Task),
        page: Default::default(),
    }
    .binding(&scope())
    .unwrap();
    let cursor = ConsolePagination::default()
        .next_cursor(&binding, &vec![ConsoleKey::Text("mixed-program".into())])
        .unwrap();
    for fields in [
        vec![("kind", "mixed")],
        vec![("kind", "workflow"), ("cursor", cursor.as_str())],
        vec![("tenant_id", "other")],
    ] {
        let response = client
            .get(query_url(&running, "/v1/console/programs/catalog", &fields))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "{fields:?}");
    }
    let calls = catalog.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(matches!(
        calls[0],
        ProgramCatalogQuery::Catalog {
            kind: Some(ConsoleProgramKind::Workflow),
            ..
        }
    ));
    assert!(queries.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn catalog_endpoint_rejects_a_store_reply_outside_the_requested_kind() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let catalog = Arc::new(Catalog::default());
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/execution-discovery-v1.json"
    ))
    .unwrap();
    *catalog.reply.lock().unwrap() = Some(ProgramCatalogReply::Catalog(
        serde_json::from_value(fixture["catalog"].clone()).unwrap(),
    ));
    let running = setup_catalog(&mock, &queries, Some(catalog)).await;
    let response = reqwest::Client::new()
        .get(query_url(
            &running,
            "/v1/console/programs/catalog",
            &[("kind", "unspecified")],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert_headers(&response);
}

#[tokio::test]
async fn console_configuration_and_errors_identify_instance_without_exposing_scope() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/v1/console/config", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_headers(&response);
    let bytes = response.bytes().await.unwrap();
    let config: ConsoleConfig = decode_unique_json(&bytes, CONSOLE_METADATA_MAX_BYTES).unwrap();
    assert_eq!(config.instance_id, "local-console");
    assert_eq!(config.contract_version, 4);
    assert!(config.capabilities.executions);
    assert!(!config.capabilities.programs);
    assert!(!config.capabilities.workers);
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value.get("scope").is_none());
    assert!(value.get("tenant_id").is_none());
    assert!(value.get("namespace").is_none());
    let response = client
        .get(format!("{}/v1/console/config?scope=x", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert_headers(&response);
    assert!(mock.calls.lock().unwrap().is_empty());
    assert!(queries.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn console_rejects_duplicate_unknown_oversized_and_malformed_queries_before_services() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    for query in [
        "tenant_id=foreign",
        "namespace=foreign",
        "scope=x",
        "limit=1&limit=1",
        "limit=1&%6cimit=2",
        "limit=0",
        "limit=101",
        "limit=-1",
        "limit=1.0",
        "cursor=zz",
        "cursor=00",
        "state=unknown",
        "submitted_from=2&submitted_until=1",
        "queue=%xx",
        "queue=%00",
        "queue=%FF",
        "unknown=x",
        "queue",
        "queue=x&&limit=1",
    ] {
        let response = client
            .get(format!("{}/v1/console/tasks?{query}", running.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "{query}");
        assert_headers(&response);
    }
    let query = format!("correlation_key={}", "a".repeat(CONSOLE_QUERY_MAX_BYTES));
    let response = client
        .get(format!("{}/v1/console/tasks?{query}", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let response = client
        .get(format!("{}/v1/console/tasks", running.url))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(mock.calls.lock().unwrap().is_empty());
    assert!(queries.calls.lock().unwrap().is_empty());
    let response = client
        .get(query_url(
            &running,
            "/v1/console/tasks",
            &[
                ("correlation_key", ""),
                ("queue", "slash/+%é"),
                ("limit", "25"),
            ],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let calls = queries.calls.lock().unwrap();
    let ConsoleQuery::Tasks { filters, page } = &calls[0] else {
        panic!()
    };
    assert_eq!(filters.correlation_key, Some("".into()));
    assert_eq!(filters.queue, Some("slash/+%é".into()));
    assert_eq!(page.limit, 25);
}

#[tokio::test]
async fn console_rejects_unknown_duplicate_and_oversized_mutations_without_submission() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    let base = submission(Value::Null);
    let mut scope = base.clone();
    scope["scope"] = json!({"tenant_id":"foreign","namespace":"foreign"});
    let mut nested = base.clone();
    nested["input"]["tenant_id"] = json!("foreign");
    let mut missing = base.clone();
    missing["input"].as_object_mut().unwrap().remove("data");
    let bytes = String::from_utf8(exact(&base)).unwrap();
    let duplicate = bytes.replacen(
        "\"queue\":\"queue\"",
        "\"queue\":\"queue\",\"queue\":\"other\"",
        1,
    );
    assert_ne!(duplicate, bytes);
    for body in [
        exact(scope),
        exact(nested),
        exact(missing),
        duplicate.into_bytes(),
        br#"{"idempotency_key":"one","idempotency_key":"two","input":{}}"#.to_vec(),
    ] {
        let response = client
            .post(format!("{}/v1/console/tasks", running.url))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_headers(&response);
    }
    let response = client
        .post(format!("{}/v1/console/tasks", running.url))
        .header("content-type", "application/json")
        .body(vec![b' '; SUBMISSION_MAX_BYTES + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    let response = client
        .post(format!("{}/v1/console/tasks?queue=x", running.url))
        .header("content-type", "application/json")
        .body(exact(&base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let response = client
        .post(format!("{}/v1/console/tasks", running.url))
        .header("content-type", "text/plain")
        .body(exact(base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 415);
    assert!(mock.calls.lock().unwrap().is_empty());
    assert!(queries.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn console_commands_inject_fixed_scope_and_preserve_lossless_user_values() {
    let data=decode_unique_json::<Value>(br#"[18446744073709551615,-9223372036854775808,9007199254740993,1,1.0,-0.0,{"nul":"\u0000","tenant_id":"user-owned"}]"#,1000).unwrap();
    let mock = Arc::new(Mock::default());
    mock.set("submit", Ok(task(data.clone())));
    mock.set("wf_submit", Ok(workflow_snapshot()));
    mock.set("cancel", Ok(TaskState::Active));
    mock.set("wf_cancel", Ok(workflow_snapshot()));
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    for route in ["tasks", "workflows"] {
        let response = client
            .post(format!("{}/v1/console/{route}", running.url))
            .header("content-type", "application/json")
            .header("origin", "https://console.example")
            .body(exact(submission(data.clone())))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_headers(&response);
        let bytes = response.bytes().await.unwrap();
        if route == "tasks" {
            let detail: ConsoleTaskDetail = decode_unique_json(&bytes, RESPONSE_MAX_BYTES).unwrap();
            assert_eq!(exact(detail.input.data), exact(data.clone()));
            let text = String::from_utf8(bytes.to_vec()).unwrap();
            for token in [
                "18446744073709551615",
                "-9223372036854775808",
                "9007199254740993",
                "1.0",
                "-0.0",
            ] {
                assert!(text.contains(token));
            }
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(value["input"].get("tenant_id").is_none());
            assert!(value["input"].get("namespace").is_none());
        }
    }
    for (route, body) in [
        ("tasks/cancel", json!({"task_id":"task +/%é"})),
        (
            "workflows/cancel",
            json!({"workflow_id":workflow_snapshot().workflow_id}),
        ),
    ] {
        let response = client
            .post(format!("{}/v1/console/{route}", running.url))
            .header("content-type", "application/json")
            .body(exact(body))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls[0].0, "submit");
    assert_eq!(calls[1].0, "wf_submit");
    for (_, command) in &calls[..2] {
        assert_eq!(command["input"]["tenant_id"], scope().tenant_id);
        assert_eq!(command["input"]["namespace"], scope().namespace);
        assert_eq!(exact(&command["input"]["data"]), exact(data.clone()));
    }
    assert_eq!(calls[2].1, json!([scope(), "task +/%é"]));
    assert_eq!(
        calls[3].1,
        json!([scope(), workflow_snapshot().workflow_id])
    );
}

#[tokio::test]
async fn console_origin_guard_covers_legacy_mutations_and_leaves_headless_clients_usable() {
    let mock = Arc::new(Mock::default());
    mock.set("submit", Ok(task(Value::Null)));
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    for (path, body) in [
        ("/v1/console/tasks", exact(submission(Value::Null))),
        ("/v1/tasks", exact(submit(Value::Null))),
    ] {
        for origin in [
            "https://other.example",
            "null",
            "https://console.example/",
            "http://console.example",
        ] {
            let response = client
                .post(format!("{}{path}", running.url))
                .header("content-type", "application/json")
                .header("origin", origin)
                .body(body.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "{path} {origin}");
        }
        let response = client
            .post(format!("{}{path}", running.url))
            .header("content-type", "application/json")
            .header("origin", "https://console.example")
            .header("origin", "https://console.example")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }
    assert!(mock.calls.lock().unwrap().is_empty());
    for origin in [None, Some("https://console.example")] {
        let mut request = client
            .post(format!("{}/v1/tasks", running.url))
            .header("content-type", "application/json")
            .body(exact(submit(Value::Null)));
        if let Some(origin) = origin {
            request = request.header("origin", origin);
        }
        assert_eq!(request.send().await.unwrap().status(), 200);
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn console_results_distinguish_pending_and_successful_null_without_mutation() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    for (state, outcome) in [
        (TaskState::Active, None),
        (
            TaskState::Succeeded,
            Some(TaskOutcome::Succeeded {
                attempt_id: "attempt".into(),
                quiescence: Quiescence::Unconfirmed,
                execution_may_have_started: true,
                output: Value::Null,
            }),
        ),
    ] {
        mock.set(
            "result",
            Ok(TaskResult {
                task: compact_status(state),
                outcome: outcome.clone(),
            }),
        );
        let response = client
            .get(query_url(
                &running,
                "/v1/console/tasks/result",
                &[("task_id", "..")],
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let bytes = response.bytes().await.unwrap();
        let reply: ConsoleTaskResult = decode_unique_json(&bytes, RESPONSE_MAX_BYTES).unwrap();
        assert_eq!(exact(reply.outcome), exact(outcome));
        assert_eq!(reply.task.state, state);
    }
    assert!(
        mock.calls
            .lock()
            .unwrap()
            .iter()
            .all(|(operation, _)| *operation == "result")
    );
}

#[tokio::test]
async fn console_escaped_ids_are_exact_and_inconsistent_adapter_replies_are_rejected() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    for id in [
        ".",
        "..",
        " leading trailing ",
        "slash/plus+percent%é",
        "%2F",
    ] {
        let mut status = compact_status(TaskState::Queued);
        status.task_id = id.into();
        mock.set("status", Ok(status));
        let response = client
            .get(query_url(
                &running,
                "/v1/console/tasks/status",
                &[("task_id", id)],
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let reply: ConsoleObservedTaskStatus =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(reply.task.task_id, id);
        assert_eq!(
            mock.calls.lock().unwrap().last().unwrap().1,
            json!({"scope":scope(),"task_id":id})
        );
    }
    let mut wrong = compact_status(TaskState::Queued);
    wrong.scope.tenant_id = "foreign".into();
    mock.set("status", Ok(wrong));
    let response = client
        .get(format!(
            "{}/v1/console/tasks/status?task_id=..",
            running.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert_headers(&response);
    let mut wrong = task(Value::Null);
    wrong.task_id = "unexpected".into();
    mock.set("inspect", Ok(wrong));
    let response = client
        .get(format!(
            "{}/v1/console/tasks/inspect?task_id=..",
            running.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    *queries.reply.lock().unwrap() = Some(ConsoleQueryReply::Tasks(ConsolePage {
        items: vec![ConsoleTaskSummary {
            task: compact_status(TaskState::Queued).into(),
            descriptor: descriptor().into(),
        }],
        next_cursor: None,
        observed_at: 100,
    }));
    let response = client
        .get(format!("{}/v1/console/tasks?state=failed", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
}

#[tokio::test]
async fn console_workflow_events_keep_original_identity_and_metadata_counters_are_strings() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    let event=WorkflowEvent::new(json!({"specversion":"1.0","id":"evt +/%é","source":"urn:billing:approval","type":"billing.approved","datacontenttype":"application/json","data":{"amount":18446744073709551615_u64}})).unwrap();
    let id = workflow_snapshot().workflow_id;
    let key = "wait +/%é";
    let receipt = WorkflowEventReceipt {
        scope: scope(),
        workflow_id: id.clone(),
        key: key.into(),
        event_id: event.id().into(),
        event_source: event.source().into(),
        accepted_at: 100,
        already_accepted: false,
    };
    mock.set("wf_event", Ok(receipt.clone()));
    let command = json!({"workflow_id":id,"key":key,"event":event});
    let response = client
        .post(format!("{}/v1/console/workflows/events", running.url))
        .header("content-type", "application/json")
        .body(exact(&command))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_headers(&response);
    let received: ConsoleWorkflowEventReceipt =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(received.event_id, receipt.event_id);
    assert_eq!(received.event_source, receipt.event_source);
    assert_eq!(received.key, key);
    assert_eq!(
        mock.calls.lock().unwrap().last().unwrap().1,
        json!({"scope":scope(),"workflow_id":id,"key":key,"event":event})
    );
    let mut wrong = receipt;
    wrong.event_id = "other-event".into();
    mock.set("wf_event", Ok(wrong));
    let response = client
        .post(format!("{}/v1/console/workflows/events", running.url))
        .header("content-type", "application/json")
        .body(exact(command))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let response = client
        .post(format!("{}/v1/console/workflows/events", running.url))
        .header("content-type", "application/json")
        .body(vec![b' '; WORKFLOW_EVENT_COMMAND_MAX_BYTES + 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    let mut workflow = workflow_snapshot();
    workflow.revision = u64::MAX;
    mock.set("wf_status", Ok(workflow));
    let response = client
        .get(query_url(
            &running,
            "/v1/console/workflows/status",
            &[("workflow_id", &id)],
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let reply: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(reply["workflow"]["revision"], "18446744073709551615");
    assert!(reply["workflow"].get("scope").is_none());
}

#[tokio::test]
async fn console_rejects_inconsistent_task_detail_from_adapters() {
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let running = setup(&mock, &queries).await;
    let client = reqwest::Client::new();
    let mut valid = task(json!(null));
    valid.task_id = "task".into();
    let url = query_url(
        &running,
        "/v1/console/tasks/inspect",
        &[("task_id", "task")],
    );
    for defect in 0..10 {
        let mut reply = valid.clone();
        match defect {
            0 => reply.submitted_at = u64::MAX,
            1 => reply.available_at = u64::MAX,
            2 => reply.current_attempt_id = None,
            3 => reply.attempt_count = 0,
            4 => reply.attempt_count = 1001,
            5 => reply.terminal_at = Some(10),
            6 => reply.parent_workflow_id = Some("unpaired".into()),
            7 => reply.workflow_activation_id = Some("another".into()),
            8 => reply.idempotency_key = String::new(),
            _ => {
                reply.origin_trace = Some(TraceContext {
                    traceparent: "invalid".into(),
                    tracestate: None,
                })
            }
        }
        mock.set("inspect", Ok(reply));
        let response = client.get(url.clone()).send().await.unwrap();
        assert_eq!(response.status(), 503, "defect {defect}");
        assert_headers(&response);
    }
    // A completed attempt has no current attempt; do not invent or require its ID.
    valid.state = TaskState::Queued;
    valid.current_attempt_id = None;
    mock.set("inspect", Ok(valid));
    assert_eq!(client.get(url).send().await.unwrap().status(), 200);
}
