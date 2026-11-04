use super::*;
impl AdmissionOperations for Mock {
    fn supports_resource_sources(&self) -> bool {
        self.resource_sources_enabled.load(Ordering::SeqCst)
    }
    fn list_resource_sources<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        q: &'a ResourceSourceQuery,
    ) -> ContractFuture<'a, ResourceSourcePage> {
        Box::pin(async move { self.reply("resource_sources", q) })
    }

    fn supports_resource_fair_sharing(&self) -> bool {
        self.fair_sharing_enabled.load(Ordering::SeqCst)
    }
    fn list_resource_decisions<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ResourceDecisionQuery,
    ) -> ContractFuture<'a, ResourceDecisionPage> {
        Box::pin(async move { self.reply("resource_decisions", c) })
    }
    fn put_resource<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt> {
        Box::pin(async move { self.reply("resource_put", c) })
    }
    fn resource<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ResourceQuery,
    ) -> ContractFuture<'a, ResourceSnapshot> {
        Box::pin(async move { self.reply("resource_query", c) })
    }
    fn list_resources<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ResourceListQuery,
    ) -> ContractFuture<'a, ResourcePage> {
        Box::pin(async move { self.reply("resource_list", c) })
    }
    fn start_admission<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt> {
        Box::pin(async move { self.reply("admission_start", c) })
    }
    fn finish_admission<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt> {
        Box::pin(async move { self.reply("admission_finish", c) })
    }
    fn inspect_admission<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a AdmissionQuery,
    ) -> ContractFuture<'a, AdmissionSnapshot> {
        Box::pin(async move { self.reply("admission_inspect", c) })
    }
    fn release_admission<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a AdmissionReleaseCommand,
    ) -> ContractFuture<'a, AdmissionReleaseReceipt> {
        Box::pin(async move { self.reply("admission_release", c) })
    }
}
#[tokio::test]
async fn resource_sources_require_sensitive_response_audit_even_on_an_empty_page() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    let q = ResourceSourceQuery {
        scope: scope(),
        name: "model".into(),
        state: None,
        cursor: None,
        limit: 5,
    };
    let body = json!(&q);
    let path = "/v1/resources/sources";
    assert_eq!(
        request(&f, path, Some(body.clone()), "source-investigator")
            .await
            .0,
        503
    );
    f.mock
        .resource_sources_enabled
        .store(true, Ordering::SeqCst);
    let page = ResourceSourcePage {
        items: vec![],
        next_cursor: None,
    };
    f.mock.set("resource_sources", Ok(&page));
    for principal in ["reader", "resource-operator", "worker"] {
        assert_eq!(
            request(&f, path, Some(body.clone()), principal).await.0,
            403
        );
    }
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("source-investigator")
        .unwrap();
    assert_eq!(client.list_resource_sources(&q).await.unwrap(), page);
    let audit = f
        .auditor
        .observations
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(audit.read, AuditReadKind::ResourceSources);
    assert_eq!(
        audit.resource,
        AuditResource::Resource {
            name: q.name.clone()
        }
    );
    let mut foreign = body.clone();
    foreign["scope"]["namespace"] = json!("foreign");
    assert_eq!(
        request(&f, path, Some(foreign), "source-investigator")
            .await
            .0,
        404
    );
    let source = ResourceSource {
        scope: q.scope.clone(),
        resource: q.name.clone(),
        sequence: 1,
        revision: 1,
        updated_at: 100,
        state: ResourceSourceState::UnknownOccupied,
        subject: ResourceSourceSubject::Task {
            task_id: "private-occurrence".into(),
            generation: 1,
        },
    };
    f.mock.set(
        "resource_sources",
        Ok(ResourceSourcePage {
            items: vec![source],
            next_cursor: None,
        }),
    );
    f.auditor.mode.store(1, Ordering::SeqCst);
    let (status, error) = request(&f, path, Some(body), "source-investigator").await;
    assert_eq!(status, 503);
    assert!(!error.to_string().contains("private-occurrence"));
}
fn occurrence() -> AdmissionOccurrence {
    AdmissionOccurrence {
        scope: scope(),
        workflow_id: "workflow".into(),
        origin_activation_id: "origin".into(),
        key: "model".into(),
    }
}
fn binding() -> AdmissionBinding {
    AdmissionBinding::Operation(OperationAdmissionBinding {
        resource: "shared-model".into(),
        program: descriptor(),
        operation: AdmissionOperation {
            callable: "program:invoke".into(),
            kind: AdmissionOperationKind::Model,
            version: "1".into(),
            arguments: json!({"text":CANARY}),
        },
    })
}
pub(super) fn start() -> AdmissionStartCommand {
    AdmissionStartCommand {
        owner: owner(),
        occurrence: occurrence(),
        offer_generation: 1,
        start_id: "start".into(),
        finish_id: "finish".into(),
    }
}
pub(super) fn source() -> AdmissionSource {
    let c = start();
    AdmissionSource {
        occurrence: c.occurrence,
        source_id: "source".into(),
        start_id: c.start_id,
        finish_id: c.finish_id,
        generation: c.offer_generation,
        binding_digest: binding().digest().unwrap(),
        owner: c.owner,
        worker: AdmissionWorkerIdentity {
            audience: AccessAudience {
                authority_id: "authority".into(),
                instance_id: "audit".into(),
                scope: scope(),
            },
            issuer: "urn:ledgence:authority:authority".into(),
            subject: "worker".into(),
            credential_id: "credential".into(),
        },
        accepted_at: 20,
        start_deadline_at: 1020,
    }
}
fn snapshot() -> AdmissionSnapshot {
    AdmissionSnapshot {
        budget_denial: None,
        occurrence: occurrence(),
        binding: binding(),
        state: AdmissionState::UnknownOccupied,
        revision: 3,
        created_at: 1,
        updated_at: 21,
        deadline_at: 60000,
        offer: None,
        source: Some(source()),
        finish: None,
        provider: None,
    }
}
#[tokio::test]
async fn generic_feature_needs_its_complete_port_but_not_effect_support() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    assert_eq!(f.mock.evidence_capabilities.load(Ordering::SeqCst), 0);
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    let access = client.discover_worker_runtime(&scope()).await.unwrap();
    assert!(access.runtime_features.resource_admission());
    assert!(access.effect_evidence.is_none());
    assert!(access.authenticated_binding.is_some());
    f.mock.admission_enabled.store(false, Ordering::SeqCst);
    let access = client.discover_worker_runtime(&scope()).await.unwrap();
    assert!(access.runtime_features.supported.is_empty());
}
#[tokio::test]
async fn admission_auth_precedes_decode_and_scope_precedes_backend() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    for (path, token) in [
        ("/v1/admissions/start", "operator"),
        ("/v1/admissions/finish", "resource-operator"),
        ("/v1/resources/put", "worker"),
        ("/v1/admissions/release", "reader"),
        ("/v1/admissions/inspect", "reader"),
    ] {
        let r = reqwest::Client::new()
            .post(format!("{}{path}", f.running.url))
            .bearer_auth(token)
            .body("bad JSON")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 403);
    }
    assert!(f.mock.calls.lock().unwrap().is_empty());
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    let mut c = start();
    c.owner.scope.namespace = "foreign".into();
    c.occurrence.scope = c.owner.scope.clone();
    assert_eq!(
        AdmissionClient::start_admission(&client, &c)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn start_and_finish_check_original_source_and_never_retry_http() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    f.mock.set(
        "admission_start",
        Ok(AdmissionStartReceipt::Granted {
            source: Box::new(source()),
        }),
    );
    assert!(matches!(
        AdmissionClient::start_admission(&client, &start())
            .await
            .unwrap(),
        AdmissionStartReceipt::Granted { .. }
    ));
    let mut changed = source();
    changed.worker.subject = "other-worker".into();
    f.mock.set(
        "admission_start",
        Ok(AdmissionStartReceipt::Granted {
            source: Box::new(changed),
        }),
    );
    assert!(matches!(
        AdmissionClient::start_admission(&client, &start()).await,
        Err(ContractError::Unavailable(_))
    ));
    let c = AdmissionFinishCommand {
        usage: None,
        source: source(),
        output: json!({"answer":42}),
    };
    f.mock.set(
        "admission_finish",
        Ok(AdmissionFinishReceipt {
            usage: None,
            source: source(),
            output: c.output.clone(),
            accepted_at: 22,
            already_accepted: false,
        }),
    );
    AdmissionClient::finish_admission(&client, &c)
        .await
        .unwrap();
    let mut changed = source();
    changed.finish_id = "another-finish".into();
    f.mock.set(
        "admission_finish",
        Ok(AdmissionFinishReceipt {
            usage: None,
            source: changed,
            output: c.output.clone(),
            accepted_at: 22,
            already_accepted: false,
        }),
    );
    assert!(matches!(
        AdmissionClient::finish_admission(&client, &c).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        f.mock.calls.lock().unwrap().len(),
        4,
        "one exchange per call, including uncertain outcomes"
    );
}
#[tokio::test]
async fn sensitive_admission_payload_is_withheld_when_audit_cannot_commit() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    f.mock.set("admission_inspect", Ok(snapshot()));
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("operator")
        .unwrap();
    let q = AdmissionQuery {
        occurrence: occurrence(),
    };
    let r = AdmissionClient::inspect_admission(&client, &q)
        .await
        .unwrap();
    assert_eq!(r.binding.operation().unwrap().arguments["text"], CANARY);
    {
        let observations = f.auditor.observations.lock().unwrap();
        assert_eq!(
            observations.last().unwrap().read,
            AuditReadKind::AdmissionInspect
        );
    }
    f.auditor.mode.store(1, Ordering::SeqCst);
    let response = reqwest::Client::new()
        .post(format!("{}/v1/admissions/inspect", f.running.url))
        .bearer_auth("operator")
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&q).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 503);
    assert!(!response.text().await.unwrap().contains(CANARY));
}

#[tokio::test]
async fn console_admission_inspection_binds_instance_scope_and_audits_sensitive_read() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    f.mock.set("admission_inspect", Ok(snapshot()));
    let path = format!("{}/v1/console/admissions/inspect", f.running.url);
    let body = json!({"workflow_id":"workflow","origin_activation_id":"origin","key":"model"});
    let response = reqwest::Client::new()
        .post(&path)
        .bearer_auth("operator")
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let value: AdmissionSnapshot =
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value.occurrence, occurrence());
    assert_eq!(
        f.auditor.observations.lock().unwrap().last().unwrap().read,
        AuditReadKind::AdmissionInspect
    );
    let mut foreign = body.clone();
    foreign["scope"] = json!({"tenant_id":"foreign","namespace":"foreign"});
    let response = reqwest::Client::new()
        .post(&path)
        .bearer_auth("operator")
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&foreign).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(f.mock.calls.lock().unwrap().len(), 1);
    f.auditor.mode.store(1, Ordering::SeqCst);
    let response = reqwest::Client::new()
        .post(&path)
        .bearer_auth("operator")
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&body).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 503);
    assert!(!response.text().await.unwrap().contains(CANARY));
}

#[tokio::test]
async fn console_configuration_discloses_only_capability_appropriate_fields() {
    let f = setup().await;
    let path = "/v1/console/config";
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    let (status, unavailable) = request(&f, path, None, "source-investigator").await;
    assert_eq!(status, 200);
    assert_eq!(unavailable["capabilities"]["resource_sources"], false);
    assert_eq!(unavailable["capabilities"]["task_admission"], false);
    f.mock
        .resource_sources_enabled
        .store(true, Ordering::SeqCst);
    let (status, restricted) = request(&f, path, None, "source-investigator").await;
    assert_eq!(status, 200);
    assert_eq!(restricted["instance_name"], "Ledgence");
    assert_eq!(restricted["server_version"], "undisclosed");
    assert_eq!(restricted["suggested_queues"], json!([]));
    assert_eq!(restricted["instance_id"], "audit");
    assert!(!restricted.to_string().contains("private-operator-queue"));
    for (key, value) in restricted["capabilities"].as_object().unwrap() {
        assert_eq!(value, &(key == "resource_sources"), "{key}");
    }
    for token in ["reader", "operator"] {
        let (status, full) = request(&f, path, None, token).await;
        assert_eq!(status, 200);
        assert_eq!(full["server_version"], "test");
        assert_eq!(full["instance_name"], "Audit");
        assert_eq!(full["suggested_queues"], json!(["private-operator-queue"]));
        assert_eq!(full["capabilities"]["resource_policies"], true);
    }
    for token in ["resource-operator", "worker"] {
        assert_eq!(request(&f, path, None, token).await.0, 403);
    }
}

#[tokio::test]
async fn console_resource_policy_uses_installation_scope_and_independent_capabilities() {
    let f = setup().await;
    let config = || request(&f, "/v1/console/config", None, "reader");
    assert_eq!(config().await.1["capabilities"]["resource_policies"], false);
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    let capabilities = config().await.1["capabilities"].clone();
    assert_eq!(capabilities["resource_policies"], true);
    assert_eq!(capabilities["resource_fair_sharing"], false);
    assert_eq!(capabilities["resource_sources"], false);
    let path = "/v1/console/resources/put";
    let mut body = json!({"name":"quota","request_id":"create","expected_revision":null,
        "reason":"Bound concurrent calls","policy":{"enabled":true,"max_occupancy":1,
        "rate":null,"offer_timeout_ms":30000,"start_timeout_ms":5000}});
    assert_eq!(request(&f, path, Some(body.clone()), "reader").await.0, 403);
    let mut scoped = body.clone();
    scoped["scope"] = json!(scope());
    assert_eq!(
        request(&f, path, Some(scoped.clone()), "resource-operator")
            .await
            .0,
        400
    );
    let command: ResourcePutCommand = serde_json::from_value(scoped.clone()).unwrap();
    let receipt = ResourcePutReceipt {
        already_accepted: false,
        resource: ResourceRecord {
            scope: scope(),
            name: command.name,
            revision: 1,
            policy: command.policy,
            updated_at: 10,
        },
    };
    f.mock.set("resource_put", Ok(&receipt));
    let (status, reply) = request(&f, path, Some(body.clone()), "resource-operator").await;
    assert_eq!(status, 200, "{reply}");
    assert_eq!(reply, json!(receipt));
    assert_eq!(f.mock.calls.lock().unwrap().last().unwrap().1, scoped);
    body["policy"]["fair_sharing"] =
        json!({"groups":[{"name":"all","weight":1,"queues":[]}],"default_group":"all"});
    assert_eq!(
        request(&f, path, Some(body), "resource-operator").await.0,
        503
    );
}

#[tokio::test]
async fn fair_resource_history_requires_capability_metadata_access_and_bound_scope() {
    let f = setup().await;
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    let config = || request(&f, "/v1/console/config", None, "reader");
    assert_eq!(
        config().await.1["capabilities"]["resource_fair_sharing"],
        false
    );
    let console_path = "/v1/console/resources/decisions";
    let body = json!({"name":"model.requests","before":null,"limit":1});
    assert_eq!(
        request(&f, console_path, Some(body.clone()), "reader")
            .await
            .0,
        503
    );
    f.mock.fair_sharing_enabled.store(true, Ordering::SeqCst);
    assert_eq!(
        config().await.1["capabilities"]["resource_fair_sharing"],
        true
    );
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/console-v14.json"
    ))
    .unwrap();
    let mut page: ResourceDecisionPage =
        serde_json::from_value(fixture["resource_decision_page"].clone()).unwrap();
    page.items[0].scope = scope();
    f.mock.set("resource_decisions", Ok(&page));
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("reader")
        .unwrap();
    let mut q = ResourceDecisionQuery {
        scope: scope(),
        name: "model.requests".into(),
        before: None,
        limit: 1,
    };
    assert_eq!(
        AdmissionClient::list_resource_decisions(&client, &q)
            .await
            .unwrap(),
        page
    );
    let (status, result) = request(&f, console_path, Some(body.clone()), "reader").await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(
        serde_json::from_value::<ResourceDecisionPage>(result).unwrap(),
        page
    );
    let mut resource: ResourceSnapshot =
        serde_json::from_value(fixture["fair_resource_snapshot"].clone()).unwrap();
    resource.resource.scope = scope();
    let resources = ResourcePage {
        items: vec![resource.clone()],
        next: None,
    };
    f.mock.set("resource_query", Ok(&resource));
    f.mock.set("resource_list", Ok(&resources));
    for (path, body, reply) in [
        (
            "/v1/console/resources/inspect",
            json!({"name":"model.requests"}),
            json!(&resource),
        ),
        (
            "/v1/console/resources/list",
            json!({"after":null,"limit":1}),
            json!(&resources),
        ),
    ] {
        let (status, data) = request(&f, path, Some(body.clone()), "reader").await;
        assert_eq!(status, 200, "{data}");
        assert_eq!(data, reply);
        let mut override_scope = body;
        override_scope["scope"] = json!(scope());
        assert_eq!(
            request(&f, path, Some(override_scope), "reader").await.0,
            400
        );
    }
    let calls = f.mock.calls.lock().unwrap().len();
    let mut override_scope = body.clone();
    override_scope["scope"] = json!(scope());
    assert_eq!(
        request(&f, console_path, Some(override_scope), "reader")
            .await
            .0,
        400
    );
    assert_eq!(
        request(&f, console_path, Some(body), "resource-operator")
            .await
            .0,
        403
    );
    q.scope.namespace = "foreign".into();
    assert_eq!(
        AdmissionClient::list_resource_decisions(&client, &q)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(f.mock.calls.lock().unwrap().len(), calls);
    q.scope = scope();
    // A misbehaving adapter may not return a decision from a different resource.
    page.items[0].resource = "elsewhere".into();
    f.mock.set("resource_decisions", Ok(page));
    assert!(matches!(
        AdmissionClient::list_resource_decisions(&client, &q).await,
        Err(ContractError::Unavailable(_))
    ));
}

impl RunBudgetOperations for Mock {
    fn run_budget<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        q: &'a RunBudgetQuery,
    ) -> ContractFuture<'a, RunBudgetSnapshot> {
        Box::pin(async move { self.reply("budget_snapshot", q) })
    }
    fn list_run_budget_entries<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        q: &'a RunBudgetEntriesQuery,
    ) -> ContractFuture<'a, RunBudgetPage> {
        Box::pin(async move { self.reply("budget_entries", q) })
    }
}
#[tokio::test]
async fn budget_inspection_requires_capability_and_preserves_authoritative_root_and_scope() {
    let f = setup().await;
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../ledgence-orchestration-api/tests/fixtures/console-v14.json"
    ))
    .unwrap();
    let mut snap: RunBudgetSnapshot =
        serde_json::from_value(fixture["run_budget"].clone()).unwrap();
    snap.scope = scope();
    let q = RunBudgetQuery {
        scope: scope(),
        subject: snap.subject.clone(),
    };
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("reader")
        .unwrap();
    assert!(matches!(
        client.run_budget(&q).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        request(&f, "/v1/console/config", None, "reader").await.1["capabilities"]["run_budgets"],
        false
    );
    f.mock.run_budgets_enabled.store(true, Ordering::SeqCst);
    f.mock.set("budget_snapshot", Ok(&snap));
    assert_eq!(client.run_budget(&q).await.unwrap(), snap);
    let path = "/v1/console/budgets/inspect";
    let body = json!({"subject":q.subject});
    assert_eq!(
        request(&f, path, Some(body.clone()), "reader").await,
        (200, serde_json::to_value(&snap).unwrap())
    );
    assert_eq!(
        request(&f, path, Some(body.clone()), "resource-operator")
            .await
            .0,
        403
    );
    assert_eq!(
        request(
            &f,
            path,
            Some(json!({"scope":scope(),"subject":q.subject})),
            "reader"
        )
        .await
        .0,
        400
    );
    let mut page: RunBudgetPage =
        serde_json::from_value(fixture["run_budget_entries"].clone()).unwrap();
    page.scope = scope();
    page.items[0].scope = scope();
    page.items[0].approval.as_mut().unwrap().scope = scope();
    f.mock.set("budget_entries", Ok(&page));
    let entries = RunBudgetEntriesQuery {
        scope: scope(),
        subject: q.subject.clone(),
        before: None,
        limit: 1,
    };
    assert_eq!(
        client.list_run_budget_entries(&entries).await.unwrap(),
        page
    );
    assert_eq!(
        request(
            &f,
            "/v1/console/budgets/entries",
            Some(json!({"subject":q.subject,"before":null,"limit":1})),
            "reader"
        )
        .await,
        (200, serde_json::to_value(&page).unwrap())
    );
    let mut other = entries.clone();
    other.scope.namespace = "other".into();
    assert_eq!(
        client.list_run_budget_entries(&other).await.unwrap_err(),
        ContractError::NotFound
    );
    page.root = RunBudgetSubject::Workflow {
        workflow_id: "foreign".into(),
    };
    f.mock.set("budget_entries", Ok(&page));
    assert!(matches!(
        client.list_run_budget_entries(&entries).await,
        Err(ContractError::Unavailable(_))
    ));
}

#[tokio::test]
async fn a_predecessor_worker_receives_terminal_refusal_and_cannot_receive_a_fenced_dispatch() {
    let f = setup().await;
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    let mut command = ApprovedEffectClaimCommand {
        budget_fence: false,
        owner: owner(),
        workflow_id: "wf".into(),
        approval_key: "review".into(),
        action: ApprovalAction {
            name: "transfer".into(),
            version: "1".into(),
            arguments: json!({}),
        },
        lookup: "lookup".into(),
    };
    let current = client.clone().with_budget_dispatch_fence();
    for reason in [RunBudgetDenial::CallsExhausted, RunBudgetDenial::Cancelled] {
        f.mock.set::<ApprovedEffectClaimReceipt>(
            "effect_claim",
            Err(ContractError::RunBudgetDenied(reason)),
        );
        command.budget_fence = false;
        assert_eq!(
            client.claim_approved_effect(&command).await.unwrap_err(),
            ContractError::InvalidInput(reason.code().into())
        );
        command.budget_fence = true;
        assert_eq!(
            current.claim_approved_effect(&command).await.unwrap_err(),
            ContractError::RunBudgetDenied(reason)
        );
    }
    let reply = ApprovedEffectClaimReceipt {
        budget_grant: Some(RunBudgetGrant::new(100, 99999).unwrap()),
        scope: command.owner.scope.clone(),
        workflow_id: command.workflow_id.clone(),
        activation_id: command.owner.task_id.clone(),
        attempt_id: command.owner.attempt_id.clone(),
        approval_key: command.approval_key.clone(),
        action: command.action.clone(),
        lookup: command.lookup.clone(),
        disposition: ApprovedEffectDisposition::Dispatch,
    };
    assert_eq!(reply.budget_grant.unwrap().expires_at, 5100);
    f.mock.set("effect_claim", Ok(&reply));
    assert!(
        current
            .claim_approved_effect(&command)
            .await
            .unwrap()
            .budget_grant
            .is_some()
    );
    command.budget_fence = false;
    assert!(matches!(
        client.claim_approved_effect(&command).await,
        Err(ContractError::Unavailable(_))
    ));
    let raw = serde_json::to_value(&command).unwrap();
    assert!(raw.get("budget_fence").is_none());
}

#[tokio::test]
async fn workflow_only_budget_support_does_not_advertise_or_accept_standalone_task_budgets() {
    let f = setup().await;
    f.mock.run_budgets_enabled.store(true, Ordering::SeqCst);
    let caps = request(&f, "/v1/console/config", None, "reader").await.1["capabilities"].clone();
    assert_eq!(caps["run_budgets"], true);
    assert_eq!(caps["task_run_budgets"], false);
    let mut command = submit(Value::Null);
    command.input.budget = Some(RunBudgetPolicy {
        max_calls: Some(2),
        max_delegations: None,
        deadline_at_ms: None,
    });
    let mut accepted = workflow::snapshot();
    accepted.correlation_key = command.input.correlation_key.clone();
    f.mock.set("wf_submit", Ok(accepted));
    let data = serde_json::to_value(&command).unwrap();
    let (status, response) = request(&f, "/v1/workflows", Some(data), "submitter").await;
    assert_eq!(status, 200, "{response}");
    command.input.admission = Some(TaskAdmissionRequirement {
        resource: "provider".into(),
    });
    let raw = serde_json::to_value(&command).unwrap();
    let (status, response) = request(&f, "/v1/tasks", Some(raw), "submitter").await;
    assert_eq!(status, 503, "{response}");
    assert!(
        !f.mock
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(name, _)| *name == "submit")
    );
}
