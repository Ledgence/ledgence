use super::*;
impl AdmissionOperations for Mock {
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
fn occurrence() -> AdmissionOccurrence {
    AdmissionOccurrence {
        scope: scope(),
        workflow_id: "workflow".into(),
        origin_activation_id: "origin".into(),
        key: "model".into(),
    }
}
fn binding() -> AdmissionBinding {
    AdmissionBinding {
        resource: "shared-model".into(),
        program: descriptor(),
        operation: AdmissionOperation {
            callable: "program:invoke".into(),
            kind: AdmissionOperationKind::Model,
            version: "1".into(),
            arguments: json!({"text":CANARY}),
        },
    }
}
fn start() -> AdmissionStartCommand {
    AdmissionStartCommand {
        owner: owner(),
        occurrence: occurrence(),
        offer_generation: 1,
        start_id: "start".into(),
        finish_id: "finish".into(),
    }
}
fn source() -> AdmissionSource {
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
        source: source(),
        output: json!({"answer":42}),
    };
    f.mock.set(
        "admission_finish",
        Ok(AdmissionFinishReceipt {
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
    assert_eq!(r.binding.operation.arguments["text"], CANARY);
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
