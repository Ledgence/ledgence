use super::*;
impl ProviderAdmissionOperations for Mock {
    fn start_provider_admission<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt> {
        Box::pin(async move { self.reply("provider_admission_start", c) })
    }
    fn report_provider_admission<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt> {
        Box::pin(async move { self.reply("provider_admission_report", c) })
    }
}
fn enable(f: &Fixture) {
    f.mock.evidence_capabilities.store(15, Ordering::SeqCst);
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    f.mock
        .provider_admission_enabled
        .store(true, Ordering::SeqCst);
}
fn start() -> ProviderAdmissionStartCommand {
    ProviderAdmissionStartCommand {
        admission: admission::start(),
    }
}
fn source() -> ProviderAdmissionSource {
    let admission = admission::source();
    let provider = ProviderDispatchSource {
        source_id: "provider-source".into(),
        claim_id: admission.start_id.clone(),
        report_id: admission.finish_id.clone(),
        ticket: ProviderOperationTicket {
            occurrence: EffectOccurrence {
                scope: scope(),
                workflow_id: admission.occurrence.workflow_id.clone(),
                activation_id: admission.occurrence.origin_activation_id.clone(),
                approval_key: "review".into(),
            },
            binding_digest: Digest(format!("sha256:{}", "a".repeat(64))),
        },
        worker: EffectWorkerIdentity {
            audience: admission.worker.audience.clone(),
            issuer: admission.worker.issuer.clone(),
            subject: admission.worker.subject.clone(),
            credential_id: admission.worker.credential_id.clone(),
        },
        accepted_at: admission.accepted_at,
        first_possible_dispatch_at: admission.accepted_at,
        reconciliation_deadline: 60000,
    };
    ProviderAdmissionSource {
        admission,
        provider,
    }
}
fn report() -> ProviderAdmissionReportCommand {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../sdk/python-client/tests/fixtures/provider-effect.json"
    ))
    .unwrap();
    let target = fixture["binding"]["approval"]["provider_binding"]["target"].clone();
    ProviderAdmissionReportCommand{source:source(),outcome:ProviderDispatchOutcome{call_completion:ProviderCallCompletion::Completed,report:serde_json::from_value(json!({"reported_target":target,"provider_reference":null,"provider_request_id":null,"knowledge":{"kind":"succeeded","result":{"text":"a".repeat(100000)}}})).unwrap()}}
}
fn receipt(c: &ProviderAdmissionReportCommand) -> ProviderAdmissionReportReceipt {
    let p = &c.source.provider;
    ProviderAdmissionReportReceipt {
        source: c.source.clone(),
        call_completion: c.outcome.call_completion,
        receipt: ProviderReportReceipt {
            ticket: p.ticket.clone(),
            source_id: p.source_id.clone(),
            source: ProviderSourceReference::Dispatch {
                claim_id: p.claim_id.clone(),
            },
            report_id: p.report_id.clone(),
            report_digest: c.outcome.report.digest().unwrap(),
            worker: p.worker.clone(),
            accepted_at: 30,
            recorded_revision: 2,
            accepted_via: ProviderReportIngress::EvidenceOnly,
            already_accepted: false,
        },
    }
}
#[tokio::test]
async fn composed_capability_requires_explicit_port_in_addition_to_both_individual_capabilities() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(15, Ordering::SeqCst);
    f.mock.admission_enabled.store(true, Ordering::SeqCst);
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    let features = client
        .discover_worker_runtime(&scope())
        .await
        .unwrap()
        .runtime_features;
    assert!(features.provider_operation() && features.resource_admission());
    assert!(!features.provider_admission());
    f.mock
        .provider_admission_enabled
        .store(true, Ordering::SeqCst);
    assert!(
        client
            .discover_worker_runtime(&scope())
            .await
            .unwrap()
            .runtime_features
            .provider_admission()
    );
    f.mock.admission_enabled.store(false, Ordering::SeqCst);
    assert!(
        !client
            .discover_worker_runtime(&scope())
            .await
            .unwrap()
            .runtime_features
            .provider_admission()
    );
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn composed_auth_and_scope_are_checked_before_backend_dispatch() {
    let f = setup().await;
    enable(&f);
    for path in [
        "/v1/provider-admissions/start",
        "/v1/provider-admissions/report",
    ] {
        let r = reqwest::Client::new()
            .post(format!("{}{path}", f.running.url))
            .bearer_auth("operator")
            .body("bad JSON")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status().as_u16(), 403);
    }
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    let mut command = start();
    command.admission.owner.scope.namespace = "foreign".into();
    command.admission.occurrence.scope = command.admission.owner.scope.clone();
    assert_eq!(
        ProviderAdmissionClient::start_provider_admission(&client, &command)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn composed_start_and_large_report_preserve_source_and_never_retry_http() {
    let f = setup().await;
    enable(&f);
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    f.mock.set(
        "provider_admission_start",
        Ok(ProviderAdmissionStartReceipt::Granted {
            source: Box::new(source()),
        }),
    );
    ProviderAdmissionClient::start_provider_admission(&client, &start())
        .await
        .unwrap();
    let command = report();
    assert!(serde_json::to_vec(&command).unwrap().len() > 65536);
    f.mock
        .set("provider_admission_report", Ok(receipt(&command)));
    let accepted = ProviderAdmissionClient::report_provider_admission(&client, &command)
        .await
        .unwrap();
    assert!(accepted.matches(&command).unwrap());
    f.mock.set::<ProviderAdmissionReportReceipt>(
        "provider_admission_report",
        Err(ContractError::Unavailable("lost receipt".into())),
    );
    assert!(
        ProviderAdmissionClient::report_provider_admission(&client, &command)
            .await
            .is_err()
    );
    let calls = f.mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[1].1, calls[2].1);
}
