use super::*;
impl ProviderOperations for Mock {
    fn claim_provider<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt> {
        Box::pin(async move { self.reply("provider_claim", c) })
    }
    fn report_dispatch<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move { self.reply("provider_dispatch", c) })
    }
    fn probe_context<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext> {
        Box::pin(async move { self.reply("provider_context", c) })
    }
    fn report_probe<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move { self.reply("provider_probe", c) })
    }
    fn observe_provider<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        c: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move { self.reply("provider_observe", c) })
    }
}
fn binding() -> ProviderBinding {
    let value: Value = serde_json::from_str(include_str!(
        "../../../../sdk/python-client/tests/fixtures/provider-effect.json"
    ))
    .unwrap();
    serde_json::from_value(value["binding"]["approval"]["provider_binding"].clone()).unwrap()
}
fn identity() -> EffectWorkerIdentity {
    EffectWorkerIdentity {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "audit".into(),
            scope: scope(),
        },
        issuer: "urn:ledgence:authority:authority".into(),
        subject: "worker".into(),
        credential_id: "credential".into(),
    }
}
fn claim() -> ProviderClaimCommand {
    let binding = binding();
    ProviderClaimCommand {
        owner: owner(),
        workflow_id: "workflow".into(),
        approval_key: "approval".into(),
        claim_id: "claim".into(),
        report_id: "report".into(),
        action: ApprovalAction {
            name: binding.contract.dispatch_callable.clone(),
            version: binding.contract.version.clone(),
            arguments: json!({"amount":1.0}),
        },
        binding,
    }
}
#[tokio::test]
async fn provider_routes_require_worker_before_body_decode_and_complete_backend() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(15, Ordering::SeqCst);
    for path in [
        "/v1/workflows/provider-operations/claim",
        "/v1/workflows/provider-operations/report",
        "/v1/workflows/provider-operations/observe",
        "/v1/provider-probes/context",
        "/v1/provider-probes/report",
    ] {
        for token in ["operator", "reader", "resolver"] {
            let response = reqwest::Client::new()
                .post(format!("{}{path}", f.running.url))
                .bearer_auth(token)
                .body("invalid")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 403, "{path}");
        }
        let response = reqwest::Client::new()
            .post(format!("{}{path}", f.running.url))
            .body("invalid")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);
    }
    assert!(f.mock.calls.lock().unwrap().is_empty());
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    for bits in [0, 7, 8, 11, 13, 14] {
        f.mock.evidence_capabilities.store(bits, Ordering::SeqCst);
        assert!(matches!(
            ProviderOperationClient::claim_provider(&client, &claim()).await,
            Err(ContractError::Unavailable(_))
        ));
    }
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn provider_claim_freezes_permission_and_rejects_changed_receipt_as_uncertain() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(15, Ordering::SeqCst);
    let command = claim();
    let ticket = command.ticket().unwrap();
    let source = ProviderDispatchSource {
        source_id: "source".into(),
        claim_id: command.claim_id.clone(),
        report_id: command.report_id.clone(),
        ticket: ticket.clone(),
        worker: identity(),
        accepted_at: 22,
        first_possible_dispatch_at: 22,
        reconciliation_deadline: 60_022,
    };
    f.mock.set(
        "provider_claim",
        Ok(ProviderClaimReceipt::Dispatch {
            ticket: ticket.clone(),
            source: Box::new(source.clone()),
        }),
    );
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("worker")
        .unwrap();
    assert!(matches!(
        ProviderOperationClient::claim_provider(&client, &command)
            .await
            .unwrap(),
        ProviderClaimReceipt::Dispatch { .. }
    ));
    let mut changed = source.clone();
    changed.worker.subject = "another-worker".into();
    f.mock.set(
        "provider_claim",
        Ok(ProviderClaimReceipt::Dispatch {
            ticket: ticket.clone(),
            source: Box::new(changed),
        }),
    );
    assert!(matches!(
        ProviderOperationClient::claim_provider(&client, &command).await,
        Err(ContractError::Unavailable(_))
    ));
    let before = f.mock.calls.lock().unwrap().len();
    let mut foreign = command.clone();
    foreign.owner.scope.namespace = "other".into();
    assert_eq!(
        ProviderOperationClient::claim_provider(&client, &foreign)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(f.mock.calls.lock().unwrap().len(), before);
    f.mock.set(
        "provider_claim",
        Ok(ProviderClaimReceipt::Existing {
            audience: identity().audience,
            ticket,
        }),
    );
    assert!(matches!(
        ProviderOperationClient::claim_provider(&client, &command)
            .await
            .unwrap(),
        ProviderClaimReceipt::Existing { .. }
    ));
}
#[tokio::test]
async fn provider_manifest_requires_opt_in_and_does_not_change_old_worker_discovery() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(15, Ordering::SeqCst);
    let base = reqwest::Client::new();
    let old: Value = serde_json::from_slice(
        &base
            .get(format!("{}/v1/access", f.running.url))
            .bearer_auth("worker")
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(old.get("worker_features").is_none());
    assert_eq!(
        old["runtime_capabilities"],
        json!({"approved_effect_evidence_v2":true})
    );
    let new: Value = serde_json::from_slice(
        &base
            .get(format!("{}/v1/access", f.running.url))
            .header("Ledgence-Worker-Features", "1")
            .bearer_auth("worker")
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        new["worker_features"],
        json!({"v":1,"supported":["native_effect_resolution.v1","provider_operation.v1"]})
    );
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
