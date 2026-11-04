use super::*;
impl ProviderOperationClient for Mock {
    fn claim_provider<'a>(
        &'a self,
        c: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.provider_claims
                .as_ref()
                .unwrap()
                .send((c.clone(), tx))
                .unwrap();
            rx.await.unwrap()
        })
    }
    fn report_dispatch<'a>(
        &'a self,
        c: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.provider_reports
                .as_ref()
                .unwrap()
                .send((c.clone(), tx))
                .unwrap();
            rx.await.unwrap()
        })
    }
    fn probe_context<'a>(
        &'a self,
        _: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext> {
        unreachable!("workflow cannot obtain probe authority")
    }
    fn report_probe<'a>(
        &'a self,
        _: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("workflow cannot report another task")
    }
    fn observe_provider<'a>(
        &'a self,
        _: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("managed workflow cannot detach late reports")
    }
}
type Claim = (
    ProviderClaimCommand,
    oneshot::Sender<Result<ProviderClaimReceipt>>,
);
type Report = (
    ProviderDispatchReportCommand,
    oneshot::Sender<Result<ProviderReportReply>>,
);
fn fixture() -> (
    WorkflowOperations,
    mpsc::UnboundedReceiver<Claim>,
    mpsc::UnboundedReceiver<Report>,
) {
    let (mut service, _, _) = super::fixture();
    let (claims, rx) = mpsc::unbounded_channel();
    let (reports, rrx) = mpsc::unbounded_channel();
    let mock = Arc::get_mut(&mut service).unwrap();
    mock.provider_claims = Some(claims);
    mock.provider_reports = Some(reports);
    let mut handler = journal(service);
    let worker = worker();
    handler.effect_access = Some(Arc::new(EffectAccess {
        audience: worker.audience,
        identity: PrincipalIdentity {
            issuer: worker.issuer,
            subject: worker.subject,
        },
    }));
    handler.runtime_features = ledgence_worker_api::RuntimeFeatures {
        v: 1,
        supported: vec![
            "native_effect_resolution.v1".into(),
            "provider_operation.v1".into(),
        ],
    };
    (handler, rx, rrx)
}
fn worker() -> EffectWorkerIdentity {
    EffectWorkerIdentity {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: owner().scope,
        },
        issuer: "urn:worker".into(),
        subject: "worker-one".into(),
        credential_id: "credential".into(),
    }
}
fn claim() -> RuntimeRequest {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../sdk/python-client/tests/fixtures/provider-effect.json"
    ))
    .unwrap();
    RuntimeRequest {
        id: 1,
        operation: "provider_operation.claim.v1".into(),
        payload: json!({"approval_key":"approve","claim_id":"claim","report_id":"report",
        "action":fixture["binding"]["approval"]["action"],"binding":fixture["binding"]["approval"]["provider_binding"]}),
    }
}
fn source(c: &ProviderClaimCommand) -> ProviderDispatchSource {
    ProviderDispatchSource {
        budget_grant: None,
        source_id: "source".into(),
        claim_id: c.claim_id.clone(),
        report_id: c.report_id.clone(),
        ticket: c.ticket().unwrap(),
        worker: worker(),
        accepted_at: 1,
        first_possible_dispatch_at: 1,
        reconciliation_deadline: 60001,
    }
}
#[tokio::test]
async fn provider_unnegotiated_or_forged_authority_never_dispatches() {
    let (mut handler, mut claims, _) = fixture();
    for key in ["owner", "worker", "workflow_id", "source_id"] {
        let mut request = claim();
        request.payload[key] = json!("forged");
        assert_eq!(
            handler
                .handle(request, RunControl::new(Duration::from_secs(1)))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Protocol
        );
    }
    let negotiated = handler.runtime_features.clone();
    handler.runtime_features = ledgence_worker_api::RuntimeFeatures {
        v: 1,
        supported: vec!["custom_runtime.v1".into()],
    };
    assert_eq!(
        handler
            .handle(claim(), RunControl::new(Duration::from_secs(1)))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    handler.runtime_features = negotiated;
    handler.effect_access = None;
    assert_eq!(
        handler
            .handle(claim(), RunControl::new(Duration::from_secs(1)))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    assert!(claims.try_recv().is_err());
}
#[tokio::test]
async fn provider_lost_grant_never_retries_and_foreign_worker_never_grants() {
    for mutate in [false, true] {
        let (handler, mut claims, _) = fixture();
        let exchange = handler.handle(claim(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (c, reply) = claims.recv().await.unwrap();
            if mutate {
                let mut s = source(&c);
                s.worker.subject = "foreign".into();
                reply
                    .send(Ok(ProviderClaimReceipt::Dispatch {
                        ticket: c.ticket().unwrap(),
                        source: Box::new(s),
                    }))
                    .unwrap();
            } else {
                reply
                    .send(Err(ContractError::Unavailable("lost grant".into())))
                    .unwrap();
            }
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        assert!(result.is_err());
        assert!(claims.try_recv().is_err());
    }
}
#[tokio::test]
async fn provider_report_retries_frozen_facts_and_preserves_existing_ingress() {
    let (handler, mut claims, mut reports) = fixture();
    let exchange = handler.handle(claim(), RunControl::new(Duration::from_secs(2)));
    let backend = async {
        let (c, reply) = claims.recv().await.unwrap();
        let s = source(&c);
        reply
            .send(Ok(ProviderClaimReceipt::Dispatch {
                ticket: c.ticket().unwrap(),
                source: Box::new(s),
            }))
            .unwrap();
    };
    let (reply, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
    let reply = reply.unwrap();
    let report = ProviderReport {
        usage: None,
        reported_target: serde_json::from_value(claim().payload["binding"]["target"].clone())
            .unwrap(),
        provider_reference: None,
        provider_request_id: None,
        knowledge: ProviderKnowledge::Pending,
    };
    let command = RuntimeRequest {
        id: 2,
        operation: "provider_operation.report.v1".into(),
        payload: json!({"ticket":reply.result["ticket"],"source_id":"source","report_id":"report","report":report}),
    };
    let exchange = handler.handle(command, RunControl::new(Duration::from_secs(2)));
    let backend = async {
        let (first, reply) = reports.recv().await.unwrap();
        reply
            .send(Err(ContractError::Unavailable("lost report".into())))
            .unwrap();
        let (second, reply) = reports.recv().await.unwrap();
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        reply
            .send(Ok(ProviderReportReply::ExistingReport {
                original_receipt: ProviderReportReceipt {
                    ticket: second.ticket.clone(),
                    source_id: second.source_id.clone(),
                    source: ProviderSourceReference::Dispatch {
                        claim_id: "claim".into(),
                    },
                    report_id: second.report_id.clone(),
                    report_digest: second.report.digest().unwrap(),
                    worker: worker(),
                    accepted_at: 5,
                    recorded_revision: 3,
                    accepted_via: ProviderReportIngress::EvidenceOnly,
                    already_accepted: true,
                },
            }))
            .unwrap();
    };
    let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
    let result = result.unwrap();
    assert_eq!(result.result["kind"], "recorded_elsewhere");
    assert_eq!(result.result["accepted_via"], "evidence_only");
    assert!(result.result.get("worker").is_none());
    assert!(reports.try_recv().is_err());
}
