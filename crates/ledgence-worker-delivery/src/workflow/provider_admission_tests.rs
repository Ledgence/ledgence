use super::*;

pub(super) type Start = (
    ProviderAdmissionStartCommand,
    oneshot::Sender<Result<ProviderAdmissionStartReceipt>>,
);
pub(super) type Report = (
    ProviderAdmissionReportCommand,
    oneshot::Sender<Result<ProviderAdmissionReportReceipt>>,
);
impl ProviderAdmissionClient for Mock {
    fn start_provider_admission<'a>(
        &'a self,
        c: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.provider_admission_starts
                .as_ref()
                .unwrap()
                .send((c.clone(), tx))
                .unwrap();
            rx.await.unwrap()
        })
    }
    fn report_provider_admission<'a>(
        &'a self,
        c: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.provider_admission_reports
                .as_ref()
                .unwrap()
                .send((c.clone(), tx))
                .unwrap();
            rx.await.unwrap()
        })
    }
}
fn fixture() -> (
    WorkflowOperations,
    mpsc::UnboundedReceiver<Start>,
    mpsc::UnboundedReceiver<Report>,
) {
    let (mut service, _, _) = super::fixture();
    let (tx, rx) = mpsc::unbounded_channel();
    let (rtx, rrx) = mpsc::unbounded_channel();
    let mock = Arc::get_mut(&mut service).unwrap();
    mock.provider_admission_starts = Some(tx);
    mock.provider_admission_reports = Some(rtx);
    let mut h = journal(service);
    h.runtime_binding = Some(Arc::new(EffectAccess {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: owner().scope,
        },
        identity: PrincipalIdentity {
            issuer: "issuer".into(),
            subject: "worker".into(),
        },
    }));
    h.effect_access = h.runtime_binding.clone();
    h.runtime_features = ledgence_worker_api::RuntimeFeatures {
        v: 1,
        supported: vec![
            "native_effect_resolution.v1".into(),
            "provider_operation.v1".into(),
            "resource_admission.v1".into(),
            "provider_admission.v1".into(),
        ],
    };
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../sdk/python-client/tests/fixtures/provider-effect.json"
    ))
    .unwrap();
    let mut resolved: ProviderBinding =
        serde_json::from_value(fixture["binding"]["approval"]["provider_binding"].clone()).unwrap();
    resolved.v = 2;
    resolved.admission = Some(ProviderAdmissionRequirement {
        resource: "model".into(),
    });
    let action: ApprovalAction =
        serde_json::from_value(fixture["binding"]["approval"]["action"].clone()).unwrap();
    let occurrence = AdmissionOccurrence {
        scope: owner().scope,
        workflow_id: "workflow".into(),
        origin_activation_id: "origin".into(),
        key: "capacity".into(),
    };
    let ticket = ProviderOperationTicket {
        occurrence: EffectOccurrence {
            scope: owner().scope,
            workflow_id: "workflow".into(),
            activation_id: "origin".into(),
            approval_key: "approve".into(),
        },
        binding_digest: resolved.digest(&action).unwrap(),
    };
    let binding = AdmissionBinding::Provider(Box::new(ProviderAdmissionBinding {
        resource: "model".into(),
        program: resolved.program.clone(),
        provider: ProviderAdmissionSubject {
            ticket,
            binding: resolved.request(),
            action,
        },
    }));
    let offer = AdmissionOffer {
        occurrence: occurrence.clone(),
        generation: 1,
        policy_revision: 1,
        activation_id: owner().task_id,
        expires_at: 10000,
        binding_digest: binding.digest().unwrap(),
    };
    h.admission_wake = Some(AdmissionWake {
        key: "capacity".into(),
        occurrence,
        waiting_activation_id: "origin".into(),
        resumed_activation_id: owner().task_id,
        resumed_revision: 3,
        binding,
        selected_at: 1000,
        selection: AdmissionSelection::Offered { offer },
    });
    (h, rx, rrx)
}
fn command(h: &WorkflowOperations) -> ProviderAdmissionStartCommand {
    ProviderAdmissionStartCommand {
        admission: AdmissionStartCommand {
            owner: owner(),
            occurrence: h.admission_wake.as_ref().unwrap().occurrence.clone(),
            offer_generation: 1,
            start_id: "start".into(),
            finish_id: "report".into(),
        },
    }
}
fn source(h: &WorkflowOperations, c: &ProviderAdmissionStartCommand) -> ProviderAdmissionSource {
    let access = h.runtime_binding.as_ref().unwrap();
    let worker = EffectWorkerIdentity {
        audience: access.audience.clone(),
        issuer: access.identity.issuer.clone(),
        subject: access.identity.subject.clone(),
        credential_id: "credential".into(),
    };
    let admission = AdmissionSource {
        occurrence: c.admission.occurrence.clone(),
        source_id: "capacity-source".into(),
        start_id: c.admission.start_id.clone(),
        finish_id: c.admission.finish_id.clone(),
        generation: 1,
        binding_digest: h.admission_wake.as_ref().unwrap().binding.digest().unwrap(),
        owner: c.admission.owner.clone(),
        worker: AdmissionWorkerIdentity {
            audience: worker.audience.clone(),
            issuer: worker.issuer.clone(),
            subject: worker.subject.clone(),
            credential_id: worker.credential_id.clone(),
        },
        accepted_at: 1000,
        start_deadline_at: 6000,
    };
    let provider = ProviderDispatchSource {
        source_id: "provider-source".into(),
        claim_id: admission.start_id.clone(),
        report_id: admission.finish_id.clone(),
        ticket: h
            .admission_wake
            .as_ref()
            .unwrap()
            .binding
            .provider()
            .unwrap()
            .ticket
            .clone(),
        worker,
        accepted_at: 1000,
        first_possible_dispatch_at: 1000,
        reconciliation_deadline: 100000,
    };
    ProviderAdmissionSource {
        admission,
        provider,
    }
}
fn request() -> RuntimeRequest {
    RuntimeRequest {
        id: 1,
        operation: "provider_admission.start.v1".into(),
        payload: json!({"start_id":"start","finish_id":"report"}),
    }
}
fn report_command(h: &WorkflowOperations) -> ProviderAdmissionReportCommand {
    let source = source(h, &command(h));
    let target = h
        .admission_wake
        .as_ref()
        .unwrap()
        .binding
        .provider()
        .unwrap()
        .binding
        .target
        .clone();
    let report:ProviderReport=serde_json::from_value(json!({"reported_target":target,"provider_reference":null,"provider_request_id":null,"knowledge":{"kind":"succeeded","result":{"data":"a".repeat(100000)}}})).unwrap();
    ProviderAdmissionReportCommand {
        source,
        outcome: ProviderDispatchOutcome {
            call_completion: ProviderCallCompletion::Unknown,
            report,
        },
    }
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
            accepted_at: 2000,
            recorded_revision: 2,
            accepted_via: ProviderReportIngress::EvidenceOnly,
            already_accepted: false,
        },
    }
}
#[tokio::test]
async fn combined_start_binds_original_approval_to_current_owner_and_never_retries() {
    for broken in 0..4 {
        let (h, mut starts, _) = fixture();
        let exchange = h.handle(request(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (c, tx) = starts.recv().await.unwrap();
            assert_eq!(c.admission.owner, owner());
            assert_eq!(c.admission.occurrence.origin_activation_id, "origin");
            let mut value = source(&h, &c);
            assert_ne!(
                value.provider.ticket.occurrence.activation_id,
                c.admission.owner.task_id
            );
            match broken {
                1 => value.provider.ticket.occurrence.activation_id = "changed".into(),
                2 => value.provider.worker.subject = "foreign".into(),
                _ => {}
            }
            tx.send(if broken == 3 {
                Err(ContractError::Unavailable("lost acknowledgement".into()))
            } else {
                Ok(ProviderAdmissionStartReceipt::Granted {
                    source: Box::new(value),
                })
            })
            .unwrap();
        };
        let (reply, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        assert_eq!(reply.is_ok(), broken == 0);
        assert!(starts.try_recv().is_err());
    }
}
#[tokio::test]
async fn independent_capabilities_and_generic_admission_never_authorize_governed_callback() {
    let (mut h, mut starts, mut reports) = fixture();
    h.runtime_features
        .supported
        .retain(|v| v != "provider_admission.v1");
    assert!(
        h.handle(request(), RunControl::new(Duration::from_secs(1)))
            .await
            .is_err()
    );
    let mut generic = request();
    generic.operation = "resource_admission.start.v1".into();
    assert!(
        h.handle(generic, RunControl::new(Duration::from_secs(1)))
            .await
            .is_err()
    );
    assert!(starts.try_recv().is_err());
    assert!(reports.try_recv().is_err());
}
#[tokio::test]
async fn combined_report_retries_identical_large_evidence_without_generic_finish() {
    let (h, mut starts, mut reports) = fixture();
    let original = report_command(&h);
    let exchange = h.handle(
        RuntimeRequest {
            id: 2,
            operation: "provider_admission.report.v1".into(),
            payload: serde_json::to_value(&original).unwrap(),
        },
        RunControl::new(Duration::from_secs(2)),
    );
    let backend = async {
        let (first, tx) = reports.recv().await.unwrap();
        assert!(serde_json::to_vec(&first).unwrap().len() > 64 * 1024);
        tx.send(Err(ContractError::Unavailable("lost Report ack".into())))
            .unwrap();
        let (second, tx) = reports.recv().await.unwrap();
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        let accepted = receipt(&second);
        accepted.validate().unwrap();
        tx.send(Ok(accepted)).unwrap();
    };
    let (reply, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
    assert!(reply.is_ok());
    assert!(starts.try_recv().is_err());
}
#[tokio::test]
async fn report_and_source_cannot_be_rebound_or_upgrade_unknown_to_completed() {
    let (h, _, mut reports) = fixture();
    let mut foreign = report_command(&h);
    foreign.source.admission.owner.attempt_id = "other".into();
    assert!(
        h.handle(
            RuntimeRequest {
                id: 2,
                operation: "provider_admission.report.v1".into(),
                payload: serde_json::to_value(foreign).unwrap()
            },
            RunControl::new(Duration::from_secs(1))
        )
        .await
        .is_err()
    );
    assert!(reports.try_recv().is_err());
    let original = report_command(&h);
    let exchange = h.handle(
        RuntimeRequest {
            id: 2,
            operation: "provider_admission.report.v1".into(),
            payload: serde_json::to_value(original).unwrap(),
        },
        RunControl::new(Duration::from_secs(2)),
    );
    let backend = async {
        let (c, tx) = reports.recv().await.unwrap();
        let mut r = receipt(&c);
        r.call_completion = ProviderCallCompletion::Completed;
        tx.send(Ok(r)).unwrap();
    };
    let (reply, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
    assert!(reply.is_err());
}
