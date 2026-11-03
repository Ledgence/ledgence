use super::*;
macro_rules! unused {
    ($method:ident,$command:ty,$reply:ty) => {
        fn $method<'a>(&'a self, _: &'a $command) -> ContractFuture<'a, $reply> {
            unreachable!("unexpected admission method")
        }
    };
}
impl AdmissionClient for Mock {
    unused!(put_resource, ResourcePutCommand, ResourcePutReceipt);
    unused!(resource, ResourceQuery, ResourceSnapshot);
    unused!(list_resources, ResourceListQuery, ResourcePage);
    unused!(inspect_admission, AdmissionQuery, AdmissionSnapshot);
    unused!(
        release_admission,
        AdmissionReleaseCommand,
        AdmissionReleaseReceipt
    );
    fn start_admission<'a>(
        &'a self,
        c: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.admission_starts
                .as_ref()
                .unwrap()
                .send((c.clone(), tx))
                .unwrap();
            rx.await.unwrap()
        })
    }
    fn finish_admission<'a>(
        &'a self,
        c: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.admission_finishes
                .as_ref()
                .unwrap()
                .send((c.clone(), tx))
                .unwrap();
            rx.await.unwrap()
        })
    }
}
type Start = (
    AdmissionStartCommand,
    oneshot::Sender<Result<AdmissionStartReceipt>>,
);
type Finish = (
    AdmissionFinishCommand,
    oneshot::Sender<Result<AdmissionFinishReceipt>>,
);
fn fixture() -> (
    WorkflowOperations,
    mpsc::UnboundedReceiver<Start>,
    mpsc::UnboundedReceiver<Finish>,
) {
    let (mut service, _, _) = super::fixture();
    let (starts, srx) = mpsc::unbounded_channel();
    let (finishes, frx) = mpsc::unbounded_channel();
    let mock = Arc::get_mut(&mut service).unwrap();
    mock.admission_starts = Some(starts);
    mock.admission_finishes = Some(finishes);
    let mut handler = journal(service);
    handler.runtime_binding = Some(Arc::new(EffectAccess {
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
    handler.runtime_features = ledgence_worker_api::RuntimeFeatures {
        v: 1,
        supported: vec!["resource_admission.v1".into()],
    };
    let occurrence = AdmissionOccurrence {
        scope: owner().scope,
        workflow_id: "workflow".into(),
        origin_activation_id: "origin".into(),
        key: "model:1".into(),
    };
    let binding:AdmissionBinding=serde_json::from_value(json!({"resource":"model","program":{"program":{"id":"workflow","version":"1"},"digest":format!("sha256:{}","a".repeat(64)),"size":1},"operation":{"callable":"program:model","kind":"model","version":"1","arguments":{"prompt":"hello"}}})).unwrap();
    let offer = AdmissionOffer {
        occurrence: occurrence.clone(),
        generation: 1,
        policy_revision: 1,
        activation_id: owner().task_id,
        expires_at: 10000,
        binding_digest: binding.digest().unwrap(),
    };
    handler.admission_wake = Some(AdmissionWake {
        key: "wait".into(),
        occurrence,
        waiting_activation_id: "origin".into(),
        resumed_activation_id: owner().task_id,
        resumed_revision: 3,
        binding,
        selected_at: 1000,
        selection: AdmissionSelection::Offered { offer },
    });
    (handler, srx, frx)
}
fn request() -> RuntimeRequest {
    RuntimeRequest {
        id: 1,
        operation: "resource_admission.start.v1".into(),
        payload: json!({"start_id":"start","finish_id":"finish"}),
    }
}
fn source(c: &AdmissionStartCommand, h: &WorkflowOperations) -> AdmissionSource {
    let access = h.runtime_binding.as_ref().unwrap();
    AdmissionSource {
        occurrence: c.occurrence.clone(),
        source_id: "source".into(),
        start_id: c.start_id.clone(),
        finish_id: c.finish_id.clone(),
        generation: c.offer_generation,
        binding_digest: h.admission_wake.as_ref().unwrap().binding.digest().unwrap(),
        owner: c.owner.clone(),
        worker: AdmissionWorkerIdentity {
            audience: access.audience.clone(),
            issuer: access.identity.issuer.clone(),
            subject: access.identity.subject.clone(),
            credential_id: "credential".into(),
        },
        accepted_at: 1000,
        start_deadline_at: 6000,
    }
}
#[tokio::test]
async fn start_is_one_shot_and_binds_host_lease_occurrence_and_identity() {
    for broken in 0..4 {
        let (handler, mut starts, _) = fixture();
        let exchange = handler.handle(request(), RunControl::new(Duration::from_secs(2)));
        let backend = async {
            let (c, tx) = starts.recv().await.unwrap();
            assert_eq!(c.owner, owner());
            assert_eq!(c.occurrence.origin_activation_id, "origin");
            let mut s = source(&c, &handler);
            match broken {
                1 => s.worker.subject = "foreign".into(),
                2 => {
                    s.binding_digest =
                        ledgence_worker_api::Digest(format!("sha256:{}", "b".repeat(64)))
                }
                _ => {}
            }
            if broken == 3 {
                tx.send(Err(ContractError::Unavailable("lost Start ack".into())))
                    .unwrap();
            } else {
                tx.send(Ok(AdmissionStartReceipt::Granted {
                    source: Box::new(s),
                }))
                .unwrap();
            }
        };
        let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
        assert_eq!(result.is_ok(), broken == 0);
        assert!(starts.try_recv().is_err());
    }
}
#[tokio::test]
async fn absent_feature_wake_or_untrusted_authority_fields_make_no_start_rpc() {
    for broken in 0..4 {
        let (mut handler, mut starts, _) = fixture();
        let mut req = request();
        match broken {
            0 => handler.runtime_binding = None,
            1 => handler.runtime_features.supported.clear(),
            2 => handler.admission_wake = None,
            _ => req.payload["owner"] = json!(owner()),
        }
        assert!(
            handler
                .handle(req, RunControl::new(Duration::from_secs(1)))
                .await
                .is_err()
        );
        assert!(starts.try_recv().is_err());
    }
}
#[tokio::test]
async fn finish_retries_identical_source_and_output_without_another_start() {
    let (handler, mut starts, mut finishes) = fixture();
    let wake = handler.admission_wake.as_ref().unwrap();
    let c = AdmissionStartCommand {
        owner: owner(),
        occurrence: wake.occurrence.clone(),
        offer_generation: 1,
        start_id: "start".into(),
        finish_id: "finish".into(),
    };
    let command = AdmissionFinishCommand {
        source: source(&c, &handler),
        output: json!({"value":1.0}),
    };
    let req = RuntimeRequest {
        id: 2,
        operation: "resource_admission.finish.v1".into(),
        payload: serde_json::to_value(&command).unwrap(),
    };
    let exchange = handler.handle(req, RunControl::new(Duration::from_secs(2)));
    let backend = async {
        let (first, tx) = finishes.recv().await.unwrap();
        assert_eq!(first, command);
        tx.send(Err(ContractError::Unavailable("lost Finish ack".into())))
            .unwrap();
        let (second, tx) = finishes.recv().await.unwrap();
        assert_eq!(first, second);
        tx.send(Ok(AdmissionFinishReceipt {
            source: second.source,
            output: second.output,
            accepted_at: 2000,
            already_accepted: true,
        }))
        .unwrap();
    };
    let (result, ()) = bounded(async { tokio::join!(exchange, backend) }).await;
    assert!(result.is_ok());
    assert!(starts.try_recv().is_err());
}
#[tokio::test]
async fn foreign_finish_is_rejected_before_any_rpc() {
    let (handler, _, mut finishes) = fixture();
    let wake = handler.admission_wake.as_ref().unwrap();
    let c = AdmissionStartCommand {
        owner: owner(),
        occurrence: wake.occurrence.clone(),
        offer_generation: 1,
        start_id: "start".into(),
        finish_id: "finish".into(),
    };
    let mut s = source(&c, &handler);
    s.owner.attempt_id = "other".into();
    let req = RuntimeRequest {
        id: 2,
        operation: "resource_admission.finish.v1".into(),
        payload: json!({"source":s,"output":1}),
    };
    assert!(
        handler
            .handle(req, RunControl::new(Duration::from_secs(1)))
            .await
            .is_err()
    );
    assert!(finishes.try_recv().is_err());
}
