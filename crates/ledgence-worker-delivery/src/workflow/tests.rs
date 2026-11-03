use super::*;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

type ContextRequest = (
    LeaseOwner,
    oneshot::Sender<Result<WorkflowActivationContext>>,
);
type CommitRequest = (
    LocalResultCommand,
    oneshot::Sender<Result<LocalResultReceipt>>,
);
type ForkRequest = (
    WorkflowForkCommand,
    oneshot::Sender<Result<WorkflowForkReceipt>>,
);

type EffectClaim = (
    ApprovedEffectClaimCommand,
    oneshot::Sender<Result<ApprovedEffectClaimReceipt>>,
);

type EffectConfirm = (
    ApprovedEffectConfirmCommand,
    oneshot::Sender<Result<ApprovedEffectConfirmReceipt>>,
);
type EffectClaimV2 = (
    EffectClaimV2Command,
    oneshot::Sender<Result<EffectClaimV2Receipt>>,
);
type EffectConfirmV2 = (
    EffectConfirmV2Command,
    oneshot::Sender<Result<EffectConfirmV2Receipt>>,
);

struct Mock {
    provider_admission_starts: Option<mpsc::UnboundedSender<provider_admission_tests::Start>>,
    provider_admission_reports: Option<mpsc::UnboundedSender<provider_admission_tests::Report>>,
    admission_starts: Option<
        mpsc::UnboundedSender<(
            AdmissionStartCommand,
            oneshot::Sender<Result<AdmissionStartReceipt>>,
        )>,
    >,
    admission_finishes: Option<
        mpsc::UnboundedSender<(
            AdmissionFinishCommand,
            oneshot::Sender<Result<AdmissionFinishReceipt>>,
        )>,
    >,
    provider_claims: Option<
        mpsc::UnboundedSender<(
            ProviderClaimCommand,
            oneshot::Sender<Result<ProviderClaimReceipt>>,
        )>,
    >,
    provider_reports: Option<
        mpsc::UnboundedSender<(
            ProviderDispatchReportCommand,
            oneshot::Sender<Result<ProviderReportReply>>,
        )>,
    >,
    contexts: mpsc::UnboundedSender<ContextRequest>,
    commits: mpsc::UnboundedSender<CommitRequest>,
    forks: Option<mpsc::UnboundedSender<ForkRequest>>,
    effect_claims: Option<mpsc::UnboundedSender<EffectClaim>>,
    effect_confirms: Option<mpsc::UnboundedSender<EffectConfirm>>,
    claims_v2: Option<mpsc::UnboundedSender<EffectClaimV2>>,
    confirms_v2: Option<mpsc::UnboundedSender<EffectConfirmV2>>,
}
impl EffectEvidenceClient for Mock {
    fn claim_v2<'a>(
        &'a self,
        command: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.claims_v2
                .as_ref()
                .expect("unexpected v2 claim")
                .send((command.clone(), reply))
                .unwrap();
            receiver.await.unwrap()
        })
    }
    fn confirm_v2<'a>(
        &'a self,
        command: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.confirms_v2
                .as_ref()
                .expect("unexpected v2 confirm")
                .send((command.clone(), reply))
                .unwrap();
            receiver.await.unwrap()
        })
    }
    fn observe<'a>(
        &'a self,
        _: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        unreachable!("managed runtime cannot send independent late observations")
    }
}
impl ApprovedEffects for Mock {
    fn confirm_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectConfirmCommand,
    ) -> ContractFuture<'a, ApprovedEffectConfirmReceipt> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.effect_confirms
                .as_ref()
                .expect("unexpected approved effect confirmation")
                .send((command.clone(), reply))
                .unwrap();
            receiver.await.unwrap()
        })
    }
    fn claim_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectClaimCommand,
    ) -> ContractFuture<'a, ApprovedEffectClaimReceipt> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.effect_claims
                .as_ref()
                .expect("unexpected approved effect claim")
                .send((command.clone(), reply))
                .unwrap();
            receiver.await.unwrap()
        })
    }
}

impl WorkflowService for Mock {
    fn provider_admission_client(&self) -> Result<&dyn ProviderAdmissionClient> {
        Ok(self)
    }
    fn admission_client(&self) -> Result<&dyn AdmissionClient> {
        Ok(self)
    }
    fn provider_operation_client(&self) -> Result<&dyn ProviderOperationClient> {
        Ok(self)
    }
    fn effect_evidence_client(&self) -> Result<&dyn EffectEvidenceClient> {
        Ok(self)
    }
    fn approved_effects(&self) -> Result<&dyn ApprovedEffects> {
        if self.effect_claims.is_none() && self.effect_confirms.is_none() {
            return Err(ContractError::InvalidInput(
                "protected effects unsupported".into(),
            ));
        }
        Ok(self)
    }

    fn fork_workflow<'a>(
        &'a self,
        command: &'a WorkflowForkCommand,
    ) -> ContractFuture<'a, WorkflowForkReceipt> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.forks
                .as_ref()
                .expect("unexpected fork")
                .send((command.clone(), reply))
                .unwrap();
            receiver.await.unwrap()
        })
    }
    fn send_workflow_event<'a>(
        &'a self,
        _: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt> {
        Box::pin(async { panic!("unexpected workflow event mutation") })
    }
    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.contexts.send((owner.clone(), reply)).unwrap();
            receiver.await.unwrap()
        })
    }
    fn record_local_result<'a>(
        &'a self,
        command: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        Box::pin(async move {
            let (reply, receiver) = oneshot::channel();
            self.commits.send((command.clone(), reply)).unwrap();
            receiver.await.unwrap()
        })
    }
    fn submit_workflow<'a>(&'a self, _: &'a SubmitCommand) -> ContractFuture<'a, WorkflowSnapshot> {
        unreachable!("unexpected submission")
    }
    fn workflow_status<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        unreachable!("unexpected status")
    }
    fn workflow_result<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, WorkflowResult> {
        unreachable!("unexpected result")
    }
    fn cancel_workflow<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        unreachable!("unexpected cancellation")
    }
}
fn fixture() -> (
    Arc<Mock>,
    mpsc::UnboundedReceiver<ContextRequest>,
    mpsc::UnboundedReceiver<CommitRequest>,
) {
    let (contexts, context_requests) = mpsc::unbounded_channel();
    let (commits, commit_requests) = mpsc::unbounded_channel();
    (
        Arc::new(Mock {
            provider_admission_starts: None,
            provider_admission_reports: None,
            admission_starts: None,
            admission_finishes: None,
            provider_claims: None,
            provider_reports: None,
            contexts,
            commits,
            forks: None,
            effect_claims: None,
            effect_confirms: None,
            claims_v2: None,
            confirms_v2: None,
        }),
        context_requests,
        commit_requests,
    )
}
fn owner() -> LeaseOwner {
    LeaseOwner {
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "ns".into(),
        },
        task_id: "activation".into(),
        attempt_id: "attempt".into(),
        lease_id: "lease".into(),
        generation: 2,
        worker_session_id: "worker".into(),
        consumer_id: 1,
    }
}
fn config() -> DeliveryConfig {
    let mut config = DeliveryConfig::new(owner().scope, "queue");
    config.request_timeout = Duration::from_secs(1);
    config.retry_delay = Duration::from_millis(1);
    config
}
fn context() -> WorkflowActivationContext {
    WorkflowActivationContext {
        admission_result: None,
        provider_admission: None,
        parent_workflow_id: None,
        root_workflow_id: None,
        v: 1,
        workflow_id: "workflow".into(),
        activation_id: owner().task_id,
        revision: 3,
        continuation: "collect".into(),
        state: json!({"round":2}),
        inputs: Default::default(),
        local_steps: vec![],
        wake: None,
    }
}
fn request() -> RuntimeRequest {
    RuntimeRequest {
        id: 7,
        operation: "local_step.commit".into(),
        payload: json!({"key":"fetch","callable":"program:fetch","input":{"number":1.0},"output":{"ok":true}}),
    }
}
fn journal(service: Arc<Mock>) -> WorkflowOperations {
    let config = config();
    WorkflowOperations {
        service,
        effect_access: None,
        runtime_binding: None,
        admission_wake: None,
        runtime_features: ledgence_worker_api::RuntimeFeatures::default(),
        owner: owner(),
        workflow_id: context().workflow_id,
        processing_trace: None,
        request_timeout: config.request_timeout,
        retry_delay: config.retry_delay,
    }
}

#[path = "admission_tests.rs"]
mod admission_tests;
#[path = "approved_effect_tests.rs"]
mod approved_effect;
#[path = "evidence_tests.rs"]
mod evidence;
#[path = "provider_tests.rs"]
mod provider;
#[path = "provider_admission_tests.rs"]
mod provider_admission_tests;

#[path = "fork_tests.rs"]
mod forks;
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("workflow test timed out")
}

#[tokio::test]
async fn wrong_workflow_activation_or_context_version_is_rejected_before_runtime_use() {
    for field in ["workflow", "activation", "version", "lineage"] {
        let (service, mut contexts, mut commits) = fixture();
        let pending = tokio::spawn(async move {
            fetch_context(
                service.as_ref(),
                &owner(),
                "workflow",
                None,
                None,
                &config(),
                &RunControl::new(Duration::from_secs(1)),
            )
            .await
        });
        let (received, reply) = bounded(contexts.recv()).await.unwrap();
        assert_eq!(received, owner());
        let mut value = context();
        match field {
            "workflow" => value.workflow_id = "wrong".into(),
            "activation" => value.activation_id = "wrong".into(),
            "lineage" => {
                value.parent_workflow_id = Some("parent".into());
                value.root_workflow_id = Some("root".into());
            }
            _ => value.v = 2,
        }
        reply.send(Ok(value)).unwrap();
        assert_eq!(
            bounded(pending).await.unwrap().unwrap_err().kind,
            ErrorKind::Protocol
        );
        assert!(commits.try_recv().is_err());
    }
}

#[tokio::test]
async fn typed_wake_scope_must_match_the_acquired_owner_before_runtime_use() {
    for provider in [false, true] {
        for changed in ["none", "tenant", "namespace"] {
            let mut wake_scope = owner().scope;
            match changed {
                "tenant" => wake_scope.tenant_id = "foreign".into(),
                "namespace" => wake_scope.namespace = "foreign".into(),
                _ => {}
            }
            let mut value = context();
            let mut wake = json!({
                "kind": if provider { "provider_operation" } else { "effect_resolution" },
                "key": "wait",
                "occurrence": {
                    "scope": wake_scope,
                    "workflow_id": value.workflow_id,
                    "activation_id": "effect_origin",
                    "approval_key": "approval"
                },
                "parking_activation_id": "parked",
                "parking_revision": 2,
                "resumed_activation_id": value.activation_id,
                "resumed_revision": 3,
                "selected_at": 20,
                "selected_effect_revision": 2
            });
            if provider {
                wake["selection"] = json!({
                    "kind": "provider_succeeded",
                    "report_ref": {
                        "report_id": "report",
                        "source_id": "source",
                        "sequence": 1,
                        "recorded_revision": 2,
                        "recorded_at": 10,
                        "knowledge": "succeeded"
                    },
                    "result": {"private_result": "foreign_scope_payload"}
                });
            } else {
                wake["resolution_id"] = json!("resolution");
                wake["accepted_at"] = json!(10);
                wake["accepted_revision"] = json!(2);
                wake["basis_revision"] = json!(1);
                wake["outcome"] = json!({
                    "kind": "recorded_success",
                    "result": {"private_result": "foreign_scope_payload"}
                });
            }
            value.wake = Some(serde_json::from_value(wake).unwrap());
            value.validate().unwrap();
            let expected = value.clone();
            let (service, mut contexts, mut commits) = fixture();
            let pending = tokio::spawn(async move {
                fetch_context(
                    service.as_ref(),
                    &owner(),
                    "workflow",
                    None,
                    None,
                    &config(),
                    &RunControl::new(Duration::from_secs(1)),
                )
                .await
            });
            let (received, reply) = bounded(contexts.recv()).await.unwrap();
            assert_eq!(received, owner());
            reply.send(Ok(value)).unwrap();
            let actual = bounded(pending).await.unwrap();
            if changed == "none" {
                assert_eq!(
                    serde_json::to_value(actual.unwrap()).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            } else {
                assert_eq!(
                    actual.unwrap_err().kind,
                    ErrorKind::Protocol,
                    "provider={provider}, {changed}"
                );
            }
            assert!(
                contexts.try_recv().is_err(),
                "no context retry after a bad reply"
            );
            assert!(commits.try_recv().is_err());
        }
    }
}

#[tokio::test]
async fn transient_context_fetch_reuses_exact_owner_and_frozen_reply() {
    let (service, mut contexts, _) = fixture();
    let pending = tokio::spawn(async move {
        fetch_context(
            service.as_ref(),
            &owner(),
            "workflow",
            None,
            None,
            &config(),
            &RunControl::new(Duration::from_secs(1)),
        )
        .await
    });
    let (first, reply) = bounded(contexts.recv()).await.unwrap();
    reply
        .send(Err(ContractError::Unavailable("reply lost".into())))
        .unwrap();
    let (second, reply) = bounded(contexts.recv()).await.unwrap();
    assert_eq!(first, second);
    let expected = serde_json::to_value(context()).unwrap();
    reply.send(Ok(context())).unwrap();
    assert_eq!(
        serde_json::to_value(bounded(pending).await.unwrap().unwrap()).unwrap(),
        expected
    );
}

#[tokio::test]
async fn lost_commit_ack_retries_identical_owner_and_record_before_runtime_reply() {
    let (service, _, mut commits) = fixture();
    let handler = journal(service);
    let pending = tokio::spawn(async move {
        handler
            .handle(request(), RunControl::new(Duration::from_secs(1)))
            .await
    });
    let (first, reply) = bounded(commits.recv()).await.unwrap();
    assert_eq!(first.owner, owner());
    reply
        .send(Err(ContractError::Unavailable(
            "committed but reply lost".into(),
        )))
        .unwrap();
    let (second, reply) = bounded(commits.recv()).await.unwrap();
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    assert!(
        !pending.is_finished(),
        "no runtime acknowledgement before durable receipt"
    );
    reply
        .send(Ok(LocalResultReceipt {
            key: "fetch".into(),
            already_accepted: true,
        }))
        .unwrap();
    let response = bounded(pending).await.unwrap().unwrap();
    assert_eq!(response.id, 7);
    assert_eq!(response.result, json!({"committed":true}));
    assert!(commits.try_recv().is_err());
}

#[tokio::test]
async fn wrong_receipt_identity_cannot_acknowledge_local_result() {
    let (service, _, mut commits) = fixture();
    let handler = journal(service);
    let pending = tokio::spawn(async move {
        handler
            .handle(request(), RunControl::new(Duration::from_secs(1)))
            .await
    });
    let (_, reply) = bounded(commits.recv()).await.unwrap();
    reply
        .send(Ok(LocalResultReceipt {
            key: "another-step".into(),
            already_accepted: false,
        }))
        .unwrap();
    assert_eq!(
        bounded(pending).await.unwrap().unwrap_err().kind,
        ErrorKind::Protocol
    );
    assert!(commits.try_recv().is_err());
}

#[tokio::test]
async fn cancellation_or_attempt_timeout_drops_pending_rpc_without_runtime_ack() {
    for cancel in [true, false] {
        let (service, _, mut commits) = fixture();
        let handler = journal(service);
        let control = RunControl::new(if cancel {
            Duration::from_secs(1)
        } else {
            Duration::from_millis(30)
        });
        let ongoing = control.clone();
        let pending = tokio::spawn(async move { handler.handle(request(), ongoing).await });
        let (_, reply) = bounded(commits.recv()).await.unwrap();
        if cancel {
            control.cancel();
        }
        let error = bounded(pending).await.unwrap().unwrap_err();
        assert_eq!(
            error.kind,
            if cancel {
                ErrorKind::Cancelled
            } else {
                ErrorKind::TimedOut
            }
        );
        assert!(
            reply.is_closed(),
            "RPC future must be dropped before returning"
        );
        assert!(
            reply
                .send(Ok(LocalResultReceipt {
                    key: "fetch".into(),
                    already_accepted: false
                }))
                .is_err()
        );
        assert!(commits.try_recv().is_err());
    }
}

#[tokio::test]
async fn receipt_ready_at_cancellation_cannot_grant_post_cancel_ack() {
    let (service, _, mut commits) = fixture();
    let handler = journal(service);
    let control = RunControl::new(Duration::from_secs(1));
    let ongoing = control.clone();
    let pending = tokio::spawn(async move { handler.handle(request(), ongoing).await });
    let (_, reply) = bounded(commits.recv()).await.unwrap();
    control.cancel();
    let _ = reply.send(Ok(LocalResultReceipt {
        key: "fetch".into(),
        already_accepted: false,
    }));
    assert_eq!(
        bounded(pending).await.unwrap().unwrap_err().kind,
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn runtime_operations_and_local_records_are_validated_before_dispatch() {
    let (service, _, mut commits) = fixture();
    let handler = journal(service);
    let mut wrong = request();
    wrong.operation = "unsupported".into();
    assert_eq!(
        handler
            .handle(wrong, RunControl::new(Duration::from_secs(1)))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Protocol
    );
    let mut wrong = request();
    wrong.payload = Value::Null;
    assert_eq!(
        handler
            .handle(wrong, RunControl::new(Duration::from_secs(1)))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Protocol
    );
    assert!(commits.try_recv().is_err());
}

#[tokio::test]
async fn control_response_crossing_request_deadline_remains_uncertain() {
    let result = controlled(
        &RunControl::new(Duration::from_secs(1)),
        Duration::from_millis(1),
        || async {
            // An adapter can return from one poll after its operation budget elapsed.
            std::thread::sleep(Duration::from_millis(5));
            Ok(42)
        },
    )
    .await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
}

#[tokio::test]
async fn control_adapter_construction_consumes_the_same_request_budget() {
    let polled = AtomicBool::new(false);
    let result = controlled(
        &RunControl::new(Duration::from_secs(1)),
        Duration::from_millis(5),
        || {
            std::thread::sleep(Duration::from_millis(20));
            async {
                polled.store(true, Ordering::SeqCst);
                Ok(42)
            }
        },
    )
    .await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert!(
        !polled.load(Ordering::SeqCst),
        "do not poll adapter I/O after construction exhausted its request budget"
    );
}

#[tokio::test]
async fn cancelled_control_does_not_construct_adapter_io() {
    let control = RunControl::new(Duration::from_secs(1));
    control.cancel();
    let constructed = AtomicBool::new(false);
    let result = controlled(&control, Duration::from_millis(50), || {
        constructed.store(true, Ordering::SeqCst);
        async { Ok(42) }
    })
    .await;
    assert_eq!(result, Err(ContractError::OwnershipLost));
    assert!(!constructed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn control_rechecks_cancellation_after_a_synchronous_reply() {
    let control = RunControl::new(Duration::from_secs(1));
    let result = controlled(&control, Duration::from_millis(50), || async {
        control.cancel();
        Ok(42)
    })
    .await;
    assert_eq!(result, Err(ContractError::OwnershipLost));
}

#[tokio::test]
async fn cancellation_during_control_construction_does_not_poll_adapter_io() {
    let control = RunControl::new(Duration::from_secs(1));
    let polled = AtomicBool::new(false);
    let result = controlled(&control, Duration::from_millis(50), || {
        control.cancel();
        async {
            polled.store(true, Ordering::SeqCst);
            Ok(42)
        }
    })
    .await;
    assert_eq!(result, Err(ContractError::OwnershipLost));
    assert!(!polled.load(Ordering::SeqCst));
}
