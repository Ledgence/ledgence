//! Startup feature metadata remains independent of effect authority throughout
//! the delivery loop, while known protected features retain their prerequisites.
use super::*;
use ledgence_orchestration_api::Result;

struct WorkflowPorts {
    evidence: bool,
    provider: bool,
    contexts: AtomicUsize,
    probes: AtomicUsize,
}
impl WorkflowPorts {
    fn new(evidence: bool, provider: bool) -> Arc<Self> {
        Arc::new(Self {
            evidence,
            provider,
            contexts: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
        })
    }
}
impl WorkflowService for WorkflowPorts {
    fn effect_evidence_client(&self) -> Result<&dyn EffectEvidenceClient> {
        if self.evidence {
            Ok(self)
        } else {
            Err(ContractError::Unavailable("no evidence client".into()))
        }
    }
    fn provider_operation_client(&self) -> Result<&dyn ProviderOperationClient> {
        if self.provider {
            Ok(self)
        } else {
            Err(ContractError::Unavailable("no provider client".into()))
        }
    }
    fn send_workflow_event<'a>(
        &'a self,
        _: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt> {
        unreachable!("feature transport cannot send workflow events")
    }
    fn submit_workflow<'a>(&'a self, _: &'a SubmitCommand) -> ContractFuture<'a, WorkflowSnapshot> {
        unreachable!("feature transport cannot submit a workflow")
    }
    fn workflow_status<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        unreachable!("feature transport cannot inspect status")
    }
    fn workflow_result<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, WorkflowResult> {
        unreachable!("feature transport cannot retrieve results")
    }
    fn cancel_workflow<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        unreachable!("feature transport cannot cancel work")
    }
    fn record_local_result<'a>(
        &'a self,
        _: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        unreachable!("feature transport cannot write results")
    }
    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        self.contexts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(WorkflowActivationContext {
                v: 1,
                workflow_id: "workflow".into(),
                activation_id: owner.task_id.clone(),
                parent_workflow_id: None,
                root_workflow_id: None,
                revision: 0,
                continuation: "start".into(),
                state: Value::Null,
                inputs: Default::default(),
                local_steps: Vec::new(),
                wake: None,
            })
        })
    }
}
impl EffectEvidenceClient for WorkflowPorts {
    fn claim_v2<'a>(
        &'a self,
        _: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        unreachable!("feature transport cannot claim an effect")
    }
    fn confirm_v2<'a>(
        &'a self,
        _: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        unreachable!("feature transport cannot confirm an effect")
    }
    fn observe<'a>(
        &'a self,
        _: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        unreachable!("feature transport cannot write evidence")
    }
}
impl ProviderOperationClient for WorkflowPorts {
    fn claim_provider<'a>(
        &'a self,
        _: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt> {
        unreachable!("feature transport cannot dispatch")
    }
    fn report_dispatch<'a>(
        &'a self,
        _: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("feature transport cannot report a dispatch")
    }
    fn probe_context<'a>(
        &'a self,
        command: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let fixture: Value = serde_json::from_str(include_str!(
                "../../../../sdk/python-client/tests/fixtures/provider-effect.json"
            ))
            .unwrap();
            let mut provider: ProviderBinding =
                serde_json::from_value(fixture["binding"]["approval"]["provider_binding"].clone())
                    .unwrap();
            provider.program = descriptor();
            let action: ApprovalAction =
                serde_json::from_value(fixture["binding"]["approval"]["action"].clone()).unwrap();
            let (audience, identity) = binding();
            let context = ProviderProbeContext {
                ticket: ProviderOperationTicket {
                    occurrence: command.probe.occurrence.clone(),
                    binding_digest: provider.digest(&action).unwrap(),
                },
                check_timeout_ms: provider.reconciliation.check_timeout_ms,
                binding: provider,
                action,
                probe_id: command.probe.probe_id.clone(),
                generation: command.probe.generation,
                task_id: command.owner.task_id.clone(),
                attempt_id: command.owner.attempt_id.clone(),
                source_id: "source".into(),
                report_id: "report".into(),
                worker: EffectWorkerIdentity {
                    audience,
                    issuer: identity.issuer,
                    subject: identity.subject,
                    credential_id: "credential".into(),
                },
                provider_reference: None,
            };
            context.validate().unwrap();
            Ok(context)
        })
    }
    fn report_probe<'a>(
        &'a self,
        _: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("workflow cannot report a probe")
    }
    fn observe_provider<'a>(
        &'a self,
        _: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("feature transport cannot write provider evidence")
    }
}
fn interactive_worker() -> (Worker, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let worker = Worker::new(
        WorkerConfig {
            concurrency: 1,
            ..WorkerConfig::default()
        },
        Arc::new(Store(counts.clone())),
        Arc::new(Cache(Mutex::new(HashMap::new()), true)),
        Arc::new(Runtime(counts.clone())),
    )
    .unwrap();
    (worker, counts)
}
fn features(names: &[&str]) -> RuntimeFeatures {
    RuntimeFeatures {
        v: 1,
        supported: names.iter().map(|s| (*s).into()).collect(),
    }
}
fn binding() -> (AccessAudience, PrincipalIdentity) {
    (
        AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: scope(),
        },
        PrincipalIdentity {
            issuer: "issuer".into(),
            subject: "worker".into(),
        },
    )
}
#[tokio::test]
async fn generic_features_reach_workflow_without_effect_authority_and_survive_builder_order() {
    // No effect binding, binding first, binding last. The latter must not erase
    // previously negotiated generic features, nor infer authority from a name.
    // A mixed manifest also retains both known protected and generic features.
    for order in 0..4 {
        let service = Service::new(1);
        service.workflow_mode.store(true, Ordering::SeqCst);
        let workflows = WorkflowPorts::new(order != 0, order == 3);
        let (worker, counts) = interactive_worker();
        *counts.injected_outcome.lock().unwrap() = Some(ProgramOutcome::Success {
            output: json!({"v":1,"activation_id":"task_1","revision":0,"kind":"complete","output":"done"}),
        });
        let expected = if order == 3 {
            features(&[
                "custom_runtime.v1",
                NATIVE_EFFECT_RESOLUTION_FEATURE,
                PROVIDER_OPERATION_FEATURE,
            ])
        } else {
            features(&["custom_runtime.v1"])
        };
        let mut driver = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .with_workflows(workflows.clone());
        if order == 1 || order == 3 {
            let (audience, identity) = binding();
            driver = driver.with_effect_evidence(audience, identity).unwrap();
        }
        driver = driver.with_runtime_features(expected.clone()).unwrap();
        if order == 2 {
            let (audience, identity) = binding();
            driver = driver.with_effect_evidence(audience, identity).unwrap();
        }
        let mut handle = driver.start();
        wait_for(|| service.accepted_count() == 1).await;
        handle.stop();
        let status = handle.wait().await;
        assert!(status.finished);
        assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
        assert_eq!(workflows.contexts.load(Ordering::SeqCst), 1);
        assert_eq!(
            *counts.observed_runtime.lock().unwrap(),
            vec![(
                expected,
                RuntimeCapabilities {
                    approved_effect_evidence_v2: order != 0
                }
            )]
        );
    }
}
#[test]
fn protected_features_require_binding_and_provider_transport_before_any_session() {
    let service = Service::new(0);
    let make_driver = || {
        let (worker, _) = setup(1);
        DeliveryDriver::new(worker, service.clone(), config()).unwrap()
    };
    for manifest in [
        features(&[NATIVE_EFFECT_RESOLUTION_FEATURE]),
        features(&[NATIVE_EFFECT_RESOLUTION_FEATURE, PROVIDER_OPERATION_FEATURE]),
    ] {
        assert!(matches!(
            make_driver().with_runtime_features(manifest),
            Err(ContractError::Unavailable(_))
        ));
    }
    // Empty and generic manifests are valid without a workflow/evidence adapter.
    assert!(
        make_driver()
            .with_runtime_features(RuntimeFeatures::default())
            .is_ok()
    );
    assert!(
        make_driver()
            .with_runtime_features(features(&["future.v2"]))
            .is_ok()
    );
    for manifest in [
        features(&[PROVIDER_OPERATION_FEATURE]),
        features(&["future.v2", "future.v2"]),
        RuntimeFeatures {
            v: 2,
            supported: vec![],
        },
    ] {
        assert!(matches!(
            make_driver().with_runtime_features(manifest),
            Err(ContractError::InvalidInput(_))
        ));
    }
    let (audience, identity) = binding();
    assert!(matches!(
        make_driver()
            .with_workflows(WorkflowPorts::new(false, true))
            .with_effect_evidence(audience, identity),
        Err(ContractError::Unavailable(_))
    ));
    for provider in [false, true] {
        let (audience, identity) = binding();
        let driver = make_driver()
            .with_workflows(WorkflowPorts::new(true, provider))
            .with_effect_evidence(audience, identity)
            .unwrap();
        let result = driver.with_runtime_features(features(&[
            NATIVE_EFFECT_RESOLUTION_FEATURE,
            PROVIDER_OPERATION_FEATURE,
        ]));
        assert_eq!(result.is_ok(), provider);
    }
    assert!(service.state.lock().unwrap().session.is_none());
}

#[tokio::test]
async fn probes_receive_mixed_features_but_cannot_orchestrate_and_require_effect_authority() {
    // Generic features without binding, binding without provider negotiation,
    // then the complete mixed manifest. Only the final case reaches the runtime.
    for mode in 0..3 {
        let service = Service::new(1);
        service.probe_mode.store(true, Ordering::SeqCst);
        let workflows = WorkflowPorts::new(true, true);
        let (worker, counts) = interactive_worker();
        counts.try_probe_orchestration.store(true, Ordering::SeqCst);
        // Rejecting a check is a defined runtime result and avoids issuing a
        // provider report unrelated to this transport/authority regression.
        *counts.injected_outcome.lock().unwrap() = Some(ProgramOutcome::Success {
            output: serde_json::to_value(ProviderProbeRuntimeResult::Rejected {
                v: 1,
                failure: ProviderProbeRuntimeRejection::ContextInvalid,
            })
            .unwrap(),
        });
        let expected = if mode == 2 {
            features(&[
                "custom_runtime.v1",
                NATIVE_EFFECT_RESOLUTION_FEATURE,
                PROVIDER_OPERATION_FEATURE,
            ])
        } else {
            features(&["custom_runtime.v1"])
        };
        let mut driver = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .with_workflows(workflows.clone());
        if mode != 0 {
            let (audience, identity) = binding();
            driver = driver.with_effect_evidence(audience, identity).unwrap();
        }
        driver = driver.with_runtime_features(expected.clone()).unwrap();
        let mut handle = driver.start();
        wait_for(|| service.accepted_count() == 1).await;
        handle.stop();
        assert!(handle.wait().await.finished);
        let called = usize::from(mode == 2);
        assert_eq!(workflows.probes.load(Ordering::SeqCst), called);
        assert_eq!(workflows.contexts.load(Ordering::SeqCst), 0);
        assert_eq!(counts.executions.load(Ordering::SeqCst), called);
        assert_eq!(
            counts.rejected_probe_requests.load(Ordering::SeqCst),
            called
        );
        let recorded = counts.observed_runtime.lock().unwrap();
        if mode == 2 {
            assert_eq!(
                *recorded,
                vec![(
                    expected,
                    RuntimeCapabilities {
                        approved_effect_evidence_v2: true
                    }
                )]
            );
        } else {
            assert!(recorded.is_empty());
            let state = service.state.lock().unwrap();
            let AttemptReport::Failed(failure) = &state.accepted["att_1"].report else {
                panic!("unsupported probe must fail before execution")
            };
            assert_eq!(failure.error.kind, ErrorKind::Incompatible);
            assert_eq!(failure.phase, Phase::Admission);
        }
    }
}
