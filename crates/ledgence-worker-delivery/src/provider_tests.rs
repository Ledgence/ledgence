use super::*;
use tokio::sync::{mpsc, oneshot};

type Exchange = (
    ProviderProbeReportCommand,
    oneshot::Sender<Result<ProviderReportReply>>,
);
struct Reports(mpsc::UnboundedSender<Exchange>);
impl ProviderOperationClient for Reports {
    fn claim_provider<'a>(
        &'a self,
        _: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt> {
        unreachable!("a probe cannot dispatch")
    }
    fn report_dispatch<'a>(
        &'a self,
        _: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("a probe cannot report dispatch")
    }
    fn probe_context<'a>(
        &'a self,
        _: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext> {
        unreachable!()
    }
    fn observe_provider<'a>(
        &'a self,
        _: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        unreachable!("the host cannot detach observations")
    }
    fn report_probe<'a>(
        &'a self,
        command: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.0.send((command.clone(), tx)).unwrap();
            rx.await.unwrap()
        })
    }
}
fn fixture() -> (ProviderProbeReportCommand, EffectAccess, DeliveryConfig) {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../sdk/python-client/tests/fixtures/provider-effect.json"
    ))
    .unwrap();
    let scope = Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    };
    let command = ProviderProbeReportCommand {
        owner: LeaseOwner {
            scope: scope.clone(),
            task_id: "probe-task".into(),
            attempt_id: "probe-attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "worker".into(),
            consumer_id: 0,
        },
        ticket: ProviderOperationTicket {
            occurrence: EffectOccurrence {
                scope: scope.clone(),
                workflow_id: "workflow".into(),
                activation_id: "origin".into(),
                approval_key: "approved".into(),
            },
            binding_digest: ledgence_worker_api::Digest(format!("sha256:{}", "a".repeat(64))),
        },
        generation: 3,
        source_id: "source".into(),
        report_id: "report".into(),
        report: ProviderReport {
            reported_target: serde_json::from_value(
                fixture["binding"]["approval"]["provider_binding"]["target"].clone(),
            )
            .unwrap(),
            provider_reference: None,
            provider_request_id: None,
            knowledge: ProviderKnowledge::Pending,
        },
    };
    let access = EffectAccess {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: scope.clone(),
        },
        identity: PrincipalIdentity {
            issuer: "worker-issuer".into(),
            subject: "worker".into(),
        },
    };
    let mut config = DeliveryConfig::new(scope, "probes");
    config.retry_delay = Duration::from_millis(1);
    (command, access, config)
}
fn receipt(command: &ProviderProbeReportCommand, access: &EffectAccess) -> ProviderReportReceipt {
    ProviderReportReceipt {
        ticket: command.ticket.clone(),
        source_id: command.source_id.clone(),
        source: ProviderSourceReference::Probe {
            probe_id: "probe".into(),
            generation: command.generation,
            task_id: command.owner.task_id.clone(),
            attempt_id: command.owner.attempt_id.clone(),
        },
        report_id: command.report_id.clone(),
        report_digest: command.report.digest().unwrap(),
        worker: EffectWorkerIdentity {
            audience: access.audience.clone(),
            issuer: access.identity.issuer.clone(),
            subject: access.identity.subject.clone(),
            credential_id: "rotated-credential".into(),
        },
        accepted_at: 1,
        recorded_revision: 4,
        accepted_via: ProviderReportIngress::LiveProbe,
        already_accepted: true,
    }
}
#[tokio::test]
async fn lost_probe_ack_retries_only_identical_report_and_keeps_original_ingress() {
    let (command, access, config) = fixture();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let service = Reports(tx);
    let control = RunControl::new(Duration::from_secs(5));
    let send = submit_probe_report(&service, &config, &access, "probe", &command, &control);
    let backend = async {
        let (first, reply) = rx.recv().await.unwrap();
        reply
            .send(Err(ContractError::Unavailable("response lost".into())))
            .unwrap();
        let (second, reply) = rx.recv().await.unwrap();
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        let mut saved = receipt(&second, &access);
        saved.accepted_via = ProviderReportIngress::EvidenceOnly;
        reply
            .send(Ok(ProviderReportReply::ExistingReport {
                original_receipt: saved,
            }))
            .unwrap();
    };
    let (reply, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(send, backend)
    })
    .await
    .unwrap();
    assert!(matches!(
        reply.unwrap(),
        ProviderReportReply::ExistingReport { .. }
    ));
    assert!(rx.try_recv().is_err());
}
#[tokio::test]
async fn probe_receipt_must_match_exact_source_worker_and_facts_without_retry() {
    for field in [
        "probe",
        "worker",
        "authority",
        "report",
        "digest",
        "attempt",
        "generation",
    ] {
        let (command, access, config) = fixture();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let service = Reports(tx);
        let control = RunControl::new(Duration::from_secs(5));
        let send = submit_probe_report(&service, &config, &access, "probe", &command, &control);
        let backend = async {
            let (actual, reply) = rx.recv().await.unwrap();
            let mut saved = receipt(&actual, &access);
            let ProviderSourceReference::Probe {
                probe_id,
                attempt_id,
                generation,
                ..
            } = &mut saved.source
            else {
                unreachable!()
            };
            match field {
                "probe" => *probe_id = "foreign".into(),
                "worker" => saved.worker.subject = "foreign".into(),
                "authority" => saved.worker.audience.authority_id = "foreign".into(),
                "report" => saved.report_id = "foreign".into(),
                "digest" => {
                    saved.report_digest =
                        ledgence_worker_api::Digest(format!("sha256:{}", "b".repeat(64)))
                }
                "attempt" => *attempt_id = "foreign".into(),
                "generation" => *generation += 1,
                _ => unreachable!(),
            }
            reply
                .send(Ok(ProviderReportReply::Accepted { receipt: saved }))
                .unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(send, backend)
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol, "{field}");
        assert!(rx.try_recv().is_err());
    }
}
#[tokio::test]
async fn cancellation_during_probe_report_delivery_never_acknowledges_or_resends() {
    let (command, access, config) = fixture();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let service = Reports(tx);
    let control = RunControl::new(Duration::from_secs(5));
    let send = submit_probe_report(&service, &config, &access, "probe", &command, &control);
    let cancel = async {
        let (_, reply) = rx.recv().await.unwrap();
        control.cancel();
        reply
    };
    let (result, held_reply) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(send, cancel) })
            .await
            .unwrap();
    assert_eq!(result.unwrap_err().kind, ErrorKind::Cancelled);
    assert!(held_reply.is_closed());
    assert!(rx.try_recv().is_err());
}

#[test]
fn report_delivery_failure_retains_the_completed_probe_observations() {
    let execution: ExecutionReport = serde_json::from_value(json!({
        "source":"urn:ledgence:orchestrator","event_id":"event","tenant_id":"tenant",
        "namespace":"tests","run_id":"run","workflow_id":"workflow","task_id":"probe-task",
        "attempt_id":"attempt","attempt_no":1,"program":{"id":"probe","version":"1.0.0"},
        "digest":format!("sha256:{}","a".repeat(64)),"process_id":17,"reused_process":true,
        "outcome":{"status":"success","output":null},"elapsed_ms":4,
        "observations":{"runtime_started_at_ms":12,"runtime_elapsed_us":4000,
        "process_cpu_user_us":500,"process_cpu_system_us":100,"process_lifetime_peak_rss_bytes":4096,
        "local_steps":[],"local_steps_truncated":false}
    })).unwrap();
    let expected_context = serde_json::to_value(&execution.context).unwrap();
    let expected_observations = execution.observations.clone();
    for (kind, expected) in [
        (
            ErrorKind::TimedOut,
            ProviderProbeFailureKind::InvocationTimeout,
        ),
        (ErrorKind::Protocol, ProviderProbeFailureKind::ResultInvalid),
    ] {
        let (report, classification) = failed_execution(
            execution.clone(),
            Error::new(kind, "provider report acknowledgement lost or invalid"),
        );
        let AttemptReport::Failed(failure) = report else {
            panic!("failure was acknowledged as success")
        };
        assert_eq!(
            serde_json::to_value(failure.context).unwrap(),
            expected_context
        );
        assert_eq!(failure.observations, expected_observations);
        assert_eq!(failure.phase, Phase::Execution);
        assert!(failure.execution_may_have_started);
        assert_eq!(classification, Some(expected));
    }
}
