//! Real network regressions for typed exposure, durable receipts and identities.
use super::*;
use ledgence_orchestration_api::console::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "progress.rs"]
mod progress;

const CANARY: &str = "private-payload-must-not-leak";
#[path = "admission.rs"]
mod admission;
#[path = "effect_evidence.rs"]
mod effect_evidence;
#[path = "effect_resolution.rs"]
mod effect_resolution;
#[path = "provider.rs"]
mod provider;
#[path = "provider_admission.rs"]
mod provider_admission;
#[path = "queue_policies.rs"]
mod queue_policies;
struct Verifier {
    audience: AccessAudience,
}
impl CredentialVerifier for Verifier {
    fn audience(&self) -> &AccessAudience {
        &self.audience
    }
    fn verify(&self, credential: &str, _: u64) -> Result<VerifiedPrincipal> {
        let capabilities = match credential {
            "operator" => vec![
                AccessCapability::MetadataRead,
                AccessCapability::SensitiveRead,
                AccessCapability::ApprovalDecide,
            ],
            "reader" => vec![AccessCapability::MetadataRead],
            "source-investigator" => vec![AccessCapability::SensitiveRead],
            "submitter" => vec![AccessCapability::ExecutionSubmit],
            "resource-operator" => vec![AccessCapability::ResourceControl],
            "resolver" => vec![AccessCapability::EffectResolve],
            "worker" => vec![AccessCapability::WorkerExecute],
            "expiring" => vec![
                AccessCapability::SensitiveRead,
                AccessCapability::ApprovalDecide,
            ],
            _ => return Err(ContractError::Unauthenticated),
        };
        VerifiedPrincipal::new(
            PrincipalIdentity {
                issuer: self.audience.issuer(),
                subject: credential.into(),
            },
            self.audience.clone(),
            "credential".into(),
            if credential == "worker" {
                PrincipalClass::Worker
            } else {
                PrincipalClass::Operator
            },
            if credential == "expiring" {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 300
            } else {
                4_102_444_800_000
            },
            capabilities,
        )
    }
}
#[derive(Default)]
struct Auditor {
    observations: Mutex<Vec<SensitiveObservation>>,
    accepted: Mutex<Vec<AuditEvent>>,
    mode: AtomicUsize,
}
impl OperatorAccess for Auditor {
    fn decide_verified<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ApprovalDecisionCommand,
        _: &'a OperatorRequest,
    ) -> ContractFuture<'a, AuditedDecision> {
        Box::pin(async move {
            let mut snapshot = approval();
            snapshot.status = ApprovalStatus::Approved;
            snapshot.decision = Some(ApprovalDecisionRecord {
                decision_id: command.decision_id.clone(),
                decision: command.decision,
                reviewer: command.reviewer.clone(),
                reason: command.reason.clone(),
                decided_at: 20,
            });
            let event = AuditEvent {
                event_id: "acceptance-event".into(),
                recorded_at: 20,
                audience: principal.audience().clone(),
                actor: AuditActor::from_principal(principal),
                operation: AuditOperation::accepted(command),
            };
            self.accepted.lock().unwrap().push(event.clone());
            Ok(AuditedDecision {
                receipt: ApprovalDecisionReceipt {
                    approval: snapshot,
                    already_accepted: false,
                },
                attribution: DecisionAttribution::Verified {
                    event: Box::new(event),
                },
            })
        })
    }
    fn record_sensitive_response<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        observation: &'a SensitiveObservation,
    ) -> ContractFuture<'a, AuditReceipt> {
        Box::pin(async move {
            self.observations.lock().unwrap().push(observation.clone());
            match self.mode.load(Ordering::SeqCst) {
                1 => {
                    return Err(ContractError::Unavailable(
                        "audit storage unavailable".into(),
                    ));
                }
                3 => return Err(ContractError::Unauthenticated),
                _ => {}
            }
            if self.mode.load(Ordering::SeqCst) == 4 {
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            Ok(AuditReceipt {
                already_recorded: false,
                event: AuditEvent {
                    event_id: if self.mode.load(Ordering::SeqCst) == 2 {
                        "unrelated-event".into()
                    } else {
                        observation.request.observation_id().into()
                    },
                    recorded_at: 30,
                    audience: principal.audience().clone(),
                    actor: AuditActor::from_principal(principal),
                    operation: AuditOperation::SensitiveResponse {
                        read: observation.read,
                        resource: observation.resource.clone(),
                    },
                },
            })
        })
    }
    fn query_audit<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        query: &'a AuditQuery,
    ) -> ContractFuture<'a, AuditPage> {
        Box::pin(async move {
            Ok(AuditPage {
                items: self
                    .accepted
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| query.filter.matches(event))
                    .take(query.limit as usize)
                    .cloned()
                    .collect(),
                next_cursor: None,
            })
        })
    }
}
struct Queries;
impl ConsoleQueryService for Queries {
    fn query_console<'a>(
        &'a self,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(async move {
            match query {
                ConsoleQuery::WorkflowInput { workflow_id } => {
                    Ok(ConsoleQueryReply::WorkflowInput(ConsoleWorkflowInput {
                        workflow_id: workflow_id.clone(),
                        data: json!(CANARY),
                        observed_at: 10,
                    }))
                }
                _ => Err(ContractError::NotFound),
            }
        })
    }
    fn inspect_local_step<'a>(
        &'a self,
        lookup: &'a ConsoleLocalStepLookup,
    ) -> ContractFuture<'a, ConsoleLocalStepDetail> {
        Box::pin(async move {
            Ok(ConsoleLocalStepDetail {
                step: ConsoleLocalStep {
                    workflow_id: lookup.workflow_id.clone(),
                    activation_id: lookup.activation_id.clone(),
                    step_key: lookup.step_key.clone(),
                    callable: "module:local".into(),
                    attempt_id: "attempt".into(),
                    accepted_at: 10,
                },
                kind: ConsoleOperationKind::Local,
                version: None,
                arguments: json!(CANARY),
                result: json!(CANARY),
            })
        })
    }
}
fn approval() -> ApprovalSnapshot {
    ApprovalSnapshot {
        provider_binding: None,
        scope: scope(),
        workflow_id: "wf".into(),
        key: "review".into(),
        activation_id: "activation".into(),
        revision: 0,
        action: ApprovalAction {
            name: "fixture:action".into(),
            version: "1".into(),
            arguments: json!({"secret":CANARY}),
        },
        protection: ApprovalProtection::Unspecified,
        proposed_arguments: None,
        created_at: 10,
        deadline: 1000,
        status: ApprovalStatus::Pending,
        decision: None,
        resumed_activation_id: None,
    }
}
fn decision(console: bool) -> Value {
    let mut value = json!({"scope":scope(),"workflow_id":"wf","key":"review","activation_id":"activation",
        "revision":0,"action":approval().action,"decision_id":"decision","decision":"approve","reviewer":"unverified-label","reason":null});
    if console {
        value.as_object_mut().unwrap().remove("scope");
        value["revision"] = json!("0");
    }
    value
}
fn terminal_task(output: Value) -> TaskResult {
    TaskResult {
        task: compact_status(TaskState::Succeeded),
        outcome: Some(TaskOutcome::Succeeded {
            attempt_id: "attempt".into(),
            quiescence: Quiescence::Unconfirmed,
            execution_may_have_started: true,
            output,
        }),
    }
}
fn terminal_workflow(output: Value) -> WorkflowResult {
    let mut snapshot = workflow::snapshot();
    snapshot.state = WorkflowState::Succeeded;
    snapshot.activation_id = None;
    snapshot.terminal_at = Some(20);
    WorkflowResult {
        workflow: snapshot,
        outcome: Some(WorkflowOutcome::Succeeded { output }),
    }
}
struct Fixture {
    running: Running,
    mock: Arc<Mock>,
    auditor: Arc<Auditor>,
}
fn console_services() -> server::console::ConsoleServices {
    let config = SelfHostedInstanceConfig {
        instance_id: "audit".into(),
        name: "Audit".into(),
        scope: scope(),
        suggested_queues: vec!["private-operator-queue".into()],
        allowed_origins: vec!["https://audit.example".into()],
    };
    let verifier = Arc::new(Verifier {
        audience: AccessAudience {
            authority_id: "authority".into(),
            instance_id: config.instance_id.clone(),
            scope: scope(),
        },
    });
    server::console::ConsoleServices::new(config, "test".into(), Arc::new(Queries), None, None)
        .unwrap()
        .with_access(verifier, false)
        .unwrap()
        .with_operator_audit()
        .unwrap()
}
async fn setup() -> Fixture {
    let mock = Arc::new(Mock::default());
    let auditor = Arc::new(Auditor::default());
    assert!(mock.operator_access.set(auditor.clone()).is_ok());
    mock.set("inspect", Ok(task(json!(CANARY))));
    mock.set("result", Ok(terminal_task(json!(CANARY))));
    mock.set("wf_result", Ok(terminal_workflow(json!(CANARY))));
    mock.set("approval", Ok(approval()));
    mock.set(
        "approvals",
        Ok(ApprovalPage {
            items: vec![approval()],
            next_cursor: None,
        }),
    );
    let router = server::router_with_console(
        mock.clone(),
        mock.clone(),
        mock.clone(),
        console_services(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(ledgence_worker_api::NoopTraceBridge),
    )
    .unwrap();
    Fixture {
        running: start(router).await,
        mock,
        auditor,
    }
}
async fn request(f: &Fixture, path: &str, body: Option<Value>, token: &str) -> (u16, Value) {
    let mut url = reqwest::Url::parse(&format!("{}{path}", f.running.url)).unwrap();
    let console = path.starts_with("/v1/console/");
    if body.is_none() {
        let mut query = url.query_pairs_mut();
        if !console {
            query
                .append_pair("tenant_id", &scope().tenant_id)
                .append_pair("namespace", &scope().namespace);
        }
        if path.contains("/tasks/") {
            query.append_pair("task_id", "..");
        }
        if path.contains("/workflows/") {
            let id = if path.ends_with("/result") {
                workflow::snapshot().workflow_id
            } else {
                "wf".into()
            };
            query.append_pair("workflow_id", &id);
        }
        if path.contains("/local-steps/") {
            query
                .append_pair("activation_id", "activation")
                .append_pair("step_key", "step");
        }
    }
    let client = reqwest::Client::new();
    let builder = if let Some(body) = body {
        client
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_string())
    } else {
        client.get(url)
    };
    let response = builder.bearer_auth(token).send().await.unwrap();
    let status = response.status().as_u16();
    (
        status,
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap(),
    )
}
fn requests() -> Vec<(&'static str, Option<Value>)> {
    let mut result = vec![
        ("/v1/tasks/inspect", None),
        ("/v1/tasks/result", None),
        ("/v1/workflows/result", None),
        ("/v1/console/tasks/inspect", None),
        ("/v1/console/tasks/result", None),
        ("/v1/console/workflows/result", None),
        ("/v1/console/workflows/input", None),
        ("/v1/console/workflows/local-steps/inspect", None),
    ];
    for (inspect, list, decide, console) in [
        (
            "/v1/workflows/approvals/inspect",
            "/v1/workflows/approvals/list",
            "/v1/workflows/approvals/decide",
            false,
        ),
        (
            "/v1/console/approvals/inspect",
            "/v1/console/approvals/list",
            "/v1/console/approvals/decide",
            true,
        ),
    ] {
        let mut reference = json!({"scope":scope(),"workflow_id":"wf","key":"review"});
        let mut page = json!({"scope":scope(),"workflow_id":"wf","after_key":null,"limit":10});
        if console {
            reference.as_object_mut().unwrap().remove("scope");
            page.as_object_mut().unwrap().remove("scope");
        }
        result.extend([
            (inspect, Some(reference)),
            (list, Some(page)),
            (decide, Some(decision(console))),
        ]);
    }
    result
}

#[tokio::test]
async fn all_fourteen_payload_aliases_require_durable_audit_before_bytes() {
    let f = setup().await;
    f.auditor.mode.store(1, Ordering::SeqCst);
    for (path, body) in requests() {
        let (status, value) = request(&f, path, body, "operator").await;
        assert_eq!(status, 503, "{path}: {value}");
        assert!(!value.to_string().contains(CANARY), "{path}");
    }
    assert_eq!(f.auditor.observations.lock().unwrap().len(), 14);
    f.auditor.mode.store(0, Ordering::SeqCst);
    for (path, body) in requests() {
        let (status, value) = request(&f, path, body, "operator").await;
        assert_eq!(status, 200, "{path}: {value}");
        assert!(value.to_string().contains(CANARY), "{path}");
    }
    assert_eq!(f.auditor.observations.lock().unwrap().len(), 28);
    assert!(
        !f.mock
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(operation, _)| *operation == "decide_approval")
    );
}

#[tokio::test]
async fn pending_result_polls_have_no_audit_writes_but_still_require_sensitive_authority() {
    let f = setup().await;
    f.mock.set(
        "result",
        Ok(TaskResult {
            task: compact_status(TaskState::Queued),
            outcome: None,
        }),
    );
    f.mock.set(
        "wf_result",
        Ok(WorkflowResult {
            workflow: workflow::snapshot(),
            outcome: None,
        }),
    );
    let paths = [
        "/v1/tasks/result",
        "/v1/console/tasks/result",
        "/v1/workflows/result",
        "/v1/console/workflows/result",
    ];
    for path in paths {
        for _ in 0..3 {
            assert_eq!(request(&f, path, None, "operator").await.0, 200, "{path}");
        }
        assert_eq!(request(&f, path, None, "reader").await.0, 403, "{path}");
    }
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    f.mock.set(
        "result",
        Ok(TaskResult {
            task: compact_status(TaskState::Succeeded),
            outcome: None,
        }),
    );
    let mut malformed = terminal_workflow(Value::Null);
    malformed.outcome = None;
    f.mock.set("wf_result", Ok(malformed));
    for path in paths {
        assert_ne!(request(&f, path, None, "operator").await.0, 200, "{path}");
    }
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    f.mock.set("result", Ok(terminal_task(Value::Null)));
    f.mock.set("wf_result", Ok(terminal_workflow(Value::Null)));
    for path in paths {
        assert_eq!(request(&f, path, None, "operator").await.0, 200, "{path}");
    }
    assert_eq!(f.auditor.observations.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn invalid_receipts_wrong_identity_and_postcommit_auth_errors_never_leak_payload() {
    let f = setup().await;
    let mut wrong = task(json!(CANARY));
    wrong.task_id = "different".into();
    f.mock.set("inspect", Ok(wrong));
    assert_eq!(
        request(&f, "/v1/tasks/inspect", None, "operator").await.0,
        503
    );
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    f.auditor.mode.store(2, Ordering::SeqCst);
    let (status, value) = request(&f, "/v1/tasks/result", None, "operator").await;
    assert_eq!(status, 503);
    assert!(!value.to_string().contains(CANARY));
    f.auditor.mode.store(3, Ordering::SeqCst);
    assert_eq!(
        request(&f, "/v1/tasks/result", None, "operator").await.0,
        401
    );
    for (path, console) in [
        ("/v1/workflows/approvals/decide", false),
        ("/v1/console/approvals/decide", true),
    ] {
        let (status, value) = request(&f, path, Some(decision(console)), "operator").await;
        assert_eq!(status, 503);
        assert!(!value.to_string().contains(CANARY));
    }
    assert_eq!(f.auditor.accepted.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn audit_queries_are_metadata_only_and_preserve_console_revision_contract() {
    let f = setup().await;
    assert_eq!(
        request(
            &f,
            "/v1/workflows/approvals/decide",
            Some(decision(false)),
            "operator"
        )
        .await
        .0,
        200
    );
    let observations = f.auditor.observations.lock().unwrap().len();
    for (path, console) in [
        ("/v1/audit/events", false),
        ("/v1/console/audit/events", true),
    ] {
        let mut query = json!({"scope":scope(),"filter":{"kind":"approval_acceptance","workflow_id":"wf",
            "key":"review","activation_id":"activation","revision":0,"decision_id":"decision"},"cursor":null,"limit":2});
        if console {
            query.as_object_mut().unwrap().remove("scope");
            query["filter"]["revision"] = json!("0");
        }
        let (status, page) = request(&f, path, Some(query), "reader").await;
        assert_eq!(status, 200, "{page}");
        assert_eq!(page["items"][0]["actor"]["subject"], "operator");
        assert_eq!(
            page["items"][0]["operation"]["revision"],
            if console { json!("0") } else { json!(0) }
        );
        assert!(!page.to_string().contains(CANARY));
    }
    assert_eq!(f.auditor.observations.lock().unwrap().len(), observations);
}

#[tokio::test]
async fn expiry_during_audit_holds_payload_and_postcommit_decision_stays_uncertain() {
    let f = setup().await;
    f.auditor.mode.store(4, Ordering::SeqCst);
    let (status, value) = request(&f, "/v1/tasks/result", None, "expiring").await;
    assert_eq!(status, 401);
    assert!(!value.to_string().contains(CANARY));
    let (status, value) = request(
        &f,
        "/v1/workflows/approvals/decide",
        Some(decision(false)),
        "expiring",
    )
    .await;
    assert_eq!(status, 503);
    assert!(!value.to_string().contains(CANARY));
    assert_eq!(f.auditor.accepted.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cancelled_and_failed_results_are_payload_observations_not_pending_polls() {
    let f = setup().await;
    f.mock.set(
        "result",
        Ok(TaskResult {
            task: compact_status(TaskState::Cancelled),
            outcome: Some(TaskOutcome::Cancelled {}),
        }),
    );
    let mut workflow = terminal_workflow(Value::Null);
    workflow.workflow.state = WorkflowState::Cancelled;
    workflow.outcome = Some(WorkflowOutcome::Cancelled {});
    f.mock.set("wf_result", Ok(workflow));
    let paths = [
        "/v1/tasks/result",
        "/v1/console/tasks/result",
        "/v1/workflows/result",
        "/v1/console/workflows/result",
    ];
    for path in paths {
        assert_eq!(request(&f, path, None, "operator").await.0, 200);
    }
    let error = ApplicationError {
        kind: "application".into(),
        message: CANARY.into(),
    };
    f.mock.set(
        "result",
        Ok(TaskResult {
            task: compact_status(TaskState::Failed),
            outcome: Some(TaskOutcome::Failed {
                attempt_id: "attempt".into(),
                quiescence: Quiescence::Unconfirmed,
                execution_may_have_started: true,
                failure: TaskFailure::Application {
                    error: error.clone(),
                },
            }),
        }),
    );
    let mut workflow = terminal_workflow(Value::Null);
    workflow.workflow.state = WorkflowState::Failed;
    workflow.outcome = Some(WorkflowOutcome::Failed { error });
    f.mock.set("wf_result", Ok(workflow));
    for path in paths {
        let (status, value) = request(&f, path, None, "operator").await;
        assert_eq!(status, 200);
        assert!(value.to_string().contains(CANARY));
    }
    assert_eq!(f.auditor.observations.lock().unwrap().len(), 8);
}

#[test]
fn audited_composition_rejects_an_incomplete_capability_before_router_creation() {
    let mock = Arc::new(Mock::default());
    assert!(
        server::router_with_console(
            mock.clone(),
            mock.clone(),
            mock,
            console_services(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(ledgence_worker_api::NoopTraceBridge)
        )
        .is_err()
    );
}
