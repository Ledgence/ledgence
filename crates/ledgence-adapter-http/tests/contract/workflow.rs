use super::*;
impl ApprovedEffects for Mock {
    fn claim_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectClaimCommand,
    ) -> ContractFuture<'a, ApprovedEffectClaimReceipt> {
        Box::pin(async move { self.reply("effect_claim", command) })
    }

    fn confirm_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectConfirmCommand,
    ) -> ContractFuture<'a, ApprovedEffectConfirmReceipt> {
        Box::pin(async move { self.reply("effect_confirm", command) })
    }
}

impl WorkflowService for Mock {
    fn provider_operations(&self) -> Result<&dyn ProviderOperations> {
        if self.evidence_capabilities.load(Ordering::SeqCst) & 8 == 8 {
            Ok(self)
        } else {
            Err(ContractError::Unavailable(
                "provider operations unavailable".into(),
            ))
        }
    }
    fn effect_resolution(&self) -> Result<&dyn EffectResolution> {
        if self.evidence_capabilities.load(Ordering::SeqCst) & 4 == 4 {
            Ok(self)
        } else {
            Err(ContractError::Unavailable(
                "native resolution unavailable".into(),
            ))
        }
    }
    fn approved_effect_evidence(&self) -> Result<&dyn ApprovedEffectEvidence> {
        if self.evidence_capabilities.load(Ordering::SeqCst) & 1 == 1 {
            Ok(self)
        } else {
            Err(ContractError::Unavailable(
                "effect evidence unavailable".into(),
            ))
        }
    }
    fn effect_evidence_reader(&self) -> Result<&dyn EffectEvidenceReader> {
        if self.evidence_capabilities.load(Ordering::SeqCst) & 2 == 2 {
            Ok(self)
        } else {
            Err(ContractError::Unavailable(
                "effect reader unavailable".into(),
            ))
        }
    }
    fn operator_access(&self) -> Result<&dyn OperatorAccess> {
        self.operator_access
            .get()
            .map(Arc::as_ref)
            .ok_or_else(|| ContractError::Unavailable("operator access unsupported".into()))
    }
    fn approved_effects(&self) -> Result<&dyn ApprovedEffects> {
        if self.protected_writes_disabled.load(Ordering::SeqCst) {
            Err(ContractError::InvalidInput(
                "protected writes unavailable".into(),
            ))
        } else {
            Ok(self)
        }
    }
    fn approval<'a>(
        &'a self,
        scope: &'a Scope,
        workflow_id: &'a str,
        key: &'a str,
    ) -> ContractFuture<'a, ApprovalSnapshot> {
        Box::pin(async move { self.reply("approval", json!([scope, workflow_id, key])) })
    }
    fn approval_inbox<'a>(
        &'a self,
        query: &'a ApprovalInboxQuery,
    ) -> ContractFuture<'a, ApprovalInboxPage> {
        Box::pin(async move { self.reply("approval_inbox", query) })
    }
    fn list_approvals<'a>(
        &'a self,
        scope: &'a Scope,
        workflow_id: &'a str,
        after_key: Option<&'a str>,
        limit: u32,
    ) -> ContractFuture<'a, ApprovalPage> {
        Box::pin(
            async move { self.reply("approvals", json!([scope, workflow_id, after_key, limit])) },
        )
    }
    fn decide_approval<'a>(
        &'a self,
        command: &'a ApprovalDecisionCommand,
    ) -> ContractFuture<'a, ApprovalDecisionReceipt> {
        Box::pin(async move { self.reply("decide_approval", command) })
    }

    fn fork_workflow<'a>(
        &'a self,
        command: &'a WorkflowForkCommand,
    ) -> ContractFuture<'a, WorkflowForkReceipt> {
        Box::pin(async move { self.reply("wf_fork", command) })
    }
    fn submit_workflow<'a>(&'a self, c: &'a SubmitCommand) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move { self.reply("wf_submit", c) })
    }
    fn send_workflow_event<'a>(
        &'a self,
        command: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt> {
        Box::pin(async move { self.reply("wf_event", command) })
    }
    fn workflow_status<'a>(
        &'a self,
        s: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move { self.reply("wf_status", json!([s, id])) })
    }
    fn workflow_result<'a>(
        &'a self,
        s: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowResult> {
        Box::pin(async move { self.reply("wf_result", json!([s, id])) })
    }
    fn activation_context<'a>(
        &'a self,
        o: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        Box::pin(async move { self.reply("wf_context", o) })
    }
    fn record_local_result<'a>(
        &'a self,
        c: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        Box::pin(async move { self.reply("wf_local", c) })
    }
    fn cancel_workflow<'a>(
        &'a self,
        s: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move { self.reply("wf_cancel", json!([s, id])) })
    }
}
pub(super) fn snapshot() -> WorkflowSnapshot {
    WorkflowSnapshot {
        workflow_id: "workflow +%/é".into(),
        parent_workflow_id: None,
        root_workflow_id: None,
        scope: scope(),
        state: WorkflowState::Running,
        revision: 0,
        activation_id: Some(owner().task_id),
        submitted_at: 10,
        terminal_at: None,
        correlation_key: Some("invoice:42".into()),
    }
}
fn context() -> WorkflowActivationContext {
    WorkflowActivationContext {
        v: 1,
        workflow_id: snapshot().workflow_id,
        parent_workflow_id: None,
        root_workflow_id: None,
        activation_id: owner().task_id,
        revision: 0,
        continuation: "start".into(),
        state: Value::Null,
        inputs: Default::default(),
        local_steps: vec![],
        wake: None,
    }
}

fn scoped_wake_context(provider: bool, scope: Scope) -> WorkflowActivationContext {
    let mut value = context();
    value.revision = 1;
    let occurrence = EffectOccurrence {
        scope,
        workflow_id: value.workflow_id.clone(),
        activation_id: "effect_origin".into(),
        approval_key: "approval".into(),
    };
    let output = json!({"private_result": "foreign_scope_payload"});
    value.wake = Some(if provider {
        WorkflowWake::ProviderOperation(Box::new(ProviderWake {
            key: "provider_wait".into(),
            occurrence,
            parking_activation_id: "parked".into(),
            parking_revision: 0,
            resumed_activation_id: value.activation_id.clone(),
            resumed_revision: 1,
            selected_at: 20,
            selected_effect_revision: 2,
            selection: ProviderSelection::ProviderSucceeded {
                report_ref: ProviderReportReference {
                    report_id: "report".into(),
                    source_id: "source".into(),
                    sequence: 1,
                    recorded_revision: 2,
                    recorded_at: 10,
                    knowledge: ProviderKnowledgeKind::Succeeded,
                },
                result: output,
            },
        }))
    } else {
        WorkflowWake::EffectResolution(Box::new(EffectResolutionWake {
            key: "effect_wait".into(),
            occurrence,
            parking_activation_id: "parked".into(),
            parking_revision: 0,
            resumed_activation_id: value.activation_id.clone(),
            resumed_revision: 1,
            resolution_id: "resolution".into(),
            accepted_at: 10,
            accepted_revision: 2,
            basis_revision: 1,
            selected_at: 20,
            selected_effect_revision: 2,
            outcome: EffectResolutionOutcome::RecordedSuccess { result: output },
        }))
    });
    value.validate().unwrap();
    value
}

async fn setup(mock: &Arc<Mock>) -> Running {
    start(server::router_with_workflows(
        mock.clone(),
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(ledgence_worker_api::NoopTraceBridge),
    ))
    .await
}

#[tokio::test]
async fn workflow_operations_preserve_identity_payload_and_pending_vs_null() {
    let mock = Arc::new(Mock::default());
    mock.set("wf_submit", Ok(snapshot()));
    mock.set("wf_status", Ok(snapshot()));
    mock.set(
        "wf_result",
        Ok(WorkflowResult {
            workflow: snapshot(),
            outcome: None,
        }),
    );
    mock.set("wf_context", Ok(context()));
    mock.set("wf_cancel", Ok(snapshot()));
    mock.set(
        "wf_local",
        Ok(LocalResultReceipt {
            key: "local".into(),
            already_accepted: false,
        }),
    );
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let command = submit(json!({"unowned": [null,-0.0,18446744073709551615u64]}));
    let accepted = client.submit_workflow(&command).await.unwrap();
    let id = &accepted.workflow_id;
    assert_eq!(
        client
            .workflow_status(&scope(), id)
            .await
            .unwrap()
            .workflow_id,
        *id
    );
    assert!(
        client
            .workflow_result(&scope(), id)
            .await
            .unwrap()
            .outcome
            .is_none()
    );
    assert_eq!(
        client
            .activation_context(&owner())
            .await
            .unwrap()
            .activation_id,
        owner().task_id
    );
    let local = LocalResultCommand {
        owner: owner(),
        record: LocalStepRecord {
            key: "local".into(),
            callable: "app:fetch".into(),
            input: json!({"x":1.0}),
            output: Value::Null,
        },
    };
    assert!(
        !client
            .record_local_result(&local)
            .await
            .unwrap()
            .already_accepted
    );
    client.cancel_workflow(&scope(), id).await.unwrap();
    let mut terminal = snapshot();
    terminal.state = WorkflowState::Succeeded;
    terminal.activation_id = None;
    terminal.terminal_at = Some(20);
    mock.set(
        "wf_result",
        Ok(WorkflowResult {
            workflow: terminal,
            outcome: Some(WorkflowOutcome::Succeeded {
                output: Value::Null,
            }),
        }),
    );
    assert!(matches!(
        client.workflow_result(&scope(), id).await.unwrap().outcome,
        Some(WorkflowOutcome::Succeeded {
            output: Value::Null
        })
    ));
    let calls = mock.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .find(|(name, _)| *name == "wf_submit")
            .unwrap()
            .1,
        serde_json::to_value(command).unwrap()
    );
    assert_eq!(
        calls
            .iter()
            .find(|(name, _)| *name == "wf_local")
            .unwrap()
            .1,
        serde_json::to_value(local).unwrap()
    );
}

#[tokio::test]
async fn workflow_identity_and_invalid_context_replies_cannot_be_accepted() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let mut wrong = snapshot();
    wrong.workflow_id = "another".into();
    mock.set("wf_status", Ok(wrong));
    assert!(matches!(
        client
            .workflow_status(&scope(), &snapshot().workflow_id)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    let mut invalid = context();
    invalid.v = 2;
    mock.set("wf_context", Ok(invalid));
    assert!(client.activation_context(&owner()).await.is_err());
    mock.set(
        "wf_local",
        Ok(LocalResultReceipt {
            key: "wrong".into(),
            already_accepted: true,
        }),
    );
    let local = LocalResultCommand {
        owner: owner(),
        record: LocalStepRecord {
            key: "local".into(),
            callable: "app:fetch".into(),
            input: Value::Null,
            output: Value::Null,
        },
    };
    assert!(matches!(
        client.record_local_result(&local).await,
        Err(ContractError::Unavailable(_))
    ));
}

#[tokio::test]
async fn activation_context_server_rejects_foreign_typed_wakes_before_serialization() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    for provider in [false, true] {
        for changed in ["none", "tenant", "namespace"] {
            let mut wake_scope = scope();
            match changed {
                "tenant" => wake_scope.tenant_id = "foreign".into(),
                "namespace" => wake_scope.namespace = "foreign".into(),
                _ => {}
            }
            let expected = scoped_wake_context(provider, wake_scope);
            mock.set("wf_context", Ok(&expected));
            let reply = raw
                .post(format!("{}/v1/workflows/activations/context", running.url))
                .header("content-type", "application/json")
                .body(exact(owner()))
                .send()
                .await
                .unwrap();
            let status = reply.status();
            let body = reply.text().await.unwrap();
            if changed == "none" {
                assert_eq!(status, 200);
                assert_eq!(
                    serde_json::from_str::<Value>(&body).unwrap(),
                    serde_json::to_value(&expected).unwrap()
                );
            } else {
                assert_eq!(status, 503, "provider={provider}, {changed}");
                assert!(!body.contains("foreign_scope_payload"));
                assert_eq!(
                    serde_json::from_str::<Value>(&body).unwrap()["code"],
                    "unavailable"
                );
            }
        }
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 6);
}

#[tokio::test]
async fn activation_context_client_rejects_foreign_typed_wakes_from_successful_raw_http() {
    for provider in [false, true] {
        for changed in ["none", "tenant", "namespace"] {
            let mut wake_scope = scope();
            match changed {
                "tenant" => wake_scope.tenant_id = "foreign".into(),
                "namespace" => wake_scope.namespace = "foreign".into(),
                _ => {}
            }
            let expected = scoped_wake_context(provider, wake_scope);
            let (running, count) =
                raw_response(response(200, "application/json", &exact(&expected)), None).await;
            let actual = HttpTaskService::new(&running.url)
                .unwrap()
                .activation_context(&owner())
                .await;
            if changed == "none" {
                assert_eq!(exact(actual.unwrap()), exact(expected));
            } else {
                assert!(
                    matches!(actual, Err(ContractError::Unavailable(_))),
                    "provider={provider}, {changed}: {actual:?}"
                );
            }
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn workflow_routes_reject_malformed_commands_and_unconfigured_capability() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let malformed = raw
        .post(format!("{}/v1/workflows/local-results", running.url))
        .header("content-type", "application/json")
        .body(r#"{"owner":{},"record":{"key":"a","key":"b"}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    assert!(mock.calls.lock().unwrap().is_empty());
    for query in [
        "tenant_id=t&namespace=n&workflow_id=w&task_id=t",
        "tenant_id=t&namespace=n&workflow_id=w&workflow_id=x",
        "tenant_id=t&namespace=n",
    ] {
        assert_eq!(
            raw.get(format!("{}/v1/workflows/status?{query}", running.url))
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    let disabled = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&disabled.url).unwrap();
    assert!(matches!(
        client.submit_workflow(&submit(Value::Null)).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_post_mutation_workflow_replies_remain_uncertain() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let raw = reqwest::Client::new();
    let mut malformed = snapshot();
    malformed.activation_id = None;
    mock.set("wf_submit", Ok(malformed.clone()));
    mock.set("wf_cancel", Ok(malformed));
    for (path, body) in [
        (
            "/v1/workflows",
            serde_json::to_value(submit(Value::Null)).unwrap(),
        ),
        (
            "/v1/workflows/cancel",
            json!({"scope":scope(),"workflow_id":snapshot().workflow_id}),
        ),
    ] {
        let reply = raw
            .post(format!("{}{path}", running.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(
            reply.status(),
            503,
            "post-mutation validation must retain uncertainty"
        );
    }
    let client = HttpTaskService::new(&running.url).unwrap();
    for changed in ["scope", "correlation"] {
        let mut wrong = snapshot();
        if changed == "scope" {
            wrong.scope.namespace = "another".into();
        } else {
            wrong.correlation_key = Some("another".into());
        }
        mock.set("wf_submit", Ok(wrong));
        assert!(matches!(
            client.submit_workflow(&submit(Value::Null)).await,
            Err(ContractError::Unavailable(_))
        ));
    }
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        4,
        "the accepted service calls were not reissued"
    );
}

#[path = "workflow_events.rs"]
mod events;

#[path = "workflow_children.rs"]
mod children;

#[path = "workflow_forks.rs"]
mod forks;

#[path = "workflow_approved_effect.rs"]
mod approved_effect;

#[path = "workflow_effect_confirmation.rs"]
mod effect_confirmation;

#[path = "workflow_approvals.rs"]
mod approvals;
