use super::*;

async fn resolve(
    service: &ApplicationService,
    work: &WorkflowWork,
) -> Result<Vec<ResolvedWorkflowChild>> {
    let mut resolved = Vec::new();
    service
        .resolve_workflow_children(work, &mut resolved)
        .await?;
    Ok(resolved)
}

fn work(commands: serde_json::Value) -> WorkflowWork {
    WorkflowWork {
        id: "completion:task_activation".into(),
        token: "lease_1".into(),
        workflow_id: "workflow_1".into(),
        source: WorkflowWorkSource::TaskTerminal {
            task_id: "task_activation".into(),
            activation: true,
        },
        outcome: Some(TaskOutcome::Succeeded {
            attempt_id: "attempt_1".into(),
            quiescence: Quiescence::Confirmed,
            execution_may_have_started: true,
            output: json!({"v":1,"activation_id":"task_activation","revision":0,"kind":"suspend","state":null,"continuation":"next","commands":commands,"until":[]}),
        }),
        resolved_children: vec![],
    }
}
fn child(key: &str) -> serde_json::Value {
    json!({"key":key,"program":command().input.program,"queue":"invoices","data":{},"retry_policy":RetryPolicy::default(),"attempt_timeout_ms":300000})
}

#[tokio::test]
async fn accepted_child_pins_survive_locator_changes_and_batch_resolution_is_shared() {
    let mut fixture = Fixture::new();
    let mut item = work(json!([child("registered"), child("new_a"), child("new_b")]));
    item.resolved_children.push(ResolvedWorkflowChild {
        kind: WorkflowChildKind::Task,
        key: "registered".into(),
        descriptor: descriptor('a'),
    });
    let service = fixture.service.clone();
    let pending = tokio::spawn(async move { resolve(&service, &item).await });
    fixture
        .resolution()
        .await
        .send(Ok(descriptor('b')))
        .unwrap();
    let resolved = bounded(pending).await.unwrap().unwrap();
    assert_eq!(resolved.len(), 3);
    assert_eq!(resolved[0].descriptor, descriptor('a'));
    assert_eq!(resolved[1].descriptor, descriptor('b'));
    assert_eq!(resolved[2].descriptor, descriptor('b'));
    assert!(
        fixture.requests.try_recv().is_err(),
        "duplicate program resolution"
    );
}

#[tokio::test]
async fn fully_registered_children_do_not_depend_on_available_program_store() {
    let mut fixture = Fixture::new();
    let mut item = work(json!([child("registered")]));
    item.resolved_children.push(ResolvedWorkflowChild {
        kind: WorkflowChildKind::Task,
        key: "registered".into(),
        descriptor: descriptor('a'),
    });
    let resolved = bounded(resolve(&fixture.service, &item)).await.unwrap();
    assert_eq!(resolved[0].descriptor, descriptor('a'));
    assert!(fixture.requests.try_recv().is_err());
}

#[tokio::test]
async fn ordinary_child_outputs_are_not_decisions_and_invalid_controller_identity_never_resolves() {
    let mut fixture = Fixture::new();
    let mut item = work(json!([child("new")]));
    item.source = WorkflowWorkSource::TaskTerminal {
        task_id: "task_activation".into(),
        activation: false,
    };
    assert!(resolve(&fixture.service, &item).await.unwrap().is_empty());
    item.source = WorkflowWorkSource::TaskTerminal {
        task_id: "different_controller".into(),
        activation: true,
    };
    assert_eq!(
        resolve(&fixture.service, &item).await.unwrap_err(),
        ContractError::Conflict
    );
    assert!(fixture.requests.try_recv().is_err());
}

#[tokio::test]
async fn child_kind_is_an_immutable_binding_and_workflow_completion_never_resolves_programs() {
    let mut fixture = Fixture::new();
    let mut command = child("registered");
    command["kind"] = json!("workflow");
    let mut item = work(json!([command]));
    item.resolved_children.push(ResolvedWorkflowChild {
        kind: WorkflowChildKind::Workflow,
        key: "registered".into(),
        descriptor: descriptor('a'),
    });
    let resolved = resolve(&fixture.service, &item).await.unwrap();
    assert_eq!(resolved[0].kind, WorkflowChildKind::Workflow);
    item.resolved_children[0].kind = WorkflowChildKind::Task;
    assert_eq!(
        resolve(&fixture.service, &item).await.unwrap_err(),
        ContractError::Conflict
    );
    item.source = WorkflowWorkSource::WorkflowTerminal {
        workflow_id: "child".into(),
    };
    assert!(resolve(&fixture.service, &item).await.unwrap().is_empty());
    assert!(fixture.requests.try_recv().is_err());
}

#[test]
fn public_workflow_submission_cannot_adopt_owned_or_wrong_scope_store_replies() {
    let command = command();
    let root = WorkflowSnapshot {
        workflow_id: "root".into(),
        scope: Scope {
            tenant_id: command.input.tenant_id.clone(),
            namespace: command.input.namespace.clone(),
        },
        parent_workflow_id: None,
        root_workflow_id: None,
        state: WorkflowState::Running,
        revision: 0,
        activation_id: Some("activation".into()),
        submitted_at: 1,
        terminal_at: None,
        correlation_key: command.input.correlation_key.clone(),
    };
    super::super::workflow::validate_workflow_submission_reply(&command, root.clone()).unwrap();
    for case in 0..4 {
        let mut reply = root.clone();
        match case {
            0 => {
                reply.parent_workflow_id = Some("parent".into());
                reply.root_workflow_id = Some("ancestor".into());
            }
            1 => reply.scope.namespace = "other".into(),
            2 => reply.correlation_key = Some("other".into()),
            _ => reply.root_workflow_id = Some("ancestor".into()),
        }
        assert!(matches!(
            super::super::workflow::validate_workflow_submission_reply(&command, reply),
            Err(ContractError::Unavailable(_))
        ));
    }
}

#[test]
fn accepted_approval_with_invalid_or_changed_store_receipt_remains_uncertain() {
    let action = ApprovalAction {
        name: "app:refund".into(),
        version: "1".into(),
        arguments: json!({"amount": 50, "currency": "USD"}),
    };
    let command = ApprovalDecisionCommand {
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "billing".into(),
        },
        workflow_id: "workflow".into(),
        key: "refund".into(),
        activation_id: "request".into(),
        revision: 0,
        action: action.clone(),
        decision_id: "review-1".into(),
        decision: ApprovalDecision::Approve,
        reviewer: "operator".into(),
        reason: None,
    };
    let receipt = ApprovalDecisionReceipt {
        approval: ApprovalSnapshot {
            scope: command.scope.clone(),
            workflow_id: command.workflow_id.clone(),
            key: command.key.clone(),
            activation_id: command.activation_id.clone(),
            revision: 0,
            action,
            proposed_arguments: Some(json!({"amount": 100})),
            created_at: 10,
            deadline: 20,
            status: ApprovalStatus::Approved,
            decision: Some(ApprovalDecisionRecord {
                decision_id: command.decision_id.clone(),
                decision: ApprovalDecision::Approve,
                reviewer: command.reviewer.clone(),
                reason: None,
                decided_at: 11,
            }),
            resumed_activation_id: Some("resumed".into()),
        },
        already_accepted: false,
    };
    let validate = super::super::workflow::validate_approval_decision_reply;
    for duplicate in [false, true] {
        let mut accepted = receipt.clone();
        accepted.already_accepted = duplicate;
        assert_eq!(
            validate(&command, accepted).unwrap().already_accepted,
            duplicate
        );
    }
    for case in 0..7 {
        let mut wrong = receipt.clone();
        match case {
            0 => wrong.approval.scope.namespace = "other".into(),
            1 => wrong.approval.activation_id = "other".into(),
            2 => wrong.approval.action.arguments["amount"] = json!(50.0),
            3 => wrong.approval.decision.as_mut().unwrap().reviewer = "other".into(),
            4 => wrong.approval.decision.as_mut().unwrap().decision_id = "other".into(),
            5 => wrong.approval.decision.as_mut().unwrap().decided_at = 20,
            _ => wrong.approval.decision = None,
        }
        assert!(
            matches!(
                validate(&command, wrong),
                Err(ContractError::Unavailable(_))
            ),
            "case {case}"
        );
    }
    // The activation's lease supplies the scope absent from the outer context.
    // Matching workflow and activation IDs cannot authorize a foreign approval.
    let owner = LeaseOwner {
        scope: command.scope.clone(),
        task_id: "resumed".into(),
        attempt_id: "attempt".into(),
        lease_id: "lease".into(),
        generation: 1,
        worker_session_id: "worker".into(),
        consumer_id: 0,
    };
    let context: WorkflowActivationContext = serde_json::from_value(json!({
        "v": 1, "workflow_id": "workflow", "activation_id": "resumed", "revision": 1,
        "continuation": "apply", "state": null, "inputs": {}, "local_steps": [],
        "wake": {"kind": "approval", "approval": receipt.approval},
    }))
    .unwrap();
    let validate_context = super::super::workflow::validate_activation_context_reply;
    validate_context(&owner, context.clone()).unwrap();
    let mut foreign_owner = owner;
    foreign_owner.scope.namespace = "other".into();
    assert!(matches!(
        validate_context(&foreign_owner, context),
        Err(ContractError::Conflict)
    ));
}

#[tokio::test]
async fn new_distributed_children_respect_catalog_but_persisted_bindings_are_replayed() {
    use super::catalog::{Catalog, scope};
    let mut fixture = Fixture::new();
    let catalog = Arc::new(Catalog::new(Some(descriptor('a'))));
    fixture.service = fixture
        .service
        .with_program_catalog(scope(), catalog.clone())
        .unwrap();
    let service = fixture.service.clone();
    let item = work(json!([child("new")]));
    let pending = tokio::spawn(async move { resolve(&service, &item).await });
    fixture
        .resolution()
        .await
        .send(Ok(descriptor('b')))
        .unwrap();
    assert_eq!(
        bounded(pending).await.unwrap().unwrap_err(),
        ContractError::Conflict
    );
    let mut persisted = work(json!([child("registered")]));
    persisted.resolved_children.push(ResolvedWorkflowChild {
        kind: WorkflowChildKind::Task,
        key: "registered".into(),
        descriptor: descriptor('b'),
    });
    *catalog.descriptor.lock().unwrap() = Err(ContractError::Unavailable("offline".into()));
    let result = resolve(&fixture.service, &persisted).await.unwrap();
    assert_eq!(result[0].descriptor, descriptor('b'));
    assert_eq!(catalog.reads.load(Ordering::SeqCst), 1);
    assert!(fixture.requests.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn healthy_distinct_programs_make_bounded_progress_across_resolution_windows() {
    let fixture = Fixture::new();
    let service = fixture.service;
    let mut requests = fixture.requests;
    tokio::spawn(async move {
        while let Some((program, reply)) = requests.recv().await {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(8)).await;
                let mut value = descriptor('a');
                value.program = program;
                let _ = reply.send(Ok(value));
            });
        }
    });
    let commands: Vec<_> = (0..24)
        .map(|i| {
            let key = format!("child-{i:02}");
            let mut command = child(&key);
            command["program"]["id"] = json!(key);
            command
        })
        .collect();
    let mut item = work(json!(commands));
    for window in 1..=3 {
        let mut resolved = Vec::new();
        let attempt = tokio::time::timeout(
            Duration::from_secs(20),
            service.resolve_workflow_children(&item, &mut resolved),
        )
        .await;
        assert_eq!(resolved.len(), window * 8);
        if window == 3 {
            attempt.unwrap().unwrap();
            for (index, binding) in resolved.iter().enumerate() {
                assert_eq!(binding.key, format!("child-{index:02}"));
                assert_eq!(binding.key, binding.descriptor.program.id);
            }
        } else {
            assert!(attempt.is_err());
        }
        item.resolved_children = resolved;
    }
}

#[tokio::test]
async fn protected_effect_capability_is_unavailable_without_a_configured_workflow_store() {
    let fixture = Fixture::new();
    assert!(matches!(
        fixture.service.approved_effects(),
        Err(ContractError::InvalidInput(_))
    ));
}
