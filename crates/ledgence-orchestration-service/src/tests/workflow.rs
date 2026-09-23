use super::*;

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
    let pending = tokio::spawn(async move { service.resolve_workflow_children(&item).await });
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
    let resolved = bounded(fixture.service.resolve_workflow_children(&item))
        .await
        .unwrap();
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
    assert!(
        fixture
            .service
            .resolve_workflow_children(&item)
            .await
            .unwrap()
            .is_empty()
    );
    item.source = WorkflowWorkSource::TaskTerminal {
        task_id: "different_controller".into(),
        activation: true,
    };
    assert_eq!(
        fixture
            .service
            .resolve_workflow_children(&item)
            .await
            .unwrap_err(),
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
    let resolved = fixture
        .service
        .resolve_workflow_children(&item)
        .await
        .unwrap();
    assert_eq!(resolved[0].kind, WorkflowChildKind::Workflow);
    item.resolved_children[0].kind = WorkflowChildKind::Task;
    assert_eq!(
        fixture
            .service
            .resolve_workflow_children(&item)
            .await
            .unwrap_err(),
        ContractError::Conflict
    );
    item.source = WorkflowWorkSource::WorkflowTerminal {
        workflow_id: "child".into(),
    };
    assert!(
        fixture
            .service
            .resolve_workflow_children(&item)
            .await
            .unwrap()
            .is_empty()
    );
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
