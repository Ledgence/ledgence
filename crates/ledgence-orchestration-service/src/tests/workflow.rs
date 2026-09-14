use super::*;

fn work(commands: serde_json::Value) -> WorkflowWork {
    WorkflowWork {
        id: "completion:task_activation".into(),
        token: "lease_1".into(),
        workflow_id: "workflow_1".into(),
        task_id: "task_activation".into(),
        activation: true,
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
    item.activation = false;
    assert!(
        fixture
            .service
            .resolve_workflow_children(&item)
            .await
            .unwrap()
            .is_empty()
    );
    item.activation = true;
    item.task_id = "different_controller".into();
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
