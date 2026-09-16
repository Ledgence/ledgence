use super::*;

fn mixed_context() -> WorkflowActivationContext {
    let mut value = context();
    value.parent_workflow_id = Some("parent".into());
    value.root_workflow_id = Some("root".into());
    value.inputs.insert(
        "task".into(),
        WorkflowChildResult::Task(WorkflowTaskResult {
            task_id: "task-child".into(),
            state: TaskState::Succeeded,
            outcome: TaskOutcome::Succeeded {
                attempt_id: "attempt-child".into(),
                quiescence: Quiescence::Confirmed,
                execution_may_have_started: true,
                output: json!([null, 1, 1.0, -0.0, 18446744073709551615u64]),
            },
        }),
    );
    for (key, state, outcome) in [
        (
            "workflow",
            WorkflowState::Succeeded,
            WorkflowOutcome::Succeeded {
                output: Value::Null,
            },
        ),
        (
            "failed",
            WorkflowState::Failed,
            WorkflowOutcome::Failed {
                error: ApplicationError {
                    kind: "business_error".into(),
                    message: "handled by parent".into(),
                },
            },
        ),
        (
            "cancelled",
            WorkflowState::Cancelled,
            WorkflowOutcome::Cancelled {},
        ),
    ] {
        value.inputs.insert(
            key.into(),
            WorkflowChildResult::Workflow(WorkflowSubworkflowResult {
                kind: WorkflowChildKind::Workflow,
                workflow_id: format!("child-{key}"),
                state,
                outcome,
            }),
        );
    }
    value
}

#[tokio::test]
async fn mixed_child_results_and_nested_lineage_survive_http_without_task_identity_fabrication() {
    let mock = Arc::new(Mock::default());
    let expected = mixed_context();
    mock.set("wf_context", Ok(expected.clone()));
    let mut nested = snapshot();
    nested.parent_workflow_id = expected.parent_workflow_id.clone();
    nested.root_workflow_id = expected.root_workflow_id.clone();
    mock.set("wf_status", Ok(nested));
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let actual = client.activation_context(&owner()).await.unwrap();
    assert_eq!(
        serde_json::to_string(&actual).unwrap(),
        serde_json::to_string(&expected).unwrap()
    );
    let encoded = serde_json::to_value(&actual).unwrap();
    assert!(encoded["inputs"]["task"].get("kind").is_none());
    assert!(encoded["inputs"]["task"].get("workflow_id").is_none());
    assert!(encoded["inputs"]["workflow"].get("task_id").is_none());
    assert_eq!(
        encoded["inputs"]["workflow"]["outcome"]["output"],
        Value::Null
    );
    let observed = client
        .workflow_status(&scope(), &expected.workflow_id)
        .await
        .unwrap();
    assert_eq!(observed.parent_workflow_id.as_deref(), Some("parent"));
    assert_eq!(observed.root_workflow_id.as_deref(), Some("root"));

    // Existing task-only contexts and root observations retain their old JSON.
    mock.set("wf_context", Ok(context()));
    mock.set("wf_status", Ok(snapshot()));
    let legacy = client.activation_context(&owner()).await.unwrap();
    let legacy = serde_json::to_value(legacy).unwrap();
    assert!(legacy.get("parent_workflow_id").is_none() && legacy.get("root_workflow_id").is_none());
    let legacy = client
        .workflow_status(&scope(), &snapshot().workflow_id)
        .await
        .unwrap();
    assert!(legacy.parent_workflow_id.is_none() && legacy.root_workflow_id.is_none());
}

#[tokio::test]
async fn service_cannot_emit_nonterminal_or_inconsistent_subworkflow_results() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for defect in [
        "nonterminal",
        "kind",
        "outcome",
        "identity",
        "error",
        "parent_pair",
        "own_ancestor",
    ] {
        let mut invalid = mixed_context();
        if defect == "parent_pair" {
            invalid.root_workflow_id = None;
        } else if defect == "own_ancestor" {
            invalid.parent_workflow_id = Some(invalid.workflow_id.clone());
        } else {
            let WorkflowChildResult::Workflow(child) = invalid.inputs.get_mut("workflow").unwrap()
            else {
                unreachable!()
            };
            match defect {
                "nonterminal" => child.state = WorkflowState::Waiting,
                "kind" => child.kind = WorkflowChildKind::Task,
                "outcome" => child.outcome = WorkflowOutcome::Cancelled {},
                "identity" => child.workflow_id.clear(),
                "error" => {
                    child.state = WorkflowState::Failed;
                    child.outcome = WorkflowOutcome::Failed {
                        error: ApplicationError {
                            kind: String::new(),
                            message: "invalid".into(),
                        },
                    };
                }
                _ => unreachable!(),
            }
        }
        mock.set("wf_context", Ok(invalid));
        assert!(
            matches!(
                client.activation_context(&owner()).await,
                Err(ContractError::Unavailable(_))
            ),
            "{defect}"
        );
    }
}

#[tokio::test]
async fn client_rejects_ambiguous_child_shapes_from_an_independent_http_peer() {
    for defect in [
        "both_ids",
        "missing_kind",
        "wrong_kind",
        "task_kind",
        "missing_output",
        "unknown",
        "parent_pair",
    ] {
        let mut body = serde_json::to_value(mixed_context()).unwrap();
        match defect {
            "both_ids" => {
                body["inputs"]["workflow"]["task_id"] = json!("fabricated-controller");
            }
            "missing_kind" => {
                body["inputs"]["workflow"]
                    .as_object_mut()
                    .unwrap()
                    .remove("kind");
            }
            "wrong_kind" => {
                body["inputs"]["workflow"]["kind"] = json!("task");
            }
            "task_kind" => {
                body["inputs"]["task"]["kind"] = json!("workflow");
            }
            "missing_output" => {
                body["inputs"]["workflow"]["outcome"]
                    .as_object_mut()
                    .unwrap()
                    .remove("output");
            }
            "unknown" => {
                body["inputs"]["workflow"]["new_field"] = json!(true);
            }
            "parent_pair" => {
                body.as_object_mut().unwrap().remove("root_workflow_id");
            }
            _ => unreachable!(),
        }
        let encoded = serde_json::to_vec(&body).unwrap();
        let running = start(axum::Router::new().route(
            "/v1/workflows/activations/context",
            axum::routing::post(move || {
                let encoded = encoded.clone();
                async move { ([("content-type", "application/json")], encoded) }
            }),
        ))
        .await;
        let client = HttpTaskService::new(&running.url).unwrap();
        assert!(
            matches!(
                client.activation_context(&owner()).await,
                Err(ContractError::Unavailable(_))
            ),
            "{defect}"
        );
    }
}

#[tokio::test]
async fn invalid_post_mutation_nested_lineage_remains_uncertain() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for defect in ["missing_parent", "missing_root", "own_ancestor"] {
        let mut invalid = snapshot();
        invalid.parent_workflow_id = Some("parent".into());
        invalid.root_workflow_id = Some("root".into());
        match defect {
            "missing_parent" => invalid.parent_workflow_id = None,
            "missing_root" => invalid.root_workflow_id = None,
            "own_ancestor" => invalid.root_workflow_id = Some(invalid.workflow_id.clone()),
            _ => unreachable!(),
        }
        mock.set("wf_cancel", Ok(invalid));
        assert!(
            matches!(
                client
                    .cancel_workflow(&scope(), &snapshot().workflow_id)
                    .await,
                Err(ContractError::Unavailable(_))
            ),
            "{defect}"
        );
    }
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        3,
        "invalid acknowledgement must not trigger an automatic mutation retry"
    );
}

#[tokio::test]
async fn task_inspection_rejects_partial_or_self_nested_lineage() {
    let mock = Arc::new(Mock::default());
    let running = setup(&mock).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for defect in [
        "missing_parent",
        "missing_root",
        "missing_workflow",
        "own_parent",
        "own_root",
    ] {
        let mut invalid = task(Value::Null);
        invalid.workflow_id = Some("nested-workflow".into());
        invalid.parent_workflow_id = Some("parent".into());
        invalid.root_workflow_id = Some("root".into());
        match defect {
            "missing_parent" => invalid.parent_workflow_id = None,
            "missing_root" => invalid.root_workflow_id = None,
            "missing_workflow" => invalid.workflow_id = None,
            "own_parent" => invalid.parent_workflow_id = invalid.workflow_id.clone(),
            "own_root" => invalid.root_workflow_id = invalid.workflow_id.clone(),
            _ => unreachable!(),
        }
        mock.set("inspect", Ok(invalid));
        assert!(
            matches!(
                client.inspect(&scope(), "..").await,
                Err(ContractError::Unavailable(_))
            ),
            "{defect}"
        );
    }
    let mut valid = task(Value::Null);
    valid.workflow_id = Some("nested-workflow".into());
    valid.parent_workflow_id = Some("parent".into());
    valid.root_workflow_id = Some("root".into());
    mock.set("inspect", Ok(valid));
    let observed = client.inspect(&scope(), "..").await.unwrap();
    assert_eq!(observed.parent_workflow_id.as_deref(), Some("parent"));
    assert_eq!(observed.root_workflow_id.as_deref(), Some("root"));
}

#[tokio::test]
async fn public_workflow_submission_cannot_acknowledge_an_owned_child() {
    let command = submit(Value::Null);
    let mut nested = snapshot();
    nested.correlation_key = command.input.correlation_key.clone();
    nested.parent_workflow_id = Some("parent".into());
    nested.root_workflow_id = Some("root".into());
    nested.validate().unwrap();
    let mock = Arc::new(Mock::default());
    mock.set("wf_submit", Ok(nested.clone()));
    let running = setup(&mock).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/workflows", running.url))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&command).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        503,
        "mutation response must remain uncertain"
    );
    assert_eq!(mock.calls.lock().unwrap().len(), 1);
    let encoded = serde_json::to_vec(&nested).unwrap();
    let peer = start(axum::Router::new().route(
        "/v1/workflows",
        axum::routing::post(move || {
            let encoded = encoded.clone();
            async move { ([("content-type", "application/json")], encoded) }
        }),
    ))
    .await;
    let client = HttpTaskService::new(&peer.url).unwrap();
    assert!(matches!(
        client.submit_workflow(&command).await,
        Err(ContractError::Unavailable(_))
    ));
}
