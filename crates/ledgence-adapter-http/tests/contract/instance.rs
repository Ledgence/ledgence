use super::*;

#[tokio::test]
async fn instance_legacy_routes_reject_foreign_scope_before_calling_any_service() {
    let mock = Arc::new(Mock::default());
    let instance = SelfHostedInstanceContext {
        instance_id: "local".into(),
        name: "Local".into(),
        scope: Scope {
            tenant_id: "fixed".into(),
            namespace: "fixed".into(),
        },
    };
    let running = start(
        server::router_with_instance(
            mock.clone(),
            mock.clone(),
            mock.clone(),
            instance,
            Arc::new(AtomicBool::new(false)),
            Arc::new(ledgence_worker_api::NoopTraceBridge),
        )
        .unwrap(),
    )
    .await;
    let client = HttpTaskService::new(&running.url).unwrap();
    assert_eq!(
        client.submit(&submit(json!(null))).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .list_tasks(&scope(), &TaskListQuery::default())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.inspect(&scope(), "task").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.status(&scope(), "task").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.result(&scope(), "task").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .inspect_attempt(&scope(), "task", "attempt")
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.history(&scope(), "task", 0).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.cancel(&scope(), "task").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.open_session(&scope(), "queue", 1).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .acquire(&acquisition(), immediate())
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .claim_dispatch(&ClaimCommand {
                acquisition: acquisition(),
                dispatch: DispatchRef {
                    scope: scope(),
                    queue: acquisition().queue,
                    task_id: "task".into(),
                    generation: 1
                }
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .renew(&RenewCommand {
                owner: owner(),
                sequence: 1,
                intent: RenewIntent::Dispatch
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.settle(&settlement(json!(null))).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.confirm_quiescence(&owner()).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .submit_workflow(&submit(json!(null)))
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.workflow_status(&scope(), "wf").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.workflow_result(&scope(), "wf").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.cancel_workflow(&scope(), "wf").await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client.activation_context(&owner()).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .record_local_result(&LocalResultCommand {
                owner: owner(),
                record: LocalStepRecord {
                    key: "step".into(),
                    callable: "read".into(),
                    input: json!(null),
                    output: json!(null)
                }
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(client.send_workflow_event(&WorkflowEventCommand {scope:scope(),workflow_id:"wf".into(),key:"event".into(),event:WorkflowEvent::new(json!({"specversion":"1.0","id":"event","source":"urn:test","type":"test","datacontenttype":"application/json","data":null})).unwrap()}).await.unwrap_err(),ContractError::NotFound);
    assert_eq!(
        client
            .subscribe_completion(&CompletionSubscribeCommand {
                scope: scope(),
                target: CompletionTarget::Task { id: "task".into() },
                destination: "dest".into(),
                idempotency_key: "key".into()
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .completion_status(&scope(), "subscription")
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(
        client
            .retry_completion(&CompletionRetryCommand {
                scope: scope(),
                subscription_id: "subscription".into(),
                expected_generation: 1
            })
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert!(
        mock.calls.lock().unwrap().is_empty(),
        "foreign binding must never reach application operations"
    );
}
