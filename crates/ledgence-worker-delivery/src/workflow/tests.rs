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

struct Mock {
    contexts: mpsc::UnboundedSender<ContextRequest>,
    commits: mpsc::UnboundedSender<CommitRequest>,
}
impl WorkflowService for Mock {
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
        Arc::new(Mock { contexts, commits }),
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
fn journal(service: Arc<Mock>) -> LocalJournal {
    let config = config();
    LocalJournal {
        service,
        owner: owner(),
        request_timeout: config.request_timeout,
        retry_delay: config.retry_delay,
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("workflow test timed out")
}

#[tokio::test]
async fn wrong_workflow_activation_or_context_version_is_rejected_before_runtime_use() {
    for field in ["workflow", "activation", "version"] {
        let (service, mut contexts, mut commits) = fixture();
        let pending = tokio::spawn(async move {
            fetch_context(
                service.as_ref(),
                &owner(),
                "workflow",
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
async fn transient_context_fetch_reuses_exact_owner_and_frozen_reply() {
    let (service, mut contexts, _) = fixture();
    let pending = tokio::spawn(async move {
        fetch_context(
            service.as_ref(),
            &owner(),
            "workflow",
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
        async {
            // An adapter can return from one poll after its operation budget elapsed.
            std::thread::sleep(Duration::from_millis(5));
            Ok(42)
        },
    )
    .await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
}
