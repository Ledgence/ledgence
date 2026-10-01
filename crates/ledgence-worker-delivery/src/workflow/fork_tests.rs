use super::*;

fn fixture() -> (WorkflowOperations, mpsc::UnboundedReceiver<ForkRequest>) {
    let (contexts, _) = mpsc::unbounded_channel();
    let (commits, _) = mpsc::unbounded_channel();
    let (forks, requests) = mpsc::unbounded_channel();
    let handler = journal(Arc::new(Mock {
        contexts,
        commits,
        forks: Some(forks),
    }));
    (handler, requests)
}

fn request() -> RuntimeRequest {
    RuntimeRequest {
        id: 9,
        operation: "workflow.fork".into(),
        payload: json!({"key":"fanout","branches":[
            {"key":"first","entrypoint":"evaluate","queue":"agents","data":{"number":1.0}},
            {"key":"second","entrypoint":"evaluate","queue":"agents","data":null}
        ]}),
    }
}

fn receipt() -> WorkflowForkReceipt {
    WorkflowForkReceipt {
        key: "fanout".into(),
        branch_keys: vec!["first".into(), "second".into()],
        already_accepted: false,
    }
}

#[tokio::test]
async fn lost_fork_receipt_retries_exact_owner_branches_and_processing_trace() {
    let (mut handler, mut forks) = fixture();
    let trace = TraceContext {
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-1111111111111111-00".into(),
        tracestate: Some("vendor=unsampled".into()),
    };
    handler.processing_trace = Some(trace.clone());
    let pending = tokio::spawn(async move {
        handler
            .handle(request(), RunControl::new(Duration::from_secs(1)))
            .await
    });
    let (first, reply) = bounded(forks.recv()).await.unwrap();
    assert_eq!(first.owner, owner());
    assert_eq!(first.processing_trace, Some(trace));
    assert!(first.fork.branches[0].data["number"].is_f64());
    assert!(!pending.is_finished());
    reply
        .send(Err(ContractError::Unavailable(
            "committed but reply lost".into(),
        )))
        .unwrap();
    let (second, reply) = bounded(forks.recv()).await.unwrap();
    assert_eq!(
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
    let mut accepted = receipt();
    accepted.already_accepted = true;
    reply.send(Ok(accepted)).unwrap();
    let result = bounded(pending).await.unwrap().unwrap();
    assert_eq!(result.id, 9);
    assert_eq!(
        result.result,
        json!({"committed":true,"key":"fanout","branch_keys":["first","second"]})
    );
    assert!(forks.try_recv().is_err());
}

#[tokio::test]
async fn malformed_or_mismatched_fork_receipts_never_acknowledge_or_retry() {
    for defect in ["key", "order", "duplicate", "missing", "extra", "empty"] {
        let (handler, mut forks) = fixture();
        let pending = tokio::spawn(async move {
            handler
                .handle(request(), RunControl::new(Duration::from_secs(1)))
                .await
        });
        let (_, reply) = bounded(forks.recv()).await.unwrap();
        let mut value = receipt();
        match defect {
            "key" => value.key = "another".into(),
            "order" => value.branch_keys.reverse(),
            "duplicate" => value.branch_keys[1] = "first".into(),
            "missing" => {
                value.branch_keys.pop();
            }
            "extra" => value.branch_keys.push("third".into()),
            "empty" => value.branch_keys.clear(),
            _ => unreachable!(),
        }
        reply.send(Ok(value)).unwrap();
        assert_eq!(
            bounded(pending).await.unwrap().unwrap_err().kind,
            ErrorKind::Protocol,
            "{defect}"
        );
        assert!(forks.try_recv().is_err());
    }
}

#[tokio::test]
async fn fork_rejects_runtime_authority_fields_and_invalid_branches_before_dispatch() {
    let (handler, mut forks) = fixture();
    for defect in [
        "owner",
        "trace",
        "duplicate",
        "missing_data",
        "empty",
        "entrypoint",
        "timeout",
    ] {
        let mut value = request();
        match defect {
            "owner" => value.payload["owner"] = serde_json::to_value(owner()).unwrap(),
            "trace" => value.payload["processing_trace"] = Value::Null,
            "duplicate" => value.payload["branches"][1]["key"] = json!("first"),
            "missing_data" => {
                value.payload["branches"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("data");
            }
            "empty" => value.payload["branches"] = json!([]),
            "entrypoint" => value.payload["branches"][0]["entrypoint"] = json!(""),
            "timeout" => value.payload["branches"][0]["attempt_timeout_ms"] = json!(0),
            _ => unreachable!(),
        }
        assert_eq!(
            handler
                .handle(value, RunControl::new(Duration::from_secs(1)))
                .await
                .unwrap_err()
                .kind,
            ErrorKind::Protocol,
            "{defect}"
        );
    }
    assert!(forks.try_recv().is_err());
}

#[tokio::test]
async fn definite_fork_failure_does_not_retry_or_acknowledge() {
    for (failure, expected) in [
        (ContractError::OwnershipLost, ErrorKind::Cancelled),
        (ContractError::Conflict, ErrorKind::Protocol),
        (
            ContractError::InvalidInput("unsupported".into()),
            ErrorKind::Protocol,
        ),
    ] {
        let (handler, mut forks) = fixture();
        let pending = tokio::spawn(async move {
            handler
                .handle(request(), RunControl::new(Duration::from_secs(1)))
                .await
        });
        let (_, reply) = bounded(forks.recv()).await.unwrap();
        reply.send(Err(failure)).unwrap();
        assert_eq!(bounded(pending).await.unwrap().unwrap_err().kind, expected);
        assert!(forks.try_recv().is_err());
    }
}

#[tokio::test]
async fn cancelling_or_expiring_pending_fork_drops_rpc_without_acknowledgement() {
    for (cancel, ready) in [(true, false), (true, true), (false, false)] {
        let (handler, mut forks) = fixture();
        let control = RunControl::new(if cancel {
            Duration::from_secs(1)
        } else {
            Duration::from_millis(30)
        });
        let ongoing = control.clone();
        let pending = tokio::spawn(async move { handler.handle(request(), ongoing).await });
        let (_, reply) = bounded(forks.recv()).await.unwrap();
        if cancel {
            control.cancel();
        }
        if ready {
            let _ = reply.send(Ok(receipt()));
        } else {
            let result = bounded(pending).await.unwrap().unwrap_err();
            assert_eq!(
                result.kind,
                if cancel {
                    ErrorKind::Cancelled
                } else {
                    ErrorKind::TimedOut
                }
            );
            assert!(reply.is_closed());
            assert!(forks.try_recv().is_err());
            continue;
        }
        assert_eq!(
            bounded(pending).await.unwrap().unwrap_err().kind,
            ErrorKind::Cancelled
        );
        assert!(forks.try_recv().is_err());
    }
}

#[tokio::test]
async fn request_timeout_retries_identical_fork_before_acknowledgement() {
    let (mut handler, mut forks) = fixture();
    handler.request_timeout = Duration::from_millis(10);
    let pending = tokio::spawn(async move {
        handler
            .handle(request(), RunControl::new(Duration::from_secs(1)))
            .await
    });
    let (first, abandoned) = bounded(forks.recv()).await.unwrap();
    let (second, reply) = bounded(forks.recv()).await.unwrap();
    assert!(abandoned.is_closed());
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    reply.send(Ok(receipt())).unwrap();
    assert!(bounded(pending).await.unwrap().is_ok());
}
