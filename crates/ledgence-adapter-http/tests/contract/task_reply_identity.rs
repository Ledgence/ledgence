//! A successful HTTP response must acknowledge the operation we actually sent.
use super::*;

#[tokio::test]
async fn submission_rejects_changed_identity_or_input_without_retry() {
    let input = json!({"amount": 1, "zero": -0.0});
    let original = serde_json::to_value(task(input.clone())).unwrap();
    for changed in [
        "key",
        "tenant",
        "namespace",
        "queue",
        "correlation",
        "program",
        "descriptor",
        "integer_to_float",
        "negative_zero",
        "retry",
        "timeout",
        "workflow",
        "activation",
        "invalid_task",
        "invalid_run",
        "invalid_trace",
    ] {
        let mut reply = original.clone();
        match changed {
            "key" => reply["idempotency_key"] = "other".into(),
            "tenant" => reply["input"]["tenant_id"] = "other".into(),
            "namespace" => reply["input"]["namespace"] = "other".into(),
            "queue" => reply["input"]["queue"] = "other".into(),
            "correlation" => reply["input"]["correlation_key"] = "other".into(),
            "program" => {
                reply["input"]["program"]["version"] = "other".into();
                reply["descriptor"]["program"]["version"] = "other".into();
            }
            "descriptor" => reply["descriptor"]["program"]["version"] = "other".into(),
            "integer_to_float" => reply["input"]["data"]["amount"] = json!(1.0),
            "negative_zero" => reply["input"]["data"]["zero"] = json!(0.0),
            "retry" => reply["input"]["retry_policy"]["max_attempts"] = json!(7),
            "timeout" => reply["input"]["attempt_timeout_ms"] = json!(120_000),
            "workflow" => reply["workflow_id"] = "owned_workflow".into(),
            "activation" => {
                reply["workflow_id"] = "owned_workflow".into();
                reply["workflow_activation_id"] = "..".into();
            }
            "invalid_task" => reply["task_id"] = "".into(),
            "invalid_run" => reply["run_id"] = "".into(),
            "invalid_trace" => reply["origin_trace"] = json!({"traceparent":"invalid"}),
            _ => unreachable!(),
        }
        let (running, count) =
            raw_response(response(200, "application/json", &exact(reply)), None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .submit(&submit(input.clone()))
            .await;
        assert!(
            matches!(result, Err(ContractError::Unavailable(_))),
            "{changed}: {result:?}"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1, "{changed}");
    }
}

#[tokio::test]
async fn submission_accepts_implicit_defaults_and_absent_correlation() {
    let mut command = submit(json!({"amount": 1}));
    command.input.correlation_key = None;
    let mut accepted = task(command.input.data.clone());
    accepted.input = command.input.clone();
    let original = serde_json::to_value(&accepted).unwrap();
    for implicit in [false, true] {
        let mut reply = original.clone();
        reply["input"]["correlation_key"] = Value::Null;
        if implicit {
            let input = reply["input"].as_object_mut().unwrap();
            input.remove("retry_policy");
            input.remove("attempt_timeout_ms");
            input.remove("correlation_key");
        }
        let (running, count) =
            raw_response(response(200, "application/json", &exact(reply)), None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .submit(&command)
            .await
            .unwrap();
        assert_eq!(result.task_id, accepted.task_id);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn submission_reconciliation_keeps_the_first_accepted_origin_trace() {
    let input = json!({"amount": 1, "zero": -0.0});
    let original = TraceContext {
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
        tracestate: None,
    };
    let mut reply = task(input.clone());
    reply.origin_trace = Some(original.clone());
    for retry_trace in [
        None,
        Some(original.clone()),
        Some(TraceContext {
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
            tracestate: Some("test=retry".into()),
        }),
    ] {
        let mut command = submit(input.clone());
        command.origin_trace = retry_trace;
        let (running, count) =
            raw_response(response(200, "application/json", &exact(&reply)), None).await;
        let accepted = HttpTaskService::new(&running.url)
            .unwrap()
            .submit(&command)
            .await
            .unwrap();
        assert_eq!(accepted.task_id, reply.task_id);
        assert_eq!(accepted.origin_trace, Some(original.clone()));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn task_inspection_binds_scope_task_and_descriptor_but_allows_owned_work() {
    let original = serde_json::to_value(task(Value::Null)).unwrap();
    for changed in [
        "none",
        "task",
        "tenant",
        "namespace",
        "descriptor",
        "activation",
    ] {
        let mut reply = original.clone();
        // Inspections include both child tasks and controller activations.
        reply["workflow_id"] = "workflow".into();
        reply["workflow_activation_id"] = "..".into();
        match changed {
            "task" => reply["task_id"] = "other".into(),
            "tenant" => reply["input"]["tenant_id"] = "other".into(),
            "namespace" => reply["input"]["namespace"] = "other".into(),
            "descriptor" => reply["descriptor"]["program"]["version"] = "other".into(),
            "activation" => reply["workflow_activation_id"] = "other".into(),
            _ => {}
        }
        let (running, count) =
            raw_response(response(200, "application/json", &exact(reply)), None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .inspect(&scope(), "..")
            .await;
        if changed == "none" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(result, Err(ContractError::Unavailable(_))),
                "{changed}: {result:?}"
            );
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn attempt_inspection_binds_owner_event_and_settlement_identities() {
    let original = serde_json::to_value(attempt(Value::Null, settlement(Value::Null))).unwrap();
    for changed in [
        "none",
        "task",
        "attempt",
        "tenant",
        "namespace",
        "event_task",
        "event_attempt",
        "event_scope",
        "event_generation",
        "renewal",
        "settlement",
        "receipt",
    ] {
        let mut reply = original.clone();
        match changed {
            "task" => reply["lease"]["owner"]["task_id"] = "other".into(),
            "attempt" => reply["lease"]["owner"]["attempt_id"] = "other".into(),
            "tenant" => reply["lease"]["owner"]["scope"]["tenant_id"] = "other".into(),
            "namespace" => reply["lease"]["owner"]["scope"]["namespace"] = "other".into(),
            "event_task" => reply["event"]["ldgtaskid"] = "other".into(),
            "event_attempt" => reply["event"]["ldgattemptid"] = "other".into(),
            "event_scope" => reply["event"]["ldgtenantid"] = "other".into(),
            "event_generation" => reply["event"]["ldgattemptno"] = json!(2),
            "renewal" => reply["last_renewal"]["owner"]["task_id"] = "other".into(),
            "settlement" => reply["settlement"]["command"]["owner"]["attempt_id"] = "other".into(),
            "receipt" => reply["settlement"]["receipt"]["attempt_id"] = "other".into(),
            _ => {}
        }
        let (running, count) =
            raw_response(response(200, "application/json", &exact(reply)), None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .inspect_attempt(&scope(), "..", &owner().attempt_id)
            .await;
        if changed == "none" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(result, Err(ContractError::Unavailable(_))),
                "{changed}: {result:?}"
            );
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn history_binds_task_and_strictly_advances_the_requested_sequence() {
    let item = |sequence, task_id: &str| RecordedHistoryEvent {
        sequence,
        event: HistoryEvent {
            task_id: task_id.into(),
            attempt_id: None,
            at: 10,
            reason: TransitionReason::Succeeded,
        },
    };
    for (reply, valid) in [
        (vec![], true),
        (vec![item(u64::MAX - 1, ".."), item(u64::MAX, "..")], true),
        (vec![item(u64::MAX, "other")], false),
        (vec![item(u64::MAX - 2, "..")], false),
        (vec![item(u64::MAX, ".."), item(u64::MAX, "..")], false),
        (vec![item(u64::MAX, ".."), item(u64::MAX - 1, "..")], false),
    ] {
        let (running, count) =
            raw_response(response(200, "application/json", &exact(reply)), None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .history(&scope(), "..", u64::MAX - 2)
            .await;
        if valid {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(result, Err(ContractError::Unavailable(_))),
                "{result:?}"
            );
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
