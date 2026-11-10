//! Raw-wire checks bypass typed fixtures so protocol and endpoint bounds are exercised.
use super::*;

fn query() -> ExecutionProgressQuery {
    ExecutionProgressQuery {
        scope: scope(),
        target: ExecutionProgressTarget::Task {
            id: "task_progress_wire".into(),
        },
        cursor: None,
        limit: 1,
    }
}

fn page(query: &ExecutionProgressQuery) -> ExecutionProgressPage {
    ExecutionProgressPage {
        scope: query.scope.clone(),
        target: query.target.clone(),
        state: "queued".into(),
        terminal: false,
        high_watermark: ProgressSequence(1),
        events: vec![ExecutionProgressEvent {
            sequence: ProgressSequence(1),
            at: 100,
            reason: "submitted".into(),
            activation_id: None,
            attempt_id: None,
        }],
        next_cursor: query.cursor_for(1).unwrap(),
        has_more: false,
    }
}

#[tokio::test]
async fn progress_rejects_malformed_raw_pages_without_retrying() {
    let query = query();
    let original = serde_json::to_value(page(&query)).unwrap();
    let mut cases = Vec::new();
    for field in ["activation_id", "attempt_id"] {
        let mut missing = original.clone();
        missing["events"][0].as_object_mut().unwrap().remove(field);
        cases.push(exact(missing));
    }
    for (field, bad_value) in [
        ("sequence", json!(1)),
        ("sequence", json!("01")),
        ("sequence", json!("18446744073709551616")),
        ("at", json!(true)),
        ("reason", json!("unknown_reason")),
        ("activation_id", json!("workflow_activation")),
    ] {
        let mut malformed = original.clone();
        malformed["events"][0][field] = bad_value;
        cases.push(exact(malformed));
    }
    let mut numeric_watermark = original.clone();
    numeric_watermark["high_watermark"] = json!(1);
    cases.push(exact(numeric_watermark));
    let mut missing_cursor = original.clone();
    missing_cursor
        .as_object_mut()
        .unwrap()
        .remove("next_cursor");
    cases.push(exact(missing_cursor));
    let raw = String::from_utf8(exact(&original)).unwrap();
    cases.push(
        raw.replacen(
            "\"terminal\":false",
            "\"terminal\":false,\"terminal\":false",
            1,
        )
        .into_bytes(),
    );
    cases.push(
        raw.replacen(
            "\"sequence\":\"1\"",
            "\"sequence\":\"1\",\"sequence\":\"1\"",
            1,
        )
        .into_bytes(),
    );

    for body in cases {
        let (running, count) = raw_response(response(200, "application/json", &body), None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .execution_progress(&query)
            .await;
        assert!(
            matches!(result, Err(ContractError::Unavailable(_))),
            "accepted malformed progress page: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn progress_enforces_declared_and_streamed_response_limit() {
    let query = query();
    let expected = page(&query);
    let mut body = exact(&expected);
    body.resize(EXECUTION_PROGRESS_PAGE_MAX_BYTES, b' ');
    let (running, _) = raw_response(response(200, "application/json", &body), None).await;
    assert_eq!(
        HttpTaskService::new(&running.url)
            .unwrap()
            .execution_progress(&query)
            .await
            .unwrap(),
        expected
    );
    body.push(b' ');
    for bytes in [
        response(200, "application/json", &body),
        [
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                .as_slice(),
            body.as_slice(),
        ]
        .concat(),
    ] {
        let (running, count) = raw_response(bytes, None).await;
        let result = HttpTaskService::new(&running.url)
            .unwrap()
            .execution_progress(&query)
            .await;
        assert_eq!(
            result,
            Err(ContractError::Unavailable(
                "HTTP response exceeds its body limit".into()
            ))
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn progress_rejects_oversized_request_before_storage_and_accepts_exact_bound() {
    let mock = Arc::new(Mock::default());
    mock.progress_enabled.store(true, Ordering::SeqCst);
    let query = query();
    let expected = page(&query);
    mock.set("progress", Ok(expected.clone()));
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    let mut body = exact(&query);
    body.resize(EXECUTION_PROGRESS_QUERY_MAX_BYTES + 1, b' ');
    let rejected = client
        .post(format!("{}/v1/executions/progress", running.url))
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 413);
    assert!(mock.calls.lock().unwrap().is_empty());

    body.pop();
    let accepted = client
        .post(format!("{}/v1/executions/progress", running.url))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 200);
    assert_eq!(
        serde_json::from_slice::<ExecutionProgressPage>(&accepted.bytes().await.unwrap()).unwrap(),
        expected
    );
    assert_eq!(
        mock.calls.lock().unwrap().as_slice(),
        [("progress", serde_json::to_value(query).unwrap())]
    );
}
