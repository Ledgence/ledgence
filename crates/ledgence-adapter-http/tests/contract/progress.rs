use super::*;
impl ExecutionProgressReader for Mock {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move {
            let reply = self.reply("progress", query);
            let delay = self.progress_delay_ms.load(Ordering::SeqCst);
            if delay != 0 {
                tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            }
            reply
        })
    }
}
fn query() -> ExecutionProgressQuery {
    ExecutionProgressQuery {
        scope: scope(),
        target: ExecutionProgressTarget::Task {
            id: "task_progress".into(),
        },
        cursor: None,
        limit: 1,
        wait_ms: 0,
    }
}
fn page(q: &ExecutionProgressQuery) -> ExecutionProgressPage {
    ExecutionProgressPage {
        scope: q.scope.clone(),
        target: q.target.clone(),
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
        next_cursor: q.cursor_for(1).unwrap(),
        has_more: false,
    }
}
#[tokio::test]
async fn progress_metadata_read_roundtrips_and_resumes_without_sensitive_reads() {
    let f = setup().await;
    f.mock.progress_enabled.store(true, Ordering::SeqCst);
    let q = query();
    let p = page(&q);
    f.mock.set("progress", Ok(p.clone()));
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("reader")
        .unwrap();
    assert_eq!(
        client
            .progress_reader()
            .unwrap()
            .execution_progress(&q)
            .await
            .unwrap(),
        p
    );
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    let q = ExecutionProgressQuery {
        cursor: Some(p.next_cursor.clone()),
        ..q
    };
    let p = ExecutionProgressPage {
        events: vec![],
        ..p
    };
    f.mock.set("progress", Ok(p.clone()));
    assert_eq!(client.execution_progress(&q).await.unwrap(), p);
    assert_eq!(f.mock.calls.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn progress_authorization_scope_and_cursor_are_checked_before_storage() {
    let f = setup().await;
    f.mock.progress_enabled.store(true, Ordering::SeqCst);
    let path = "/v1/executions/progress";
    for token in ["worker", "submitter", "source-investigator"] {
        assert_eq!(
            request(&f, path, Some(json!({"malformed":true})), token)
                .await
                .0,
            403
        );
    }
    let mut q = query();
    q.cursor = Some("broken".into());
    assert_eq!(request(&f, path, Some(json!(q)), "reader").await.0, 400);
    q.cursor = None;
    q.scope.namespace = "foreign".into();
    assert_eq!(request(&f, path, Some(json!(q)), "reader").await.0, 404);
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
#[tokio::test]
async fn progress_rejects_inconsistent_adapter_snapshots_and_unavailable_capability() {
    let f = setup().await;
    let q = query();
    let path = "/v1/executions/progress";
    assert_eq!(request(&f, path, Some(json!(q)), "reader").await.0, 503);
    f.mock.progress_enabled.store(true, Ordering::SeqCst);
    for p in [
        ExecutionProgressPage {
            scope: Scope {
                tenant_id: "foreign".into(),
                ..scope()
            },
            ..page(&q)
        },
        ExecutionProgressPage {
            next_cursor: q.cursor_for(0).unwrap(),
            ..page(&q)
        },
        ExecutionProgressPage {
            terminal: true,
            ..page(&q)
        },
    ] {
        f.mock.set("progress", Ok(p));
        assert_eq!(request(&f, path, Some(json!(q)), "reader").await.0, 503);
    }
    f.mock
        .set::<ExecutionProgressPage>("progress", Err(ContractError::NotFound));
    assert_eq!(request(&f, path, Some(json!(q)), "reader").await.0, 404);
}

#[tokio::test]
async fn console_progress_injects_scope_and_preserves_metadata_authority() {
    let f = setup().await;
    let (_, config) = request(&f, "/v1/console/config", None, "reader").await;
    assert_eq!(config["capabilities"]["execution_progress"], false);
    f.mock.progress_enabled.store(true, Ordering::SeqCst);
    let (_, config) = request(&f, "/v1/console/config", None, "reader").await;
    assert_eq!(config["capabilities"]["execution_progress"], true);
    let (_, restricted) = request(&f, "/v1/console/config", None, "source-investigator").await;
    assert_eq!(restricted["capabilities"]["execution_progress"], false);
    let q = query();
    f.mock.set("progress", Ok(page(&q)));
    let body = json!({"target": q.target, "cursor": null, "limit": 1, "wait_ms": 5000});
    let path = "/v1/console/executions/progress";
    let (status, actual) = request(&f, path, Some(body.clone()), "reader").await;
    assert_eq!(status, 200);
    assert_eq!(actual, json!(page(&q)));
    let call = f.mock.calls.lock().unwrap().last().unwrap().1.clone();
    assert_eq!(call["scope"], json!(scope()));
    assert_eq!(call["wait_ms"], 5000);
    f.mock.calls.lock().unwrap().clear();
    for token in ["worker", "submitter", "source-investigator"] {
        assert_eq!(request(&f, path, Some(body.clone()), token).await.0, 403);
    }
    for bad in [
        json!({"scope": scope(), "target": q.target}),
        json!({"target": q.target, "wait_ms": 20001}),
        json!({"target": q.target, "wait_ms": -1}),
        json!({"target": q.target, "wait_ms": true}),
    ] {
        assert_eq!(request(&f, path, Some(bad), "reader").await.0, 400);
    }
    assert!(f.mock.calls.lock().unwrap().is_empty());
    assert!(f.auditor.observations.lock().unwrap().is_empty());
}

#[tokio::test]
async fn progress_wait_rechecks_expired_authority_before_returning_records() {
    for path in ["/v1/executions/progress", "/v1/console/executions/progress"] {
        let f = setup().await;
        f.mock.progress_enabled.store(true, Ordering::SeqCst);
        f.mock.progress_delay_ms.store(450, Ordering::SeqCst);
        let mut q = query();
        q.wait_ms = 1000;
        f.mock.set("progress", Ok(page(&q)));
        let mut body = json!(q);
        if path.contains("/console/") {
            body.as_object_mut().unwrap().remove("scope");
        }
        assert_eq!(
            request(&f, path, Some(body), "expiring-reader").await.0,
            401
        );
        assert_eq!(f.mock.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn console_progress_checks_origin_and_response_identity_headers() {
    let f = setup().await;
    f.mock.progress_enabled.store(true, Ordering::SeqCst);
    let q = query();
    f.mock.set("progress", Ok(page(&q)));
    let client = reqwest::Client::new();
    let url = format!("{}/v1/console/executions/progress", f.running.url);
    let body = json!({"target": q.target, "limit": 1});
    let rejected = client
        .post(&url)
        .bearer_auth("reader")
        .header("Origin", "https://foreign.example")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);
    assert!(f.mock.calls.lock().unwrap().is_empty());
    let accepted = client
        .post(&url)
        .bearer_auth("reader")
        .header("Origin", "https://audit.example")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 200);
    assert_eq!(accepted.headers()["ledgence-console-contract"], "16");
    assert_eq!(accepted.headers()["ledgence-instance-id"], "audit");
    assert!(accepted.headers().contains_key("request-id"));
    assert_eq!(accepted.headers()["cache-control"], "no-store");
}
