use super::*;
impl ExecutionProgressReader for Mock {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move { self.reply("progress", query) })
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
