use super::*;

impl ExecutionProgressReader for MemoryStore {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move {
            assert_eq!(query.wait_ms, 0);
            self.progress_calls.fetch_add(1, Ordering::SeqCst);
            self.progress_reply
                .lock()
                .unwrap()
                .clone()
                .expect("configured progress response")
        })
    }
}
fn query() -> ExecutionProgressQuery {
    ExecutionProgressQuery {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        },
        target: ExecutionProgressTarget::Task {
            id: "task_1".into(),
        },
        cursor: None,
        limit: 100,
        wait_ms: 0,
    }
}
fn response(query: &ExecutionProgressQuery) -> ExecutionProgressPage {
    ExecutionProgressPage {
        scope: query.scope.clone(),
        target: query.target.clone(),
        state: "queued".into(),
        terminal: false,
        high_watermark: ProgressSequence(1),
        events: vec![ExecutionProgressEvent {
            sequence: ProgressSequence(1),
            at: 1,
            reason: "submitted".into(),
            activation_id: None,
            attempt_id: None,
        }],
        next_cursor: query.cursor_for(1).unwrap(),
        has_more: false,
    }
}

#[tokio::test]
async fn progress_requires_the_optional_store_capability_and_never_resolves_programs() {
    let mut fixture = Fixture::new();
    let query = query();
    assert!(matches!(
        fixture.service.progress_reader(),
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        fixture.service.execution_progress(&query).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(fixture.store.progress_calls.load(Ordering::SeqCst), 0);
    let expected = response(&query);
    *fixture.store.progress_reply.lock().unwrap() = Some(Ok(expected.clone()));
    assert_eq!(
        fixture
            .service
            .progress_reader()
            .unwrap()
            .execution_progress(&query)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(fixture.store.progress_calls.load(Ordering::SeqCst), 1);
    assert!(fixture.requests.try_recv().is_err());
}

#[tokio::test]
async fn progress_validates_input_and_rejects_mismatched_or_invented_backend_snapshots() {
    let fixture = Fixture::new();
    let query = query();
    let expected = response(&query);
    *fixture.store.progress_reply.lock().unwrap() = Some(Ok(expected.clone()));
    let mut bad_query = query.clone();
    bad_query.limit = 0;
    assert!(matches!(
        fixture.service.execution_progress(&bad_query).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(fixture.store.progress_calls.load(Ordering::SeqCst), 0);
    let mut other_scope = expected.clone();
    other_scope.scope.tenant_id = "other".into();
    let mut invented_cursor = expected.clone();
    invented_cursor.next_cursor = query.cursor_for(2).unwrap();
    let mut wrong_terminal = expected.clone();
    wrong_terminal.terminal = true;
    for response in [other_scope, invented_cursor, wrong_terminal] {
        *fixture.store.progress_reply.lock().unwrap() = Some(Ok(response));
        assert!(matches!(
            fixture.service.execution_progress(&query).await,
            Err(ContractError::Unavailable(_))
        ));
    }
    *fixture.store.progress_reply.lock().unwrap() = Some(Err(ContractError::NotFound));
    assert!(matches!(
        fixture.service.execution_progress(&query).await,
        Err(ContractError::NotFound)
    ));
}

#[tokio::test(start_paused = true)]
async fn cloned_services_share_progress_waits_and_shutdown_without_program_resolution() {
    let mut fixture = Fixture::new();
    let mut query = query();
    query.cursor = Some(query.cursor_for(1).unwrap());
    query.wait_ms = 1100;
    let mut page = response(&query);
    page.events.clear();
    *fixture.store.progress_reply.lock().unwrap() = Some(Ok(page));
    let tasks: Vec<_> = (0..2)
        .map(|_| {
            let service = fixture.service.clone();
            let query = query.clone();
            tokio::spawn(async move { service.execution_progress(&query).await })
        })
        .collect();
    for _ in 0..100 {
        if fixture.service.progress_statistics().waiters == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.service.progress_statistics().waiters, 2);
    tokio::time::advance(Duration::from_millis(1000)).await;
    for _ in 0..100 {
        if fixture.service.progress_statistics().total_probes == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_millis(100)).await;
    for task in tasks {
        assert!(task.await.unwrap().unwrap().events.is_empty());
    }
    assert_eq!(fixture.store.progress_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.service.progress_statistics().total_probes, 2);
    assert_eq!(fixture.service.progress_statistics().keys, 0);
    fixture.service.clone().stop_acquisitions();
    assert!(matches!(
        fixture.service.execution_progress(&query).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(fixture.store.progress_calls.load(Ordering::SeqCst), 2);
    assert!(fixture.requests.try_recv().is_err());
}
