//! Real PostgreSQL lifecycle reads. Each test owns and removes its database.
use super::*;
use crate::tests::{
    TestDb, acquire_command, assignment, command, completed, descriptor, scope,
    wait_for_lock_waiter,
};
use ledgence_worker_api::{
    Error, ErrorKind, ExecutionContext, ExecutionFailure, InvocationIdentity, Phase,
};
use serde_json::{Value, json};
use sqlx::AssertSqlSafe;

fn query(target: ExecutionProgressTarget) -> ExecutionProgressQuery {
    ExecutionProgressQuery {
        scope: scope(),
        target,
        cursor: None,
        limit: 100,
    }
}
fn task_query(id: &str) -> ExecutionProgressQuery {
    query(ExecutionProgressTarget::Task { id: id.into() })
}
fn workflow_query(id: &str) -> ExecutionProgressQuery {
    query(ExecutionProgressTarget::Workflow { id: id.into() })
}
async fn task(db: &TestDb) -> TaskSnapshot {
    db.store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap()
}
async fn page(db: &TestDb, query: &ExecutionProgressQuery) -> ExecutionProgressPage {
    let page = db
        .store
        .progress_reader()
        .unwrap()
        .execution_progress(query)
        .await
        .unwrap();
    page.validate_for(query).unwrap();
    page
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn task_pages_resume_across_reopen_and_replayed_settlement_without_payload_reads() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let mut query = task_query(&task.task_id);
    query.limit = 1;
    let initial = page(&db, &query).await;
    assert_eq!(initial.events.len(), 1);
    assert_eq!(initial.events[0].reason, "submitted");
    assert!(!initial.terminal);
    assert!(!initial.has_more);

    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    db.store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    let settlement = completed(
        &assigned,
        Quiescence::Confirmed,
        json!({"secret": "not-progress"}),
    );
    db.store.settle(&settlement).await.unwrap();
    let tail = page(&db, &task_query(&task.task_id)).await.high_watermark;
    db.store.settle(&settlement).await.unwrap();
    assert_eq!(
        page(&db, &task_query(&task.task_id)).await.high_watermark,
        tail
    );

    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    query.cursor = Some(initial.next_cursor);
    let mut reasons = Vec::new();
    loop {
        let observed = reopened.execution_progress(&query).await.unwrap();
        assert!(observed.terminal);
        assert_eq!(observed.high_watermark, tail);
        assert_eq!(observed.events.len(), 1);
        assert!(observed.events[0].attempt_id.is_some());
        reasons.push(observed.events[0].reason.clone());
        query.cursor = Some(observed.next_cursor);
        if !observed.has_more {
            break;
        }
    }
    assert_eq!(
        reasons,
        [
            "claimed",
            "dispatch_authorized",
            "report_accepted",
            "succeeded"
        ]
    );
    let caught_up = reopened.execution_progress(&query).await.unwrap();
    assert!(caught_up.events.is_empty());
    assert!(caught_up.terminal);
    assert_eq!(Some(caught_up.next_cursor), query.cursor);
    reopened.close().await;

    // Deliberately unreadable business data establishes that this metadata
    // endpoint never selects/decodes the full task payload.
    sqlx::query("UPDATE tasks SET input_bytes=$2 WHERE task_id=$1")
        .bind(&task.task_id)
        .bind(vec![0xffu8])
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(db.store.inspect(&scope(), &task.task_id).await.is_err());
    let observed = page(&db, &query).await;
    assert!(observed.events.is_empty());
    assert_eq!(observed.high_watermark, tail);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn retry_attempts_preserve_progress_identity_and_replayed_settlements_add_no_events() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let request = task_query(&task.task_id);
    let initial = page(&db, &request).await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let first = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    db.store
        .renew(&RenewCommand {
            owner: first.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    let failure = SettleCommand {
        provider_probe_failure: None,
        owner: first.lease.owner.clone(),
        operation_id: "progress_retry_failure".into(),
        report: AttemptReport::Failed(ExecutionFailure {
            observations: None,
            context: Box::new(ExecutionContext {
                identity: InvocationIdentity::from(&first.event),
                program: first.descriptor.program.clone(),
                digest: first.descriptor.digest.clone(),
            }),
            phase: Phase::Execution,
            error: Error::new(ErrorKind::Io, "temporary failure"),
            execution_may_have_started: true,
            cleanup_error: None,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    };
    assert_eq!(
        db.store.settle(&failure).await.unwrap().task_state,
        TaskState::Queued
    );
    let retried = page(&db, &request).await;
    assert_eq!(retried.state, "queued");
    assert!(!retried.terminal);
    assert!(db.store.settle(&failure).await.unwrap().already_accepted);
    assert_eq!(page(&db, &request).await, retried);

    let second = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_ne!(first.lease.owner.attempt_id, second.lease.owner.attempt_id);
    db.store
        .renew(&RenewCommand {
            owner: second.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    let active = page(&db, &request).await;
    assert!(db.store.settle(&failure).await.unwrap().already_accepted);
    assert_eq!(page(&db, &request).await, active);

    let success = completed(&second, Quiescence::Confirmed, json!({"ok": true}));
    assert_eq!(
        db.store.settle(&success).await.unwrap().task_state,
        TaskState::Succeeded
    );
    let terminal = page(&db, &request).await;
    assert!(db.store.settle(&success).await.unwrap().already_accepted);
    assert_eq!(page(&db, &request).await, terminal);
    assert_eq!(terminal.events[..retried.events.len()], retried.events);
    assert_eq!(
        terminal
            .events
            .iter()
            .map(|event| event.reason.as_str())
            .collect::<Vec<_>>(),
        [
            "submitted",
            "claimed",
            "dispatch_authorized",
            "report_accepted",
            "retry_scheduled",
            "claimed",
            "dispatch_authorized",
            "report_accepted",
            "succeeded",
        ]
    );
    assert!(terminal.events[0].attempt_id.is_none());
    for (events, attempt_id) in [
        (&terminal.events[1..5], &first.lease.owner.attempt_id),
        (&terminal.events[5..], &second.lease.owner.attempt_id),
    ] {
        assert!(events.iter().all(|event| {
            event.attempt_id.as_ref() == Some(attempt_id) && event.activation_id.is_none()
        }));
    }

    // Resume the cursor saved before either attempt. Pages cross the retry
    // boundary without changing previously observed event identities or contents.
    let mut request = request;
    request.limit = 3;
    request.cursor = Some(initial.next_cursor);
    let mut events = initial.events;
    loop {
        let observed = page(&db, &request).await;
        assert_eq!(observed.state, "succeeded");
        assert!(observed.terminal);
        assert_eq!(observed.high_watermark, terminal.high_watermark);
        assert!(!observed.events.is_empty());
        events.extend(observed.events);
        request.cursor = Some(observed.next_cursor);
        if !observed.has_more {
            break;
        }
    }
    assert_eq!(events, terminal.events);
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence.0)
            .collect::<Vec<_>>(),
        (1..=terminal.high_watermark.0).collect::<Vec<_>>()
    );
    assert!(page(&db, &request).await.events.is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn workflow_progress_is_separate_from_controller_history_and_survives_restart() {
    let db = TestDb::new().await;
    let workflow = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let mut query = workflow_query(&workflow.workflow_id);
    query.limit = 1;
    let first = page(&db, &query).await;
    assert_eq!(first.state, "running");
    assert!(!first.terminal);
    assert_eq!(first.events[0].activation_id, workflow.activation_id);
    assert!(first.events[0].attempt_id.is_none());
    assert_eq!(first.events[0].reason, "started");

    let controller = page(&db, &task_query(workflow.activation_id.as_ref().unwrap())).await;
    assert_eq!(controller.events[0].sequence, ProgressSequence(1));
    assert_eq!(controller.events[0].activation_id, None);
    db.store
        .cancel_workflow(&scope(), &workflow.workflow_id)
        .await
        .unwrap();
    let after_cancel = page(&db, &workflow_query(&workflow.workflow_id)).await;
    assert!(after_cancel.high_watermark > first.high_watermark);

    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    query.cursor = Some(first.next_cursor);
    let remaining = reopened.execution_progress(&query).await.unwrap();
    assert_eq!(remaining.events[0].sequence.0, 2);
    assert_eq!(remaining.events[0].reason, "cancel_requested");
    assert_eq!(remaining.state, after_cancel.state);
    reopened.close().await;

    let mut foreign_cursor = task_query(workflow.activation_id.as_ref().unwrap());
    foreign_cursor.cursor = query.cursor;
    assert!(matches!(
        db.store.execution_progress(&foreign_cursor).await,
        Err(ContractError::InvalidInput(_))
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn scoped_reads_reject_ahead_cursors_and_hide_retired_executions_as_a_whole() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let request = task_query(&task.task_id);
    let current = page(&db, &request).await;
    let mut wrong = request.clone();
    wrong.scope.namespace = "other".into();
    assert!(matches!(
        db.store.execution_progress(&wrong).await,
        Err(ContractError::NotFound)
    ));
    assert!(matches!(
        db.store
            .execution_progress(&workflow_query(&task.task_id))
            .await,
        Err(ContractError::NotFound)
    ));
    wrong = request.clone();
    wrong.cursor = Some(request.cursor_for(current.high_watermark.0 + 1).unwrap());
    assert!(matches!(
        db.store.execution_progress(&wrong).await,
        Err(ContractError::InvalidInput(_))
    ));

    db.store.cancel(&scope(), &task.task_id).await.unwrap();
    sqlx::query("UPDATE tasks SET retiring_at_ms=terminal_at_ms WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    // Rows deliberately still exist: retirement hides the owner before physical collection.
    assert!(matches!(
        db.store.execution_progress(&request).await,
        Err(ContractError::NotFound)
    ));
    sqlx::query("DELETE FROM task_history WHERE task_id=$1 AND sequence=1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.execution_progress(&request).await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn rollback_and_competing_commits_never_publish_a_cursor_past_an_uncommitted_event() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let query = task_query(&task.task_id);
    let initial = page(&db, &query).await;
    let mut tx = db.store.pool.begin().await.unwrap();
    crate::persistence::history(
        &mut tx,
        &[HistoryEvent {
            task_id: task.task_id.clone(),
            attempt_id: None,
            at: task.submitted_at,
            reason: TransitionReason::CancelRequested,
        }],
    )
    .await
    .unwrap();
    assert_eq!(page(&db, &query).await, initial);
    tx.rollback().await.unwrap();
    assert_eq!(page(&db, &query).await, initial);

    let mut tx = db.store.pool.begin().await.unwrap();
    crate::persistence::history(
        &mut tx,
        &[HistoryEvent {
            task_id: task.task_id.clone(),
            attempt_id: None,
            at: task.submitted_at,
            reason: TransitionReason::CancelRequested,
        }],
    )
    .await
    .unwrap();
    let store = db.store.clone();
    let task_id = task.task_id.clone();
    let writer = tokio::spawn(async move { store.cancel(&scope(), &task_id).await });
    wait_for_lock_waiter(&db.store).await;
    // Writer B cannot publish a later event while A owns the task counter.
    assert_eq!(page(&db, &query).await, initial);
    tx.commit().await.unwrap();
    writer.await.unwrap().unwrap();
    let committed = page(&db, &query).await;
    assert!(committed.terminal);
    assert_eq!(committed.state, "cancelled");
    assert_eq!(
        committed
            .events
            .iter()
            .map(|event| event.sequence.0)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert_eq!(committed.events[1].reason, "cancel_requested");
    assert_eq!(committed.events[2].reason, "cancel_requested");
    assert_eq!(committed.events[3].reason, "cancelled");
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn terminal_snapshot_is_not_a_seal_and_large_sequences_remain_exact() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    db.store.cancel(&scope(), &task.task_id).await.unwrap();
    let mut query = task_query(&task.task_id);
    let terminal = page(&db, &query).await;
    assert!(terminal.terminal && !terminal.has_more);
    query.cursor = Some(terminal.next_cursor.clone());

    // This is the same append operation used by the terminal budget-denial
    // cleanup path. Observers must be able to resume through its later event.
    let mut tx = db.store.pool.begin().await.unwrap();
    crate::persistence::history(
        &mut tx,
        &[HistoryEvent {
            task_id: task.task_id.clone(),
            attempt_id: None,
            at: task.submitted_at,
            reason: TransitionReason::CleanupConfirmed,
        }],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let late = page(&db, &query).await;
    assert!(late.terminal);
    assert_eq!(late.events.len(), 1);
    assert_eq!(late.events[0].reason, "cleanup_confirmed");

    // Rebase this synthetic history to the u64 boundary without converting
    // through a signed SQL integer or a floating-point browser number.
    let offset = u64::MAX - late.high_watermark.0;
    let mut tx = db.store.pool.begin().await.unwrap();
    sqlx::query("UPDATE task_history SET sequence=sequence+($2::text)::ldg_u64 WHERE task_id=$1")
        .bind(&task.task_id)
        .bind(offset.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE tasks SET history_sequence=history_sequence+($2::text)::ldg_u64 WHERE task_id=$1",
    )
    .bind(&task.task_id)
    .bind(offset.to_string())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    query.cursor = Some(query.cursor_for(offset).unwrap());
    let full_width = page(&db, &query).await;
    assert_eq!(full_width.high_watermark.0, u64::MAX);
    assert_eq!(full_width.events.last().unwrap().sequence.0, u64::MAX);
    let encoded = serde_json::to_value(&full_width).unwrap();
    assert_eq!(encoded["high_watermark"], json!(u64::MAX.to_string()));
    query.cursor = Some(full_width.next_cursor);
    assert!(page(&db, &query).await.events.is_empty());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn bounded_keyset_read_uses_history_index_and_rejects_missing_rows() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    sqlx::query("INSERT INTO task_history(task_id,sequence,at_ms,reason) SELECT $1,n,0,'submitted' FROM generate_series(2,20000) n")
        .bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET history_sequence=20000 WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("ANALYZE task_history")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let mut query = task_query(&task.task_id);
    query.limit = 7;
    query.cursor = Some(query.cursor_for(19000).unwrap());
    let observed = page(&db, &query).await;
    assert_eq!(observed.events.len(), 7);
    assert_eq!(observed.events[0].sequence.0, 19001);
    assert!(observed.has_more);

    let plan: Value = sqlx::query_scalar(AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {TASK_HISTORY_SQL}"
    )))
    .bind(&task.task_id)
    .bind("19000")
    .bind(7i64)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(7.0));
    let scan = &plan[0]["Plan"]["Plans"][0];
    assert_eq!(scan["Node Type"], "Index Scan");
    assert_eq!(scan["Actual Rows"].as_f64(), Some(7.0));
    assert_eq!(scan["Index Name"], "task_history_pkey");
    sqlx::query("DELETE FROM task_history WHERE task_id=$1 AND sequence=19003")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.execution_progress(&query).await,
        Err(ContractError::Unavailable(_))
    ));
    db.finish().await;
}

struct NoPrograms;
impl ledgence_worker_api::ProgramStore for NoPrograms {
    fn resolve<'a>(
        &'a self,
        _: &'a ledgence_worker_api::ProgramRef,
    ) -> ledgence_worker_api::PortFuture<'a, ledgence_worker_api::ProgramDescriptor> {
        panic!("progress must not resolve application programs")
    }
    fn fetch<'a>(
        &'a self,
        _: &'a ledgence_worker_api::ProgramDescriptor,
    ) -> ledgence_worker_api::PortFuture<'a, Vec<u8>> {
        panic!("progress must not fetch application packages")
    }
}
struct ProgressServer {
    client: ledgence_adapter_http::HttpTaskService,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}
impl ProgressServer {
    async fn new(store: PostgresStore) -> Self {
        use std::future::IntoFuture;
        let service = ledgence_orchestration_service::ApplicationService::new(
            Arc::new(store),
            Arc::new(NoPrograms),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            axum::serve(
                listener,
                ledgence_adapter_http::server::router(Arc::new(service)),
            )
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .into_future(),
        );
        Self {
            client: ledgence_adapter_http::HttpTaskService::new(&format!("http://{address}"))
                .unwrap(),
            stop: Some(stop),
            task: Some(server),
        }
    }
    async fn finish(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(10), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for ProgressServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn http_service_progress_resumes_after_server_and_database_adapter_restart() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let mut request = task_query(&task.task_id);
    let server = ProgressServer::new(db.store.clone()).await;
    let first = server
        .client
        .progress_reader()
        .unwrap()
        .execution_progress(&request)
        .await
        .unwrap();
    assert_eq!(first.state, "queued");
    assert_eq!(first.events[0].reason, "submitted");
    request.cursor = Some(first.next_cursor);
    server.finish().await;

    db.store.cancel(&scope(), &task.task_id).await.unwrap();
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let replacement = ProgressServer::new(reopened.clone()).await;
    let resumed = replacement
        .client
        .execution_progress(&request)
        .await
        .unwrap();
    assert_eq!(resumed.events.len(), 2);
    assert_eq!(resumed.events[0].reason, "cancel_requested");
    assert_eq!(resumed.events[1].reason, "cancelled");
    assert!(resumed.terminal && !resumed.has_more);

    let workflow = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    let workflow_page = replacement
        .client
        .execution_progress(&workflow_query(&workflow.workflow_id))
        .await
        .unwrap();
    assert_eq!(workflow_page.state, "running");
    assert_eq!(
        workflow_page.events[0].activation_id,
        workflow.activation_id
    );
    let mut wrong = task_query(&task.task_id);
    wrong.scope.tenant_id = "other".into();
    assert!(matches!(
        replacement.client.execution_progress(&wrong).await,
        Err(ContractError::NotFound)
    ));
    replacement.finish().await;
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn repeatable_read_keeps_state_tail_and_events_coherent_during_completion_and_collection() {
    let db = TestDb::new().await;
    let task = task(&db).await;
    let request = task_query(&task.task_id);
    let initial = page(&db, &request).await;
    let mut connection = db.store.transaction_connection().await.unwrap();
    let mut tx = connection.begin_read().await.unwrap();
    // Establish the same snapshot used by the production owner read, then
    // commit a new state/history on another connection before reading the page.
    sqlx::query("SELECT state FROM tasks WHERE task_id=$1")
        .bind(&task.task_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    db.store.cancel(&scope(), &task.task_id).await.unwrap();
    let before_commit = read_page(&mut tx, &request, 0).await.unwrap();
    assert_eq!(before_commit, initial);
    tx.commit().await.unwrap();
    let terminal = page(&db, &request).await;
    assert!(terminal.terminal);

    let mut tx = connection.begin_read().await.unwrap();
    sqlx::query("SELECT state FROM tasks WHERE task_id=$1")
        .bind(&task.task_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let mut collection = db.store.pool.begin().await.unwrap();
    sqlx::query("UPDATE tasks SET retiring_at_ms=terminal_at_ms WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&mut *collection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_history WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&mut *collection)
        .await
        .unwrap();
    collection.commit().await.unwrap();
    let before_retirement = read_page(&mut tx, &request, 0).await.unwrap();
    assert_eq!(before_retirement, terminal);
    tx.commit().await.unwrap();
    drop(connection);
    assert!(matches!(
        db.store.execution_progress(&request).await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}
