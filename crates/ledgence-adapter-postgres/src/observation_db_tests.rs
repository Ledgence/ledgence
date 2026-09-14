//! Real storage observations: lifecycle authority, snapshot consistency, and corruption.

use crate::{tests::*, *};
use ledgence_worker_api::{Error, ErrorKind, Phase, ProgramOutcome};
use serde_json::{Value, json};
use sqlx::{Column, Executor, TypeInfo};

async fn read(store: &PostgresStore, id: &str) -> TaskResult {
    let result = store.result(&scope(), id).await.unwrap();
    assert_eq!(store.status(&scope(), id).await.unwrap(), result.task);
    result
}

async fn force_expired(store: &PostgresStore, owner: &LeaseOwner) {
    let mut tx = store.pool.begin().await.unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=0 WHERE attempt_id=$1")
        .bind(&owner.attempt_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=0 WHERE task_id=$1")
        .bind(&owner.task_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn observations_distinguish_pending_null_and_current_cleanup_after_reconnect() {
    let db = TestDb::new().await;
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    assert!(read(&db.store, &task.task_id).await.outcome.is_none());
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let command = completed(&assigned, Quiescence::Unconfirmed, Value::Null);
    db.store.settle(&command).await.unwrap();
    assert!(read(&db.store, &task.task_id).await.outcome.is_none());
    let store = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    store
        .confirm_quiescence(&assigned.lease.owner)
        .await
        .unwrap();
    let result = read(&store, &task.task_id).await;
    assert_eq!(
        result.outcome,
        Some(TaskOutcome::Succeeded {
            attempt_id: assigned.lease.owner.attempt_id.clone(),
            quiescence: Quiescence::Confirmed,
            execution_may_have_started: true,
            output: Value::Null,
        })
    );
    let replay = store.settle(&command).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(read(&store, &task.task_id).await, result);
    let attempt = store
        .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        attempt.settlement.unwrap().command.quiescence,
        Quiescence::Unconfirmed
    );
    store.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn reads_do_not_expire_work_and_expiry_preserves_accepted_output_and_cleanup_evidence() {
    let db = TestDb::new().await;
    let (task, _, assigned) = claimed(&db.store).await;
    let payload = json!([null, false, -0.0, u64::MAX, i64::MIN, "left\u{0000}right"]);
    let command = completed(&assigned, Quiescence::Unconfirmed, payload.clone());
    db.store.settle(&command).await.unwrap();
    force_expired(&db.store, &assigned.lease.owner).await;
    let history_before = db.store.history(&scope(), &task.task_id, 0).await.unwrap();
    let before = read(&db.store, &task.task_id).await;
    assert_eq!(before.task.state, TaskState::Active);
    assert!(before.outcome.is_none());
    assert_eq!(
        db.store
            .history(&scope(), &task.task_id, 0)
            .await
            .unwrap()
            .len(),
        history_before.len()
    );
    assert_eq!(db.store.expire_batch(1).await.unwrap().expired, 1);
    let result = read(&db.store, &task.task_id).await;
    let Some(TaskOutcome::Succeeded {
        output, quiescence, ..
    }) = result.outcome
    else {
        panic!("expected success")
    };
    assert_eq!(quiescence, Quiescence::Unconfirmed);
    assert_eq!(
        canonical_json_bytes(&output).unwrap(),
        canonical_json_bytes(&payload).unwrap()
    );
    assert!(db.store.settle(&command).await.unwrap().already_accepted);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn result_tracks_latest_retry_and_cancel_does_not_use_earlier_failure_as_its_cause() {
    let db = TestDb::new().await;
    let (task, session, assigned) = claimed(&db.store).await;
    let mut failed = completed(&assigned, Quiescence::Confirmed, Value::Null);
    let AttemptReport::Completed(report) = &failed.report else {
        unreachable!()
    };
    failed.report = AttemptReport::Failed(ledgence_worker_api::ExecutionFailure {
        context: report.context.clone(),
        error: Error::new(ErrorKind::Io, "transient failure"),
        cleanup_error: Some(Error::new(ErrorKind::Runtime, "cleanup diagnostic")),
        phase: Phase::Preparation,
        execution_may_have_started: false,
    });
    db.store.settle(&failed).await.unwrap();
    let retry = read(&db.store, &task.task_id).await;
    assert_eq!(retry.task.state, TaskState::Queued);
    assert!(retry.outcome.is_none());
    assert_eq!(
        retry.task.latest_attempt_id.as_ref(),
        Some(&assigned.lease.owner.attempt_id)
    );
    let next = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 2))
            .await
            .unwrap(),
    );
    assert_ne!(next.lease.owner.attempt_id, assigned.lease.owner.attempt_id);
    let next_failure = completed(&next, Quiescence::Confirmed, json!(42));
    db.store.settle(&next_failure).await.unwrap();
    let done = read(&db.store, &task.task_id).await;
    assert_eq!(done.task.attempt_count, 2);
    assert!(
        matches!(done.outcome, Some(TaskOutcome::Succeeded { attempt_id, output, .. }) if attempt_id == next.lease.owner.attempt_id && output == json!(42))
    );
    assert!(db.store.settle(&failed).await.unwrap().already_accepted);
    assert_eq!(
        db.store.result(&scope(), &task.task_id).await.unwrap().task,
        done.task
    );

    let mut new_command = command();
    new_command.idempotency_key = "cancelled_retry".into();
    let new_task = db
        .store
        .accept_resolved_submission(&new_command, &descriptor())
        .await
        .unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 3))
            .await
            .unwrap(),
    );
    force_expired(&db.store, &assigned.lease.owner).await;
    db.store.expire_batch(1).await.unwrap();
    db.store.cancel(&scope(), &new_task.task_id).await.unwrap();
    let cancelled = read(&db.store, &new_task.task_id).await;
    assert_eq!(cancelled.outcome, Some(TaskOutcome::Cancelled {}));
    assert_eq!(
        cancelled.task.latest_attempt_id,
        Some(assigned.lease.owner.attempt_id)
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_order_and_terminal_failure_evidence_survive_projection() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    for (index, mode) in [
        "cancel_success",
        "success_cancel",
        "application",
        "execution",
        "lost",
    ]
    .into_iter()
    .enumerate()
    {
        let mut submit = command();
        submit.idempotency_key = mode.into();
        submit.input.retry_policy.max_attempts = 1;
        let task = db
            .store
            .accept_resolved_submission(&submit, &descriptor())
            .await
            .unwrap();
        let assigned = assignment(
            db.store
                .acquire(&acquire_command(&session, 0, index as u64 + 1))
                .await
                .unwrap(),
        );
        let mut report = completed(&assigned, Quiescence::Confirmed, Value::Null);
        match mode {
            "cancel_success" => {
                db.store.cancel(&scope(), &task.task_id).await.unwrap();
            }
            "application" => {
                let AttemptReport::Completed(r) = &mut report.report else {
                    unreachable!()
                };
                r.outcome = ProgramOutcome::Failure {
                    kind: "InvoiceError".into(),
                    message: "bad invoice".into(),
                };
            }
            "execution" => {
                let AttemptReport::Completed(r) = &report.report else {
                    unreachable!()
                };
                report.report = AttemptReport::Failed(ledgence_worker_api::ExecutionFailure {
                    context: r.context.clone(),
                    error: Error::new(ErrorKind::Runtime, "runtime stopped"),
                    cleanup_error: Some(Error::new(ErrorKind::Io, "cleanup diagnostic")),
                    phase: Phase::Execution,
                    execution_may_have_started: true,
                });
            }
            "lost" => {
                force_expired(&db.store, &assigned.lease.owner).await;
                db.store.expire_batch(1).await.unwrap();
            }
            _ => {}
        }
        if mode != "lost" {
            db.store.settle(&report).await.unwrap();
        }
        if mode == "success_cancel" {
            db.store.cancel(&scope(), &task.task_id).await.unwrap();
        }
        let result = read(&db.store, &task.task_id).await;
        match (mode, result.outcome) {
            ("cancel_success", Some(TaskOutcome::Cancelled {}))
            | ("success_cancel", Some(TaskOutcome::Succeeded { .. })) => {}
            (
                "application",
                Some(TaskOutcome::Failed {
                    failure: TaskFailure::Application { error },
                    ..
                }),
            ) => assert_eq!(error.kind, "InvoiceError"),
            (
                "execution",
                Some(TaskOutcome::Failed {
                    failure: TaskFailure::Execution { cleanup_error, .. },
                    ..
                }),
            ) => assert!(cleanup_error.is_some()),
            (
                "lost",
                Some(TaskOutcome::Failed {
                    failure: TaskFailure::AttemptLost {},
                    execution_may_have_started: false,
                    ..
                }),
            ) => {}
            other => panic!("unexpected projected outcome: {other:?}"),
        }
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn one_statement_result_remains_coherent_across_uncommitted_and_committed_finalization() {
    let db = TestDb::new().await;
    let (task, _, assigned) = claimed(&db.store).await;
    let command = completed(&assigned, Quiescence::Confirmed, json!("done"));
    let mut tx = db.store.pool.begin().await.unwrap();
    let current = persistence::load_task(&mut tx, &scope(), &task.task_id, true)
        .await
        .unwrap();
    let attempt = persistence::load_attempt(&mut tx, &current, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    let transition = ledgence_orchestration_core::settle(
        &current,
        &attempt,
        &command,
        persistence::now(&mut tx).await.unwrap(),
    )
    .unwrap();
    persistence::apply(&mut tx, &transition).await.unwrap();
    // Real task/attempt/report changes exist but are not committed; observations
    // must see the earlier snapshot and must not queue for a mutation row lock.
    let before = tokio::time::timeout(
        Duration::from_secs(1),
        db.store.result(&scope(), &task.task_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(before.task.state, TaskState::Active);
    assert!(before.outcome.is_none());
    let store = db.store.clone();
    let task_id = task.task_id.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..40 {
            let result = store.result(&scope(), &task_id).await.unwrap();
            match result.task.state {
                TaskState::Active => assert!(result.outcome.is_none()),
                TaskState::Succeeded => assert!(
                    matches!(result.outcome, Some(TaskOutcome::Succeeded { output, .. }) if output == json!("done"))
                ),
                other => panic!("unexpected state: {other:?}"),
            }
        }
    });
    tx.commit().await.unwrap();
    reader.await.unwrap();
    assert_eq!(
        read(&db.store, &task.task_id).await.task.state,
        TaskState::Succeeded
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn compact_status_selects_no_payload_columns_and_corrupt_results_are_unavailable() {
    let db = TestDb::new().await;
    let described = db
        .store
        .pool
        .describe(sqlx::SqlStr::from_static(include_str!(
            "../queries/task_status.sql"
        )))
        .await
        .unwrap();
    assert!(
        described
            .columns()
            .iter()
            .all(|column| column.type_info().name() != "BYTEA")
    );
    assert_eq!(described.columns().len(), 16);
    let (task, _, assigned) = claimed(&db.store).await;
    db.store
        .settle(&completed(&assigned, Quiescence::Confirmed, Value::Null))
        .await
        .unwrap();
    let original: Vec<u8> =
        sqlx::query_scalar("SELECT accepted_command FROM accepted_settlements WHERE attempt_id=$1")
            .bind(&assigned.lease.owner.attempt_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    sqlx::query("UPDATE accepted_settlements SET accepted_command=$2 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .bind(b"{\"duplicate\":1,\"duplicate\":2}".as_slice())
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .status(&scope(), &task.task_id)
            .await
            .unwrap()
            .state,
        TaskState::Succeeded
    );
    assert!(matches!(
        db.store.result(&scope(), &task.task_id).await,
        Err(ContractError::Unavailable(_))
    ));
    sqlx::query("UPDATE accepted_settlements SET accepted_command=$2 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .bind(original)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM accepted_settlements WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.result(&scope(), &task.task_id).await,
        Err(ContractError::Unavailable(_))
    ));
    sqlx::query("UPDATE tasks SET attempt_count=2 WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.status(&scope(), &task.task_id).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        db.store.result(&scope(), &task.task_id).await,
        Err(ContractError::Unavailable(_))
    ));
    for read_result in [
        db.store.result(&scope(), "missing").await.map(|_| ()),
        db.store
            .status(
                &Scope {
                    tenant_id: "other".into(),
                    namespace: "billing".into(),
                },
                &task.task_id,
            )
            .await
            .map(|_| ()),
    ] {
        assert_eq!(read_result, Err(ContractError::NotFound));
    }
    db.store.close().await;
    assert!(matches!(
        db.store.result(&scope(), &task.task_id).await,
        Err(ContractError::Unavailable(_))
    ));
    db.finish().await;
}
