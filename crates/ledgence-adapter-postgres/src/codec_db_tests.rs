//! Storage fidelity and corruption tests against the actual migrated schema.

use crate::{codec, tests::*, *};
use ledgence_worker_api::{ExecutionContext, ExecutionReport, ExecutionRequest, ProgramOutcome};
use serde_json::json;

async fn claimed(db: &TestDb, command: &SubmitCommand) -> (TaskSnapshot, Assignment) {
    let task = db
        .store
        .accept_resolved_submission(command, &descriptor())
        .await
        .unwrap();
    let session = db
        .store
        .open_session(&scope(), &command.input.queue, 1)
        .await
        .unwrap();
    let assigned = assignment(
        db.store
            .acquire(&AcquireCommand {
                scope: scope(),
                queue: command.input.queue.clone(),
                worker_session_id: session.id,
                consumer_id: 0,
                sequence: 1,
            })
            .await
            .unwrap(),
    );
    (task, assigned)
}

#[tokio::test]
#[ignore = "requires LEDGENCE_POSTGRES_URL pointing to PostgreSQL 18"]
async fn codec_numeric_domain_preserves_full_u64_and_rejects_fractional_coercion() {
    let db = TestDb::new().await;
    for (value, expected) in [
        ("0", 0),
        ("1.000", 1),
        ("9223372036854775808", 1_u64 << 63),
        ("18446744073709551615", u64::MAX),
    ] {
        let text: String = sqlx::query_scalar("SELECT trunc(($1::text)::ldg_u64)::text")
            .bind(value)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        assert_eq!(codec::u64_text(&text).unwrap(), expected);
    }
    for value in ["-1", "1.5", "18446744073709551616", "NaN", "Infinity"] {
        let error = sqlx::query_scalar::<_, String>("SELECT (($1::text)::ldg_u64)::text")
            .bind(value)
            .fetch_one(&db.store.pool)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("23514"),
            "domain must reject {value} instead of rounding or wrapping"
        );
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires LEDGENCE_POSTGRES_URL pointing to PostgreSQL 18"]
async fn codec_indexed_submission_corruption_is_unavailable() {
    let db = TestDb::new().await;
    let command = command();
    let task = db
        .store
        .accept_resolved_submission(&command, &descriptor())
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET queue='different-queue' WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.inspect(&scope(), &task.task_id).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        db.store
            .lookup_submission(&scope(), &command.idempotency_key)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires LEDGENCE_POSTGRES_URL pointing to PostgreSQL 18"]
async fn codec_event_data_preserves_numbers_and_detects_changed_negative_zero() {
    let db = TestDb::new().await;
    let mut command = command();
    command.input.data = json!({"zero": -0.0, "integer": u64::MAX, "nul": "\u{0}"});
    let (task, assigned) = claimed(&db, &command).await;
    let restored = db
        .store
        .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        codec::encode(&restored.event.value()["data"]).unwrap(),
        codec::encode(&command.input.data).unwrap()
    );
    assert_eq!(
        restored.event.value()["data"]["zero"]
            .as_f64()
            .unwrap()
            .to_bits(),
        (-0.0_f64).to_bits()
    );
    let original = codec::encode(&assigned.event).unwrap();
    let mut changed = assigned.event.into_value();
    changed["data"]["zero"] = json!(0.0);
    sqlx::query("UPDATE attempts SET event_bytes=$2 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .bind(codec::encode(&changed).unwrap())
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    sqlx::query("UPDATE attempts SET event_bytes=$2 WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id)
        .bind(original)
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store
        .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires LEDGENCE_POSTGRES_URL pointing to PostgreSQL 18"]
async fn codec_accepted_receipt_and_report_bindings_are_checked_before_replay() {
    let db = TestDb::new().await;
    let (task, assigned) = claimed(&db, &command()).await;
    let mut settlement = SettleCommand {
        owner: assigned.lease.owner.clone(),
        operation_id: "codec-settlement".into(),
        report: AttemptReport::Completed(ExecutionReport {
            context: Box::new(ExecutionContext::from(&ExecutionRequest {
                descriptor: assigned.descriptor,
                event: assigned.event,
            })),
            process_id: 123,
            reused_process: false,
            outcome: ProgramOutcome::Success {
                output: json!({"zero": -0.0, "integer": u64::MAX, "nul": "\u{0}"}),
            },
            elapsed_ms: 1,
        }),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
    };
    let accepted = db.store.settle(&settlement).await.unwrap();
    let replay = db.store.settle(&settlement).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(accepted.receipt.accepted_at, replay.receipt.accepted_at);
    let restored = db
        .store
        .inspect_attempt(&scope(), &task.task_id, &settlement.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        codec::encode(&restored.settlement.unwrap().command).unwrap(),
        codec::encode(&settlement).unwrap()
    );

    sqlx::query(
        "UPDATE accepted_settlements SET operation_id='different-operation' WHERE attempt_id=$1",
    )
    .bind(&settlement.owner.attempt_id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert!(matches!(
        db.store.settle(&settlement).await,
        Err(ContractError::Unavailable(_))
    ));
    sqlx::query("UPDATE accepted_settlements SET operation_id=$2 WHERE attempt_id=$1")
        .bind(&settlement.owner.attempt_id)
        .bind(&settlement.operation_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    if let AttemptReport::Completed(report) = &mut settlement.report {
        report.context.identity.event_id = "different-event".into();
    }
    sqlx::query("UPDATE accepted_settlements SET accepted_command=$2 WHERE attempt_id=$1")
        .bind(&settlement.owner.attempt_id)
        .bind(codec::encode(&settlement).unwrap())
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .inspect_attempt(&scope(), &task.task_id, &settlement.owner.attempt_id)
            .await,
        Err(ContractError::Unavailable(_))
    ));
    db.finish().await;
}
