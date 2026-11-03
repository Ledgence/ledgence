//! Exercise the real previous schema. Encode old grants and journal rows as
//! the previous writer did, independently of the new typed confirmation path.
use super::*;
use sqlx::migrate::Migrator;

async fn previous_database() -> TestDb {
    let db = TestDb::without_migrations().await;
    let previous = Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|m| m.version < crate::migration_effects::VERSION)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    db.begin_legacy_workflow_seed().await;
    db
}

async fn old_grant(store: &PostgresStore, queue: &str) -> (Assignment, ApprovalDecisionCommand) {
    let decision = approved_effects::request(store, queue).await;
    let (assignment, _) = approved_effects::resume(store, queue, &decision).await;
    dispatched(store, &assignment).await;
    sqlx::query("UPDATE workflow_approvals SET effect_dispatch_claimed=true, effect_dispatch_attempt_id=$2 WHERE workflow_id=$1")
        .bind(&decision.workflow_id).bind(&assignment.lease.owner.attempt_id).execute(&store.pool).await.unwrap();
    (assignment, decision)
}

async fn journal(
    store: &PostgresStore,
    accepting: &Assignment,
    decision: &ApprovalDecisionCommand,
    action: &ApprovalAction,
) {
    let key: String =
        sqlx::query_scalar("SELECT '_approval:' || encode(sha256(convert_to($1,'UTF8')),'hex')")
            .bind(&decision.key)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    let record = LocalStepRecord {
        key,
        callable: decision.action.name.clone(),
        input: json!({"v":1,"action_json":serde_json::to_string(action).unwrap(),"lookup":"payments:lookup"}),
        output: json!({"provider_receipt": "receipt-1042"}),
    };
    sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms,callable) VALUES($1,$2,$3,$4,1,$5)")
        .bind(&accepting.lease.owner.task_id).bind(&record.key).bind(codec::encode(&record).unwrap())
        .bind(&accepting.lease.owner.attempt_id).bind(&record.callable).execute(&store.pool).await.unwrap();
}

async fn complete(store: &PostgresStore, assignment: &Assignment) {
    apply_decision(
        store,
        assignment,
        1,
        WorkflowAction::Complete {
            output: json!("done"),
        },
        &[],
    )
    .await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn exact_historic_receipts_release_retention_while_uncertain_dispatches_survive() {
    let db = previous_database().await;
    let (sent, exact) = old_grant(&db.store, "exact").await;
    // A replacement worker got an external receipt via lookup; its attempt is
    // intentionally different from the grant. The old action contains NUL.
    owned::failed_attempt(&db.store, &sent).await;
    let replacement = acquire(&db.store, "exact").await;
    dispatched(&db.store, &replacement).await;
    journal(&db.store, &replacement, &exact, &exact.action).await;
    complete(&db.store, &replacement).await;

    let (conflicting, changed) = old_grant(&db.store, "changed").await;
    let mut wrong = changed.action.clone();
    wrong.arguments["count"] = json!(1.0);
    journal(&db.store, &conflicting, &changed, &wrong).await;
    complete(&db.store, &conflicting).await;

    let (uncertain, missing) = old_grant(&db.store, "missing").await;
    db.store
        .cancel_workflow(&scope(), &missing.workflow_id)
        .await
        .unwrap();
    // Finish the in-flight task and process the workflow's terminal drain.
    db.store
        .settle(&completed(&uncertain, Quiescence::Confirmed, json!(null)))
        .await
        .unwrap();
    drain_available(&db.store).await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &missing.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );

    let (legacy_fence, fenced) = old_grant(&db.store, "previous-ordinary").await;
    sqlx::query(
        "UPDATE workflow_approvals SET effect_dispatch_attempt_id=NULL WHERE workflow_id=$1",
    )
    .bind(&fenced.workflow_id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    complete(&db.store, &legacy_fence).await;

    // Model a deployment interrupted after SQLx committed the schema, before
    // inspecting the old bytes. The new server must refuse startup until resumed.
    db.finish_legacy_workflow_seed().await;
    MIGRATOR.run(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.verify_schema().await,
        Err(ContractError::Unavailable(_))
    ));
    db.store.migrate().await.unwrap();
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    type EvidenceRow = (String, Option<String>, Option<String>, Option<String>);
    let rows: Vec<EvidenceRow> = sqlx::query_as(
        "SELECT workflow_id,effect_evidence_state,effect_lookup,effect_confirmed_attempt_id FROM workflow_approvals",
    ).fetch_all(&db.store.pool).await.unwrap();
    assert_eq!(rows.len(), 4);
    assert!(rows.contains(&(
        exact.workflow_id.clone(),
        Some("legacy_confirmed".into()),
        Some("payments:lookup".into()),
        Some(replacement.lease.owner.attempt_id)
    )));
    for id in [&missing.workflow_id, &changed.workflow_id] {
        assert!(rows.contains(&(id.clone(), Some("open".into()), None, None)));
    }
    assert!(rows.contains(&(fenced.workflow_id.clone(), None, None, None)));

    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    for _ in 0..500 {
        db.store
            .retain_batch(
                &scope(),
                &RetentionPolicy::default(),
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
    for id in [&changed.workflow_id, &missing.workflow_id] {
        assert!(db.store.workflow_result(&scope(), id).await.is_ok());
        let preserved: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_approvals a JOIN attempts t ON t.attempt_id=a.effect_dispatch_attempt_id WHERE a.workflow_id=$1)")
            .bind(id).fetch_one(&db.store.pool).await.unwrap();
        assert!(
            preserved,
            "unresolved original dispatch attempt must survive retention"
        );
    }
    for id in [&exact.workflow_id, &fenced.workflow_id] {
        assert!(matches!(
            db.store.workflow_status(&scope(), id).await,
            Err(ContractError::NotFound)
        ));
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn pending_retirement_of_a_surviving_unconfirmed_grant_requires_restoration() {
    let db = previous_database().await;
    let (assigned, decision) = old_grant(&db.store, "already-retiring").await;
    complete(&db.store, &assigned).await;
    sqlx::query("UPDATE workflow_runs SET retiring_at_ms=terminal_at_ms WHERE workflow_id=$1")
        .bind(&decision.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.finish_legacy_workflow_seed().await;
    let error = db.store.migrate().await.unwrap_err();
    assert!(
        matches!(error,ContractError::Unavailable(message) if message.contains("pre-retirement backup"))
    );
    assert!(db.store.verify_schema().await.is_err());
    db.finish().await;
}
