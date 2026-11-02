use super::*;

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn inspector_reads_only_one_live_accepted_record_and_rejects_mismatched_projection() {
    let db = TestDb::new().await;
    let (workflow, assigned) = workflow(&db, "inspector").await;
    let owner = &assigned.lease.owner;
    let request = ConsoleLocalStepLookup {
        workflow_id: workflow.workflow_id.clone(),
        activation_id: owner.task_id.clone(),
        step_key: "tool:1".into(),
    };
    let other = ConsoleLocalStepLookup {
        step_key: "other".into(),
        ..request.clone()
    };
    let arguments = json!({"record":u64::MAX,"amount":1.0,"label":"a\0b"});
    let command = LocalResultCommand {
        owner: owner.clone(),
        record: LocalStepRecord {
            key: request.step_key.clone(),
            callable: "tool:billing:issue".into(),
            input: json!({"v":1,"kind":"tool","version":"1","arguments":arguments}),
            output: json!({"done":true,"id":9007199254740993_u64}),
        },
    };
    db.store.record_local_result(&command).await.unwrap();
    db.store
        .record_local_result(&LocalResultCommand {
            owner: owner.clone(),
            record: LocalStepRecord {
                key: other.step_key.clone(),
                ..command.record.clone()
            },
        })
        .await
        .unwrap();
    // The presence of an unreadable neighboring record cannot prevent the exact lookup.
    sqlx::query("UPDATE workflow_local_results SET record_bytes=decode('00','hex') WHERE activation_id=$1 AND step_key=$2")
        .bind(&other.activation_id).bind(&other.step_key).execute(&db.store.pool).await.unwrap();
    let accepted = db
        .store
        .inspect_local_step(&scope(), &request)
        .await
        .unwrap();
    assert_eq!(accepted.step.attempt_id, owner.attempt_id);
    assert!(accepted.step.accepted_at > 0);
    assert_eq!(accepted.kind, ConsoleOperationKind::Tool);
    assert_eq!(accepted.version.as_deref(), Some("1"));
    assert_eq!(accepted.arguments, arguments);
    assert_eq!(accepted.result, command.record.output);
    for missing in [
        ConsoleLocalStepLookup {
            workflow_id: "another-workflow".into(),
            ..request.clone()
        },
        ConsoleLocalStepLookup {
            activation_id: "another-activation".into(),
            ..request.clone()
        },
        ConsoleLocalStepLookup {
            step_key: "missing".into(),
            ..request.clone()
        },
    ] {
        assert_eq!(
            db.store
                .inspect_local_step(&scope(), &missing)
                .await
                .unwrap_err(),
            ContractError::NotFound
        );
    }
    let wrong_scope = Scope {
        namespace: "other".into(),
        ..scope()
    };
    assert_eq!(
        db.store
            .inspect_local_step(&wrong_scope, &request)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert!(matches!(
        db.store.inspect_local_step(&scope(), &other).await,
        Err(ContractError::Unavailable(_))
    ));
    sqlx::query(
        "UPDATE workflow_local_results SET callable='wrong' WHERE activation_id=$1 AND step_key=$2",
    )
    .bind(&request.activation_id)
    .bind(&request.step_key)
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert!(matches!(
        db.store.inspect_local_step(&scope(), &request).await,
        Err(ContractError::Unavailable(_))
    ));
    apply(
        &db,
        &assigned,
        WorkflowAction::Complete {
            output: Value::Null,
        },
        &[],
    )
    .await;
    sqlx::query("UPDATE tasks SET retiring_at_ms=terminal_at_ms WHERE task_id=$1")
        .bind(&owner.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .inspect_local_step(&scope(), &request)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    sqlx::query("UPDATE tasks SET retiring_at_ms=NULL WHERE task_id=$1")
        .bind(&owner.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_runs SET retiring_at_ms=terminal_at_ms WHERE workflow_id=$1")
        .bind(&workflow.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .inspect_local_step(&scope(), &request)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    db.finish().await;
}
