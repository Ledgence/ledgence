//! Real transactions exercise identity, rollback, recovery and bounded collection.
use super::*;
use sqlx::Connection;
fn config() -> SelfHostedInstanceConfig {
    SelfHostedInstanceConfig {
        instance_id: "audit-test".into(),
        name: "Audit fixture".into(),
        scope: scope(),
        suggested_queues: vec!["python".into()],
        allowed_origins: vec![],
    }
}
fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "audit-authority".into(),
        instance_id: config().instance_id,
        scope: scope(),
    }
}
fn principal(subject: &str, credential: &str, expires: u64) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: subject.into(),
        },
        audience(),
        credential.into(),
        PrincipalClass::Operator,
        expires,
        vec![
            AccessCapability::SensitiveRead,
            AccessCapability::MetadataRead,
            AccessCapability::ApprovalDecide,
        ],
    )
    .unwrap()
}
fn actor() -> VerifiedPrincipal {
    principal("alice", "credential-one", 253_402_300_799_999)
}
fn request(id: &str) -> OperatorRequest {
    OperatorRequest::new(id.into(), Some("same-caller-correlation".into())).unwrap()
}
async fn bind(store: &PostgresStore) {
    store
        .initialize_instance_with_access(Some(&config()), Some(&audience()))
        .await
        .unwrap();
}
async fn pending(store: &PostgresStore) -> ApprovalDecisionCommand {
    let workflow = start(store).await;
    let assigned = acquire(store, "python").await;
    apply_decision(store, &assigned, 0, approvals::wait("review", 60_000), &[]).await;
    approvals::decide(&workflow, &assigned, "review")
}
fn exact(command: &ApprovalDecisionCommand) -> AuditQuery {
    AuditQuery {
        scope: scope(),
        filter: AuditFilter::ApprovalAcceptance {
            workflow_id: command.workflow_id.clone(),
            key: command.key.clone(),
            activation_id: command.activation_id.clone(),
            revision: command.revision,
            decision_id: command.decision_id.clone(),
        },
        cursor: None,
        limit: 2,
    }
}
fn all() -> AuditQuery {
    AuditQuery {
        scope: scope(),
        filter: AuditFilter::All,
        cursor: None,
        limit: 100,
    }
}
fn observation(id: &str, task: &str) -> SensitiveObservation {
    SensitiveObservation {
        request: request(id),
        read: AuditReadKind::TaskResult,
        resource: AuditResource::Task {
            task_id: task.into(),
        },
    }
}
async fn count(store: &PostgresStore) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM operator_audit_events")
        .fetch_one(&store.pool)
        .await
        .unwrap()
}
async fn now(store: &PostgresStore) -> u64 {
    let mut conn = store.pool.acquire().await.unwrap();
    db::now(&mut conn).await.unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn verified_acceptance_replays_only_original_actor_across_rotation_and_cancellation() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let command = pending(&db.store).await;
    let result = db
        .store
        .decide_verified(&actor(), &command, &request("request-1"))
        .await
        .unwrap();
    result.validate_for(&actor(), &command).unwrap();
    assert!(!result.receipt.already_accepted);
    let DecisionAttribution::Verified { event } = result.attribution else {
        panic!("missing acceptance")
    };
    assert_eq!(event.actor.subject, "alice");
    assert_eq!(
        event.recorded_at,
        result
            .receipt
            .approval
            .decision
            .as_ref()
            .unwrap()
            .decided_at
    );
    let stored: Vec<u8> =
        sqlx::query_scalar("SELECT decision_bytes FROM workflow_approvals WHERE workflow_id=$1")
            .bind(&command.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(stored, codec::encode(&command).unwrap());
    assert_eq!(count(&db.store).await, 1);
    let page = db
        .store
        .query_audit(&actor(), &exact(&command))
        .await
        .unwrap();
    assert_eq!(page.items, vec![(*event).clone()]);
    assert!(page.next_cursor.is_none());
    let rotated = principal("alice", "credential-two", 253_402_300_799_999);
    let replay = db
        .store
        .decide_verified(&rotated, &command, &request("request-2"))
        .await
        .unwrap();
    assert!(replay.receipt.already_accepted);
    let DecisionAttribution::Verified { event: replayed } = replay.attribution else {
        panic!("missing attribution")
    };
    assert_eq!(replayed, event);
    assert!(matches!(
        db.store
            .decide_verified(
                &principal("bob", "b", 253_402_300_799_999),
                &command,
                &request("request-3")
            )
            .await,
        Err(ContractError::Conflict)
    ));
    let mut changed = command.clone();
    changed.reason = Some("different bytes".into());
    assert!(matches!(
        db.store
            .decide_verified(&actor(), &changed, &request("request-4"))
            .await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        db.store.decide_approval(&command).await,
        Err(ContractError::Forbidden)
    ));
    db.store
        .cancel_workflow(&scope(), &command.workflow_id)
        .await
        .unwrap();
    assert!(
        db.store
            .decide_verified(&rotated, &command, &request("request-5"))
            .await
            .unwrap()
            .receipt
            .already_accepted
    );
    assert_eq!(count(&db.store).await, 1);
    let reconnect = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        reconnect.decide_approval(&command).await,
        Err(ContractError::Forbidden)
    ));
    reconnect.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn legacy_decision_bytes_remain_unattributed_after_access_is_enabled() {
    let db = TestDb::new().await;
    let command = pending(&db.store).await;
    db.store.decide_approval(&command).await.unwrap();
    let before: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT snapshot_bytes,decision_bytes FROM workflow_approvals WHERE workflow_id=$1",
    )
    .bind(&command.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    bind(&db.store).await;
    let replay = db
        .store
        .decide_verified(&actor(), &command, &request("legacy-replay"))
        .await
        .unwrap();
    assert!(replay.receipt.already_accepted);
    assert!(matches!(
        replay.attribution,
        DecisionAttribution::LegacyUnverified
    ));
    assert!(
        db.store
            .query_audit(&actor(), &exact(&command))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let after: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT snapshot_bytes,decision_bytes FROM workflow_approvals WHERE workflow_id=$1",
    )
    .bind(&command.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(before, after);
    assert_eq!(count(&db.store).await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn suppressed_acceptance_decision_wake_or_history_rolls_back_all_effects() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let command = pending(&db.store).await;
    sqlx::query("CREATE FUNCTION audit_test_suppress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$").execute(&db.store.pool).await.unwrap();
    for statement in [
        "CREATE TRIGGER suppress_audit BEFORE INSERT ON operator_audit_events FOR EACH ROW EXECUTE FUNCTION audit_test_suppress()",
        "CREATE TRIGGER suppress_audit BEFORE UPDATE ON workflow_approvals FOR EACH ROW EXECUTE FUNCTION audit_test_suppress()",
        "CREATE TRIGGER suppress_audit BEFORE UPDATE ON workflow_work FOR EACH ROW EXECUTE FUNCTION audit_test_suppress()",
        "CREATE TRIGGER suppress_audit BEFORE INSERT ON workflow_history FOR EACH ROW EXECUTE FUNCTION audit_test_suppress()",
    ] {
        sqlx::query(statement)
            .execute(&db.store.pool)
            .await
            .unwrap();
        assert!(matches!(
            db.store
                .decide_verified(&actor(), &command, &request("suppressed"))
                .await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(count(&db.store).await, 0);
        assert_eq!(
            db.store
                .approval(&scope(), &command.workflow_id, &command.key)
                .await
                .unwrap()
                .status,
            ApprovalStatus::Pending
        );
        assert!(db.store.claim_work(16).await.unwrap().is_empty());
        for drop in [
            "DROP TRIGGER IF EXISTS suppress_audit ON operator_audit_events",
            "DROP TRIGGER IF EXISTS suppress_audit ON workflow_approvals",
            "DROP TRIGGER IF EXISTS suppress_audit ON workflow_work",
            "DROP TRIGGER IF EXISTS suppress_audit ON workflow_history",
        ] {
            sqlx::query(drop).execute(&db.store.pool).await.unwrap();
        }
    }
    db.store
        .decide_verified(&actor(), &command, &request("accepted"))
        .await
        .unwrap();
    assert_eq!(count(&db.store).await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn principal_expiring_while_waiting_for_workflow_lock_cannot_accept() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let command = pending(&db.store).await;
    let p = principal("alice", "short", now(&db.store).await + 400);
    let mut conn = db.store.pool.acquire().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&command.workflow_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let store = db.store.clone();
    let decision = command.clone();
    let pending = tokio::spawn(async move {
        store
            .decide_verified(&p, &decision, &request("expired-lock"))
            .await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    tx.commit().await.unwrap();
    drop(conn);
    assert!(matches!(
        pending.await.unwrap(),
        Err(ContractError::Unauthenticated)
    ));
    assert_eq!(count(&db.store).await, 0);
    assert_eq!(
        db.store
            .approval(&scope(), &command.workflow_id, &command.key)
            .await
            .unwrap()
            .status,
        ApprovalStatus::Pending
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn response_observations_deduplicate_only_server_identity_and_query_by_scoped_resource() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let first = observation("obs-one", "task-one");
    let accepted = db
        .store
        .record_sensitive_response(&actor(), &first)
        .await
        .unwrap();
    assert!(!accepted.already_recorded);
    let replay = db
        .store
        .record_sensitive_response(&actor(), &first)
        .await
        .unwrap();
    assert!(replay.already_recorded);
    assert_eq!(replay.event, accepted.event);
    let conflicting = observation("obs-one", "task-two");
    assert!(matches!(
        db.store
            .record_sensitive_response(&actor(), &conflicting)
            .await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        db.store
            .record_sensitive_response(&principal("bob", "other", 253_402_300_799_999), &first)
            .await,
        Err(ContractError::Conflict)
    ));
    let other = observation("obs-two", "task-two");
    db.store
        .record_sensitive_response(&actor(), &other)
        .await
        .unwrap();
    assert_eq!(count(&db.store).await, 2);
    let mut q = all();
    q.limit = 1;
    let newest = db.store.query_audit(&actor(), &q).await.unwrap();
    assert_eq!(newest.items[0].event_id, "obs-two");
    assert!(newest.next_cursor.is_some());
    q.cursor = newest.next_cursor;
    let oldest = db.store.query_audit(&actor(), &q).await.unwrap();
    assert_eq!(oldest.items[0].event_id, "obs-one");
    assert!(oldest.next_cursor.is_none());
    q = all();
    q.filter = AuditFilter::Resource {
        resource_kind: AuditResourceKind::Task,
        resource_id: "task-one".into(),
    };
    assert_eq!(
        db.store.query_audit(&actor(), &q).await.unwrap().items,
        vec![accepted.event]
    );
    let expired = principal("alice", "expired", 1);
    assert!(matches!(
        db.store
            .record_sensitive_response(&expired, &observation("expired", "t"))
            .await,
        Err(ContractError::Unauthenticated)
    ));
    assert!(matches!(
        db.store.query_audit(&expired, &all()).await,
        Err(ContractError::Unauthenticated)
    ));
    assert_eq!(count(&db.store).await, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn audience_permission_and_suppressed_observation_fail_closed() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let mut foreign = audience();
    foreign.authority_id = "foreign".into();
    let foreign = VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: foreign.issuer(),
            subject: "alice".into(),
        },
        foreign,
        "key".into(),
        PrincipalClass::Operator,
        253_402_300_799_999,
        vec![
            AccessCapability::SensitiveRead,
            AccessCapability::MetadataRead,
        ],
    )
    .unwrap();
    assert!(matches!(
        db.store
            .record_sensitive_response(&foreign, &observation("wrong-authority", "task"))
            .await,
        Err(ContractError::Forbidden)
    ));
    assert!(matches!(
        db.store.query_audit(&foreign, &all()).await,
        Err(ContractError::Forbidden)
    ));
    let metadata = VerifiedPrincipal::new(
        actor().identity().clone(),
        audience(),
        "metadata".into(),
        PrincipalClass::Operator,
        253_402_300_799_999,
        vec![AccessCapability::MetadataRead],
    )
    .unwrap();
    assert!(matches!(
        db.store
            .record_sensitive_response(&metadata, &observation("forbidden", "task"))
            .await,
        Err(ContractError::Forbidden)
    ));
    assert!(
        db.store
            .query_audit(&metadata, &all())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    sqlx::query("CREATE FUNCTION audit_test_suppress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$").execute(&db.store.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER suppress_audit BEFORE INSERT ON operator_audit_events FOR EACH ROW EXECUTE FUNCTION audit_test_suppress()").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store
            .record_sensitive_response(&actor(), &observation("suppressed", "task"))
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(count(&db.store).await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn read_retention_is_bounded_and_never_deletes_acceptance_evidence() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let command = pending(&db.store).await;
    db.store
        .decide_verified(&actor(), &command, &request("accept"))
        .await
        .unwrap();
    for n in 0..5 {
        let mut event = db
            .store
            .record_sensitive_response(&actor(), &observation(&format!("old-{n}"), "task"))
            .await
            .unwrap()
            .event;
        event.recorded_at = 1;
        sqlx::query(
            "UPDATE operator_audit_events SET recorded_at_ms=1,event_bytes=$2 WHERE event_id=$1",
        )
        .bind(&event.event_id)
        .bind(codec::encode(&event).unwrap())
        .execute(&db.store.pool)
        .await
        .unwrap();
    }
    let mut intermediate = db
        .store
        .record_sensitive_response(&actor(), &observation("intermediate", "task"))
        .await
        .unwrap()
        .event;
    intermediate.recorded_at = now(&db.store).await - 100 * 24 * 60 * 60 * 1000;
    sqlx::query(
        "UPDATE operator_audit_events SET recorded_at_ms=$2,event_bytes=$3 WHERE event_id=$1",
    )
    .bind(&intermediate.event_id)
    .bind(codec::ms(intermediate.recorded_at).unwrap())
    .bind(codec::encode(&intermediate).unwrap())
    .execute(&db.store.pool)
    .await
    .unwrap();
    let policy = RetentionPolicy {
        batch_size: 2,
        retain_for_ms: 365 * 24 * 60 * 60 * 1000,
    };
    let preview = db
        .store
        .retention_preview(&scope(), &policy, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(preview.audit_observation_candidates.len(), 2);
    for _ in 0..3 {
        sqlx::query("INSERT INTO retention_turn(tenant_id,namespace,lane) VALUES($1,$2,3) ON CONFLICT(tenant_id,namespace) DO UPDATE SET lane=3").bind(scope().tenant_id).bind(scope().namespace).execute(&db.store.pool).await.unwrap();
        let progress = db
            .store
            .retain_batch(&scope(), &policy, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        assert!(progress.deleted_rows <= 2);
    }
    assert_eq!(count(&db.store).await, 2);
    assert!(
        db.store
            .query_audit(&actor(), &all())
            .await
            .unwrap()
            .items
            .iter()
            .any(|event| event.event_id == "intermediate")
    );
    assert_eq!(
        db.store
            .query_audit(&actor(), &exact(&command))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert!(
        db.store
            .retention_preview(&scope(), &policy, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap()
            .audit_observation_candidates
            .is_empty()
    );
    // A longer selected interval wins over the 90-day default. Once the
    // operator selects that default, the intermediate read becomes eligible.
    let default = RetentionPolicy {
        batch_size: 2,
        ..Default::default()
    };
    assert_eq!(
        db.store
            .retention_preview(&scope(), &default, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap()
            .audit_observation_candidates,
        vec!["intermediate"]
    );
    sqlx::query("UPDATE retention_turn SET lane=3 WHERE tenant_id=$1 AND namespace=$2")
        .bind(scope().tenant_id)
        .bind(scope().namespace)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .retain_batch(&scope(), &default, Instant::now() + Duration::from_secs(10))
            .await
            .unwrap()
            .deleted_rows,
        1
    );
    assert_eq!(count(&db.store).await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn competing_actors_have_one_acceptance_and_cancelled_pending_has_none() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    let command = pending(&db.store).await;
    let alice = actor();
    let bob = principal("bob", "bob-key", 253_402_300_799_999);
    let a = request("race-a");
    let b = request("race-b");
    let (first, second) = tokio::join!(
        db.store.decide_verified(&alice, &command, &a),
        db.store.decide_verified(&bob, &command, &b)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert!(matches!(
        first.as_ref().err().or(second.as_ref().err()),
        Some(ContractError::Conflict)
    ));
    let accepted = first.or(second).unwrap();
    let DecisionAttribution::Verified { event } = accepted.attribution else {
        panic!("no verified winner")
    };
    let page = db
        .store
        .query_audit(&actor(), &exact(&command))
        .await
        .unwrap();
    assert_eq!(page.items, vec![*event]);
    assert_eq!(count(&db.store).await, 1);
    db.finish().await;

    let db = TestDb::new().await;
    bind(&db.store).await;
    let command = pending(&db.store).await;
    db.store
        .cancel_workflow(&scope(), &command.workflow_id)
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .decide_verified(&actor(), &command, &request("cancelled"))
            .await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert_eq!(count(&db.store).await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn audit_migration_preserves_prior_schema_bytes_and_rejects_schema_drift() {
    let db = TestDb::without_migrations().await;
    let previous = sqlx::migrate::Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|m| m.version < 20261103020000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    let command = pending(&db.store).await;
    db.store.decide_approval(&command).await.unwrap();
    let before: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT snapshot_bytes,decision_bytes FROM workflow_approvals WHERE workflow_id=$1",
    )
    .bind(&command.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO self_hosted_instance(singleton,instance_id,tenant_id,namespace,access_authority_id) VALUES(true,$1,$2,$3,$4)").bind(config().instance_id).bind(scope().tenant_id).bind(scope().namespace).bind(audience().authority_id).execute(&db.store.pool).await.unwrap();
    assert!(db.store.verify_schema().await.is_err());
    db.store.migrate().await.unwrap();
    bind(&db.store).await;
    db.store.verify_schema().await.unwrap();
    let after: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT snapshot_bytes,decision_bytes FROM workflow_approvals WHERE workflow_id=$1",
    )
    .bind(&command.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(before, after);
    assert!(matches!(
        db.store
            .decide_verified(&actor(), &command, &request("upgraded-replay"))
            .await
            .unwrap()
            .attribution,
        DecisionAttribution::LegacyUnverified
    ));
    assert_eq!(count(&db.store).await, 0);
    sqlx::query(
        "UPDATE _sqlx_migrations SET checksum=decode('00','hex') WHERE version=20261103020000",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert!(db.store.verify_schema().await.is_err());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn changed_observation_identity_cannot_acknowledge_an_unstored_receipt() {
    let db = TestDb::new().await;
    bind(&db.store).await;
    sqlx::query("CREATE FUNCTION audit_test_change_id() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.event_id = 'different'; RETURN NEW; END $$").execute(&db.store.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER change_audit BEFORE INSERT ON operator_audit_events FOR EACH ROW EXECUTE FUNCTION audit_test_change_id()").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store
            .record_sensitive_response(&actor(), &observation("original", "task"))
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(count(&db.store).await, 0);
    db.finish().await;
}
