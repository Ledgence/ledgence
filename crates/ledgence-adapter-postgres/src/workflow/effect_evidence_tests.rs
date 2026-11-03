//! Real transactions for v2 provenance, retained replay and bounded upgrade.
use super::*;
use sqlx::Connection;
fn config() -> SelfHostedInstanceConfig {
    SelfHostedInstanceConfig {
        instance_id: "evidence-test".into(),
        name: "Evidence fixture".into(),
        scope: scope(),
        suggested_queues: vec!["python".into()],
        allowed_origins: vec![],
    }
}
fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "evidence-authority".into(),
        instance_id: config().instance_id,
        scope: scope(),
    }
}
fn worker(subject: &str, credential: &str, expires: u64) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: subject.into(),
        },
        audience(),
        credential.into(),
        PrincipalClass::Worker,
        expires,
        vec![AccessCapability::WorkerExecute],
    )
    .unwrap()
}
fn actor() -> VerifiedPrincipal {
    worker("first-worker", "credential-one", 253_402_300_799_999)
}
async fn bind(store: &PostgresStore) {
    store
        .initialize_instance_with_access(Some(&config()), Some(&audience()))
        .await
        .unwrap();
}
async fn fixture() -> (TestDb, Assignment, EffectClaimV2Command) {
    let db = TestDb::new().await;
    let d = request_with_protection(
        &db.store,
        "evidence",
        "approve-send",
        ApprovalProtection::ExternalEffect,
    )
    .await;
    let (assignment, c) = resume(&db.store, "evidence", &d).await;
    dispatched(&db.store, &assignment).await;
    bind(&db.store).await;
    let c = EffectClaimV2Command {
        owner: c.owner,
        workflow_id: c.workflow_id,
        approval_key: c.approval_key,
        action: c.action,
        lookup: c.lookup,
        claim_id: "claim-one".into(),
    };
    (db, assignment, c)
}
fn confirm(c: &EffectClaimV2Command, id: &str) -> EffectConfirmV2Command {
    EffectConfirmV2Command {
        owner: c.owner.clone(),
        workflow_id: c.workflow_id.clone(),
        approval_key: c.approval_key.clone(),
        lookup: c.lookup.clone(),
        record: LocalStepRecord {
            key: approved_effect_step_key(&c.approval_key),
            callable: c.action.name.clone(),
            input: json!({"v":1,"lookup":c.lookup,"action_json":serde_json::to_string(&c.action).unwrap()}),
            output: json!({"receipt":"r-1","integer":9007199254740993_u64,"nul":"\0","literal":"\\u0000"}),
        },
        claim_id: c.claim_id.clone(),
        observation_id: id.into(),
        reported_callback: EffectReportedCallback::Dispatch,
    }
}
fn observation(c: &EffectClaimV2Command, id: &str) -> EffectObservationCommand {
    EffectObservationCommand {
        occurrence: c.occurrence(),
        claim_id: c.claim_id.clone(),
        observation_id: id.into(),
        reported_callback: EffectReportedCallback::Lookup,
        output: json!({"found":true}),
    }
}
async fn detail(store: &PostgresStore, c: &EffectClaimV2Command) -> EffectEvidenceDetail {
    store
        .inspect_effect(&EffectInspectQuery {
            occurrence: c.occurrence(),
            cursor: None,
            limit: 4,
        })
        .await
        .unwrap()
}
async fn counts(store: &PostgresStore) -> (i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM approved_effect_occurrences),(SELECT count(*) FROM approved_effect_evidence_entries),(SELECT count(*) FROM workflow_local_results)").fetch_one(&store.pool).await.unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_concurrent_claims_preserve_one_grant_and_stable_rotated_source() {
    let (db, _, c) = fixture().await;
    let p = actor();
    let other = EffectClaimV2Command {
        claim_id: "claim-two".into(),
        ..c.clone()
    };
    let (a, b) = tokio::join!(db.store.claim_v2(&p, &c), db.store.claim_v2(&p, &other));
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, EffectClaimV2Receipt::Dispatch { .. }))
            .count(),
        1
    );
    let rotated = worker("first-worker", "rotated", 253_402_300_799_999);
    let EffectClaimV2Receipt::LookupOnly { claim } = db.store.claim_v2(&rotated, &c).await.unwrap()
    else {
        panic!("duplicate grant")
    };
    assert!(claim.already_accepted);
    assert_eq!(claim.worker.credential_id, "credential-one");
    let mut changed = c.clone();
    changed.lookup = "different:lookup".into();
    assert!(matches!(
        db.store.claim_v2(&p, &changed).await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        db.store
            .claim_v2(&worker("other", "other-key", 253_402_300_799_999), &c)
            .await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(counts(&db.store).await, (1, 2, 0));
    let d = detail(&db.store, &c).await;
    assert!(matches!(
        d.summary.dispatch_provenance,
        EffectDispatchProvenance::VerifiedClaim { .. }
    ));
    assert_eq!(d.summary.entry_count, 2);
    assert!(!serde_json::to_string(&d).unwrap().contains("lease_id"));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_journal_race_replays_without_inventing_claim_and_survives_actual_collection() {
    let (db, assigned, c) = fixture().await;
    let p = actor();
    db.store.claim_v2(&p, &c).await.unwrap();
    let done = confirm(&c, "confirm-one");
    let receipt = db.store.confirm_v2(&p, &done).await.unwrap();
    assert!(!receipt.already_accepted);
    let initial = detail(&db.store, &c).await;
    let cache = EffectClaimV2Command {
        claim_id: "never-accepted-claim".into(),
        ..c.clone()
    };
    let replay = db.store.claim_v2(&p, &cache).await.unwrap();
    let EffectClaimV2Receipt::Replay { record, .. } = replay else {
        panic!("expected journal replay")
    };
    assert!(record.matches(&done.record).unwrap());
    assert_eq!(counts(&db.store).await, (1, 2, 1));
    let mut bad = c.clone();
    bad.action.arguments["count"] = json!(1.0);
    assert!(matches!(
        db.store.claim_v2(&p, &bad).await,
        Err(ContractError::Conflict)
    ));
    apply_decision(
        &db.store,
        &assigned,
        1,
        WorkflowAction::Complete {
            output: json!("done"),
        },
        &[],
    )
    .await;
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
    assert!(matches!(
        db.store.workflow_status(&scope(), &c.workflow_id).await,
        Err(ContractError::NotFound)
    ));
    let rotated = worker("first-worker", "rotated", 253_402_300_799_999);
    let duplicate = db.store.confirm_v2(&rotated, &done).await.unwrap();
    assert!(duplicate.already_accepted);
    assert_eq!(duplicate.recorded_at, receipt.recorded_at);
    assert_eq!(duplicate.recorded_revision, receipt.recorded_revision);
    assert_eq!(duplicate.worker, receipt.worker);
    let d = detail(&db.store, &c).await;
    assert_eq!(
        codec::encode(&d.binding).unwrap(),
        codec::encode(&initial.binding).unwrap()
    );
    assert_eq!(d.summary.journal_state, EffectJournalState::Confirmed);
    assert_eq!(counts(&db.store).await, (1, 2, 0));
    let late = db
        .store
        .observe(&rotated, &observation(&c, "after-collection"))
        .await
        .unwrap();
    assert_eq!(late.disposition, EffectObservationDisposition::EvidenceOnly);
    assert_eq!(counts(&db.store).await, (1, 3, 0));
    assert!(detail(&db.store, &c).await.summary.has_discrepancy);
    assert!(db.store.claim_v2(&p, &c).await.is_err());
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_late_observation_is_separate_after_cancel_and_never_accepts_a_journal() {
    let (db, _, c) = fixture().await;
    let p = actor();
    db.store.claim_v2(&p, &c).await.unwrap();
    db.store
        .cancel_workflow(&scope(), &c.workflow_id)
        .await
        .unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=0 WHERE attempt_id=$1")
        .bind(&c.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let o = observation(&c, "late");
    let receipt = db.store.observe(&p, &o).await.unwrap();
    let d = detail(&db.store, &c).await;
    assert_eq!(d.summary.journal_state, EffectJournalState::Unconfirmed);
    assert_eq!(counts(&db.store).await, (1, 2, 0));
    assert!(
        db.store
            .observe(&worker("other", "other-key", 253_402_300_799_999), &o)
            .await
            .is_err()
    );
    let replay = db
        .store
        .observe(&worker("first-worker", "rotated", 253_402_300_799_999), &o)
        .await
        .unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.recorded_at, receipt.recorded_at);
    let mut changed = o.clone();
    changed.output = json!(null);
    assert!(matches!(
        db.store.observe(&p, &changed).await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        db.store.confirm_v2(&p, &confirm(&c, "late")).await,
        Err(ContractError::Conflict)
    ));
    assert!(
        db.store
            .confirm_v2(&p, &confirm(&c, "new-confirm"))
            .await
            .is_err()
    );
    assert_eq!(counts(&db.store).await, (1, 2, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_suppressed_and_mutated_required_writes_rollback_the_original_grant_and_journal() {
    for (table, event, body) in [
        ("approved_effect_occurrences", "INSERT", "RETURN NULL"),
        ("approved_effect_evidence_entries", "INSERT", "RETURN NULL"),
        (
            "approved_effect_evidence_entries",
            "INSERT",
            "NEW.command_id := 'wrong'; RETURN NEW",
        ),
        ("approved_effect_occurrences", "UPDATE", "RETURN NULL"),
    ] {
        let (db, _, c) = fixture().await;
        let p = actor();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE FUNCTION block_effect() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN {body}; END$$"
        )))
        .execute(&db.store.pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE TRIGGER block_effect BEFORE {event} ON {table} FOR EACH ROW EXECUTE FUNCTION block_effect()"))).execute(&db.store.pool).await.unwrap();
        assert!(db.store.claim_v2(&p, &c).await.is_err());
        assert_eq!(counts(&db.store).await, (0, 0, 0));
        let granted: bool = sqlx::query_scalar(
            "SELECT effect_dispatch_claimed FROM workflow_approvals WHERE workflow_id=$1",
        )
        .bind(&c.workflow_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert!(!granted);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TRIGGER block_effect ON {table}"
        )))
        .execute(&db.store.pool)
        .await
        .unwrap();
        assert!(matches!(
            db.store.claim_v2(&p, &c).await.unwrap(),
            EffectClaimV2Receipt::Dispatch { .. }
        ));
        sqlx::query("CREATE TRIGGER block_confirmation BEFORE INSERT ON approved_effect_evidence_entries FOR EACH ROW EXECUTE FUNCTION block_effect()").execute(&db.store.pool).await.unwrap();
        assert!(db.store.confirm_v2(&p, &confirm(&c, "done")).await.is_err());
        assert_eq!(counts(&db.store).await, (1, 1, 0));
        assert_eq!(
            detail(&db.store, &c).await.summary.journal_state,
            EffectJournalState::Unconfirmed
        );
        db.finish().await;
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_expiry_after_workflow_lock_wait_and_foreign_audience_cannot_accept() {
    let (db, _, c) = fixture().await;
    let mut conn = db.store.pool.acquire().await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&c.workflow_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let now = db::now(&mut tx).await.unwrap();
    let expiry = now + 100;
    let p = worker("first-worker", "short", expiry);
    let store = db.store.clone();
    let call = c.clone();
    let pending = tokio::spawn(async move { store.claim_v2(&p, &call).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while db::now(&mut tx).await.unwrap() < expiry {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();
    drop(conn);
    assert!(matches!(
        pending.await.unwrap(),
        Err(ContractError::Unauthenticated)
    ));
    assert_eq!(counts(&db.store).await, (0, 0, 0));
    let mut foreign = audience();
    foreign.instance_id = "different".into();
    let p = VerifiedPrincipal::new(
        actor().identity().clone(),
        foreign,
        "foreign".into(),
        PrincipalClass::Worker,
        253_402_300_799_999,
        vec![AccessCapability::WorkerExecute],
    )
    .unwrap();
    assert!(matches!(
        db.store.claim_v2(&p, &c).await,
        Err(ContractError::Forbidden)
    ));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_pagination_is_scoped_bounded_and_preserves_integer_float_and_nul_discrepancies() {
    let (db, _, c) = fixture().await;
    let p = actor();
    db.store.claim_v2(&p, &c).await.unwrap();
    for n in 0..8 {
        let mut o = observation(&c, &format!("report-{n}"));
        o.output = if n == 0 {
            json!({"value":1,"nul":"\0","literal":"\\u0000"})
        } else {
            json!({"value":1.0,"nul":"\0","literal":"\\u0000"})
        };
        db.store.observe(&p, &o).await.unwrap();
    }
    let mut q = EffectInspectQuery {
        occurrence: c.occurrence(),
        cursor: None,
        limit: 4,
    };
    let mut sequences = Vec::new();
    loop {
        let page = db.store.inspect_effect(&q).await.unwrap();
        assert!(page.entries.len() <= 4);
        assert!(page.summary.has_discrepancy);
        sequences.extend(page.entries.iter().map(EffectEvidenceEntry::sequence));
        if page.next_cursor.is_none() {
            break;
        }
        q.cursor = page.next_cursor;
    }
    assert_eq!(sequences, (1..=9).collect::<Vec<_>>());
    let mut wrong = q;
    wrong.occurrence.approval_key = "wrong".into();
    assert!(matches!(
        db.store.inspect_effect(&wrong).await,
        Err(ContractError::InvalidInput(_))
    ));
    let inbox = EffectInboxQuery {
        scope: scope(),
        filter: EffectInboxFilter::Discrepant,
        workflow_id: Some(c.workflow_id.clone()),
        cursor: None,
        limit: 1,
    };
    assert_eq!(db.store.effect_inbox(&inbox).await.unwrap().items.len(), 1);
    let empty = EffectInboxQuery {
        filter: EffectInboxFilter::Confirmed,
        ..inbox
    };
    assert!(
        db.store
            .effect_inbox(&empty)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_ordinary_execution_never_acquires_a_retained_evidence_table_lock() {
    let db = TestDb::new().await;
    let workflow = start(&db.store).await;
    let assigned = acquire(&db.store, "python").await;
    dispatched(&db.store, &assigned).await;
    let mut conn = db.store.pool.acquire().await.unwrap();
    let mut blocker = conn.begin().await.unwrap();
    sqlx::query("LOCK TABLE approved_effect_occurrences,approved_effect_evidence_entries IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await.unwrap();
    // Any read or write to either ledger would block here. This exercises the
    // actual normal local, status, settlement and empty acquisition operations.
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        db.store
            .record_local_result(&local(&assigned, "ordinary"))
            .await
            .unwrap();
        db.store
            .inspect(&scope(), &assigned.lease.owner.task_id)
            .await
            .unwrap();
        apply_decision(
            &db.store,
            &assigned,
            0,
            WorkflowAction::Complete {
                output: json!("ordinary"),
            },
            &[],
        )
        .await;
        let session = db
            .store
            .open_session(&scope(), "empty-ordinary", 1)
            .await
            .unwrap();
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap();
    })
    .await;
    blocker.rollback().await.unwrap();
    drop(conn);
    result.unwrap();
    assert_eq!(
        db.store
            .workflow_status(&scope(), &workflow.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Succeeded
    );
    assert_eq!(counts(&db.store).await, (0, 0, 1));
    db.finish().await;
}

async fn pre_retained() -> TestDb {
    let db = TestDb::without_migrations().await;
    let previous = Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|m| m.version < crate::migration_effect_retained::VERSION)
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
async fn historical_confirmation(
    store: &PostgresStore,
    queue: &str,
    key: &str,
) -> (EffectClaimV2Command, Vec<u8>) {
    let decision =
        request_with_protection(store, queue, key, ApprovalProtection::ExternalEffect).await;
    let (assigned, c) = resume(store, queue, &decision).await;
    dispatched(store, &assigned).await;
    let c = EffectClaimV2Command {
        owner: c.owner,
        workflow_id: c.workflow_id,
        approval_key: c.approval_key,
        action: c.action,
        lookup: c.lookup,
        claim_id: "not-a-historical-claim".into(),
    };
    let done = confirm(&c, "not-a-historical-observation");
    let bytes = serde_json::to_vec_pretty(&done.record).unwrap();
    sqlx::query("INSERT INTO workflow_local_results(activation_id,step_key,record_bytes,attempt_id,accepted_at_ms,callable) VALUES($1,$2,$3,$4,1,$5)")
        .bind(&c.owner.task_id).bind(&done.record.key).bind(&bytes).bind(&c.owner.attempt_id).bind(&done.record.callable).execute(&store.pool).await.unwrap();
    sqlx::query("UPDATE workflow_approvals SET effect_dispatch_claimed=true,effect_dispatch_attempt_id=$3,effect_evidence_state='confirmed',effect_step_key=$4,effect_lookup=$5,effect_confirmed_attempt_id=$3 WHERE workflow_id=$1 AND wait_key=$2")
        .bind(&c.workflow_id).bind(&c.approval_key).bind(&c.owner.attempt_id).bind(&done.record.key).bind(&c.lookup).execute(&store.pool).await.unwrap();
    (c, bytes)
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_backfill_commits_sixteen_at_a_time_resumes_and_retains_exact_old_bytes() {
    let db = pre_retained().await;
    let mut originals = Vec::new();
    for n in 0..17 {
        originals
            .push(historical_confirmation(&db.store, &format!("old-{n}"), "approve\\u0000").await);
    }
    // Apply only the additive SQL, simulating a stopped deployment before the
    // Rust-side backfill. Startup rejects this incomplete state.
    db.finish_legacy_workflow_seed().await;
    MIGRATOR.run(&db.store.pool).await.unwrap();
    assert!(db.store.verify_schema().await.is_err());
    originals.sort_by(|a, b| a.0.workflow_id.cmp(&b.0.workflow_id));
    let last = &originals.last().unwrap().0.workflow_id;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE FUNCTION interrupt_projection() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.workflow_id='{last}' THEN RETURN NULL; END IF; RETURN NEW; END$$"))).execute(&db.store.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER interrupt_projection BEFORE INSERT ON approved_effect_occurrences FOR EACH ROW EXECUTE FUNCTION interrupt_projection()").execute(&db.store.pool).await.unwrap();
    assert!(db.store.migrate().await.is_err());
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_approvals WHERE effect_retained_pending")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(pending, 1);
    assert_eq!(counts(&db.store).await, (16, 16, 17));
    assert!(db.store.verify_schema().await.is_err());
    assert!(
        matches!(db.store.retain_batch(&scope(),&RetentionPolicy::default(),std::time::Instant::now()+Duration::from_secs(5)).await,Err(ContractError::Unavailable(message)) if message.contains("upgrade is incomplete"))
    );
    assert_eq!(counts(&db.store).await, (16, 16, 17));
    sqlx::query("DROP TRIGGER interrupt_projection ON approved_effect_occurrences")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.migrate().await.unwrap();
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    assert_eq!(counts(&db.store).await, (17, 17, 17));
    for (c, bytes) in originals {
        let page = detail(&db.store, &c).await;
        assert!(matches!(
            page.summary.dispatch_provenance,
            EffectDispatchProvenance::LegacyAttempt { .. }
        ));
        assert!(matches!(
            &page.entries[0],
            EffectEvidenceEntry::Observation {
                source: EffectObservationSource::LegacyJournal {
                    journal_accepted_at: 1,
                    ..
                },
                ..
            }
        ));
        assert!(
            page.entries
                .iter()
                .all(|e| matches!(e, EffectEvidenceEntry::Observation { .. }))
        );
        let retained: Vec<u8> = sqlx::query_scalar(
            "SELECT journal_bytes FROM approved_effect_occurrences WHERE workflow_id=$1",
        )
        .bind(&c.workflow_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
        assert_eq!(retained, bytes);
    }
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_missing_legacy_confirmation_cannot_be_fabricated_by_migration() {
    let db = pre_retained().await;
    let (c, _) = historical_confirmation(&db.store, "lost-proof", "approve-send").await;
    sqlx::query("DELETE FROM workflow_local_results WHERE activation_id=$1")
        .bind(&c.owner.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.finish_legacy_workflow_seed().await;
    let failure = db.store.migrate().await.unwrap_err();
    assert!(
        matches!(failure,ContractError::Unavailable(message) if message.contains("restore pre-retirement backup"))
    );
    assert!(db.store.verify_schema().await.is_err());
    assert_eq!(counts(&db.store).await, (0, 0, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_reconnect_preserves_original_receipt_source_and_observation_id_namespace() {
    let (db, _, c) = fixture().await;
    let p = actor();
    db.store.claim_v2(&p, &c).await.unwrap();
    let done = confirm(&c, "shared-id");
    let original = db.store.confirm_v2(&p, &done).await.unwrap();
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    reopened.verify_schema().await.unwrap();
    bind(&reopened).await;
    let rotated = worker("first-worker", "rotated", 253_402_300_799_999);
    let replay = reopened.confirm_v2(&rotated, &done).await.unwrap();
    assert!(replay.already_accepted);
    assert_eq!(replay.worker, original.worker);
    assert_eq!(replay.recorded_at, original.recorded_at);
    let mut report = observation(&c, "shared-id");
    report.output = done.record.output;
    assert!(matches!(
        reopened.observe(&p, &report).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(counts(&reopened).await, (1, 2, 1));
    reopened.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_detail_holds_one_consistent_projection_while_a_report_waits_to_append() {
    let (db, _, c) = fixture().await;
    let p = actor();
    db.store.claim_v2(&p, &c).await.unwrap();
    let mut conn = db.store.pool.acquire().await.unwrap();
    let mut blocked = conn.begin().await.unwrap();
    sqlx::query("LOCK TABLE approved_effect_evidence_entries IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocked)
        .await
        .unwrap();
    let reading = db.store.clone();
    let q = EffectInspectQuery {
        occurrence: c.occurrence(),
        cursor: None,
        limit: 4,
    };
    let read = tokio::spawn(async move { reading.inspect_effect(&q).await });
    tokio::time::timeout(Duration::from_secs(5),async{loop{let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT entry_bytes FROM approved_effect_evidence_entries%'").fetch_one(&db.store.pool).await.unwrap();if waiting==1{break;}tokio::task::yield_now().await;}}).await.unwrap();
    let writing = db.store.clone();
    let report = observation(&c, "after-reader");
    let write = tokio::spawn(async move { writing.observe(&p, &report).await });
    tokio::time::timeout(Duration::from_secs(5),async{loop{let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'").fetch_one(&db.store.pool).await.unwrap();if waiting>=2{break;}tokio::task::yield_now().await;}}).await.unwrap();
    blocked.commit().await.unwrap();
    drop(conn);
    let page = read.await.unwrap().unwrap();
    assert_eq!(page.summary.entry_count, 1);
    assert_eq!(page.entries.len(), 1);
    assert!(page.next_cursor.is_none());
    write.await.unwrap().unwrap();
    assert_eq!(detail(&db.store, &c).await.summary.entry_count, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_replacement_worker_has_its_own_lookup_claim_and_cannot_report_dispatch() {
    let (db, first, c) = fixture().await;
    let p = actor();
    db.store.claim_v2(&p, &c).await.unwrap();
    owned::failed_attempt(&db.store, &first).await;
    let next = acquire(&db.store, "evidence").await;
    dispatched(&db.store, &next).await;
    let replacement = EffectClaimV2Command {
        owner: next.lease.owner.clone(),
        claim_id: "replacement-claim".into(),
        ..c.clone()
    };
    let second = worker("replacement-worker", "replacement-key", 253_402_300_799_999);
    let EffectClaimV2Receipt::LookupOnly { claim } =
        db.store.claim_v2(&second, &replacement).await.unwrap()
    else {
        panic!("replacement got dispatch")
    };
    assert_eq!(
        claim.initial_permission,
        ApprovedEffectDisposition::LookupOnly
    );
    assert!(!claim.already_accepted);
    let mut invalid = observation(&replacement, "forged-dispatch");
    invalid.reported_callback = EffectReportedCallback::Dispatch;
    assert!(matches!(
        db.store.observe(&second, &invalid).await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        db.store
            .observe(&second, &observation(&c, "wrong-claim"))
            .await,
        Err(ContractError::Conflict)
    ));
    let mut done = confirm(&replacement, "recovered");
    done.reported_callback = EffectReportedCallback::Lookup;
    let receipt = db.store.confirm_v2(&second, &done).await.unwrap();
    assert_eq!(receipt.worker.subject, "replacement-worker");
    let page = detail(&db.store, &c).await;
    assert_eq!(page.summary.entry_count, 3);
    assert_eq!(page.summary.journal_state, EffectJournalState::Confirmed);
    assert_eq!(counts(&db.store).await, (1, 3, 1));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_and_v1_share_one_authoritative_dispatch_bit() {
    let (db, _, c) = fixture().await;
    let p = actor();
    let legacy = c.legacy();
    let (a, b) = tokio::join!(
        db.store.claim_v2(&p, &c),
        db.store.claim_approved_effect(&legacy)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_ne!(
        matches!(a, EffectClaimV2Receipt::Dispatch { .. }),
        b.disposition == ApprovedEffectDisposition::Dispatch
    );
    assert_eq!(counts(&db.store).await, (1, 1, 0));
    let page = detail(&db.store, &c).await;
    if matches!(a, EffectClaimV2Receipt::Dispatch { .. }) {
        assert!(matches!(
            page.summary.dispatch_provenance,
            EffectDispatchProvenance::VerifiedClaim { .. }
        ));
    } else {
        assert!(matches!(
            page.summary.dispatch_provenance,
            EffectDispatchProvenance::LegacyAttempt { .. }
        ));
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn v2_mutated_grant_or_journal_cannot_acknowledge_an_unstored_binding() {
    let (db, _, c) = fixture().await;
    let p = actor();
    sqlx::query("CREATE FUNCTION change_lookup() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.effect_lookup := 'different:lookup'; RETURN NEW; END$$").execute(&db.store.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER change_lookup BEFORE UPDATE OF effect_dispatch_claimed ON workflow_approvals FOR EACH ROW EXECUTE FUNCTION change_lookup()").execute(&db.store.pool).await.unwrap();
    assert!(db.store.claim_v2(&p, &c).await.is_err());
    assert!(db.store.claim_approved_effect(&c.legacy()).await.is_err());
    assert_eq!(counts(&db.store).await, (0, 0, 0));
    let granted: bool = sqlx::query_scalar(
        "SELECT effect_dispatch_claimed FROM workflow_approvals WHERE workflow_id=$1",
    )
    .bind(&c.workflow_id)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert!(!granted);
    sqlx::query("DROP TRIGGER change_lookup ON workflow_approvals")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.claim_v2(&p, &c).await.unwrap();
    sqlx::query("CREATE FUNCTION change_journal() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.record_bytes := convert_to('{}','UTF8'); RETURN NEW; END$$").execute(&db.store.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER change_journal BEFORE INSERT ON workflow_local_results FOR EACH ROW EXECUTE FUNCTION change_journal()").execute(&db.store.pool).await.unwrap();
    assert!(
        db.store
            .confirm_v2(&p, &confirm(&c, "bad-journal"))
            .await
            .is_err()
    );
    assert_eq!(counts(&db.store).await, (1, 1, 0));
    sqlx::query("DROP TRIGGER change_journal ON workflow_local_results")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(
        !db.store
            .confirm_v2(&p, &confirm(&c, "bad-journal"))
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(counts(&db.store).await, (1, 2, 1));
    db.finish().await;
}

#[path = "native_resolution_tests.rs"]
mod native_resolution;
