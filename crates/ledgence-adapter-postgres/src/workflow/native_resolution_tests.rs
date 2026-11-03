//! Real transactions: assessments, dormant native waits and immutable selection.
use super::*;
fn operator(subject: &str, credential: &str, expires: u64) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: subject.into(),
        },
        audience(),
        credential.into(),
        PrincipalClass::Operator,
        expires,
        vec![AccessCapability::EffectResolve],
    )
    .unwrap()
}
fn resolver() -> VerifiedPrincipal {
    operator("reviewer", "reviewer-key", 253_402_300_799_999)
}
async fn resolution(
    store: &PostgresStore,
    c: &EffectClaimV2Command,
    id: &str,
    result: Value,
) -> EffectResolutionCommand {
    let d = detail(store, c).await;
    EffectResolutionCommand {
        occurrence: c.occurrence(),
        resolution_id: id.into(),
        expected_effect_revision: d.summary.effect_revision,
        supersedes_resolution_id: d
            .summary
            .operator_resolution
            .map(|r| r.reference.resolution_id),
        action: c.action.clone(),
        assessment: EffectResolutionAssessment::RecordSuccess { result },
        reason: "Provider receipt examined".into(),
        evidence_references: vec!["receipt:test".into()],
    }
}
fn wait(c: &EffectClaimV2Command, key: &str, after: Option<&str>) -> WorkflowAction {
    WorkflowAction::Wait {
        state: json!({"original":c.occurrence()}),
        continuation: "reviewed".into(),
        commands: vec![],
        wait: WorkflowWait::EffectResolution {
            key: key.into(),
            occurrence: c.occurrence(),
            after_resolution_id: after.map(str::to_owned),
        },
    }
}
async fn context(store: &PostgresStore, a: &Assignment) -> EffectResolutionWake {
    let c = store.activation_context(&a.lease.owner).await.unwrap();
    let Some(WorkflowWake::EffectResolution(wake)) = c.wake else {
        panic!("native wake missing")
    };
    *wake
}
async fn table_counts(store: &PostgresStore) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM approved_effect_evidence_entries WHERE entry_kind='assessment'),(SELECT count(*) FROM operator_audit_events WHERE event_kind='effect_resolved'),(SELECT count(*) FROM workflow_work WHERE kind='effect_wait'),(SELECT count(*) FROM workflow_local_results)").fetch_one(&store.pool).await.unwrap()
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_before_park_is_one_atomic_wake_and_rotation_replays_original_proof() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    let command = resolution(
        &db.store,
        &c,
        "res-1",
        json!({"integer":9007199254740993_u64,"nul":"\0","literal":"\\u0000"}),
    )
    .await;
    let proof = db
        .store
        .resolve_effect(&resolver(), &command)
        .await
        .unwrap();
    proof.validate_for(&resolver(), &command).unwrap();
    let reader = VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: "auditor".into(),
        },
        audience(),
        "reader-key".into(),
        PrincipalClass::Operator,
        253_402_300_799_999,
        vec![AccessCapability::MetadataRead],
    )
    .unwrap();
    let query = AuditQuery {
        scope: scope(),
        filter: AuditFilter::Event {
            event_id: proof.receipt.acceptance_event_id.clone(),
        },
        cursor: None,
        limit: 2,
    };
    let page = OperatorAccess::query_audit(&db.store, &reader, &query)
        .await
        .unwrap();
    page.validate_for(&query, &audience()).unwrap();
    assert_eq!(page.items, vec![proof.acceptance_event.clone()]);
    assert!(page.next_cursor.is_none());

    assert_eq!(table_counts(&db.store).await, (1, 1, 0, 0));
    let rotated = operator("reviewer", "rotated-key", 253_402_300_799_999);
    let replay = db.store.resolve_effect(&rotated, &command).await.unwrap();
    assert!(replay.receipt.already_accepted);
    assert_eq!(replay.acceptance_event, proof.acceptance_event);
    assert!(matches!(
        db.store
            .resolve_effect(
                &operator("different", "other", 253_402_300_799_999),
                &command
            )
            .await,
        Err(ContractError::Conflict)
    ));
    let progress = apply_decision(&db.store, &a, 1, wait(&c, "review", None), &[]).await;
    assert_eq!(progress.activations_scheduled, 1);
    let resumed = acquire(&db.store, "evidence").await;
    let wake = context(&db.store, &resumed).await;
    assert_eq!(wake.occurrence, c.occurrence());
    assert_eq!(wake.parking_activation_id, a.lease.owner.task_id);
    assert_eq!(wake.resumed_activation_id, resumed.lease.owner.task_id);
    assert_eq!((wake.parking_revision, wake.resumed_revision), (1, 2));
    assert!(matches!(
        wake.outcome,
        EffectResolutionOutcome::RecordedSuccess { .. }
    ));
    assert_eq!(
        detail(&db.store, &c).await.summary.journal_state,
        EffectJournalState::Unconfirmed
    );
    assert_eq!(table_counts(&db.store).await, (1, 1, 0, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_and_legacy_equal_keys_are_independent_and_after_park_enqueues_once() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    db.store.send_workflow_event(&WorkflowEventCommand {scope:scope(),workflow_id:c.workflow_id.clone(),key:"review".into(),event:WorkflowEvent::new(json!({"specversion":"1.0","id":"event","source":"urn:test","type":"test","datacontenttype":"application/json","data":{"ready":true}})).unwrap()}).await.unwrap();
    let progress = apply_decision(&db.store, &a, 1, wait(&c, "review", None), &[]).await;
    assert_eq!(progress.activations_scheduled, 0);
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    console_native(&db.store, &c, &a.lease.owner.task_id, None, true).await;
    let command = resolution(&db.store, &c, "res-1", json!(42)).await;
    let p = resolver();
    let (first, second) = tokio::join!(
        db.store.resolve_effect(&p, &command),
        db.store.resolve_effect(&p, &command)
    );
    assert_eq!(
        [
            first.unwrap().receipt.already_accepted,
            second.unwrap().receipt.already_accepted
        ]
        .iter()
        .filter(|v| !**v)
        .count(),
        1
    );
    assert_eq!(table_counts(&db.store).await, (1, 1, 1, 0));
    let work = one_work(&db.store).await;
    assert_eq!(work.source, WorkflowWorkSource::EffectResolutionWait);
    let progress = EffectResolutionStore::apply_effect_work(&db.store, &work, &[])
        .await
        .unwrap();
    assert_eq!(progress.activations_scheduled, 1);
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    let resumed = acquire(&db.store, "evidence").await;
    assert_eq!(context(&db.store, &resumed).await.resolution_id, "res-1");
    console_native(
        &db.store,
        &c,
        &a.lease.owner.task_id,
        Some(&resumed.lease.owner.task_id),
        false,
    )
    .await;
    let progress = apply_decision(
        &db.store,
        &resumed,
        2,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "after-event".into(),
            commands: vec![],
            wait: WorkflowWait::Event {
                key: "review".into(),
                timeout_ms: None,
            },
        },
        &[],
    )
    .await;
    assert_eq!(progress.activations_scheduled, 1);
    let legacy = acquire(&db.store, "evidence").await;
    assert!(matches!(
        db.store
            .activation_context(&legacy.lease.owner)
            .await
            .unwrap()
            .wake,
        Some(WorkflowWake::Event { .. })
    ));
    use ledgence_orchestration_api::console::*;
    let mut cursor = None;
    let mut same = vec![];
    loop {
        let ConsoleQueryReply::Waits(page) = db
            .store
            .query_console(
                &scope(),
                &ConsoleQuery::Waits {
                    workflow_id: c.workflow_id.clone(),
                    page: ConsolePagination { limit: 1, cursor },
                },
            )
            .await
            .unwrap()
        else {
            panic!("waits")
        };
        for wait in page.page.items {
            if wait.wait_key == "review" {
                same.push(wait.kind);
            }
        }
        cursor = page.page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(same.len(), 2);
    assert!(same.contains(&ConsoleWaitKind::Event));
    assert!(same.contains(&ConsoleWaitKind::EffectResolution));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_dispute_is_actionable_without_rewriting_the_selected_wake() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    let first = resolution(&db.store, &c, "res-1", json!({"status":"success"})).await;
    db.store.resolve_effect(&resolver(), &first).await.unwrap();
    let mut report = observation(&c, "late");
    report.output = json!({"status":"different"});
    db.store.observe(&actor(), &report).await.unwrap();
    apply_decision(&db.store, &a, 1, wait(&c, "review", None), &[]).await;
    let resumed = acquire(&db.store, "evidence").await;
    let old = context(&db.store, &resumed).await;
    assert!(matches!(
        old.outcome,
        EffectResolutionOutcome::Disputed { .. }
    ));
    let progress = apply_decision(
        &db.store,
        &resumed,
        2,
        wait(&c, "review:again", Some("res-1")),
        &[],
    )
    .await;
    assert_eq!(progress.activations_scheduled, 0);
    let next = resolution(&db.store, &c, "res-2", report.output.clone()).await;
    db.store
        .resolve_effect(
            &operator("second-reviewer", "second-key", 253_402_300_799_999),
            &next,
        )
        .await
        .unwrap();
    db.store
        .apply_work(&one_work(&db.store).await, &[])
        .await
        .unwrap();
    let after = acquire(&db.store, "evidence").await;
    assert!(matches!(
        context(&db.store, &after).await.outcome,
        EffectResolutionOutcome::RecordedSuccess { .. }
    ));
    let old_bytes: Vec<u8> =
        sqlx::query_scalar("SELECT context_bytes FROM workflow_activations WHERE activation_id=$1")
            .bind(&resumed.lease.owner.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let stored: WorkflowActivationContext = codec::decode(&old_bytes).unwrap();
    assert_eq!(
        stored.wake,
        Some(WorkflowWake::EffectResolution(Box::new(old)))
    );
    let d = detail(&db.store, &c).await;
    assert!(
        d.summary
            .operator_resolution
            .as_ref()
            .unwrap()
            .first_conflicting_entry_sequence
            .is_none()
    );
    assert_eq!(
        d.summary
            .operator_resolution
            .unwrap()
            .reference
            .resolution_id,
        "res-2"
    );
    assert_eq!(d.summary.journal_state, EffectJournalState::Unconfirmed);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_cancellation_closes_wait_but_keeps_assessment_and_never_revives() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    apply_decision(&db.store, &a, 1, wait(&c, "review", None), &[]).await;
    db.store
        .cancel_workflow(&scope(), &c.workflow_id)
        .await
        .unwrap();
    drain_available(&db.store).await;
    let mut command = resolution(&db.store, &c, "closed", Value::Null).await;
    command.assessment = EffectResolutionAssessment::CloseUnknown;
    command.evidence_references.clear();
    db.store
        .resolve_effect(&resolver(), &command)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .workflow_status(&scope(), &c.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelled
    );
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    let selected: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT wake_bytes FROM workflow_effect_waits WHERE workflow_id=$1")
            .bind(&c.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(selected.is_none());
    console_native(&db.store, &c, &a.lease.owner.task_id, None, false).await;
    assert_eq!(table_counts(&db.store).await, (1, 1, 0, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_suppressed_obligation_rolls_back_assessment_audit_and_revision() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    apply_decision(&db.store, &a, 1, wait(&c, "review", None), &[]).await;
    sqlx::raw_sql("CREATE FUNCTION suppress_native_work() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='effect_wait' THEN RETURN NULL; END IF; RETURN NEW; END $$; CREATE TRIGGER suppress_native_work BEFORE INSERT ON workflow_work FOR EACH ROW EXECUTE FUNCTION suppress_native_work();").execute(&db.store.pool).await.unwrap();
    let command = resolution(&db.store, &c, "res-1", json!(true)).await;
    assert!(matches!(
        db.store.resolve_effect(&resolver(), &command).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(table_counts(&db.store).await, (0, 0, 0, 0));
    assert_eq!(
        detail(&db.store, &c).await.summary.effect_revision,
        command.expected_effect_revision
    );
    sqlx::query("DROP TRIGGER suppress_native_work ON workflow_work")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store
        .resolve_effect(&resolver(), &command)
        .await
        .unwrap();
    assert_eq!(table_counts(&db.store).await, (1, 1, 1, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_cas_and_mutated_audit_are_atomic_and_historical_receipts_survive_collection() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    let first = resolution(&db.store, &c, "res-1", json!(1.0)).await;
    let mut competing = first.clone();
    competing.resolution_id = "res-2".into();
    let p = resolver();
    let (one, two) = tokio::join!(
        db.store.resolve_effect(&p, &first),
        db.store.resolve_effect(&p, &competing)
    );
    assert_eq!([&one, &two].iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        [&one, &two]
            .iter()
            .any(|r| matches!(r, Err(ContractError::Conflict)))
    );
    let original = if one.is_ok() { first } else { competing };
    let originalproof = if let Ok(p) = one { p } else { two.unwrap() };
    let mut changed = original.clone();
    changed.assessment = EffectResolutionAssessment::RecordSuccess { result: json!(1) };
    assert!(matches!(
        db.store.resolve_effect(&resolver(), &changed).await,
        Err(ContractError::Conflict)
    ));
    let later = resolution(&db.store, &c, "later", Value::Null).await;
    sqlx::raw_sql("CREATE FUNCTION alter_resolution_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_kind='effect_resolved' THEN NEW.operation_key:='wrong'; END IF; RETURN NEW; END $$; CREATE TRIGGER alter_resolution_event BEFORE INSERT ON operator_audit_events FOR EACH ROW EXECUTE FUNCTION alter_resolution_event();").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.resolve_effect(&resolver(), &later).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(table_counts(&db.store).await, (1, 1, 0, 0));
    sqlx::query("DROP TRIGGER alter_resolution_event ON operator_audit_events")
        .execute(&db.store.pool)
        .await
        .unwrap();
    apply_decision(
        &db.store,
        &a,
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
    let proof = db
        .store
        .resolve_effect(
            &operator("reviewer", "rotated", 253_402_300_799_999),
            &original,
        )
        .await
        .unwrap();
    assert!(proof.receipt.already_accepted);
    assert_eq!(proof.acceptance_event, originalproof.acceptance_event);
    db.store.resolve_effect(&resolver(), &later).await.unwrap();
    assert_eq!(table_counts(&db.store).await, (2, 2, 0, 0));
    assert!(db.store.claim_work(16).await.unwrap().is_empty());
    assert!(matches!(
        db.store
            .resolve_effect(&operator("reviewer", "expired", 1), &original)
            .await,
        Err(ContractError::Unauthenticated)
    ));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_maximum_assessment_pages_are_complete_and_close_unknown_does_not_infer_provider_status()
 {
    let (db, _, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    let mut command = resolution(
        &db.store,
        &c,
        "large",
        json!("x".repeat(EFFECT_RESOLUTION_RESULT_MAX_BYTES - 2)),
    )
    .await;
    command.reason = "r".repeat(EFFECT_RESOLUTION_REASON_MAX_BYTES);
    command.evidence_references = (0..EFFECT_RESOLUTION_MAX_REFERENCES)
        .map(|_| "e".repeat(EFFECT_RESOLUTION_REFERENCE_MAX_BYTES))
        .collect();
    db.store
        .resolve_effect(&resolver(), &command)
        .await
        .unwrap();
    let mut close = resolution(&db.store, &c, "close", Value::Null).await;
    close.assessment = EffectResolutionAssessment::CloseUnknown;
    close.evidence_references.clear();
    db.store.resolve_effect(&resolver(), &close).await.unwrap();
    let mut later = observation(&c, "later");
    later.output = json!({"provider":"failure"});
    db.store.observe(&actor(), &later).await.unwrap();
    let mut cursor = None;
    let mut sequences = vec![];
    loop {
        let query = EffectInspectQuery {
            occurrence: c.occurrence(),
            cursor,
            limit: 1,
        };
        let page = db.store.inspect_effect(&query).await.unwrap();
        page.validate_for(&query).unwrap();
        assert!(codec::encode(&page).unwrap().len() <= EFFECT_EVIDENCE_PAGE_MAX_BYTES);
        assert!(
            page.summary
                .operator_resolution
                .as_ref()
                .unwrap()
                .first_conflicting_entry_sequence
                .is_none()
        );
        assert_eq!(
            page.current_assessment.as_ref().unwrap().command_id(),
            "close"
        );
        sequences.extend(page.entries.iter().map(EffectEvidenceEntry::sequence));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(sequences, vec![1, 2, 3, 4]);
    db.finish().await;
}
async fn console_native(
    store: &PostgresStore,
    c: &EffectClaimV2Command,
    parking: &str,
    resumed: Option<&str>,
    active: bool,
) {
    use ledgence_orchestration_api::console::*;
    let ConsoleQueryReply::Workflow(detail) = store
        .query_console(
            &scope(),
            &ConsoleQuery::Workflow {
                workflow_id: c.workflow_id.clone(),
            },
        )
        .await
        .unwrap()
    else {
        panic!("detail")
    };
    if active {
        assert_eq!(
            detail
                .native_effect_wait
                .as_ref()
                .unwrap()
                .parking_activation_id,
            parking
        );
        assert!(detail.child_wait.is_none());
        assert!(detail.external_wait_key.is_none());
    } else {
        assert!(detail.native_effect_wait.is_none());
    }
    let ConsoleQueryReply::Explorer(graph) = store
        .query_console(
            &scope(),
            &ConsoleQuery::Explorer {
                workflow_id: c.workflow_id.clone(),
                page: ConsolePagination {
                    limit: 100,
                    cursor: None,
                },
            },
        )
        .await
        .unwrap()
    else {
        panic!("graph")
    };
    let node = graph
        .page
        .items
        .iter()
        .find(|n| {
            n.activation_id == parking
                && matches!(n.data, ConsoleExplorerData::NativeEffectWait { .. })
        })
        .unwrap();
    assert_eq!(node.relations.len(), 1 + usize::from(resumed.is_some()));
    assert!(
        node.relations
            .iter()
            .any(|r| r.kind == ConsoleExplorerRelationKind::Registers
                && r.source
                    == ConsoleExplorerReference::Entrypoint {
                        activation_id: parking.into()
                    }
                && r.target
                    == ConsoleExplorerReference::NativeEffectWait {
                        parking_activation_id: parking.into()
                    })
    );
    if let Some(next) = resumed {
        assert!(
            node.relations
                .iter()
                .any(|r| r.kind == ConsoleExplorerRelationKind::Resumes
                    && r.target
                        == ConsoleExplorerReference::Entrypoint {
                            activation_id: next.into()
                        })
        );
    }
    let text = serde_json::to_string(node).unwrap();
    assert!(!text.contains("Provider receipt examined"));
    assert!(!text.contains("evidence_references"));
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_expiry_after_waiting_for_authority_rolls_back_before_acceptance() {
    let (db, _, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    let command = resolution(&db.store, &c, "expires", json!(true)).await;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let expires = now as u64 + 1000;
    let principal = operator("reviewer", "short-lived", expires);
    let mut held = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&c.workflow_id)
        .execute(&mut *held)
        .await
        .unwrap();
    let store = db.store.clone();
    let task = tokio::spawn(async move { store.resolve_effect(&principal, &command).await });
    // Prove the operation is waiting at the DB lock, not just queued in Tokio.
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT state,native_wait_activation_id,%')").fetch_one(&db.store.pool).await.unwrap();if waiting {break;}tokio::time::sleep(Duration::from_millis(5)).await;}
        loop {let now:i64=sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint").fetch_one(&db.store.pool).await.unwrap();if now as u64>=expires {break;}tokio::time::sleep(Duration::from_millis(5)).await;}
    }).await.unwrap();
    held.commit().await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap(),
        Err(ContractError::Unauthenticated)
    ));
    assert_eq!(table_counts(&db.store).await, (0, 0, 0, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn native_failed_activation_insert_preserves_open_wait_and_recovery_obligation() {
    let (db, a, c) = fixture().await;
    db.store.claim_v2(&actor(), &c).await.unwrap();
    apply_decision(&db.store, &a, 1, wait(&c, "review", None), &[]).await;
    let command = resolution(&db.store, &c, "res-1", json!(true)).await;
    db.store
        .resolve_effect(&resolver(), &command)
        .await
        .unwrap();
    let work = one_work(&db.store).await;
    sqlx::raw_sql("CREATE FUNCTION suppress_native_activation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.revision>1 THEN RETURN NULL; END IF; RETURN NEW; END $$; CREATE TRIGGER suppress_native_activation BEFORE INSERT ON workflow_activations FOR EACH ROW EXECUTE FUNCTION suppress_native_activation();").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.apply_work(&work, &[]).await,
        Err(ContractError::Unavailable(_))
    ));
    let facts:(bool,bool,i64)=sqlx::query_as("SELECT (SELECT closed_at_ms IS NULL AND wake_bytes IS NULL FROM workflow_effect_waits WHERE workflow_id=$1),(SELECT processed_at_ms IS NULL FROM workflow_work WHERE id=$2),(SELECT count(*) FROM workflow_activations WHERE workflow_id=$1)").bind(&c.workflow_id).bind(&work.id).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(facts, (true, true, 2));
    sqlx::query("DROP TRIGGER suppress_native_activation ON workflow_activations")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    assert_eq!(
        db.store
            .apply_work(&work, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    db.finish().await;
}
