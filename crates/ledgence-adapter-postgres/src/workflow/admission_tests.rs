//! Durable admission acceptance against isolated PostgreSQL databases.
use super::*;
fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "admission-authority".into(),
        instance_id: "admission-instance".into(),
        scope: scope(),
    }
}
fn principal(subject: &str, class: PrincipalClass) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: subject.into(),
        },
        audience(),
        format!("credential:{subject}"),
        class,
        253402300799999,
        if class == PrincipalClass::Worker {
            vec![AccessCapability::WorkerExecute]
        } else {
            vec![
                AccessCapability::ResourceControl,
                AccessCapability::MetadataRead,
                AccessCapability::SensitiveRead,
            ]
        },
    )
    .unwrap()
}
fn operator() -> VerifiedPrincipal {
    principal("operator", PrincipalClass::Operator)
}
fn worker() -> VerifiedPrincipal {
    principal("worker", PrincipalClass::Worker)
}
fn policy(occupancy: u32) -> ResourcePolicy {
    ResourcePolicy {
        fair_sharing: None,
        enabled: true,
        max_occupancy: occupancy,
        rate: None,
        offer_timeout_ms: 30000,
        start_timeout_ms: 1000,
    }
}
async fn bind(store: &PostgresStore, dispatch: AdmissionDispatchPolicy) {
    store
        .initialize_instance_with_access(
            Some(&SelfHostedInstanceConfig {
                instance_id: audience().instance_id,
                name: "Admission acceptance".into(),
                scope: scope(),
                suggested_queues: vec!["python".into()],
                allowed_origins: vec![],
            }),
            Some(&audience()),
        )
        .await
        .unwrap();
    store
        .configure_admissions(&AdmissionConfig {
            v: 1,
            audience: audience(),
            dispatch,
        })
        .unwrap();
}
async fn put(
    store: &PostgresStore,
    name: &str,
    p: ResourcePolicy,
    revision: Option<u64>,
    request_id: &str,
) -> ResourcePutReceipt {
    store
        .put_resource(
            &operator(),
            &ResourcePutCommand {
                scope: scope(),
                name: name.into(),
                request_id: request_id.into(),
                expected_revision: revision,
                policy: p,
                reason: "Acceptance test policy".into(),
            },
        )
        .await
        .unwrap()
}
async fn fixture(p: ResourcePolicy) -> TestDb {
    let db = TestDb::new().await;
    bind(&db.store, AdmissionDispatchPolicy::Enabled).await;
    put(&db.store, "model", p, None, "create").await;
    db
}
async fn park(store: &PostgresStore, id: &str, resource: &str) -> AdmissionOccurrence {
    let mut submission = command();
    submission.idempotency_key = id.into();
    submission.input.queue = id.into();
    let w = store
        .accept_resolved_workflow(&submission, &descriptor())
        .await
        .unwrap();
    let a = acquire(store, id).await;
    apply_decision(store,&a,0,WorkflowAction::Wait{state:json!({"resume":"admitted"}),continuation:"admitted".into(),commands:vec![],wait:WorkflowWait::ResourceAdmission{key:"call".into(),request:AdmissionWaitRequest::New{resource:resource.into(),operation:AdmissionOperation{callable:"app:model".into(),kind:AdmissionOperationKind::Model,version:"1".into(),arguments:json!({"prompt":"hello","large":9007199254740993_u64,"nul":"\0"})}},timeout_ms:60000}},&[]).await;
    AdmissionOccurrence {
        scope: scope(),
        workflow_id: w.workflow_id,
        origin_activation_id: a.lease.owner.task_id,
        key: "call".into(),
    }
}
async fn advance(store: &PostgresStore) -> AdmissionMaintenanceProgress {
    store
        .advance_admissions(&AdmissionMaintenanceQuery { limit: 16 })
        .await
        .unwrap()
}
async fn claim(
    store: &PostgresStore,
    id: &str,
    o: &AdmissionOccurrence,
) -> (Assignment, AdmissionStartCommand) {
    let a = acquire(store, id).await;
    let context = store.activation_context(&a.lease.owner).await.unwrap();
    let Some(WorkflowWake::ResourceAdmission(wake)) = context.wake else {
        panic!("missing admission wake")
    };
    assert_eq!(&wake.occurrence, o);
    let AdmissionSelection::Offered { offer } = wake.selection else {
        panic!("missing offer")
    };
    dispatched(store, &a).await;
    let c = AdmissionStartCommand {
        owner: a.lease.owner.clone(),
        occurrence: o.clone(),
        offer_generation: offer.generation,
        start_id: format!("start:{id}"),
        finish_id: format!("finish:{id}"),
    };
    (a, c)
}
async fn start_call(
    store: &PostgresStore,
    id: &str,
) -> (Assignment, AdmissionStartCommand, AdmissionSource) {
    let o = park(store, id, "model").await;
    assert_eq!(advance(store).await.offered, 1);
    let (a, c) = claim(store, id, &o).await;
    let AdmissionStartReceipt::Granted { source } =
        store.start_admission(&worker(), &c).await.unwrap()
    else {
        panic!("not granted")
    };
    (a, c, *source)
}
async fn resource(store: &PostgresStore) -> ResourceSnapshot {
    store
        .resource(
            &operator(),
            &ResourceQuery {
                scope: scope(),
                name: "model".into(),
            },
        )
        .await
        .unwrap()
}
async fn inspect(store: &PostgresStore, o: &AdmissionOccurrence) -> AdmissionSnapshot {
    store
        .inspect_admission(
            &operator(),
            &AdmissionQuery {
                occurrence: o.clone(),
            },
        )
        .await
        .unwrap()
}
fn finish(source: &AdmissionSource) -> AdmissionFinishCommand {
    AdmissionFinishCommand {
        usage: None,
        source: source.clone(),
        output: json!({"answer":"done","large":9007199254740993_u64,"nul":"\0"}),
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_one_grant_finish_replay_and_result_preload() {
    let db = fixture(policy(1)).await;
    let o = park(&db.store, "one", "model").await;
    assert_eq!(advance(&db.store).await.offered, 1);
    let (a, c) = claim(&db.store, "one", &o).await;
    let p = worker();
    let (x, y) = tokio::join!(
        db.store.start_admission(&p, &c),
        db.store.start_admission(&p, &c)
    );
    let replies = [x.unwrap(), y.unwrap()];
    assert_eq!(
        replies
            .iter()
            .filter(|r| matches!(r, AdmissionStartReceipt::Granted { .. }))
            .count(),
        1
    );
    let source = replies
        .into_iter()
        .find_map(|r| {
            if let AdmissionStartReceipt::Granted { source } = r {
                Some(*source)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(resource(&db.store).await.occupied, 1);
    let c = finish(&source);
    let (x, y) = tokio::join!(
        db.store.finish_admission(&p, &c),
        db.store.finish_admission(&p, &c)
    );
    let x = x.unwrap();
    let y = y.unwrap();
    assert_ne!(x.already_accepted, y.already_accepted);
    assert_eq!(resource(&db.store).await.occupied, 0);
    let context = db.store.activation_context(&a.lease.owner).await.unwrap();
    assert_eq!(context.admission_result.unwrap().output, c.output);
    assert!(
        matches!(context.wake,Some(WorkflowWake::ResourceAdmission(w)) if matches!(w.selection,AdmissionSelection::Offered{..}))
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_local_results")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_explorer_records WHERE kind='admission_operation'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
    let mut changed = c;
    changed.output = json!({"different":true});
    assert!(matches!(
        db.store.finish_admission(&worker(), &changed).await,
        Err(ContractError::Conflict)
    ));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_fifo_last_slot_and_independent_resources() {
    let db = fixture(policy(1)).await;
    put(&db.store, "other", policy(1), None, "other").await;
    let first = park(&db.store, "first", "model").await;
    let second = park(&db.store, "second", "model").await;
    let other = park(&db.store, "other", "other").await;
    let (a, b) = tokio::join!(advance(&db.store), advance(&db.store));
    assert_eq!(a.offered + b.offered, 2);
    assert_eq!(
        inspect(&db.store, &first).await.state,
        AdmissionState::Offered
    );
    assert_eq!(
        inspect(&db.store, &second).await.state,
        AdmissionState::Waiting
    );
    assert_eq!(
        inspect(&db.store, &other).await.state,
        AdmissionState::Offered
    );
    let (_, c) = claim(&db.store, "first", &first).await;
    let AdmissionStartReceipt::Granted { source } =
        db.store.start_admission(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert_eq!(advance(&db.store).await.offered, 1);
    assert_eq!(
        inspect(&db.store, &second).await.state,
        AdmissionState::Offered
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_offer_expiry_requires_explicit_rewait() {
    let mut p = policy(1);
    p.offer_timeout_ms = 1000;
    let db = fixture(p).await;
    let o = park(&db.store, "expired", "model").await;
    advance(&db.store).await;
    let (a, c) = claim(&db.store, "expired", &o).await;
    tokio::time::sleep(Duration::from_millis(1050)).await;
    let p = advance(&db.store).await;
    assert_eq!(p.expired, 1);
    assert_eq!(p.resumed, 0);
    assert_eq!(
        inspect(&db.store, &o).await.state,
        AdmissionState::AwaitingRewait
    );
    assert_eq!(resource(&db.store).await.occupied, 0);
    assert!(
        matches!(db.store.start_admission(&worker(),&c).await.unwrap(),AdmissionStartReceipt::Existing{snapshot} if snapshot.state==AdmissionState::AwaitingRewait)
    );
    apply_decision(
        &db.store,
        &a,
        1,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "admitted".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "again".into(),
                request: AdmissionWaitRequest::Existing {
                    occurrence: o.clone(),
                },
                timeout_ms: 60000,
            },
        },
        &[],
    )
    .await;
    assert_eq!(advance(&db.store).await.offered, 1);
    let s = inspect(&db.store, &o).await;
    assert_eq!(s.offer.unwrap().generation, 2);
    assert_eq!(s.occurrence, o);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_policy_epoch_invalidates_offers_preserves_started_debt_and_audit() {
    let db = fixture(policy(2)).await;
    let (_a, _c, source) = start_call(&db.store, "started").await;
    let o = park(&db.store, "offered", "model").await;
    advance(&db.store).await;
    let (_, c) = claim(&db.store, "offered", &o).await;
    let updated = put(&db.store, "model", policy(1), Some(1), "reduce").await;
    assert_eq!(updated.resource.revision, 2);
    assert_eq!(resource(&db.store).await.occupied, 1);
    assert_eq!(resource(&db.store).await.offered, 0);
    assert!(
        matches!(db.store.start_admission(&worker(),&c).await.unwrap(),AdmissionStartReceipt::Existing{snapshot} if snapshot.state==AdmissionState::AwaitingRewait)
    );
    let replay = put(&db.store, "model", policy(1), Some(1), "reduce").await;
    assert!(replay.already_accepted);
    assert!(
        db.store
            .put_resource(
                &operator(),
                &ResourcePutCommand {
                    scope: scope(),
                    name: "model".into(),
                    request_id: "reduce".into(),
                    expected_revision: Some(1),
                    policy: policy(3),
                    reason: "changed".into()
                }
            )
            .await
            .is_err()
    );
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert_eq!(resource(&db.store).await.occupied, 0);
    let audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM operator_audit_events WHERE event_kind='resource_policy_accepted'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(audits, 2);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_lost_owner_retains_occupancy_and_late_finish_settles() {
    let db = fixture(policy(1)).await;
    let (_a, c, source) = start_call(&db.store, "lost").await;
    sqlx::query("UPDATE attempts SET expires_at_ms=1 WHERE attempt_id=$1")
        .bind(&c.owner.attempt_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE admission_requests SET check_at_ms=1 WHERE workflow_id=$1")
        .bind(&c.occurrence.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    advance(&db.store).await;
    assert_eq!(
        inspect(&db.store, &c.occurrence).await.state,
        AdmissionState::UnknownOccupied
    );
    assert_eq!(resource(&db.store).await.occupied, 1);
    assert_eq!(resource(&db.store).await.unknown, 1);
    let c = finish(&source);
    assert!(matches!(
        db.store
            .finish_admission(&principal("foreign", PrincipalClass::Worker), &c)
            .await,
        Err(ContractError::Forbidden)
    ));
    assert!(
        !db.store
            .finish_admission(&worker(), &c)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(resource(&db.store).await.occupied, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_operator_release_late_finish_does_not_release_twice() {
    let db = fixture(policy(1)).await;
    let (_a, c, source) = start_call(&db.store, "release").await;
    let snapshot = inspect(&db.store, &c.occurrence).await;
    let release = AdmissionReleaseCommand {
        occurrence: c.occurrence.clone(),
        source_id: source.source_id.clone(),
        expected_revision: snapshot.revision,
        request_id: "manual".into(),
        reason: "Operator verified callback stopped".into(),
    };
    assert!(
        !db.store
            .release_admission(&operator(), &release)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(resource(&db.store).await.occupied, 0);
    let _other = park(&db.store, "next", "model").await;
    assert_eq!(advance(&db.store).await.offered, 1);
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert_eq!(resource(&db.store).await.occupied, 1);
    let snapshot = inspect(&db.store, &c.occurrence).await;
    assert_eq!(snapshot.state, AdmissionState::AdministrativelyReleased);
    assert!(snapshot.finish.is_some());
    assert!(
        db.store
            .release_admission(&operator(), &release)
            .await
            .unwrap()
            .already_accepted
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_quarantine_blocks_old_and_absent_start_but_accepts_late_finish() {
    let db = fixture(policy(1)).await;
    let (_a, c, source) = start_call(&db.store, "restore").await;
    let restored = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&restored, AdmissionDispatchPolicy::Quarantined).await;
    assert!(matches!(
        restored.start_admission(&worker(), &c).await,
        Err(ContractError::Unavailable(_))
    ));
    let mut absent = c;
    absent.occurrence.key = "absent".into();
    assert!(matches!(
        restored.start_admission(&worker(), &absent).await,
        Err(ContractError::Unavailable(_))
    ));
    restored
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert_eq!(resource(&restored).await.occupied, 0);
    assert!(resource(&restored).await.quarantined);
    let o = park(&restored, "parked", "model").await;
    assert_eq!(advance(&restored).await.offered, 0);
    assert_eq!(inspect(&restored, &o).await.state, AdmissionState::Waiting);
    let (ready,cleanup):(i64,i64)=sqlx::query_as("SELECT next_due_ms,quarantined_due_ms FROM admission_resources WHERE resource_name='model'").fetch_one(&db.store.pool).await.unwrap();
    assert!(
        ready < cleanup,
        "quarantine must retain ready scheduling separately from cleanup"
    );
    assert_eq!(
        advance(&restored).await.examined,
        0,
        "quarantine must not poll a ready waiter repeatedly"
    );
    restored.close().await;
    let enabled = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&enabled, AdmissionDispatchPolicy::Enabled).await;
    assert_eq!(
        advance(&enabled).await.offered,
        1,
        "reenabling must not wait until admission deadline"
    );
    enabled.close().await;
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_unknown_rewait_finish_wakes_current_episode_once() {
    let db = fixture(policy(1)).await;
    let (a, c, source) = start_call(&db.store, "rewait").await;
    apply_decision(
        &db.store,
        &a,
        1,
        WorkflowAction::Wait {
            state: json!({"saved":true}),
            continuation: "result".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "resolve".into(),
                request: AdmissionWaitRequest::Existing {
                    occurrence: c.occurrence.clone(),
                },
                timeout_ms: 60000,
            },
        },
        &[],
    )
    .await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &c.occurrence.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Waiting
    );
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    drain_available(&db.store).await;
    let next = acquire(&db.store, "rewait").await;
    let context = db
        .store
        .activation_context(&next.lease.owner)
        .await
        .unwrap();
    assert_eq!(context.continuation, "result");
    assert!(
        matches!(context.wake,Some(WorkflowWake::ResourceAdmission(w)) if matches!(w.selection,AdmissionSelection::Finished{..})&&w.waiting_activation_id==a.lease.owner.task_id)
    );
    assert!(context.admission_result.is_some());
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_cancel_releases_unstarted_but_retains_started() {
    let db = fixture(policy(2)).await;
    let (_a, c, source) = start_call(&db.store, "cancel-started").await;
    let pending = park(&db.store, "cancel-offer", "model").await;
    advance(&db.store).await;
    db.store
        .cancel_workflow(&scope(), &pending.workflow_id)
        .await
        .unwrap();
    db.store
        .cancel_workflow(&scope(), &c.occurrence.workflow_id)
        .await
        .unwrap();
    drain_available(&db.store).await;
    advance(&db.store).await;
    assert_eq!(resource(&db.store).await.occupied, 1);
    assert_eq!(resource(&db.store).await.unknown, 1);
    let before = db
        .store
        .workflow_status(&scope(), &c.occurrence.workflow_id)
        .await
        .unwrap();
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    drain_available(&db.store).await;
    let after = db
        .store
        .workflow_status(&scope(), &c.occurrence.workflow_id)
        .await
        .unwrap();
    assert_eq!(before.activation_id, after.activation_id);
    assert!(!matches!(
        after.state,
        WorkflowState::Running | WorkflowState::Waiting
    ));
    assert_eq!(resource(&db.store).await.occupied, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_rate_uses_shadow_credit_only_spends_on_start() {
    let mut p = policy(3);
    p.rate = Some(ResourceRate {
        numerator: 1,
        period_ms: 60000,
        burst: 1,
    });
    let db = fixture(p).await;
    let o = park(&db.store, "rate-one", "model").await;
    park(&db.store, "rate-two", "model").await;
    assert_eq!(advance(&db.store).await.offered, 1);
    let credit: i64 =
        sqlx::query_scalar("SELECT credit FROM admission_resources WHERE resource_name='model'")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(credit, 60000);
    let (_, c) = claim(&db.store, "rate-one", &o).await;
    let AdmissionStartReceipt::Granted { source } =
        db.store.start_admission(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert_eq!(advance(&db.store).await.offered, 0);
    let credit: i64 =
        sqlx::query_scalar("SELECT credit FROM admission_resources WHERE resource_name='model'")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(credit < 60000);
    let due: Option<i64> = sqlx::query_scalar(
        "SELECT next_due_ms FROM admission_resources WHERE resource_name='model'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert!(due.is_some());
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_retains_source_after_execution_retention_and_restart() {
    let db = fixture(policy(1)).await;
    let (a, c, source) = start_call(&db.store, "retained").await;
    apply_decision(
        &db.store,
        &a,
        1,
        WorkflowAction::Complete {
            output: json!({"user_completed":true}),
        },
        &[],
    )
    .await;
    assert_eq!(resource(&db.store).await.unknown, 1);
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let mut remaining = 1;
    for _ in 0..100 {
        db.store
            .retain_batch(
                &scope(),
                &RetentionPolicy {
                    retain_for_ms: MIN_RETENTION_MS,
                    batch_size: 64,
                },
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
        remaining = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_runs")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        if remaining == 0 {
            break;
        }
    }
    assert_eq!(remaining, 0);
    let restarted = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&restarted, AdmissionDispatchPolicy::Enabled).await;
    assert_eq!(resource(&restarted).await.occupied, 1);
    let receipt = restarted
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert!(!receipt.already_accepted);
    assert_eq!(resource(&restarted).await.occupied, 0);
    assert_eq!(
        inspect(&restarted, &c.occurrence)
            .await
            .finish
            .unwrap()
            .source,
        source
    );
    assert!(
        restarted
            .finish_admission(&worker(), &finish(&source))
            .await
            .unwrap()
            .already_accepted
    );
    restarted.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_upgrade_preserves_existing_waits_and_defaults_disabled() {
    let db = TestDb::without_migrations().await;
    let prior = sqlx::migrate::Migrator::with_migrations(
        crate::MIGRATOR
            .iter()
            .filter(|m| m.version < 20261103070000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(crate::MigrationOptions::default(), &prior)
        .await
        .unwrap();
    db.begin_legacy_task_seed().await;
    // Only bridge the new default projection for fixture seeding; remove it
    // before migration so the candidate receives the actual prior schema.
    sqlx::raw_sql("ALTER TABLE workflow_runs ADD COLUMN has_admissions boolean NOT NULL DEFAULT false, ADD COLUMN admission_wait_activation_id text COLLATE \"C\"").execute(&db.store.pool).await.unwrap();
    let w = start(&db.store).await;
    let a = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &a,
        0,
        WorkflowAction::Wait {
            state: json!({"preserved":true}),
            continuation: "after_timer".into(),
            commands: vec![],
            wait: WorkflowWait::Timer {
                key: "existing".into(),
                delay_ms: 60000,
            },
        },
        &[],
    )
    .await;
    db.finish_legacy_task_seed().await;
    sqlx::raw_sql("ALTER TABLE workflow_runs DROP COLUMN has_admissions,DROP COLUMN admission_wait_activation_id").execute(&db.store.pool).await.unwrap();
    db.store.migrate().await.unwrap();
    let after = db
        .store
        .workflow_status(&scope(), &w.workflow_id)
        .await
        .unwrap();
    assert_eq!(after.state, WorkflowState::Waiting);
    assert_eq!(after.revision, 1);
    assert!(matches!(
        WorkflowStore::admissions(&db.store),
        Err(ContractError::Unavailable(_))
    ));
    let has: bool =
        sqlx::query_scalar("SELECT has_admissions FROM workflow_runs WHERE workflow_id=$1")
            .bind(&w.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(!has);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_ordinary_execution_has_no_admission_table_access() {
    let db = TestDb::new().await;
    sqlx::raw_sql("DROP TABLE run_budget_fork_refusals,run_budget_entries,run_budget_members,run_budgets; DROP TABLE admission_offer_decisions; DROP TABLE task_admission_requests; DROP TABLE workflow_admission_waits; DROP TABLE provider_admission_links; DROP TABLE admission_requests; DROP TABLE admission_operator_receipts; DROP TABLE admission_resources").execute(&db.store.pool).await.unwrap();
    let w = start(&db.store).await;
    let a = acquire(&db.store, "python").await;
    let context = db.store.activation_context(&a.lease.owner).await.unwrap();
    assert!(context.admission_result.is_none());
    assert!(context.provider_admission.is_none());
    dispatched(&db.store, &a).await;
    db.store
        .record_local_result(&local(&a, "ordinary"))
        .await
        .unwrap();
    apply_decision(
        &db.store,
        &a,
        0,
        WorkflowAction::Complete {
            output: json!({"done":true}),
        },
        &[],
    )
    .await;
    assert_eq!(
        db.store
            .workflow_status(&scope(), &w.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Succeeded
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_unknown_wait_timeout_retains_occupancy_without_resume_loop() {
    let db = fixture(policy(1)).await;
    let (a, c, source) = start_call(&db.store, "wait-timeout").await;
    // Expiry limits new Start authority, not the time allowed to wait for facts.
    let mut expired = inspect(&db.store, &c.occurrence).await;
    expired.deadline_at = source.accepted_at + 1;
    expired.validate().unwrap();
    sqlx::query(
        "UPDATE admission_requests SET deadline_at_ms=$2,snapshot_bytes=$3 WHERE workflow_id=$1",
    )
    .bind(&c.occurrence.workflow_id)
    .bind(expired.deadline_at as i64)
    .bind(codec::encode(&expired).unwrap())
    .execute(&db.store.pool)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert!(matches!(
        db.store.start_admission(&worker(), &c).await.unwrap(),
        AdmissionStartReceipt::Existing { .. }
    ));
    apply_decision(
        &db.store,
        &a,
        1,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "review".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "resolve".into(),
                request: AdmissionWaitRequest::Existing {
                    occurrence: c.occurrence.clone(),
                },
                timeout_ms: 100,
            },
        },
        &[],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    drain_available(&db.store).await;
    let next = acquire(&db.store, "wait-timeout").await;
    let context = db
        .store
        .activation_context(&next.lease.owner)
        .await
        .unwrap();
    assert!(
        matches!(context.wake,Some(WorkflowWake::ResourceAdmission(w)) if matches!(w.selection,AdmissionSelection::NeedsReview{..}))
    );
    assert_eq!(resource(&db.store).await.occupied, 1);
    assert_eq!(advance(&db.store).await.resumed, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_maintenance_is_bounded_and_workflow_locks_do_not_block_other_resources() {
    let mut p = policy(20);
    p.offer_timeout_ms = 300000;
    let db = fixture(p).await;
    put(&db.store, "other", policy(1), None, "other").await;
    let first = park(&db.store, "blocked", "model").await;
    let other = park(&db.store, "free", "other").await;
    let mut tx = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&first.workflow_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let q = AdmissionMaintenanceQuery { limit: 1 };
    let first = tokio::time::timeout(Duration::from_secs(1), db.store.advance_admissions(&q))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.offered, 0);
    let progress = tokio::time::timeout(Duration::from_secs(1), db.store.advance_admissions(&q))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        progress.offered, 1,
        "a locked first page must not starve the next resource"
    );
    assert_eq!(
        inspect(&db.store, &other).await.state,
        AdmissionState::Offered
    );
    tx.rollback().await.unwrap();
    for i in 0..5 {
        park(&db.store, &format!("batch-{i}"), "model").await;
    }
    let p = db
        .store
        .advance_admissions(&AdmissionMaintenanceQuery { limit: 3 })
        .await
        .unwrap();
    assert_eq!(p.examined, 3);
    assert_eq!(p.offered, 3);
    let waiting: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM admission_requests WHERE resource_name='model' AND state='waiting'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(waiting, 3);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_clock_rollback_defers_offers_and_start_but_accepts_late_facts() {
    let db = fixture(policy(3)).await;
    let (_a, _c, source) = start_call(&db.store, "clock-started").await;
    let offered = park(&db.store, "clock-offered", "model").await;
    advance(&db.store).await;
    let (_a, c) = claim(&db.store, "clock-offered", &offered).await;
    let waiting = park(&db.store, "clock-waiting", "model").await;
    let future: u64 = sqlx::query_scalar::<_, i64>(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint+45000",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap() as u64;
    let mut snapshot = inspect(&db.store, &waiting).await;
    snapshot.updated_at = future;
    snapshot.validate().unwrap();
    sqlx::query(
        "UPDATE admission_requests SET updated_at_ms=$2,snapshot_bytes=$3 WHERE workflow_id=$1",
    )
    .bind(&waiting.workflow_id)
    .bind(future as i64)
    .bind(codec::encode(&snapshot).unwrap())
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE admission_resources SET high_water_at_ms=$1,next_due_ms=1 WHERE resource_name='model'").bind(future as i64).execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.start_admission(&worker(), &c).await,
        Err(ContractError::Unavailable(_))
    ));
    let progress = advance(&db.store).await;
    assert_eq!(progress.offered, 0);
    assert_eq!(progress.resumed, 0);
    let due: i64 = sqlx::query_scalar(
        "SELECT next_due_ms FROM admission_resources WHERE resource_name='model'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(due, future as i64);
    // A request's observed time can lead the bucket clock after historical
    // invalidation. It independently fences selection, not just bucket time.
    sqlx::query("UPDATE admission_resources SET high_water_at_ms=1,next_due_ms=1 WHERE resource_name='model'").execute(&db.store.pool).await.unwrap();
    assert_eq!(advance(&db.store).await.offered, 0);
    let due: i64 = sqlx::query_scalar(
        "SELECT next_due_ms FROM admission_resources WHERE resource_name='model'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(due, future as i64);
    db.store
        .finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    assert_eq!(resource(&db.store).await.occupied, 1);
    assert_eq!(resource(&db.store).await.offered, 1);
    assert_eq!(advance(&db.store).await.offered, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn admission_enabling_rate_during_clock_rollback_preserves_watermark_and_no_credit() {
    let db = fixture(policy(2)).await;
    let future: i64 = sqlx::query_scalar(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint+10000",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE admission_resources SET high_water_at_ms=$1 WHERE resource_name='model'")
        .bind(future)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let mut p = policy(2);
    p.rate = Some(ResourceRate {
        numerator: 1,
        period_ms: 1000,
        burst: 2,
    });
    put(&db.store, "model", p, Some(1), "enable-rate").await;
    let (high_water, credit): (i64, i64) = sqlx::query_as(
        "SELECT high_water_at_ms,credit FROM admission_resources WHERE resource_name='model'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(high_water, future);
    assert_eq!(credit, 0);
    db.finish().await;
}

#[path = "fair_admission_tests.rs"]
mod fair;

#[path = "task_admission_tests.rs"]
mod standalone;

#[path = "run_budget_tests.rs"]
mod run_budgets;
