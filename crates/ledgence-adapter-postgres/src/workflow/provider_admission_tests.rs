//! Atomic composition, race and recovery tests on real isolated PostgreSQL.
use super::providers::{approved, audience, binding, worker};
use super::*;

fn operator() -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: "capacity-operator".into(),
        },
        audience(),
        "capacity-operator-key".into(),
        PrincipalClass::Operator,
        253402300799999,
        vec![
            AccessCapability::ResourceControl,
            AccessCapability::MetadataRead,
            AccessCapability::SensitiveRead,
        ],
    )
    .unwrap()
}
fn governed() -> ProviderBindingRequest {
    let mut binding = binding();
    binding.v = 2;
    binding.admission = Some(ProviderAdmissionRequirement {
        resource: "provider".into(),
    });
    binding
}
async fn fixture() -> TestDb {
    let db = TestDb::new().await;
    providers::bind(&db.store, ProviderDispatchPolicy::Enabled).await;
    db.store
        .configure_admissions(&AdmissionConfig {
            v: 1,
            audience: audience(),
            dispatch: AdmissionDispatchPolicy::Enabled,
        })
        .unwrap();
    db.store
        .put_resource(
            &operator(),
            &ResourcePutCommand {
                scope: scope(),
                name: "provider".into(),
                request_id: "policy".into(),
                expected_revision: None,
                policy: ResourcePolicy {
                    enabled: true,
                    max_occupancy: 4,
                    rate: Some(ResourceRate {
                        numerator: 1,
                        period_ms: 60_000,
                        burst: 4,
                    }),
                    offer_timeout_ms: 30_000,
                    start_timeout_ms: 1000,
                },
                reason: "test admission".into(),
            },
        )
        .await
        .unwrap();
    db
}
async fn park(
    store: &PostgresStore,
    id: &str,
    binding: ProviderBindingRequest,
) -> (ProviderClaimCommand, AdmissionOccurrence) {
    let (a, c) = approved(store, id, binding).await;
    assert!(store.claim_provider(&worker(), &c).await.is_err());
    apply_decision(
        store,
        &a,
        1,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "admitted".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "dispatch".into(),
                request: AdmissionWaitRequest::Provider {
                    approval_key: c.approval_key.clone(),
                },
                timeout_ms: 60000,
            },
        },
        &[],
    )
    .await;
    let occurrence = AdmissionOccurrence {
        scope: scope(),
        workflow_id: c.workflow_id.clone(),
        origin_activation_id: c.owner.task_id.clone(),
        key: "dispatch".into(),
    };
    (c, occurrence)
}
async fn offer(
    store: &PostgresStore,
    id: &str,
    o: &AdmissionOccurrence,
) -> (Assignment, ProviderAdmissionStartCommand) {
    store
        .advance_admissions(&AdmissionMaintenanceQuery { limit: 16 })
        .await
        .unwrap();
    let a = acquire(store, id).await;
    let ctx = store.activation_context(&a.lease.owner).await.unwrap();
    let Some(WorkflowWake::ResourceAdmission(wake)) = ctx.wake else {
        panic!("resource wake missing")
    };
    assert_eq!(&wake.occurrence, o);
    assert_eq!(
        wake.binding
            .provider()
            .unwrap()
            .ticket
            .occurrence
            .activation_id,
        o.origin_activation_id
    );
    let AdmissionSelection::Offered { offer } = wake.selection else {
        panic!("offer missing")
    };
    dispatched(store, &a).await;
    (
        a.clone(),
        ProviderAdmissionStartCommand {
            admission: AdmissionStartCommand {
                owner: a.lease.owner,
                occurrence: o.clone(),
                offer_generation: offer.generation,
                start_id: format!("start:{id}"),
                finish_id: format!("report:{id}"),
            },
        },
    )
}
async fn start(
    store: &PostgresStore,
    id: &str,
) -> (
    ProviderClaimCommand,
    Assignment,
    ProviderAdmissionStartCommand,
    ProviderAdmissionSource,
) {
    let (c, o) = park(store, id, governed()).await;
    let (a, start) = offer(store, id, &o).await;
    let ProviderAdmissionStartReceipt::Granted { source } = store
        .start_provider_admission(&worker(), &start)
        .await
        .unwrap()
    else {
        panic!("not granted")
    };
    (c, a, start, *source)
}
fn report(
    c: &ProviderClaimCommand,
    source: &ProviderAdmissionSource,
    completion: ProviderCallCompletion,
    knowledge: ProviderKnowledge,
) -> ProviderAdmissionReportCommand {
    ProviderAdmissionReportCommand {
        source: source.clone(),
        outcome: ProviderDispatchOutcome {
            call_completion: completion,
            report: providers::reported(c, &source.provider, knowledge).report,
        },
    }
}
async fn counters(store: &PostgresStore) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT offered,started,unknown FROM admission_resources WHERE resource_name='provider'",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap()
}
async fn snapshot(store: &PostgresStore, o: &AdmissionOccurrence) -> AdmissionSnapshot {
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
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_atomic_start_race_preserves_original_approval_and_preloads() {
    let db = fixture().await;
    let (c, o) = park(&db.store, "race", governed()).await;
    let original = db
        .store
        .approval(&scope(), &c.workflow_id, &c.approval_key)
        .await
        .unwrap();
    let (a, start) = offer(&db.store, "race", &o).await;
    assert_ne!(a.lease.owner.task_id, o.origin_activation_id);
    let p = worker();
    let (one, two) = tokio::join!(
        db.store.start_provider_admission(&p, &start),
        db.store.start_provider_admission(&p, &start)
    );
    let receipts = [one.unwrap(), two.unwrap()];
    assert_eq!(
        receipts
            .iter()
            .filter(|v| matches!(v, ProviderAdmissionStartReceipt::Granted { .. }))
            .count(),
        1
    );
    assert_eq!(counters(&db.store).await, (0, 1, 0));
    let source = receipts
        .iter()
        .find_map(|v| match v {
            ProviderAdmissionStartReceipt::Granted { source } => Some(source),
            _ => None,
        })
        .unwrap();
    assert_eq!(source.provider.ticket.occurrence, c.occurrence());
    assert_eq!(
        db.store
            .approval(&scope(), &c.workflow_id, &c.approval_key)
            .await
            .unwrap(),
        original
    );
    let context = db.store.activation_context(&a.lease.owner).await.unwrap();
    assert_eq!(
        context.provider_admission.unwrap().source.as_ref(),
        Some(source.as_ref())
    );
    assert!(
        db.store
            .start_admission(&p, &start.admission)
            .await
            .is_err()
    );
    assert!(
        db.store
            .finish_admission(
                &p,
                &AdmissionFinishCommand {
                    source: source.admission.clone(),
                    output: Value::Null
                }
            )
            .await
            .is_err()
    );
    let direct = providers::reported(&c, &source.provider, ProviderKnowledge::Pending);
    assert!(db.store.report_dispatch(&p, &direct).await.is_err());
    assert!(
        db.store
            .observe_provider(
                &p,
                &ProviderObservationCommand {
                    ticket: direct.ticket,
                    source_id: direct.source_id,
                    report_id: direct.report_id,
                    report: direct.report
                }
            )
            .await
            .is_err()
    );
    let mut altered = start.clone();
    altered.admission.owner.attempt_id = "another".into();
    assert!(
        db.store
            .start_provider_admission(&p, &altered)
            .await
            .is_err()
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_completed_pending_releases_once_and_keeps_recovery() {
    let db = fixture().await;
    let (c, _, start, source) = start(&db.store, "pending").await;
    let command = report(
        &c,
        &source,
        ProviderCallCompletion::Completed,
        ProviderKnowledge::Pending,
    );
    let receipt = db
        .store
        .report_provider_admission(&worker(), &command)
        .await
        .unwrap();
    assert!(!receipt.receipt.already_accepted);
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    let saved = snapshot(&db.store, &start.admission.occurrence).await;
    assert_eq!(saved.state, AdmissionState::Finished);
    assert!(saved.finish.is_none());
    assert!(
        db.store
            .report_provider_admission(&worker(), &command)
            .await
            .unwrap()
            .receipt
            .already_accepted
    );
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    let evidence = db
        .store
        .inspect_effect(&EffectInspectQuery {
            occurrence: c.occurrence(),
            cursor: None,
            limit: 4,
        })
        .await
        .unwrap();
    assert_eq!(
        evidence.summary.provider.unwrap().knowledge,
        ProviderKnowledgeKind::Pending
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_unknown_success_stays_occupied_and_report_is_immutable() {
    let db = fixture().await;
    let (c, _, start, source) = start(&db.store, "unknown").await;
    let command = report(
        &c,
        &source,
        ProviderCallCompletion::Unknown,
        ProviderKnowledge::Succeeded {
            result: json!({"ok":true}),
        },
    );
    db.store
        .report_provider_admission(&worker(), &command)
        .await
        .unwrap();
    assert_eq!(counters(&db.store).await, (0, 1, 1));
    assert_eq!(
        snapshot(&db.store, &start.admission.occurrence).await.state,
        AdmissionState::UnknownOccupied
    );
    let mut changed = command.clone();
    changed.outcome.call_completion = ProviderCallCompletion::Completed;
    assert!(
        db.store
            .report_provider_admission(&worker(), &changed)
            .await
            .is_err()
    );
    assert_eq!(counters(&db.store).await, (0, 1, 1));
    let snapshot = snapshot(&db.store, &start.admission.occurrence).await;
    db.store
        .release_admission(
            &operator(),
            &AdmissionReleaseCommand {
                source_id: source.admission.source_id.clone(),
                occurrence: snapshot.occurrence,
                request_id: "release".into(),
                expected_revision: snapshot.revision,
                reason: "Reviewed unknown occupancy".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    assert!(
        db.store
            .report_provider_admission(&worker(), &command)
            .await
            .unwrap()
            .receipt
            .already_accepted
    );
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_duplicate_business_closes_offer_without_token_debit() {
    let db = fixture().await;
    let (_, _, _, first) = start(&db.store, "first").await;
    let (_, o) = park(&db.store, "duplicate", governed()).await;
    let (_, command) = offer(&db.store, "duplicate", &o).await;
    let credit: i64 =
        sqlx::query_scalar("SELECT credit FROM admission_resources WHERE resource_name='provider'")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let ProviderAdmissionStartReceipt::Existing { snapshot, .. } = db
        .store
        .start_provider_admission(&worker(), &command)
        .await
        .unwrap()
    else {
        panic!("duplicate dispatched")
    };
    assert_eq!(snapshot.state, AdmissionState::ClosedBeforeStart);
    let record = snapshot.provider.unwrap();
    assert_eq!(record.ticket, first.provider.ticket);
    assert!(record.source.is_none());
    assert_eq!(counters(&db.store).await, (0, 1, 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT credit FROM admission_resources WHERE resource_name='provider'"
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap(),
        credit
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_grant_and_report_rollback_leave_no_partial_authority() {
    let db = fixture().await;
    let (c, o) = park(&db.store, "rollback", governed()).await;
    let (_, command) = offer(&db.store, "rollback", &o).await;
    sqlx::raw_sql("CREATE FUNCTION suppress_admission_start() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='possibly_dispatched' THEN RETURN NULL; END IF; RETURN NEW; END $$; CREATE TRIGGER suppress_admission_start BEFORE UPDATE ON admission_requests FOR EACH ROW EXECUTE FUNCTION suppress_admission_start()").execute(&db.store.pool).await.unwrap();
    assert!(
        db.store
            .start_provider_admission(&worker(), &command)
            .await
            .is_err()
    );
    assert_eq!(counters(&db.store).await, (1, 0, 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM provider_operations")
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT effect_dispatch_claimed FROM workflow_approvals WHERE workflow_id=$1"
        )
        .bind(&c.workflow_id)
        .fetch_one(&db.store.pool)
        .await
        .unwrap()
    );
    sqlx::raw_sql("DROP TRIGGER suppress_admission_start ON admission_requests")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let ProviderAdmissionStartReceipt::Granted { source } = db
        .store
        .start_provider_admission(&worker(), &command)
        .await
        .unwrap()
    else {
        panic!("not granted")
    };
    sqlx::raw_sql("CREATE FUNCTION suppress_admission_finish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='finished' THEN RETURN NULL; END IF; RETURN NEW; END $$; CREATE TRIGGER suppress_admission_finish BEFORE UPDATE ON admission_requests FOR EACH ROW EXECUTE FUNCTION suppress_admission_finish()").execute(&db.store.pool).await.unwrap();
    let report = report(
        &c,
        &source,
        ProviderCallCompletion::Completed,
        ProviderKnowledge::Pending,
    );
    assert!(
        db.store
            .report_provider_admission(&worker(), &report)
            .await
            .is_err()
    );
    assert_eq!(counters(&db.store).await, (0, 1, 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM provider_reports")
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
        0
    );
    sqlx::raw_sql("DROP TRIGGER suppress_admission_finish ON admission_requests")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store
        .report_provider_admission(&worker(), &report)
        .await
        .unwrap();
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_each_quarantine_blocks_fresh_start_but_accepts_frozen_report() {
    let db = fixture().await;
    let (c, _, _, source) = start(&db.store, "started").await;
    let mut next = governed();
    next.business.key = "second".into();
    next.deduplication.as_mut().unwrap().key = "second".into();
    let (_, o) = park(&db.store, "fresh", next).await;
    let (_, command) = offer(&db.store, "fresh", &o).await;
    for (resource, provider) in [
        (
            AdmissionDispatchPolicy::Quarantined,
            ProviderDispatchPolicy::Enabled,
        ),
        (
            AdmissionDispatchPolicy::Enabled,
            ProviderDispatchPolicy::DeclaredRestoreQuarantine,
        ),
    ] {
        let restored = PostgresStore::connect(&db.url, PostgresOptions::default())
            .await
            .unwrap();
        providers::bind(&restored, provider).await;
        restored
            .configure_admissions(&AdmissionConfig {
                v: 1,
                audience: audience(),
                dispatch: resource,
            })
            .unwrap();
        assert!(
            restored
                .start_provider_admission(&worker(), &command)
                .await
                .is_err()
        );
        let report = report(
            &c,
            &source,
            ProviderCallCompletion::Completed,
            ProviderKnowledge::Pending,
        );
        restored
            .report_provider_admission(&worker(), &report)
            .await
            .unwrap();
        assert_eq!(counters(&db.store).await, (1, 0, 0));
        restored.close().await;
    }
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_probe_completion_never_releases_unknown_call() {
    let db = fixture().await;
    let (c, _, _, source) = start(&db.store, "probe").await;
    let command = report(
        &c,
        &source,
        ProviderCallCompletion::Unknown,
        ProviderKnowledge::Pending,
    );
    db.store
        .report_provider_admission(&worker(), &command)
        .await
        .unwrap();
    let (probe, context) = providers::admitted_probe(&db, &c).await;
    db.store
        .report_probe(
            &worker(),
            &ProviderProbeReportCommand {
                owner: probe.lease.owner.clone(),
                ticket: context.ticket,
                source_id: context.source_id,
                report_id: context.report_id,
                generation: context.generation,
                report: ProviderReport {
                    reported_target: c.binding.target.clone(),
                    provider_reference: Some("remote-1".into()),
                    provider_request_id: None,
                    knowledge: ProviderKnowledge::Succeeded {
                        result: json!({"done":true}),
                    },
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(counters(&db.store).await, (0, 1, 1));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_original_completed_report_survives_release_and_execution_retirement() {
    let db = fixture().await;
    let (c, a, start, source) = start(&db.store, "retire").await;
    let report = report(
        &c,
        &source,
        ProviderCallCompletion::Completed,
        ProviderKnowledge::Succeeded {
            result: json!({"large":"x".repeat(100_000)}),
        },
    );
    // Business terminal evidence permits history collection but cannot release
    // the unreported original call's capacity obligation.
    let (probe, context) = providers::admitted_probe(&db, &c).await;
    db.store
        .report_probe(
            &worker(),
            &ProviderProbeReportCommand {
                owner: probe.lease.owner.clone(),
                ticket: context.ticket,
                source_id: context.source_id,
                report_id: context.report_id,
                generation: context.generation,
                report: report.outcome.report.clone(),
            },
        )
        .await
        .unwrap();
    db.store
        .settle(&completed(&probe, Quiescence::Confirmed, Value::Null))
        .await
        .unwrap();
    drain_available(&db.store).await;
    db.store
        .cancel_workflow(&scope(), &c.workflow_id)
        .await
        .unwrap();
    db.store
        .settle(&completed(&a, Quiescence::Confirmed, Value::Null))
        .await
        .unwrap();
    drain_available(&db.store).await;
    db.store
        .advance_admissions(&AdmissionMaintenanceQuery { limit: 16 })
        .await
        .unwrap();
    assert_eq!(counters(&db.store).await, (0, 1, 1));
    let saved = snapshot(&db.store, &start.admission.occurrence).await;
    db.store
        .release_admission(
            &operator(),
            &AdmissionReleaseCommand {
                occurrence: saved.occurrence.clone(),
                source_id: source.admission.source_id.clone(),
                expected_revision: saved.revision,
                request_id: "release-before-collection".into(),
                reason: "Confirmed worker stopped".into(),
            },
        )
        .await
        .unwrap();
    providers::collect_execution(&db).await;
    db.store
        .report_provider_admission(&worker(), &report)
        .await
        .unwrap();
    let saved = snapshot(&db.store, &start.admission.occurrence).await;
    assert_eq!(saved.state, AdmissionState::AdministrativelyReleased);
    assert_eq!(
        saved.provider.unwrap().report.unwrap().call_completion,
        ProviderCallCompletion::Completed
    );
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    assert!(
        db.store
            .report_provider_admission(&worker(), &report)
            .await
            .unwrap()
            .receipt
            .already_accepted
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_runs")
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
        0
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_upgrade_preserves_ungoverned_v1_authority_bytes() {
    let db = TestDb::without_migrations().await;
    let prior = sqlx::migrate::Migrator::with_migrations(
        crate::MIGRATOR
            .iter()
            .filter(|v| v.version < 20261103080000)
            .cloned()
            .collect(),
    );
    prior.run(&db.store.pool).await.unwrap();
    db.begin_legacy_task_seed().await;
    providers::bind(&db.store, ProviderDispatchPolicy::Enabled).await;
    let (_, command) = approved(&db.store, "upgrade-v1", binding()).await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &command).await.unwrap()
    else {
        panic!("missing v1 grant")
    };
    let bytes: (Vec<u8>,Vec<u8>,Vec<u8>) = sqlx::query_as("SELECT a.snapshot_bytes,p.state_bytes,i.source_bytes FROM workflow_approvals a JOIN provider_operations p ON p.workflow_id=a.workflow_id JOIN provider_invocations i ON i.workflow_id=a.workflow_id").fetch_one(&db.store.pool).await.unwrap();
    db.finish_legacy_task_seed().await;
    db.store.migrate().await.unwrap();
    let upgraded: (Vec<u8>,Vec<u8>,Vec<u8>) = sqlx::query_as("SELECT a.snapshot_bytes,p.state_bytes,i.source_bytes FROM workflow_approvals a JOIN provider_operations p ON p.workflow_id=a.workflow_id JOIN provider_invocations i ON i.workflow_id=a.workflow_id").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(bytes, upgraded);
    assert!(WorkflowStore::provider_admissions(&db.store).is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM provider_admission_links")
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
        0
    );
    // A real upgrade starts the new server with a fresh prepared-statement cache.
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    providers::bind(&reopened, ProviderDispatchPolicy::Enabled).await;
    reopened
        .report_dispatch(
            &worker(),
            &providers::reported(&command, &source, ProviderKnowledge::Pending),
        )
        .await
        .unwrap();
    reopened.close().await;
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_release_does_not_close_pending_provider_recovery() {
    let db = fixture().await;
    let (c, _, start, source) = start(&db.store, "release-pending").await;
    db.store
        .report_provider_admission(
            &worker(),
            &report(
                &c,
                &source,
                ProviderCallCompletion::Unknown,
                ProviderKnowledge::Pending,
            ),
        )
        .await
        .unwrap();
    let saved = snapshot(&db.store, &start.admission.occurrence).await;
    db.store
        .release_admission(
            &operator(),
            &AdmissionReleaseCommand {
                occurrence: saved.occurrence,
                source_id: source.admission.source_id,
                expected_revision: saved.revision,
                request_id: "release-pending".into(),
                reason: "Caller confirmed terminated".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    let detail = db
        .store
        .inspect_effect(&EffectInspectQuery {
            occurrence: c.occurrence(),
            cursor: None,
            limit: 4,
        })
        .await
        .unwrap();
    let provider = detail.summary.provider.unwrap();
    assert_eq!(provider.knowledge, ProviderKnowledgeKind::Pending);
    assert_eq!(provider.handling, ProviderHandlingState::Unresolved);
    assert!(provider.reconciliation.stopped_reason.is_none());
    let (_, context) = providers::admitted_probe(&db, &c).await;
    assert_eq!(context.ticket.occurrence, c.occurrence());
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_report_racing_release_settles_capacity_once() {
    let db = fixture().await;
    let (c, a, start, source) = start(&db.store, "report-release-race").await;
    db.store
        .cancel_workflow(&scope(), &c.workflow_id)
        .await
        .unwrap();
    db.store
        .settle(&completed(&a, Quiescence::Confirmed, Value::Null))
        .await
        .unwrap();
    drain_available(&db.store).await;
    db.store
        .advance_admissions(&AdmissionMaintenanceQuery { limit: 16 })
        .await
        .unwrap();
    let saved = snapshot(&db.store, &start.admission.occurrence).await;
    assert_eq!(saved.state, AdmissionState::UnknownOccupied);
    let release = AdmissionReleaseCommand {
        occurrence: saved.occurrence,
        source_id: source.admission.source_id.clone(),
        expected_revision: saved.revision,
        request_id: "race-release".into(),
        reason: "Worker stopped".into(),
    };
    let report = report(
        &c,
        &source,
        ProviderCallCompletion::Completed,
        ProviderKnowledge::Pending,
    );
    let (p, op) = (worker(), operator());
    let (reported, released) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            db.store.report_provider_admission(&p, &report),
            db.store.release_admission(&op, &release)
        )
    })
    .await
    .expect("report/release lock order must not deadlock");
    reported.unwrap();
    assert!(released.is_ok() || matches!(released, Err(ContractError::Conflict)));
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    assert!(
        db.store
            .report_provider_admission(&worker(), &report)
            .await
            .unwrap()
            .receipt
            .already_accepted
    );
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    db.finish().await;
}
#[derive(Clone, Copy)]
enum ExpiryFence {
    ProviderLock,
    FinalWrites,
}
#[derive(Clone, Copy)]
enum AuthorityLoss {
    Lease,
    Principal,
}
async fn shortened_offer(store: &PostgresStore, o: &AdmissionOccurrence, absolute: bool) -> i64 {
    let mut saved = snapshot(store, o).await;
    let deadline: i64 = sqlx::query_scalar(
        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint + 1000",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap();
    saved.offer.as_mut().unwrap().expires_at = deadline as u64;
    if absolute {
        saved.deadline_at = deadline as u64;
    }
    saved.validate().unwrap();
    sqlx::query("UPDATE admission_requests SET offer_expires_at_ms=$2,deadline_at_ms=$3,snapshot_bytes=$4 WHERE workflow_id=$1")
        .bind(&o.workflow_id).bind(deadline).bind(saved.deadline_at as i64).bind(codec::encode(&saved).unwrap())
        .execute(&store.pool).await.unwrap();
    deadline
}
async fn expiry_barrier<'a>(
    store: &'a PostgresStore,
    c: &ProviderClaimCommand,
    fence: ExpiryFence,
) -> sqlx::Transaction<'a, sqlx::Postgres> {
    let mut barrier = store.pool.begin().await.unwrap();
    match fence {
        ExpiryFence::ProviderLock => {
            let business = canonical_json_bytes(
                &serde_json::to_value((&audience().instance_id, scope(), &c.binding.business))
                    .unwrap(),
            )
            .unwrap();
            sqlx::query(
                "SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea,'hex'),1))",
            )
            .bind(business)
            .execute(&mut *barrier)
            .await
            .unwrap();
        }
        ExpiryFence::FinalWrites => {
            // By this write, the provider grant, source, retained evidence and
            // admission request are already tentative. Release only after expiry.
            sqlx::raw_sql("CREATE FUNCTION gate_admission_start() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.started > OLD.started THEN PERFORM pg_advisory_xact_lock(493782); END IF; RETURN NEW; END $$; CREATE TRIGGER gate_admission_start BEFORE UPDATE ON admission_resources FOR EACH ROW EXECUTE FUNCTION gate_admission_start()")
                .execute(&store.pool).await.unwrap();
            sqlx::query("SELECT pg_advisory_xact_lock(493782)")
                .execute(&mut *barrier)
                .await
                .unwrap();
        }
    }
    barrier
}
async fn expire_while_starting(
    store: &PostgresStore,
    command: &ProviderAdmissionStartCommand,
    principal: &VerifiedPrincipal,
    deadline: i64,
    barrier: sqlx::Transaction<'_, sqlx::Postgres>,
) -> StoreResult<ProviderAdmissionStartReceipt> {
    let (result, ()) = crate::tests::bounded(async {
        // Calling once prevents store retries from hiding an incorrect rollback.
        tokio::join!(
            store.start_provider_admission_once(principal, command),
            async {
                crate::tests::wait_for_lock_waiter(store).await;
                loop {
                    let expired: bool = sqlx::query_scalar(
                        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint >= $1",
                    )
                    .bind(deadline)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
                    if expired {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                barrier.rollback().await.unwrap();
            }
        )
    })
    .await;
    result
}
async fn assert_no_dispatch(store: &PostgresStore, o: &AdmissionOccurrence, credit: i64) {
    let (operations, sources, effects, entries, probes): (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM provider_operations), (SELECT count(*) FROM provider_invocations), (SELECT count(*) FROM approved_effect_occurrences), (SELECT count(*) FROM approved_effect_evidence_entries), (SELECT count(*) FROM provider_probe_tasks)",
    ).fetch_one(&store.pool).await.unwrap();
    assert_eq!(
        (operations, sources, effects, entries, probes),
        (0, 0, 0, 0, 0)
    );
    let (claimed, recovered, started): (bool, bool, bool) = sqlx::query_as(
        "SELECT (SELECT effect_dispatch_claimed FROM workflow_approvals WHERE workflow_id=$1), (SELECT has_provider_operations FROM workflow_runs WHERE workflow_id=$1), (SELECT start_command_bytes IS NOT NULL FROM admission_requests WHERE workflow_id=$1)",
    ).bind(&o.workflow_id).fetch_one(&store.pool).await.unwrap();
    assert_eq!((claimed, recovered, started), (false, false, false));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT credit FROM admission_resources WHERE resource_name='provider'"
        )
        .fetch_one(&store.pool)
        .await
        .unwrap(),
        credit
    );
    let saved = snapshot(store, o).await;
    assert!(saved.source.is_none() && saved.provider.is_none() && saved.finish.is_none());
}
async fn rewait(store: &PostgresStore, a: &Assignment, o: &AdmissionOccurrence) {
    let ctx = store.activation_context(&a.lease.owner).await.unwrap();
    apply_decision(
        store,
        a,
        ctx.revision,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "admitted".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "dispatch".into(),
                request: AdmissionWaitRequest::Existing {
                    occurrence: o.clone(),
                },
                timeout_ms: 60000,
            },
        },
        &[],
    )
    .await;
}
async fn capacity_expiry_recovers(fence: ExpiryFence, absolute: bool) {
    let db = fixture().await;
    let id = "capacity-expired";
    let (c, o) = park(&db.store, id, governed()).await;
    let (a, command) = offer(&db.store, id, &o).await;
    let barrier = expiry_barrier(&db.store, &c, fence).await;
    let deadline = shortened_offer(&db.store, &o, absolute).await;
    let before = snapshot(&db.store, &o).await;
    let credit: i64 =
        sqlx::query_scalar("SELECT credit FROM admission_resources WHERE resource_name='provider'")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_work WHERE processed_at_ms IS NULL")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let receipt = expire_while_starting(&db.store, &command, &worker(), deadline, barrier)
        .await
        .unwrap();
    let ProviderAdmissionStartReceipt::Existing {
        snapshot: observed, ..
    } = receipt
    else {
        panic!("expired capacity must return an observation, not a grant")
    };
    assert_eq!(observed.state, AdmissionState::AwaitingRewait);
    assert_eq!(observed.occurrence, o);
    assert_eq!(observed.deadline_at, before.deadline_at);
    assert_eq!(observed.binding, before.binding);
    assert_eq!(counters(&db.store).await, (0, 0, 0));
    assert_no_dispatch(&db.store, &o, credit).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM workflow_work WHERE processed_at_ms IS NULL"
        )
        .fetch_one(&db.store.pool)
        .await
        .unwrap(),
        pending
    );
    // An exact network retry still cannot create a grant from the expired offer.
    assert!(matches!(
        db.store
            .start_provider_admission(&worker(), &command)
            .await
            .unwrap(),
        ProviderAdmissionStartReceipt::Existing { .. }
    ));
    rewait(&db.store, &a, &o).await;
    assert_eq!(
        snapshot(&db.store, &o).await.deadline_at,
        before.deadline_at
    );
    if absolute {
        let resumed = acquire(&db.store, id).await;
        let context = db
            .store
            .activation_context(&resumed.lease.owner)
            .await
            .unwrap();
        let Some(WorkflowWake::ResourceAdmission(wake)) = context.wake else {
            panic!("missing deadline wake")
        };
        assert!(matches!(
            wake.selection,
            AdmissionSelection::Closed {
                reason: AdmissionClosedReason::Deadline
            }
        ));
        assert_eq!(counters(&db.store).await, (0, 0, 0));
        assert_no_dispatch(&db.store, &o, credit).await;
    } else {
        let (_, mut fresh) = offer(&db.store, id, &o).await;
        fresh.admission.start_id = "start:fresh".into();
        fresh.admission.finish_id = "report:fresh".into();
        assert!(fresh.admission.offer_generation > command.admission.offer_generation);
        let ProviderAdmissionStartReceipt::Granted { source } = db
            .store
            .start_provider_admission(&worker(), &fresh)
            .await
            .unwrap()
        else {
            panic!("fresh offer must grant")
        };
        assert_eq!(source.provider.ticket.occurrence, c.occurrence());
        assert_eq!(source.admission.occurrence, o);
        assert!(matches!(
            db.store
                .start_provider_admission(&worker(), &fresh)
                .await
                .unwrap(),
            ProviderAdmissionStartReceipt::Existing { .. }
        ));
        assert_eq!(counters(&db.store).await, (0, 1, 0));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM provider_invocations")
                .fetch_one(&db.store.pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT credit FROM admission_resources WHERE resource_name='provider'"
            )
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
            credit - 60_000
        );
    }
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_advisory_lock_expiry_recovers_by_rewaiting() {
    capacity_expiry_recovers(ExpiryFence::ProviderLock, false).await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_final_fence_expiry_rolls_back_and_recovers() {
    capacity_expiry_recovers(ExpiryFence::FinalWrites, false).await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_expiry_rewait_preserves_absolute_deadline() {
    capacity_expiry_recovers(ExpiryFence::FinalWrites, true).await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_expiry_recovery_preserves_authority_errors() {
    for fence in [ExpiryFence::ProviderLock, ExpiryFence::FinalWrites] {
        for loss in [AuthorityLoss::Lease, AuthorityLoss::Principal] {
            for capacity_expired in [false, true] {
                let db = fixture().await;
                let (c, o) = park(&db.store, "expired-authority", governed()).await;
                let (_, command) = offer(&db.store, "expired-authority", &o).await;
                let barrier = expiry_barrier(&db.store, &c, fence).await;
                let deadline = if capacity_expired {
                    shortened_offer(&db.store, &o, false).await
                } else {
                    sqlx::query_scalar(
                        "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint + 1000",
                    )
                    .fetch_one(&db.store.pool)
                    .await
                    .unwrap()
                };
                let p = match loss {
                    AuthorityLoss::Lease => {
                        sqlx::query("UPDATE attempts SET expires_at_ms=$2 WHERE attempt_id=$1")
                            .bind(&command.admission.owner.attempt_id)
                            .bind(deadline)
                            .execute(&db.store.pool)
                            .await
                            .unwrap();
                        sqlx::query("UPDATE tasks SET next_expiry_ms=$2 WHERE task_id=$1")
                            .bind(&command.admission.owner.task_id)
                            .bind(deadline)
                            .execute(&db.store.pool)
                            .await
                            .unwrap();
                        worker()
                    }
                    AuthorityLoss::Principal => VerifiedPrincipal::new(
                        PrincipalIdentity {
                            issuer: audience().issuer(),
                            subject: "worker".into(),
                        },
                        audience(),
                        "short-lived-worker".into(),
                        PrincipalClass::Worker,
                        deadline as u64,
                        vec![AccessCapability::WorkerExecute],
                    )
                    .unwrap(),
                };
                let credit: i64 = sqlx::query_scalar(
                    "SELECT credit FROM admission_resources WHERE resource_name='provider'",
                )
                .fetch_one(&db.store.pool)
                .await
                .unwrap();
                let response =
                    expire_while_starting(&db.store, &command, &p, deadline, barrier).await;
                match loss {
                    AuthorityLoss::Lease => assert!(matches!(
                        response,
                        Err(StoreError::Contract(ContractError::OwnershipLost))
                    )),
                    AuthorityLoss::Principal => assert!(matches!(
                        response,
                        Err(StoreError::Contract(ContractError::Unauthenticated))
                    )),
                }
                // Invalid authority cannot commit even the capacity-state recovery.
                assert_eq!(counters(&db.store).await, (1, 0, 0));
                assert_no_dispatch(&db.store, &o, credit).await;
                db.finish().await;
            }
        }
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_admission_duplicate_start_and_original_cancel_share_lock_order() {
    let db = fixture().await;
    let (original, _, _, _) = start(&db.store, "original-lock-order").await;
    let (_, o) = park(&db.store, "duplicate-lock-order", governed()).await;
    let (_, start) = offer(&db.store, "duplicate-lock-order", &o).await;
    db.store
        .cancel_workflow(&scope(), &original.workflow_id)
        .await
        .unwrap();
    let work = db
        .store
        .claim_work(16)
        .await
        .unwrap()
        .into_iter()
        .find(|w| {
            w.workflow_id == original.workflow_id && matches!(w.source, WorkflowWorkSource::Drain)
        })
        .expect("cancellation drain");
    sqlx::raw_sql("CREATE FUNCTION gate_provider_drain() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.stopped_reason='workflow_cancelled' AND OLD.stopped_reason IS NULL THEN PERFORM pg_advisory_xact_lock(493781); END IF; RETURN NEW; END $$; CREATE TRIGGER gate_provider_drain BEFORE UPDATE ON provider_operations FOR EACH ROW EXECUTE FUNCTION gate_provider_drain()").execute(&db.store.pool).await.unwrap();
    let mut barrier = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(493781)")
        .execute(&mut *barrier)
        .await
        .unwrap();
    let p = worker();
    let (drained, started) = crate::tests::bounded(async {
        tokio::join!(db.store.apply_work_once(&work, &[]), async {
            // Drain has reached provider mutation while retaining its earlier locks.
            crate::tests::wait_for_lock_waiter(&db.store).await;
            let (started, ()) = tokio::join!(db.store.start_provider_admission_once(&p, &start), async {
                loop {
                    let blocked:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()").fetch_one(&db.store.pool).await.unwrap();
                    if blocked >= 2 { break; }
                    tokio::task::yield_now().await;
                }
                barrier.rollback().await.unwrap();
            });
            started
        })
    }).await;
    // Use transaction-local entry points: automatic deadlock retries cannot mask
    // an inconsistent lock order in this controlled interleaving.
    drained.unwrap();
    assert!(matches!(
        started.unwrap(),
        ProviderAdmissionStartReceipt::Existing { .. }
    ));
    assert_eq!(counters(&db.store).await, (0, 1, 1));
    db.finish().await;
}
