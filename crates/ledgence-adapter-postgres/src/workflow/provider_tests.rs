//! Real provider transactions; every test owns a disposable database.
use super::*;
pub(super) fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "provider-authority".into(),
        instance_id: "provider-instance".into(),
        scope: scope(),
    }
}
fn principal(subject: &str, credential: &str, class: PrincipalClass) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: subject.into(),
        },
        audience(),
        credential.into(),
        class,
        253402300799999,
        if class == PrincipalClass::Worker {
            vec![AccessCapability::WorkerExecute]
        } else {
            vec![
                AccessCapability::ApprovalDecide,
                AccessCapability::SensitiveRead,
                AccessCapability::MetadataRead,
                AccessCapability::EffectResolve,
            ]
        },
    )
    .unwrap()
}
pub(super) fn worker() -> VerifiedPrincipal {
    principal("worker", "worker-key", PrincipalClass::Worker)
}
pub(super) fn operator() -> VerifiedPrincipal {
    principal("operator", "operator-key", PrincipalClass::Operator)
}
pub(super) fn binding() -> ProviderBindingRequest {
    serde_json::from_value(json!({"v":1,"business":{"namespace":"exports","key":"business:1"},"contract":{"adapter_id":"example.export","version":"1","digest":format!("sha256:{}","b".repeat(64)),"completion_criterion":"Artifact committed","dispatch_callable":"app:send","reconcile_callable":"app:lookup","lookup":"by_binding"},"target":{"provider":"example","environment":"local","account":"demo","api_variant":"v1","operation":"export","endpoint_identity":"local-provider","dimensions":{}},"deduplication":{"domain":{"provider":"example","namespace":"account","dimensions":{"account":"demo"}},"key":"effect:1","horizon":{"kind":"unknown"}},"credential_reference":"local-provider","reconciliation":{"queue":"recovery","initial_delay_ms":1000,"interval_ms":1000,"maximum_interval_ms":2000,"max_checks":2,"max_elapsed_ms":60000,"check_timeout_ms":1000}})).unwrap()
}
pub(super) async fn bind(store: &PostgresStore, dispatch: ProviderDispatchPolicy) {
    let config = SelfHostedInstanceConfig {
        instance_id: audience().instance_id,
        name: "Provider tests".into(),
        scope: scope(),
        suggested_queues: vec!["python".into(), "recovery".into()],
        allowed_origins: vec![],
    };
    store
        .initialize_instance_with_access(Some(&config), Some(&audience()))
        .await
        .unwrap();
    store
        .configure_provider_operations(&ProviderOperationConfig {
            v: 1,
            audience: audience(),
            dispatch,
        })
        .unwrap();
}
pub(super) async fn approved(
    store: &PostgresStore,
    id: &str,
    request: ProviderBindingRequest,
) -> (Assignment, ProviderClaimCommand) {
    approved_with_budget(store, id, request, None).await
}
pub(super) async fn approved_with_budget(
    store: &PostgresStore,
    id: &str,
    request: ProviderBindingRequest,
    budget: Option<RunBudgetPolicy>,
) -> (Assignment, ProviderClaimCommand) {
    let mut submission = command();
    submission.input.budget = budget;
    submission.idempotency_key = id.into();
    submission.input.queue = id.into();
    let w = store
        .accept_resolved_workflow(&submission, &descriptor())
        .await
        .unwrap();
    let a = acquire(store, id).await;
    let action = ApprovalAction {
        name: "app:send".into(),
        version: "1".into(),
        arguments: json!({"number":9007199254740993_u64,"nul":"\0","literal":"\\u0000","float":1.0}),
    };
    apply_decision(
        store,
        &a,
        0,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "dispatch".into(),
            commands: vec![],
            wait: WorkflowWait::Approval {
                key: "approve".into(),
                action: action.clone(),
                protection: ApprovalProtection::ProviderOperation,
                provider_binding: Some(Box::new(request)),
                proposed_arguments: None,
                timeout_ms: 180000,
            },
        },
        &[],
    )
    .await;
    let approval = store
        .approval(&scope(), &w.workflow_id, "approve")
        .await
        .unwrap();
    let b = approval.provider_binding.clone().unwrap();
    let c = ApprovalDecisionCommand {
        scope: scope(),
        workflow_id: w.workflow_id.clone(),
        key: approval.key,
        activation_id: a.lease.owner.task_id,
        revision: 0,
        action: action.clone(),
        protection: ApprovalProtection::ProviderOperation,
        provider_binding: Some(b.clone()),
        decision_id: format!("decision:{id}"),
        decision: ApprovalDecision::Approve,
        reviewer: "caller label".into(),
        reason: Some("Reviewed".into()),
    };
    store
        .decide_verified(
            &operator(),
            &c,
            &OperatorRequest::new(format!("request:{id}"), None).unwrap(),
        )
        .await
        .unwrap();
    store.apply_work(&one_work(store).await, &[]).await.unwrap();
    let a = acquire(store, id).await;
    dispatched(store, &a).await;
    (
        a.clone(),
        ProviderClaimCommand {
            budget_fence: false,
            owner: a.lease.owner,
            workflow_id: w.workflow_id,
            approval_key: "approve".into(),
            claim_id: format!("claim:{id}"),
            report_id: format!("report:{id}"),
            action,
            binding: b,
        },
    )
}
async fn fixture() -> (TestDb, Assignment, ProviderClaimCommand) {
    let db = TestDb::new().await;
    bind(&db.store, ProviderDispatchPolicy::Enabled).await;
    let (a, c) = approved(&db.store, "python", binding()).await;
    (db, a, c)
}
pub(super) fn reported(
    c: &ProviderClaimCommand,
    source: &ProviderDispatchSource,
    knowledge: ProviderKnowledge,
) -> ProviderDispatchReportCommand {
    ProviderDispatchReportCommand {
        owner: c.owner.clone(),
        ticket: source.ticket.clone(),
        source_id: source.source_id.clone(),
        report_id: source.report_id.clone(),
        report: ProviderReport {
            usage: None,
            reported_target: c.binding.target.clone(),
            provider_reference: Some("remote-1".into()),
            provider_request_id: Some("req-1".into()),
            knowledge,
        },
    }
}
async fn inspect(store: &PostgresStore, c: &ProviderClaimCommand) -> EffectEvidenceDetail {
    store
        .inspect_effect(&EffectInspectQuery {
            occurrence: c.occurrence(),
            cursor: None,
            limit: 4,
        })
        .await
        .unwrap()
}
pub(super) async fn due(store: &PostgresStore, c: &ProviderClaimCommand) {
    let o = c.occurrence();
    let mut tx = store.pool.begin().await.unwrap();
    let mut r = super::super::effect_retained::load(&mut tx, &o)
        .await
        .unwrap()
        .unwrap();
    let state = super::super::provider::state::load(&mut tx, &r)
        .await
        .unwrap();
    r.summary
        .provider
        .as_mut()
        .unwrap()
        .reconciliation
        .next_due_at = Some(1);
    super::super::provider::state::save(&mut tx, &r, &state)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
fn wait(c: &ProviderClaimCommand, key: &str, after: Option<u64>) -> WorkflowAction {
    WorkflowAction::Wait {
        state: Value::Null,
        continuation: "finish".into(),
        commands: vec![],
        wait: WorkflowWait::ProviderOperation {
            key: key.into(),
            occurrence: c.occurrence(),
            after_effect_revision: after,
        },
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_one_grant_exact_report_and_no_journal() {
    let (db, a, c) = fixture().await;
    let p = worker();
    let (first, second) = tokio::join!(
        db.store.claim_provider(&p, &c),
        db.store.claim_provider(&p, &c)
    );
    let results = [first.unwrap(), second.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, ProviderClaimReceipt::Dispatch { .. }))
            .count(),
        1
    );
    let source = results
        .into_iter()
        .find_map(|r| match r {
            ProviderClaimReceipt::Dispatch { source, .. } => Some(source),
            _ => None,
        })
        .unwrap();
    let c = reported(
        &c,
        &source,
        ProviderKnowledge::Succeeded {
            result: json!({"value":9007199254740993_u64,"nul":"\0"}),
        },
    );
    let receipt = db.store.report_dispatch(&p, &c).await.unwrap();
    assert!(!receipt.receipt().already_accepted);
    let rotated = principal("worker", "rotated", PrincipalClass::Worker);
    assert!(
        db.store
            .report_dispatch(&rotated, &c)
            .await
            .unwrap()
            .receipt()
            .already_accepted
    );
    let mut changed = c.clone();
    changed.report.knowledge = ProviderKnowledge::Succeeded { result: json!(1.0) };
    assert!(matches!(
        db.store.report_dispatch(&p, &changed).await,
        Err(ContractError::Conflict)
    ));
    let claim = ProviderClaimCommand {
        budget_fence: false,
        owner: a.lease.owner.clone(),
        workflow_id: c.ticket.occurrence.workflow_id.clone(),
        approval_key: "approve".into(),
        claim_id: "another".into(),
        report_id: "another".into(),
        action: ApprovalAction {
            name: "app:send".into(),
            version: "1".into(),
            arguments: json!({"number":9007199254740993_u64,"nul":"\0","literal":"\\u0000","float":1.0}),
        },
        binding: binding().resolve(descriptor()).unwrap(),
    };
    let d = inspect(&db.store, &claim).await;
    assert_eq!(
        d.summary.provider.unwrap().handling,
        ProviderHandlingState::ProviderTerminal
    );
    assert_eq!(d.summary.journal_state, EffectJournalState::Unconfirmed);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_local_results")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(matches!(
        db.store
            .claim_approved_effect(&ApprovedEffectClaimCommand {
                budget_fence: false,
                owner: a.lease.owner,
                workflow_id: claim.workflow_id,
                approval_key: claim.approval_key,
                action: claim.action,
                lookup: "app:lookup".into()
            })
            .await,
        Err(ContractError::Conflict)
    ));
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_business_uniqueness_and_declared_restore_quarantine() {
    let (db, _, c) = fixture().await;
    db.store.claim_provider(&worker(), &c).await.unwrap();
    let (_, other) = approved(&db.store, "other", binding()).await;
    assert!(matches!(
        db.store.claim_provider(&worker(), &other).await.unwrap(),
        ProviderClaimReceipt::ExistingOperation { .. }
    ));
    let mut changed = other.clone();
    changed.action.arguments["float"] = json!(1);
    assert!(matches!(
        db.store.claim_provider(&worker(), &changed).await,
        Err(ContractError::Conflict)
    ));
    let recovered = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(
        &recovered,
        ProviderDispatchPolicy::DeclaredRestoreQuarantine,
    )
    .await;
    assert!(matches!(
        recovered.claim_provider(&worker(), &c).await.unwrap(),
        ProviderClaimReceipt::Existing { .. }
    ));
    let mut unique = binding();
    unique.business.key = "new".into();
    unique.deduplication.as_mut().unwrap().key = "new".into();
    let (_, new) = approved(&recovered, "new", unique).await;
    assert!(matches!(
        recovered.claim_provider(&worker(), &new).await,
        Err(ContractError::Forbidden)
    ));
    recovered.pool.close().await;
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_probe_report_cleanup_and_one_native_wake() {
    let (db, a, c) = fixture().await;
    db.store.claim_provider(&worker(), &c).await.unwrap();
    apply_decision(&db.store, &a, 1, wait(&c, "provider:0", None), &[]).await;
    due(&db.store, &c).await;
    let progress = db
        .store
        .advance_provider_operations(&ProviderMaintenanceQuery {
            cursor: None,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(progress.allocated, 1);
    let probe = acquire(&db.store, "recovery").await;
    let marker = probe.provider_probe.clone().unwrap();
    let page = db
        .store
        .list_tasks(
            &scope(),
            &TaskListQuery {
                filters: TaskFilters {
                    queue: Some("recovery".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].provider_probe, Some(marker.clone()));
    let ctx = db
        .store
        .probe_context(
            &worker(),
            &ProviderProbeContextCommand {
                owner: probe.lease.owner.clone(),
                probe: marker.clone(),
            },
        )
        .await
        .unwrap();
    dispatched(&db.store, &probe).await;
    let report = ProviderProbeReportCommand {
        owner: probe.lease.owner.clone(),
        ticket: ctx.ticket,
        source_id: ctx.source_id,
        report_id: ctx.report_id,
        generation: marker.generation,
        report: ProviderReport {
            usage: None,
            reported_target: c.binding.target.clone(),
            provider_reference: Some("remote-1".into()),
            provider_request_id: None,
            knowledge: ProviderKnowledge::Succeeded {
                result: json!({"done":true}),
            },
        },
    };
    db.store.report_probe(&worker(), &report).await.unwrap();
    report_task(&db.store, &probe).await;
    drain_available(&db.store).await;
    let resumed = acquire(&db.store, "python").await;
    let context = db
        .store
        .activation_context(&resumed.lease.owner)
        .await
        .unwrap();
    assert!(
        matches!(context.wake,Some(WorkflowWake::ProviderOperation(w)) if matches!(w.selection,ProviderSelection::ProviderSucceeded{..}))
    );
    assert!(
        inspect(&db.store, &c)
            .await
            .summary
            .provider
            .unwrap()
            .reconciliation
            .active_task_id
            .is_none()
    );
    db.finish().await;
}
async fn report_task(store: &PostgresStore, a: &Assignment) {
    store
        .settle(&completed(
            a,
            Quiescence::Confirmed,
            json!({"provider_report_recorded":true}),
        ))
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_no_lookup_and_evidence_after_cancel_never_revives() {
    let db = TestDb::new().await;
    bind(&db.store, ProviderDispatchPolicy::Enabled).await;
    let mut b = binding();
    b.contract.lookup = ProviderLookup::Unavailable;
    b.contract.reconcile_callable = None;
    let (a, c) = approved(&db.store, "python", b).await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    apply_decision(&db.store, &a, 1, wait(&c, "wait", None), &[]).await;
    due(&db.store, &c).await;
    let progress = db
        .store
        .advance_provider_operations(&ProviderMaintenanceQuery {
            cursor: None,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(progress.allocated, 0);
    assert_eq!(progress.stopped, 1);
    drain_available(&db.store).await;
    let resumed = acquire(&db.store, "python").await;
    assert!(
        matches!(db.store.activation_context(&resumed.lease.owner).await.unwrap().wake,Some(WorkflowWake::ProviderOperation(w)) if matches!(w.selection,ProviderSelection::NeedsReview{reason:ProviderRecoveryStopReason::LookupUnavailable,..}))
    );
    db.store
        .cancel_workflow(&scope(), &c.workflow_id)
        .await
        .unwrap();
    let live = reported(
        &c,
        &source,
        ProviderKnowledge::Succeeded {
            result: Value::Null,
        },
    );
    let observe = ProviderObservationCommand {
        ticket: live.ticket,
        source_id: live.source_id,
        report_id: live.report_id,
        report: live.report,
    };
    let receipt = db
        .store
        .observe_provider(&worker(), &observe)
        .await
        .unwrap();
    assert_eq!(
        receipt.receipt().accepted_via,
        ProviderReportIngress::EvidenceOnly
    );
    assert!(matches!(
        db.store
            .workflow_status(&scope(), &c.workflow_id)
            .await
            .unwrap()
            .state,
        WorkflowState::Cancelling | WorkflowState::Cancelled
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_mutated_grant_and_suppressed_report_roll_back_all_proof() {
    let (db, _, c) = fixture().await;
    sqlx::raw_sql("CREATE FUNCTION mutate_provider_grant() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.effect_dispatch_claimed AND NOT OLD.effect_dispatch_claimed THEN NEW.effect_dispatch_attempt_id='other'; END IF; RETURN NEW; END $$; CREATE TRIGGER mutate_provider_grant BEFORE UPDATE ON workflow_approvals FOR EACH ROW EXECUTE FUNCTION mutate_provider_grant()").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.claim_provider(&worker(), &c).await,
        Err(ContractError::Unavailable(_))
    ));
    let counts:(i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM provider_operations),(SELECT count(*) FROM approved_effect_occurrences)").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(counts, (0, 0));
    sqlx::query("DROP TRIGGER mutate_provider_grant ON workflow_approvals")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    let before = inspect(&db.store, &c).await;
    sqlx::raw_sql("CREATE FUNCTION suppress_provider_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER suppress_provider_report BEFORE INSERT ON provider_reports FOR EACH ROW EXECUTE FUNCTION suppress_provider_report()").execute(&db.store.pool).await.unwrap();
    let command = reported(
        &c,
        &source,
        ProviderKnowledge::Succeeded {
            result: json!({"count":1}),
        },
    );
    assert!(matches!(
        db.store.report_dispatch(&worker(), &command).await,
        Err(ContractError::Unavailable(_))
    ));
    let after = inspect(&db.store, &c).await;
    assert_eq!(
        after.summary.effect_revision,
        before.summary.effect_revision
    );
    assert_eq!(after.summary.entry_count, before.summary.entry_count);
    assert_eq!(
        after.summary.provider.unwrap().knowledge,
        ProviderKnowledgeKind::Unknown
    );
    let facts: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_terminal_facts")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(facts, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_report_wins_cross_ingress_without_fabricating_live_ack() {
    let (db, _, c) = fixture().await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    let command = reported(
        &c,
        &source,
        ProviderKnowledge::Succeeded {
            result: Value::Null,
        },
    );
    let observation = ProviderObservationCommand {
        ticket: command.ticket.clone(),
        source_id: command.source_id.clone(),
        report_id: command.report_id.clone(),
        report: command.report.clone(),
    };
    assert!(matches!(
        db.store
            .observe_provider(
                &principal("other", "other", PrincipalClass::Worker),
                &observation
            )
            .await,
        Err(ContractError::Forbidden)
    ));
    db.store
        .observe_provider(&worker(), &observation)
        .await
        .unwrap();
    let ProviderReportReply::ExistingReport { original_receipt } =
        db.store.report_dispatch(&worker(), &command).await.unwrap()
    else {
        panic!("evidence-only acceptance became a live ack")
    };
    assert_eq!(
        original_receipt.accepted_via,
        ProviderReportIngress::EvidenceOnly
    );
    assert!(original_receipt.already_accepted);
    assert_eq!(inspect(&db.store, &c).await.summary.entry_count, 2);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_expiry_after_lock_and_busy_scheduler_leave_no_authority() {
    let (db, _, c) = fixture().await;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let p = VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: "worker".into(),
        },
        audience(),
        "short".into(),
        PrincipalClass::Worker,
        now as u64 + 250,
        vec![AccessCapability::WorkerExecute],
    )
    .unwrap();
    let mut blocker = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR NO KEY UPDATE")
        .bind(&c.workflow_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let (response, ()) = tokio::join!(db.store.claim_provider(&p, &c), async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        blocker.commit().await.unwrap();
    });
    assert!(matches!(response, Err(ContractError::Unauthenticated)));
    db.store.claim_provider(&worker(), &c).await.unwrap();
    due(&db.store, &c).await;
    let mut blocker = db.store.pool.begin().await.unwrap();
    sqlx::query(
        "SELECT workflow_id FROM approved_effect_occurrences WHERE workflow_id=$1 FOR UPDATE",
    )
    .bind(&c.workflow_id)
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    let progress = db
        .store
        .advance_provider_operations(&ProviderMaintenanceQuery {
            cursor: None,
            limit: 16,
        })
        .await
        .unwrap();
    assert_eq!(progress.skipped_busy, 1);
    assert_eq!(progress.allocated, 0);
    assert!(progress.next_cursor.is_some());
    blocker.commit().await.unwrap();
    assert_eq!(
        db.store
            .advance_provider_operations(&ProviderMaintenanceQuery {
                cursor: None,
                limit: 16
            })
            .await
            .unwrap()
            .allocated,
        1
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_finite_pending_budget_stops_without_overlapping_probes() {
    let (db, a, c) = fixture().await;
    db.store.claim_provider(&worker(), &c).await.unwrap();
    apply_decision(&db.store, &a, 1, wait(&c, "wait", None), &[]).await;
    for generation in 1..=2 {
        due(&db.store, &c).await;
        assert_eq!(
            db.store
                .advance_provider_operations(&ProviderMaintenanceQuery {
                    cursor: None,
                    limit: 16
                })
                .await
                .unwrap()
                .allocated,
            1
        );
        assert_eq!(
            db.store
                .advance_provider_operations(&ProviderMaintenanceQuery {
                    cursor: None,
                    limit: 16
                })
                .await
                .unwrap()
                .allocated,
            0
        );
        let a = acquire(&db.store, "recovery").await;
        let marker = a.provider_probe.clone().unwrap();
        assert_eq!(marker.generation, generation);
        let context = db
            .store
            .probe_context(
                &worker(),
                &ProviderProbeContextCommand {
                    owner: a.lease.owner.clone(),
                    probe: marker,
                },
            )
            .await
            .unwrap();
        dispatched(&db.store, &a).await;
        let command = ProviderProbeReportCommand {
            owner: a.lease.owner.clone(),
            ticket: context.ticket,
            source_id: context.source_id,
            report_id: context.report_id,
            generation,
            report: ProviderReport {
                usage: None,
                reported_target: c.binding.target.clone(),
                provider_reference: None,
                provider_request_id: None,
                knowledge: ProviderKnowledge::Pending,
            },
        };
        db.store.report_probe(&worker(), &command).await.unwrap();
        assert!(
            inspect(&db.store, &c)
                .await
                .summary
                .provider
                .unwrap()
                .reconciliation
                .active_task_id
                .is_some()
        );
        report_task(&db.store, &a).await;
        drain_available(&db.store).await;
    }
    let detail = inspect(&db.store, &c).await;
    let s = detail.summary.provider.unwrap();
    assert_eq!(s.reconciliation.checks_used, 2);
    assert_eq!(
        s.reconciliation.stopped_reason,
        Some(ProviderRecoveryStopReason::BudgetExhausted)
    );
    assert!(s.reconciliation.next_due_at.is_none());
    let a = acquire(&db.store, "python").await;
    assert!(
        matches!(db.store.activation_context(&a.lease.owner).await.unwrap().wake,Some(WorkflowWake::ProviderOperation(w)) if matches!(w.selection,ProviderSelection::NeedsReview{reason:ProviderRecoveryStopReason::BudgetExhausted,..}))
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_collection_preserves_frozen_receipt_and_exact_canonical_bytes() {
    let (db, a, c) = fixture().await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    let command = reported(
        &c,
        &source,
        ProviderKnowledge::Succeeded {
            result: json!({"int":1,"float":1.0,"negative":-0.0,"nul":"\0"}),
        },
    );
    db.store.report_dispatch(&worker(), &command).await.unwrap();
    apply_decision(
        &db.store,
        &a,
        1,
        WorkflowAction::Complete {
            output: Value::Null,
        },
        &[],
    )
    .await;
    let before = serde_json::to_vec(&inspect(&db.store, &c).await).unwrap();
    collect_execution(&db).await;
    assert_eq!(
        before,
        serde_json::to_vec(&inspect(&db.store, &c).await).unwrap()
    );
    assert!(
        db.store
            .report_dispatch(&worker(), &command)
            .await
            .unwrap()
            .receipt()
            .already_accepted
    );
    assert!(db.store.claim_provider(&worker(), &c).await.is_err());
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_operator_failure_assessment_is_challenged_only_by_new_terminal_fact() {
    let (db, a, c) = fixture().await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    due(&db.store, &c).await;
    db.store
        .advance_provider_operations(&ProviderMaintenanceQuery {
            cursor: None,
            limit: 16,
        })
        .await
        .unwrap();
    let probe = acquire(&db.store, "recovery").await;
    let ctx = db
        .store
        .probe_context(
            &worker(),
            &ProviderProbeContextCommand {
                owner: probe.lease.owner.clone(),
                probe: probe.provider_probe.clone().unwrap(),
            },
        )
        .await
        .unwrap();
    let d = inspect(&db.store, &c).await;
    let resolution = EffectResolutionCommand {
        occurrence: c.occurrence(),
        resolution_id: "resolve-failure".into(),
        expected_effect_revision: d.summary.effect_revision,
        supersedes_resolution_id: None,
        action: c.action.clone(),
        assessment: EffectResolutionAssessment::RecordFailure {
            failure: ApplicationError {
                kind: "declined".into(),
                message: "declined by provider".into(),
            },
        },
        reason: "Examined provider console".into(),
        evidence_references: vec!["provider:review".into()],
    };
    db.store
        .resolve_effect(&operator(), &resolution)
        .await
        .unwrap();
    let first = reported(
        &c,
        &source,
        ProviderKnowledge::Failed {
            failure: ApplicationError {
                kind: "declined".into(),
                message: "declined by provider".into(),
            },
        },
    );
    let observation = ProviderObservationCommand {
        ticket: first.ticket,
        source_id: first.source_id,
        report_id: first.report_id,
        report: first.report,
    };
    db.store
        .observe_provider(&worker(), &observation)
        .await
        .unwrap();
    assert!(
        inspect(&db.store, &c)
            .await
            .summary
            .operator_resolution
            .unwrap()
            .first_conflicting_entry_sequence
            .is_none()
    );
    let late = ProviderObservationCommand {
        ticket: ctx.ticket,
        source_id: ctx.source_id,
        report_id: ctx.report_id,
        report: ProviderReport {
            usage: None,
            reported_target: c.binding.target.clone(),
            provider_reference: Some("remote-1".into()),
            provider_request_id: None,
            knowledge: ProviderKnowledge::Succeeded {
                result: json!({"done":true}),
            },
        },
    };
    db.store.observe_provider(&worker(), &late).await.unwrap();
    let d = inspect(&db.store, &c).await;
    assert!(
        d.summary
            .operator_resolution
            .unwrap()
            .first_conflicting_entry_sequence
            .is_some()
    );
    assert_eq!(
        d.summary.provider.unwrap().knowledge,
        ProviderKnowledgeKind::Disputed
    );
    apply_decision(&db.store, &a, 1, wait(&c, "assessed", None), &[]).await;
    let resumed = acquire(&db.store, "python").await;
    assert!(
        matches!(db.store.activation_context(&resumed.lease.owner).await.unwrap().wake,Some(WorkflowWake::ProviderOperation(w)) if matches!(w.selection,ProviderSelection::OperatorAssessment{outcome:EffectResolutionOutcome::Disputed{..},..}))
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_live_report_expiring_during_insert_rolls_back_but_late_observe_survives() {
    let (db, _, c) = fixture().await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    sqlx::raw_sql("CREATE FUNCTION delay_provider_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$; CREATE TRIGGER delay_provider_report BEFORE INSERT ON provider_reports FOR EACH ROW EXECUTE FUNCTION delay_provider_report()").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+150 WHERE attempt_id=$1").bind(&c.owner.attempt_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=(SELECT LEAST(expires_at_ms,authority_deadline_ms) FROM attempts WHERE attempt_id=$2) WHERE task_id=$1").bind(&c.owner.task_id).bind(&c.owner.attempt_id).execute(&db.store.pool).await.unwrap();
    let command = reported(
        &c,
        &source,
        ProviderKnowledge::Succeeded {
            result: json!({"finished":true}),
        },
    );
    assert!(matches!(
        db.store.report_dispatch(&worker(), &command).await,
        Err(ContractError::ObsoleteOperation)
    ));
    assert_eq!(inspect(&db.store, &c).await.summary.entry_count, 1);
    sqlx::query("DROP TRIGGER delay_provider_report ON provider_reports")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let observe = ProviderObservationCommand {
        ticket: command.ticket,
        source_id: command.source_id,
        report_id: command.report_id,
        report: command.report,
    };
    assert_eq!(
        db.store
            .observe_provider(&worker(), &observe)
            .await
            .unwrap()
            .receipt()
            .accepted_via,
        ProviderReportIngress::EvidenceOnly
    );
    db.finish().await;
}

pub(super) async fn collect_execution(db: &TestDb) {
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2 WHERE terminal_at_ms IS NOT NULL").execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    for _ in 0..500 {
        db.store
            .retain_batch(
                &scope(),
                &RetentionPolicy {
                    retain_for_ms: MIN_RETENTION_MS,
                    batch_size: 2,
                },
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
            .unwrap();
    }
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_runs")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

pub(super) async fn admitted_probe(
    db: &TestDb,
    c: &ProviderClaimCommand,
) -> (Assignment, ProviderProbeContext) {
    due(&db.store, c).await;
    assert_eq!(
        db.store
            .advance_provider_operations(&ProviderMaintenanceQuery {
                cursor: None,
                limit: 16
            })
            .await
            .unwrap()
            .allocated,
        1
    );
    let probe = acquire(&db.store, "recovery").await;
    let context = db
        .store
        .probe_context(
            &worker(),
            &ProviderProbeContextCommand {
                owner: probe.lease.owner.clone(),
                probe: probe.provider_probe.clone().unwrap(),
            },
        )
        .await
        .unwrap();
    dispatched(&db.store, &probe).await;
    (probe, context)
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_late_conflicting_terminal_never_reopens_handling_before_or_after_collection() {
    for after_collection in [false, true] {
        let (db, a, c) = fixture().await;
        let ProviderClaimReceipt::Dispatch { source, .. } =
            db.store.claim_provider(&worker(), &c).await.unwrap()
        else {
            panic!()
        };
        let (probe, context) = admitted_probe(&db, &c).await;
        let success = ProviderProbeReportCommand {
            owner: probe.lease.owner.clone(),
            ticket: context.ticket,
            source_id: context.source_id,
            report_id: context.report_id,
            generation: context.generation,
            report: ProviderReport {
                usage: None,
                reported_target: c.binding.target.clone(),
                provider_reference: Some("remote-1".into()),
                provider_request_id: None,
                knowledge: ProviderKnowledge::Succeeded {
                    result: json!({"done":true}),
                },
            },
        };
        db.store.report_probe(&worker(), &success).await.unwrap();
        report_task(&db.store, &probe).await;
        drain_available(&db.store).await;
        let failure = reported(
            &c,
            &source,
            ProviderKnowledge::Failed {
                failure: ApplicationError {
                    kind: "provider_rejected".into(),
                    message: "retained conflicting fact".into(),
                },
            },
        );
        let observation = ProviderObservationCommand {
            ticket: failure.ticket,
            source_id: failure.source_id,
            report_id: failure.report_id,
            report: failure.report,
        };
        if !after_collection {
            db.store
                .observe_provider(&worker(), &observation)
                .await
                .unwrap();
        }
        apply_decision(
            &db.store,
            &a,
            1,
            WorkflowAction::Complete {
                output: Value::Null,
            },
            &[],
        )
        .await;
        collect_execution(&db).await;
        if after_collection {
            db.store
                .observe_provider(&worker(), &observation)
                .await
                .unwrap();
        }
        let detail = inspect(&db.store, &c).await;
        let provider = detail.summary.provider.unwrap();
        assert_eq!(provider.knowledge, ProviderKnowledgeKind::Disputed);
        assert_eq!(provider.handling, ProviderHandlingState::ProviderTerminal);
        assert_eq!(provider.conflicting_reports.len(), 2);
        assert!(detail.summary.operator_resolution.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_runs")
                .fetch_one(&db.store.pool)
                .await
                .unwrap(),
            0
        );
        assert!(
            db.store
                .observe_provider(&worker(), &observation)
                .await
                .unwrap()
                .receipt()
                .already_accepted
        );
        db.finish().await;
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_concurrent_nonterminal_reports_preserve_highest_knowledge_rank() {
    for (high, low, expected) in [
        (
            ProviderKnowledge::Pending,
            ProviderKnowledge::Unknown {
                reason: "not yet visible".into(),
            },
            ProviderKnowledgeKind::Pending,
        ),
        (
            ProviderKnowledge::RequiresAction {
                action_kind: "user_confirmation".into(),
            },
            ProviderKnowledge::Pending,
            ProviderKnowledgeKind::RequiresAction,
        ),
        (
            ProviderKnowledge::RequiresAction {
                action_kind: "user_confirmation".into(),
            },
            ProviderKnowledge::Unknown {
                reason: "stale replica".into(),
            },
            ProviderKnowledgeKind::RequiresAction,
        ),
    ] {
        let (db, _, c) = fixture().await;
        let ProviderClaimReceipt::Dispatch { source, .. } =
            db.store.claim_provider(&worker(), &c).await.unwrap()
        else {
            panic!()
        };
        let (_, context) = admitted_probe(&db, &c).await;
        let high = reported(&c, &source, high);
        let low = ProviderObservationCommand {
            ticket: context.ticket,
            source_id: context.source_id,
            report_id: context.report_id,
            report: ProviderReport {
                usage: None,
                reported_target: c.binding.target.clone(),
                provider_reference: None,
                provider_request_id: None,
                knowledge: low,
            },
        };
        let p = worker();
        let (one, two) = tokio::join!(
            db.store.report_dispatch(&p, &high),
            db.store.observe_provider(&p, &low)
        );
        one.unwrap();
        two.unwrap();
        let detail = inspect(&db.store, &c).await;
        let provider = detail.summary.provider.unwrap();
        assert_eq!(provider.knowledge, expected);
        assert_eq!(provider.selected_report.unwrap().report_id, high.report_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM provider_reports")
                .fetch_one(&db.store.pool)
                .await
                .unwrap(),
            2
        );
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn ordinary_execution_avoids_provider_ledger_queries_and_wait_writes() {
    for enabled in [false, true] {
        let db = TestDb::new().await;
        if enabled {
            bind(&db.store, ProviderDispatchPolicy::Enabled).await;
        }
        let mut blocker = db.store.pool.begin().await.unwrap();
        // Any explicit read/write of a provider ledger blocks. The native wait
        // table permits FK relation metadata access required even for a NULL
        // anchor, but prohibits wait writes. This does not prove zero FK cost
        // or independently rule out wait-table reads; source paths cover those.
        sqlx::query("LOCK TABLE provider_operations,provider_probe_tasks,provider_invocations,provider_reports,provider_terminal_facts IN ACCESS EXCLUSIVE MODE").execute(&mut *blocker).await.unwrap();
        sqlx::query("LOCK TABLE workflow_provider_waits IN SHARE ROW EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut c = command();
            c.input.queue = "standalone".into();
            let ordinary = db
                .store
                .accept_resolved_submission(&c, &descriptor())
                .await
                .unwrap();
            let task = acquire(&db.store, "standalone").await;
            dispatched(&db.store, &task).await;
            db.store
                .settle(&completed(&task, Quiescence::Confirmed, Value::Null))
                .await
                .unwrap();
            db.store.result(&scope(), &ordinary.task_id).await.unwrap();
            let workflow = start(&db.store).await;
            let a = acquire(&db.store, "python").await;
            dispatched(&db.store, &a).await;
            db.store
                .record_local_result(&local(&a, "ordinary"))
                .await
                .unwrap();
            db.store
                .inspect(&scope(), &a.lease.owner.task_id)
                .await
                .unwrap();
            apply_decision(
                &db.store,
                &a,
                0,
                WorkflowAction::Complete {
                    output: json!("ordinary"),
                },
                &[],
            )
            .await;
            assert_eq!(
                db.store
                    .workflow_status(&scope(), &workflow.workflow_id)
                    .await
                    .unwrap()
                    .state,
                WorkflowState::Succeeded
            );
            let session = db.store.open_session(&scope(), "empty", 1).await.unwrap();
            db.store
                .acquire(&acquire_command(&session, 0, 1))
                .await
                .unwrap();
        })
        .await;
        blocker.rollback().await.unwrap();
        completed.expect("ordinary execution queried a provider ledger or wrote a native wait");
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_due_queries_seek_past_one_hundred_thousand_sleeping_operations() {
    use sqlx::AssertSqlSafe;
    let db = TestDb::new().await;
    // Relational query-shape fixture, not fabricated runtime evidence: no public
    // decoder or scheduler mutation reads these intentionally minimal payloads.
    sqlx::raw_sql("INSERT INTO approved_effect_occurrences(tenant_id,namespace,workflow_id,activation_id,approval_key,journal_state,has_discrepancy,effect_revision,entry_count,created_at_ms,updated_at_ms,binding_bytes,summary_bytes,approval_bytes,program_bytes) SELECT 'acme','billing','w'||lpad(n::text,6,'0'),'a','key','unconfirmed',false,1,0,0,0,'{}'::bytea,'{}'::bytea,'{}'::bytea,'{}'::bytea FROM generate_series(1,102032)n; INSERT INTO provider_operations(tenant_id,namespace,workflow_id,activation_id,approval_key,instance_id,binding_digest,business_namespace,business_key,first_due_at_ms,deadline_at_ms,next_due_at_ms,active_probe_id,active_task_id,stopped_reason,knowledge,handling,state_bytes) SELECT 'acme','billing','w'||lpad(n::text,6,'0'),'a','key','instance','sha256:'||repeat('a',64),'business','key'||n,0,20000000,CASE WHEN n<=32 THEN 1000 WHEN n<=100032 THEN 20000000 ELSE NULL END,CASE WHEN n>32 AND n<=100032 AND n%2=0 THEN 'probe'||n END,CASE WHEN n>32 AND n<=100032 AND n%2=0 THEN 'task'||n END,CASE WHEN n>100032 THEN 'workflow_cancelled' END,'unknown','unresolved','{}'::bytea FROM generate_series(1,102032)n; ANALYZE provider_operations;").execute(&db.store.pool).await.unwrap();
    let mut connection = db.store.pool.acquire().await.unwrap();
    sqlx::query("SET plan_cache_mode=force_generic_plan")
        .execute(&mut *connection)
        .await
        .unwrap();
    for after in [None, Some(16), Some(32)] {
        let query = if after.is_some() {
            super::super::provider::scheduler::DUE_AFTER_SQL
        } else {
            super::super::provider::scheduler::DUE_SQL
        };
        let mut q = sqlx::query_scalar::<_, serde_json::Value>(AssertSqlSafe(format!(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {query}"
        )))
        .bind("acme")
        .bind("billing")
        .bind(2000_i64);
        if let Some(n) = after {
            q = q
                .bind(1000_i64)
                .bind(format!("w{n:06}"))
                .bind("a")
                .bind("key");
        }
        let plan = q.bind(16_i64).fetch_one(&mut *connection).await.unwrap();
        let limit = &plan[0]["Plan"];
        let index = &limit["Plans"][0];
        assert_eq!(limit["Node Type"], "Limit");
        assert!(
            matches!(
                index["Node Type"].as_str(),
                Some("Index Only Scan" | "Index Scan")
            ),
            "{plan}"
        );
        assert_eq!(index["Index Name"], "provider_operations_due");
        assert_eq!(
            limit["Actual Rows"].as_f64().unwrap(),
            if after == Some(32) { 0.0 } else { 16.0 }
        );
        assert!(index["Actual Rows"].as_f64().unwrap() <= 16.0);
        assert_eq!(index["Rows Removed by Filter"].as_f64().unwrap_or(0.0), 0.0);
        let condition = index["Index Cond"].as_str().unwrap();
        assert!(condition.contains("next_due_at_ms <="), "{condition}");
        if after.is_some() {
            assert!(
                condition.contains("ROW(next_due_at_ms") && condition.contains(" > "),
                "{condition}"
            );
        }
        let blocks = limit["Shared Hit Blocks"].as_f64().unwrap()
            + limit["Shared Read Blocks"].as_f64().unwrap();
        assert!(
            blocks < 128.0,
            "bounded page touched {blocks} blocks: {plan}"
        );
        println!("provider due after {after:?}: {plan}");
    }
    drop(connection);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn provider_nonterminal_reference_conflict_stops_checks_and_terminal_later_closes_handling() {
    let (db, _, c) = fixture().await;
    let ProviderClaimReceipt::Dispatch { source, .. } =
        db.store.claim_provider(&worker(), &c).await.unwrap()
    else {
        panic!()
    };
    let (first, ctx) = admitted_probe(&db, &c).await;
    let pending = ProviderProbeReportCommand {
        owner: first.lease.owner.clone(),
        ticket: ctx.ticket,
        source_id: ctx.source_id,
        report_id: ctx.report_id,
        generation: ctx.generation,
        report: ProviderReport {
            usage: None,
            reported_target: c.binding.target.clone(),
            provider_reference: Some("reference-a".into()),
            provider_request_id: None,
            knowledge: ProviderKnowledge::Pending,
        },
    };
    db.store.report_probe(&worker(), &pending).await.unwrap();
    report_task(&db.store, &first).await;
    drain_available(&db.store).await;
    let (_, second) = admitted_probe(&db, &c).await;
    let mut conflict = reported(
        &c,
        &source,
        ProviderKnowledge::Unknown {
            reason: "different resource".into(),
        },
    );
    conflict.report.provider_reference = Some("reference-b".into());
    db.store
        .report_dispatch(&worker(), &conflict)
        .await
        .unwrap();
    let d = inspect(&db.store, &c).await;
    let s = d.summary.provider.unwrap();
    assert_eq!(s.knowledge, ProviderKnowledgeKind::Disputed);
    assert_eq!(s.handling, ProviderHandlingState::Unresolved);
    assert_eq!(
        s.reconciliation.stopped_reason,
        Some(ProviderRecoveryStopReason::Disputed)
    );
    assert!(s.reconciliation.next_due_at.is_none());
    let refs = s.conflicting_reports;
    let terminal = ProviderObservationCommand {
        ticket: second.ticket,
        source_id: second.source_id,
        report_id: second.report_id,
        report: ProviderReport {
            usage: None,
            reported_target: c.binding.target.clone(),
            provider_reference: Some("reference-a".into()),
            provider_request_id: None,
            knowledge: ProviderKnowledge::Succeeded {
                result: Value::Null,
            },
        },
    };
    db.store
        .observe_provider(&worker(), &terminal)
        .await
        .unwrap();
    let d = inspect(&db.store, &c).await;
    let s = d.summary.provider.unwrap();
    assert_eq!(s.knowledge, ProviderKnowledgeKind::Disputed);
    assert_eq!(s.handling, ProviderHandlingState::ProviderTerminal);
    assert_eq!(s.conflicting_reports, refs);
    let detail = inspect(&db.store, &c).await;
    db.store
        .resolve_effect(
            &operator(),
            &EffectResolutionCommand {
                occurrence: c.occurrence(),
                resolution_id: "reviewed-references".into(),
                expected_effect_revision: detail.summary.effect_revision,
                supersedes_resolution_id: None,
                action: c.action.clone(),
                assessment: EffectResolutionAssessment::CloseUnknown,
                reason: "Reviewed the reference discrepancy without asserting an outcome".into(),
                evidence_references: vec![],
            },
        )
        .await
        .unwrap();
    let detail = inspect(&db.store, &c).await;
    assert!(!ProviderInboxFilter::NeedsReview.matches(&detail.summary));
    assert!(ProviderInboxFilter::Disputed.matches(&detail.summary));
    let review = db
        .store
        .effect_inbox(&EffectInboxQuery {
            scope: scope(),
            workflow_id: Some(c.workflow_id.clone()),
            filter: EffectInboxFilter::All,
            provider_filter: Some(ProviderInboxFilter::NeedsReview),
            cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(review.items.is_empty());
    db.finish().await;
}
