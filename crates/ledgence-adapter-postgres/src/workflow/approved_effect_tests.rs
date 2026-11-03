use super::*;
use sqlx::migrate::Migrator;

// Each fixture has its own queue so the actor acquiring a new activation is explicit.
pub(super) async fn request(store: &PostgresStore, queue: &str) -> ApprovalDecisionCommand {
    request_with_protection(
        store,
        queue,
        "approve-send",
        ApprovalProtection::Unspecified,
    )
    .await
}

async fn request_with_protection(
    store: &PostgresStore,
    queue: &str,
    key: &str,
    protection: ApprovalProtection,
) -> ApprovalDecisionCommand {
    let mut submission = command();
    submission.input.queue = queue.into();
    submission.idempotency_key = queue.into();
    let workflow = store
        .accept_resolved_workflow(&submission, &descriptor())
        .await
        .unwrap();
    let initial = acquire(store, queue).await;
    let mut decision = approvals::decide(&workflow, &initial, key);
    decision.protection = protection;
    let mut waiting = approvals::wait(&decision.key, 180_000);
    if let WorkflowAction::Wait {
        wait: WorkflowWait::Approval {
            protection: selected,
            ..
        },
        ..
    } = &mut waiting
    {
        *selected = protection;
    }
    apply_decision(store, &initial, 0, waiting, &[]).await;
    decision
}

pub(super) async fn resume(
    store: &PostgresStore,
    queue: &str,
    decision: &ApprovalDecisionCommand,
) -> (Assignment, ApprovedEffectClaimCommand) {
    store.decide_approval(decision).await.unwrap();
    activate(store, queue, decision).await
}

pub(super) async fn activate(
    store: &PostgresStore,
    queue: &str,
    decision: &ApprovalDecisionCommand,
) -> (Assignment, ApprovedEffectClaimCommand) {
    store.apply_work(&one_work(store).await, &[]).await.unwrap();
    let assignment = acquire(store, queue).await;
    let claim = ApprovedEffectClaimCommand {
        owner: assignment.lease.owner.clone(),
        workflow_id: decision.workflow_id.clone(),
        approval_key: decision.key.clone(),
        action: decision.action.clone(),
        lookup: "app:lookup_send".into(),
    };
    (assignment, claim)
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn concurrent_claims_lost_reply_and_replacement_attempt_never_regrant_dispatch() {
    let db = TestDb::new().await;
    let decision = request(&db.store, "race").await;
    let (first, claim) = resume(&db.store, "race", &decision).await;
    assert!(matches!(
        db.store.claim_approved_effect(&claim).await,
        Err(ContractError::OwnershipLost)
    ));
    dispatched(&db.store, &first).await;

    let other = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let (one, two) = tokio::join!(
        db.store.claim_approved_effect(&claim),
        other.claim_approved_effect(&claim)
    );
    let receipts = [one.unwrap(), two.unwrap()];
    assert!(receipts.iter().all(|r| r.matches(&claim).unwrap()));
    assert_eq!(
        receipts
            .iter()
            .filter(|r| r.disposition == ApprovedEffectDisposition::Dispatch)
            .count(),
        1
    );
    // Losing an acknowledged response and repeating the call on the same lease
    // must not restore dispatch permission, even through another DB connection.
    assert_eq!(
        other
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::LookupOnly
    );
    owned::failed_attempt(&other, &first).await;
    let second = acquire(&other, "race").await;
    assert_eq!(first.lease.owner.task_id, second.lease.owner.task_id);
    assert_ne!(first.lease.owner.attempt_id, second.lease.owner.attempt_id);
    dispatched(&other, &second).await;
    assert!(matches!(
        other.claim_approved_effect(&claim).await,
        Err(ContractError::OwnershipLost)
    ));
    let replacement = ApprovedEffectClaimCommand {
        owner: second.lease.owner.clone(),
        ..claim.clone()
    };
    let recovered = other.claim_approved_effect(&replacement).await.unwrap();
    assert!(recovered.matches(&replacement).unwrap());
    assert_eq!(recovered.disposition, ApprovedEffectDisposition::LookupOnly);

    let recorded: Option<String> = sqlx::query_scalar("SELECT effect_dispatch_attempt_id FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2")
        .bind(&decision.workflow_id).bind(&decision.key).fetch_one(&other.pool).await.unwrap();
    assert_eq!(recorded.as_ref(), Some(&first.lease.owner.attempt_id));

    other
        .cancel_workflow(&scope(), &decision.workflow_id)
        .await
        .unwrap();
    assert!(matches!(
        other.claim_approved_effect(&replacement).await,
        Err(ContractError::OwnershipLost)
    ));
    other.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn a_claim_is_bound_to_its_approval_action_and_live_resumed_activation() {
    let db = TestDb::new().await;
    let decision = request(&db.store, "binding").await;
    let (resumed, claim) = resume(&db.store, "binding", &decision).await;
    dispatched(&db.store, &resumed).await;
    for (field, value) in [
        ("name", json!("unapproved")),
        ("version", json!("2")),
        ("arguments", json!({"count":1.0})),
    ] {
        let mut changed = serde_json::to_value(&claim).unwrap();
        changed["action"][field] = value;
        let changed = serde_json::from_value(changed).unwrap();
        assert!(matches!(
            db.store.claim_approved_effect(&changed).await,
            Err(ContractError::Conflict)
        ));
    }
    let mut foreign = claim.clone();
    foreign.workflow_id = "some-other-workflow".into();
    assert!(matches!(
        db.store.claim_approved_effect(&foreign).await,
        Err(ContractError::Conflict)
    ));
    foreign = claim.clone();
    foreign.owner.scope.tenant_id = "other".into();
    assert!(matches!(
        db.store.claim_approved_effect(&foreign).await,
        Err(ContractError::NotFound)
    ));
    foreign = claim.clone();
    foreign.approval_key = "unknown".into();
    assert!(matches!(
        db.store.claim_approved_effect(&foreign).await,
        Err(ContractError::NotFound)
    ));

    // A new approval in the same workflow does not allow reuse of its previous
    // approved wake, even with the same scoped workflow ID and immutable action.
    let mut next = approvals::decide(
        &db.store
            .workflow_status(&scope(), &claim.workflow_id)
            .await
            .unwrap(),
        &resumed,
        "again",
    );
    next.revision = 1;
    apply_decision(
        &db.store,
        &resumed,
        1,
        approvals::wait(&next.key, 180_000),
        &[],
    )
    .await;
    next.decision = ApprovalDecision::Reject;
    let (rejected, rejected_claim) = resume(&db.store, "binding", &next).await;
    dispatched(&db.store, &rejected).await;
    assert!(matches!(
        db.store.claim_approved_effect(&rejected_claim).await,
        Err(ContractError::ObsoleteOperation)
    ));
    let stale_wake = ApprovedEffectClaimCommand {
        owner: rejected.lease.owner,
        ..claim
    };
    assert!(matches!(
        db.store.claim_approved_effect(&stale_wake).await,
        Err(ContractError::ObsoleteOperation)
    ));
    let consumed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_approvals WHERE effect_dispatch_claimed")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(consumed, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn upgrading_fences_previously_approved_effects_and_preserves_pending_eligibility() {
    let mut db = TestDb::without_migrations().await;
    let previous = Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|m| m.version < 20261102020000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    db.begin_legacy_workflow_seed().await;
    let decision = request(&db.store, "old-approved").await;
    let (old, old_claim) = resume(&db.store, "old-approved", &decision).await;
    let pending = request(&db.store, "old-pending").await;
    let queued = request(&db.store, "old-approved-before-resume").await;
    db.store.decide_approval(&queued).await.unwrap();
    db.finish_legacy_workflow_seed().await;
    db.store.migrate().await.unwrap();
    // Upgrade deploys stop old writers and reopen pools with the new row shape.
    db.store.close().await;
    db.store = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    db.store.verify_schema().await.unwrap();
    dispatched(&db.store, &old).await;
    assert_eq!(
        db.store
            .claim_approved_effect(&old_claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::LookupOnly
    );
    let recorded: Option<String> = sqlx::query_scalar("SELECT effect_dispatch_attempt_id FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2")
        .bind(&old_claim.workflow_id).bind(&old_claim.approval_key).fetch_one(&db.store.pool).await.unwrap();
    assert!(recorded.is_none());
    let (delayed, claim) = activate(&db.store, "old-approved-before-resume", &queued).await;
    dispatched(&db.store, &delayed).await;
    assert_eq!(
        db.store
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::LookupOnly
    );

    let (new, claim) = resume(&db.store, "old-pending", &pending).await;
    dispatched(&db.store, &new).await;
    assert_eq!(
        db.store
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::Dispatch
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cancellation_and_lease_expiry_fence_even_the_first_claim() {
    let db = TestDb::new().await;
    for queue in ["expired-lease", "cancelled-workflow"] {
        let decision = request(&db.store, queue).await;
        let (activation, claim) = resume(&db.store, queue, &decision).await;
        dispatched(&db.store, &activation).await;
        if queue == "expired-lease" {
            sqlx::query("UPDATE attempts SET expires_at_ms=0 WHERE attempt_id=$1")
                .bind(&claim.owner.attempt_id)
                .execute(&db.store.pool)
                .await
                .unwrap();
        } else {
            db.store
                .cancel_workflow(&scope(), &decision.workflow_id)
                .await
                .unwrap();
        }
        assert!(matches!(
            db.store.claim_approved_effect(&claim).await,
            Err(ContractError::OwnershipLost)
        ));
    }
    let consumed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_approvals WHERE effect_dispatch_claimed")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(consumed, 0);
    db.finish().await;
}

#[path = "approved_effect_confirm_tests.rs"]
mod confirmations;

#[path = "effect_evidence_tests.rs"]
mod evidence_v2;
