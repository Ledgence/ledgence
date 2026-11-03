use super::*;

const KEY: &str = "_approval:af6d14bbf712558f51a5fca2276b0e154f6289c9f8e46df79f404069895aaaec";

fn confirmation(claim: &ApprovedEffectClaimCommand) -> ApprovedEffectConfirmCommand {
    assert_eq!(claim.approval_key, "approve-send");
    ApprovedEffectConfirmCommand {
        owner: claim.owner.clone(),
        workflow_id: claim.workflow_id.clone(),
        approval_key: claim.approval_key.clone(),
        lookup: claim.lookup.clone(),
        record: LocalStepRecord {
            key: KEY.into(),
            callable: claim.action.name.clone(),
            input: json!({"v":1, "action_json":serde_json::to_string(&claim.action).unwrap(),"lookup":claim.lookup}),
            output: json!({"receipt":"provider-r42","amount":9007199254740993_u64}),
        },
    }
}

fn generic(command: &ApprovedEffectConfirmCommand) -> LocalResultCommand {
    LocalResultCommand {
        owner: command.owner.clone(),
        record: command.record.clone(),
    }
}

async fn state(
    db: &TestDb,
    claim: &ApprovedEffectClaimCommand,
) -> (Option<String>, Option<String>, i64) {
    sqlx::query_as(
        "SELECT effect_evidence_state,effect_confirmed_attempt_id,
                (SELECT count(*) FROM workflow_local_results WHERE activation_id=$3 AND step_key=$4)
         FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2",
    )
    .bind(&claim.workflow_id)
    .bind(&claim.approval_key)
    .bind(&claim.owner.task_id)
    .bind(KEY)
    .fetch_one(&db.store.pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn a_suppressed_grant_write_must_never_return_dispatch_permission() {
    let db = TestDb::new().await;
    let decision = request(&db.store, "missing-write").await;
    let (worker, claim) = resume(&db.store, "missing-write", &decision).await;
    dispatched(&db.store, &worker).await;
    sqlx::query("CREATE FUNCTION suppress_grant() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'")
        .execute(&db.store.pool).await.unwrap();
    sqlx::query("CREATE TRIGGER suppress_grant BEFORE UPDATE OF effect_dispatch_claimed ON workflow_approvals
                 FOR EACH ROW EXECUTE FUNCTION suppress_grant()").execute(&db.store.pool).await.unwrap();
    for _ in 0..2 {
        assert!(db.store.claim_approved_effect(&claim).await.is_err());
    }
    assert_eq!(state(&db, &claim).await, (None, None, 0));
    sqlx::query("DROP TRIGGER suppress_grant ON workflow_approvals")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::Dispatch
    );
    assert_eq!(
        db.store
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::LookupOnly
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn confirmation_requires_a_bound_grant_and_cannot_be_forged_through_the_generic_journal() {
    let db = TestDb::new().await;
    let store: &dyn WorkflowStore = &db.store;
    let effects = store.approved_effects().unwrap();
    let review = request(&db.store, "effect-binding").await;
    let (worker, claim) = resume(&db.store, "effect-binding", &review).await;
    dispatched(&db.store, &worker).await;
    let confirm = confirmation(&claim);
    // Even before the first grant, a generic commit must not seed a matching SDK replay.
    assert!(matches!(
        db.store.record_local_result(&generic(&confirm)).await,
        Err(ContractError::Conflict)
    ));
    assert!(matches!(
        effects.confirm_approved_effect(&confirm).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(state(&db, &claim).await, (None, None, 0));
    assert_eq!(
        effects
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::Dispatch
    );
    let rebind = ApprovedEffectClaimCommand {
        lookup: "app:other_lookup".into(),
        ..claim.clone()
    };
    assert!(matches!(
        effects.claim_approved_effect(&rebind).await,
        Err(ContractError::Conflict)
    ));
    let mut poison = generic(&confirm);
    poison.record.input = json!({"hearsay":true});
    assert!(matches!(
        db.store.record_local_result(&poison).await,
        Err(ContractError::Conflict)
    ));
    let mut wrong_key = confirm.clone();
    wrong_key.record.key = format!("_approval:{}", "a".repeat(64));
    let mut wrong_name = confirm.clone();
    wrong_name.record.callable = "other:send".into();
    let mut wrong_lookup = confirm.clone();
    wrong_lookup.lookup = "app:attacker".into();
    wrong_lookup.record.input["lookup"] = json!(wrong_lookup.lookup);
    let mut wrong_occurrence = confirm.clone();
    wrong_occurrence.workflow_id = "different".into();
    let mut wrong_v = confirm.clone();
    wrong_v.record.input["v"] = json!(1.0);
    let mut extra = confirm.clone();
    extra.record.input["claimed_by"] = json!("remote");
    let mut wrong_action = confirm.clone();
    let mut changed = review.action.clone();
    changed.arguments["count"] = json!(1.0);
    wrong_action.record.input["action_json"] = json!(serde_json::to_string(&changed).unwrap());
    let mut duplicate = confirm.clone();
    let duplicate_json =
        serde_json::to_string(&review.action)
            .unwrap()
            .replacen('{', r#"{"name":"unapproved","#, 1);
    duplicate.record.input["action_json"] = json!(duplicate_json);
    for invalid in [
        wrong_key,
        wrong_name,
        wrong_lookup,
        wrong_occurrence,
        wrong_v,
        extra,
        wrong_action,
        duplicate,
    ] {
        assert!(effects.confirm_approved_effect(&invalid).await.is_err());
    }
    assert_eq!(state(&db, &claim).await, (Some("open".into()), None, 0));

    // Failure after the journal insertion and evidence UPDATE tests actual rollback.
    sqlx::query("ALTER TABLE workflow_explorer_records ADD CONSTRAINT reject_local_for_test CHECK (kind <> 'local')").execute(&db.store.pool).await.unwrap();
    assert!(effects.confirm_approved_effect(&confirm).await.is_err());
    assert_eq!(state(&db, &claim).await, (Some("open".into()), None, 0));
    sqlx::query("ALTER TABLE workflow_explorer_records DROP CONSTRAINT reject_local_for_test")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let receipt = effects.confirm_approved_effect(&confirm).await.unwrap();
    assert!(receipt.matches(&confirm).unwrap());
    assert!(!receipt.already_accepted);
    assert_eq!(
        state(&db, &claim).await,
        (
            Some("confirmed".into()),
            Some(claim.owner.attempt_id.clone()),
            1
        )
    );
    assert!(matches!(
        db.store.record_local_result(&generic(&confirm)).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(
        db.store
            .activation_context(&claim.owner)
            .await
            .unwrap()
            .local_steps,
        vec![confirm.record]
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn replacement_reconciles_once_and_only_the_accepting_attempt_can_replay_after_cancellation()
{
    let db = TestDb::new().await;
    let review = request(&db.store, "reconcile").await;
    let (first, claim) = resume(&db.store, "reconcile", &review).await;
    dispatched(&db.store, &first).await;
    db.store.claim_approved_effect(&claim).await.unwrap();
    let old_confirmation = confirmation(&claim);
    owned::failed_attempt(&db.store, &first).await;
    let second = acquire(&db.store, "reconcile").await;
    dispatched(&db.store, &second).await;
    let replacement = ApprovedEffectClaimCommand {
        owner: second.lease.owner.clone(),
        ..claim.clone()
    };
    assert_eq!(
        db.store
            .claim_approved_effect(&replacement)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::LookupOnly
    );
    assert!(matches!(
        db.store.confirm_approved_effect(&old_confirmation).await,
        Err(ContractError::OwnershipLost)
    ));
    let one = confirmation(&replacement);
    let mut two = one.clone();
    two.record.output["receipt"] = json!("disagrees");
    let (a, b) = tokio::join!(
        db.store.confirm_approved_effect(&one),
        db.store.confirm_approved_effect(&two)
    );
    let accepted = match (a, b) {
        (Ok(receipt), Err(ContractError::Conflict)) if !receipt.already_accepted => &one,
        (Err(ContractError::Conflict), Ok(receipt)) if !receipt.already_accepted => &two,
        other => panic!("expected exactly one accepted provider receipt: {other:?}"),
    };
    let row: (String,String) = sqlx::query_as("SELECT effect_dispatch_attempt_id,effect_confirmed_attempt_id FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2")
        .bind(&review.workflow_id).bind(&review.key).fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(
        row,
        (
            claim.owner.attempt_id.clone(),
            replacement.owner.attempt_id.clone()
        )
    );
    // A live later attempt may replay, but does not become the original accepting owner.
    owned::failed_attempt(&db.store, &second).await;
    let third = acquire(&db.store, "reconcile").await;
    dispatched(&db.store, &third).await;
    let replay = ApprovedEffectConfirmCommand {
        owner: third.lease.owner.clone(),
        ..accepted.clone()
    };
    assert!(
        db.store
            .confirm_approved_effect(&replay)
            .await
            .unwrap()
            .already_accepted
    );
    db.store
        .cancel_workflow(&scope(), &claim.workflow_id)
        .await
        .unwrap();
    assert!(
        db.store
            .confirm_approved_effect(accepted)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store.confirm_approved_effect(&replay).await,
        Err(ContractError::OwnershipLost)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn ordinary_approved_local_result_excludes_a_later_protected_dispatch() {
    let db = TestDb::new().await;
    let review = request(&db.store, "ordinary").await;
    let (worker, claim) = resume(&db.store, "ordinary", &review).await;
    dispatched(&db.store, &worker).await;
    let mut local = generic(&confirmation(&claim));
    local.record.input = review.action.arguments.clone();
    assert!(
        !db.store
            .record_local_result(&local)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        db.store
            .record_local_result(&local)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        db.store.claim_approved_effect(&claim).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(state(&db, &claim).await, (None, None, 1));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn typed_protection_rejects_downgrade_and_plain_journal_before_a_grant() {
    let db = TestDb::new().await;
    let decision = request_with_protection(
        &db.store,
        "typed-effect",
        "approve-send",
        ApprovalProtection::ExternalEffect,
    )
    .await;
    let mut downgraded = decision.clone();
    downgraded.protection = ApprovalProtection::Unspecified;
    assert!(matches!(
        db.store.decide_approval(&downgraded).await,
        Err(ContractError::Conflict)
    ));
    let pending = db
        .store
        .approval(&scope(), &decision.workflow_id, &decision.key)
        .await
        .unwrap();
    assert_eq!(pending.protection, ApprovalProtection::ExternalEffect);
    assert_eq!(pending.status, ApprovalStatus::Pending);
    assert!(pending.resumed_activation_id.is_none());

    let (worker, claim) = resume(&db.store, "typed-effect", &decision).await;
    dispatched(&db.store, &worker).await;
    let confirmation = confirmation(&claim);
    // approved_local sends the approved arguments without any protected marker.
    // The stored request must reject that record even before dispatch is claimed.
    let mut plain = generic(&confirmation);
    plain.record.input = decision.action.arguments.clone();
    assert!(plain.record.input.get("action_json").is_none());
    assert!(matches!(
        db.store.record_local_result(&plain).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(state(&db, &claim).await, (None, None, 0));
    // The protection applies to this approved occurrence; ordinary internal
    // work in the same activation still uses its normal journal contract.
    let internal = local(&worker, "prepare-audit-note");
    assert!(
        !db.store
            .record_local_result(&internal)
            .await
            .unwrap()
            .already_accepted
    );

    assert_eq!(
        db.store
            .claim_approved_effect(&claim)
            .await
            .unwrap()
            .disposition,
        ApprovedEffectDisposition::Dispatch
    );
    assert!(
        !db.store
            .confirm_approved_effect(&confirmation)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        db.store
            .confirm_approved_effect(&confirmation)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(
        state(&db, &claim).await,
        (
            Some("confirmed".into()),
            Some(claim.owner.attempt_id.clone()),
            1
        )
    );
    let context = db.store.activation_context(&claim.owner).await.unwrap();
    let Some(WorkflowWake::Approval { approval }) = context.wake else {
        panic!("protected approval wake expected");
    };
    assert_eq!(approval.protection, ApprovalProtection::ExternalEffect);
    assert!(
        context
            .local_steps
            .iter()
            .any(|record| record.matches(&confirmation.record).unwrap())
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn decision_cannot_reclassify_an_existing_generic_approval() {
    let db = TestDb::new().await;
    let decision = request(&db.store, "generic-existing").await;
    let mut changed = decision.clone();
    changed.protection = ApprovalProtection::ExternalEffect;
    assert!(matches!(
        db.store.decide_approval(&changed).await,
        Err(ContractError::Conflict)
    ));
    let accepted = db.store.decide_approval(&decision).await.unwrap();
    assert_eq!(
        accepted.approval.protection,
        ApprovalProtection::Unspecified
    );
    assert!(!accepted.already_accepted);
    assert!(
        db.store
            .decide_approval(&decision)
            .await
            .unwrap()
            .already_accepted
    );
    let (stored,): (Vec<u8>,) = sqlx::query_as(
        "SELECT decision_bytes FROM workflow_approvals WHERE workflow_id=$1 AND wait_key=$2",
    )
    .bind(&decision.workflow_id)
    .bind(&decision.key)
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(stored, codec::encode(&decision).unwrap());
    assert!(
        !serde_json::from_slice::<Value>(&stored)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("protection")
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn protected_occurrence_keeps_literal_unicode_escape_text_in_its_key() {
    let db = TestDb::new().await;
    let decision = request_with_protection(
        &db.store,
        "literal-key",
        r"approve\u0000send",
        ApprovalProtection::ExternalEffect,
    )
    .await;
    let (worker, _) = resume(&db.store, "literal-key", &decision).await;
    dispatched(&db.store, &worker).await;
    let context_bytes: Vec<u8> =
        sqlx::query_scalar("SELECT context_bytes FROM workflow_activations WHERE activation_id=$1")
            .bind(&worker.lease.owner.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    let saved: Value = serde_json::from_slice(&context_bytes).unwrap();
    assert_eq!(
        saved["wake"]["approval"]["key"],
        json!(r"approve\u0000send")
    );
    assert_eq!(
        saved["wake"]["approval"]["action"]["arguments"]["note"],
        json!("a\u{0000}b")
    );
    let record = LocalResultCommand {
        owner: worker.lease.owner.clone(),
        record: LocalStepRecord {
            key: "_approval:28d9fc6d9913005b31de9e24b7277b302edf3565834c93c5a30604bb57962cfd"
                .into(),
            callable: decision.action.name.clone(),
            input: decision.action.arguments,
            output: json!({"receipt": "unprotected"}),
        },
    };
    assert!(matches!(
        db.store.record_local_result(&record).await,
        Err(ContractError::Conflict)
    ));
    let after: Vec<u8> =
        sqlx::query_scalar("SELECT context_bytes FROM workflow_activations WHERE activation_id=$1")
            .bind(&worker.lease.owner.task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(after, context_bytes);
    db.finish().await;
}
