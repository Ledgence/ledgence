use super::*;
use sqlx::{AssertSqlSafe, Execute, migrate::Migrator};

pub(super) fn query(status: Option<ApprovalStatus>) -> ApprovalInboxQuery {
    ApprovalInboxQuery {
        scope: scope(),
        status,
        program_id: None,
        due_before: None,
        cursor: None,
        limit: 100,
    }
}

// Build genuine pre-inbox requests with identical local keys across workflows.
async fn request(
    store: &PostgresStore,
    owner: Scope,
    program: &str,
    submission: &str,
    timeout: u64,
) -> (WorkflowSnapshot, ApprovalDecisionCommand) {
    let mut cmd = command();
    cmd.idempotency_key = submission.into();
    cmd.input.program.id = program.into();
    cmd.input.namespace = owner.namespace.clone();
    cmd.input.tenant_id = owner.tenant_id.clone();
    let mut artifact = descriptor();
    artifact.program = cmd.input.program.clone();
    let wf = store
        .accept_resolved_workflow(&cmd, &artifact)
        .await
        .unwrap();
    let session = store.open_session(&owner, "python", 1).await.unwrap();
    let lease = assignment(
        store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let mut wait = approvals::wait("review", timeout);
    let WorkflowAction::Wait {
        wait: WorkflowWait::Approval { action, .. },
        ..
    } = &mut wait
    else {
        unreachable!()
    };
    // A literal backslash-u sequence in platform metadata is not the NUL
    // in the application's arguments. Neither may change during backfill.
    action.name = r"send-\u0000".into();
    let mut decision = approvals::decide(&wf, &lease, "review");
    decision.scope = owner;
    decision.action = action.clone();
    apply_decision(store, &lease, 0, wait, &[]).await;
    (wf, decision)
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn inbox_backfills_existing_requests_fences_decisions_and_hides_retired_owners() {
    let db = TestDb::without_migrations().await;
    let previous = Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|m| m.version < 20261102010000)
            .cloned()
            .collect(),
    );
    db.store
        .migrate_with_migrator(MigrationOptions::default(), &previous)
        .await
        .unwrap();
    let (later, _) = request(&db.store, scope(), "agent-b", "later", 180_000).await;
    let (earlier, approve) = request(&db.store, scope(), "agent-a", "earlier", 120_000).await;
    let mut foreign = scope();
    foreign.namespace = "foreign".into();
    let (other, _) = request(&db.store, foreign.clone(), "agent-a", "other", 90_000).await;
    let before: Vec<u8> =
        sqlx::query_scalar("SELECT snapshot_bytes FROM workflow_approvals WHERE workflow_id=$1")
            .bind(&earlier.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    db.store.migrate().await.unwrap();
    let after: Vec<u8> =
        sqlx::query_scalar("SELECT snapshot_bytes FROM workflow_approvals WHERE workflow_id=$1")
            .bind(&earlier.workflow_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    let pending = query(Some(ApprovalStatus::Pending));
    let first = db
        .store
        .approval_inbox(&ApprovalInboxQuery {
            limit: 1,
            ..pending.clone()
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].workflow_id, earlier.workflow_id);
    assert_eq!(first.items[0].action_name, r"send-\u0000");
    assert_eq!(first.items[0].activation_id, approve.activation_id);
    let before = first.items[0].deadline;
    for (cutoff, expected) in [(before, 0), (before + 1, 1)] {
        let page = db
            .store
            .approval_inbox(&ApprovalInboxQuery {
                due_before: Some(cutoff),
                limit: 1,
                ..pending.clone()
            })
            .await
            .unwrap();
        assert_eq!(page.items.len(), expected);
        assert!(page.next_cursor.is_none());
    }
    let cursor = first.next_cursor.unwrap();
    assert!(
        db.store
            .approval_inbox(&ApprovalInboxQuery {
                scope: foreign.clone(),
                cursor: Some(cursor.clone()),
                ..pending.clone()
            })
            .await
            .is_err()
    );
    let page = db
        .store
        .approval_inbox(&ApprovalInboxQuery {
            scope: foreign,
            ..pending.clone()
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].workflow_id, other.workflow_id);

    let filter = ApprovalInboxQuery {
        program_id: Some("agent-a".into()),
        ..pending.clone()
    };
    assert_eq!(
        db.store.approval_inbox(&filter).await.unwrap().items.len(),
        1
    );
    let mut reject = approve.clone();
    reject.decision = ApprovalDecision::Reject;
    reject.decision_id = "second-reviewer".into();
    let (one, two) = tokio::join!(
        db.store.decide_approval(&approve),
        db.store.decide_approval(&reject)
    );
    let (winner, loser) = if one.is_ok() { (one, two) } else { (two, one) };
    let status = winner.unwrap().approval.status;
    assert!(matches!(loser, Err(ContractError::Conflict)));
    assert!(
        db.store
            .approval_inbox(&filter)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        db.store
            .approval_inbox(&query(Some(status)))
            .await
            .unwrap()
            .items[0]
            .workflow_id,
        earlier.workflow_id
    );
    let next = db
        .store
        .approval_inbox(&ApprovalInboxQuery {
            cursor: Some(cursor),
            ..pending.clone()
        })
        .await
        .unwrap();
    assert_eq!(next.items.len(), 1);
    assert!(next.next_cursor.is_none());
    assert_eq!(next.items[0].workflow_id, later.workflow_id);
    let job = one_work(&db.store).await;
    assert_eq!(
        db.store
            .apply_work(&job, &[])
            .await
            .unwrap()
            .activations_scheduled,
        1
    );
    assert_eq!(
        db.store
            .apply_work(&job, &[])
            .await
            .unwrap()
            .activations_scheduled,
        0
    );
    db.store
        .cancel_workflow(&scope(), &later.workflow_id)
        .await
        .unwrap();
    assert!(
        db.store
            .approval_inbox(&pending)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        db.store
            .approval_inbox(&query(Some(ApprovalStatus::Cancelled)))
            .await
            .unwrap()
            .items[0]
            .workflow_id,
        later.workflow_id
    );
    assert_eq!(
        db.store
            .approval_inbox(&query(None))
            .await
            .unwrap()
            .items
            .len(),
        2
    );
    drain_available(&db.store).await;
    sqlx::query("UPDATE workflow_runs SET retiring_at_ms=terminal_at_ms WHERE workflow_id=$1")
        .bind(&later.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(
        db.store
            .approval_inbox(&query(None))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn inbox_seek_uses_ordered_indexes_for_every_filter_combination() {
    let db = TestDb::new().await;
    let mut c = db.store.pool.acquire().await.unwrap();
    // LIKE reuses the actual migrated column types, defaults, collation and indexes.
    // Temporary metadata fixtures keep the check independent of payload cost.
    for statement in [
        "CREATE TEMP TABLE workflow_approvals (LIKE public.workflow_approvals INCLUDING DEFAULTS INCLUDING INDEXES)",
        "CREATE TEMP TABLE workflow_runs(workflow_id text COLLATE \"C\" PRIMARY KEY,retiring_at_ms bigint)",
        "INSERT INTO workflow_runs SELECT 'wf_'||n,NULL FROM generate_series(1,40000) n",
        "INSERT INTO workflow_approvals(workflow_id,wait_key,snapshot_bytes,tenant_id,namespace,program_id,program_version,activation_id,action_name,action_version,created_at_ms,deadline_ms,status) SELECT 'wf_'||n,'review',convert_to('{}','UTF8'),'acme','billing','p'||(n%11),'1','a','publish','1',0,n/5,CASE WHEN n%3=0 THEN 'approved' ELSE 'pending' END FROM generate_series(1,40000) n",
        "ANALYZE workflow_runs",
        "ANALYZE workflow_approvals",
    ] {
        sqlx::query(statement).execute(&mut *c).await.unwrap();
    }
    for (status, program) in [
        (None, None),
        (Some(ApprovalStatus::Approved), None),
        (None, Some("p2")),
        (Some(ApprovalStatus::Approved), Some("p2")),
    ] {
        let query = ApprovalInboxQuery {
            program_id: program.map(str::to_owned),
            limit: 10,
            ..query(status)
        };
        for cutoff in [None, Some(4100)] {
            let query = ApprovalInboxQuery {
                due_before: cutoff,
                ..query.clone()
            };
            let mut builder = super::super::approval_inbox::select(
                &query,
                Some(ApprovalInboxPosition {
                    deadline: 3900,
                    workflow_id: "wf_19500".into(),
                    key: "review".into(),
                }),
            )
            .unwrap();
            let mut sql = builder.build();
            let args = sql.take_arguments().unwrap().unwrap();
            let explain =
                AssertSqlSafe(format!("EXPLAIN (ANALYZE,BUFFERS) {}", sql.sql().as_str()));
            let plan = sqlx::query_scalar_with::<_, String, _>(explain, args)
                .fetch_all(&mut *c)
                .await
                .unwrap()
                .join("\n");
            println!("approval inbox filters {status:?} {program:?}:\n{plan}");
            assert!(
                plan.contains("Index Scan")
                    && plan.contains("Index Cond")
                    && !plan.contains("Seq Scan")
                    && !plan.contains("Sort Key"),
                "{plan}"
            );
            if cutoff.is_some() {
                assert!(
                    plan.lines()
                        .any(|line| line.contains("Index Cond:") && line.contains("deadline_ms <")),
                    "{plan}"
                );
            }
        }
    }
    drop(c);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn inbox_explorer_targets_late_wait_even_on_page_of_one_and_returns_normal_continuation() {
    use ledgence_orchestration_api::console::{
        ConsoleApprovalFocus, ConsolePagination, ConsoleQuery, ConsoleQueryReply, ConsoleQueryStore,
    };
    let db = TestDb::new().await;
    let (wf, first) = request(&db.store, scope(), "agent-a", "first", 120_000).await;
    db.store.decide_approval(&first).await.unwrap();
    drain_available(&db.store).await;
    let resumed = acquire(&db.store, "python").await;
    apply_decision(
        &db.store,
        &resumed,
        1,
        approvals::wait("review:late", 120_000),
        &[],
    )
    .await;
    let mut second = approvals::decide(&wf, &resumed, "review:late");
    second.revision = 1;
    db.store.decide_approval(&second).await.unwrap();
    drain_available(&db.store).await;
    let page = ConsolePagination {
        limit: 1,
        cursor: None,
    };
    let ordinary = ConsoleQuery::Explorer {
        workflow_id: wf.workflow_id.clone(),
        page: page.clone(),
    };
    let ConsoleQueryReply::Explorer(first_page) =
        db.store.query_console(&scope(), &ordinary).await.unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(first_page.page.items.len(), 1);
    assert_ne!(first_page.page.items[0].data.key(), "review:late");
    let target = ConsoleApprovalFocus {
        workflow_id: wf.workflow_id.clone(),
        approval_key: "review:late".into(),
        page: page.clone(),
    };
    let focused = db.store.explore_approval(&scope(), &target).await.unwrap();
    target.validate_reply(&scope(), &focused).unwrap();
    assert!(matches!(
        target.validate_reply(&scope(), &first_page),
        Err(ContractError::Unavailable(_))
    ));
    let node = &focused.page.items[0];
    assert_eq!(node.data.key(), "review:late");
    assert_eq!(node.activation_id, second.activation_id);
    assert_eq!(node.revision.0, 1);
    let next = ConsolePagination {
        limit: 1,
        cursor: Some(focused.page.next_cursor.unwrap()),
    };
    let invalid = ConsoleApprovalFocus {
        workflow_id: wf.workflow_id.clone(),
        approval_key: "review:late".into(),
        page: next.clone(),
    };
    assert!(matches!(
        db.store.explore_approval(&scope(), &invalid).await,
        Err(ContractError::InvalidInput(_))
    ));
    let normal = ConsoleQuery::Explorer {
        workflow_id: wf.workflow_id.clone(),
        page: next,
    };
    let ConsoleQueryReply::Explorer(after) =
        db.store.query_console(&scope(), &normal).await.unwrap()
    else {
        panic!("unexpected response")
    };
    assert_eq!(after.page.items[0].revision.0, 2);
    let mut other = scope();
    other.tenant_id = "other".into();
    assert!(matches!(
        db.store.explore_approval(&other, &target).await,
        Err(ContractError::NotFound)
    ));
    let missing = ConsoleApprovalFocus {
        workflow_id: wf.workflow_id.clone(),
        approval_key: "absent".into(),
        page,
    };
    assert!(matches!(
        db.store.explore_approval(&scope(), &missing).await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}
