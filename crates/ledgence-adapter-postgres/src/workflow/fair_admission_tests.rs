use super::*;

fn weighted(cap: u32) -> ResourcePolicy {
    let mut p = policy(cap);
    p.fair_sharing = Some(ResourceFairSharing {
        default_group: "bulk".into(),
        groups: vec![
            ResourceFairGroup {
                name: "fast".into(),
                weight: 3,
                queues: (0..8).map(|n| format!("fast{n}")).collect(),
            },
            ResourceFairGroup {
                name: "bulk".into(),
                weight: 1,
                queues: vec![],
            },
        ],
    });
    p
}
async fn task(store: &PostgresStore, queue: &str) -> TaskSnapshot {
    let mut c = command();
    c.input.queue = queue.into();
    c.idempotency_key = queue.into();
    c.input.admission = Some(TaskAdmissionRequirement {
        resource: "model".into(),
    });
    store
        .accept_resolved_submission(&c, &descriptor())
        .await
        .unwrap()
}
async fn page(store: &PostgresStore, before: Option<u64>, limit: u32) -> ResourceDecisionPage {
    store
        .list_resource_decisions(
            &operator(),
            &ResourceDecisionQuery {
                scope: scope(),
                name: "model".into(),
                before,
                limit,
            },
        )
        .await
        .unwrap()
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_mixed_legacy_waits_resume_cycle_across_restart_and_policy_edits() {
    let db = fixture(policy(12)).await;
    let s = &db.store;
    let b = task(s, "unlisted0").await;
    park(s, "fast0", "model").await;
    park(s, "unlisted1", "model").await;
    for queue in ["fast1", "fast2", "fast3", "unlisted2", "fast4"] {
        task(s, queue).await;
    }
    // Simulate pre-migration/rolling FIFO registrations. Owners, rather than
    // caller-supplied labels, determine both classification and provenance.
    sqlx::raw_sql("UPDATE admission_requests SET origin_queue=NULL; UPDATE task_admission_requests SET origin_queue=NULL").execute(&s.pool).await.unwrap();
    put(s, "model", weighted(12), Some(1), "enable").await;
    assert!(page(s, None, 10).await.items.is_empty());
    s.advance_admissions(&AdmissionMaintenanceQuery { limit: 1 })
        .await
        .unwrap();
    let initial = page(s, None, 1).await;
    assert_eq!(initial.items[0].queue, "fast0");
    assert!(matches!(
        initial.items[0].subject,
        ResourceDecisionSubject::Workflow { .. }
    ));
    let restarted = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&restarted, AdmissionDispatchPolicy::Enabled).await;
    // Revision invalidates the old offer but retains the next group's turn.
    let mut scaled = weighted(13);
    let groups = &mut scaled.fair_sharing.as_mut().unwrap().groups;
    groups[0].weight = 300;
    groups[0].queues.reverse();
    groups[1].weight = 100;
    // An explicit queue in the default group has the same mapping as omission.
    groups[1].queues.push("unlisted0".into());
    put(&restarted, "model", scaled, Some(2), "resize").await;
    advance(&restarted).await;
    let mut history = page(&restarted, Some(u64::MAX), 100).await.items;
    history.reverse();
    assert_eq!(
        history
            .iter()
            .take(4)
            .map(|d| d.queue.as_str())
            .collect::<Vec<_>>(),
        ["fast0", "unlisted0", "fast1", "fast2"]
    );
    assert_eq!(
        history
            .iter()
            .take(4)
            .map(|d| d.cycle_round)
            .collect::<Vec<_>>(),
        [1, 1, 2, 3]
    );
    assert!(
        history
            .iter()
            .take(4)
            .all(|d| d.reason == ResourceDecisionReason::Weighted && d.eligible_groups == 2)
    );
    assert_eq!(history[0].policy_revision, 2);
    assert_eq!(
        (history[0].weight, history[1].weight, history[2].weight),
        (3, 100, 300)
    );
    assert_eq!(history[1].policy_revision, 3);
    assert!(
        matches!(&history[1].subject,ResourceDecisionSubject::Task{task_id,generation:1} if task_id==&b.task_id)
    );
    assert_eq!(history[1].offer_generation, 1);
    assert_eq!(
        history
            .iter()
            .filter(|d| d.group == "bulk")
            .map(|d| d.queue.as_str())
            .collect::<Vec<_>>(),
        ["unlisted0", "unlisted1", "unlisted2"]
    );
    assert_eq!(
        history.last().unwrap().reason,
        ResourceDecisionReason::SoleEligible
    );
    assert!(
        history
            .iter()
            .enumerate()
            .all(|(n, d)| d.sequence == n as u64 + 1)
    );
    let one = page(&restarted, None, 2).await;
    assert_eq!(one.next, Some(one.items[1].sequence));
    let two = page(&restarted, one.next, 2).await;
    assert_eq!(two.items[0].sequence + 1, one.items[1].sequence);
    let last = history.len() as i64;
    let mut edited = weighted(13);
    edited.fair_sharing.as_mut().unwrap().groups.reverse();
    put(&restarted, "model", edited, Some(3), "regroup").await;
    let cursor: (i32, i32, i64) =
        sqlx::query_as("SELECT fair_round,fair_group_index,fair_sequence FROM admission_resources")
            .fetch_one(&restarted.pool)
            .await
            .unwrap();
    assert_eq!(cursor, (0, -1, last));
    restarted.close().await;
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_uncommitted_or_contended_offer_cannot_spend_a_turn() {
    let db = fixture(weighted(1)).await;
    let s = &db.store;
    let first = task(s, "fast0").await;
    let _other = park(s, "other", "model").await;
    let mut busy = s.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR UPDATE")
        .bind(&first.task_id)
        .fetch_one(&mut *busy)
        .await
        .unwrap();
    let empty = advance(s).await;
    assert_eq!(empty.offered + empty.task_offered, 0);
    assert!(page(s, None, 5).await.items.is_empty());
    busy.rollback().await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION skip_decision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER skip_decision BEFORE INSERT ON admission_offer_decisions FOR EACH ROW EXECUTE FUNCTION skip_decision()").execute(&s.pool).await.unwrap();
    assert!(
        s.advance_admissions(&AdmissionMaintenanceQuery { limit: 1 })
            .await
            .is_err()
    );
    assert_eq!(resource(s).await.occupied, 0);
    assert!(page(s, None, 5).await.items.is_empty());
    sqlx::query("DROP TRIGGER skip_decision ON admission_offer_decisions")
        .execute(&s.pool)
        .await
        .unwrap();
    let (x, y) = tokio::join!(advance(s), advance(s));
    assert_eq!(x.task_offered + y.task_offered, 1);
    assert_eq!(x.offered + y.offered, 0);
    let history = page(s, None, 5).await;
    assert_eq!(history.items.len(), 1);
    assert_eq!(history.items[0].sequence, 1);
    assert_eq!(history.items[0].queue, "fast0");
    assert_eq!(resource(s).await.occupied, 1);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_group_heads_use_two_index_seeks_per_group() {
    let db = fixture(weighted(1)).await;
    let s = &db.store;
    // Real partial indexes on both sides of a resource with a skewed backlog.
    sqlx::raw_sql("INSERT INTO task_admission_requests(tenant_id,namespace,resource_name,task_id,generation,state,revision,created_at_ms,updated_at_ms,available_at_ms,snapshot_bytes,origin_queue,fair_group) SELECT tenant_id,namespace,resource_name,'task'||i,1,'waiting',1,0,0,0,'x',CASE WHEN i%11=0 THEN 'fast0' ELSE 'other' END,CASE WHEN i%11=0 THEN 'fast' ELSE 'bulk' END FROM admission_resources,generate_series(1,12000) i;
    INSERT INTO admission_requests(tenant_id,namespace,resource_name,workflow_id,origin_activation_id,logical_key,state,revision,created_at_ms,updated_at_ms,deadline_at_ms,parking_activation_id,snapshot_bytes,origin_queue,fair_group) SELECT tenant_id,namespace,resource_name,'wf'||i,'act','k','waiting',1,0,0,10000,'act','x',CASE WHEN i%11=0 THEN 'fast0' ELSE 'other' END,CASE WHEN i%11=0 THEN 'fast' ELSE 'bulk' END FROM admission_resources,generate_series(1,12000) i;
    ANALYZE admission_requests; ANALYZE task_admission_requests").execute(&s.pool).await.unwrap();
    let mut tx = s.pool.begin().await.unwrap();
    let statement = include_str!("../../queries/admission_fair_heads.sql");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "PREPARE fairhead(text,text,text,text[],bigint) AS {statement}"
    )))
    .execute(&mut *tx)
    .await
    .unwrap();
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL plan_cache_mode={mode}"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
        let plan:serde_json::Value=sqlx::query_scalar("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE fairhead('acme','billing','model',ARRAY['fast','bulk','empty'],0)").fetch_one(&mut *tx).await.unwrap();
        let plan_text = plan.to_string();
        assert!(
            plan_text.contains("admission_requests_group_ready")
                && plan_text.contains("task_admissions_group_ready"),
            "{plan}"
        );
        assert!(!plan_text.contains("Seq Scan"), "{plan}");
        let root = &plan[0]["Plan"];
        let buffers = root["Shared Hit Blocks"].as_u64().unwrap()
            + root["Shared Read Blocks"].as_u64().unwrap();
        assert!(buffers < 64, "{plan}");
        eprintln!(
            "{mode}: 24,000 requests, three groups including one empty, {buffers} shared buffers"
        );
    }
    tx.rollback().await.unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_rate_block_never_consumes_a_turn_and_history_is_scoped() {
    let mut p = weighted(8);
    p.rate = Some(ResourceRate {
        numerator: 1,
        period_ms: 60_000,
        burst: 1,
    });
    let db = fixture(p).await;
    let s = &db.store;
    let o = park(s, "fast0", "model").await;
    task(s, "other").await;
    advance(s).await;
    let (_, cmd) = claim(s, "fast0", &o).await;
    let AdmissionStartReceipt::Granted { source } =
        s.start_admission(&worker(), &cmd).await.unwrap()
    else {
        panic!()
    };
    s.finish_admission(&worker(), &finish(&source))
        .await
        .unwrap();
    let history = page(s, None, 10).await;
    assert_eq!(history.items.len(), 1);
    assert_eq!(resource(s).await.occupied, 0);
    for _ in 0..3 {
        let p = advance(s).await;
        assert_eq!(p.offered + p.task_offered, 0);
    }
    assert_eq!(history, page(s, None, 10).await);
    let foreign = ResourceDecisionQuery {
        scope: Scope {
            tenant_id: "other".into(),
            ..scope()
        },
        name: "model".into(),
        before: None,
        limit: 1,
    };
    assert!(
        s.list_resource_decisions(&operator(), &foreign)
            .await
            .is_err()
    );
    let q = ResourceDecisionQuery {
        scope: scope(),
        ..foreign
    };
    assert!(s.list_resource_decisions(&worker(), &q).await.is_err());
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_policy_change_is_atomic_with_wait_reclassification() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    task(s, "fast0").await;
    sqlx::raw_sql("CREATE FUNCTION suppress_mapping() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER suppress_mapping BEFORE UPDATE OF fair_group ON task_admission_requests FOR EACH ROW EXECUTE FUNCTION suppress_mapping()").execute(&s.pool).await.unwrap();
    let command = ResourcePutCommand {
        scope: scope(),
        name: "model".into(),
        request_id: "enable".into(),
        expected_revision: Some(1),
        policy: weighted(1),
        reason: "enable fairness".into(),
    };
    assert!(s.put_resource(&operator(), &command).await.is_err());
    let resource = resource(s).await;
    assert_eq!(resource.resource.revision, 1);
    assert!(resource.resource.policy.fair_sharing.is_none());
    assert!(page(s, None, 1).await.items.is_empty());
    sqlx::query("DROP TRIGGER suppress_mapping ON task_admission_requests")
        .execute(&s.pool)
        .await
        .unwrap();
    assert!(
        !s.put_resource(&operator(), &command)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        s.put_resource(&operator(), &command)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(advance(s).await.task_offered, 1);
    assert_eq!(page(s, None, 1).await.items[0].queue, "fast0");
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_large_policies_page_by_bytes_without_losing_a_cursor() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    let mut p = weighted(1);
    let group = &mut p.fair_sharing.as_mut().unwrap().groups[0];
    group.queues = (0..64)
        .map(|i| format!("{}{i:02}", "q".repeat(126)))
        .collect();
    for n in 0..39 {
        let name = format!("resource-{n:02}");
        put(s, &name, p.clone(), None, &format!("create-{name}")).await;
    }
    let mut q = ResourceListQuery {
        scope: scope(),
        limit: 100,
        after: None,
    };
    let first = s.list_resources(&operator(), &q).await.unwrap();
    assert!(first.items.len() < 40);
    assert!(first.next.is_some());
    q.after = first.next;
    let rest = s.list_resources(&operator(), &q).await.unwrap();
    assert_eq!(first.items.len() + rest.items.len(), 40);
    assert!(rest.next.is_none());
    db.finish().await;
}

/// Exercise the production locked winner with an explicit decision time.
/// The resource lock prevents concurrent registrations changing the groups.
async fn grantable(store: &PostgresStore, t: &TaskSnapshot, now: u64) -> bool {
    let mut tx = store.pool.begin().await.unwrap();
    let r = crate::workflow::admission::state::resource(&mut tx, &scope(), "model", true)
        .await
        .unwrap();
    let seq =
        sqlx::query_scalar("SELECT request_seq FROM task_admission_requests WHERE task_id=$1")
            .bind(&t.task_id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let grant = crate::workflow::admission::fair::winner(&mut tx, &r, seq, now)
        .await
        .unwrap()
        .is_some();
    tx.rollback().await.unwrap();
    grant
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_crossing_a_rewait_deadline_requires_cleanup_before_a_durable_offer() {
    let db = fixture(weighted(2)).await;
    let s = &db.store;
    let o = park(s, "bulk-workflow", "model").await;
    advance(s).await;
    let (a, _) = claim(s, "bulk-workflow", &o).await;
    apply_decision(
        s,
        &a,
        1,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "again".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "shorter".into(),
                request: AdmissionWaitRequest::Existing { occurrence: o },
                timeout_ms: 500,
            },
        },
        &[],
    )
    .await;
    let t = task(s, "fast0").await;
    let (deadline, original): (i64, i64) =
        sqlx::query_as("SELECT short_episode_deadline_ms,deadline_at_ms FROM admission_requests")
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert!(deadline < original);
    assert!(grantable(s, &t, deadline as u64 - 1).await);
    assert!(!grantable(s, &t, deadline as u64).await);
    assert_eq!(page(s, None, 10).await.items.len(), 1);
    // Let the actual clock reach the tested boundary and run only admissions.
    tokio::time::sleep(Duration::from_millis(520)).await;
    let p = advance(s).await;
    assert_eq!((p.expired, p.task_offered), (1, 1));
    let history = page(s, None, 10).await;
    assert_eq!(history.items.len(), 2);
    assert_eq!(
        history.items[0].reason,
        ResourceDecisionReason::SoleEligible
    );
    assert_eq!(advance(s).await.examined, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_crossing_a_delayed_task_boundary_promotes_before_counting_groups() {
    let db = fixture(weighted(2)).await;
    let s = &db.store;
    let bulk = task(s, "other").await;
    let fast = task(s, "fast0").await;
    let at:i64=sqlx::query_scalar("UPDATE task_admission_requests SET available_at_ms=updated_at_ms+100 WHERE task_id=$1 RETURNING available_at_ms").bind(&fast.task_id).fetch_one(&s.pool).await.unwrap();
    assert!(grantable(s, &bulk, at as u64 - 1).await);
    assert!(!grantable(s, &bulk, at as u64).await);
    assert!(page(s, None, 10).await.items.is_empty());
    tokio::time::sleep(Duration::from_millis(120)).await;
    let p = advance(s).await;
    assert_eq!(p.task_offered, 2);
    let h = page(s, None, 10).await;
    assert_eq!(
        (h.items[1].queue.as_str(), h.items[1].reason),
        ("fast0", ResourceDecisionReason::Weighted)
    );
    assert_eq!(h.items[0].reason, ResourceDecisionReason::SoleEligible);
    assert_eq!(advance(s).await.examined, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_policy_edit_seeks_live_rows_of_one_resource() {
    let db = fixture(weighted(2)).await;
    let s = &db.store;
    put(s, "other", policy(1), None, "other").await;
    task(s, "fast0").await;
    park(s, "workflow", "model").await;
    // Historical state of both the target and another resource, and unrelated
    // live waits, should have almost no impact on a tiny reclassification.
    sqlx::raw_sql("INSERT INTO task_admission_requests(tenant_id,namespace,resource_name,task_id,generation,state,revision,created_at_ms,updated_at_ms,available_at_ms,snapshot_bytes) SELECT 'acme','billing',CASE WHEN i<=15000 THEN 'model' ELSE 'other' END,'st'||i,1,CASE WHEN i>30000 THEN 'waiting' ELSE 'finished' END,1,0,0,0,'x' FROM generate_series(1,32000) i;
       INSERT INTO admission_requests(tenant_id,namespace,resource_name,workflow_id,origin_activation_id,logical_key,state,revision,created_at_ms,updated_at_ms,deadline_at_ms,snapshot_bytes) SELECT 'acme','billing',CASE WHEN i<=15000 THEN 'model' ELSE 'other' END,'wf'||i,'a','k',CASE WHEN i>30000 THEN 'awaiting_rewait' ELSE 'finished' END,1,0,0,10000,'x' FROM generate_series(1,32000) i;
       ANALYZE task_admission_requests; ANALYZE admission_requests").execute(&s.pool).await.unwrap();
    let mut tx = s.pool.begin().await.unwrap();
    for (name, statement, index) in [
        (
            "rt",
            include_str!("../../queries/admission_reclassify_tasks.sql"),
            "task_admissions_reclassify",
        ),
        (
            "rw",
            include_str!("../../queries/admission_reclassify_workflows.sql"),
            "admission_requests_reclassify",
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "PREPARE {name}(text,text,text,text[],text[],text) AS {statement}"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
        for (mode, fallback) in [
            ("force_custom_plan", "first"),
            ("force_generic_plan", "second"),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "SET LOCAL plan_cache_mode={mode}"
            )))
            .execute(&mut *tx)
            .await
            .unwrap();
            let plan:serde_json::Value=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE {name}('acme','billing','model',ARRAY[]::text[],ARRAY[]::text[],'{fallback}')"))).fetch_one(&mut *tx).await.unwrap();
            assert!(plan.to_string().contains(index), "{plan}");
            let root = &plan[0]["Plan"];
            let buffers = root["Shared Hit Blocks"].as_u64().unwrap()
                + root["Shared Read Blocks"].as_u64().unwrap();
            assert!(buffers < 160, "{plan}");
            eprintln!("{mode} {name}: 32,000 retained/unrelated rows, {buffers} shared buffers");
        }
    }
    tx.rollback().await.unwrap();
    db.finish().await;
}

async fn scaled_schedule(scale: u32) -> Vec<(String, u32, u32)> {
    let mut p = weighted(20);
    for g in &mut p.fair_sharing.as_mut().unwrap().groups {
        g.weight *= scale;
    }
    let db = fixture(p).await;
    let s = &db.store;
    for n in 0..8 {
        task(s, &format!("fast{n}")).await;
    }
    task(s, "bulk0").await;
    task(s, "bulk1").await;
    let last = task(s, "bulk2").await;
    for _ in 0..8 {
        s.advance_admissions(&AdmissionMaintenanceQuery { limit: 1 })
            .await
            .unwrap();
    }
    s.cancel(&scope(), &last.task_id).await.unwrap();
    s.advance_admissions(&AdmissionMaintenanceQuery { limit: 1 })
        .await
        .unwrap();
    task(s, "bulk3").await;
    advance(s).await;
    let mut decisions = page(s, None, 100).await.items;
    decisions.reverse();
    let trace = decisions
        .into_iter()
        .map(|d| {
            let weight = if d.group == "fast" { 3 } else { 1 };
            assert_eq!(d.weight, weight * scale);
            (d.group, d.cycle_round, d.eligible_groups)
        })
        .collect();
    db.finish().await;
    trace
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_proportional_weights_preserve_spacing_through_idle_and_reentry() {
    let base = scaled_schedule(1).await;
    assert_eq!(base, scaled_schedule(100).await);
    let prefix: Vec<_> = base
        .iter()
        .take(8)
        .map(|(g, r, n)| (g.as_str(), *r, *n))
        .collect();
    assert_eq!(
        prefix,
        [
            ("fast", 1, 2),
            ("bulk", 1, 2),
            ("fast", 2, 2),
            ("fast", 3, 2),
            ("fast", 1, 2),
            ("bulk", 1, 2),
            ("fast", 2, 2),
            ("fast", 3, 2)
        ]
    );
    assert_eq!(base[8], ("fast".into(), 1, 1));
    assert_eq!(base[9], ("bulk".into(), 1, 2));
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn fair_admission_fifo_ignores_a_stale_short_episode_during_rolling_upgrade() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    task(s, "blocker").await;
    assert_eq!(advance(s).await.task_offered, 1);
    let o = park(s, "normal", "model").await;
    // Old FIFO binaries repark without rewriting the new projection. The
    // actual episode and occurrence are both still open for a full minute.
    sqlx::query("UPDATE admission_requests SET short_episode_deadline_ms=created_at_ms WHERE workflow_id=$1").bind(&o.workflow_id).execute(&s.pool).await.unwrap();
    let mut busy = s.pool.begin().await.unwrap();
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 FOR UPDATE")
        .bind(&o.workflow_id)
        .fetch_one(&mut *busy)
        .await
        .unwrap();
    let p = advance(s).await;
    assert_eq!((p.examined, p.expired, p.offered), (1, 0, 0));
    busy.rollback().await.unwrap();
    assert_eq!(advance(s).await.examined, 0);
    assert_eq!(resource(s).await.occupied, 1);
    assert_eq!(inspect(s, &o).await.state, AdmissionState::Waiting);
    db.finish().await;
}
