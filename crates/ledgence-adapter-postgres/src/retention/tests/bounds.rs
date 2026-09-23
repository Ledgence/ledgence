use super::*;

/// Inspect execution work, not elapsed time. Temp relations mirror the real
/// identity indexes without creating 200,000 unrelated runnable task payloads.
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn sparse_and_drained_large_activation_ledgers_have_bounded_journal_probes() {
    fn relation_work(node: &serde_json::Value, relation: &str) -> (f64, f64) {
        let mut probes = 0.0;
        let mut rows = 0.0;
        if node["Relation Name"].as_str() == Some(relation) {
            let loops = node["Actual Loops"].as_f64().unwrap_or(0.0);
            probes += loops;
            rows += loops
                * (node["Actual Rows"].as_f64().unwrap_or(0.0)
                    + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0));
            assert_ne!(node["Node Type"].as_str(), Some("Seq Scan"), "{node}");
        }
        if let Some(plans) = node["Plans"].as_array() {
            for plan in plans {
                let work = relation_work(plan, relation);
                probes += work.0;
                rows += work.1;
            }
        }
        (probes, rows)
    }
    let db = TestDb::new().await;
    let run = db
        .store
        .accept_resolved_workflow(&command(), &descriptor())
        .await
        .unwrap();
    db.store
        .cancel_workflow(&scope(), &run.workflow_id)
        .await
        .unwrap();
    drain_work(&db.store).await;
    sqlx::query("UPDATE workflow_runs SET submitted_at_ms=1,terminal_at_ms=2 WHERE workflow_id=$1")
        .bind(&run.workflow_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    rounds(&db.store, 6, 128).await;
    let mut connection = db.store.pool.acquire().await.unwrap();
    sqlx::raw_sql("CREATE TEMP TABLE workflow_activations(activation_id text COLLATE \"C\" PRIMARY KEY,workflow_id text COLLATE \"C\" NOT NULL,revision numeric NOT NULL,UNIQUE(workflow_id,revision),UNIQUE(workflow_id,activation_id)); CREATE TEMP TABLE workflow_local_results(activation_id text COLLATE \"C\" NOT NULL,step_key text COLLATE \"C\" NOT NULL,attempt_id text COLLATE \"C\" NOT NULL,PRIMARY KEY(activation_id,step_key)); CREATE INDEX local_results_attempt ON workflow_local_results(attempt_id);").execute(&mut *connection).await.unwrap();
    sqlx::query("INSERT INTO workflow_activations SELECT 'act_'||lpad(n::text,6,'0'),CASE WHEN n<=100000 THEN $1 ELSE 'other' END,n FROM generate_series(1,200000) n").bind(&run.workflow_id).execute(&mut *connection).await.unwrap();
    sqlx::query("INSERT INTO workflow_local_results SELECT activation_id,'step','attempt' FROM workflow_activations WHERE activation_id>='act_100000'").execute(&mut *connection).await.unwrap();
    sqlx::raw_sql("ANALYZE workflow_activations; ANALYZE workflow_local_results;")
        .execute(&mut *connection)
        .await
        .unwrap();
    for after in ["-1", "99999", "100000"] {
        let plan: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {ACTIVATION_PAGE_SQL}"
        )))
        .bind(&run.workflow_id)
        .bind(after)
        .bind(128_i64)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
        let work = relation_work(&plan[0]["Plan"], "workflow_activations");
        assert!(work.0 <= 1.0 && work.1 <= 128.0, "{plan}");
        let activations: Vec<String> = sqlx::query_scalar(ACTIVATION_PAGE_SQL)
            .bind(&run.workflow_id)
            .bind(after)
            .bind(128_i64)
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        let plan: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {JOURNAL_PAGE_SQL}"
        )))
        .bind(&activations)
        .bind(128_i64)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
        let work = relation_work(&plan[0]["Plan"], "workflow_local_results");
        assert!(work.0 <= 129.0 && work.1 <= 256.0, "{plan}");
        assert!(
            plan[0]["Plan"]["Local Hit Blocks"].as_u64().unwrap_or(0) < 2000,
            "{plan}"
        );
    }
    // The sparse target result was deleted by EXPLAIN ANALYZE. Walk all empty
    // activation pages, proving durable advancement and completion in O(N/B).
    for _ in 0..782 {
        assert!(
            collect_workflow_journals(
                &mut connection,
                &run.workflow_id,
                128,
                &mut RetentionProgress::default()
            )
            .await
            .unwrap()
        );
    }
    assert!(
        !collect_workflow_journals(
            &mut connection,
            &run.workflow_id,
            128,
            &mut RetentionProgress::default()
        )
        .await
        .unwrap()
    );
    // All 100k empty target activations still exist. If a completed phase tries
    // either relation again it now fails, rather than hiding an expensive scan.
    sqlx::raw_sql("ALTER TABLE pg_temp.workflow_activations RENAME TO drained_activations; ALTER TABLE pg_temp.workflow_local_results RENAME TO drained_results; SET search_path TO pg_temp;").execute(&mut *connection).await.unwrap();
    // Keep only the identity metadata accessible in the constrained search path.
    sqlx::query("CREATE TEMP VIEW workflow_runs AS SELECT * FROM public.workflow_runs")
        .execute(&mut *connection)
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(
            !collect_workflow_journals(
                &mut connection,
                &run.workflow_id,
                128,
                &mut RetentionProgress::default()
            )
            .await
            .unwrap()
        );
    }
    drop(connection);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_does_not_scan_retiring_backlogs_when_visible_rows_are_sparse_or_absent() {
    use sqlx::Execute;
    let db = TestDb::new().await;
    let task = cancelled(&db.store, "visible").await;
    sqlx::query("UPDATE tasks SET correlation_key='invoice',submitted_at_ms=1 WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,correlation_key,input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,terminal_at_ms,attempt_count,retiring_at_ms) SELECT 'retiring_task_'||n,'retiring_run_'||n,t.tenant_id,t.namespace,t.queue,'retiring_key_'||n,t.correlation_key,t.input_bytes,t.descriptor_bytes,'cancelled',n,n,n,0,n FROM tasks t CROSS JOIN generate_series(2,20000) n WHERE t.task_id=$1").bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    for visible in [true, false] {
        if !visible {
            sqlx::query("UPDATE tasks SET retiring_at_ms=terminal_at_ms WHERE task_id=$1")
                .bind(&task.task_id)
                .execute(&db.store.pool)
                .await
                .unwrap();
        }
        sqlx::query("ANALYZE tasks")
            .execute(&db.store.pool)
            .await
            .unwrap();
        for filters in [
            TaskFilters::default(),
            TaskFilters {
                state: Some(TaskState::Cancelled),
                ..Default::default()
            },
            TaskFilters {
                queue: Some("python".into()),
                ..Default::default()
            },
            TaskFilters {
                correlation_key: Some("invoice".into()),
                ..Default::default()
            },
        ] {
            let scope = scope();
            let request = TaskListQuery {
                filters,
                ..Default::default()
            };
            let mut builder = crate::discovery::query(&scope, &request, None);
            let mut query = builder.build();
            let args = query.take_arguments().unwrap().unwrap();
            let sql = format!(
                "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {}",
                query.sql().as_str()
            );
            let plan: serde_json::Value = sqlx::query_scalar_with(AssertSqlSafe(sql), args)
                .fetch_one(&db.store.pool)
                .await
                .unwrap();
            let text = plan.to_string();
            assert!(text.contains("tasks_discovery"), "{text}");
            assert!(!text.contains("Seq Scan"), "{text}");
            assert!(
                plan[0]["Plan"]["Shared Hit Blocks"].as_u64().unwrap_or(0) < 100,
                "{text}"
            );
            let page = db.store.list_tasks(&scope, &request).await.unwrap();
            assert_eq!(page.items.len(), usize::from(visible));
        }
    }
    // Retirement does not release the submission binding before identity deletion.
    assert!(matches!(
        db.store
            .accept_resolved_submission(
                &{
                    let mut c = command();
                    c.idempotency_key = "visible".into();
                    c
                },
                &descriptor()
            )
            .await,
        Err(ContractError::NotFound)
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18; cleanup throughput measurement"]
async fn cleanup_throughput_for_completed_tasks_with_history_and_receipts() {
    let db = TestDb::new().await;
    let (task, session, assigned) = claimed(&db.store).await;
    db.store
        .renew(&RenewCommand {
            owner: assigned.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    db.store
        .settle(&completed(
            &assigned,
            Quiescence::Confirmed,
            json!({"done":true}),
        ))
        .await
        .unwrap();
    db.store
        .acquire(&acquire_command(&session, 0, 2))
        .await
        .unwrap();
    age_task(&db.store, &task.task_id).await;
    // Clone the real completed storage shape. Snapshot payload bytes are not
    // decoded by collection; relational IDs/FKs remain independently valid.
    sqlx::query("INSERT INTO tasks SELECT (jsonb_populate_record(NULL::tasks,to_jsonb(t)||jsonb_build_object('task_id','bench_task_'||n,'run_id','bench_run_'||n,'idempotency_key','bench_key_'||n))).* FROM tasks t CROSS JOIN generate_series(1,1000) n WHERE t.task_id=$1").bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO attempts SELECT (jsonb_populate_record(NULL::attempts,to_jsonb(a)||jsonb_build_object('attempt_id','bench_attempt_'||n,'task_id','bench_task_'||n,'lease_id','bench_lease_'||n,'event_id','bench_event_'||n))).* FROM attempts a CROSS JOIN generate_series(1,1000) n WHERE a.task_id=$1").bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO accepted_settlements SELECT (jsonb_populate_record(NULL::accepted_settlements,to_jsonb(s)||jsonb_build_object('attempt_id','bench_attempt_'||n))).* FROM accepted_settlements s CROSS JOIN generate_series(1,1000) n WHERE s.attempt_id=$1").bind(&assigned.lease.owner.attempt_id).execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO task_history SELECT (jsonb_populate_record(NULL::task_history,to_jsonb(h)||jsonb_build_object('task_id','bench_task_'||n,'attempt_id',CASE WHEN h.attempt_id IS NULL THEN NULL ELSE 'bench_attempt_'||n END))).* FROM task_history h CROSS JOIN generate_series(1,1000) n WHERE h.task_id=$1").bind(&task.task_id).execute(&db.store.pool).await.unwrap();
    sqlx::raw_sql(
        "ANALYZE tasks; ANALYZE attempts; ANALYZE task_history; ANALYZE accepted_settlements;",
    )
    .execute(&db.store.pool)
    .await
    .unwrap();
    let started = Instant::now();
    let mut deleted = 0;
    let mut batches = 0;
    let mut empty = 0;
    while deleted < 1001 && batches < 100000 {
        let progress = db
            .store
            .retain_batch(&scope(), &RetentionPolicy::default(), deadline())
            .await
            .unwrap();
        deleted += progress.deleted_executions;
        batches += 1;
        empty += u32::from(progress.examined == 0);
        if batches % 5000 == 0 {
            eprintln!(
                "retention measurement: batches={batches}, deleted={deleted}, elapsed_s={:.3}",
                started.elapsed().as_secs_f64()
            );
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    eprintln!(
        "retention completed-task measurement: executions={deleted}, seconds={seconds:.3}, executions_per_second={:.3}, batches={batches}, empty_candidate_batches={empty}, batches_per_execution={:.3}",
        f64::from(deleted) / seconds,
        f64::from(batches) / f64::from(deleted)
    );
    assert_eq!(deleted, 1001);
    db.finish().await;
}
