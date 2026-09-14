//! Discovery against real PostgreSQL: index plans, live pagination, and upgrades.

use crate::{tests::*, *};
use ledgence_worker_api::ProgramOutcome;
use serde_json::Value;
use sqlx::{AssertSqlSafe, Column, Execute, Row, TypeInfo};

async fn seed(
    store: &PostgresStore,
    tenant: &Scope,
    key: &str,
    at: Timestamp,
    queue: &str,
    correlation: Option<&str>,
) -> TaskStatus {
    let mut submit = command();
    submit.idempotency_key = key.into();
    submit.input.tenant_id = tenant.tenant_id.clone();
    submit.input.namespace = tenant.namespace.clone();
    submit.input.queue = queue.into();
    submit.input.correlation_key = correlation.map(str::to_owned);
    submit.input.retry_policy.max_attempts = 1;
    let task = store
        .accept_resolved_submission(&submit, &descriptor())
        .await
        .unwrap();
    // Deterministic submission-time boundaries independent of clock precision.
    sqlx::query("UPDATE tasks SET submitted_at_ms=$2,available_at_ms=$2 WHERE task_id=$1")
        .bind(&task.task_id)
        .bind(at as i64)
        .execute(&store.pool)
        .await
        .unwrap();
    store.status(tenant, &task.task_id).await.unwrap()
}

fn ids(page: &TaskPage) -> Vec<&str> {
    page.items
        .iter()
        .map(|item| item.task_id.as_str())
        .collect()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_filters_are_scoped_exact_and_respect_time_boundaries() {
    let db = TestDb::new().await;
    let scope = scope();
    let key = "INV-'雪%_1042";
    let old = seed(&db.store, &scope, "old", 99, "python", Some(key)).await;
    let from = seed(&db.store, &scope, "from", 100, "python", Some(key)).await;
    let end = seed(&db.store, &scope, "end", 199, "python", Some(key)).await;
    seed(&db.store, &scope, "until", 200, "python", Some(key)).await;
    seed(&db.store, &scope, "queue", 150, "other", Some(key)).await;
    seed(
        &db.store,
        &scope,
        "correlation",
        150,
        "python",
        Some("INV-X1042"),
    )
    .await;
    seed(&db.store, &scope, "null", 150, "python", None).await;
    let empty = seed(&db.store, &scope, "empty", 150, "python", Some("")).await;
    for other in [
        Scope {
            tenant_id: "other".into(),
            ..scope.clone()
        },
        Scope {
            namespace: "other".into(),
            ..scope.clone()
        },
    ] {
        seed(&db.store, &other, "foreign", 150, "python", Some(key)).await;
    }
    let query = TaskListQuery {
        filters: TaskFilters {
            state: Some(TaskState::Queued),
            queue: Some("python".into()),
            submitted_from: Some(100),
            submitted_until: Some(200),
            correlation_key: Some(key.into()),
        },
        ..TaskListQuery::default()
    };
    let page = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(ids(&page), [end.task_id.as_str(), from.task_id.as_str()]);
    assert!(page.next_cursor.is_none());
    assert!(!ids(&page).contains(&old.task_id.as_str()));
    let empty_filter = TaskListQuery {
        filters: TaskFilters {
            correlation_key: Some(String::new()),
            ..TaskFilters::default()
        },
        ..TaskListQuery::default()
    };
    assert_eq!(
        ids(&db.store.list_tasks(&scope, &empty_filter).await.unwrap()),
        [empty.task_id.as_str()]
    );
    let all = db
        .store
        .list_tasks(&scope, &TaskListQuery::default())
        .await
        .unwrap();
    assert_eq!(all.items.len(), 8);
    assert!(all.items.iter().all(|task| task.scope == scope));
    let absent = Scope {
        tenant_id: "missing".into(),
        ..scope.clone()
    };
    assert!(
        db.store
            .list_tasks(&absent, &TaskListQuery::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_keyset_handles_ties_page_sizes_and_cursor_mismatch() {
    let db = TestDb::new().await;
    let scope = scope();
    let mut expected = Vec::new();
    for n in 0..7 {
        expected.push(seed(&db.store, &scope, &format!("tie-{n}"), 100, "python", None).await);
    }
    expected.sort_by_key(|task| std::cmp::Reverse(TaskPosition::from(task)));
    let mut query = TaskListQuery {
        limit: 3,
        ..TaskListQuery::default()
    };
    let first = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(first.items, expected[..3]);
    query.cursor = first.next_cursor;
    assert!(query.cursor.is_some());
    query.limit = 2; // The cursor binds filters and scope, not page size.
    let second = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(second.items, expected[3..5]);
    query.cursor = second.next_cursor;
    let last = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(last.items, expected[5..]);
    assert!(
        last.next_cursor.is_none(),
        "an exact-size final page has no continuation"
    );
    let exhausted = TaskListQuery {
        cursor: Some(
            query
                .next_cursor(&scope, &TaskPosition::from(expected.last().unwrap()))
                .unwrap(),
        ),
        ..query.clone()
    };
    let empty = db.store.list_tasks(&scope, &exhausted).await.unwrap();
    assert!(empty.items.is_empty() && empty.next_cursor.is_none());

    let foreign = Scope {
        tenant_id: "another".into(),
        ..scope.clone()
    };
    assert!(matches!(
        db.store.list_tasks(&foreign, &query).await,
        Err(ContractError::InvalidInput(_))
    ));
    let changed = TaskListQuery {
        filters: TaskFilters {
            queue: Some("python".into()),
            ..TaskFilters::default()
        },
        ..query.clone()
    };
    assert!(matches!(
        db.store.list_tasks(&scope, &changed).await,
        Err(ContractError::InvalidInput(_))
    ));
    for cursor in [
        "".to_owned(),
        "not-a-cursor".to_owned(),
        "a".repeat(TASK_CURSOR_MAX_BYTES + 1),
    ] {
        let invalid = TaskListQuery {
            cursor: Some(cursor),
            ..TaskListQuery::default()
        };
        assert!(matches!(
            db.store.list_tasks(&scope, &invalid).await,
            Err(ContractError::InvalidInput(_))
        ));
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_pages_observe_later_commits_without_offset_shifts() {
    let db = TestDb::new().await;
    let scope = scope();
    let first = seed(&db.store, &scope, "first", 300, "python", None).await;
    let middle = seed(&db.store, &scope, "middle", 200, "python", None).await;
    let last = seed(&db.store, &scope, "last", 100, "python", None).await;
    let mut query = TaskListQuery {
        limit: 1,
        ..TaskListQuery::default()
    };
    let page = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(ids(&page), [first.task_id.as_str()]);
    query.cursor = page.next_cursor;
    let newer = seed(&db.store, &scope, "newer", 400, "python", None).await;
    db.store.cancel(&scope, &first.task_id).await.unwrap();
    let second = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(ids(&second), [middle.task_id.as_str()]);
    query.cursor = second.next_cursor;
    let third = db.store.list_tasks(&scope, &query).await.unwrap();
    assert_eq!(ids(&third), [last.task_id.as_str()]);
    assert!(third.next_cursor.is_none());
    assert_eq!(
        db.store
            .list_tasks(&scope, &TaskListQuery::default())
            .await
            .unwrap()
            .items[0]
            .task_id,
        newer.task_id
    );

    // A task can enter a mutable state filter below the seek position.
    db.store.cancel(&scope, &last.task_id).await.unwrap();
    let mut cancelled = TaskListQuery {
        filters: TaskFilters {
            state: Some(TaskState::Cancelled),
            ..TaskFilters::default()
        },
        limit: 1,
        cursor: None,
    };
    let page = db.store.list_tasks(&scope, &cancelled).await.unwrap();
    assert_eq!(ids(&page), [first.task_id.as_str()]);
    cancelled.cursor = page.next_cursor;
    db.store.cancel(&scope, &middle.task_id).await.unwrap();
    let page = db.store.list_tasks(&scope, &cancelled).await.unwrap();
    assert_eq!(ids(&page), [middle.task_id.as_str()]);
    cancelled.cursor = page.next_cursor;
    assert_eq!(
        ids(&db.store.list_tasks(&scope, &cancelled).await.unwrap()),
        [last.task_id.as_str()]
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_projects_all_states_and_does_not_read_application_payloads() {
    let db = TestDb::new().await;
    let scope = scope();
    for (n, state) in [
        TaskState::Queued,
        TaskState::Active,
        TaskState::Succeeded,
        TaskState::Failed,
        TaskState::Cancelled,
    ]
    .into_iter()
    .enumerate()
    {
        let queue = format!("state-{n}");
        let task = seed(&db.store, &scope, &queue, n as u64, &queue, None).await;
        if state == TaskState::Cancelled {
            db.store.cancel(&scope, &task.task_id).await.unwrap();
        } else if state != TaskState::Queued {
            let session = db.store.open_session(&scope, &queue, 1).await.unwrap();
            let assigned = assignment(
                db.store
                    .acquire(&acquire_command(&session, 0, 1))
                    .await
                    .unwrap(),
            );
            if state != TaskState::Active {
                let mut settled = completed(&assigned, Quiescence::Confirmed, Value::Null);
                if state == TaskState::Failed {
                    let AttemptReport::Completed(report) = &mut settled.report else {
                        unreachable!()
                    };
                    report.outcome = ProgramOutcome::Failure {
                        kind: "test".into(),
                        message: "failed".into(),
                    };
                }
                db.store.settle(&settled).await.unwrap();
            }
        }
        let query = TaskListQuery {
            filters: TaskFilters {
                state: Some(state),
                ..TaskFilters::default()
            },
            ..TaskListQuery::default()
        };
        let page = db.store.list_tasks(&scope, &query).await.unwrap();
        assert_eq!(ids(&page), [task.task_id.as_str()]);
        assert_eq!(
            page.items[0],
            db.store.status(&scope, &task.task_id).await.unwrap()
        );
    }
    // Even unreadable application bytes cannot affect a scalar discovery read.
    sqlx::query("UPDATE tasks SET input_bytes='not-json',descriptor_bytes='not-json'")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let request = TaskListQuery::default();
    let rows = discovery::query(&scope, &request, None)
        .build()
        .fetch_all(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert!(
        rows[0]
            .columns()
            .iter()
            .all(|column| column.type_info().name() != "BYTEA")
    );
    assert_eq!(
        db.store
            .list_tasks(&scope, &request)
            .await
            .unwrap()
            .items
            .len(),
        5
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_migration_upgrades_existing_task_history_without_rewriting_it() {
    let db = TestDb::without_migrations().await;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("20260913000000_initial.sql"),
        include_str!("../migrations/20260913000000_initial.sql"),
    )
    .unwrap();
    sqlx::migrate::Migrator::new(directory.path())
        .await
        .unwrap()
        .run(&db.store.pool)
        .await
        .unwrap();
    // Seed through the old schema, not the current adapter's acceptance path:
    // current acceptance requires the dispatch migration before it can serve.
    let mut input = command();
    input.idempotency_key = "before-upgrade".into();
    input.input.correlation_key = Some("INV-1042".into());
    input.input.retry_policy.max_attempts = 1;
    let transition = ledgence_orchestration_core::submit(
        &input,
        &descriptor(),
        "task_before_upgrade",
        "run_before_upgrade",
        100,
    )
    .unwrap();
    let task = &transition.task;
    let mut tx = db.store.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,correlation_key,input_bytes,descriptor_bytes,origin_trace_bytes,state,submitted_at_ms,available_at_ms,attempt_count) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'queued',100,100,0)")
        .bind(&task.task_id).bind(&task.run_id).bind(&task.input.tenant_id).bind(&task.input.namespace)
        .bind(&task.input.queue).bind(&task.idempotency_key).bind(&task.input.correlation_key)
        .bind(codec::encode(&task.input).unwrap()).bind(codec::encode(&task.descriptor).unwrap())
        .bind(task.origin_trace.as_ref().map(codec::encode).transpose().unwrap())
        .execute(&mut *tx).await.unwrap();
    persistence::history(&mut tx, &transition.history)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Current status queries require the new nullable workflow identity columns.
    // The historical seed is the expected snapshot until all migrations apply.
    let before = ledgence_orchestration_core::task_status(task, None).unwrap();
    let history = sqlx::query("SELECT *,trunc(sequence)::text AS sequence_text FROM task_history WHERE task_id=$1 ORDER BY sequence")
        .bind(&before.task_id).fetch_all(&db.store.pool).await.unwrap()
        .iter().map(codec::history).collect::<Result<Vec<_>>>().unwrap();
    assert!(
        db.store.verify_schema().await.is_err(),
        "old schemas cannot serve new queries before explicit migration"
    );
    db.store.migrate().await.unwrap();
    db.store.verify_schema().await.unwrap();
    let page = db
        .store
        .list_tasks(&scope(), &TaskListQuery::default())
        .await
        .unwrap();
    assert_eq!(page.items, [before]);
    assert_eq!(
        serde_json::to_value(
            db.store
                .history(&scope(), &page.items[0].task_id, 0)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(history).unwrap()
    );
    let indexes: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND indexname LIKE 'tasks_discovery%'").fetch_one(&db.store.pool).await.unwrap();
    assert_eq!(indexes, 4);
    let destination: Option<String> =
        sqlx::query_scalar("SELECT dispatch_destination FROM tasks WHERE task_id=$1")
            .bind(&page.items[0].task_id)
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
    assert!(
        destination.is_none(),
        "upgrade preserves integrated delivery"
    );
    let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_intents")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(intents, 0);
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    assert_eq!(assigned.lease.owner.task_id, page.items[0].task_id);
    db.finish().await;
}

async fn plan(store: &PostgresStore, scope: &Scope, request: &TaskListQuery) -> String {
    let position = request.validate(scope).unwrap();
    let mut builder = discovery::query(scope, request, position.as_ref());
    let mut query = builder.build();
    let arguments = query.take_arguments().unwrap().unwrap();
    let sql = format!("EXPLAIN (ANALYZE, BUFFERS) {}", query.sql().as_str());
    sqlx::query_scalar_with::<_, String, _>(AssertSqlSafe(sql), arguments)
        .fetch_all(&store.pool)
        .await
        .unwrap()
        .join("\n")
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_uses_ordered_index_ranges_for_large_histories_and_deep_pages() {
    let db = TestDb::new().await;
    // Real columns, mixed states, queues, tenants and correlations; application
    // payloads are deliberately tiny because discovery never selects them.
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,correlation_key,input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,attempt_count,terminal_at_ms,cancel_requested_at_ms) SELECT 'task_'||lpad(n::text,8,'0'),'run_'||n,CASE WHEN n%5=0 THEN 'other' ELSE 'acme' END,'billing',CASE WHEN n%10=1 THEN 'rare' ELSE 'python' END,'key_'||n,CASE WHEN n%7=1 THEN 'invoice:'||(n%100)::text ELSE NULL END,'{}','{}',CASE WHEN n%11=1 THEN 'cancelled' ELSE 'queued' END,n,n,0,CASE WHEN n%11=1 THEN n ELSE NULL END,CASE WHEN n%11=1 THEN n ELSE NULL END FROM generate_series(1,100000) n")
        .execute(&db.store.pool).await.unwrap();
    sqlx::query("INSERT INTO attempts(attempt_id,task_id,generation,lease_id,worker_session_id,consumer_id,event_source,event_id,event_bytes,expires_at_ms,deadline_ms,authority_deadline_ms,state,execution_may_have_started,quiescence,finished_at_ms) SELECT 'att_'||n,'task_'||lpad(n::text,8,'0'),1,'lease_'||n,'session',0,'urn:discovery','event_'||n,'{}',n,n,n,'failed',false,'confirmed',n FROM generate_series(1,100000) n")
        .execute(&db.store.pool).await.unwrap();
    sqlx::query("UPDATE tasks SET attempt_count=1")
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("VACUUM ANALYZE attempts")
        .execute(&db.store.pool)
        .await
        .unwrap();
    sqlx::query("VACUUM ANALYZE tasks")
        .execute(&db.store.pool)
        .await
        .unwrap();
    let scope = scope();
    for (label, filters, expected_index) in [
        ("scope", TaskFilters::default(), "tasks_discovery"),
        (
            "state",
            TaskFilters {
                state: Some(TaskState::Cancelled),
                ..TaskFilters::default()
            },
            "tasks_discovery_state",
        ),
        (
            "queue",
            TaskFilters {
                queue: Some("rare".into()),
                ..TaskFilters::default()
            },
            "tasks_discovery_queue",
        ),
        (
            "correlation",
            TaskFilters {
                correlation_key: Some("invoice:1".into()),
                ..TaskFilters::default()
            },
            "tasks_discovery_correlation",
        ),
    ] {
        for deep in [false, true] {
            let mut request = TaskListQuery {
                filters: filters.clone(),
                ..TaskListQuery::default()
            };
            if deep {
                request.cursor = Some(
                    request
                        .next_cursor(
                            &scope,
                            &TaskPosition {
                                submitted_at: 10000,
                                task_id: "task_00010000".into(),
                            },
                        )
                        .unwrap(),
                );
            }
            let plan = plan(&db.store, &scope, &request).await;
            println!("Discovery {label} deep={deep} on 100,000 tasks:\n{plan}");
            assert!(
                plan.contains(&format!("using {expected_index} on tasks")),
                "missing ordered access path:\n{plan}"
            );
            assert!(
                !plan.contains("Seq Scan on tasks") && !plan.contains("Seq Scan on attempts"),
                "must not scan task or attempt history:\n{plan}"
            );
            if deep {
                assert!(
                    plan.lines().any(|line| line.contains("Index Cond:")
                        && line.contains("ROW(submitted_at_ms, task_id)")),
                    "seek position must be an index range boundary:\n{plan}"
                );
            }
            let page = db.store.list_tasks(&scope, &request).await.unwrap();
            assert!(page.items.len() <= 50);
        }
    }
    let combined = TaskListQuery {
        filters: TaskFilters {
            state: Some(TaskState::Cancelled),
            queue: Some("rare".into()),
            correlation_key: Some("invoice:1".into()),
            submitted_from: Some(10000),
            submitted_until: Some(90000),
        },
        ..TaskListQuery::default()
    };
    let plan = plan(&db.store, &scope, &combined).await;
    println!("Discovery combined filters on 100,000 tasks:\n{plan}");
    assert!(
        plan.contains("tasks_discovery_correlation"),
        "exact business key should narrow combined candidates:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on tasks"),
        "combined filters must not scan task history:\n{plan}"
    );
    let page = db.store.list_tasks(&scope, &combined).await.unwrap();
    assert!(!page.items.is_empty());
    assert!(page.items.iter().all(|item| combined.filters.matches(item)));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn discovery_reads_committed_state_without_waiting_for_locked_transitions() {
    let db = TestDb::new().await;
    let scope = scope();
    let task = seed(&db.store, &scope, "locked", 100, "python", None).await;
    let mut transition = db.store.pool.begin().await.unwrap();
    sqlx::query("UPDATE tasks SET state='cancelled',cancel_requested_at_ms=200,terminal_at_ms=200 WHERE task_id=$1")
        .bind(&task.task_id).execute(&mut *transition).await.unwrap();
    // The writer remains uncommitted and holds the row lock during this read.
    let before = tokio::time::timeout(
        Duration::from_secs(2),
        db.store.list_tasks(&scope, &TaskListQuery::default()),
    )
    .await
    .expect("discovery should not wait on a lifecycle row lock")
    .unwrap();
    assert_eq!(before.items, [task]);
    transition.commit().await.unwrap();
    let after = db
        .store
        .list_tasks(&scope, &TaskListQuery::default())
        .await
        .unwrap();
    assert_eq!(after.items[0].state, TaskState::Cancelled);
    assert_eq!(after.items[0].terminal_at, Some(200));
    assert_eq!(after.items[0].cancel_requested_at, Some(200));
    db.finish().await;
}
