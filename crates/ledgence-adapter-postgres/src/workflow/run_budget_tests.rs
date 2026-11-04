//! Durable public-port regressions. Each scenario runs on a migrated, isolated database.
use super::*;
use crate::run_budget::{task, workflow};
fn limits(calls: u32, children: u32) -> RunBudgetPolicy {
    RunBudgetPolicy {
        max_calls: Some(calls),
        max_delegations: Some(children),
        deadline_at_ms: None,
    }
}
async fn root(store: &PostgresStore, p: RunBudgetPolicy) -> (WorkflowSnapshot, Assignment) {
    let mut cmd = command();
    cmd.input.budget = Some(p);
    cmd.input.queue = "root".into();
    let w = store
        .accept_resolved_workflow(&cmd, &descriptor())
        .await
        .unwrap();
    assert_eq!(
        store
            .accept_resolved_workflow(&cmd, &descriptor())
            .await
            .unwrap()
            .workflow_id,
        w.workflow_id
    );
    let a = acquire(store, "root").await;
    dispatched(store, &a).await;
    (w, a)
}
fn fork(owner: &Assignment, key: &str, queues: &[&str]) -> WorkflowForkCommand {
    WorkflowForkCommand {
        owner: owner.lease.owner.clone(),
        processing_trace: None,
        fork: WorkflowForkRequest {
            key: key.into(),
            branches: queues
                .iter()
                .map(|q| WorkflowBranch {
                    key: (*q).into(),
                    queue: (*q).into(),
                    entrypoint: "ask".into(),
                    data: Value::Null,
                    retry_policy: command().input.retry_policy,
                    attempt_timeout_ms: 300_000,
                })
                .collect(),
        },
    }
}
async fn child_wait(store: &PostgresStore, queue: &str) -> AdmissionOccurrence {
    child_wait_for(store, queue, 60_000).await
}
async fn child_wait_for(
    store: &PostgresStore,
    queue: &str,
    timeout_ms: u64,
) -> AdmissionOccurrence {
    let a = acquire(store, queue).await;
    let workflow_id = store
        .activation_context(&a.lease.owner)
        .await
        .unwrap()
        .workflow_id;
    let request = AdmissionWaitRequest::New {
        resource: "model".into(),
        operation: AdmissionOperation {
            callable: "app:ask".into(),
            kind: AdmissionOperationKind::Model,
            version: "1".into(),
            arguments: json!({}),
        },
    };
    apply_decision(
        store,
        &a,
        0,
        WorkflowAction::Wait {
            state: Value::Null,
            continuation: "admitted".into(),
            commands: vec![],
            wait: WorkflowWait::ResourceAdmission {
                key: "ask".into(),
                request,
                timeout_ms,
            },
        },
        &[],
    )
    .await;
    AdmissionOccurrence {
        scope: scope(),
        workflow_id,
        origin_activation_id: a.lease.owner.task_id,
        key: "ask".into(),
    }
}
async fn snap(store: &PostgresStore, subject: RunBudgetSubject) -> RunBudgetSnapshot {
    store
        .run_budget(
            &operator(),
            &RunBudgetQuery {
                scope: scope(),
                subject,
            },
        )
        .await
        .unwrap()
}
async fn entries(store: &PostgresStore, subject: RunBudgetSubject) -> Vec<RunBudgetEntry> {
    let mut q = RunBudgetEntriesQuery {
        scope: scope(),
        subject,
        before: None,
        limit: 1,
    };
    let mut result = vec![];
    loop {
        let p = store
            .list_run_budget_entries(&operator(), &q)
            .await
            .unwrap();
        assert!(p.items.len() <= 1);
        result.extend(p.items);
        let Some(next) = p.next else { break };
        q.before = Some(next);
    }
    result
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_two_children_race_last_call_and_recover_without_new_authority() {
    let db = fixture(policy(2)).await;
    let (w, activation) = root(&db.store, limits(1, 2)).await;
    let f = fork(&activation, "research", &["support", "batch"]);
    assert!(!db.store.fork_workflow(&f).await.unwrap().already_accepted);
    assert!(db.store.fork_workflow(&f).await.unwrap().already_accepted);
    let left = child_wait(&db.store, "support").await;
    let right = child_wait(&db.store, "batch").await;
    assert_eq!(
        advance(&db.store).await.offered + advance(&db.store).await.offered,
        2
    );
    let (_, a) = claim(&db.store, "support", &left).await;
    let (_, b) = claim(&db.store, "batch", &right).await;
    let service = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&service, AdmissionDispatchPolicy::Enabled).await;
    let worker = worker();
    let (one, two) = tokio::join!(
        db.store.start_admission(&worker, &a),
        service.start_admission(&worker, &b)
    );
    let mut granted = None;
    let mut refused = 0;
    for receipt in [one.unwrap(), two.unwrap()] {
        match receipt {
            AdmissionStartReceipt::Granted { source } => {
                assert!(granted.replace(source).is_none());
            }
            AdmissionStartReceipt::Existing { snapshot } => {
                assert_eq!(snapshot.state, AdmissionState::ClosedBeforeStart);
                assert_eq!(
                    snapshot.budget_denial,
                    Some(RunBudgetDenial::CallsExhausted)
                );
                refused += 1;
            }
        }
    }
    assert_eq!(refused, 1);
    let source = granted.unwrap();
    assert_eq!(resource(&db.store).await.occupied, 1);
    service.close().await;
    let restarted = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&restarted, AdmissionDispatchPolicy::Enabled).await;
    for cmd in [&a, &b] {
        assert!(matches!(
            restarted.start_admission(&worker, cmd).await.unwrap(),
            AdmissionStartReceipt::Existing { .. }
        ));
    }
    let mut f = finish(&source);
    f.usage = Some(RunBudgetUsage::Reported {
        input_tokens: Some(123),
        output_tokens: Some(5),
    });
    assert!(
        !restarted
            .finish_admission(&worker, &f)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(
        restarted
            .finish_admission(&worker, &f)
            .await
            .unwrap()
            .already_accepted
    );
    let mut altered = f.clone();
    altered.usage = Some(RunBudgetUsage::Estimated {
        input_tokens: Some(123),
        output_tokens: None,
    });
    assert!(matches!(
        restarted.finish_admission(&worker, &altered).await,
        Err(ContractError::Conflict)
    ));
    for id in [&left.workflow_id, &right.workflow_id, &w.workflow_id] {
        let s = snap(&restarted, workflow(id)).await;
        assert_eq!(s.root, workflow(&w.workflow_id));
        assert_eq!((s.calls, s.delegations), (1, 2));
    }
    let rows = entries(&restarted, workflow(&w.workflow_id)).await;
    assert_eq!(rows.len(), 4);
    assert!(rows.windows(2).all(|a| a[0].sequence == a[1].sequence + 1));
    assert_eq!(
        rows.iter()
            .filter(|e| e.reason == Some(RunBudgetDenial::CallsExhausted))
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .find(|e| e.source_id == source.source_id)
            .unwrap()
            .usage,
        f.usage.unwrap()
    );
    assert_eq!(resource(&restarted).await.occupied, 0);
    assert!(matches!(
        restarted
            .run_budget(
                &worker,
                &RunBudgetQuery {
                    scope: scope(),
                    subject: workflow(&w.workflow_id)
                }
            )
            .await,
        Err(ContractError::Forbidden)
    ));
    restarted.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_atomic_delegation_denial_and_exact_fork_replay() {
    let db = fixture(policy(2)).await;
    let (w, a) = root(&db.store, limits(5, 1)).await;
    let oversized = fork(&a, "two", &["one", "two"]);
    for _ in 0..2 {
        assert!(matches!(
            db.store.fork_workflow(&oversized).await,
            Err(ContractError::RunBudgetDenied(
                RunBudgetDenial::DelegationsExhausted
            ))
        ));
    }
    assert_eq!(entries(&db.store, workflow(&w.workflow_id)).await.len(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM owned_workflow_links")
            .fetch_one(&db.store.pool)
            .await
            .unwrap(),
        0
    );
    let mut changed = oversized.clone();
    changed.fork.branches.pop();
    assert!(matches!(
        db.store.fork_workflow(&changed).await,
        Err(ContractError::Conflict)
    ));
    let allowed = fork(&a, "one", &["one"]);
    db.store.fork_workflow(&allowed).await.unwrap();
    assert!(
        db.store
            .fork_workflow(&allowed)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(
        snap(&db.store, workflow(&w.workflow_id)).await.delegations,
        1
    );
    // Previously accepted forks did not reserve a second child for the next checkpoint.
    let result = apply_decision(
        &db.store,
        &a,
        0,
        WorkflowAction::Continue {
            state: Value::Null,
            continuation: "next".into(),
            commands: vec![child("excess")],
        },
        &[resolved("excess")],
    )
    .await;
    assert_eq!(result.children_scheduled, 0);
    assert_eq!(result.activations_scheduled, 0);
    let status = db
        .store
        .workflow_status(&scope(), &w.workflow_id)
        .await
        .unwrap();
    assert!(matches!(
        status.state,
        WorkflowState::Failing | WorkflowState::Failed
    ));
    assert_eq!(
        snap(&db.store, workflow(&w.workflow_id)).await.delegations,
        1
    );
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM workflow_task_links WHERE NOT is_activation AND command_key='excess'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_deadline_blocks_new_work_but_allows_root_inspection() {
    let db = fixture(policy(3)).await;
    let mut p = limits(2, 2);
    p.deadline_at_ms = Some(1);
    let (w, a) = root(&db.store, p).await;
    assert!(matches!(
        db.store
            .fork_workflow(&fork(&a, "expired", &["late"]))
            .await,
        Err(ContractError::RunBudgetDenied(
            RunBudgetDenial::DeadlineExceeded
        ))
    ));
    let s = snap(&db.store, workflow(&w.workflow_id)).await;
    assert_eq!(s.delegations, 0);
    let activation = snap(&db.store, task(&a.lease.owner.task_id)).await;
    assert_eq!(activation.root, s.root);
    assert_eq!(
        entries(&db.store, workflow(&w.workflow_id)).await[0].reason,
        Some(RunBudgetDenial::DeadlineExceeded)
    );
    db.finish().await
}

async fn delayed_owner<F, T>(store: &PostgresStore, root: &str, owner: &LeaseOwner, action: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let mut blocker = store.pool.begin().await.unwrap();
    sqlx::query("SELECT root_id FROM run_budgets WHERE root_id=$1 FOR NO KEY UPDATE")
        .bind(root)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let expires:i64=sqlx::query_scalar("UPDATE attempts SET expires_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+1800 WHERE attempt_id=$1 RETURNING expires_at_ms").bind(&owner.attempt_id).fetch_one(&store.pool).await.unwrap();
    let release = async {
        tokio::time::timeout(Duration::from_secs(2),async {
            loop{
                let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid())").fetch_one(&store.pool).await.unwrap();
                if blocked{break;}tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("test must actually observe budget lock contention");
        let now = db::now(&mut blocker).await.unwrap();
        tokio::time::sleep(Duration::from_millis(
            (expires as u64).saturating_sub(now) + 15,
        ))
        .await;
        blocker.commit().await.unwrap();
    };
    let (result, ()) = tokio::join!(action, release);
    result
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_waiting_for_authority_cannot_commit_an_expired_fork_or_refusal() {
    for maximum in [0, 1] {
        let db = fixture(policy(2)).await;
        let (w, a) = root(&db.store, limits(1, maximum)).await;
        let f = fork(&a, "late", &["child"]);
        let result = delayed_owner(
            &db.store,
            &w.workflow_id,
            &a.lease.owner,
            db.store.fork_workflow(&f),
        )
        .await;
        assert!(
            matches!(result, Err(ContractError::OwnershipLost)),
            "{result:?}"
        );
        assert_eq!(
            snap(&db.store, workflow(&w.workflow_id)).await.delegations,
            0
        );
        assert!(
            entries(&db.store, workflow(&w.workflow_id))
                .await
                .is_empty()
        );
        let count:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM workflow_forks) + (SELECT count(*) FROM run_budget_fork_refusals) + (SELECT count(*) FROM owned_workflow_links)").fetch_one(&db.store.pool).await.unwrap();
        assert_eq!(count, 0);
        db.finish().await;
    }
}
async fn named(
    store: &PostgresStore,
    p: RunBudgetPolicy,
) -> (WorkflowSnapshot, AdmissionOccurrence, AdmissionStartCommand) {
    let (w, o, _, c) = named_with_wait(store, Some(p), 60_000).await;
    (w, o, c)
}
async fn named_with_wait(
    store: &PostgresStore,
    p: Option<RunBudgetPolicy>,
    wait_ms: u64,
) -> (
    WorkflowSnapshot,
    AdmissionOccurrence,
    Assignment,
    AdmissionStartCommand,
) {
    let mut submit = command();
    submit.idempotency_key = "named".into();
    submit.input.queue = "named".into();
    submit.input.budget = p;
    let w = store
        .accept_resolved_workflow(&submit, &descriptor())
        .await
        .unwrap();
    let o = child_wait_for(store, "named", wait_ms).await;
    assert_eq!(advance(store).await.offered, 1);
    let (a, c) = claim(store, "named", &o).await;
    (w, o, a, c)
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_waiting_for_authority_cannot_consume_offer_to_refuse_an_expired_caller() {
    let db = fixture(policy(1)).await;
    let (w, o, c) = named(&db.store, limits(0, 0)).await;
    let p = worker();
    let result = delayed_owner(
        &db.store,
        &w.workflow_id,
        &c.owner,
        db.store.start_admission(&p, &c),
    )
    .await;
    assert!(
        matches!(result, Err(ContractError::OwnershipLost)),
        "{result:?}"
    );
    assert_eq!(inspect(&db.store, &o).await.state, AdmissionState::Offered);
    assert_eq!(resource(&db.store).await.offered, 1);
    assert!(
        entries(&db.store, workflow(&w.workflow_id))
            .await
            .is_empty()
    );
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_slow_final_write_cannot_issue_an_expired_callback_permit() {
    let mut config = policy(1);
    config.start_timeout_ms = 100;
    let db = fixture(config).await;
    let (w, o, c) = named(&db.store, limits(1, 0)).await;
    sqlx::raw_sql("CREATE FUNCTION slow_budget_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(.25); RETURN NEW; END $$; CREATE TRIGGER slow_start AFTER UPDATE ON admission_resources FOR EACH ROW WHEN (NEW.started>OLD.started) EXECUTE FUNCTION slow_budget_write()").execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        db.store.start_admission(&worker(), &c).await,
        Err(ContractError::OwnershipLost)
    ));
    assert_eq!(snap(&db.store, workflow(&w.workflow_id)).await.calls, 0);
    assert!(
        entries(&db.store, workflow(&w.workflow_id))
            .await
            .is_empty()
    );
    assert_eq!(inspect(&db.store, &o).await.state, AdmissionState::Offered);
    sqlx::query("DROP TRIGGER slow_start ON admission_resources")
        .execute(&db.store.pool)
        .await
        .unwrap();
    assert!(matches!(
        db.store.start_admission(&worker(), &c).await.unwrap(),
        AdmissionStartReceipt::Granted { .. }
    ));
    db.finish().await;
}
#[derive(Clone, Copy, Debug)]
enum CallbackExpiry {
    Send,
    Offer,
    Wait,
    Lease,
    Budget,
}
async fn callback_start_window(budgeted: bool, cutoff: CallbackExpiry) {
    let mut config = policy(1);
    config.start_timeout_ms = if matches!(cutoff, CallbackExpiry::Send) {
        100
    } else {
        10_000
    };
    config.offer_timeout_ms = if matches!(cutoff, CallbackExpiry::Offer) {
        1_000
    } else {
        30_000
    };
    config.rate = Some(ResourceRate {
        numerator: 1,
        period_ms: 60_000,
        burst: 1,
    });
    let db = fixture(config).await;
    let store = &db.store;
    let mut tx = store.pool.begin().await.unwrap();
    let deadline =
        matches!(cutoff, CallbackExpiry::Budget).then_some(db::now(&mut tx).await.unwrap() + 2_000);
    tx.rollback().await.unwrap();
    let budget = budgeted.then_some(RunBudgetPolicy {
        deadline_at_ms: deadline,
        ..limits(1, 0)
    });
    let wait_ms = if matches!(cutoff, CallbackExpiry::Wait) {
        1_000
    } else {
        60_000
    };
    let (w, o, attempt, c) = named_with_wait(store, budget, wait_ms).await;
    let original = inspect(store, &o).await;
    let barrier = final_write_barrier(store).await;
    let expiry = match cutoff {
        CallbackExpiry::Send => StartExpiry::AfterLock(105),
        CallbackExpiry::Offer => {
            StartExpiry::At(original.offer.as_ref().unwrap().expires_at as i64)
        }
        CallbackExpiry::Wait => {
            assert_eq!(
                original.offer.as_ref().unwrap().expires_at,
                original.deadline_at
            );
            StartExpiry::At(original.deadline_at as i64)
        }
        CallbackExpiry::Budget => StartExpiry::At(deadline.unwrap() as i64),
        CallbackExpiry::Lease => {
            let at:i64=sqlx::query_scalar("UPDATE attempts SET expires_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint+1000 WHERE attempt_id=$1 RETURNING expires_at_ms").bind(&c.owner.attempt_id).fetch_one(&store.pool).await.unwrap();
            StartExpiry::At(at)
        }
    };
    let p = worker();
    let result = expire_during_start(store, store.start_admission(&p, &c), expiry, barrier).await;
    let offered = if matches!(cutoff, CallbackExpiry::Offer | CallbackExpiry::Wait) {
        assert!(
            matches!(result, Ok(AdmissionStartReceipt::Existing { ref snapshot }) if snapshot.state==AdmissionState::AwaitingRewait),
            "{result:?}"
        );
        assert!(
            matches!(store.start_admission(&p,&c).await.unwrap(), AdmissionStartReceipt::Existing { snapshot } if snapshot.state==AdmissionState::AwaitingRewait)
        );
        0
    } else {
        assert!(
            matches!(result, Err(ContractError::OwnershipLost)),
            "{result:?}"
        );
        assert_eq!(inspect(store, &o).await, original);
        1
    };
    let state = inspect(store, &o).await;
    assert!(state.source.is_none() && state.finish.is_none() && state.budget_denial.is_none());
    let counters:(i64,i64,i64,i64)=sqlx::query_as("SELECT offered,started,unknown,credit FROM admission_resources WHERE resource_name='model'").fetch_one(&store.pool).await.unwrap();
    assert_eq!(counters, (offered, 0, 0, 60_000));
    if budgeted {
        assert_eq!(snap(store, workflow(&w.workflow_id)).await.calls, 0);
        assert!(entries(store, workflow(&w.workflow_id)).await.is_empty());
    }
    match cutoff {
        CallbackExpiry::Send => {
            settle_rejected_start(store, &attempt).await;
            let (_, mut fresh) = claim(store, "named", &o).await;
            assert_ne!(c.owner.attempt_id, fresh.owner.attempt_id);
            assert_eq!(c.offer_generation, fresh.offer_generation);
            fresh.start_id = "start:next".into();
            fresh.finish_id = "finish:next".into();
            assert!(matches!(
                store.start_admission(&p, &c).await,
                Err(ContractError::OwnershipLost)
            ));
            let AdmissionStartReceipt::Granted { source } =
                store.start_admission(&p, &fresh).await.unwrap()
            else {
                panic!("live offer did not grant");
            };
            assert!(
                matches!(store.start_admission(&p,&fresh).await.unwrap(), AdmissionStartReceipt::Existing { snapshot } if snapshot.source.as_ref()==Some(&*source))
            );
            if budgeted {
                assert_eq!(snap(store, workflow(&w.workflow_id)).await.calls, 1);
                assert_eq!(entries(store, workflow(&w.workflow_id)).await.len(), 1);
            }
        }
        CallbackExpiry::Budget => {
            for _ in 0..2 {
                assert!(
                    matches!(store.start_admission(&p,&c).await.unwrap(), AdmissionStartReceipt::Existing { snapshot } if snapshot.budget_denial==Some(RunBudgetDenial::DeadlineExceeded))
                );
            }
            assert_eq!(snap(store, workflow(&w.workflow_id)).await.calls, 0);
            let rows = entries(store, workflow(&w.workflow_id)).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].reason, Some(RunBudgetDenial::DeadlineExceeded));
        }
        CallbackExpiry::Offer | CallbackExpiry::Wait | CallbackExpiry::Lease => {}
    }
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn callback_start_window_final_write_rejects_expired_send_authority() {
    for budgeted in [false, true] {
        callback_start_window(budgeted, CallbackExpiry::Send).await;
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn callback_start_window_final_write_recovers_expired_offer() {
    for budgeted in [false, true] {
        callback_start_window(budgeted, CallbackExpiry::Offer).await;
        callback_start_window(budgeted, CallbackExpiry::Wait).await;
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn callback_start_window_final_write_fences_lost_lease_and_root_deadline() {
    for budgeted in [false, true] {
        callback_start_window(budgeted, CallbackExpiry::Lease).await;
    }
    callback_start_window(true, CallbackExpiry::Budget).await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn callback_start_window_keeps_the_send_interval_after_a_valid_grant() {
    let mut config = policy(1);
    config.offer_timeout_ms = 1_000;
    config.start_timeout_ms = 10_000;
    let db = fixture(config).await;
    let (_, o, _, cmd) = named_with_wait(&db.store, None, 60_000).await;
    let expires_at = inspect(&db.store, &o).await.offer.unwrap().expires_at;
    let AdmissionStartReceipt::Granted { source } =
        db.store.start_admission(&worker(), &cmd).await.unwrap()
    else {
        panic!("live offer did not grant");
    };
    assert!(source.accepted_at < expires_at && source.start_deadline_at > expires_at);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_omitted_allowance_does_not_saturate_at_u32_max() {
    let db = fixture(policy(1)).await;
    let (w, _, c) = named(
        &db.store,
        RunBudgetPolicy {
            max_calls: None,
            max_delegations: Some(0),
            deadline_at_ms: None,
        },
    )
    .await;
    sqlx::query(
        "UPDATE run_budgets SET calls=4294967295,last_sequence=4294967295 WHERE root_id=$1",
    )
    .bind(&w.workflow_id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    assert!(matches!(
        db.store.start_admission(&worker(), &c).await.unwrap(),
        AdmissionStartReceipt::Granted { .. }
    ));
    let s = snap(&db.store, workflow(&w.workflow_id)).await;
    assert_eq!(s.calls, 4294967296);
    let rows = entries(&db.store, workflow(&w.workflow_id)).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sequence, 4294967296);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_owner_stop_fences_descendants_before_drain_without_disturbing_siblings() {
    for mode in ["cancel_root", "cancel_subtree", "fail_subtree"] {
        let db = fixture(policy(3)).await;
        let s = &db.store;
        let (w, a) = root(s, limits(8, 8)).await;
        s.fork_workflow(&fork(&a, "root-fork", &["left", "right"]))
            .await
            .unwrap();
        let left = acquire(s, "left").await;
        dispatched(s, &left).await;
        let left_id = s
            .activation_context(&left.lease.owner)
            .await
            .unwrap()
            .workflow_id;
        s.fork_workflow(&fork(&left, "nested", &["grandchild"]))
            .await
            .unwrap();
        let grandchild = child_wait(s, "grandchild").await;
        let sibling = child_wait(s, "right").await;
        assert_eq!(advance(s).await.offered + advance(s).await.offered, 2);
        let (grand_owner, grand) = claim(s, "grandchild", &grandchild).await;
        let (_, right) = claim(s, "right", &sibling).await;
        let before = if mode == "cancel_root" {
            match s.start_admission(&worker(), &right).await.unwrap() {
                AdmissionStartReceipt::Granted { source } => Some(source),
                other => panic!("{other:?}"),
            }
        } else {
            None
        };
        match mode {
            "cancel_root" => {
                s.cancel_workflow(&scope(), &w.workflow_id).await.unwrap();
            }
            "cancel_subtree" => {
                s.cancel_workflow(&scope(), &left_id).await.unwrap();
            }
            _ => {
                apply_decision(
                    s,
                    &left,
                    0,
                    WorkflowAction::Fail {
                        error: ApplicationError {
                            kind: "stop".into(),
                            message: "stop this branch".into(),
                        },
                    },
                    &[],
                )
                .await;
            }
        }
        // No drain work has run. The grandchild still owns a live activation.
        assert_eq!(
            s.workflow_status(&scope(), &grandchild.workflow_id)
                .await
                .unwrap()
                .state,
            WorkflowState::Running
        );
        assert!(matches!(
            s.fork_workflow(&fork(&grand_owner, "too-late", &["another"]))
                .await,
            Err(ContractError::RunBudgetDenied(RunBudgetDenial::Cancelled))
        ));
        for _ in 0..2 {
            let AdmissionStartReceipt::Existing { snapshot } =
                s.start_admission(&worker(), &grand).await.unwrap()
            else {
                panic!("work escaped ancestor stop")
            };
            assert_eq!(snapshot.budget_denial, Some(RunBudgetDenial::Cancelled));
        }
        let surviving = match before {
            Some(src) => {
                assert!(matches!(
                    s.start_admission(&worker(), &right).await.unwrap(),
                    AdmissionStartReceipt::Existing { .. }
                ));
                src
            }
            None => match s.start_admission(&worker(), &right).await.unwrap() {
                AdmissionStartReceipt::Granted { source } => source,
                other => panic!("sibling should still be live: {other:?}"),
            },
        };
        s.finish_admission(&worker(), &finish(&surviving))
            .await
            .unwrap();
        let totals = snap(s, workflow(&w.workflow_id)).await;
        assert_eq!((totals.calls, totals.delegations), (1, 3));
        assert_eq!(resource(s).await.occupied, 0);
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn run_budget_fork_and_checkpoint_discard_every_child_if_the_final_write_crosses_the_deadline()
 {
    for interactive in [true, false] {
        let db = fixture(policy(2)).await;
        let store = &db.store;
        let at = {
            let mut tx = store.pool.begin().await.unwrap();
            db::now(&mut tx).await.unwrap() + 1500
        };
        let (w, a) = root(
            store,
            RunBudgetPolicy {
                max_calls: None,
                max_delegations: Some(2),
                deadline_at_ms: Some(at),
            },
        )
        .await;
        // A sequence is intentionally nontransactional, proving a tentative grant
        // was written before expiry and rolled back after the injected delay.
        sqlx::raw_sql("CREATE SEQUENCE delayed_delegation;
            CREATE TABLE fixture_expiry (at_ms bigint);
            CREATE FUNCTION delay_delegation() RETURNS trigger LANGUAGE plpgsql AS $$
              BEGIN PERFORM nextval('delayed_delegation');
                PERFORM pg_sleep(GREATEST(0,((SELECT at_ms FROM fixture_expiry)-extract(epoch FROM clock_timestamp())*1000)/1000.0)+0.05);
                RETURN NEW;
              END $$;
            CREATE TRIGGER delay_grant AFTER INSERT ON run_budget_entries FOR EACH ROW
            WHEN (NEW.action='delegation' AND left(NEW.source_id,4)<>'den_') EXECUTE FUNCTION delay_delegation()").execute(&store.pool).await.unwrap();
        sqlx::query("INSERT INTO fixture_expiry VALUES ($1)")
            .bind(at as i64)
            .execute(&store.pool)
            .await
            .unwrap();
        if interactive {
            let f = fork(&a, "late", &["child"]);
            for _ in 0..2 {
                assert!(matches!(
                    store.fork_workflow(&f).await,
                    Err(ContractError::RunBudgetDenied(
                        RunBudgetDenial::DeadlineExceeded
                    ))
                ));
            }
        } else {
            let progress = apply_decision(
                store,
                &a,
                0,
                WorkflowAction::Continue {
                    commands: vec![child("late")],
                    continuation: "continued".into(),
                    state: Value::Null,
                },
                &[resolved("late")],
            )
            .await;
            assert_eq!(
                (progress.children_scheduled, progress.activations_scheduled),
                (0, 0)
            );
        }
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT is_called FROM delayed_delegation")
                .fetch_one(&store.pool)
                .await
                .unwrap()
        );
        assert_eq!(snap(store, workflow(&w.workflow_id)).await.delegations, 0);
        let rows = entries(store, workflow(&w.workflow_id)).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason, Some(RunBudgetDenial::DeadlineExceeded));
        let children:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM owned_workflow_links)+(SELECT count(*) FROM workflow_task_links WHERE NOT is_activation)").fetch_one(&store.pool).await.unwrap();
        assert_eq!(children, 0);
        db.finish().await;
    }
}
