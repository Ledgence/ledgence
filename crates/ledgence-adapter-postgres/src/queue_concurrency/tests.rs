use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, completed, descriptor, scope};
use serde_json::json;

fn audience() -> AccessAudience {
    AccessAudience {
        authority_id: "queue-test".into(),
        instance_id: "primary".into(),
        scope: scope(),
    }
}
fn actor(class: PrincipalClass) -> VerifiedPrincipal {
    let capabilities = if class == PrincipalClass::Worker {
        vec![AccessCapability::WorkerExecute]
    } else {
        vec![
            AccessCapability::MetadataRead,
            AccessCapability::SensitiveRead,
            AccessCapability::ResourceControl,
        ]
    };
    actor_with(class, capabilities)
}
fn actor_with(class: PrincipalClass, capabilities: Vec<AccessCapability>) -> VerifiedPrincipal {
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience().issuer(),
            subject: format!("{class:?}"),
        },
        audience(),
        "credential".into(),
        class,
        253402300799999,
        capabilities,
    )
    .unwrap()
}
async fn fixture() -> TestDb {
    let db = TestDb::new().await;
    db.store
        .initialize_instance_with_access(
            Some(&SelfHostedInstanceConfig {
                instance_id: audience().instance_id,
                name: "queues".into(),
                scope: scope(),
                suggested_queues: vec!["python".into()],
                allowed_origins: vec![],
            }),
            Some(&audience()),
        )
        .await
        .unwrap();
    db
}
fn put(
    key: &str,
    expected: Option<u64>,
    max: Option<u32>,
    timeout: Option<QueueHoldTimeout>,
) -> QueuePolicyPut {
    QueuePolicyPut {
        scope: scope(),
        queue: "python".into(),
        request_id: key.into(),
        expected_revision: expected,
        policy: QueueConcurrencyPolicy {
            max_concurrency: max,
            timeout,
        },
        reason: "test policy change".into(),
    }
}
async fn configure(store: &PostgresStore, c: &QueuePolicyPut) -> QueuePolicyReceipt {
    store
        .put_queue_policy(&actor(PrincipalClass::Operator), c)
        .await
        .unwrap()
}
async fn snapshot(store: &PostgresStore) -> QueuePolicySnapshot {
    store
        .inspect_queue_policy(
            &actor(PrincipalClass::Operator),
            &QueuePolicyQuery {
                scope: scope(),
                queue: "python".into(),
            },
        )
        .await
        .unwrap()
}
async fn holds(store: &PostgresStore) -> Vec<QueueHoldSnapshot> {
    store
        .list_queue_holds(
            &actor(PrincipalClass::Operator),
            &QueueHoldList {
                scope: scope(),
                queue: "python".into(),
                after: None,
                limit: 100,
                include_released: true,
            },
        )
        .await
        .unwrap()
        .items
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn source_bound_holds_require_sensitive_read_independently_of_metadata_read() {
    let db = fixture().await;
    let store = &db.store;
    configure(store, &put("enroll", None, Some(1), None)).await;
    submit(store, "held").await;
    let worker = session(store).await;
    let a = assignment(run(store, &acquire_command(&worker, 0, 1)).await);
    let query = QueueHoldList {
        scope: scope(),
        queue: "python".into(),
        after: None,
        limit: 1,
        include_released: false,
    };
    let reader = |cap| actor_with(PrincipalClass::Operator, vec![cap]);
    let metadata = reader(AccessCapability::MetadataRead);
    let sensitive = reader(AccessCapability::SensitiveRead);
    assert!(matches!(
        store.list_queue_holds(&metadata, &query).await,
        Err(ContractError::Forbidden)
    ));
    let page = store.list_queue_holds(&sensitive, &query).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].attempt_id, a.lease.owner.attempt_id);
    let aggregate = QueuePolicyQuery {
        scope: scope(),
        queue: "python".into(),
    };
    assert_eq!(
        store
            .inspect_queue_policy(&metadata, &aggregate)
            .await
            .unwrap()
            .occupied,
        1
    );
    assert!(matches!(
        store.inspect_queue_policy(&sensitive, &aggregate).await,
        Err(ContractError::Forbidden)
    ));
    db.finish().await;
}
async fn submit(store: &PostgresStore, key: &str) -> TaskSnapshot {
    let mut cmd = command();
    cmd.idempotency_key = key.into();
    store
        .accept_resolved_submission(&cmd, &descriptor())
        .await
        .unwrap()
}
async fn session(store: &PostgresStore) -> WorkerSession {
    store.open_session(&scope(), "python", 1).await.unwrap()
}
async fn run(store: &PostgresStore, c: &AcquireCommand) -> AcquireReply {
    store.acquire(c).await.unwrap()
}
async fn dispatch(store: &PostgresStore, a: &Assignment) {
    assert!(
        store
            .renew(&RenewCommand {
                owner: a.lease.owner.clone(),
                sequence: 1,
                intent: RenewIntent::Dispatch
            })
            .await
            .unwrap()
            .dispatch_allowed
    )
}
fn close(a: &Assignment) -> QueueHoldClose {
    QueueHoldClose {
        queue: "python".into(),
        owner: a.lease.owner.clone(),
    }
}
async fn expire(store: &PostgresStore, a: &Assignment) {
    sqlx::query("UPDATE attempts SET expires_at_ms=1 WHERE attempt_id=$1")
        .bind(&a.lease.owner.attempt_id)
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=1 WHERE task_id=$1")
        .bind(&a.lease.owner.task_id)
        .execute(&store.pool)
        .await
        .unwrap();
    store.expire_batch(100).await.unwrap();
}

/// A separate queue leaves the crash test's ordinary Python cursor/sequence
/// unchanged while proving committed uncertainty survives actual WAL recovery.
pub(crate) async fn crash_fixture() -> (TestDb, Assignment) {
    let db = fixture().await;
    let s = &db.store;
    let queue = "crash-sensitive";
    let mut policy = put("crash-policy", None, Some(1), None);
    policy.queue = queue.into();
    configure(s, &policy).await;
    let mut c = command();
    c.idempotency_key = "sensitive-during-crash".into();
    c.input.queue = queue.into();
    let t = s
        .accept_resolved_submission(&c, &descriptor())
        .await
        .unwrap();
    let sess = s.open_session(&scope(), queue, 1).await.unwrap();
    let a = assignment(run(s, &acquire_command(&sess, 0, 1)).await);
    dispatch(s, &a).await;
    s.cancel(&scope(), &t.task_id).await.unwrap();
    expire(s, &a).await;
    let q = QueuePolicyQuery {
        scope: scope(),
        queue: queue.into(),
    };
    let before = s
        .inspect_queue_policy(&actor(PrincipalClass::Operator), &q)
        .await
        .unwrap();
    assert_eq!((before.occupied, before.uncertain), (1, 1));
    (db, a)
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn two_replicas_share_last_slot_without_charging_empty_or_replay() {
    let db = fixture().await;
    let store = &db.store;
    let c = put("one", None, Some(1), None);
    assert!(!configure(store, &c).await.already_accepted);
    assert!(configure(store, &c).await.already_accepted);
    let other = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    submit(store, "one").await;
    submit(store, "two").await;
    let a = acquire_command(&session(store).await, 0, 1);
    let b = acquire_command(&session(&other).await, 0, 1);
    let (left, right) = tokio::join!(run(store, &a), run(&other, &b));
    let (winner, loser, assigned) = match (left, right) {
        (AcquireReply::Assigned { assignment, .. }, AcquireReply::Empty { .. }) => {
            (&a, &b, *assignment)
        }
        (AcquireReply::Empty { .. }, AcquireReply::Assigned { assignment, .. }) => {
            (&b, &a, *assignment)
        }
        pair => panic!("expected one grant: {pair:?}"),
    };
    assert!(assigned.queue_concurrency);
    assert!(assignment(run(store, winner).await).queue_concurrency);
    assert!(matches!(
        run(store, loser).await,
        AcquireReply::Empty { .. }
    ));
    assert_eq!(snapshot(store).await.occupied, 1);
    dispatch(store, &assigned).await;
    expire(store, &assigned).await;
    assert_eq!(snapshot(store).await.uncertain, 1);
    let mut next = loser.clone();
    next.sequence = 2;
    assert!(matches!(
        run(store, &next).await,
        AcquireReply::Empty { .. }
    ));
    let mut wrong = close(&assigned);
    wrong.owner.lease_id.push('x');
    assert!(
        store
            .close_queue_hold(&actor(PrincipalClass::Worker), &wrong)
            .await
            .is_err()
    );
    assert_eq!(snapshot(store).await.occupied, 1);
    let r = store
        .close_queue_hold(&actor(PrincipalClass::Worker), &close(&assigned))
        .await
        .unwrap();
    assert!(!r.already_closed);
    assert!(
        store
            .close_queue_hold(&actor(PrincipalClass::Worker), &close(&assigned))
            .await
            .unwrap()
            .already_closed
    );
    next.sequence = 3;
    assert!(matches!(
        run(store, &next).await,
        AcquireReply::Assigned { .. }
    ));
    assert_eq!(snapshot(store).await.occupied, 1);
    other.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn cutover_adopts_dispatched_work_and_downshift_does_not_revoke_it() {
    let db = fixture().await;
    let s = &db.store;
    submit(s, "before").await;
    let cmd = acquire_command(&session(s).await, 0, 1);
    let a = assignment(run(s, &cmd).await);
    assert!(!a.queue_concurrency);
    dispatch(s, &a).await;
    let create = put("cutover", None, Some(0), None);
    configure(s, &create).await;
    assert_eq!(snapshot(s).await.occupied, 1);
    assert!(assignment(run(s, &cmd).await).queue_concurrency);
    submit(s, "after").await;
    let c = acquire_command(&session(s).await, 0, 1);
    assert!(matches!(run(s, &c).await, AcquireReply::Empty { .. }));
    assert!(
        s.put_queue_policy(
            &actor(PrincipalClass::Operator),
            &put("wrong", None, Some(1), None)
        )
        .await
        .is_err()
    );
    assert!(
        s.renew(&RenewCommand {
            owner: a.lease.owner.clone(),
            sequence: 2,
            intent: RenewIntent::KeepAlive
        })
        .await
        .unwrap()
        .remaining_ms
            > 0
    );
    s.settle(&completed(&a, Quiescence::Confirmed, json!(true)))
        .await
        .unwrap();
    assert_eq!(snapshot(s).await.occupied, 0);
    configure(s, &put("up", Some(1), Some(1), None)).await;
    let mut c = c;
    c.sequence = 2;
    assert!(matches!(run(s, &c).await, AcquireReply::Assigned { .. }));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn late_cleanup_after_manual_release_retains_original_actor_and_reason() {
    let db = fixture().await;
    let s = &db.store;
    configure(s, &put("one", None, Some(1), None)).await;
    submit(s, "orphan").await;
    let a = assignment(run(s, &acquire_command(&session(s).await, 0, 1)).await);
    dispatch(s, &a).await;
    expire(s, &a).await;
    let h = holds(s).await.pop().unwrap();
    assert_eq!(h.state, QueueHoldState::Uncertain);
    let c = QueueHoldRelease {
        scope: scope(),
        queue: "python".into(),
        task_id: h.task_id.clone(),
        attempt_id: h.attempt_id.clone(),
        expected_revision: h.revision,
        request_id: "ops-release".into(),
        reason: "remote system verified idle".into(),
    };
    let r = s
        .release_queue_hold(&actor(PrincipalClass::Operator), &c)
        .await
        .unwrap();
    assert!(!r.already_accepted);
    assert!(
        s.release_queue_hold(&actor(PrincipalClass::Operator), &c)
            .await
            .unwrap()
            .already_accepted
    );
    let late = s
        .close_queue_hold(&actor(PrincipalClass::Worker), &close(&a))
        .await
        .unwrap();
    assert!(late.already_closed);
    assert_eq!(late.release_kind, QueueHoldReleaseKind::Operator);
    assert_eq!(snapshot(s).await.occupied, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn grace_begins_at_actual_durable_stop_not_overdue_policy_deadline() {
    let db = fixture().await;
    let s = &db.store;
    let timeout = QueueHoldTimeout {
        after_ms: 60000,
        release: QueueTimeoutRelease::AfterGrace { grace_ms: 200 },
    };
    configure(s, &put("old-policy", None, Some(1), Some(timeout))).await;
    submit(s, "slow").await;
    let cmd = acquire_command(&session(s).await, 0, 1);
    let a = assignment(run(s, &cmd).await);
    dispatch(s, &a).await;
    configure(s, &put("edit", Some(1), Some(1), None)).await;
    // Simulate a completely stalled maintenance service. Keep coherent historical timestamps.
    sqlx::query("UPDATE queue_concurrency_holds SET acquired_at_ms=acquired_at_ms-70000,deadline_ms=deadline_ms-70000 WHERE attempt_id=$1").bind(&a.lease.owner.attempt_id).execute(&s.pool).await.unwrap();
    s.expire_batch(50).await.unwrap();
    let h = holds(s).await.pop().unwrap();
    assert_eq!(h.state, QueueHoldState::Uncertain);
    assert_eq!(h.policy_revision, 1);
    assert!(h.stop_requested_at.unwrap() > h.deadline.unwrap() + 5000);
    assert_eq!(h.force_at, h.stop_requested_at.map(|v| v + 200));
    assert_eq!(snapshot(s).await.occupied, 1);
    let auth = s
        .renew(&RenewCommand {
            owner: a.lease.owner.clone(),
            sequence: 2,
            intent: RenewIntent::KeepAlive,
        })
        .await
        .unwrap();
    assert!(auth.cancel_requested);
    assert!(!auth.dispatch_allowed);
    tokio::time::sleep(Duration::from_millis(210)).await;
    let (a1, a2) = tokio::join!(s.expire_batch(100), s.expire_batch(100));
    a1.unwrap();
    a2.unwrap();
    let late = s
        .close_queue_hold(&actor(PrincipalClass::Worker), &close(&a))
        .await
        .unwrap();
    assert!(late.already_closed);
    assert_eq!(late.release_kind, QueueHoldReleaseKind::Timeout);
    assert_eq!(snapshot(s).await.occupied, 0);
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM queue_concurrency_events WHERE attempt_id=$1")
            .bind(&a.lease.owner.attempt_id)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(events, 2);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn broker_full_ack_is_backed_by_a_replayable_publication() {
    let db = fixture().await;
    let s = &db.store;
    let route = DispatchRoute {
        scope: scope(),
        queue: "python".into(),
        destination: "external".into(),
    };
    s.configure_route(&route).await.unwrap();
    configure(s, &put("pause", None, Some(0), None)).await;
    let t = submit(s, "target").await;
    let ws = session(s).await;
    let claim = ClaimCommand {
        acquisition: acquire_command(&ws, 0, 1),
        dispatch: DispatchRef {
            scope: scope(),
            queue: "python".into(),
            task_id: t.task_id.clone(),
            generation: 1,
        },
    };
    let first = s.claim_dispatch_once(&claim).await.unwrap();
    let ClaimDisposition::Deferred { available_at } = first.disposition else {
        panic!("not deferred")
    };
    let durable: i64 = sqlx::query_scalar(
        "SELECT next_publish_at_ms FROM dispatch_intents WHERE task_id=$1 AND generation=1",
    )
    .bind(&t.task_id)
    .fetch_one(&s.pool)
    .await
    .unwrap();
    assert_eq!(durable, available_at as i64);
    assert_eq!(
        serde_json::to_value(s.claim_dispatch_once(&claim).await.unwrap()).unwrap(),
        serde_json::to_value(first).unwrap()
    );
    let publication: String =
        sqlx::query_scalar("SELECT publication_id FROM dispatch_intents WHERE task_id=$1")
            .bind(&t.task_id)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    let duplicate = ClaimCommand {
        acquisition: acquire_command(&session(s).await, 0, 1),
        dispatch: claim.dispatch.clone(),
    };
    assert!(
        matches!(s.claim_dispatch_once(&duplicate).await.unwrap().disposition,ClaimDisposition::Deferred { available_at: next } if next==available_at)
    );
    let unchanged: String =
        sqlx::query_scalar("SELECT publication_id FROM dispatch_intents WHERE task_id=$1")
            .bind(&t.task_id)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(unchanged, publication);
    assert_eq!(snapshot(s).await.occupied, 0);
    configure(s, &put("enable", Some(1), Some(1), None)).await;
    let mut claim = claim;
    claim.acquisition.sequence = 2;
    let ClaimDisposition::Claimed { reply } =
        s.claim_dispatch_once(&claim).await.unwrap().disposition
    else {
        panic!("not claimed")
    };
    assert!(assignment(reply).queue_concurrency);
    assert_eq!(snapshot(s).await.occupied, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn first_policy_waits_for_an_inflight_ungoverned_insert_then_adopts_it() {
    let db = fixture().await;
    let s = &db.store;
    submit(s, "already-writing").await;
    let c = acquire_command(&session(s).await, 0, 1);
    let policy = put("pause", None, Some(0), None);
    sqlx::raw_sql("CREATE FUNCTION pause_queue_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(4792977); RETURN NEW; END $$; CREATE TRIGGER zz_pause_insert AFTER INSERT ON attempts FOR EACH ROW EXECUTE FUNCTION pause_queue_insert();")
        .execute(&s.pool).await.unwrap();
    let mut blocker = s.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(4792977)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut claim = s.acquire(&c);
    tokio::select! {
        r = &mut claim => panic!("claim crossed barrier: {r:?}"),
        _ = crate::tests::wait_for_lock_waiter(s) => {}
    }
    let p = actor(PrincipalClass::Operator);
    let mut enable = s.put_queue_policy(&p, &policy);
    tokio::select! {
        r = &mut enable => panic!("policy did not wait for in-flight attempt: {r:?}"),
        _ = crate::tests::bounded(async {
            loop {
                let waiters:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'")
                    .fetch_one(&s.pool).await.unwrap();
                if waiters == 2 { break; }
                tokio::task::yield_now().await;
            }
        }) => {}
    }
    blocker.commit().await.unwrap();
    let (granted, enabled) = tokio::join!(claim, enable);
    assert!(!assignment(granted.unwrap()).queue_concurrency);
    enabled.unwrap();
    assert_eq!(snapshot(s).await.occupied, 1);
    assert_eq!(holds(s).await[0].state, QueueHoldState::Reserved);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn suppressed_reservation_rolls_back_the_entire_grant_and_legacy_writes_fail_closed() {
    let db = fixture().await;
    let s = &db.store;
    configure(s, &put("one", None, Some(1), None)).await;
    let t = submit(s, "reservation").await;
    let c = acquire_command(&session(s).await, 0, 1);
    sqlx::raw_sql("CREATE FUNCTION drop_queue_reservation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER reject_hold BEFORE INSERT ON queue_concurrency_holds FOR EACH ROW EXECUTE FUNCTION drop_queue_reservation()").execute(&s.pool).await.unwrap();
    assert!(s.acquire(&c).await.is_err());
    let state:(String,i64,i64)=sqlx::query_as("SELECT state,(SELECT count(*) FROM attempts WHERE task_id=$1),(SELECT count(*) FROM consumer_cursors WHERE session_id=$2) FROM tasks WHERE task_id=$1")
        .bind(&t.task_id).bind(&c.worker_session_id).fetch_one(&s.pool).await.unwrap();
    assert_eq!(state, ("queued".into(), 0, 0));
    assert_eq!(snapshot(s).await.occupied, 0);
    sqlx::query("DROP TRIGGER reject_hold ON queue_concurrency_holds")
        .execute(&s.pool)
        .await
        .unwrap();
    let assigned = assignment(run(s, &c).await);
    assert!(assigned.queue_concurrency);
    let fresh = submit(s, "old-replica").await;
    let err=sqlx::query("INSERT INTO attempts(attempt_id,task_id,generation,lease_id,worker_session_id,consumer_id,event_source,event_id,event_bytes,expires_at_ms,deadline_ms,authority_deadline_ms,state,execution_may_have_started,quiescence) SELECT 'legacy-attempt',$2,1,'legacy-lease',worker_session_id,consumer_id,event_source,'legacy-event',event_bytes,expires_at_ms,deadline_ms,authority_deadline_ms,'active',false,'unconfirmed' FROM attempts WHERE attempt_id=$1")
        .bind(&assigned.lease.owner.attempt_id).bind(&fresh.task_id).execute(&s.pool).await.unwrap_err();
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("55000")
    );
    assert_eq!(snapshot(s).await.occupied, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn source_close_before_business_report_fences_replay_and_allows_late_settlement() {
    let db = fixture().await;
    let s = &db.store;
    configure(s, &put("enable", None, Some(1), None)).await;
    submit(s, "close-before-report").await;
    let c = acquire_command(&session(s).await, 0, 1);
    let a = assignment(run(s, &c).await);
    dispatch(s, &a).await;
    let r = s
        .close_queue_hold(&actor(PrincipalClass::Worker), &close(&a))
        .await
        .unwrap();
    assert_eq!(r.release_kind, QueueHoldReleaseKind::Confirmed);
    let repeat = s
        .renew(&RenewCommand {
            owner: a.lease.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await
        .unwrap();
    assert!(!repeat.dispatch_allowed);
    assert!(repeat.cancel_requested);
    let replay = assignment(run(s, &c).await);
    assert_eq!(replay.authority.execution_remaining_ms, 0);
    assert_eq!(snapshot(s).await.occupied, 0);
    let settlement = completed(&a, Quiescence::Confirmed, json!("business response"));
    s.settle(&settlement).await.unwrap();
    assert!(
        s.close_queue_hold(&actor(PrincipalClass::Worker), &close(&a))
            .await
            .unwrap()
            .already_closed
    );
    assert_eq!(holds(s).await[0].revision, r.revision);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn confirmation_only_timeout_survives_retirement_of_source_and_reopens_only_on_close() {
    let db = fixture().await;
    let s = &db.store;
    configure(
        s,
        &put(
            "limit",
            None,
            Some(1),
            Some(QueueHoldTimeout {
                after_ms: 60000,
                release: QueueTimeoutRelease::Confirm,
            }),
        ),
    )
    .await;
    submit(s, "retained").await;
    let a = assignment(run(s, &acquire_command(&session(s).await, 0, 1)).await);
    dispatch(s, &a).await;
    sqlx::query("UPDATE queue_concurrency_holds SET acquired_at_ms=acquired_at_ms-70000,deadline_ms=deadline_ms-70000 WHERE attempt_id=$1").bind(&a.lease.owner.attempt_id).execute(&s.pool).await.unwrap();
    s.expire_batch(50).await.unwrap();
    let h = holds(s).await.pop().unwrap();
    assert_eq!(h.state, QueueHoldState::Uncertain);
    assert!(h.stop_requested_at.is_some() && h.force_at.is_none());
    s.cancel(&scope(), &a.lease.owner.task_id).await.unwrap();
    expire(s, &a).await;
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=1 WHERE session_id=$1")
        .bind(&a.lease.owner.worker_session_id)
        .execute(&s.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET submitted_at_ms=1,available_at_ms=1,terminal_at_ms=2,cancel_requested_at_ms=2 WHERE task_id=$1 AND state='cancelled'")
        .bind(&a.lease.owner.task_id).execute(&s.pool).await.unwrap();
    for _ in 0..50 {
        s.retain_batch(
            &scope(),
            &RetentionPolicy::default(),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        if !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_id=$1)")
            .bind(&a.lease.owner.task_id)
            .fetch_one(&s.pool)
            .await
            .unwrap()
        {
            break;
        }
    }
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM tasks WHERE task_id=$1)")
            .bind(&a.lease.owner.task_id)
            .fetch_one(&s.pool)
            .await
            .unwrap()
    );
    s.expire_batch(50).await.unwrap();
    assert_eq!(snapshot(s).await.occupied, 1);
    assert_eq!(
        s.close_queue_hold(&actor(PrincipalClass::Worker), &close(&a))
            .await
            .unwrap()
            .release_kind,
        QueueHoldReleaseKind::Confirmed
    );
    assert_eq!(snapshot(s).await.occupied, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn hot_first_source_cannot_block_unrelated_timeout_with_batch_size_one() {
    let db = fixture().await;
    let s = &db.store;
    configure(
        s,
        &put(
            "two",
            None,
            Some(2),
            Some(QueueHoldTimeout {
                after_ms: 60000,
                release: QueueTimeoutRelease::Confirm,
            }),
        ),
    )
    .await;
    submit(s, "blocked").await;
    let first = assignment(run(s, &acquire_command(&session(s).await, 0, 1)).await);
    submit(s, "independent").await;
    let second = assignment(run(s, &acquire_command(&session(s).await, 0, 1)).await);
    sqlx::query("UPDATE queue_concurrency_holds SET acquired_at_ms=acquired_at_ms-100000,deadline_ms=CASE WHEN attempt_id=$1 THEN acquired_at_ms-90000 ELSE acquired_at_ms-70000 END")
        .bind(&first.lease.owner.attempt_id).execute(&s.pool).await.unwrap();
    let mut lock = s.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&first.lease.owner.task_id)
        .execute(&mut *lock)
        .await
        .unwrap();
    s.advance_queue_timeouts(1).await.unwrap();
    assert_eq!(snapshot(s).await.occupied, 2);
    s.advance_queue_timeouts(1).await.unwrap();
    let rows = holds(s).await;
    assert_eq!(
        rows.iter()
            .find(|h| h.attempt_id == second.lease.owner.attempt_id)
            .unwrap()
            .release_kind,
        Some(QueueHoldReleaseKind::NeverStarted)
    );
    assert_eq!(snapshot(s).await.occupied, 1);
    lock.commit().await.unwrap();
    s.advance_queue_timeouts(1).await.unwrap();
    assert_eq!(snapshot(s).await.occupied, 0);
    db.finish().await;
}
