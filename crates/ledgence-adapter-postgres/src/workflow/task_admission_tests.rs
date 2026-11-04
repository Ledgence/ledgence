//! Standalone tasks and workflows share one resource, but a task has no
//! activation or workflow identity. Each case uses a dedicated PostgreSQL DB.
use super::*;

fn submission(key: &str) -> SubmitCommand {
    let mut c = command();
    c.idempotency_key = key.into();
    c.input.retry_policy.max_attempts = 1;
    c.input.admission = Some(TaskAdmissionRequirement {
        resource: "model".into(),
    });
    c
}
async fn submit(s: &PostgresStore, key: &str) -> TaskSnapshot {
    s.accept_resolved_submission(&submission(key), &descriptor())
        .await
        .unwrap()
}
async fn session(s: &PostgresStore) -> WorkerSession {
    s.open_task_admission_session(
        &worker(),
        &TaskAdmissionSessionCommand {
            scope: scope(),
            queue: "python".into(),
            concurrency: 1,
        },
    )
    .await
    .unwrap()
}
fn occurrence(t: &TaskSnapshot, generation: u32) -> TaskAdmissionOccurrence {
    TaskAdmissionOccurrence {
        scope: t.scope(),
        task_id: t.task_id.clone(),
        generation,
    }
}
async fn ledger(s: &PostgresStore, o: TaskAdmissionOccurrence) -> TaskAdmissionSnapshot {
    s.inspect_task_admission(&operator(), &TaskAdmissionQuery { occurrence: o })
        .await
        .unwrap()
}
async fn prepare(
    s: &PostgresStore,
    key: &str,
) -> (TaskSnapshot, Assignment, TaskAdmissionStartCommand) {
    let t = submit(s, key).await;
    assert_eq!(advance(s).await.task_offered, 1);
    let session = session(s).await;
    let acquire = acquire_command(&session, 0, 1);
    let a = assignment(s.acquire(&acquire).await.unwrap());
    let replay = assignment(s.acquire(&acquire).await.unwrap());
    assert_eq!(a.task_admission, replay.task_admission);
    let start = TaskAdmissionStartCommand {
        owner: a.lease.owner.clone(),
        offer_generation: a.task_admission.as_ref().unwrap().offer_generation,
        start_id: format!("start:{key}"),
    };
    (t, a, start)
}
fn close(
    c: &TaskAdmissionStartCommand,
    outcome: TaskAdmissionCloseOutcome,
) -> TaskAdmissionCloseCommand {
    TaskAdmissionCloseCommand {
        owner: c.owner.clone(),
        offer_generation: c.offer_generation,
        start_id: c.start_id.clone(),
        operation_id: "close:1".into(),
        outcome,
    }
}
async fn expire(s: &PostgresStore, a: &Assignment) {
    let mut tx = s.pool.begin().await.unwrap();
    sqlx::query("UPDATE attempts SET expires_at_ms=1 WHERE attempt_id=$1")
        .bind(&a.lease.owner.attempt_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=1 WHERE task_id=$1")
        .bind(&a.lease.owner.task_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(s.expire_batch(16).await.unwrap().expired, 1);
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_no_worker_while_waiting_mixed_fifo_and_cancel() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    let task = submit(s, "waiting").await;
    let o = occurrence(&task, 1);
    let first = ledger(s, o.clone()).await;
    assert_eq!(first.state, TaskAdmissionState::Waiting);
    assert!(first.owner.is_none());
    assert!(
        s.accept_resolved_workflow(&submission("invalid-workflow"), &descriptor())
            .await
            .is_err()
    );
    assert_eq!(submit(s, "waiting").await.task_id, task.task_id);
    let ordinary = s
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let old = s.open_session(&scope(), "python", 1).await.unwrap();
    let acquired = assignment(s.acquire(&acquire_command(&old, 0, 1)).await.unwrap());
    assert_eq!(acquired.lease.owner.task_id, ordinary.task_id);
    let workflow = park(s, "workflow", "model").await;
    assert_eq!(advance(s).await.task_offered, 1);
    assert_eq!(
        ledger(s, o.clone()).await.state,
        TaskAdmissionState::Offered
    );
    let old = s.open_session(&scope(), "python", 1).await.unwrap();
    assert!(matches!(
        s.acquire(&acquire_command(&old, 0, 1)).await.unwrap(),
        AcquireReply::Empty { .. }
    ));
    assert_eq!(inspect(s, &workflow).await.state, AdmissionState::Waiting);
    assert_eq!(
        s.status(&scope(), &task.task_id)
            .await
            .unwrap()
            .attempt_count,
        0
    );
    assert_eq!(
        s.cancel(&scope(), &task.task_id).await.unwrap(),
        TaskState::Cancelled
    );
    assert_eq!(
        ledger(s, o).await.state,
        TaskAdmissionState::ClosedBeforeStart
    );
    assert_eq!(advance(s).await.offered, 1);
    assert_eq!(inspect(s, &workflow).await.state, AdmissionState::Offered);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_ready_grants_once_generic_settlement_cannot_release() {
    let mut policy = policy(1);
    policy.rate = Some(ResourceRate {
        numerator: 1,
        period_ms: 60_000,
        burst: 1,
    });
    let db = fixture(policy).await;
    let s = &db.store;
    let (t, a, c) = prepare(s, "grant").await;
    let o = occurrence(&t, 1);
    let preparing = ledger(s, o.clone()).await;
    assert_eq!(preparing.state, TaskAdmissionState::Preparing);
    assert!(preparing.offer.unwrap().expires_at > a.lease.expires_at);
    assert!(
        s.renew(&RenewCommand {
            owner: c.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Dispatch
        })
        .await
        .is_err()
    );
    let renewed = s
        .renew(&RenewCommand {
            owner: c.owner.clone(),
            sequence: 1,
            intent: RenewIntent::Prepare,
        })
        .await
        .unwrap();
    assert!(!renewed.dispatch_allowed);
    assert!(
        s.settle(&completed(&a, Quiescence::Confirmed, json!(1)))
            .await
            .is_err()
    );
    let worker = worker();
    let (x, y) = tokio::join!(
        s.start_task_admission(&worker, &c),
        s.start_task_admission(&worker, &c)
    );
    let receipts = [x.unwrap(), y.unwrap()];
    assert_eq!(
        receipts
            .iter()
            .filter(|r| matches!(r, TaskAdmissionStartReceipt::Granted { .. }))
            .count(),
        1
    );
    assert_eq!(resource(s).await.occupied, 1);
    for (sequence, intent) in [(2, RenewIntent::KeepAlive), (3, RenewIntent::Prepare)] {
        assert!(
            !s.renew(&RenewCommand {
                owner: c.owner.clone(),
                sequence,
                intent
            })
            .await
            .unwrap()
            .dispatch_allowed
        );
    }
    let replay = assignment(
        s.acquire(&AcquireCommand {
            scope: scope(),
            queue: "python".into(),
            worker_session_id: c.owner.worker_session_id.clone(),
            consumer_id: c.owner.consumer_id,
            sequence: 1,
        })
        .await
        .unwrap(),
    );
    assert!(!replay.authority.dispatch_allowed);
    assert!(matches!(
        s.start_task_admission(&worker, &c).await.unwrap(),
        TaskAdmissionStartReceipt::Existing { .. }
    ));
    s.settle(&completed(&a, Quiescence::Confirmed, json!(1)))
        .await
        .unwrap();
    assert_eq!(
        ledger(s, o.clone()).await.state,
        TaskAdmissionState::UnknownOccupied
    );
    assert_eq!(resource(s).await.unknown, 1);
    let closing = close(&c, TaskAdmissionCloseOutcome::Quiescent);
    let wrong = principal("other-worker", PrincipalClass::Worker);
    assert!(matches!(
        s.close_task_admission(&wrong, &closing).await,
        Err(ContractError::Forbidden)
    ));
    let (x, y) = tokio::join!(
        s.close_task_admission(&worker, &closing),
        s.close_task_admission(&worker, &closing)
    );
    let (x, y) = (x.unwrap(), y.unwrap());
    assert_ne!(x.already_accepted, y.already_accepted);
    assert_eq!(resource(s).await.occupied, 0);
    assert_eq!(ledger(s, o).await.state, TaskAdmissionState::Finished);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_close_before_start_requeues_without_spending_retry() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    let (t, _, c) = prepare(s, "defer").await;
    let o = occurrence(&t, 1);
    let closing = close(&c, TaskAdmissionCloseOutcome::NotInvoked);
    let w = worker();
    let receipt = s.close_task_admission(&w, &closing).await.unwrap();
    assert_eq!(receipt.disposition, TaskAdmissionCloseDisposition::Deferred);
    assert!(
        s.close_task_admission(&w, &closing)
            .await
            .unwrap()
            .already_accepted
    );
    assert!(matches!(
        s.start_task_admission(&w, &c).await.unwrap(),
        TaskAdmissionStartReceipt::Existing { .. }
    ));
    let mut changed = c.clone();
    changed.start_id = "replacement".into();
    assert!(matches!(
        s.start_task_admission(&w, &changed).await,
        Err(ContractError::Conflict)
    ));
    assert_eq!(ledger(s, o).await.source, None);
    let status = s.status(&scope(), &t.task_id).await.unwrap();
    assert_eq!(
        (
            status.state,
            status.attempt_count,
            status.admission_deferrals
        ),
        (TaskState::Queued, 1, 1)
    );
    assert_eq!(
        ledger(s, occurrence(&t, 2)).await.state,
        TaskAdmissionState::Waiting
    );
    tokio::time::sleep(Duration::from_millis(
        TASK_ADMISSION_DEFERRAL_BACKOFF_MS + 10,
    ))
    .await;
    assert_eq!(advance(s).await.task_offered, 1);
    let sess = session(s).await;
    let a = assignment(s.acquire(&acquire_command(&sess, 0, 1)).await.unwrap());
    assert_eq!(a.lease.owner.generation, 2);
    let next = TaskAdmissionStartCommand {
        owner: a.lease.owner.clone(),
        offer_generation: a.task_admission.unwrap().offer_generation,
        start_id: "second".into(),
    };
    assert!(matches!(
        s.start_task_admission(&w, &next).await.unwrap(),
        TaskAdmissionStartReceipt::Granted { .. }
    ));
    let stopped = s
        .close_task_admission(&w, &close(&next, TaskAdmissionCloseOutcome::NotInvoked))
        .await
        .unwrap();
    assert_eq!(
        stopped.disposition,
        TaskAdmissionCloseDisposition::ChargedNotInvoked
    );
    let state = s.status(&scope(), &t.task_id).await.unwrap();
    assert_eq!(
        (state.state, state.attempt_count, state.admission_deferrals),
        (TaskState::Failed, 2, 1)
    );
    assert!(s.result(&scope(), &t.task_id).await.is_ok());
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_lease_expiry_before_vs_after_start_and_prep_failure() {
    for mode in 0..3 {
        let db = fixture(policy(1)).await;
        let s = &db.store;
        let (t, a, c) = prepare(s, "lease").await;
        if mode == 1 {
            s.start_task_admission(&worker(), &c).await.unwrap();
        }
        if mode == 2 {
            s.close_task_admission(&worker(), &close(&c, TaskAdmissionCloseOutcome::Quiescent))
                .await
                .unwrap();
        }
        expire(s, &a).await;
        let state = s.status(&scope(), &t.task_id).await.unwrap();
        let snap = ledger(s, occurrence(&t, 1)).await;
        match mode {
            0 => {
                assert_eq!(
                    (state.state, state.admission_deferrals),
                    (TaskState::Queued, 1)
                );
                assert_eq!(snap.state, TaskAdmissionState::ClosedBeforeStart);
            }
            1 => {
                assert_eq!(
                    (state.state, state.admission_deferrals),
                    (TaskState::Failed, 0)
                );
                assert_eq!(snap.state, TaskAdmissionState::UnknownOccupied);
                assert_eq!(resource(s).await.occupied, 1);
            }
            _ => {
                assert_eq!(
                    (state.state, state.admission_deferrals),
                    (TaskState::Failed, 0)
                );
                assert_eq!(snap.state, TaskAdmissionState::ClosedBeforeStart);
            }
        }
        db.finish().await;
    }
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_policy_invalidation_new_broker_publication_same_generation() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    sqlx::query("INSERT INTO dispatch_routes(tenant_id,namespace,queue,destination) VALUES('acme','billing','python','standard')").execute(&s.pool).await.unwrap();
    let t = submit(s, "broker").await;
    let o = occurrence(&t, 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dispatch_intents")
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(advance(s).await.task_offered, 1);
    let first: String = sqlx::query_scalar("SELECT publication_id FROM dispatch_intents")
        .fetch_one(&s.pool)
        .await
        .unwrap();
    let old = s.open_session(&scope(), "python", 1).await.unwrap();
    let dispatch = DispatchRef {
        scope: scope(),
        queue: "python".into(),
        task_id: t.task_id.clone(),
        generation: 1,
    };
    let legacy = ClaimCommand {
        acquisition: acquire_command(&old, 0, 1),
        dispatch: dispatch.clone(),
    };
    let ClaimDisposition::Deferred { available_at } =
        s.claim_dispatch(&legacy).await.unwrap().disposition
    else {
        panic!("missing legacy handoff")
    };
    let duplicate = ClaimCommand {
        acquisition: acquire_command(&old, 0, 2),
        ..legacy.clone()
    };
    assert!(
        matches!(s.claim_dispatch(&duplicate).await.unwrap().disposition,ClaimDisposition::Deferred{available_at:second} if second==available_at)
    );
    assert_eq!(
        s.status(&scope(), &t.task_id).await.unwrap().attempt_count,
        0
    );
    let mut disabled = policy(1);
    disabled.enabled = false;
    put(s, "model", disabled, Some(1), "disable").await;
    let session = session(s).await;
    let c = ClaimCommand {
        acquisition: acquire_command(&session, 0, 1),
        dispatch,
    };
    let reply = s.claim_dispatch(&c).await.unwrap();
    assert!(
        matches!(reply.disposition,ClaimDisposition::AdmissionDeferred{ref occurrence} if *occurrence==o)
    );
    assert_eq!(
        ledger(s, o.clone()).await.state,
        TaskAdmissionState::Waiting
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatch_intents")
            .fetch_one(&s.pool)
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        s.claim_dispatch(&c).await.unwrap().disposition,
        ClaimDisposition::AdmissionDeferred { .. }
    ));
    tokio::time::sleep(Duration::from_millis(
        TASK_ADMISSION_DEFERRAL_BACKOFF_MS + 10,
    ))
    .await;
    assert_eq!(advance(s).await.task_offered, 0);
    let times: (i64, Option<i64>) = sqlx::query_as(
        "SELECT available_at_ms,last_rewait_at_ms FROM task_admission_requests WHERE task_id=$1",
    )
    .bind(&t.task_id)
    .fetch_one(&s.pool)
    .await
    .unwrap();
    assert_eq!(times, (0, Some(available_at as i64)));
    let aged = ClaimCommand {
        acquisition: acquire_command(&old, 0, 3),
        ..legacy
    };
    assert!(
        matches!(s.claim_dispatch(&aged).await.unwrap().disposition,ClaimDisposition::Deferred{available_at:second} if second==available_at)
    );
    put(s, "model", policy(1), Some(2), "enable").await;
    assert_eq!(advance(s).await.task_offered, 1);
    let second: (String, i64) =
        sqlx::query_as("SELECT publication_id,generation FROM dispatch_intents")
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_ne!(first, second.0);
    assert_eq!(second.1, 1);
    let c = ClaimCommand {
        acquisition: acquire_command(&session, 0, 2),
        ..c
    };
    let reply = s.claim_dispatch(&c).await.unwrap();
    let ClaimDisposition::Claimed { reply } = reply.disposition else {
        panic!("missing claim")
    };
    let a = assignment(reply);
    assert_eq!(a.task_admission.unwrap().offer_generation, 2);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_partial_start_rolls_back_and_close_accepts_clock_rollback() {
    let mut p = policy(1);
    p.rate = Some(ResourceRate {
        numerator: 1,
        period_ms: 60_000,
        burst: 1,
    });
    let db = fixture(p).await;
    let s = &db.store;
    let (t, a, c) = prepare(s, "rollback").await;
    let o = occurrence(&t, 1);
    sqlx::raw_sql("CREATE FUNCTION suppress_task_start() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_id IS NOT NULL THEN RETURN NULL; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_start BEFORE UPDATE ON task_admission_requests FOR EACH ROW EXECUTE FUNCTION suppress_task_start()").execute(&s.pool).await.unwrap();
    assert!(s.start_task_admission(&worker(), &c).await.is_err());
    assert!(
        !s.inspect_attempt(&scope(), &t.task_id, &a.lease.owner.attempt_id)
            .await
            .unwrap()
            .execution_may_have_started
    );
    let snap = ledger(s, o.clone()).await;
    assert_eq!(snap.state, TaskAdmissionState::Preparing);
    assert!(snap.source.is_none());
    assert_eq!(resource(s).await.offered, 1);
    sqlx::query("DROP TRIGGER fail_start ON task_admission_requests")
        .execute(&s.pool)
        .await
        .unwrap();
    s.start_task_admission(&worker(), &c).await.unwrap();
    // Simulate a DB clock rollback by moving the last accepted evidence into
    // the future. The cleaned worker must remain able to report that fact.
    let mut snap = ledger(s, o).await;
    let source = snap.source.as_mut().unwrap();
    source.accepted_at += 1_000;
    source.start_deadline_at += 1_000;
    snap.updated_at = source.accepted_at;
    snap.validate().unwrap();
    sqlx::query(
        "UPDATE task_admission_requests SET updated_at_ms=$1,snapshot_bytes=$2 WHERE task_id=$3",
    )
    .bind(codec::ms(snap.updated_at).unwrap())
    .bind(codec::encode(&snap).unwrap())
    .bind(&t.task_id)
    .execute(&s.pool)
    .await
    .unwrap();
    let c = close(&c, TaskAdmissionCloseOutcome::Quiescent);
    let receipt = s.close_task_admission(&worker(), &c).await.unwrap();
    assert!(receipt.accepted_at >= snap.updated_at);
    assert!(
        s.close_task_admission(&worker(), &c)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(resource(s).await.occupied, 0);
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_admin_release_then_retention_late_original_cleanup() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    let (t, a, c) = prepare(s, "retained").await;
    let o = occurrence(&t, 1);
    s.start_task_admission(&worker(), &c).await.unwrap();
    expire(s, &a).await;
    let unknown = ledger(s, o.clone()).await;
    let source = unknown.source.as_ref().unwrap();
    let release = TaskAdmissionReleaseCommand {
        occurrence: o.clone(),
        source_id: source.source_id.clone(),
        expected_revision: unknown.revision,
        request_id: "release:1".into(),
        reason: "Worker decommissioned".into(),
    };
    let wrong = TaskAdmissionReleaseCommand {
        expected_revision: release.expected_revision - 1,
        ..release.clone()
    };
    assert!(matches!(
        s.release_task_admission(&operator(), &wrong).await,
        Err(ContractError::Conflict)
    ));
    sqlx::query(
        "UPDATE tasks SET submitted_at_ms=0,available_at_ms=0,terminal_at_ms=1 WHERE task_id=$1",
    )
    .bind(&t.task_id)
    .execute(&s.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=1")
        .execute(&s.pool)
        .await
        .unwrap();
    for _ in 0..24 {
        s.retain_batch(
            &scope(),
            &RetentionPolicy {
                retain_for_ms: MIN_RETENTION_MS,
                batch_size: 64,
            },
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        s.status(&scope(), &t.task_id).await.unwrap().state,
        TaskState::Failed
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM worker_sessions")
            .fetch_one(&s.pool)
            .await
            .unwrap(),
        0
    );
    let released = s
        .release_task_admission(&operator(), &release)
        .await
        .unwrap();
    assert!(!released.already_accepted);
    assert!(
        s.release_task_admission(&operator(), &release)
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(resource(s).await.occupied, 0);
    for _ in 0..48 {
        s.retain_batch(
            &scope(),
            &RetentionPolicy {
                retain_for_ms: MIN_RETENTION_MS,
                batch_size: 64,
            },
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        if sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&s.pool)
            .await
            .unwrap()
            == 0
        {
            break;
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tasks")
            .fetch_one(&s.pool)
            .await
            .unwrap(),
        0
    );
    let reopened = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&reopened, AdmissionDispatchPolicy::Enabled).await;
    let cleanup = close(&c, TaskAdmissionCloseOutcome::Quiescent);
    let receipt = reopened
        .close_task_admission(&worker(), &cleanup)
        .await
        .unwrap();
    assert_eq!(
        receipt.disposition,
        TaskAdmissionCloseDisposition::AlreadyReleased
    );
    assert_eq!(
        ledger(&reopened, o).await.state,
        TaskAdmissionState::AdministrativelyReleased
    );
    assert_eq!(resource(&reopened).await.occupied, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM operator_audit_events WHERE event_kind='task_admission_released'"
        )
        .fetch_one(&s.pool)
        .await
        .unwrap(),
        1
    );
    reopened.close().await;
    db.finish().await;
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_last_representable_generation_free_deferral_is_terminal() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    let t = submit(s, "limit").await;
    let mut snapshot = ledger(s, occurrence(&t, 1)).await;
    snapshot.occurrence.generation = MAX_TASK_ATTEMPT_GENERATION;
    // Synthetic boundary: do not allocate billions of immutable prior rows.
    sqlx::query("UPDATE tasks SET attempt_count=$2,admission_deferrals=$2 WHERE task_id=$1")
        .bind(&t.task_id)
        .bind(i64::from(MAX_TASK_ATTEMPT_GENERATION - 1))
        .execute(&s.pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_admission_requests SET generation=$2,snapshot_bytes=$3 WHERE task_id=$1",
    )
    .bind(&t.task_id)
    .bind(i64::from(MAX_TASK_ATTEMPT_GENERATION))
    .bind(codec::encode(&snapshot).unwrap())
    .execute(&s.pool)
    .await
    .unwrap();
    assert_eq!(advance(s).await.task_offered, 1);
    let session = session(s).await;
    let a = assignment(s.acquire(&acquire_command(&session, 0, 1)).await.unwrap());
    assert_eq!(a.lease.owner.generation, MAX_TASK_ATTEMPT_GENERATION);
    let c = TaskAdmissionStartCommand {
        owner: a.lease.owner,
        offer_generation: a.task_admission.unwrap().offer_generation,
        start_id: "last".into(),
    };
    s.close_task_admission(&worker(), &close(&c, TaskAdmissionCloseOutcome::NotInvoked))
        .await
        .unwrap();
    let result = s.result(&scope(), &t.task_id).await.unwrap();
    assert_eq!(result.task.admission_deferrals, MAX_TASK_ATTEMPT_GENERATION);
    assert!(matches!(
        result.outcome,
        Some(TaskOutcome::Failed {
            execution_may_have_started: false,
            failure: TaskFailure::AdmissionGenerationExhausted {},
            ..
        })
    ));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_unclaimed_expired_task_rotates_behind_waiting_workflow() {
    let mut p = policy(1);
    p.offer_timeout_ms = 1000;
    let db = fixture(p).await;
    let s = &db.store;
    let t = submit(s, "no-worker").await;
    let w = park(s, "next", "model").await;
    let first = advance(s).await;
    assert_eq!(
        (first.task_offered, first.offered, first.resumed),
        (1, 0, 0)
    );
    tokio::time::sleep(Duration::from_millis(1005)).await;
    let next = advance(s).await;
    assert_eq!((next.task_offered, next.offered, next.resumed), (0, 1, 1));
    assert_eq!(
        ledger(s, occurrence(&t, 1)).await.state,
        TaskAdmissionState::Waiting
    );
    assert_eq!(inspect(s, &w).await.state, AdmissionState::Offered);
    assert_eq!(
        s.status(&scope(), &t.task_id).await.unwrap().attempt_count,
        0
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_upgrade_from_populated_pre_task_admission_schema() {
    let db = TestDb::without_migrations().await;
    let prior = sqlx::migrate::Migrator::with_migrations(
        crate::MIGRATOR
            .iter()
            .filter(|v| v.version < 20261103090000)
            .cloned()
            .collect(),
    );
    prior.run(&db.store.pool).await.unwrap();
    db.begin_legacy_task_seed().await;
    bind(&db.store, AdmissionDispatchPolicy::Enabled).await;
    put(&db.store, "model", policy(1), None, "create").await;
    let w = park(&db.store, "legacy-workflow", "model").await;
    let t = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let old = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let saved:(Vec<u8>,Vec<u8>)=sqlx::query_as("SELECT t.input_bytes,a.event_bytes FROM tasks t JOIN attempts a ON a.task_id=t.task_id WHERE t.task_id=$1").bind(&t.task_id).fetch_one(&db.store.pool).await.unwrap();
    db.finish_legacy_task_seed().await;
    db.store.migrate().await.unwrap();
    let s = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    bind(&s, AdmissionDispatchPolicy::Enabled).await;
    assert_eq!(saved,sqlx::query_as("SELECT t.input_bytes,a.event_bytes FROM tasks t JOIN attempts a ON a.task_id=t.task_id WHERE t.task_id=$1").bind(&t.task_id).fetch_one(&s.pool).await.unwrap());
    assert_eq!(
        s.extend_session(&session.id).await.unwrap().features,
        Vec::<String>::new()
    );
    assert!(
        s.inspect_attempt(&scope(), &t.task_id, &old.lease.owner.attempt_id)
            .await
            .unwrap()
            .task_admission
            .is_none()
    );
    let governed = submit(&s, "first-after-upgrade").await;
    assert_eq!(advance(&s).await.offered, 1);
    assert_eq!(inspect(&s, &w).await.state, AdmissionState::Offered);
    assert_eq!(
        ledger(&s, occurrence(&governed, 1)).await.state,
        TaskAdmissionState::Waiting
    );
    s.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_preparation_failure_settles_first_then_authenticated_cleanup() {
    for (quiet, expire_before_close) in [
        (Quiescence::Confirmed, false),
        (Quiescence::Unconfirmed, false),
        (Quiescence::Unconfirmed, true),
    ] {
        let db = fixture(policy(1)).await;
        let s = &db.store;
        let (t, a, c) = prepare(s, "failure-first").await;
        // A revision change revokes the old Preparing epoch. It must not turn
        // an actual failed preparation into a free policy deferral.
        put(s, "model", policy(1), Some(1), "revision").await;
        let report = SettleCommand {
            owner: c.owner.clone(),
            operation_id: "actual-prep-failure".into(),
            processing_trace: None,
            provider_probe_failure: None,
            quiescence: quiet,
            report: AttemptReport::Failed(ExecutionFailure {
                observations: None,
                context: Box::new(ExecutionContext {
                    identity: InvocationIdentity::from(&a.event),
                    program: a.descriptor.program.clone(),
                    digest: a.descriptor.digest.clone(),
                }),
                phase: Phase::Preparation,
                error: Error::new(ErrorKind::Io, "fixture preparation failed"),
                execution_may_have_started: false,
                cleanup_error: None,
            }),
        };
        let accepted = s.settle(&report).await.unwrap();
        assert_eq!(
            accepted.task_state,
            if quiet == Quiescence::Confirmed {
                TaskState::Failed
            } else {
                TaskState::Active
            }
        );
        let snap = ledger(s, occurrence(&t, 1)).await;
        assert_eq!(snap.state, TaskAdmissionState::ClosedBeforeStart);
        assert!(snap.source.is_none());
        assert_eq!(resource(s).await.occupied, 0);
        assert!(matches!(
            s.start_task_admission(&worker(), &c).await.unwrap(),
            TaskAdmissionStartReceipt::Existing { .. }
        ));
        if expire_before_close {
            expire(s, &a).await;
        }
        let cleanup = close(&c, TaskAdmissionCloseOutcome::Quiescent);
        let closed = s.close_task_admission(&worker(), &cleanup).await.unwrap();
        assert_eq!(closed.disposition, TaskAdmissionCloseDisposition::Closed);
        assert!(closed.source.is_none());
        let result = s.result(&scope(), &t.task_id).await.unwrap();
        assert_eq!(
            (
                result.task.state,
                result.task.attempt_count,
                result.task.admission_deferrals
            ),
            (TaskState::Failed, 1, 0)
        );
        assert!(
            matches!(result.outcome,Some(TaskOutcome::Failed{failure:TaskFailure::Execution{..},quiescence,..}) if quiescence==if expire_before_close {Quiescence::Unconfirmed} else {Quiescence::Confirmed})
        );
        let attempt = s
            .inspect_attempt(&scope(), &t.task_id, &c.owner.attempt_id)
            .await
            .unwrap();
        assert_eq!(attempt.settlement.unwrap().command.quiescence, quiet);
        assert!(
            s.close_task_admission(&worker(), &cleanup)
                .await
                .unwrap()
                .already_accepted
        );
        db.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_broker_publication_suppression_rolls_back_offer_and_handoff() {
    let db = fixture(policy(1)).await;
    let s = &db.store;
    sqlx::query("INSERT INTO dispatch_routes(tenant_id,namespace,queue,destination) VALUES('acme','billing','python','standard')").execute(&s.pool).await.unwrap();
    let task = submit(s, "suppressed-publication").await;
    let o = occurrence(&task, 1);
    sqlx::raw_sql("CREATE FUNCTION suppress_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER suppress_insert BEFORE INSERT ON dispatch_intents FOR EACH ROW EXECUTE FUNCTION suppress_publication()").execute(&s.pool).await.unwrap();
    assert!(matches!(
        s.advance_admissions(&AdmissionMaintenanceQuery { limit: 1 })
            .await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        ledger(s, o.clone()).await.state,
        TaskAdmissionState::Waiting
    );
    assert_eq!(resource(s).await.occupied, 0);
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT admission_ready FROM tasks WHERE task_id=$1")
            .bind(&task.task_id)
            .fetch_one(&s.pool)
            .await
            .unwrap()
    );
    sqlx::query("DROP TRIGGER suppress_insert ON dispatch_intents")
        .execute(&s.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO dispatch_intents(task_id,generation,destination,available_at_ms,next_publish_at_ms,publication_id) VALUES($1,1,'standard',0,0,'leftover')").bind(&task.task_id).execute(&s.pool).await.unwrap();
    assert!(
        s.advance_admissions(&AdmissionMaintenanceQuery { limit: 1 })
            .await
            .is_err()
    );
    assert_eq!(
        ledger(s, o.clone()).await.state,
        TaskAdmissionState::Waiting
    );
    sqlx::query("DELETE FROM dispatch_intents WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&s.pool)
        .await
        .unwrap();
    assert_eq!(advance(s).await.task_offered, 1);
    let publication: String =
        sqlx::query_scalar("SELECT publication_id FROM dispatch_intents WHERE task_id=$1")
            .bind(&task.task_id)
            .fetch_one(&s.pool)
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER suppress_delete BEFORE DELETE ON dispatch_intents FOR EACH ROW EXECUTE FUNCTION suppress_publication()").execute(&s.pool).await.unwrap();
    let old = s.open_session(&scope(), "python", 1).await.unwrap();
    let c = ClaimCommand {
        acquisition: acquire_command(&old, 0, 1),
        dispatch: DispatchRef {
            scope: scope(),
            queue: "python".into(),
            task_id: task.task_id.clone(),
            generation: 1,
        },
    };
    assert!(matches!(
        s.claim_dispatch(&c).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        ledger(s, o.clone()).await.state,
        TaskAdmissionState::Offered
    );
    assert_eq!(resource(s).await.offered, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatch_claim_receipts")
            .fetch_one(&s.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT publication_id FROM dispatch_intents WHERE task_id=$1"
        )
        .bind(&task.task_id)
        .fetch_one(&s.pool)
        .await
        .unwrap(),
        publication
    );
    let current = session(s).await;
    assert!(matches!(
        s.claim_dispatch(&ClaimCommand {
            acquisition: acquire_command(&current, 0, 1),
            ..c.clone()
        })
        .await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        s.status(&scope(), &task.task_id)
            .await
            .unwrap()
            .attempt_count,
        0
    );
    sqlx::query("DROP TRIGGER suppress_delete ON dispatch_intents")
        .execute(&s.pool)
        .await
        .unwrap();
    assert!(matches!(
        s.claim_dispatch(&c).await.unwrap().disposition,
        ClaimDisposition::Deferred { .. }
    ));
    assert_eq!(ledger(s, o).await.state, TaskAdmissionState::Waiting);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_integrated_selection_orders_heads_skips_busy_tasks_and_never_loses_loser() {
    let db = fixture(policy(4)).await;
    let s = &db.store;
    // Three ordinary tasks, including the oldest deliberately locked one.
    let mut ordinary = Vec::new();
    for key in ["old", "later", "latest"] {
        let mut c = command();
        c.idempotency_key = key.into();
        ordinary.push(
            s.accept_resolved_submission(&c, &descriptor())
                .await
                .unwrap(),
        );
    }
    let governed = submit(s, "governed-early").await;
    let other = submit(s, "governed-late").await;
    assert_eq!(advance(s).await.task_offered, 2);
    for (task, at) in [
        (&ordinary[0], 10),
        (&governed, 20),
        (&ordinary[1], 30),
        (&other, 40),
        (&ordinary[2], 50),
    ] {
        sqlx::query("UPDATE tasks SET submitted_at_ms=$1,available_at_ms=$1 WHERE task_id=$2")
            .bind(at as i64)
            .bind(&task.task_id)
            .execute(&s.pool)
            .await
            .unwrap();
    }
    let mut locked = s.pool.begin().await.unwrap();
    sqlx::query("SELECT task_id FROM tasks WHERE task_id=$1 FOR NO KEY UPDATE")
        .bind(&ordinary[0].task_id)
        .fetch_one(&mut *locked)
        .await
        .unwrap();
    let capable = session(s).await;
    let chosen = assignment(s.acquire(&acquire_command(&capable, 0, 1)).await.unwrap());
    assert_eq!(chosen.lease.owner.task_id, governed.task_id);
    locked.rollback().await.unwrap();
    let next = session(s).await;
    assert_eq!(
        assignment(s.acquire(&acquire_command(&next, 0, 1)).await.unwrap())
            .lease
            .owner
            .task_id,
        ordinary[0].task_id
    );
    // Independent sessions can skip a busy class head. Any head locked only
    // while choosing remains queued and available once that transaction exits.
    let (a, b) = (session(s).await, session(s).await);
    let (ca, cb) = (acquire_command(&a, 0, 1), acquire_command(&b, 0, 1));
    let (ra, rb) = tokio::join!(s.acquire(&ca), s.acquire(&cb));
    let mut ids = std::collections::BTreeSet::new();
    for (session, reply) in [(a, ra.unwrap()), (b, rb.unwrap())] {
        let reply = if matches!(reply, AcquireReply::Empty { .. }) {
            s.acquire(&acquire_command(&session, 0, 2)).await.unwrap()
        } else {
            reply
        };
        assert!(ids.insert(assignment(reply).lease.owner.task_id));
    }
    let final_session = session(s).await;
    assert!(
        ids.insert(
            assignment(
                s.acquire(&acquire_command(&final_session, 0, 1))
                    .await
                    .unwrap()
            )
            .lease
            .owner
            .task_id
        )
    );
    assert_eq!(
        ids,
        std::collections::BTreeSet::from([
            ordinary[1].task_id.clone(),
            ordinary[2].task_id.clone(),
            other.task_id
        ])
    );
    db.finish().await;
}

// Assert the production statements themselves, under PostgreSQL's generic
// prepared plan, against overwhelmingly older incompatible offers.
#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_integrated_generic_plan_seeks_each_class_with_bounded_buffers() {
    let db = TestDb::new().await;
    let mut tx = db.store.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO tasks(task_id,run_id,tenant_id,namespace,queue,idempotency_key,input_bytes,descriptor_bytes,state,submitted_at_ms,available_at_ms,attempt_count,admission_resource) SELECT 'task_'||i,'run_'||i,'acme','billing','mixed','key_'||i,'{}'::bytea,'{}'::bytea,'queued',i,i,0,CASE WHEN i<=60000 THEN 'model' END FROM generate_series(1,60001) i").execute(&mut *tx).await.unwrap();
    sqlx::query("ANALYZE tasks")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SET LOCAL plan_cache_mode=force_generic_plan")
        .execute(&mut *tx)
        .await
        .unwrap();
    use sqlx::AssertSqlSafe;
    for (name, query, indexes) in [
        (
            "old",
            include_str!("../../queries/acquire_ordinary.sql"),
            vec!["tasks_due_ordinary"],
        ),
        (
            "capable",
            include_str!("../../queries/acquire_admission_capable.sql"),
            vec!["tasks_due_ordinary", "tasks_due_admitted", "tasks_pkey"],
        ),
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "PREPARE {name}(text,text,text,bigint) AS {query}"
        )))
        .execute(&mut *tx)
        .await
        .unwrap();
        let plan: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE {name}('acme','billing','mixed',100000)"
        )))
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let root = &plan[0]["Plan"];
        let mut nodes = vec![root];
        let mut found = std::collections::BTreeSet::new();
        while let Some(node) = nodes.pop() {
            if let Some(index) = node["Index Name"].as_str() {
                found.insert(index);
            }
            assert_ne!(node["Node Type"].as_str(), Some("Seq Scan"), "{plan}");
            assert!(
                node["Rows Removed by Filter"].as_u64().unwrap_or(0) <= 2,
                "{plan}"
            );
            if let Some(children) = node["Plans"].as_array() {
                nodes.extend(children);
            }
        }
        for required in indexes {
            assert!(found.contains(required), "{plan}");
        }
        let buffers = root["Shared Hit Blocks"].as_u64().unwrap_or(0)
            + root["Shared Read Blocks"].as_u64().unwrap_or(0);
        assert!(buffers < 128, "{plan}");
        eprintln!("A3 generic {name}: 60,000 incompatible offers, {buffers} shared buffers");
    }
    tx.rollback().await.unwrap();
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn task_admission_acquisition_returns_version_observed_by_winning_head_lock() {
    let db = TestDb::new().await;
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let mut writer = db.store.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(7046632)")
        .execute(&mut *writer)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET attempt_count=1 WHERE task_id=$1")
        .bind(&task.task_id)
        .execute(&mut *writer)
        .await
        .unwrap();
    let pool = db.store.pool.clone();
    let reader = tokio::spawn(async move {
        // Only the time cutoff has a test barrier. The actual heads, locks,
        // ordering and payload lookup are the production acquisition statement.
        let production = include_str!("../../queries/acquire_admission_capable.sql");
        let sql = production.replace(
            "$4",
            "(SELECT $4::bigint FROM pg_advisory_xact_lock(7046632))",
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind("acme")
            .bind("billing")
            .bind("python")
            .bind(i64::MAX)
            .fetch_one(&pool)
            .await
            .unwrap();
        row.try_get::<i64, _>("attempt_count").unwrap()
    });
    crate::tests::bounded(async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=7046632 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))").fetch_one(&db.store.pool).await.unwrap();
            if waiting {break}
            tokio::task::yield_now().await;
        }
    }).await;
    writer.commit().await.unwrap();
    assert_eq!(
        reader.await.unwrap(),
        1,
        "the payload must follow the locked row version"
    );
    db.finish().await;
}
