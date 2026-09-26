use super::*;
use crate::tests::{TestDb, acquire_command, assignment, command, descriptor, scope};
use ledgence_worker_api::{
    InvocationObservation, ProcessSlotState, SlotObservation, WorkerObservationScope,
    WorkerObservationSnapshot,
};

fn observation(session: &WorkerSession, sequence: u64) -> WorkerObservationCommand {
    let detailed = session.concurrency <= 1024;
    WorkerObservationCommand {
        schema_version: 1,
        worker_session_id: session.id.clone(),
        scope: session.scope.clone(),
        sequence: ConsoleU64(sequence),
        display_name: Some("Test worker".into()),
        snapshot: WorkerObservationSnapshot {
            configured_concurrency: session.concurrency as usize,
            accepting: true,
            active_consumers: 0,
            occupied_process_slots: 0,
            detail_state: if detailed {
                WorkerObservationDetailState::Available
            } else {
                WorkerObservationDetailState::UnsupportedCapacity
            },
            slots: if detailed {
                (0..session.concurrency)
                    .map(|slot_id| SlotObservation {
                        slot_id: slot_id as usize,
                        state: ProcessSlotState::Empty,
                        process_instance_id: None,
                        process_id: None,
                        program: None,
                        digest: None,
                        scope: None,
                        invocation: None,
                    })
                    .collect()
            } else {
                vec![]
            },
        },
    }
}
async fn detail(
    store: &PostgresStore,
    session: &WorkerSession,
    page: ConsolePagination,
) -> ConsoleWorkerDetail {
    let reply = store
        .query_workers(
            &session.scope,
            &WorkerObservationQuery::Inspect {
                worker_session_id: session.id.clone(),
                page,
            },
        )
        .await
        .unwrap();
    match reply {
        WorkerObservationReply::Inspect(reply) => *reply,
        _ => panic!("wrong reply"),
    }
}
async fn row_count(store: &PostgresStore) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM worker_observations")
        .fetch_one(&store.pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn receipt_replays_preserve_freshness_and_late_or_conflicting_reports_cannot_replace() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 2).await.unwrap();
    let mut command = observation(&session, 1);
    let first = db.store.record_observation(&command).await.unwrap();
    assert!(!first.already_received);
    command.snapshot.slots.reverse();
    let repeat = db.store.record_observation(&command).await.unwrap();
    assert!(repeat.already_received);
    assert_eq!(repeat.received_at, first.received_at);
    command.display_name = Some("Changed".into());
    assert_eq!(
        db.store.record_observation(&command).await.unwrap_err(),
        ContractError::Conflict
    );
    command.sequence = ConsoleU64(3);
    let latest = db.store.record_observation(&command).await.unwrap();
    command.sequence = ConsoleU64(2);
    assert_eq!(
        db.store.record_observation(&command).await.unwrap_err(),
        ContractError::Conflict
    );
    let result = detail(&db.store, &session, ConsolePagination::default()).await;
    assert_eq!(result.worker.snapshot_sequence, Some(ConsoleU64(3)));
    assert_eq!(result.worker.received_at, Some(latest.received_at));
    assert_eq!(result.worker.session_expires_at, session.expires_at);
    assert_eq!(row_count(&db.store).await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn observations_require_existing_live_session_binding_and_capacity_without_renewing_it() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let command = observation(&session, 1);
    for invalid in 0..3 {
        let mut changed = command.clone();
        match invalid {
            0 => changed.worker_session_id = "unknown-session".into(),
            1 => changed.scope.namespace = "foreign".into(),
            _ => changed.snapshot.configured_concurrency = 2,
        }
        if invalid == 2 {
            changed.snapshot.detail_state = WorkerObservationDetailState::Unavailable;
            changed.snapshot.slots.clear();
        }
        assert!(db.store.record_observation(&changed).await.is_err());
    }
    assert_eq!(row_count(&db.store).await, 0);
    db.store.record_observation(&command).await.unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0 WHERE session_id=$1")
        .bind(&session.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let mut later = command.clone();
    later.sequence = ConsoleU64(2);
    assert_eq!(
        db.store.record_observation(&later).await.unwrap_err(),
        ContractError::UnknownSession
    );
    let result = detail(&db.store, &session, ConsolePagination::default()).await;
    assert!(result.worker.session_expired);
    assert_eq!(result.worker.session_expires_at, 0);
    assert_eq!(result.worker.snapshot_sequence, Some(ConsoleU64(1)));
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn stable_slot_cursor_reaches_1024_while_snapshots_change_between_every_page() {
    let db = TestDb::new().await;
    let session = db
        .store
        .open_session(&scope(), "python", 1024)
        .await
        .unwrap();
    let mut command = observation(&session, 1);
    let mut page = ConsolePagination {
        limit: 37,
        cursor: None,
    };
    let mut seen = Vec::new();
    loop {
        command.snapshot.accepting = !command.snapshot.accepting;
        db.store.record_observation(&command).await.unwrap();
        let detail = detail(&db.store, &session, page.clone()).await;
        assert_eq!(detail.worker.snapshot_sequence, Some(command.sequence));
        assert_eq!(detail.worker.accepting, Some(command.snapshot.accepting));
        seen.extend(detail.slots.items.into_iter().map(|slot| slot.slot_id));
        let Some(cursor) = detail.slots.next_cursor else {
            break;
        };
        page.cursor = Some(cursor);
        command.sequence.0 += 1;
    }
    assert_eq!(seen, (0..1024).collect::<Vec<_>>());
    assert_eq!(row_count(&db.store).await, 1);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn old_workers_and_large_embedders_have_real_summaries_without_invented_slots() {
    let db = TestDb::new().await;
    let legacy = db.store.open_session(&scope(), "python", 4).await.unwrap();
    let initial = detail(&db.store, &legacy, ConsolePagination::default()).await;
    assert_eq!(
        initial.worker.freshness,
        WorkerObservationFreshness::Unavailable
    );
    assert_eq!(initial.worker.active_consumers, None);
    assert!(initial.slots.items.is_empty());
    let large = db
        .store
        .open_session(&scope(), "bulk", u32::MAX)
        .await
        .unwrap();
    db.store
        .record_observation(&observation(&large, 1))
        .await
        .unwrap();
    let result = detail(&db.store, &large, ConsolePagination::default()).await;
    assert_eq!(result.worker.capacity, u32::MAX);
    assert_eq!(
        result.worker.detail_state,
        Some(WorkerObservationDetailState::UnsupportedCapacity)
    );
    assert!(result.slots.items.is_empty());
    let reply = db
        .store
        .query_workers(
            &scope(),
            &WorkerObservationQuery::Workers {
                queue: Some("bulk".into()),
                page: ConsolePagination::default(),
            },
        )
        .await
        .unwrap();
    let WorkerObservationReply::Workers(page) = reply else {
        panic!("wrong reply")
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].worker_session_id, large.id);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn snapshots_validate_links_without_modifying_attempt_authority() {
    let db = TestDb::new().await;
    let task = db
        .store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let assigned = assignment(
        db.store
            .acquire(&acquire_command(&session, 0, 1))
            .await
            .unwrap(),
    );
    let mut snapshot = observation(&session, 1);
    snapshot.snapshot.active_consumers = 1;
    snapshot.snapshot.occupied_process_slots = 1;
    snapshot.snapshot.slots[0] = SlotObservation {
        slot_id: 0,
        state: ProcessSlotState::Executing,
        process_instance_id: Some("proc_1".into()),
        process_id: Some(12),
        program: Some(descriptor().program),
        digest: Some(descriptor().digest),
        scope: Some(WorkerObservationScope {
            tenant_id: scope().tenant_id,
            namespace: scope().namespace,
        }),
        invocation: Some(InvocationObservation {
            task_id: task.task_id.clone(),
            attempt_id: assigned.lease.owner.attempt_id.clone(),
        }),
    };
    db.store.record_observation(&snapshot).await.unwrap();
    let valid = detail(&db.store, &session, ConsolePagination::default()).await;
    assert_eq!(valid.slots.items[0].task_id, Some(task.task_id.clone()));
    assert_eq!(valid.slots.items[0].consumer_id, Some(0));
    snapshot.sequence = ConsoleU64(2);
    snapshot.snapshot.slots[0]
        .invocation
        .as_mut()
        .unwrap()
        .task_id = "unrelated-task".into();
    db.store.record_observation(&snapshot).await.unwrap();
    let invalid = detail(&db.store, &session, ConsolePagination::default()).await;
    assert_eq!(invalid.slots.items[0].task_id, None);
    assert_eq!(invalid.slots.items[0].attempt_id, None);
    assert_eq!(invalid.slots.items[0].consumer_id, None);
    assert_eq!(
        invalid.slots.items[0].link_diagnostic,
        Some(WorkerLinkDiagnostic::AuthorityMismatch)
    );
    let attempt = db
        .store
        .inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id)
        .await
        .unwrap();
    assert_eq!(attempt.lease.expires_at, assigned.lease.expires_at);
    assert!(!attempt.execution_may_have_started);
    let other = db.store.open_session(&scope(), "python", 1).await.unwrap();
    snapshot.worker_session_id = other.id.clone();
    snapshot.sequence = ConsoleU64(1);
    snapshot.snapshot.slots[0]
        .invocation
        .as_mut()
        .unwrap()
        .task_id = task.task_id;
    db.store.record_observation(&snapshot).await.unwrap();
    let foreign = detail(&db.store, &other, ConsolePagination::default()).await;
    assert_eq!(foreign.slots.items[0].attempt_id, None);
    assert_eq!(
        foreign.slots.items[0].link_diagnostic,
        Some(WorkerLinkDiagnostic::AuthorityMismatch)
    );
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn observation_freshness_uses_server_time_and_preserves_last_known_occupancy() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let mut command = observation(&session, 1);
    command.snapshot.occupied_process_slots = 1;
    command.snapshot.slots[0].state = ProcessSlotState::Unknown;
    db.store.record_observation(&command).await.unwrap();
    for (age, expected) in [
        (0, WorkerObservationFreshness::Fresh),
        (16000, WorkerObservationFreshness::Stale),
        (61000, WorkerObservationFreshness::NoRecentReport),
    ] {
        sqlx::query("UPDATE worker_observations SET received_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint-$2 WHERE session_id=$1")
            .bind(&session.id).bind(age as i64).execute(&db.store.pool).await.unwrap();
        let result = detail(&db.store, &session, ConsolePagination::default()).await;
        assert_eq!(result.worker.freshness, expected);
        assert_eq!(result.worker.occupied_process_slots, Some(1));
        assert_eq!(result.slots.items[0].state, ProcessSlotState::Unknown);
    }
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn latest_snapshot_retention_obeys_the_active_attempt_session_protection() {
    let db = TestDb::new().await;
    db.store
        .accept_resolved_submission(&command(), &descriptor())
        .await
        .unwrap();
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    db.store
        .acquire(&acquire_command(&session, 0, 1))
        .await
        .unwrap();
    db.store
        .record_observation(&observation(&session, 1))
        .await
        .unwrap();
    sqlx::query("UPDATE worker_sessions SET expires_at_ms=0 WHERE session_id=$1")
        .bind(&session.id)
        .execute(&db.store.pool)
        .await
        .unwrap();
    let policy = RetentionPolicy {
        batch_size: 1,
        ..Default::default()
    };
    for _ in 0..6 {
        db.store
            .retain_batch(&scope(), &policy, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
    assert_eq!(row_count(&db.store).await, 1);
    sqlx::query(
        "UPDATE attempts SET expires_at_ms=0,authority_deadline_ms=0 WHERE worker_session_id=$1",
    )
    .bind(&session.id)
    .execute(&db.store.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE tasks SET next_expiry_ms=0 WHERE state='active'")
        .execute(&db.store.pool)
        .await
        .unwrap();
    db.store.expire_batch(10).await.unwrap();
    for _ in 0..18 {
        db.store
            .retain_batch(&scope(), &policy, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
    assert_eq!(row_count(&db.store).await, 0);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn racing_reports_keep_only_the_greatest_full_width_sequence() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    let mut writes = tokio::task::JoinSet::new();
    for offset in 0..16 {
        let store = db.store.clone();
        let command = observation(&session, u64::MAX - offset);
        writes.spawn(async move { store.record_observation(&command).await });
    }
    let mut accepted = 0;
    while let Some(result) = writes.join_next().await {
        match result.unwrap() {
            Ok(_) => accepted += 1,
            Err(ContractError::Conflict) => {}
            other => panic!("unexpected racing result: {other:?}"),
        }
    }
    assert!(accepted >= 1);
    let latest = detail(&db.store, &session, ConsolePagination::default()).await;
    assert_eq!(latest.worker.snapshot_sequence, Some(ConsoleU64(u64::MAX)));
    assert_eq!(row_count(&db.store).await, 1);
    let replay = db
        .store
        .record_observation(&observation(&session, u64::MAX))
        .await
        .unwrap();
    assert!(replay.already_received);
    assert_eq!(Some(replay.received_at), latest.worker.received_at);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 via LEDGENCE_POSTGRES_URL"]
async fn compact_worker_list_does_not_decode_payload_and_detail_rejects_corruption() {
    let db = TestDb::new().await;
    let session = db.store.open_session(&scope(), "python", 1).await.unwrap();
    db.store
        .record_observation(&observation(&session, 1))
        .await
        .unwrap();
    sqlx::query("UPDATE worker_observations SET snapshot_bytes=$2 WHERE session_id=$1")
        .bind(&session.id)
        .bind(b"invalid snapshot".as_slice())
        .execute(&db.store.pool)
        .await
        .unwrap();
    let reply = db
        .store
        .query_workers(
            &scope(),
            &WorkerObservationQuery::Workers {
                queue: None,
                page: ConsolePagination::default(),
            },
        )
        .await
        .unwrap();
    let WorkerObservationReply::Workers(page) = reply else {
        panic!("wrong reply");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].snapshot_sequence, Some(ConsoleU64(1)));
    assert!(matches!(
        db.store
            .query_workers(
                &scope(),
                &WorkerObservationQuery::Inspect {
                    worker_session_id: session.id.clone(),
                    page: ConsolePagination::default(),
                }
            )
            .await,
        Err(ContractError::Unavailable(_))
    ));
    db.finish().await;
}
