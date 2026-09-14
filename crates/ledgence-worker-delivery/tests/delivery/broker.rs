use super::*;
use ledgence_worker_delivery::{AcquisitionSource, BrokerAcquisitionSource, SourceReply};
use std::collections::VecDeque;
use tokio::sync::Notify;

#[derive(Clone, Copy, Default)]
enum AckBehavior {
    #[default]
    Confirmed,
    Error,
    Missing,
    WrongReceipt,
    Pending,
}
#[derive(Default)]
struct Queue {
    deliveries: Mutex<VecDeque<Vec<QueueDelivery>>>,
    polls: AtomicUsize,
    acknowledgments: Mutex<Vec<Vec<String>>>,
    ack_behavior: Mutex<VecDeque<AckBehavior>>,
    ack_entered: Notify,
    receive_pending: AtomicBool,
    receive_panics: AtomicUsize,
    receive_entered: Notify,
    receive_release: Notify,
}
impl Queue {
    fn with_records(records: impl IntoIterator<Item = Vec<QueueDelivery>>) -> Arc<Self> {
        Arc::new(Self {
            deliveries: Mutex::new(records.into_iter().collect()),
            ..Self::default()
        })
    }
}
impl AckQueue for Queue {
    fn limits(&self) -> QueueLimits {
        QueueLimits {
            max_publish_batch: 10,
            max_receive_batch: 10,
            max_ack_batch: 10,
            max_message_bytes: 1024 * 1024,
        }
    }
    fn receive(&self, max: u32, _: Duration, _: Instant) -> ContractFuture<'_, Vec<QueueDelivery>> {
        Box::pin(async move {
            assert_eq!(max, 1, "one record per reserved execution slot");
            self.polls.fetch_add(1, Ordering::SeqCst);
            assert!(!lose(&self.receive_panics), "injected receive poll panic");
            if self.receive_pending.swap(false, Ordering::SeqCst) {
                self.receive_entered.notify_one();
                self.receive_release.notified().await;
            }
            Ok(self
                .deliveries
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default())
        })
    }
    fn acknowledge<'a>(
        &'a self,
        receipts: &'a [String],
        _: Instant,
    ) -> ContractFuture<'a, Vec<AckResult>> {
        Box::pin(async move {
            self.acknowledgments.lock().unwrap().push(receipts.to_vec());
            let behavior = self
                .ack_behavior
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default();
            match behavior {
                AckBehavior::Error => Err(unavailable()),
                AckBehavior::Missing => Ok(Vec::new()),
                AckBehavior::WrongReceipt => Ok(vec![AckResult {
                    receipt: "other-receipt".into(),
                    confirmed: true,
                }]),
                AckBehavior::Pending => {
                    self.ack_entered.notify_one();
                    std::future::pending().await
                }
                AckBehavior::Confirmed => Ok(receipts
                    .iter()
                    .map(|receipt| AckResult {
                        receipt: receipt.clone(),
                        confirmed: true,
                    })
                    .collect()),
            }
        })
    }
}
fn record(task: usize) -> QueueDelivery {
    QueueDelivery {
        body: serde_json::to_vec(&PublishedDispatch {
            dispatch: DispatchRef {
                scope: scope(),
                queue: "invoices".into(),
                task_id: format!("task_{task}"),
                generation: 1,
            },
            publication_id: format!("publication_{task}"),
        })
        .unwrap(),
        receipt: format!("receipt_{task}"),
    }
}
fn options() -> AcquireOptions {
    AcquireOptions::immediate(Instant::now() + WAIT)
}
async fn source(
    queue: Arc<Queue>,
    service: Arc<Service>,
    concurrency: u32,
) -> (Arc<BrokerAcquisitionSource>, AcquireCommand) {
    let session = service
        .open_session(&scope(), "invoices", concurrency)
        .await
        .unwrap();
    let source = Arc::new(BrokerAcquisitionSource::new(queue, service).unwrap());
    source.start_session(&session).unwrap();
    let command = AcquireCommand {
        scope: session.scope,
        queue: session.queue,
        worker_session_id: session.id,
        consumer_id: 0,
        sequence: 1,
    };
    (source, command)
}
fn assigned(reply: SourceReply) -> Box<Assignment> {
    let SourceReply::Completed(AcquireReply::Assigned { assignment, .. }) = reply else {
        panic!("assigned reply expected")
    };
    assignment
}

#[tokio::test]
async fn broker_empty_polls_preserve_sequence_and_next_durable_handoff_advances_it() {
    let service = Service::new(0);
    *service.broker_disposition.lock().unwrap() = Some(ClaimDisposition::TerminalOrSuperseded);
    let queue = Queue::with_records([vec![], vec![], vec![record(1)], vec![]]);
    let (source, mut command) = source(queue.clone(), service.clone(), 1).await;
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Idle
    ));
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Idle
    ));
    assert!(service.broker_commands.lock().unwrap().is_empty());
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Discarded { sequence: 1 }
    ));
    command.sequence = 2;
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Idle
    ));
    assert_eq!(
        service.broker_commands.lock().unwrap()[0]
            .acquisition
            .sequence,
        1
    );
    assert_eq!(queue.acknowledgments.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn dropped_claim_future_reconciles_original_record_without_receiving_again() {
    let service = Service::new(1);
    service
        .first_acquire_delay_ms
        .store(30_000, Ordering::SeqCst);
    let queue = Queue::with_records([vec![record(1)], vec![record(2)]]);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    let operation = tokio::spawn({
        let source = source.clone();
        let command = command.clone();
        async move { source.acquire(&command, options()).await }
    });
    wait_for(|| service.state.lock().unwrap().replies.len() == 1).await;
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    let assignment = assigned(source.acquire(&command, options()).await.unwrap());
    assert_eq!(assignment.lease.owner.task_id, "task_1");
    let commands = service.broker_commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0], commands[1]);
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    assert_eq!(queue.acknowledgments.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn dropped_ack_future_replays_same_claim_and_receipt_before_exposing_authority() {
    let service = Service::new(1);
    let queue = Queue::with_records([vec![record(1)], vec![record(2)]]);
    queue
        .ack_behavior
        .lock()
        .unwrap()
        .push_back(AckBehavior::Pending);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    let operation = tokio::spawn({
        let source = source.clone();
        let command = command.clone();
        async move { source.acquire(&command, options()).await }
    });
    tokio::time::timeout(WAIT, queue.ack_entered.notified())
        .await
        .unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    {
        let mut state = service.state.lock().unwrap();
        let AcquireReply::Assigned { assignment, .. } = state.replies.get_mut(&(0, 1)).unwrap()
        else {
            panic!("committed assignment expected");
        };
        assignment.authority.remaining_ms = 12_345;
    }
    let assignment = assigned(source.acquire(&command, options()).await.unwrap());
    assert_eq!(assignment.lease.owner.attempt_id, "att_1");
    assert_eq!(
        assignment.authority.remaining_ms, 12_345,
        "reconcile current authority, never cached TTL"
    );
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    let acks = queue.acknowledgments.lock().unwrap();
    assert_eq!(acks.len(), 2);
    assert_eq!(acks[0], acks[1]);
    let commands = service.broker_commands.lock().unwrap();
    assert_eq!(commands[0], commands[1]);
}

#[tokio::test]
async fn unconfirmed_ack_does_not_block_execution_or_substitute_another_dispatch() {
    for behavior in [
        AckBehavior::Error,
        AckBehavior::Missing,
        AckBehavior::WrongReceipt,
    ] {
        let service = Service::new(1);
        let queue = Queue::with_records([vec![record(1)], vec![record(2)]]);
        queue.ack_behavior.lock().unwrap().push_back(behavior);
        let (source, command) = source(queue.clone(), service.clone(), 1).await;
        let first = assigned(source.acquire(&command, options()).await.unwrap());
        let replay = assigned(source.acquire(&command, options()).await.unwrap());
        assert_eq!(first.lease.owner, replay.lease.owner);
        assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
        assert_eq!(service.state.lock().unwrap().next_task, 1);
    }
}

#[tokio::test]
async fn another_claimants_handoff_never_grants_execution_authority() {
    let service = Service::new(0);
    *service.broker_disposition.lock().unwrap() = Some(ClaimDisposition::AlreadyHandedOff {
        attempt: AttemptRef {
            task_id: "task_1".into(),
            attempt_id: "other_attempt".into(),
        },
    });
    let queue = Queue::with_records([vec![record(1)]]);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Discarded { sequence: 1 }
    ));
    assert_eq!(queue.acknowledgments.lock().unwrap().len(), 1);
    assert!(service.state.lock().unwrap().active.is_empty());
}

#[tokio::test]
async fn malformed_claim_reply_remains_unacknowledged_and_retains_original_identity() {
    let service = Service::new(1);
    service.malformed_claim.store(true, Ordering::SeqCst);
    let queue = Queue::with_records([vec![record(1)], vec![record(2)]]);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    assert!(matches!(
        source.acquire(&command, options()).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(queue.acknowledgments.lock().unwrap().is_empty());
    service.malformed_claim.store(false, Ordering::SeqCst);
    let reply = assigned(source.acquire(&command, options()).await.unwrap());
    assert_eq!(reply.lease.owner.task_id, "task_1");
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unknown_claim_cannot_be_replaced_by_a_changed_sequence_or_consumer() {
    let service = Service::new(1);
    service.lost_acquire_replies.store(1, Ordering::SeqCst);
    let queue = Queue::with_records([vec![record(1)], vec![record(2)]]);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    assert!(source.acquire(&command, options()).await.is_err());
    for changed in [
        AcquireCommand {
            sequence: 2,
            ..command.clone()
        },
        AcquireCommand {
            consumer_id: 1,
            ..command.clone()
        },
        AcquireCommand {
            queue: "other".into(),
            ..command.clone()
        },
    ] {
        assert!(source.acquire(&changed, options()).await.is_err());
    }
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        assigned(source.acquire(&command, options()).await.unwrap())
            .lease
            .owner
            .task_id,
        "task_1"
    );
}

#[tokio::test]
async fn poison_and_wrong_route_records_fail_stop_all_new_admission_without_ack() {
    let mut wrong = record(1);
    let mut record_value = serde_json::from_slice::<Value>(&wrong.body).unwrap();
    record_value["dispatch"]["scope"]["namespace"] = json!("other");
    wrong.body = serde_json::to_vec(&record_value).unwrap();
    for bad in [
        QueueDelivery {
            body: b"not-json".to_vec(),
            receipt: "receipt".into(),
        },
        wrong,
        QueueDelivery {
            body: vec![b'x'; DISPATCH_MAX_BYTES + 1],
            receipt: "receipt".into(),
        },
    ] {
        let service = Service::new(0);
        let queue = Queue::with_records([vec![bad], vec![record(2)]]);
        let (source, command) = source(queue.clone(), service.clone(), 2).await;
        assert!(matches!(
            source.acquire(&command, options()).await.unwrap(),
            SourceReply::Stopped { .. }
        ));
        let other = AcquireCommand {
            consumer_id: 1,
            ..command
        };
        assert!(matches!(
            source.acquire(&other, options()).await.unwrap(),
            SourceReply::Stopped { .. }
        ));
        assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
        assert!(service.broker_commands.lock().unwrap().is_empty());
        assert!(queue.acknowledgments.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn unexpected_receive_batch_cannot_exceed_reserved_capacity() {
    let service = Service::new(0);
    let queue = Queue::with_records([vec![record(1), record(2)]]);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    assert!(matches!(
        source.acquire(&command, options()).await.unwrap(),
        SourceReply::Stopped { .. }
    ));
    assert!(service.broker_commands.lock().unwrap().is_empty());
    assert!(queue.acknowledgments.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_receive_has_no_claim_and_session_finish_removes_retained_slots() {
    let service = Service::new(1);
    let queue = Queue::with_records([]);
    queue.receive_pending.store(true, Ordering::SeqCst);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    let operation = tokio::spawn({
        let source = source.clone();
        let command = command.clone();
        async move { source.acquire(&command, options()).await }
    });
    tokio::time::timeout(WAIT, queue.receive_entered.notified())
        .await
        .unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    assert!(service.broker_commands.lock().unwrap().is_empty());
    queue.deliveries.lock().unwrap().push_back(vec![record(1)]);
    queue.receive_release.notify_one();
    assert_eq!(
        assigned(source.acquire(&command, options()).await.unwrap())
            .lease
            .owner
            .task_id,
        "task_1"
    );
    assert_eq!(
        queue.polls.load(Ordering::SeqCst),
        1,
        "cancellation must not orphan a receive or start a second one"
    );
    source.finish_session(&command.worker_session_id);
    assert!(source.acquire(&command, options()).await.is_err());
    let mut session = service.state.lock().unwrap().session.clone().unwrap();
    session.id = "new-session".into();
    assert!(source.start_session(&session).is_ok());
    assert!(source.acquire(&command, options()).await.is_err());
    let next = AcquireCommand {
        worker_session_id: session.id,
        ..command
    };
    assert!(matches!(
        source.acquire(&next, options()).await.unwrap(),
        SourceReply::Idle
    ));
}

#[tokio::test]
async fn driver_uses_same_capacity_and_runtime_reuse_with_broker_acquisition() {
    let (worker, counts) = setup(1);
    let service = Service::new(2);
    service.lost_acquire_replies.store(1, Ordering::SeqCst);
    let queue = Queue::with_records([vec![], vec![], vec![record(1)], vec![record(2)]]);
    queue
        .ack_behavior
        .lock()
        .unwrap()
        .push_back(AckBehavior::Error);
    let source = Arc::new(BrokerAcquisitionSource::new(queue.clone(), service.clone()).unwrap());
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .with_acquisition_source(source)
        .start();
    wait_for(|| handle.status().settled_attempts == 2).await;
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 2);
    assert_eq!(counts.starts.load(Ordering::SeqCst), 1);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    assert_eq!(counts.implicit_drops.load(Ordering::SeqCst), 0);
    let commands = service.broker_commands.lock().unwrap();
    assert_eq!(commands[0].acquisition.sequence, 1);
    assert_eq!(commands[0], commands[1]);
    assert_eq!(commands[2].acquisition.sequence, 2);
    assert_eq!(service.state.lock().unwrap().capacity_violations, 0);
}

#[tokio::test]
async fn driver_poison_record_stops_cleanly_without_execution_or_ack() {
    let (worker, counts) = setup(1);
    let service = Service::new(0);
    let queue = Queue::with_records([vec![QueueDelivery {
        body: b"poison".to_vec(),
        receipt: "receipt".into(),
    }]]);
    let source = Arc::new(BrokerAcquisitionSource::new(queue.clone(), service.clone()).unwrap());
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_acquisition_source(source)
        .start();
    let status = tokio::time::timeout(WAIT, handle.wait()).await.unwrap();
    assert!(status.finished);
    assert!(matches!(
        status.last_error,
        Some(ContractError::InvalidInput(_))
    ));
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert!(service.broker_commands.lock().unwrap().is_empty());
    assert!(queue.acknowledgments.lock().unwrap().is_empty());
}

#[tokio::test]
async fn driver_shutdown_reconciles_selected_claim_without_starting_user_code() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    service
        .first_acquire_delay_ms
        .store(30_000, Ordering::SeqCst);
    let queue = Queue::with_records([vec![record(1)]]);
    let source = Arc::new(BrokerAcquisitionSource::new(queue.clone(), service.clone()).unwrap());
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_acquisition_source(source)
        .start();
    wait_for(|| !service.state.lock().unwrap().replies.is_empty()).await;
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
    assert_eq!(service.accepted_count(), 1);
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    let commands = service.broker_commands.lock().unwrap();
    assert!(commands.len() >= 2);
    assert!(commands.iter().all(|command| command == &commands[0]));
}

#[tokio::test]
async fn broker_driver_reserves_capacity_before_receive_and_never_exceeds_n() {
    let (worker, counts) = setup(2);
    counts.hold_execution.store(true, Ordering::SeqCst);
    let reservation = worker
        .reserve_consumer(RunControl::new(WAIT))
        .await
        .unwrap();
    let service = Service::new(3);
    let queue = Queue::with_records([vec![record(1)], vec![record(2)], vec![record(3)]]);
    let source = Arc::new(BrokerAcquisitionSource::new(queue.clone(), service.clone()).unwrap());
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .with_acquisition_source(source)
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    reservation.release();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 2).await;
    assert_eq!(queue.polls.load(Ordering::SeqCst), 2);
    counts.hold_execution.store(false, Ordering::SeqCst);
    wait_for(|| handle.status().settled_attempts == 3).await;
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
    assert!(counts.peak.load(Ordering::SeqCst) <= 2);
    assert_eq!(service.state.lock().unwrap().capacity_violations, 0);
}

#[tokio::test]
async fn confirmed_duplicates_drain_without_empty_queue_pacing() {
    let (worker, counts) = setup(1);
    let service = Service::new(0);
    *service.broker_disposition.lock().unwrap() = Some(ClaimDisposition::AlreadyHandedOff {
        attempt: AttemptRef {
            task_id: "task_1".into(),
            attempt_id: "elsewhere".into(),
        },
    });
    let queue = Queue::with_records([vec![record(1)], vec![record(1)], vec![record(1)]]);
    let source = Arc::new(BrokerAcquisitionSource::new(queue.clone(), service.clone()).unwrap());
    let mut config = config();
    config.idle_delay = Duration::from_secs(30);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config)
        .unwrap()
        .with_acquisition_source(source)
        .start();
    wait_for(|| service.broker_commands.lock().unwrap().len() == 3).await;
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
    let sequences: Vec<_> = service
        .broker_commands
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.acquisition.sequence)
        .collect();
    assert_eq!(sequences, vec![1, 2, 3]);
    assert_eq!(queue.acknowledgments.lock().unwrap().len(), 3);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn poison_stops_new_admission_but_does_not_abandon_another_uncertain_claim() {
    let service = Service::new(1);
    service
        .first_acquire_delay_ms
        .store(30_000, Ordering::SeqCst);
    let queue = Queue::with_records([
        vec![record(1)],
        vec![QueueDelivery {
            body: b"poison".to_vec(),
            receipt: "bad-receipt".into(),
        }],
    ]);
    let (source, command) = source(queue.clone(), service.clone(), 2).await;
    let operation = tokio::spawn({
        let source = source.clone();
        let command = command.clone();
        async move { source.acquire(&command, options()).await }
    });
    wait_for(|| !service.state.lock().unwrap().replies.is_empty()).await;
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    let other = AcquireCommand {
        consumer_id: 1,
        ..command.clone()
    };
    assert!(matches!(
        source.acquire(&other, options()).await.unwrap(),
        SourceReply::Stopped { .. }
    ));
    assert_eq!(
        assigned(source.acquire(&command, options()).await.unwrap())
            .lease
            .owner
            .task_id,
        "task_1"
    );
    assert_eq!(
        queue.acknowledgments.lock().unwrap().as_slice(),
        &[vec!["receipt_1".to_string()]]
    );
}

struct PanickingLifecycleSource {
    start: bool,
}
impl AcquisitionSource for PanickingLifecycleSource {
    fn start_session(&self, _: &WorkerSession) -> ledgence_orchestration_api::Result<()> {
        assert!(!self.start, "injected setup panic");
        Ok(())
    }
    fn acquire<'a>(
        &'a self,
        _: &'a AcquireCommand,
        _: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        Box::pin(async { Ok(SourceReply::Idle) })
    }
    fn finish_session(&self, _: &str) {
        panic!("injected finish panic");
    }
}
#[tokio::test]
async fn source_lifecycle_panics_do_not_skip_driver_shutdown() {
    for start in [true, false] {
        let (worker, counts) = setup(1);
        let service = Service::new(0);
        let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
            .unwrap()
            .with_acquisition_source(Arc::new(PanickingLifecycleSource { start }))
            .start();
        wait_for(|| service.state.lock().unwrap().session.is_some()).await;
        let status = handle.shutdown(WAIT).await.unwrap();
        assert!(status.finished);
        assert!(matches!(
            status.last_error,
            Some(ContractError::Unavailable(_))
        ));
        assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
        assert!(!worker.stats().await.accepting);
    }
}

#[tokio::test]
async fn graceful_stop_drains_retained_receive_and_never_orphans_a_later_task() {
    let (worker, counts) = setup(1);
    let service = Service::new(1);
    let queue = Queue::with_records([]);
    queue.receive_pending.store(true, Ordering::SeqCst);
    let source = Arc::new(BrokerAcquisitionSource::new(queue.clone(), service.clone()).unwrap());
    let mut config = config();
    config.request_timeout = WAIT;
    let mut handle = DeliveryDriver::new(worker, service.clone(), config)
        .unwrap()
        .with_acquisition_source(source)
        .start();
    tokio::time::timeout(WAIT, queue.receive_entered.notified())
        .await
        .unwrap();
    handle.stop();
    queue.deliveries.lock().unwrap().push_back(vec![record(1)]);
    queue.receive_release.notify_one();
    assert!(handle.shutdown(WAIT).await.unwrap().finished);
    assert_eq!(
        queue.polls.load(Ordering::SeqCst),
        1,
        "same healthy receive must be drained after stop"
    );
    assert_eq!(
        queue.acknowledgments.lock().unwrap().as_slice(),
        &[vec!["receipt_1".to_string()]]
    );
    assert_eq!(
        service.accepted_count(),
        1,
        "received task is durably settled without user execution"
    );
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelled_receive_keeps_its_original_deadline_on_reconciliation() {
    let service = Service::new(1);
    let queue = Queue::with_records([vec![record(1)]]);
    queue.receive_pending.store(true, Ordering::SeqCst);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    let original_deadline = Instant::now() + Duration::from_millis(100);
    let operation = tokio::spawn({
        let source = source.clone();
        let command = command.clone();
        async move {
            source
                .acquire(&command, AcquireOptions::immediate(original_deadline))
                .await
        }
    });
    tokio::time::timeout(WAIT, queue.receive_entered.notified())
        .await
        .unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    tokio::time::sleep_until((original_deadline + Duration::from_millis(10)).into()).await;
    assert!(
        matches!(
            source.acquire(&command, options()).await,
            Err(ContractError::Unavailable(_))
        ),
        "new caller budget must not revive expired receive"
    );
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    assert!(service.broker_commands.lock().unwrap().is_empty());
    assert_eq!(
        assigned(source.acquire(&command, options()).await.unwrap())
            .lease
            .owner
            .task_id,
        "task_1"
    );
    assert_eq!(
        queue.polls.load(Ordering::SeqCst),
        2,
        "a genuinely expired receive can be retried"
    );
}

#[tokio::test]
async fn explicit_external_route_rejection_stops_integrated_driver_without_reconciliation_loop() {
    let (worker, counts) = setup(1);
    let service = Service::new(0);
    *service.acquire_error.lock().unwrap() = Some(ContractError::ExternalDispatchRequired);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .start();
    let status = tokio::time::timeout(WAIT, handle.wait()).await.unwrap();
    assert!(status.finished);
    assert_eq!(
        status.last_error,
        Some(ContractError::ExternalDispatchRequired)
    );
    assert_eq!(service.state.lock().unwrap().acquisitions.len(), 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn panicking_receive_does_not_leave_a_poisoned_retained_future() {
    let service = Service::new(1);
    let queue = Queue::with_records([vec![record(1)]]);
    queue.receive_panics.store(1, Ordering::SeqCst);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    assert!(matches!(
        source.acquire(&command, options()).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(service.broker_commands.lock().unwrap().is_empty());
    assert_eq!(
        assigned(source.acquire(&command, options()).await.unwrap())
            .lease
            .owner
            .task_id,
        "task_1"
    );
    assert_eq!(queue.polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn queue_protocol_error_from_a_claim_does_not_abandon_unknown_authority() {
    let service = Service::new(1);
    *service.acquire_error.lock().unwrap() = Some(ContractError::InvalidQueueDelivery(
        "wrong adapter stage".into(),
    ));
    let queue = Queue::with_records([vec![record(1)], vec![record(2)]]);
    let (source, command) = source(queue.clone(), service.clone(), 1).await;
    assert!(matches!(
        source.acquire(&command, options()).await,
        Err(ContractError::InvalidQueueDelivery(_))
    ));
    assert!(queue.acknowledgments.lock().unwrap().is_empty());
    *service.acquire_error.lock().unwrap() = None;
    assert_eq!(
        assigned(source.acquire(&command, options()).await.unwrap())
            .lease
            .owner
            .task_id,
        "task_1"
    );
    assert_eq!(queue.polls.load(Ordering::SeqCst), 1);
    let commands = service.broker_commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0], commands[1]);
}
