//! Queue release records the exact subprocess source, independently of task settlement.
use super::*;

#[derive(Default)]
pub(super) struct Fixture {
    pub(super) enabled: AtomicBool,
    pub(super) stop_requested: AtomicBool,
    pub(super) settlement_fenced: AtomicBool,
    closes: Mutex<Vec<QueueHoldClose>>,
    lose_replies: AtomicUsize,
    corrupt_source: AtomicBool,
}
macro_rules! operator_api {
    ($name:ident, $request:ty, $reply:ty) => {
        fn $name<'a>(&'a self, _: &'a $request) -> ContractFuture<'a, $reply> {
            Box::pin(async { panic!("worker invoked operator API {}", stringify!($name)) })
        }
    };
}
impl QueuePolicyClient for Fixture {
    operator_api!(put_queue_policy, QueuePolicyPut, QueuePolicyReceipt);
    operator_api!(inspect_queue_policy, QueuePolicyQuery, QueuePolicySnapshot);
    operator_api!(list_queue_policies, QueuePolicyList, QueuePolicyPage);
    operator_api!(list_queue_holds, QueueHoldList, QueueHoldPage);
    operator_api!(release_queue_hold, QueueHoldRelease, QueueHoldReceipt);

    fn close_queue_hold<'a>(
        &'a self,
        c: &'a QueueHoldClose,
    ) -> ContractFuture<'a, QueueHoldCloseReceipt> {
        Box::pin(async move {
            let already_closed = {
                let mut closes = self.closes.lock().unwrap();
                let present = closes.contains(c);
                closes.push(c.clone());
                present
            };
            if lose(&self.lose_replies) {
                return Err(unavailable());
            }
            let mut reply =
                QueueHoldCloseReceipt::new(c, 2, QueueHoldReleaseKind::Confirmed, already_closed)?;
            if self.corrupt_source.load(Ordering::SeqCst) {
                let mut other = c.clone();
                // Preserve public IDs but substitute authority from another owner.
                other.owner.lease_id.push('x');
                reply.source_digest = other.digest()?;
            }
            Ok(reply)
        })
    }
}

#[tokio::test]
async fn close_waits_for_reaped_source_and_retries_independently_of_an_unknown_settlement() {
    let (worker, counts) = setup(1);
    counts.execution_error.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(true, Ordering::SeqCst);
    let service = Service::new(2);
    service.queue.enabled.store(true, Ordering::SeqCst);
    service.queue.lose_replies.store(1, Ordering::SeqCst);
    service.settle_unavailable.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker.clone(), service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| service.accepted_count() == 1).await;
    assert!(handle.shutdown(Duration::from_millis(50)).await.is_err());
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(service.queue.closes.lock().unwrap().is_empty());

    counts.hold_cleanup.store(false, Ordering::SeqCst);
    wait_for(|| service.queue.closes.lock().unwrap().len() >= 2).await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    assert_eq!(handle.status().settled_attempts, 0);
    assert_eq!(worker.stats().await.active_consumers, 1);
    let expected = QueueHoldClose {
        queue: "invoices".into(),
        owner: service.state.lock().unwrap().accepted["att_1"]
            .owner
            .clone(),
    };
    assert_eq!(
        *service.queue.closes.lock().unwrap(),
        [expected.clone(), expected]
    );
    service.settle_unavailable.store(false, Ordering::SeqCst);
    let status = handle.shutdown(WAIT).await.unwrap();
    assert_eq!(status.settled_attempts, 1);
    assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
    assert_eq!(worker.stats().await.active_consumers, 0);
}

#[tokio::test]
async fn remote_stop_cancels_work_and_lost_task_ownership_does_not_discard_late_cleanup() {
    let (worker, counts) = setup(1);
    counts.hold_execution.store(true, Ordering::SeqCst);
    counts.hold_cleanup.store(true, Ordering::SeqCst);
    let service = Service::new(1);
    service.queue.enabled.store(true, Ordering::SeqCst);
    let mut handle = DeliveryDriver::new(worker, service.clone(), config())
        .unwrap()
        .start();
    wait_for(|| counts.executions.load(Ordering::SeqCst) == 1).await;
    service
        .queue
        .settlement_fenced
        .store(true, Ordering::SeqCst);
    service.queue.stop_requested.store(true, Ordering::SeqCst);
    wait_for(|| counts.cancelled.load(Ordering::SeqCst) == 1).await;
    wait_for(|| handle.status().lost_attempts == 1).await;
    assert_eq!(counts.live.load(Ordering::SeqCst), 1);
    assert!(service.queue.closes.lock().unwrap().is_empty());
    counts.hold_cleanup.store(false, Ordering::SeqCst);
    let status = handle.shutdown(WAIT).await.unwrap();
    assert_eq!(status.settled_attempts, 0);
    assert_eq!(status.lost_attempts, 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
    let closes = service.queue.closes.lock().unwrap();
    assert_eq!(closes.len(), 1);
    let state = service.state.lock().unwrap();
    assert_eq!(closes[0].owner, state.renewals[0].owner);
    assert!(state.accepted.is_empty());
    assert!(state.confirmations.is_empty());
}

#[tokio::test]
async fn unbound_queue_receipt_stops_worker_and_unmanaged_queues_never_request_close() {
    for managed in [false, true] {
        let (worker, counts) = setup(1);
        let service = Service::new(2);
        service.queue.enabled.store(managed, Ordering::SeqCst);
        service.queue.corrupt_source.store(true, Ordering::SeqCst);
        let mut handle = DeliveryDriver::new(worker, service.clone(), config())
            .unwrap()
            .start();
        if managed {
            wait_for(|| handle.status().stopping).await;
        } else {
            wait_for(|| handle.status().settled_attempts == 2).await;
        }
        let status = handle.shutdown(WAIT).await.unwrap();
        assert_eq!(
            service.queue.closes.lock().unwrap().len(),
            usize::from(managed)
        );
        assert_eq!(
            counts.executions.load(Ordering::SeqCst),
            if managed { 1 } else { 2 }
        );
        if managed {
            assert!(
                status
                    .last_error
                    .unwrap()
                    .to_string()
                    .contains("queue cleanup receipt")
            );
        }
    }
}
