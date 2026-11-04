use super::*;
use serde_json::json;
fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "orders".into(),
    }
}
fn put() -> QueuePolicyPut {
    QueuePolicyPut {
        scope: scope(),
        queue: "fraud".into(),
        request_id: "request1".into(),
        expected_revision: None,
        policy: QueueConcurrencyPolicy {
            max_concurrency: Some(5),
            timeout: Some(QueueHoldTimeout {
                after_ms: 45_000,
                release: QueueTimeoutRelease::AfterGrace { grace_ms: 10_000 },
            }),
        },
        reason: "respect provider capacity".into(),
    }
}
#[test]
fn wire_requires_explicit_nulls_and_decimal_revisions() {
    let mut wire = serde_json::to_value(put()).unwrap();
    for value in [json!(9007199254740993u64), json!("01"), json!("-1")] {
        wire["expected_revision"] = value;
        assert!(serde_json::from_value::<QueuePolicyPut>(wire.clone()).is_err());
    }
    wire["expected_revision"] = json!("9007199254740993");
    let q: QueuePolicyPut = serde_json::from_value(wire.clone()).unwrap();
    q.validate().unwrap();
    assert_eq!(q.expected_revision, Some(9007199254740993));
    wire.as_object_mut().unwrap().remove("expected_revision");
    assert!(serde_json::from_value::<QueuePolicyPut>(wire).is_err());
    let p = json!({"timeout":null});
    assert!(serde_json::from_value::<QueueConcurrencyPolicy>(p).is_err());
    assert!(
        serde_json::from_value::<QueueConcurrencyPolicy>(json!({"max_concurrency":5})).is_err()
    );
}
#[test]
fn limits_pause_unlimited_and_receipt_replay_keep_distinct_meanings() {
    let q = put();
    let record = QueuePolicyRecord {
        scope: scope(),
        queue: q.queue.clone(),
        revision: 1,
        policy: q.policy.clone(),
        updated_at: 1,
    };
    let receipt = QueuePolicyReceipt {
        record,
        already_accepted: true,
    };
    receipt.validate_for(&q).unwrap();
    for limit in [None, Some(0), Some(1), Some(6)] {
        let changed = QueuePolicyPut {
            policy: QueueConcurrencyPolicy {
                max_concurrency: limit,
                timeout: None,
            },
            ..q.clone()
        };
        changed.validate().unwrap();
        assert!(receipt.validate_for(&changed).is_err());
    }
    let mut invalid = q.policy;
    invalid.timeout.as_mut().unwrap().after_ms = 0;
    assert!(invalid.validate().is_err());
}
#[test]
fn hold_pages_reject_wrong_source_and_timeout_without_stop_evidence() {
    let hold = QueueHoldSnapshot {
        scope: scope(),
        queue: "fraud".into(),
        task_id: "t".into(),
        attempt_id: "a".into(),
        worker_session_id: "s".into(),
        consumer_id: 0,
        revision: 1,
        state: QueueHoldState::Uncertain,
        acquired_at: 10,
        policy_revision: 1,
        grace_ms: Some(10),
        force_at: None,
        deadline: Some(20),
        stop_requested_at: None,
        released_at: None,
        release_kind: None,
    };
    let q = QueueHoldList {
        scope: scope(),
        queue: "fraud".into(),
        include_released: false,
        after: None,
        limit: 1,
    };
    let mut page = QueueHoldPage {
        items: vec![hold],
        next: None,
    };
    page.validate_for(&q).unwrap();
    page.next = Some("else".into());
    assert!(page.validate_for(&q).is_err());
    let mut h = page.items.pop().unwrap();
    h.state = QueueHoldState::Released;
    h.released_at = Some(30);
    h.release_kind = Some(QueueHoldReleaseKind::Timeout);
    assert!(h.validate().is_err());
    h.stop_requested_at = Some(20);
    assert!(h.validate().is_err());
    h.force_at = Some(30);
    h.validate().unwrap();
    page.next = None;
    page.items = vec![h];
    assert!(page.validate_for(&q).is_err());
    page.validate_for(&QueueHoldList {
        include_released: true,
        ..q
    })
    .unwrap();
}

#[test]
fn close_receipt_is_bound_to_full_opaque_attempt_authority() {
    let c = QueueHoldClose {
        queue: "billing".into(),
        owner: LeaseOwner {
            scope: scope(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_id: "lease".into(),
            generation: 1,
            worker_session_id: "session".into(),
            consumer_id: 0,
        },
    };
    let receipt =
        QueueHoldCloseReceipt::new(&c, 2, QueueHoldReleaseKind::Confirmed, false).unwrap();
    assert!(
        serde_json::to_string(&receipt)
            .unwrap()
            .contains("source_digest")
    );
    assert!(
        !serde_json::to_string(&receipt)
            .unwrap()
            .contains("lease_id")
    );
    let mut other = c.clone();
    other.owner.lease_id = "different-private-source".into();
    assert!(receipt.validate_for(&other).is_err());
    assert!(QueueHoldCloseReceipt::new(&c, 2, QueueHoldReleaseKind::Timeout, false).is_err());
    QueueHoldCloseReceipt::new(&c, 2, QueueHoldReleaseKind::Timeout, true).unwrap();
}
