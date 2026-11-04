use super::*;
macro_rules! operation {
    ($method:ident,$request:ty,$response:ty) => {
        fn $method<'a>(
            &'a self,
            _: &'a VerifiedPrincipal,
            q: &'a $request,
        ) -> ContractFuture<'a, $response> {
            Box::pin(async move { self.reply(stringify!($method), q) })
        }
    };
}
impl QueuePolicyOperations for Mock {
    operation!(put_queue_policy, QueuePolicyPut, QueuePolicyReceipt);
    operation!(inspect_queue_policy, QueuePolicyQuery, QueuePolicySnapshot);
    operation!(list_queue_policies, QueuePolicyList, QueuePolicyPage);
    operation!(list_queue_holds, QueueHoldList, QueueHoldPage);
    operation!(release_queue_hold, QueueHoldRelease, QueueHoldReceipt);
    operation!(close_queue_hold, QueueHoldClose, QueueHoldCloseReceipt);
}

#[tokio::test]
async fn scope_comes_from_console_instance_and_resource_control_is_required_before_decode() {
    let f = setup().await;
    f.mock.queue_policies_enabled.store(true, Ordering::SeqCst);
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("resource-operator")
        .unwrap();
    let c = QueuePolicyPut {
        scope: scope(),
        queue: "billing".into(),
        request_id: "change".into(),
        expected_revision: None,
        policy: QueueConcurrencyPolicy {
            max_concurrency: Some(5),
            timeout: None,
        },
        reason: "protect payments".into(),
    };
    let receipt = QueuePolicyReceipt {
        record: QueuePolicyRecord {
            scope: scope(),
            queue: c.queue.clone(),
            revision: 1,
            policy: c.policy.clone(),
            updated_at: 100,
        },
        already_accepted: false,
    };
    f.mock.set("put_queue_policy", Ok(receipt.clone()));
    for path in ["/v1/queue-policies/put", "/v1/console/queue-policies/put"] {
        assert_eq!(
            request(&f, path, Some(json!({"malformed":true})), "reader")
                .await
                .0,
            403
        );
    }
    let mut body = serde_json::to_value(&c).unwrap();
    assert_eq!(
        request(
            &f,
            "/v1/console/queue-policies/put",
            Some(body.clone()),
            "resource-operator"
        )
        .await
        .0,
        400
    );
    body.as_object_mut().unwrap().remove("scope");
    let (status, value) = request(
        &f,
        "/v1/console/queue-policies/put",
        Some(body),
        "resource-operator",
    )
    .await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value, serde_json::to_value(&receipt).unwrap());
    assert_eq!(
        client
            .queue_policy_client()
            .unwrap()
            .put_queue_policy(&c)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(f.mock.calls.lock().unwrap().as_slice().len(), 2);
    let mut bad = receipt.clone();
    bad.record.queue = "foreign".into();
    f.mock.set("put_queue_policy", Ok(bad));
    assert!(matches!(
        client.put_queue_policy(&c).await,
        Err(ContractError::Unavailable(_))
    ));
}
#[tokio::test]
async fn hold_ids_are_withheld_until_sensitive_access_audit_succeeds() {
    let f = setup().await;
    f.mock.queue_policies_enabled.store(true, Ordering::SeqCst);
    let query = json!({"queue":"billing","after":null,"limit":10,"include_released":false});
    let path = "/v1/console/queue-policies/holds";
    f.mock.set(
        "list_queue_holds",
        Ok(QueueHoldPage {
            items: vec![],
            next: None,
        }),
    );
    assert_eq!(
        request(&f, path, Some(query.clone()), "reader").await.0,
        403
    );
    assert_eq!(
        request(&f, path, Some(query.clone()), "operator").await.0,
        200
    );
    let recorded = f
        .auditor
        .observations
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(recorded.read, AuditReadKind::QueueHolds);
    assert_eq!(
        recorded.resource,
        AuditResource::Queue {
            queue: "billing".into()
        }
    );
    f.auditor.mode.store(1, Ordering::SeqCst);
    assert_eq!(request(&f, path, Some(query), "operator").await.0, 503);
}

#[tokio::test]
async fn late_close_requires_worker_and_exact_original_source() {
    let f = setup().await;
    f.mock.queue_policies_enabled.store(true, Ordering::SeqCst);
    let c = QueueHoldClose {
        queue: "billing".into(),
        owner: LeaseOwner {
            scope: scope(),
            task_id: "one".into(),
            attempt_id: "a".into(),
            lease_id: "secret".into(),
            generation: 1,
            worker_session_id: "w".into(),
            consumer_id: 0,
        },
    };
    let body = serde_json::to_value(&c).unwrap();
    let path = "/v1/queue-policies/close";
    assert_eq!(
        request(&f, path, Some(body.clone()), "operator").await.0,
        403
    );
    let original =
        QueueHoldCloseReceipt::new(&c, 2, QueueHoldReleaseKind::Confirmed, true).unwrap();
    f.mock.set("close_queue_hold", Ok(original.clone()));
    assert_eq!(request(&f, path, Some(body), "worker").await.0, 200);
    let mut other = c;
    other.owner.generation += 1;
    assert_eq!(
        request(
            &f,
            path,
            Some(serde_json::to_value(&other).unwrap()),
            "worker"
        )
        .await
        .0,
        503
    );
    assert_eq!(
        request(
            &f,
            "/v1/console/queue-policies/close",
            Some(json!({})),
            "operator"
        )
        .await
        .0,
        404
    );
}
