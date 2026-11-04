use super::*;
impl ResponseValue for QueuePolicyReceipt {
    const MAX_BYTES: usize = QUEUE_POLICY_PAGE_MAX_BYTES;
}
impl ResponseValue for QueuePolicySnapshot {
    const MAX_BYTES: usize = QUEUE_POLICY_PAGE_MAX_BYTES;
}
impl ResponseValue for QueuePolicyPage {
    const MAX_BYTES: usize = QUEUE_POLICY_PAGE_MAX_BYTES;
}
impl ResponseValue for QueueHoldPage {
    const MAX_BYTES: usize = QUEUE_POLICY_PAGE_MAX_BYTES;
}
impl ResponseValue for QueueHoldReceipt {
    const MAX_BYTES: usize = QUEUE_POLICY_PAGE_MAX_BYTES;
}
impl ResponseValue for QueueHoldCloseReceipt {
    const MAX_BYTES: usize = QUEUE_POLICY_PAGE_MAX_BYTES;
}
impl QueuePolicyClient for HttpTaskService {
    fn put_queue_policy<'a>(
        &'a self,
        request: &'a QueuePolicyPut,
    ) -> ContractFuture<'a, QueuePolicyReceipt> {
        Box::pin(async move {
            request.validate()?;
            let expected = request.clone();
            self.post_validated(
                "v1/queue-policies/put",
                request,
                QUEUE_POLICY_COMMAND_MAX_BYTES,
                move |reply: &QueuePolicyReceipt| {
                    reply.validate_for(&expected).map_err(|_| {
                        unavailable(
                            "invalid queue policy response; reconcile the identical request",
                        )
                    })
                },
            )
            .await
        })
    }
    fn inspect_queue_policy<'a>(
        &'a self,
        request: &'a QueuePolicyQuery,
    ) -> ContractFuture<'a, QueuePolicySnapshot> {
        Box::pin(async move {
            request.validate()?;
            let expected = request.clone();
            self.post_validated(
                "v1/queue-policies/inspect",
                request,
                QUEUE_POLICY_COMMAND_MAX_BYTES,
                move |reply: &QueuePolicySnapshot| {
                    reply.validate_for(&expected).map_err(|_| {
                        unavailable(
                            "invalid queue policy response; reconcile the identical request",
                        )
                    })
                },
            )
            .await
        })
    }
    fn list_queue_policies<'a>(
        &'a self,
        request: &'a QueuePolicyList,
    ) -> ContractFuture<'a, QueuePolicyPage> {
        Box::pin(async move {
            request.validate()?;
            let expected = request.clone();
            self.post_validated(
                "v1/queue-policies/list",
                request,
                QUEUE_POLICY_COMMAND_MAX_BYTES,
                move |reply: &QueuePolicyPage| {
                    reply.validate_for(&expected).map_err(|_| {
                        unavailable(
                            "invalid queue policy response; reconcile the identical request",
                        )
                    })
                },
            )
            .await
        })
    }
    fn list_queue_holds<'a>(
        &'a self,
        request: &'a QueueHoldList,
    ) -> ContractFuture<'a, QueueHoldPage> {
        Box::pin(async move {
            request.validate()?;
            let expected = request.clone();
            self.post_validated(
                "v1/queue-policies/holds",
                request,
                QUEUE_POLICY_COMMAND_MAX_BYTES,
                move |reply: &QueueHoldPage| {
                    reply.validate_for(&expected).map_err(|_| {
                        unavailable(
                            "invalid queue policy response; reconcile the identical request",
                        )
                    })
                },
            )
            .await
        })
    }
    fn release_queue_hold<'a>(
        &'a self,
        request: &'a QueueHoldRelease,
    ) -> ContractFuture<'a, QueueHoldReceipt> {
        Box::pin(async move {
            request.validate()?;
            let expected = request.clone();
            self.post_validated(
                "v1/queue-policies/release",
                request,
                QUEUE_POLICY_COMMAND_MAX_BYTES,
                move |reply: &QueueHoldReceipt| {
                    reply.validate_for(&expected).map_err(|_| {
                        unavailable(
                            "invalid queue policy response; reconcile the identical request",
                        )
                    })
                },
            )
            .await
        })
    }
    fn close_queue_hold<'a>(
        &'a self,
        request: &'a QueueHoldClose,
    ) -> ContractFuture<'a, QueueHoldCloseReceipt> {
        Box::pin(async move {
            request.validate()?;
            let expected = request.clone();
            self.post_validated(
                "v1/queue-policies/close",
                request,
                QUEUE_POLICY_COMMAND_MAX_BYTES,
                move |reply: &QueueHoldCloseReceipt| {
                    reply.validate_for(&expected).map_err(|_| {
                        unavailable(
                            "invalid queue policy response; reconcile the identical request",
                        )
                    })
                },
            )
            .await
        })
    }
}
