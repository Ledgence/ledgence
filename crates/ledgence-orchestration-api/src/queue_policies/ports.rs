use super::*;
/// Server-side operations bind the authenticated principal; no client chooses an actor.
pub trait QueuePolicyOperations: Send + Sync {
    fn put_queue_policy<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueuePolicyPut,
    ) -> ContractFuture<'a, QueuePolicyReceipt>;
    fn inspect_queue_policy<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueuePolicyQuery,
    ) -> ContractFuture<'a, QueuePolicySnapshot>;
    fn list_queue_policies<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueuePolicyList,
    ) -> ContractFuture<'a, QueuePolicyPage>;
    fn list_queue_holds<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueueHoldList,
    ) -> ContractFuture<'a, QueueHoldPage>;
    fn release_queue_hold<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueueHoldRelease,
    ) -> ContractFuture<'a, QueueHoldReceipt>;
    fn close_queue_hold<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueueHoldClose,
    ) -> ContractFuture<'a, QueueHoldCloseReceipt>;
}
/// Store mutations and their audit receipts commit with the durable queue transition.
pub trait QueuePolicyStore: QueuePolicyOperations {}
/// Client operations derive the principal from transport credentials.
pub trait QueuePolicyClient: Send + Sync {
    fn put_queue_policy<'a>(
        &'a self,
        request: &'a QueuePolicyPut,
    ) -> ContractFuture<'a, QueuePolicyReceipt>;
    fn inspect_queue_policy<'a>(
        &'a self,
        request: &'a QueuePolicyQuery,
    ) -> ContractFuture<'a, QueuePolicySnapshot>;
    fn list_queue_policies<'a>(
        &'a self,
        request: &'a QueuePolicyList,
    ) -> ContractFuture<'a, QueuePolicyPage>;
    fn list_queue_holds<'a>(
        &'a self,
        request: &'a QueueHoldList,
    ) -> ContractFuture<'a, QueueHoldPage>;
    fn release_queue_hold<'a>(
        &'a self,
        request: &'a QueueHoldRelease,
    ) -> ContractFuture<'a, QueueHoldReceipt>;
    fn close_queue_hold<'a>(
        &'a self,
        request: &'a QueueHoldClose,
    ) -> ContractFuture<'a, QueueHoldCloseReceipt>;
}
