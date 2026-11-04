use crate::*;
impl QueuePolicyOperations for ApplicationService {
    fn put_queue_policy<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueuePolicyPut,
    ) -> ContractFuture<'a, QueuePolicyReceipt> {
        Box::pin(async move {
            request.validate()?;
            crate::admission::operator(
                principal,
                &request.scope,
                AccessCapability::ResourceControl,
            )?;
            let reply = self
                .store
                .queue_policies()?
                .put_queue_policy(principal, request)
                .await?;
            reply.validate_for(request).map_err(invalid_reply)?;
            Ok(reply)
        })
    }
    fn inspect_queue_policy<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueuePolicyQuery,
    ) -> ContractFuture<'a, QueuePolicySnapshot> {
        Box::pin(async move {
            request.validate()?;
            crate::admission::operator(principal, &request.scope, AccessCapability::MetadataRead)?;
            let reply = self
                .store
                .queue_policies()?
                .inspect_queue_policy(principal, request)
                .await?;
            reply.validate_for(request).map_err(invalid_reply)?;
            Ok(reply)
        })
    }
    fn list_queue_policies<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueuePolicyList,
    ) -> ContractFuture<'a, QueuePolicyPage> {
        Box::pin(async move {
            request.validate()?;
            crate::admission::operator(principal, &request.scope, AccessCapability::MetadataRead)?;
            let reply = self
                .store
                .queue_policies()?
                .list_queue_policies(principal, request)
                .await?;
            reply.validate_for(request).map_err(invalid_reply)?;
            Ok(reply)
        })
    }
    fn list_queue_holds<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueueHoldList,
    ) -> ContractFuture<'a, QueueHoldPage> {
        Box::pin(async move {
            request.validate()?;
            crate::admission::operator(principal, &request.scope, AccessCapability::SensitiveRead)?;
            let reply = self
                .store
                .queue_policies()?
                .list_queue_holds(principal, request)
                .await?;
            reply.validate_for(request).map_err(invalid_reply)?;
            Ok(reply)
        })
    }
    fn release_queue_hold<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueueHoldRelease,
    ) -> ContractFuture<'a, QueueHoldReceipt> {
        Box::pin(async move {
            request.validate()?;
            crate::admission::operator(
                principal,
                &request.scope,
                AccessCapability::ResourceControl,
            )?;
            let reply = self
                .store
                .queue_policies()?
                .release_queue_hold(principal, request)
                .await?;
            reply.validate_for(request).map_err(invalid_reply)?;
            Ok(reply)
        })
    }
    fn close_queue_hold<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        request: &'a QueueHoldClose,
    ) -> ContractFuture<'a, QueueHoldCloseReceipt> {
        Box::pin(async move {
            request.validate()?;
            crate::admission::worker(principal, &request.owner.scope)?;
            let reply = self
                .store
                .queue_policies()?
                .close_queue_hold(principal, request)
                .await?;
            reply.validate_for(request).map_err(invalid_reply)?;
            Ok(reply)
        })
    }
}

fn invalid_reply(_: ContractError) -> ContractError {
    ContractError::Unavailable(
        "queue policy adapter returned an invalid or unbound response".into(),
    )
}
