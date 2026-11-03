//! Provider mutation permission and immutable callback reports stay in the host.
use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimRequest {
    approval_key: String,
    action: ApprovalAction,
    binding: ProviderBinding,
    claim_id: String,
    report_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportRequest {
    ticket: ProviderOperationTicket,
    source_id: String,
    report_id: String,
    report: ProviderReport,
}
impl WorkflowOperations {
    fn provider_access(&self) -> ledgence_worker_api::Result<&EffectAccess> {
        self.effect_access
            .as_deref()
            .filter(|_| self.runtime_features.provider_operation())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Incompatible,
                    "provider operations were not negotiated",
                )
            })
    }
    pub(super) async fn claim_provider(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let access = self.provider_access()?;
        let payload: ClaimRequest = serde_json::from_value(request.payload)
            .map_err(|_| Error::new(ErrorKind::Protocol, "invalid provider claim request"))?;
        let command = ProviderClaimCommand {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: payload.approval_key,
            action: payload.action,
            binding: payload.binding,
            claim_id: payload.claim_id,
            report_id: payload.report_id,
        };
        command.validate().map_err(runtime_error)?;
        control.check()?;
        let service = self
            .service
            .provider_operation_client()
            .map_err(runtime_error)?;
        // A lost grant response never causes the host to dispatch again. An
        // explicit retry recovers only the existing operation's ticket.
        let reply = controlled(&control, self.request_timeout, || {
            service.claim_provider(&command)
        })
        .await;
        control.check()?;
        let reply = reply.map_err(runtime_error)?;
        if !reply.matches(&command).unwrap_or(false) {
            return Err(super::super::provider::mismatch());
        }
        let result = match reply {
            ProviderClaimReceipt::Dispatch { ticket, source } => {
                if !access.matches(&source.worker) {
                    return Err(super::super::provider::mismatch());
                }
                json!({"kind":"dispatch","ticket":ticket,"source":{
                    "source_id":source.source_id,"claim_id":source.claim_id,"report_id":source.report_id,
                    "ticket":source.ticket,"accepted_at":source.accepted_at,
                    "first_possible_dispatch_at":source.first_possible_dispatch_at,
                    "reconciliation_deadline":source.reconciliation_deadline}})
            }
            ProviderClaimReceipt::Existing { audience, ticket } => {
                if audience != access.audience {
                    return Err(super::super::provider::mismatch());
                }
                json!({"kind":"existing","ticket":ticket})
            }
            ProviderClaimReceipt::ExistingOperation { audience, ticket } => {
                if audience != access.audience {
                    return Err(super::super::provider::mismatch());
                }
                json!({"kind":"existing_operation","ticket":ticket})
            }
        };
        Ok(RuntimeReply {
            id: request.id,
            result,
        })
    }
    pub(super) async fn report_provider(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let access = self.provider_access()?;
        let payload: ReportRequest = serde_json::from_value(request.payload)
            .map_err(|_| Error::new(ErrorKind::Protocol, "invalid provider report request"))?;
        let command = ProviderDispatchReportCommand {
            owner: self.owner.clone(),
            ticket: payload.ticket,
            source_id: payload.source_id,
            report_id: payload.report_id,
            report: payload.report,
        };
        command.validate().map_err(runtime_error)?;
        let service = self
            .service
            .provider_operation_client()
            .map_err(runtime_error)?;
        loop {
            control.check()?;
            match controlled(&control, self.request_timeout, || {
                service.report_dispatch(&command)
            })
            .await
            {
                Ok(reply) => {
                    control.check()?;
                    if !reply.matches(&command).unwrap_or(false)
                        || !access.matches(&reply.receipt().worker)
                    {
                        return Err(super::super::provider::mismatch());
                    }
                    return Ok(RuntimeReply {
                        id: request.id,
                        result: super::super::provider::report_projection(&reply),
                    });
                }
                Err(error) if retryable(&error) => pause(&control, self.retry_delay).await?,
                Err(error) => {
                    control.check()?;
                    return Err(runtime_error(error));
                }
            }
        }
    }
}
