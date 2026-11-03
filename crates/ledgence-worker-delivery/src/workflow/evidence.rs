//! V2 retains evidence separately from unchanged local journal bytes.
use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimRequest {
    approval_key: String,
    action: ApprovalAction,
    lookup: String,
    claim_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmRequest {
    approval_key: String,
    lookup: String,
    record: LocalStepRecord,
    claim_id: String,
    observation_id: String,
    reported_callback: EffectReportedCallback,
}
fn mismatch() -> Error {
    Error::new(
        ErrorKind::Protocol,
        "effect evidence reply disagrees with its negotiated connection or command",
    )
}
impl WorkflowOperations {
    fn evidence_access(&self) -> ledgence_worker_api::Result<&EffectAccess> {
        self.effect_access.as_deref().ok_or_else(|| {
            Error::new(
                ErrorKind::Incompatible,
                "effect evidence v2 was not negotiated",
            )
        })
    }
    pub(super) async fn claim_effect_v2(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let access = self.evidence_access()?;
        let payload: ClaimRequest = serde_json::from_value(request.payload)
            .map_err(|_| Error::new(ErrorKind::Protocol, "invalid effect claim v2 request"))?;
        let command = EffectClaimV2Command {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: payload.approval_key,
            action: payload.action,
            lookup: payload.lookup,
            claim_id: payload.claim_id,
        };
        command.validate().map_err(runtime_error)?;
        control.check()?;
        let effects = self
            .service
            .effect_evidence_client()
            .map_err(runtime_error)?;
        // Do not resend a possible dispatch grant. Recovery requires a new
        // explicit owned invocation and its verified current permission.
        let reply = controlled(&control, self.request_timeout, || {
            effects.claim_v2(&command)
        })
        .await;
        control.check()?;
        let reply = reply.map_err(runtime_error)?;
        if !matches!(reply.matches(&command), Ok(true)) {
            return Err(mismatch());
        }
        let result = match reply {
            EffectClaimV2Receipt::Replay {
                audience,
                occurrence,
                record,
            } => {
                if audience != access.audience {
                    return Err(mismatch());
                }
                json!({"kind":"replay", "workflow_id":occurrence.workflow_id,
                    "activation_id":occurrence.activation_id,"approval_key":occurrence.approval_key,
                    "record":record})
            }
            receipt @ (EffectClaimV2Receipt::Dispatch { .. }
            | EffectClaimV2Receipt::LookupOnly { .. }) => {
                let kind = if matches!(receipt, EffectClaimV2Receipt::Dispatch { .. }) {
                    "dispatch"
                } else {
                    "lookup_only"
                };
                let claim = match receipt {
                    EffectClaimV2Receipt::Dispatch { claim }
                    | EffectClaimV2Receipt::LookupOnly { claim } => claim,
                    _ => unreachable!(),
                };
                if !access.matches(&claim.worker) {
                    return Err(mismatch());
                }
                json!({"kind":kind,"claim":{
                    "workflow_id":claim.workflow_id,"activation_id":claim.activation_id,
                    "approval_key":claim.approval_key,"claim_id":claim.claim_id,
                    "action":claim.action,"lookup":claim.lookup,"claimed_at":claim.claimed_at,
                    "initial_permission":claim.initial_permission,"recorded_revision":claim.recorded_revision,
                    "already_accepted":claim.already_accepted}})
            }
        };
        Ok(RuntimeReply {
            id: request.id,
            result,
        })
    }
    pub(super) async fn confirm_effect_v2(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let access = self.evidence_access()?;
        let payload: ConfirmRequest = serde_json::from_value(request.payload).map_err(|_| {
            Error::new(
                ErrorKind::Protocol,
                "invalid effect confirmation v2 request",
            )
        })?;
        let command = EffectConfirmV2Command {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: payload.approval_key,
            lookup: payload.lookup,
            record: payload.record,
            claim_id: payload.claim_id,
            observation_id: payload.observation_id,
            reported_callback: payload.reported_callback,
        };
        command.validate().map_err(runtime_error)?;
        let effects = self
            .service
            .effect_evidence_client()
            .map_err(runtime_error)?;
        loop {
            control.check()?;
            match controlled(&control, self.request_timeout, || {
                effects.confirm_v2(&command)
            })
            .await
            {
                Ok(reply) => {
                    control.check()?;
                    if !matches!(reply.matches(&command), Ok(true))
                        || !access.matches(&reply.worker)
                    {
                        return Err(mismatch());
                    }
                    return Ok(RuntimeReply {
                        id: request.id,
                        result: json!({
                            "committed":true,"workflow_id":reply.workflow_id,"activation_id":reply.activation_id,
                            "approval_key":reply.approval_key,"key":reply.key,"claim_id":reply.claim_id,
                            "observation_id":reply.observation_id,"recorded_at":reply.recorded_at,
                            "recorded_revision":reply.recorded_revision,"already_accepted":reply.already_accepted,
                        }),
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
