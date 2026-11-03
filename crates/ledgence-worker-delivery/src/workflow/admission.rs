//! One-shot authorization and immutable completed-result reporting.
use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRequest {
    start_id: String,
    finish_id: String,
}

fn mismatch() -> Error {
    Error::new(ErrorKind::Protocol, "admission receipt binding mismatch")
}
impl WorkflowOperations {
    pub(super) fn admission_access(
        &self,
    ) -> ledgence_worker_api::Result<(&EffectAccess, &AdmissionWake)> {
        let access = self
            .runtime_binding
            .as_deref()
            .filter(|_| self.runtime_features.resource_admission())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Incompatible,
                    "resource admission was not negotiated",
                )
            })?;
        let wake = self
            .admission_wake
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::Protocol, "activation has no admission wake"))?;
        Ok((access, wake))
    }
    pub(super) async fn start_admission(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let (access, wake) = self.admission_access()?;
        if wake.binding.provider().is_some() {
            return Err(mismatch());
        }
        let AdmissionSelection::Offered { offer } = &wake.selection else {
            return Err(mismatch());
        };
        let payload: StartRequest =
            serde_json::from_value(request.payload).map_err(|_| mismatch())?;
        let command = AdmissionStartCommand {
            owner: self.owner.clone(),
            occurrence: wake.occurrence.clone(),
            offer_generation: offer.generation,
            start_id: payload.start_id,
            finish_id: payload.finish_id,
        };
        command.validate().map_err(runtime_error)?;
        let service = self.service.admission_client().map_err(runtime_error)?;
        control.check()?;
        // Never retry Start: only its first reply can authorize a callback.
        // Budget the entire exchange using a monotonic clock, conservatively
        // including transport time before the server accepted the request.
        let began = Instant::now();
        let reply = controlled(&control, self.request_timeout, || {
            service.start_admission(&command)
        })
        .await;
        control.check()?;
        let reply = reply.map_err(runtime_error)?;
        reply.validate().map_err(runtime_error)?;
        if !reply.matches(&command) {
            return Err(mismatch());
        }
        match &reply {
            AdmissionStartReceipt::Granted { source } => {
                if !source.matches_offer(offer)
                    || source.worker.audience != access.audience
                    || source.worker.issuer != access.identity.issuer
                    || source.worker.subject != access.identity.subject
                    || began.elapsed()
                        >= Duration::from_millis(source.start_deadline_at - source.accepted_at)
                {
                    return Err(mismatch());
                }
            }
            AdmissionStartReceipt::Existing { snapshot } => {
                if snapshot.binding != wake.binding {
                    return Err(mismatch());
                }
            }
        }
        Ok(RuntimeReply {
            id: request.id,
            result: serde_json::to_value(reply).map_err(|_| mismatch())?,
        })
    }
    pub(super) async fn finish_admission(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let (access, wake) = self.admission_access()?;
        if wake.binding.provider().is_some() {
            return Err(mismatch());
        }
        let command: AdmissionFinishCommand =
            serde_json::from_value(request.payload).map_err(|_| mismatch())?;
        command.validate().map_err(runtime_error)?;
        let source = &command.source;
        if source.occurrence != wake.occurrence
            || source.binding_digest != wake.binding.digest().map_err(runtime_error)?
            || source.owner != self.owner
            || source.worker.audience != access.audience
            || source.worker.issuer != access.identity.issuer
            || source.worker.subject != access.identity.subject
        {
            return Err(mismatch());
        }
        let AdmissionSelection::Offered { offer } = &wake.selection else {
            return Err(mismatch());
        };
        if !source.matches_offer(offer) {
            return Err(mismatch());
        }
        let service = self.service.admission_client().map_err(runtime_error)?;
        // Finish retains the exact source, report identity and output across
        // transport uncertainty; it never starts another callback.
        loop {
            control.check()?;
            match controlled(&control, self.request_timeout, || {
                service.finish_admission(&command)
            })
            .await
            {
                Ok(reply) => {
                    control.check()?;
                    reply.validate().map_err(runtime_error)?;
                    if !reply.matches(&command) {
                        return Err(mismatch());
                    }
                    return Ok(RuntimeReply {
                        id: request.id,
                        result: serde_json::to_value(reply).map_err(|_| mismatch())?,
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
