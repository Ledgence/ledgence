//! One atomic dispatch grant and immutable source-bound provider evidence.
use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRequest {
    start_id: String,
    finish_id: String,
}
fn mismatch() -> Error {
    Error::new(ErrorKind::Protocol, "provider admission authority mismatch")
}
impl WorkflowOperations {
    fn provider_admission_access(
        &self,
    ) -> ledgence_worker_api::Result<(&EffectAccess, &AdmissionWake, &ProviderAdmissionBinding)>
    {
        if !self.runtime_features.provider_admission() {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "atomic provider admission was not negotiated",
            ));
        }
        let (access, wake) = self.admission_access()?;
        let AdmissionBinding::Provider(binding) = &wake.binding else {
            return Err(mismatch());
        };
        Ok((access, wake, binding))
    }
    pub(super) async fn start_provider_admission(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let (access, wake, binding) = self.provider_admission_access()?;
        let AdmissionSelection::Offered { offer } = &wake.selection else {
            return Err(mismatch());
        };
        let payload: StartRequest =
            serde_json::from_value(request.payload).map_err(|_| mismatch())?;
        let command = ProviderAdmissionStartCommand {
            admission: AdmissionStartCommand {
                owner: self.owner.clone(),
                occurrence: wake.occurrence.clone(),
                offer_generation: offer.generation,
                start_id: payload.start_id,
                finish_id: payload.finish_id,
            },
        };
        command.validate().map_err(runtime_error)?;
        let service = self
            .service
            .provider_admission_client()
            .map_err(runtime_error)?;
        control.check()?;
        let began = Instant::now();
        // Never retry Start: a lost first reply may already have consumed both
        // authorities. Inspection recovers a ticket, never dispatch permission.
        let reply = controlled(&control, self.request_timeout, || {
            service.start_provider_admission(&command)
        })
        .await;
        control.check()?;
        let reply = reply.map_err(runtime_error)?;
        if !reply.matches(&command).map_err(runtime_error)? {
            return Err(mismatch());
        }
        match &reply {
            ProviderAdmissionStartReceipt::Granted { source } => {
                let admission = &source.admission;
                if !admission.matches_offer(offer)
                    || !access.matches(&source.provider.worker)
                    || !source.matches_binding(binding).map_err(runtime_error)?
                    || began.elapsed()
                        >= Duration::from_millis(
                            admission.start_deadline_at - admission.accepted_at,
                        )
                {
                    return Err(mismatch());
                }
            }
            ProviderAdmissionStartReceipt::Existing { audience, snapshot } => {
                if audience != &access.audience || snapshot.binding != wake.binding {
                    return Err(mismatch());
                }
            }
        }
        Ok(RuntimeReply {
            id: request.id,
            result: serde_json::to_value(reply).map_err(|_| mismatch())?,
        })
    }
    pub(super) async fn report_provider_admission(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let (access, wake, binding) = self.provider_admission_access()?;
        let command: ProviderAdmissionReportCommand =
            serde_json::from_value(request.payload).map_err(|_| mismatch())?;
        command.validate().map_err(runtime_error)?;
        let source = &command.source;
        let AdmissionSelection::Offered { offer } = &wake.selection else {
            return Err(mismatch());
        };
        if source.admission.owner != self.owner
            || !source.admission.matches_offer(offer)
            || !source.matches_binding(binding).map_err(runtime_error)?
            || !access.matches(&source.provider.worker)
        {
            return Err(mismatch());
        }
        let service = self
            .service
            .provider_admission_client()
            .map_err(runtime_error)?;
        loop {
            control.check()?;
            match controlled(&control, self.request_timeout, || {
                service.report_provider_admission(&command)
            })
            .await
            {
                Ok(reply) => {
                    control.check()?;
                    if !reply.matches(&command).map_err(runtime_error)? {
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
