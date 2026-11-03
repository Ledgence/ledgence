use super::*;

impl ResponseValue for ProviderClaimReceipt {
    const MAX_BYTES: usize = PROVIDER_RECEIPT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ProviderReportReply {
    const MAX_BYTES: usize = PROVIDER_RECEIPT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ProviderProbeContext {
    const MAX_BYTES: usize = PROVIDER_CONTEXT_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}

macro_rules! operation {
    ($method:ident, $command:ty, $reply:ty, $path:literal) => {
        fn $method<'a>(&'a self, command: &'a $command) -> ContractFuture<'a, $reply> {
            Box::pin(async move {
                command.validate()?;
                let expected = command.clone();
                self.post_validated(
                    $path,
                    command,
                    PROVIDER_COMMAND_MAX_BYTES,
                    move |reply: &$reply| {
                        if matches!(reply.matches(&expected), Ok(true)) {
                            Ok(())
                        } else {
                            Err(unavailable("provider response identity mismatch"))
                        }
                    },
                )
                .await
            })
        }
    };
}
impl ProviderOperationClient for HttpTaskService {
    operation!(
        claim_provider,
        ProviderClaimCommand,
        ProviderClaimReceipt,
        "v1/workflows/provider-operations/claim"
    );
    operation!(
        report_dispatch,
        ProviderDispatchReportCommand,
        ProviderReportReply,
        "v1/workflows/provider-operations/report"
    );
    operation!(
        probe_context,
        ProviderProbeContextCommand,
        ProviderProbeContext,
        "v1/provider-probes/context"
    );
    operation!(
        report_probe,
        ProviderProbeReportCommand,
        ProviderReportReply,
        "v1/provider-probes/report"
    );
    operation!(
        observe_provider,
        ProviderObservationCommand,
        ProviderReportReply,
        "v1/workflows/provider-operations/observe"
    );
}
