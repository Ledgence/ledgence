//! Atomic provider dispatch and resource authority share one server exchange.
use super::*;

impl ResponseValue for ProviderAdmissionStartReceipt {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ProviderAdmissionReportReceipt {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
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
                        if reply.matches(&expected)? {
                            Ok(())
                        } else {
                            Err(unavailable("provider admission response identity mismatch"))
                        }
                    },
                )
                .await
            })
        }
    };
}
impl ProviderAdmissionClient for HttpTaskService {
    operation!(
        start_provider_admission,
        ProviderAdmissionStartCommand,
        ProviderAdmissionStartReceipt,
        "v1/provider-admissions/start"
    );
    operation!(
        report_provider_admission,
        ProviderAdmissionReportCommand,
        ProviderAdmissionReportReceipt,
        "v1/provider-admissions/report"
    );
}
