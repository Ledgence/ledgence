use super::*;
impl ResponseValue for ResourcePutReceipt {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ResourceSnapshot {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ResourceDecisionPage {
    const MAX_BYTES: usize = ADMISSION_PAGE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for ResourcePage {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for AdmissionStartReceipt {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for AdmissionFinishReceipt {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for AdmissionSnapshot {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for AdmissionReleaseReceipt {
    const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl AdmissionClient for HttpTaskService {
    fn list_resource_decisions<'a>(
        &'a self,
        query: &'a ResourceDecisionQuery,
    ) -> ContractFuture<'a, ResourceDecisionPage> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/resources/decisions",
                query,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &ResourceDecisionPage| reply.validate_for(&expected),
            )
            .await
        })
    }
    fn put_resource<'a>(
        &'a self,
        command: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/resources/put",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &ResourcePutReceipt| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("admission response identity mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn resource<'a>(&'a self, command: &'a ResourceQuery) -> ContractFuture<'a, ResourceSnapshot> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/resources/query",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &ResourceSnapshot| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("admission response identity mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn list_resources<'a>(
        &'a self,
        command: &'a ResourceListQuery,
    ) -> ContractFuture<'a, ResourcePage> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/resources/list",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &ResourcePage| reply.validate_for(&expected),
            )
            .await
        })
    }
    fn start_admission<'a>(
        &'a self,
        command: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/admissions/start",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &AdmissionStartReceipt| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("admission response identity mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn finish_admission<'a>(
        &'a self,
        command: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/admissions/finish",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &AdmissionFinishReceipt| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("admission response identity mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn inspect_admission<'a>(
        &'a self,
        command: &'a AdmissionQuery,
    ) -> ContractFuture<'a, AdmissionSnapshot> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/admissions/inspect",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &AdmissionSnapshot| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("admission response identity mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn release_admission<'a>(
        &'a self,
        command: &'a AdmissionReleaseCommand,
    ) -> ContractFuture<'a, AdmissionReleaseReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/admissions/release",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &AdmissionReleaseReceipt| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("admission response identity mismatch"))
                    }
                },
            )
            .await
        })
    }
}
