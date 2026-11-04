//! Authenticated standalone task admission; no automatic grant or close retries.
use super::*;

macro_rules! response {
    ($($kind:ty),+ $(,)?) => { $(impl ResponseValue for $kind {
        const MAX_BYTES: usize = ADMISSION_RESPONSE_MAX_BYTES;
        fn validate_values(&self) -> Result<()> { self.validate() }
    })+ };
}
response!(
    TaskAdmissionStartReceipt,
    TaskAdmissionCloseReceipt,
    TaskAdmissionSnapshot,
    TaskAdmissionReleaseReceipt
);

impl TaskAdmissionClient for HttpTaskService {
    fn open_task_admission_session<'a>(
        &'a self,
        command: &'a TaskAdmissionSessionCommand,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/task-admissions/sessions",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &WorkerSession| {
                    validate_text(&reply.id, 128)?;
                    if expected.matches(reply) {
                        Ok(())
                    } else {
                        Err(unavailable("task admission session mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn start_task_admission<'a>(
        &'a self,
        command: &'a TaskAdmissionStartCommand,
    ) -> ContractFuture<'a, TaskAdmissionStartReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/task-admissions/start",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &TaskAdmissionStartReceipt| {
                    if reply.matches(&expected)? {
                        Ok(())
                    } else {
                        Err(unavailable("task admission Start mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn close_task_admission<'a>(
        &'a self,
        command: &'a TaskAdmissionCloseCommand,
    ) -> ContractFuture<'a, TaskAdmissionCloseReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/task-admissions/close",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &TaskAdmissionCloseReceipt| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("task admission Close mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn inspect_task_admission<'a>(
        &'a self,
        query: &'a TaskAdmissionQuery,
    ) -> ContractFuture<'a, TaskAdmissionSnapshot> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.occurrence.clone();
            self.post_validated(
                "v1/task-admissions/inspect",
                query,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &TaskAdmissionSnapshot| {
                    if reply.occurrence == expected {
                        Ok(())
                    } else {
                        Err(unavailable("task admission inspection mismatch"))
                    }
                },
            )
            .await
        })
    }
    fn release_task_admission<'a>(
        &'a self,
        command: &'a TaskAdmissionReleaseCommand,
    ) -> ContractFuture<'a, TaskAdmissionReleaseReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/task-admissions/release",
                command,
                ADMISSION_COMMAND_MAX_BYTES,
                move |reply: &TaskAdmissionReleaseReceipt| {
                    if reply.matches(&expected) {
                        Ok(())
                    } else {
                        Err(unavailable("task admission release mismatch"))
                    }
                },
            )
            .await
        })
    }
}
