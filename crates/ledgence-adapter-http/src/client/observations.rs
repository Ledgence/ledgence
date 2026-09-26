use super::*;
use ledgence_orchestration_api::console::{
    CONSOLE_MAX_TIMESTAMP, WORKER_OBSERVATION_MAX_BYTES, WorkerObservationCommand,
    WorkerObservationPublisher, WorkerObservationReceipt,
};

impl ResponseValue for WorkerObservationReceipt {
    const MAX_BYTES: usize = 4096;

    fn validate_values(&self) -> Result<()> {
        validate_text(&self.worker_session_id, 128)?;
        if self.sequence.0 == 0 || self.received_at > CONSOLE_MAX_TIMESTAMP {
            return Err(unavailable("invalid worker observation receipt"));
        }
        Ok(())
    }
}

impl WorkerObservationPublisher for HttpTaskService {
    fn publish_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt> {
        Box::pin(async move {
            command.validate()?;
            let session = command.worker_session_id.clone();
            let sequence = command.sequence;
            self.post_validated(
                "v1/worker-observations",
                command,
                WORKER_OBSERVATION_MAX_BYTES,
                move |receipt: &WorkerObservationReceipt| {
                    if receipt.worker_session_id != session || receipt.sequence != sequence {
                        return Err(unavailable("worker observation receipt identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
}
