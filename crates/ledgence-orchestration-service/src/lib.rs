//! Application coordination over portable, atomic persistence operations.
//!
//! Program resolution happens outside storage transactions. The store remains
//! authoritative for concurrent submission acceptance, generated identities,
//! database time, and commit. No database implementation is selected here.

mod acquisition;
pub use acquisition::AcquisitionStatistics;

use ledgence_orchestration_api::*;
use ledgence_orchestration_core::{replay_submission, validate_submission};
use ledgence_worker_api::{Error, ErrorKind, ProgramStore};
use std::sync::Arc;
use tracing::Instrument;

/// Implements client/worker operations using independently supplied adapters.
#[derive(Clone)]
pub struct ApplicationService {
    store: Arc<dyn TaskStore>,
    programs: Arc<dyn ProgramStore>,
    acquisition: Arc<acquisition::Coordinator>,
}

impl ApplicationService {
    pub fn new(store: Arc<dyn TaskStore>, programs: Arc<dyn ProgramStore>) -> Self {
        Self {
            store,
            programs,
            acquisition: acquisition::Coordinator::new(),
        }
    }

    /// Adapter wake sink; hints are advisory and contain no cached authority.
    pub fn acquisition_wake(&self) -> Arc<dyn AcquisitionWake> {
        self.acquisition.clone()
    }

    /// Reject new acquisitions and finalize accepted waiters under their original budgets.
    pub fn stop_acquisitions(&self) {
        self.acquisition.stop();
    }

    pub fn acquisition_statistics(&self) -> AcquisitionStatistics {
        self.acquisition.statistics()
    }

    async fn accepted_submission(&self, command: &SubmitCommand) -> Result<Option<TaskSnapshot>> {
        let scope = Scope {
            tenant_id: command.input.tenant_id.clone(),
            namespace: command.input.namespace.clone(),
        };
        let accepted = self
            .store
            .lookup_submission(&scope, &command.idempotency_key)
            .await?;
        if let Some(task) = &accepted {
            replay_submission(task, command)?;
        }
        Ok(accepted)
    }
}

impl TaskService for ApplicationService {
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        self.store.open_session(scope, queue, concurrency)
    }

    fn extend_session<'a>(
        &'a self,
        worker_session_id: &'a str,
    ) -> ContractFuture<'a, WorkerSession> {
        self.store.extend_session(worker_session_id)
    }

    fn submit<'a>(&'a self, command: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move {
            validate_submission(command)?;
            let span = tracing::info_span!(
                "ledgence.task.submit",
                otel.kind = "internal",
                ledgence.tenant.id = command.input.tenant_id,
                ledgence.namespace = command.input.namespace,
                ledgence.program.id = command.input.program.id,
                ledgence.program.version = command.input.program.version,
                ledgence.business.correlation_key = command.input.correlation_key.as_deref(),
                ledgence.run.id = tracing::field::Empty,
                ledgence.task.id = tracing::field::Empty,
                ledgence.program.digest = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            let result = async {
                if let Some(accepted) = self.accepted_submission(command).await? {
                    return Ok(accepted);
                }

                let resolved = self
                    .programs
                    .resolve(&command.input.program)
                    .await
                    .and_then(|descriptor| {
                        descriptor.validate().map_err(|error| {
                            Error::new(
                                ErrorKind::Integrity,
                                format!("invalid resolved program descriptor: {error}"),
                            )
                        })?;
                        if descriptor.program != command.input.program {
                            return Err(Error::new(
                                ErrorKind::Protocol,
                                "program store resolved a different program",
                            ));
                        }
                        Ok(descriptor)
                    });
                let descriptor = match resolved {
                    Ok(descriptor) => descriptor,
                    Err(error) => {
                        // A concurrent submitter may have committed while this
                        // resolver was waiting or failing. Its binding wins.
                        return match self.accepted_submission(command).await? {
                            Some(accepted) => Ok(accepted),
                            None => Err(resolution_error(error)),
                        };
                    }
                };
                self.store
                    .accept_resolved_submission(command, &descriptor)
                    .await
            }
            .instrument(span.clone())
            .await;
            match &result {
                Ok(task) => {
                    span.record("ledgence.run.id", &task.run_id);
                    span.record("ledgence.task.id", &task.task_id);
                    span.record("ledgence.program.digest", &task.descriptor.digest.0);
                }
                Err(_) => {
                    span.record("otel.status_code", "ERROR");
                }
            }
            result
        })
    }

    fn inspect<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
    ) -> ContractFuture<'a, TaskSnapshot> {
        self.store.inspect(scope, task_id)
    }

    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        attempt_id: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        self.store.inspect_attempt(scope, task_id, attempt_id)
    }

    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        after_sequence: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        self.store.history(scope, task_id, after_sequence)
    }

    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, AcquireReply> {
        Box::pin(
            self.acquisition
                .acquire(self.store.as_ref(), command, options),
        )
    }

    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        self.store.renew(command)
    }

    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        self.store.settle(command)
    }

    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        self.store.confirm_quiescence(owner)
    }

    fn cancel<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskState> {
        self.store.cancel(scope, task_id)
    }
}

fn resolution_error(error: Error) -> ContractError {
    match error.kind {
        ErrorKind::NotFound => ContractError::NotFound,
        ErrorKind::InvalidInput | ErrorKind::Incompatible => {
            ContractError::InvalidInput(error.to_string())
        }
        ErrorKind::Integrity
        | ErrorKind::Protocol
        | ErrorKind::Unavailable
        | ErrorKind::Cancelled
        | ErrorKind::TimedOut
        | ErrorKind::Runtime
        | ErrorKind::Io
        | ErrorKind::Capacity => ContractError::Unavailable(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
