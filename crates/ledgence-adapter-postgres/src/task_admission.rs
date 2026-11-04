//! Task-local capacity authority. Lock order: session/cursor (when required),
//! task, resource, request; retained source evidence survives task collection.
mod execution;
mod lifecycle;
mod scheduler;
mod state;
use crate::workflow::admission::state as capacity;
use crate::{persistence as db, *};
use ledgence_orchestration_core as core;
pub(crate) use lifecycle::{
    apply_transition, defer_legacy_claim, expire_transition, prepare_assignment, register,
    validate_candidate,
};
pub(crate) use scheduler::{candidate, due, first_ready, tick};
use sqlx::{PgConnection, Row, postgres::PgRow};
use state::*;

impl TaskAdmissionOperations for PostgresStore {
    fn open_task_admission_session<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a TaskAdmissionSessionCommand,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(self.run(move || self.open_task_admission_session_once(p, c)))
    }
    fn start_task_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a TaskAdmissionStartCommand,
    ) -> ContractFuture<'a, TaskAdmissionStartReceipt> {
        Box::pin(self.run(move || self.start_task_admission_once(p, c)))
    }
    fn close_task_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a TaskAdmissionCloseCommand,
    ) -> ContractFuture<'a, TaskAdmissionCloseReceipt> {
        Box::pin(self.run(move || self.close_task_admission_once(p, c)))
    }
    fn inspect_task_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a TaskAdmissionQuery,
    ) -> ContractFuture<'a, TaskAdmissionSnapshot> {
        Box::pin(self.run(move || self.inspect_task_admission_once(p, q)))
    }
    fn release_task_admission<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a TaskAdmissionReleaseCommand,
    ) -> ContractFuture<'a, TaskAdmissionReleaseReceipt> {
        Box::pin(self.run(move || self.release_task_admission_once(p, c)))
    }
}
impl TaskAdmissionStore for PostgresStore {}

fn corrupt(message: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid task admission record: {message}"))
}
fn canonical<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    canonical_json_bytes(&serde_json::to_value(value).map_err(|_| corrupt("encoding"))?)
        .map_err(Into::into)
}
fn proof(value: Option<bool>, message: &str) -> StoreResult<()> {
    if value == Some(true) {
        Ok(())
    } else {
        Err(corrupt(message).into())
    }
}
