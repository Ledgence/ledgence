use super::*;

/// Complete authenticated task admission capability. Partial implementations
/// must not advertise it or accept governed submissions.
pub trait TaskAdmissionOperations: Send + Sync {
    fn open_task_admission_session<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionSessionCommand,
    ) -> ContractFuture<'a, WorkerSession>;
    fn start_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionStartCommand,
    ) -> ContractFuture<'a, TaskAdmissionStartReceipt>;
    fn close_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionCloseCommand,
    ) -> ContractFuture<'a, TaskAdmissionCloseReceipt>;
    fn inspect_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a TaskAdmissionQuery,
    ) -> ContractFuture<'a, TaskAdmissionSnapshot>;
    fn release_task_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a TaskAdmissionReleaseCommand,
    ) -> ContractFuture<'a, TaskAdmissionReleaseReceipt>;
}
/// Implementors also atomically integrate submission, acquisition, expiry,
/// settlement, cancellation, retention and the shared admission scheduler.
pub trait TaskAdmissionStore: TaskAdmissionOperations {}
pub trait TaskAdmissionClient: Send + Sync {
    fn open_task_admission_session<'a>(
        &'a self,
        command: &'a TaskAdmissionSessionCommand,
    ) -> ContractFuture<'a, WorkerSession>;
    fn start_task_admission<'a>(
        &'a self,
        command: &'a TaskAdmissionStartCommand,
    ) -> ContractFuture<'a, TaskAdmissionStartReceipt>;
    fn close_task_admission<'a>(
        &'a self,
        command: &'a TaskAdmissionCloseCommand,
    ) -> ContractFuture<'a, TaskAdmissionCloseReceipt>;
    fn inspect_task_admission<'a>(
        &'a self,
        query: &'a TaskAdmissionQuery,
    ) -> ContractFuture<'a, TaskAdmissionSnapshot>;
    fn release_task_admission<'a>(
        &'a self,
        command: &'a TaskAdmissionReleaseCommand,
    ) -> ContractFuture<'a, TaskAdmissionReleaseReceipt>;
}
