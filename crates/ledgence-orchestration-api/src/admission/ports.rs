use super::*;
/// Complete provider-neutral admission capability. Returned grants become durable
/// before replies. Replaying Start may never reissue executable permission.
pub trait AdmissionOperations: Send + Sync {
    fn put_resource<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt>;
    fn resource<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a ResourceQuery,
    ) -> ContractFuture<'a, ResourceSnapshot>;
    fn list_resources<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a ResourceListQuery,
    ) -> ContractFuture<'a, ResourcePage>;
    fn start_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt>;
    fn finish_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt>;
    fn inspect_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a AdmissionQuery,
    ) -> ContractFuture<'a, AdmissionSnapshot>;
    fn release_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a AdmissionReleaseCommand,
    ) -> ContractFuture<'a, AdmissionReleaseReceipt>;
}
/// A store must commit its wait/wake/task/outbox and accounting writes together.
/// Maintenance is bounded; no transaction spans execution or a network request.
pub trait AdmissionStore: AdmissionOperations {
    fn advance_admissions<'a>(
        &'a self,
        query: &'a AdmissionMaintenanceQuery,
    ) -> ContractFuture<'a, AdmissionMaintenanceProgress>;
    fn apply_admission_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        resolved_children: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress>;
}
pub trait AdmissionClient: Send + Sync {
    fn put_resource<'a>(
        &'a self,
        command: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt>;
    fn resource<'a>(&'a self, query: &'a ResourceQuery) -> ContractFuture<'a, ResourceSnapshot>;
    fn list_resources<'a>(
        &'a self,
        query: &'a ResourceListQuery,
    ) -> ContractFuture<'a, ResourcePage>;
    fn start_admission<'a>(
        &'a self,
        command: &'a AdmissionStartCommand,
    ) -> ContractFuture<'a, AdmissionStartReceipt>;
    fn finish_admission<'a>(
        &'a self,
        command: &'a AdmissionFinishCommand,
    ) -> ContractFuture<'a, AdmissionFinishReceipt>;
    fn inspect_admission<'a>(
        &'a self,
        query: &'a AdmissionQuery,
    ) -> ContractFuture<'a, AdmissionSnapshot>;
    fn release_admission<'a>(
        &'a self,
        command: &'a AdmissionReleaseCommand,
    ) -> ContractFuture<'a, AdmissionReleaseReceipt>;
}
