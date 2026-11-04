//! Composition boundaries for custom stores: no partial feature may do work.
use super::*;

#[derive(Clone, Copy)]
enum Missing {
    Reader,
    Audit,
    Evidence,
    Admission,
    Provider,
    Composition,
}
struct Store {
    missing: Option<Missing>,
    calls: AtomicUsize,
    pending: Mutex<Vec<WorkflowWork>>,
}
impl Store {
    fn new(missing: Option<Missing>) -> Self {
        Self {
            missing,
            calls: AtomicUsize::new(0),
            pending: Mutex::new(vec![]),
        }
    }
    fn reached<T>(&self) -> ContractFuture<'_, T> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(ContractError::NotFound) })
    }
}
macro_rules! unused_methods {
    ($(fn $name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {$ (
        fn $name<'a>(&'a self, $($arg: $ty),*) -> ContractFuture<'a, $result> { unused() }
    )*};
}
impl WorkflowStore for Store {
    fn admissions(&self) -> Result<&dyn AdmissionStore> {
        if matches!(self.missing, Some(Missing::Admission)) {
            Err(unavailable())
        } else {
            Ok(self)
        }
    }
    fn provider_operations(&self) -> Result<&dyn ProviderOperationStore> {
        if matches!(self.missing, Some(Missing::Provider)) {
            Err(unavailable())
        } else {
            Ok(self)
        }
    }
    fn provider_admissions(&self) -> Result<&dyn ProviderAdmissionStore> {
        if matches!(self.missing, Some(Missing::Composition)) {
            Err(unavailable())
        } else {
            Ok(self)
        }
    }
    fn approved_effect_evidence(&self) -> Result<&dyn ApprovedEffectEvidence> {
        if matches!(self.missing, Some(Missing::Evidence)) {
            Err(unavailable())
        } else {
            Ok(self)
        }
    }
    fn effect_resolution(&self) -> Result<&dyn EffectResolutionStore> {
        Ok(self)
    }
    fn effect_evidence_reader(&self) -> Result<&dyn EffectEvidenceReader> {
        if matches!(self.missing, Some(Missing::Reader)) {
            Err(unavailable())
        } else {
            Ok(self)
        }
    }
    fn operator_access(&self) -> Result<&dyn OperatorAccess> {
        if matches!(self.missing, Some(Missing::Audit)) {
            Err(unavailable())
        } else {
            Ok(self)
        }
    }
    fn claim_work(&self, _: u32) -> ContractFuture<'_, Vec<WorkflowWork>> {
        Box::pin(async { Ok(std::mem::take(&mut *self.pending.lock().unwrap())) })
    }
    unused_methods! {
        fn send_workflow_event(_c: &'a WorkflowEventCommand) -> WorkflowEventReceipt;
        fn lookup_workflow_submission(_s: &'a Scope, _key: &'a str) -> Option<WorkflowSnapshot>;
        fn replay_workflow_submission(_c: &'a SubmitCommand) -> Option<WorkflowSnapshot>;
        fn accept_resolved_workflow(_c: &'a SubmitCommand, _p: &'a ProgramDescriptor) -> WorkflowSnapshot;
        fn workflow_status(_s: &'a Scope, _id: &'a str) -> WorkflowSnapshot;
        fn workflow_result(_s: &'a Scope, _id: &'a str) -> WorkflowResult;
        fn activation_context(_o: &'a LeaseOwner) -> WorkflowActivationContext;
        fn record_local_result(_c: &'a LocalResultCommand) -> LocalResultReceipt;
        fn apply_work(_w: &'a WorkflowWork, _r: &'a [ResolvedWorkflowChild]) -> WorkflowProgress;
        fn retry_work(_w: &'a WorkflowWork, _reason: &'a str) -> ();
        fn reject_work(_w: &'a WorkflowWork, _error: &'a ApplicationError) -> ();
        fn cancel_workflow(_s: &'a Scope, _id: &'a str) -> WorkflowSnapshot;
    }
}
impl ApprovedEffectEvidence for Store {
    fn claim_v2<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        _: &'a EffectClaimV2Command,
    ) -> ContractFuture<'a, EffectClaimV2Receipt> {
        self.reached()
    }
    fn confirm_v2<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        _: &'a EffectConfirmV2Command,
    ) -> ContractFuture<'a, EffectConfirmV2Receipt> {
        self.reached()
    }
    fn observe<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        _: &'a EffectObservationCommand,
    ) -> ContractFuture<'a, EffectObservationReceipt> {
        self.reached()
    }
}
impl EffectResolution for Store {
    fn resolve_effect<'a>(
        &'a self,
        _: &'a VerifiedPrincipal,
        _: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, AuditedEffectResolution> {
        self.reached()
    }
}
impl EffectResolutionStore for Store {
    fn apply_effect_work<'a>(
        &'a self,
        _: &'a WorkflowWork,
        _: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(WorkflowProgress {
                processed: 1,
                activations_scheduled: 1,
                children_scheduled: 0,
            })
        })
    }
}
impl EffectEvidenceReader for Store {
    unused_methods! {
        fn effect_inbox(_q: &'a EffectInboxQuery) -> EffectInboxPage;
        fn inspect_effect(_q: &'a EffectInspectQuery) -> EffectEvidenceDetail;
    }
}
impl OperatorAccess for Store {
    unused_methods! {
        fn decide_verified(_p: &'a VerifiedPrincipal, _c: &'a ApprovalDecisionCommand, _r: &'a OperatorRequest) -> AuditedDecision;
        fn record_sensitive_response(_p: &'a VerifiedPrincipal, _r: &'a SensitiveObservation) -> AuditReceipt;
        fn query_audit(_p: &'a VerifiedPrincipal, _q: &'a AuditQuery) -> AuditPage;
    }
}
fn unavailable() -> ContractError {
    ContractError::Unavailable("intentionally absent capability".into())
}
fn owner() -> LeaseOwner {
    LeaseOwner {
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "billing".into(),
        },
        task_id: "activation".into(),
        attempt_id: "attempt".into(),
        lease_id: "lease".into(),
        generation: 1,
        worker_session_id: "session".into(),
        consumer_id: 0,
    }
}
fn principal(worker: bool) -> VerifiedPrincipal {
    let audience = AccessAudience {
        authority_id: "authority".into(),
        instance_id: "instance".into(),
        scope: owner().scope,
    };
    VerifiedPrincipal::new(
        PrincipalIdentity {
            issuer: audience.issuer(),
            subject: "subject".into(),
        },
        audience,
        "credential".into(),
        if worker {
            PrincipalClass::Worker
        } else {
            PrincipalClass::Operator
        },
        1000,
        vec![if worker {
            AccessCapability::WorkerExecute
        } else {
            AccessCapability::EffectResolve
        }],
    )
    .unwrap()
}
fn claim() -> EffectClaimV2Command {
    EffectClaimV2Command {
        budget_fence: false,
        owner: owner(),
        workflow_id: "workflow".into(),
        approval_key: "approval".into(),
        action: ApprovalAction {
            name: "app:send".into(),
            version: "1".into(),
            arguments: json!({}),
        },
        lookup: "app:lookup".into(),
        claim_id: "claim".into(),
    }
}
fn service(missing: Option<Missing>) -> (ApplicationService, Arc<Store>) {
    let store = Arc::new(Store::new(missing));
    (Fixture::new().service.with_workflows(store.clone()), store)
}
async fn evidence_call(which: u8, missing: Option<Missing>) -> (Result<()>, usize) {
    let (service, store) = service(missing);
    let c = claim();
    let p = principal(true);
    let result=match which {
        0=>ApprovedEffectEvidence::claim_v2(&service,&p,&c).await.map(|_|()),
        1=>ApprovedEffectEvidence::confirm_v2(&service,&p,&EffectConfirmV2Command {owner:c.owner.clone(),workflow_id:c.workflow_id.clone(),approval_key:c.approval_key.clone(),lookup:c.lookup.clone(),record:LocalStepRecord {key:approved_effect_step_key(&c.approval_key),callable:c.action.name.clone(),input:json!({"v":1,"action_json":serde_json::to_string(&c.action).unwrap(),"lookup":c.lookup}),output:json!(null)},claim_id:c.claim_id.clone(),observation_id:"observation".into(),reported_callback:EffectReportedCallback::Dispatch}).await.map(|_|()),
        _=>ApprovedEffectEvidence::observe(&service,&p,&EffectObservationCommand {occurrence:c.occurrence(),claim_id:c.claim_id.clone(),observation_id:"observation".into(),reported_callback:EffectReportedCallback::Dispatch,output:json!(null)}).await.map(|_|()),
    };
    (result, store.calls.load(Ordering::SeqCst))
}
async fn evidence_is_complete(which: u8) {
    for missing in [Missing::Reader, Missing::Audit] {
        let (result, calls) = evidence_call(which, Some(missing)).await;
        assert!(matches!(result, Err(ContractError::Unavailable(_))));
        assert_eq!(calls, 0, "partial store must not receive mutation");
    }
    let (result, calls) = evidence_call(which, None).await;
    assert_eq!(result, Err(ContractError::NotFound));
    assert_eq!(calls, 1, "complete store remains callable");
}
#[tokio::test]
async fn direct_claim_requires_complete_evidence_capability() {
    evidence_is_complete(0).await;
}
#[tokio::test]
async fn direct_confirm_requires_complete_evidence_capability() {
    evidence_is_complete(1).await;
}
#[tokio::test]
async fn direct_observe_requires_complete_evidence_capability() {
    evidence_is_complete(2).await;
}
#[tokio::test]
async fn direct_resolution_requires_complete_native_capability() {
    for missing in [
        Some(Missing::Reader),
        Some(Missing::Audit),
        Some(Missing::Evidence),
        None,
    ] {
        let (service, store) = service(missing);
        let c = claim();
        let command = EffectResolutionCommand {
            occurrence: c.occurrence(),
            resolution_id: "resolution".into(),
            expected_effect_revision: 1,
            supersedes_resolution_id: None,
            action: c.action,
            assessment: EffectResolutionAssessment::CloseUnknown,
            reason: "Close without asserting provider truth".into(),
            evidence_references: vec![],
        };
        let result = EffectResolution::resolve_effect(&service, &principal(false), &command).await;
        if missing.is_some() {
            assert!(matches!(result, Err(ContractError::Unavailable(_))));
            assert_eq!(store.calls.load(Ordering::SeqCst), 0);
        } else {
            assert!(matches!(result, Err(ContractError::NotFound)));
            assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        }
    }
}
fn native_work(park: bool) -> WorkflowWork {
    WorkflowWork {id:"work".into(),token:"token".into(),workflow_id:"workflow".into(),source:if park {WorkflowWorkSource::TaskTerminal{task_id:"activation".into(),activation:true}} else {WorkflowWorkSource::EffectResolutionWait}, outcome:park.then(||TaskOutcome::Succeeded{attempt_id:"attempt".into(),quiescence:Quiescence::Confirmed,execution_may_have_started:true,output:json!({"v":1,"activation_id":"activation","revision":0,"kind":"wait","state":null,"continuation":"next","commands":[],"wait":{"kind":"effect_resolution","key":"park","occurrence":claim().occurrence(),"after_resolution_id":null}})}),resolved_children:vec![]}
}
#[tokio::test]
async fn native_park_admission_requires_complete_capability() {
    for missing in [Missing::Reader, Missing::Audit, Missing::Evidence] {
        let (service, _) = service(Some(missing));
        let result = service
            .resolve_workflow_children(&native_work(true), &mut vec![])
            .await;
        assert!(matches!(result, Err(ContractError::InvalidInput(_))));
    }
    let (service, _) = service(None);
    assert!(
        service
            .resolve_workflow_children(&native_work(true), &mut vec![])
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn existing_native_work_does_not_bypass_complete_capability() {
    for missing in [Missing::Reader, Missing::Audit, Missing::Evidence] {
        let (service, store) = service(Some(missing));
        store.pending.lock().unwrap().push(native_work(false));
        assert!(matches!(
            service.advance_workflows(1).await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    }
}
fn context(wake: WorkflowWake) -> WorkflowActivationContext {
    WorkflowActivationContext {
        v: 1,
        parent_workflow_id: None,
        root_workflow_id: None,
        workflow_id: "workflow".into(),
        activation_id: "activation".into(),
        revision: 1,
        continuation: "next".into(),
        state: json!(null),
        inputs: Default::default(),
        local_steps: vec![],
        admission_result: None,
        provider_admission: None,
        wake: Some(wake),
    }
}
fn assert_context_scope(wake: WorkflowWake) {
    let context = context(wake);
    context.validate().unwrap();
    let validate = super::super::workflow::validate_activation_context_reply;
    validate(&owner(), context.clone()).unwrap();
    for tenant in [false, true] {
        let mut foreign = owner();
        if tenant {
            foreign.scope.tenant_id = "foreign".into();
        } else {
            foreign.scope.namespace = "foreign".into();
        }
        assert!(matches!(
            validate(&foreign, context.clone()),
            Err(ContractError::Conflict)
        ));
    }
}
#[test]
fn resolution_context_scope_must_match_current_lease() {
    assert_context_scope(WorkflowWake::EffectResolution(Box::new(
        EffectResolutionWake {
            key: "park".into(),
            occurrence: claim().occurrence(),
            parking_activation_id: "parking".into(),
            parking_revision: 0,
            resumed_activation_id: "activation".into(),
            resumed_revision: 1,
            resolution_id: "resolution".into(),
            accepted_at: 1,
            accepted_revision: 2,
            basis_revision: 1,
            selected_at: 2,
            selected_effect_revision: 2,
            outcome: EffectResolutionOutcome::ClosedUnknown,
        },
    )));
}
#[test]
fn provider_context_scope_must_match_current_lease() {
    assert_context_scope(WorkflowWake::ProviderOperation(Box::new(ProviderWake {
        key: "park".into(),
        occurrence: claim().occurrence(),
        parking_activation_id: "parking".into(),
        parking_revision: 0,
        resumed_activation_id: "activation".into(),
        resumed_revision: 1,
        selected_at: 2,
        selected_effect_revision: 2,
        selection: ProviderSelection::NeedsReview {
            reason: ProviderRecoveryStopReason::LookupUnavailable,
            checks_used: 0,
            deadline_at: 60000,
        },
    })));
}

impl AdmissionOperations for Store {
    fn put_resource<'a>(
        &'a self,
        _p: &'a VerifiedPrincipal,
        _c: &'a ResourcePutCommand,
    ) -> ContractFuture<'a, ResourcePutReceipt> {
        self.reached()
    }
    unused_methods! {
        fn resource(_p: &'a VerifiedPrincipal, _q: &'a ResourceQuery) -> ResourceSnapshot;
        fn list_resources(_p: &'a VerifiedPrincipal, _q: &'a ResourceListQuery) -> ResourcePage;
        fn start_admission(_p: &'a VerifiedPrincipal, _c: &'a AdmissionStartCommand) -> AdmissionStartReceipt;
        fn finish_admission(_p: &'a VerifiedPrincipal, _c: &'a AdmissionFinishCommand) -> AdmissionFinishReceipt;
        fn inspect_admission(_p: &'a VerifiedPrincipal, _q: &'a AdmissionQuery) -> AdmissionSnapshot;
        fn release_admission(_p: &'a VerifiedPrincipal, _c: &'a AdmissionReleaseCommand) -> AdmissionReleaseReceipt;
    }
}
impl AdmissionStore for Store {
    unused_methods! {
        fn advance_admissions(_q: &'a AdmissionMaintenanceQuery) -> AdmissionMaintenanceProgress;
        fn apply_admission_work(_w: &'a WorkflowWork, _r: &'a [ResolvedWorkflowChild]) -> WorkflowProgress;
    }
}
impl ProviderOperations for Store {
    unused_methods! {
        fn claim_provider(_p: &'a VerifiedPrincipal, _c: &'a ProviderClaimCommand) -> ProviderClaimReceipt;
        fn report_dispatch(_p: &'a VerifiedPrincipal, _c: &'a ProviderDispatchReportCommand) -> ProviderReportReply;
        fn report_probe(_p: &'a VerifiedPrincipal, _c: &'a ProviderProbeReportCommand) -> ProviderReportReply;
        fn observe_provider(_p: &'a VerifiedPrincipal, _c: &'a ProviderObservationCommand) -> ProviderReportReply;
        fn probe_context(_p: &'a VerifiedPrincipal, _c: &'a ProviderProbeContextCommand) -> ProviderProbeContext;
    }
}
impl ProviderOperationStore for Store {
    unused_methods! {
        fn advance_provider_operations(_q: &'a ProviderMaintenanceQuery) -> ProviderMaintenanceProgress;
        fn apply_provider_work(_w: &'a WorkflowWork, _r: &'a [ResolvedWorkflowChild]) -> WorkflowProgress;
    }
}
impl ProviderAdmissionOperations for Store {
    fn start_provider_admission<'a>(
        &'a self,
        _p: &'a VerifiedPrincipal,
        _c: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt> {
        self.reached()
    }
    fn report_provider_admission<'a>(
        &'a self,
        _p: &'a VerifiedPrincipal,
        _c: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt> {
        self.reached()
    }
}
impl ProviderAdmissionStore for Store {}
#[tokio::test]
async fn protected_provider_admission_requires_complete_atomic_store_before_mutation() {
    let c = ProviderAdmissionStartCommand {
        admission: AdmissionStartCommand {
            owner: owner(),
            occurrence: AdmissionOccurrence {
                scope: owner().scope,
                workflow_id: "workflow".into(),
                origin_activation_id: "approved-activation".into(),
                key: "provider-call".into(),
            },
            offer_generation: 1,
            start_id: "start".into(),
            finish_id: "report".into(),
        },
    };
    for missing in [
        Missing::Reader,
        Missing::Audit,
        Missing::Evidence,
        Missing::Admission,
        Missing::Provider,
        Missing::Composition,
    ] {
        let (service, store) = service(Some(missing));
        assert!(matches!(
            service.start_provider_admission(&principal(true), &c).await,
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    }
    let (service, store) = service(None);
    assert_eq!(
        service
            .start_provider_admission(&principal(true), &c)
            .await
            .unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn protected_provider_wait_requires_composed_capability_before_work_application() {
    let mut work = native_work(true);
    if let Some(TaskOutcome::Succeeded { output, .. }) = &mut work.outcome {
        output["wait"] = json!({"kind":"resource_admission","key":"provider-call","request":{"kind":"provider","approval_key":"approval"},"timeout_ms":60000});
    }
    for missing in [
        Missing::Reader,
        Missing::Audit,
        Missing::Evidence,
        Missing::Admission,
        Missing::Provider,
        Missing::Composition,
    ] {
        let (service, store) = service(Some(missing));
        assert!(matches!(
            service.resolve_workflow_children(&work, &mut vec![]).await,
            Err(ContractError::InvalidInput(_))
        ));
        assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    }
    let (service, _) = service(None);
    assert!(
        !service
            .resolve_workflow_children(&work, &mut vec![])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn legacy_admission_store_cannot_accept_or_explain_weighted_offers() {
    let (svc, store) = service(None);
    let actor = principal(false);
    let operator = VerifiedPrincipal::new(
        actor.identity().clone(),
        actor.audience().clone(),
        "credential".into(),
        PrincipalClass::Operator,
        1000,
        vec![
            AccessCapability::MetadataRead,
            AccessCapability::ResourceControl,
        ],
    )
    .unwrap();
    assert!(!svc.supports_resource_fair_sharing());
    let scope = owner().scope;
    let mut cmd: ResourcePutCommand = serde_json::from_value(serde_json::json!({
        "scope":scope, "name":"shared", "request_id":"update", "expected_revision":null,
        "reason":"test", "policy":{"enabled":true,"max_occupancy":1,"rate":null,
        "offer_timeout_ms":1000,"start_timeout_ms":1000,"fair_sharing":{
            "groups":[{"name":"batch","weight":1,"queues":[]}],"default_group":"batch"}}
    }))
    .unwrap();
    let query = ResourceDecisionQuery {
        scope: scope.clone(),
        name: "shared".into(),
        before: None,
        limit: 10,
    };
    assert!(matches!(
        svc.put_resource(&operator, &cmd).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        svc.list_resource_decisions(&operator, &query).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(
        svc.list_resource_decisions(&actor, &query)
            .await
            .unwrap_err(),
        ContractError::Forbidden
    );
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    cmd.policy.fair_sharing = None;
    // FIFO still reaches the existing adapter instead of requiring a new capability.
    assert_eq!(
        svc.put_resource(&operator, &cmd).await.unwrap_err(),
        ContractError::NotFound
    );
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
}
