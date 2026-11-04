//! Atomic composition of approved provider dispatch and durable capacity admission.
//! A ticket is an observation handle, never executable dispatch authority.
use crate::provider_binding::{provider_bound, provider_invalid};
use crate::*;
use ledgence_worker_api::ProgramDescriptor;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionSubject {
    pub ticket: ProviderOperationTicket,
    pub binding: ProviderBindingRequest,
    pub action: ApprovalAction,
}
impl ProviderAdmissionSubject {
    pub fn validate_for(&self, program: &ProgramDescriptor, resource: &str) -> Result<()> {
        self.ticket.validate()?;
        let binding = self.binding.resolve(program.clone())?;
        if binding.admission.as_ref().map(|v| v.resource.as_str()) != Some(resource)
            || binding.digest(&self.action)? != self.ticket.binding_digest
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionStartCommand {
    pub admission: AdmissionStartCommand,
}
impl ProviderAdmissionStartCommand {
    pub fn validate(&self) -> Result<()> {
        self.admission.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionSource {
    pub admission: AdmissionSource,
    pub provider: ProviderDispatchSource,
}
impl ProviderAdmissionSource {
    pub fn validate(&self) -> Result<()> {
        self.admission.validate()?;
        self.provider.validate()?;
        let a = &self.admission;
        let p = &self.provider;
        if a.worker.audience != p.worker.audience
            || a.worker.issuer != p.worker.issuer
            || a.worker.subject != p.worker.subject
            || a.worker.credential_id != p.worker.credential_id
            || a.accepted_at != p.accepted_at
            || a.start_id != p.claim_id
            || a.finish_id != p.report_id
            || a.occurrence.scope != p.ticket.occurrence.scope
            || a.occurrence.workflow_id != p.ticket.occurrence.workflow_id
            || a.occurrence.origin_activation_id != p.ticket.occurrence.activation_id
        {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
    pub fn matches_binding(&self, binding: &ProviderAdmissionBinding) -> Result<bool> {
        self.validate()?;
        let admission = AdmissionBinding::Provider(Box::new(binding.clone()));
        admission.validate()?;
        Ok(self.provider.ticket == binding.provider.ticket
            && self.admission.binding_digest == admission.digest()?)
    }
    pub fn matches(&self, command: &ProviderAdmissionStartCommand) -> bool {
        self.admission.matches(&command.admission)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderAdmissionStartReceipt {
    Granted {
        source: Box<ProviderAdmissionSource>,
    },
    Existing {
        audience: AccessAudience,
        snapshot: Box<AdmissionSnapshot>,
    },
}
impl ProviderAdmissionStartReceipt {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Granted { source } => source.validate(),
            Self::Existing { audience, snapshot } => {
                audience.validate()?;
                snapshot.validate()?;
                if snapshot.binding.provider().is_none()
                    || audience.scope != snapshot.occurrence.scope
                {
                    return Err(provider_invalid());
                }
                Ok(())
            }
        }
    }
    pub fn matches(&self, command: &ProviderAdmissionStartCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(match self {
            Self::Granted { source } => source.matches(command),
            Self::Existing { snapshot, .. } => snapshot.occurrence == command.admission.occurrence,
        })
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &ProviderAdmissionStartCommand,
    ) -> Result<()> {
        if !self.matches(command)?
            || principal.class() != PrincipalClass::Worker
            || principal.audience().scope != command.admission.owner.scope
        {
            return Err(ContractError::Conflict);
        }
        let matches = match self {
            Self::Granted { source } => source.admission.worker.matches_principal(principal),
            Self::Existing { audience, .. } => audience == principal.audience(),
        };
        if !matches {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCallCompletion {
    Completed,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDispatchOutcome {
    pub call_completion: ProviderCallCompletion,
    pub report: ProviderReport,
}
impl ProviderDispatchOutcome {
    pub fn validate(&self) -> Result<()> {
        self.report.validate()?;
        provider_bound(self, PROVIDER_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionReportCommand {
    pub source: ProviderAdmissionSource,
    pub outcome: ProviderDispatchOutcome,
}
impl ProviderAdmissionReportCommand {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.outcome.validate()?;
        provider_bound(self, PROVIDER_COMMAND_MAX_BYTES)
    }
}
impl ProviderReportedCommand for ProviderAdmissionReportCommand {
    fn ticket(&self) -> &ProviderOperationTicket {
        &self.source.provider.ticket
    }
    fn source_id(&self) -> &str {
        &self.source.provider.source_id
    }
    fn report_id(&self) -> &str {
        &self.source.provider.report_id
    }
    fn report(&self) -> &ProviderReport {
        &self.outcome.report
    }
    fn ingress(&self) -> ProviderReportIngress {
        ProviderReportIngress::EvidenceOnly
    }
    fn validate(&self) -> Result<()> {
        ProviderAdmissionReportCommand::validate(self)
    }
    fn source_matches(&self, source: &ProviderSourceReference) -> bool {
        matches!(source, ProviderSourceReference::Dispatch { claim_id } if claim_id == &self.source.provider.claim_id)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionReportReceipt {
    pub source: ProviderAdmissionSource,
    pub call_completion: ProviderCallCompletion,
    pub receipt: ProviderReportReceipt,
}
impl ProviderAdmissionReportReceipt {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.receipt.validate()?;
        let p = &self.source.provider;
        let r = &self.receipt;
        if r.ticket != p.ticket
            || r.source_id != p.source_id
            || r.report_id != p.report_id
            || !matches!(&r.source, ProviderSourceReference::Dispatch { claim_id } if claim_id == &p.claim_id)
            || r.accepted_via != ProviderReportIngress::EvidenceOnly
            || !r.worker.same_worker(&p.worker)
            || r.accepted_at < p.accepted_at
        {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
    pub fn matches(&self, command: &ProviderAdmissionReportCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.source == command.source
            && self.call_completion == command.outcome.call_completion
            && self.receipt.matches(command)?)
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &ProviderAdmissionReportCommand,
    ) -> Result<()> {
        if !self.matches(command)? || !self.source.admission.worker.matches_principal(principal) {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
/// Retained dispatch-phase evidence; does not assert provider business success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAdmissionRecord {
    pub ticket: ProviderOperationTicket,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub source: Option<ProviderAdmissionSource>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub report: Option<ProviderAdmissionReportReceipt>,
}
impl ProviderAdmissionRecord {
    pub fn validate(&self) -> Result<()> {
        self.ticket.validate()?;
        if let Some(source) = &self.source {
            source.validate()?;
            if source.provider.ticket != self.ticket {
                return Err(provider_invalid());
            }
        }
        if let Some(report) = &self.report {
            report.validate()?;
            if self.source.as_ref() != Some(&report.source) {
                return Err(provider_invalid());
            }
        }
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
    pub fn validate_for(
        &self,
        binding: &ProviderAdmissionBinding,
        occurrence: &AdmissionOccurrence,
    ) -> Result<()> {
        self.validate()?;
        occurrence.validate()?;
        binding
            .provider
            .validate_for(&binding.program, &binding.resource)?;
        let expected = &binding.provider.ticket;
        if self.ticket.binding_digest != expected.binding_digest
            || self.ticket.occurrence.scope != occurrence.scope
            || expected.occurrence.scope != occurrence.scope
            || expected.occurrence.workflow_id != occurrence.workflow_id
            || expected.occurrence.activation_id != occurrence.origin_activation_id
        {
            return Err(provider_invalid());
        }
        if let Some(source) = &self.source
            && (source.admission.occurrence != *occurrence || !source.matches_binding(binding)?)
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
}
/// Implement only when approval, capacity, provider grant and recovery are one commit.
pub trait ProviderAdmissionOperations: Send + Sync {
    fn start_provider_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt>;
    fn report_provider_admission<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt>;
}
/// Full store composition, including admission wait maintenance and durable report settlement.
pub trait ProviderAdmissionStore: ProviderAdmissionOperations {}
pub trait ProviderAdmissionClient: Send + Sync {
    fn start_provider_admission<'a>(
        &'a self,
        command: &'a ProviderAdmissionStartCommand,
    ) -> ContractFuture<'a, ProviderAdmissionStartReceipt>;
    fn report_provider_admission<'a>(
        &'a self,
        command: &'a ProviderAdmissionReportCommand,
    ) -> ContractFuture<'a, ProviderAdmissionReportReceipt>;
}
