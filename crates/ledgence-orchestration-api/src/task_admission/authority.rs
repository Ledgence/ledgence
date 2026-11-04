use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionStartCommand {
    pub owner: LeaseOwner,
    pub offer_generation: u64,
    pub start_id: String,
}
impl TaskAdmissionStartCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        crate::admission::positive(self.offer_generation)?;
        validate_text(&self.start_id, 128)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionSource {
    pub owner: LeaseOwner,
    pub offer_generation: u64,
    pub resource: String,
    pub binding_digest: Digest,
    pub start_id: String,
    pub source_id: String,
    pub worker: AdmissionWorkerIdentity,
    pub accepted_at: Timestamp,
    pub start_deadline_at: Timestamp,
}
impl TaskAdmissionSource {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        crate::admission::positive(self.offer_generation)?;
        crate::admission::name(&self.resource)?;
        self.binding_digest.validate()?;
        validate_text(&self.start_id, 128)?;
        validate_text(&self.source_id, 128)?;
        self.worker.validate()?;
        time(self.accepted_at)?;
        time(self.start_deadline_at)?;
        if self.owner.scope != self.worker.audience.scope
            || self.start_deadline_at <= self.accepted_at
            || self.start_deadline_at - self.accepted_at > 30_000
        {
            return Err(invalid("inconsistent task admission source"));
        }
        Ok(())
    }
    pub fn matches(&self, command: &TaskAdmissionStartCommand) -> bool {
        self.owner == command.owner
            && self.offer_generation == command.offer_generation
            && self.start_id == command.start_id
    }
    pub fn matches_binding(&self, binding: &TaskAdmissionBinding) -> Result<bool> {
        self.validate()?;
        Ok(binding.occurrence.matches_owner(&self.owner)
            && binding.resource == self.resource
            && binding.digest()? == self.binding_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskAdmissionStartReceipt {
    Granted {
        source: Box<TaskAdmissionSource>,
        authority: Authority,
    },
    Existing {
        snapshot: Box<TaskAdmissionSnapshot>,
    },
}
impl TaskAdmissionStartReceipt {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Granted { source, authority } => {
                source.validate()?;
                let sampled_at = authority
                    .expires_at
                    .checked_sub(authority.remaining_ms)
                    .ok_or_else(|| invalid("invalid task admission lease sample"))?;
                if authority.owner != source.owner
                    || !authority.dispatch_allowed
                    || authority.cancel_requested
                    || authority.remaining_ms == 0
                    || authority.execution_remaining_ms == 0
                    || authority.expires_at < source.start_deadline_at
                    || sampled_at < source.accepted_at
                    || sampled_at >= source.start_deadline_at
                    || source.start_deadline_at - sampled_at > authority.execution_remaining_ms
                {
                    return Err(invalid("invalid task admission invocation authority"));
                }
                time(authority.expires_at)?;
                bound(self)
            }
            Self::Existing { snapshot } => snapshot.validate(),
        }
    }
    pub fn matches(&self, command: &TaskAdmissionStartCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(match self {
            Self::Granted { source, .. } => source.matches(command),
            Self::Existing { snapshot } => {
                snapshot.occurrence.matches_owner(&command.owner)
                    && snapshot.owner.as_ref() == Some(&command.owner)
                    && snapshot
                        .offer
                        .as_ref()
                        .is_some_and(|offer| offer.generation == command.offer_generation)
                    && snapshot
                        .source
                        .as_ref()
                        .is_none_or(|source| source.matches(command))
                    && snapshot
                        .close
                        .as_ref()
                        .is_none_or(|close| close.start_id == command.start_id)
            }
        })
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &TaskAdmissionStartCommand,
    ) -> Result<()> {
        if !self.matches(command)?
            || principal.class() != PrincipalClass::Worker
            || principal.audience().scope != command.owner.scope
        {
            return Err(ContractError::Conflict);
        }
        let source = match self {
            Self::Granted { source, .. } => Some(source.as_ref()),
            Self::Existing { snapshot } => snapshot.source.as_ref(),
        };
        if source.is_some_and(|source| !source.worker.matches_principal(principal)) {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAdmissionCloseOutcome {
    /// Worker has confirmed that this preparation never invoked the handler.
    NotInvoked,
    /// Invocation cleanup is complete; it says nothing about external effects.
    Quiescent,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionCloseCommand {
    pub owner: LeaseOwner,
    pub offer_generation: u64,
    pub start_id: String,
    pub operation_id: String,
    pub outcome: TaskAdmissionCloseOutcome,
}
impl TaskAdmissionCloseCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        crate::admission::positive(self.offer_generation)?;
        validate_text(&self.start_id, 128)?;
        validate_text(&self.operation_id, 128)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAdmissionCloseDisposition {
    Deferred,
    ChargedNotInvoked,
    Released,
    AlreadyReleased,
    Closed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionCloseReceipt {
    pub owner: LeaseOwner,
    pub offer_generation: u64,
    pub start_id: String,
    pub operation_id: String,
    pub outcome: TaskAdmissionCloseOutcome,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub source: Option<TaskAdmissionSource>,
    pub disposition: TaskAdmissionCloseDisposition,
    pub accepted_at: Timestamp,
    pub already_accepted: bool,
}
impl TaskAdmissionCloseReceipt {
    pub fn validate(&self) -> Result<()> {
        let command = TaskAdmissionCloseCommand {
            owner: self.owner.clone(),
            offer_generation: self.offer_generation,
            start_id: self.start_id.clone(),
            operation_id: self.operation_id.clone(),
            outcome: self.outcome,
        };
        command.validate()?;
        time(self.accepted_at)?;
        if let Some(source) = &self.source {
            source.validate()?;
            if source.owner != self.owner
                || source.offer_generation != self.offer_generation
                || source.start_id != self.start_id
                || self.accepted_at < source.accepted_at
            {
                return Err(invalid("task admission close source mismatch"));
            }
        }
        let valid = match self.disposition {
            TaskAdmissionCloseDisposition::Deferred => {
                self.source.is_none() && self.outcome == TaskAdmissionCloseOutcome::NotInvoked
            }
            TaskAdmissionCloseDisposition::ChargedNotInvoked => {
                self.source.is_some() && self.outcome == TaskAdmissionCloseOutcome::NotInvoked
            }
            TaskAdmissionCloseDisposition::Released => {
                self.source.is_some() && self.outcome == TaskAdmissionCloseOutcome::Quiescent
            }
            TaskAdmissionCloseDisposition::AlreadyReleased => self.source.is_some(),
            TaskAdmissionCloseDisposition::Closed => self.source.is_none(),
        };
        if !valid {
            return Err(invalid("invalid task admission close disposition"));
        }
        bound(self)
    }
    pub fn matches(&self, command: &TaskAdmissionCloseCommand) -> bool {
        self.owner == command.owner
            && self.offer_generation == command.offer_generation
            && self.start_id == command.start_id
            && self.operation_id == command.operation_id
            && self.outcome == command.outcome
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &TaskAdmissionCloseCommand,
    ) -> Result<()> {
        self.validate()?;
        command.validate()?;
        if !self.matches(command)
            || principal.class() != PrincipalClass::Worker
            || principal.audience().scope != self.owner.scope
            || self
                .source
                .as_ref()
                .is_some_and(|source| !source.worker.matches_principal(principal))
        {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
