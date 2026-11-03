use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionOffer {
    pub occurrence: AdmissionOccurrence,
    pub generation: u64,
    pub policy_revision: u64,
    pub activation_id: String,
    pub expires_at: Timestamp,
    pub binding_digest: Digest,
}
impl AdmissionOffer {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        positive(self.generation)?;
        positive(self.policy_revision)?;
        validate_text(&self.activation_id, 128)?;
        time(self.expires_at)?;
        self.binding_digest.validate()?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionWorkerIdentity {
    pub audience: AccessAudience,
    pub issuer: String,
    pub subject: String,
    pub credential_id: String,
}
impl AdmissionWorkerIdentity {
    pub fn validate(&self) -> Result<()> {
        self.audience.validate()?;
        validate_text(&self.issuer, 256)?;
        validate_text(&self.subject, 128)?;
        validate_text(&self.credential_id, 128)
    }
    pub fn from_principal(p: &VerifiedPrincipal) -> Result<Self> {
        p.require(AccessCapability::WorkerExecute)?;
        if p.class() != PrincipalClass::Worker {
            return Err(ContractError::Forbidden);
        }
        Ok(Self {
            audience: p.audience().clone(),
            issuer: p.identity().issuer.clone(),
            subject: p.identity().subject.clone(),
            credential_id: p.credential_id().into(),
        })
    }
    /// Stable identity permits credential rotation, but never another worker actor.
    pub fn matches_principal(&self, p: &VerifiedPrincipal) -> bool {
        p.class() == PrincipalClass::Worker
            && &self.audience == p.audience()
            && self.issuer == p.identity().issuer
            && self.subject == p.identity().subject
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionSource {
    pub occurrence: AdmissionOccurrence,
    pub source_id: String,
    pub start_id: String,
    pub finish_id: String,
    pub generation: u64,
    pub binding_digest: Digest,
    pub owner: LeaseOwner,
    pub worker: AdmissionWorkerIdentity,
    pub accepted_at: Timestamp,
    pub start_deadline_at: Timestamp,
}
impl AdmissionSource {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        self.owner.validate()?;
        self.worker.validate()?;
        self.binding_digest.validate()?;
        for id in [&self.source_id, &self.start_id, &self.finish_id] {
            validate_text(id, 128)?;
        }
        positive(self.generation)?;
        time(self.accepted_at)?;
        time(self.start_deadline_at)?;
        if self.occurrence.scope != self.owner.scope
            || self.owner.scope != self.worker.audience.scope
            || self.start_deadline_at <= self.accepted_at
            || self.start_deadline_at - self.accepted_at > 30_000
        {
            return Err(invalid("inconsistent admission source"));
        }
        Ok(())
    }
    pub fn matches(&self, c: &AdmissionStartCommand) -> bool {
        self.occurrence == c.occurrence
            && self.owner == c.owner
            && self.generation == c.offer_generation
            && self.start_id == c.start_id
            && self.finish_id == c.finish_id
    }
    pub fn matches_offer(&self, o: &AdmissionOffer) -> bool {
        self.occurrence == o.occurrence
            && self.generation == o.generation
            && self.owner.task_id == o.activation_id
            && self.binding_digest == o.binding_digest
            && self.accepted_at < o.expires_at
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionState {
    Waiting,
    Offered,
    AwaitingRewait,
    PossiblyDispatched,
    UnknownOccupied,
    Finished,
    ClosedBeforeStart,
    AdministrativelyReleased,
}
impl AdmissionState {
    pub const fn is_occupied(self) -> bool {
        matches!(
            self,
            Self::Offered | Self::PossiblyDispatched | Self::UnknownOccupied
        )
    }
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Finished | Self::ClosedBeforeStart | Self::AdministrativelyReleased
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionFinishReceipt {
    pub source: AdmissionSource,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub output: Value,
    pub accepted_at: Timestamp,
    pub already_accepted: bool,
}
impl AdmissionFinishReceipt {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        validate_wire_value(&self.output)?;
        bound(&self.output, ADMISSION_OUTPUT_MAX_BYTES)?;
        time(self.accepted_at)?;
        if self.accepted_at < self.source.accepted_at {
            return Err(invalid("finish predates admission source"));
        }
        Ok(())
    }
    pub fn matches(&self, c: &AdmissionFinishCommand) -> bool {
        self.source == c.source && canonical_eq(&self.output, &c.output)
    }
    pub fn validate_for(&self, p: &VerifiedPrincipal, c: &AdmissionFinishCommand) -> Result<()> {
        self.validate()?;
        c.validate()?;
        if !self.matches(c) || !self.source.worker.matches_principal(p) {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionSnapshot {
    pub occurrence: AdmissionOccurrence,
    pub binding: AdmissionBinding,
    pub state: AdmissionState,
    pub revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub deadline_at: Timestamp,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub offer: Option<AdmissionOffer>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub source: Option<AdmissionSource>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub finish: Option<AdmissionFinishReceipt>,
}
impl AdmissionSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        self.binding.validate()?;
        positive(self.revision)?;
        for t in [self.created_at, self.updated_at, self.deadline_at] {
            time(t)?;
        }
        if self.updated_at < self.created_at || self.deadline_at < self.created_at {
            return Err(invalid("inconsistent admission timestamps"));
        }
        let digest = self.binding.digest()?;
        if let Some(o) = &self.offer {
            o.validate()?;
            if o.occurrence != self.occurrence
                || o.binding_digest != digest
                || o.expires_at > self.deadline_at
            {
                return Err(invalid("foreign admission offer"));
            }
        }
        if let Some(s) = &self.source {
            s.validate()?;
            if s.occurrence != self.occurrence
                || s.binding_digest != digest
                || s.accepted_at > self.updated_at
                || s.accepted_at < self.created_at
                || s.accepted_at >= self.deadline_at
            {
                return Err(invalid("foreign admission source"));
            }
        }
        if let Some(f) = &self.finish {
            f.validate()?;
            if self.source.as_ref() != Some(&f.source) || f.accepted_at > self.updated_at {
                return Err(invalid("foreign admission finish"));
            }
        }
        let consistent = match self.state {
            AdmissionState::Waiting
            | AdmissionState::AwaitingRewait
            | AdmissionState::ClosedBeforeStart => {
                self.offer.is_none() && self.source.is_none() && self.finish.is_none()
            }
            AdmissionState::Offered => {
                self.offer.is_some() && self.source.is_none() && self.finish.is_none()
            }
            AdmissionState::PossiblyDispatched | AdmissionState::UnknownOccupied => {
                self.offer.is_none() && self.source.is_some() && self.finish.is_none()
            }
            AdmissionState::Finished => {
                self.offer.is_none() && self.source.is_some() && self.finish.is_some()
            }
            AdmissionState::AdministrativelyReleased => {
                self.offer.is_none() && self.source.is_some()
            }
        };
        if !consistent {
            return Err(invalid("inconsistent admission state"));
        }
        bound(self, ADMISSION_RESPONSE_MAX_BYTES)
    }
    pub fn matches(&self, q: &AdmissionQuery) -> bool {
        self.occurrence == q.occurrence
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdmissionStartReceipt {
    Granted { source: Box<AdmissionSource> },
    Existing { snapshot: Box<AdmissionSnapshot> },
}
impl AdmissionStartReceipt {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Granted { source } => source.validate(),
            Self::Existing { snapshot } => snapshot.validate(),
        }
    }
    pub fn matches(&self, c: &AdmissionStartCommand) -> bool {
        match self {
            Self::Granted { source } => source.matches(c),
            Self::Existing { snapshot } => snapshot.occurrence == c.occurrence,
        }
    }
    pub fn validate_for(&self, p: &VerifiedPrincipal, c: &AdmissionStartCommand) -> Result<()> {
        self.validate()?;
        c.validate()?;
        if !self.matches(c)
            || p.audience().scope != c.owner.scope
            || p.class() != PrincipalClass::Worker
        {
            return Err(ContractError::Conflict);
        }
        if let Self::Granted { source } = self
            && !source.worker.matches_principal(p)
        {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionClosedReason {
    Cancelled,
    Deadline,
    AdministrativelyReleased,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdmissionSelection {
    Offered {
        offer: AdmissionOffer,
    },
    Finished {
        receipt: Box<AdmissionFinishReceipt>,
    },
    Closed {
        reason: AdmissionClosedReason,
    },
    NeedsReview {
        source_id: String,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionWake {
    pub key: String,
    pub occurrence: AdmissionOccurrence,
    pub waiting_activation_id: String,
    pub resumed_activation_id: String,
    pub resumed_revision: u64,
    pub binding: AdmissionBinding,
    pub selected_at: Timestamp,
    pub selection: AdmissionSelection,
}
impl AdmissionWake {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        self.binding.validate()?;
        for id in [
            &self.key,
            &self.waiting_activation_id,
            &self.resumed_activation_id,
        ] {
            validate_text(id, 128)?;
        }
        positive(self.resumed_revision)?;
        time(self.selected_at)?;
        if self.waiting_activation_id == self.resumed_activation_id {
            return Err(invalid("admission wake reuses parking activation"));
        }
        let digest = self.binding.digest()?;
        match &self.selection {
            AdmissionSelection::Offered { offer } => {
                offer.validate()?;
                if offer.occurrence != self.occurrence
                    || offer.binding_digest != digest
                    || offer.activation_id != self.resumed_activation_id
                    || offer.expires_at <= self.selected_at
                {
                    return Err(invalid("admission wake offer mismatch"));
                }
            }
            AdmissionSelection::Finished { receipt } => {
                receipt.validate()?;
                if receipt.source.occurrence != self.occurrence
                    || receipt.source.binding_digest != digest
                    || receipt.accepted_at > self.selected_at
                {
                    return Err(invalid("admission wake finish mismatch"));
                }
            }
            AdmissionSelection::NeedsReview { source_id } => validate_text(source_id, 128)?,
            AdmissionSelection::Closed { .. } => {}
        }
        bound(self, ADMISSION_RESPONSE_MAX_BYTES)
    }
    pub fn validate_result(&self, result: &AdmissionFinishReceipt) -> Result<()> {
        self.validate()?;
        result.validate()?;
        if result.source.occurrence != self.occurrence
            || result.source.binding_digest != self.binding.digest()?
        {
            return Err(invalid("preloaded admission result binding mismatch"));
        }
        if let AdmissionSelection::Offered { offer } = &self.selection
            && !result.source.matches_offer(offer)
        {
            return Err(invalid("preloaded admission result offer mismatch"));
        }
        if let AdmissionSelection::NeedsReview { source_id } = &self.selection
            && source_id != &result.source.source_id
        {
            return Err(invalid("preloaded admission result source mismatch"));
        }
        if matches!(self.selection, AdmissionSelection::Closed { .. }) {
            return Err(invalid("closed admission wake has a result"));
        }
        if let AdmissionSelection::Finished { receipt } = &self.selection
            && (receipt.source != result.source
                || !canonical_eq(&receipt.output, &result.output)
                || receipt.accepted_at != result.accepted_at)
        {
            return Err(invalid("preloaded admission result changed"));
        }
        Ok(())
    }
}
