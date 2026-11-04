use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAdmissionState {
    Waiting,
    Offered,
    Preparing,
    PossiblyDispatched,
    UnknownOccupied,
    Finished,
    ClosedBeforeStart,
    AdministrativelyReleased,
}
impl TaskAdmissionState {
    pub const fn is_occupied(self) -> bool {
        matches!(
            self,
            Self::Offered | Self::Preparing | Self::PossiblyDispatched | Self::UnknownOccupied
        )
    }
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Finished | Self::ClosedBeforeStart | Self::AdministrativelyReleased
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionSnapshot {
    pub occurrence: TaskAdmissionOccurrence,
    pub resource: String,
    pub program: ProgramDescriptor,
    pub state: TaskAdmissionState,
    pub revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub offer: Option<TaskAdmissionOffer>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub owner: Option<LeaseOwner>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub source: Option<TaskAdmissionSource>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub close: Option<TaskAdmissionCloseReceipt>,
}
impl TaskAdmissionSnapshot {
    pub fn binding(&self) -> TaskAdmissionBinding {
        TaskAdmissionBinding {
            occurrence: self.occurrence.clone(),
            resource: self.resource.clone(),
            program: self.program.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.binding().validate()?;
        crate::admission::positive(self.revision)?;
        time(self.created_at)?;
        time(self.updated_at)?;
        if self.updated_at < self.created_at {
            return Err(invalid("task admission timestamps reversed"));
        }
        if let Some(offer) = &self.offer {
            offer.validate()?;
        }
        if let Some(owner) = &self.owner {
            owner.validate()?;
            if !self.occurrence.matches_owner(owner) {
                return Err(invalid("task admission owner mismatch"));
            }
        }
        if let Some(source) = &self.source
            && (!source.matches_binding(&self.binding())?
                || self.owner.as_ref() != Some(&source.owner)
                || self.offer.as_ref().is_none_or(|offer| {
                    offer.generation != source.offer_generation
                        || source.accepted_at >= offer.expires_at
                        || source.start_deadline_at > offer.expires_at
                })
                || source.accepted_at < self.created_at
                || source.accepted_at > self.updated_at)
        {
            return Err(invalid("task admission source mismatch"));
        }
        if let Some(close) = &self.close {
            close.validate()?;
            if close.already_accepted
                || self.owner.as_ref() != Some(&close.owner)
                || self.source != close.source
                || self
                    .offer
                    .as_ref()
                    .is_none_or(|offer| offer.generation != close.offer_generation)
                || close.accepted_at < self.created_at
                || close.accepted_at > self.updated_at
            {
                return Err(invalid("task admission close mismatch"));
            }
        }
        let valid = match self.state {
            TaskAdmissionState::Waiting => {
                self.offer.is_none()
                    && self.owner.is_none()
                    && self.source.is_none()
                    && self.close.is_none()
            }
            TaskAdmissionState::Offered => {
                self.offer.is_some()
                    && self.owner.is_none()
                    && self.source.is_none()
                    && self.close.is_none()
            }
            TaskAdmissionState::Preparing => {
                self.offer.is_some()
                    && self.owner.is_some()
                    && self.source.is_none()
                    && self.close.is_none()
            }
            TaskAdmissionState::PossiblyDispatched | TaskAdmissionState::UnknownOccupied => {
                self.source.is_some() && self.close.is_none()
            }
            TaskAdmissionState::Finished => {
                self.source.is_some()
                    && self.close.as_ref().is_some_and(|close| {
                        matches!(
                            close.disposition,
                            TaskAdmissionCloseDisposition::Released
                                | TaskAdmissionCloseDisposition::ChargedNotInvoked
                        )
                    })
            }
            TaskAdmissionState::AdministrativelyReleased => {
                self.source.is_some()
                    && self.close.as_ref().is_none_or(|close| {
                        close.disposition == TaskAdmissionCloseDisposition::AlreadyReleased
                    })
            }
            TaskAdmissionState::ClosedBeforeStart => {
                self.source.is_none()
                    && self.owner.is_some() == self.offer.is_some()
                    && self.close.as_ref().is_none_or(|close| {
                        matches!(
                            close.disposition,
                            TaskAdmissionCloseDisposition::Closed
                                | TaskAdmissionCloseDisposition::Deferred
                        )
                    })
            }
        };
        if !valid {
            return Err(invalid("inconsistent task admission state"));
        }
        bound(self)
    }
    pub fn matches(&self, query: &TaskAdmissionQuery) -> bool {
        self.occurrence == query.occurrence
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionQuery {
    pub occurrence: TaskAdmissionOccurrence,
}
impl TaskAdmissionQuery {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionReleaseCommand {
    pub occurrence: TaskAdmissionOccurrence,
    pub source_id: String,
    pub expected_revision: u64,
    pub request_id: String,
    pub reason: String,
}
impl TaskAdmissionReleaseCommand {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        validate_text(&self.source_id, 128)?;
        crate::admission::positive(self.expected_revision)?;
        crate::admission::positive(
            self.expected_revision
                .checked_add(1)
                .ok_or_else(|| invalid("task admission revision overflow"))?,
        )?;
        validate_text(&self.request_id, 128)?;
        validate_text(&self.reason, 1024)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionReleaseReceipt {
    pub snapshot: TaskAdmissionSnapshot,
    pub already_accepted: bool,
}
impl TaskAdmissionReleaseReceipt {
    pub fn validate(&self) -> Result<()> {
        self.snapshot.validate()?;
        if self.snapshot.state != TaskAdmissionState::AdministrativelyReleased {
            return Err(invalid("task admission release state mismatch"));
        }
        Ok(())
    }
    pub fn matches(&self, command: &TaskAdmissionReleaseCommand) -> bool {
        self.snapshot.occurrence == command.occurrence
            && self
                .snapshot
                .source
                .as_ref()
                .is_some_and(|source| source.source_id == command.source_id)
            && Some(self.snapshot.revision) == command.expected_revision.checked_add(1)
    }
}
