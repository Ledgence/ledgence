use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePutCommand {
    pub scope: Scope,
    pub name: String,
    pub request_id: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub expected_revision: Option<u64>,
    pub policy: ResourcePolicy,
    pub reason: String,
}
impl ResourcePutCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        name(&self.name)?;
        validate_text(&self.request_id, 128)?;
        validate_text(&self.reason, 1024)?;
        self.policy.validate()?;
        if let Some(rev) = self.expected_revision {
            positive(rev)?;
            positive(
                rev.checked_add(1)
                    .ok_or_else(|| invalid("resource revision overflow"))?,
            )?;
        }
        bound(self, ADMISSION_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePutReceipt {
    pub resource: ResourceRecord,
    pub already_accepted: bool,
}
impl ResourcePutReceipt {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()
    }
    pub fn matches(&self, c: &ResourcePutCommand) -> bool {
        self.resource.scope == c.scope
            && self.resource.name == c.name
            && self.resource.policy == c.policy
            && Some(self.resource.revision) == c.expected_revision.unwrap_or(0).checked_add(1)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQuery {
    pub scope: Scope,
    pub name: String,
}
impl ResourceQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        name(&self.name)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceListQuery {
    pub scope: Scope,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub after: Option<String>,
    pub limit: u32,
}
impl ResourceListQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if let Some(after) = &self.after {
            name(after)?;
        }
        if !(1..=100).contains(&self.limit) {
            return Err(invalid("resource page size exceeds bound"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePage {
    pub items: Vec<ResourceSnapshot>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next: Option<String>,
}
impl ResourcePage {
    pub fn validate(&self) -> Result<()> {
        if self.items.len() > 100 {
            return Err(invalid("resource page exceeds bound"));
        }
        let mut previous: Option<&str> = None;
        for item in &self.items {
            item.validate()?;
            if previous.is_some_and(|p| p >= item.resource.name.as_str()) {
                return Err(invalid("resource page order"));
            }
            previous = Some(&item.resource.name);
        }
        if let Some(next) = &self.next {
            name(next)?;
            if previous != Some(next.as_str()) {
                return Err(invalid("resource next cursor mismatch"));
            }
        }
        bound(self, ADMISSION_PAGE_MAX_BYTES)
    }
    pub fn validate_for(&self, q: &ResourceListQuery) -> Result<()> {
        self.validate()?;
        q.validate()?;
        if self.items.len() > q.limit as usize
            || self.items.iter().any(|i| {
                i.resource.scope != q.scope
                    || q.after.as_ref().is_some_and(|a| &i.resource.name <= a)
            })
        {
            return Err(invalid("resource page disagrees with query"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionStartCommand {
    pub owner: LeaseOwner,
    pub occurrence: AdmissionOccurrence,
    pub offer_generation: u64,
    pub start_id: String,
    pub finish_id: String,
}
impl AdmissionStartCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        self.occurrence.validate()?;
        positive(self.offer_generation)?;
        validate_text(&self.start_id, 128)?;
        validate_text(&self.finish_id, 128)?;
        if self.owner.scope != self.occurrence.scope {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionFinishCommand {
    pub source: AdmissionSource,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub output: Value,
}
impl AdmissionFinishCommand {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()?;
        validate_wire_value(&self.output)?;
        bound(&self.output, ADMISSION_OUTPUT_MAX_BYTES)?;
        bound(self, ADMISSION_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionQuery {
    pub occurrence: AdmissionOccurrence,
}
impl AdmissionQuery {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionReleaseCommand {
    pub occurrence: AdmissionOccurrence,
    pub source_id: String,
    pub expected_revision: u64,
    pub request_id: String,
    pub reason: String,
}
impl AdmissionReleaseCommand {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        validate_text(&self.source_id, 128)?;
        positive(self.expected_revision)?;
        positive(
            self.expected_revision
                .checked_add(1)
                .ok_or_else(|| invalid("admission revision overflow"))?,
        )?;
        validate_text(&self.request_id, 128)?;
        validate_text(&self.reason, 1024)?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionReleaseReceipt {
    pub occurrence: AdmissionOccurrence,
    pub source_id: String,
    pub revision: u64,
    pub accepted_at: Timestamp,
    pub already_accepted: bool,
}
impl AdmissionReleaseReceipt {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        validate_text(&self.source_id, 128)?;
        positive(self.revision)?;
        time(self.accepted_at)
    }
    pub fn matches(&self, c: &AdmissionReleaseCommand) -> bool {
        self.occurrence == c.occurrence
            && self.source_id == c.source_id
            && Some(self.revision) == c.expected_revision.checked_add(1)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionMaintenanceQuery {
    pub limit: u32,
}
impl AdmissionMaintenanceQuery {
    pub fn validate(&self) -> Result<()> {
        if !(1..=ADMISSION_MAX_MAINTENANCE_BATCH).contains(&self.limit) {
            return Err(invalid("admission maintenance limit exceeds bound"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionMaintenanceProgress {
    pub examined: u32,
    /// Workflow admission offers; each resumes an activation.
    pub offered: u32,
    /// Standalone task offers schedule delivery without resuming a workflow.
    #[serde(default, skip_serializing_if = "crate::task_admission::is_zero")]
    pub task_offered: u32,
    pub expired: u32,
    pub resumed: u32,
}
