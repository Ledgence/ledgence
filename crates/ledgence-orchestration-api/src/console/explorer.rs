//! Bounded, durable workflow evidence shared by Graph and Timeline. Relationships
//! are explicit; ordering, timestamps and containment never imply dependencies.
use super::*;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleEvidenceAvailability {
    Available,
    /// A retained identity exists, but its target cannot be inspected. This is
    /// not a claim that retention, rather than another cause, removed it.
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleDecisionKind {
    Continue,
    Suspend,
    Wait,
    Complete,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleWakeReason {
    Event,
    Timer,
    Timeout,
    Approved,
    Rejected,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleEffectOutcomeKind {
    RecordedSuccess,
    RecordedFailure,
    ClosedUnknown,
    Disputed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleLocalObservation {
    pub attempt_id: String,
    pub started_at: Timestamp,
    pub elapsed_us: ConsoleU64,
    /// Callable outcome, not durable-result acceptance.
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsoleExplorerData {
    Entrypoint {
        state: Option<TaskState>,
        availability: ConsoleEvidenceAvailability,
        submitted_at: Timestamp,
        terminal_at: Option<Timestamp>,
        applied_at: Option<Timestamp>,
        decision_kind: Option<ConsoleDecisionKind>,
        error: Option<ApplicationError>,
        /// Only a successfully applied decision can establish this edge.
        resumed_activation_id: Option<String>,
    },
    Child {
        key: String,
        execution: ConsoleExecutionIdentity,
        program: ProgramRef,
        fork_key: Option<String>,
        availability: ConsoleEvidenceAvailability,
        state: Option<ConsoleExecutionState>,
        submitted_at: Timestamp,
        terminal_at: Option<Timestamp>,
    },
    Fork {
        key: String,
        branch_keys: Vec<String>,
        accepted_at: Timestamp,
        accepting_attempt_id: String,
    },
    Local {
        key: String,
        callable: String,
        accepted_at: Option<Timestamp>,
        accepting_attempt_id: Option<String>,
        observation: Option<ConsoleLocalObservation>,
    },
    ChildWait {
        member_keys: Vec<String>,
        resume: String,
        applied_at: Timestamp,
        resumed_activation_id: Option<String>,
    },
    ExternalWait {
        key: String,
        wait_kind: ConsoleWaitKind,
        deadline: Option<Timestamp>,
        registered_at: Timestamp,
        closed_at: Option<Timestamp>,
        /// Only an actually scheduled activation's frozen wake establishes this.
        wake_reason: Option<ConsoleWakeReason>,
        resumed_activation_id: Option<String>,
    },
    NativeEffectWait {
        key: String,
        occurrence: EffectOccurrence,
        after_resolution_id: Option<String>,
        registered_at: Timestamp,
        closed_at: Option<Timestamp>,
        resolution_id: Option<String>,
        outcome_kind: Option<ConsoleEffectOutcomeKind>,
        resumed_activation_id: Option<String>,
    },
    AdmissionWait {
        #[serde(default)]
        subject: ConsoleAdmissionSubject,
        key: String,
        occurrence: AdmissionOccurrence,
        resource: String,
        deadline: Timestamp,
        registered_at: Timestamp,
        closed_at: Option<Timestamp>,
        selection_kind: Option<ConsoleAdmissionSelectionKind>,
        resumed_activation_id: Option<String>,
    },
    /// Exists only after the original source durably accepts its result.
    AdmissionOperation {
        occurrence: AdmissionOccurrence,
        resource: String,
        callable: String,
        operation_kind: AdmissionOperationKind,
        operation_version: String,
        binding_digest: Digest,
        source_id: String,
        accepting_attempt_id: String,
        accepted_at: Timestamp,
    },
    ProviderWait {
        key: String,
        occurrence: EffectOccurrence,
        after_effect_revision: Option<ConsoleU64>,
        registered_at: Timestamp,
        closed_at: Option<Timestamp>,
        selected_effect_revision: Option<ConsoleU64>,
        selection_kind: Option<ConsoleProviderSelectionKind>,
        resumed_activation_id: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleAdmissionSelectionKind {
    Provider,
    Offered,
    Finished,
    Closed,
    NeedsReview,
}
impl From<&AdmissionSelection> for ConsoleAdmissionSelectionKind {
    fn from(value: &AdmissionSelection) -> Self {
        match value {
            AdmissionSelection::Provider { .. } => Self::Provider,
            AdmissionSelection::Offered { .. } => Self::Offered,
            AdmissionSelection::Finished { .. } => Self::Finished,
            AdmissionSelection::Closed { .. } => Self::Closed,
            AdmissionSelection::NeedsReview { .. } => Self::NeedsReview,
        }
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleProviderSelectionKind {
    ProviderSucceeded,
    ProviderFailed,
    ProviderCancelled,
    OperatorAssessment,
    Disputed,
    NeedsReview,
}
impl From<&ProviderSelection> for ConsoleProviderSelectionKind {
    fn from(v: &ProviderSelection) -> Self {
        match v {
            ProviderSelection::ProviderSucceeded { .. } => Self::ProviderSucceeded,
            ProviderSelection::ProviderFailed { .. } => Self::ProviderFailed,
            ProviderSelection::ProviderCancelled { .. } => Self::ProviderCancelled,
            ProviderSelection::OperatorAssessment { .. } => Self::OperatorAssessment,
            ProviderSelection::Disputed { .. } => Self::Disputed,
            ProviderSelection::NeedsReview { .. } => Self::NeedsReview,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleExplorerNode {
    /// Canonical JSON tuple [kind, workflow_id, activation_id, key]. Treat as opaque.
    pub id: String,
    pub activation_id: String,
    pub revision: ConsoleU64,
    /// Handler name repeated on evidence records to make bounded pages useful.
    pub entrypoint: String,
    /// Durable relationships carried by this record, including unloaded endpoints.
    pub relations: Vec<ConsoleExplorerRelation>,
    #[serde(flatten)]
    pub data: ConsoleExplorerData,
}
impl ConsoleExplorerData {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Entrypoint { .. } => "entrypoint",
            Self::Child { .. } => "child",
            Self::Fork { .. } => "fork",
            Self::Local { .. } => "local",
            Self::ChildWait { .. } => "child_wait",
            Self::ExternalWait { .. } => "external_wait",
            Self::NativeEffectWait { .. } => "native_effect_wait",
            Self::ProviderWait { .. } => "provider_wait",
            Self::AdmissionWait { .. } => "admission_wait",
            Self::AdmissionOperation { .. } => "admission_operation",
        }
    }
    pub fn key(&self) -> &str {
        match self {
            Self::Entrypoint { .. } | Self::ChildWait { .. } => "",
            Self::Child { key, .. }
            | Self::Fork { key, .. }
            | Self::Local { key, .. }
            | Self::ExternalWait { key, .. }
            | Self::NativeEffectWait { key, .. }
            | Self::ProviderWait { key, .. }
            | Self::AdmissionWait { key, .. } => key,
            Self::AdmissionOperation { source_id, .. } => source_id,
        }
    }
    pub fn validate(&self) -> Result<()> {
        let optional_id =
            |id: &Option<String>| id.as_deref().map_or(Ok(()), |id| validate_text(id, 128));
        let time = |at: Option<Timestamp>| at.map_or(Ok(()), timestamp);
        match self {
            Self::Entrypoint {
                state,
                availability,
                submitted_at,
                terminal_at,
                applied_at,
                decision_kind,
                error,
                resumed_activation_id,
                ..
            } => {
                timestamp(*submitted_at)?;
                time(*terminal_at)?;
                time(*applied_at)?;
                optional_id(resumed_activation_id)?;
                if *availability == ConsoleEvidenceAvailability::Unavailable && state.is_some() {
                    return Err(invalid("unavailable entrypoint cannot claim current state"));
                }
                if let Some(state) = state
                    && state.is_terminal() != terminal_at.is_some()
                {
                    return Err(invalid("inconsistent explorer entrypoint terminal state"));
                }
                if error.is_some() && decision_kind.is_some() {
                    return Err(invalid(
                        "rejected decision cannot establish explorer effects",
                    ));
                }
                if decision_kind.is_some() && applied_at.is_none() {
                    return Err(invalid("explorer decision requires application evidence"));
                }
                if resumed_activation_id.is_some()
                    && !matches!(
                        decision_kind,
                        Some(
                            ConsoleDecisionKind::Continue
                                | ConsoleDecisionKind::Suspend
                                | ConsoleDecisionKind::Wait
                        )
                    )
                {
                    return Err(invalid("explorer resume requires an applied continuation"));
                }
                if let Some(error) = error {
                    validate_workflow_error(error)?;
                }
            }
            Self::Child {
                key,
                execution,
                program,
                fork_key,
                availability,
                state,
                submitted_at,
                terminal_at,
            } => {
                validate_text(key, 128)?;
                validate_text(&execution.id, 128)?;
                program.validate()?;
                optional_id(fork_key)?;
                timestamp(*submitted_at)?;
                time(*terminal_at)?;
                if *availability == ConsoleEvidenceAvailability::Unavailable && state.is_some() {
                    return Err(invalid("unavailable child cannot claim current state"));
                }
                if let Some(state) = state
                    && (!state.supports(execution.kind)
                        || state.is_terminal() != terminal_at.is_some())
                {
                    return Err(invalid("inconsistent explorer child state"));
                }
            }
            Self::Fork {
                key,
                branch_keys,
                accepted_at,
                accepting_attempt_id,
            } => {
                validate_text(key, 128)?;
                keys(branch_keys)?;
                timestamp(*accepted_at)?;
                validate_text(accepting_attempt_id, 128)?;
            }
            Self::Local {
                key,
                callable,
                accepted_at,
                accepting_attempt_id,
                observation,
            } => {
                validate_text(key, 128)?;
                validate_text(callable, 512)?;
                time(*accepted_at)?;
                optional_id(accepting_attempt_id)?;
                if accepted_at.is_some() != accepting_attempt_id.is_some()
                    || (accepted_at.is_none() && observation.is_none())
                {
                    return Err(invalid("local explorer node requires recorded evidence"));
                }
                if let Some(observation) = observation {
                    validate_text(&observation.attempt_id, 128)?;
                    timestamp(observation.started_at)?;
                    if !matches!(
                        observation.state.as_str(),
                        "returned" | "failed" | "cancelled" | "replayed"
                    ) {
                        return Err(invalid("invalid local observation state"));
                    }
                }
            }
            Self::ChildWait {
                member_keys,
                resume,
                applied_at,
                resumed_activation_id,
            } => {
                keys(member_keys)?;
                validate_text(resume, 128)?;
                timestamp(*applied_at)?;
                optional_id(resumed_activation_id)?;
            }
            Self::ExternalWait {
                key,
                wait_kind,
                deadline,
                registered_at,
                closed_at,
                wake_reason,
                resumed_activation_id,
            } => {
                validate_text(key, 128)?;
                time(*deadline)?;
                timestamp(*registered_at)?;
                time(*closed_at)?;
                optional_id(resumed_activation_id)?;
                if wake_reason.is_some() != resumed_activation_id.is_some()
                    || (wake_reason.is_some() && closed_at.is_none())
                {
                    return Err(invalid("external wake requires a scheduled activation"));
                }
                let compatible = match wait_kind {
                    ConsoleWaitKind::Event => matches!(
                        wake_reason,
                        None | Some(ConsoleWakeReason::Event | ConsoleWakeReason::Timeout)
                    ),
                    ConsoleWaitKind::Timer => {
                        matches!(wake_reason, None | Some(ConsoleWakeReason::Timer))
                    }
                    ConsoleWaitKind::Approval => matches!(
                        wake_reason,
                        None | Some(
                            ConsoleWakeReason::Approved
                                | ConsoleWakeReason::Rejected
                                | ConsoleWakeReason::Expired
                        )
                    ),
                    ConsoleWaitKind::EffectResolution
                    | ConsoleWaitKind::ProviderOperation
                    | ConsoleWaitKind::ResourceAdmission => false,
                };
                if !compatible
                    || (matches!(
                        wait_kind,
                        ConsoleWaitKind::Timer | ConsoleWaitKind::Approval
                    ) && deadline.is_none())
                    || (*wake_reason == Some(ConsoleWakeReason::Timeout) && deadline.is_none())
                {
                    return Err(invalid("inconsistent external wait wake kind"));
                }
            }
            Self::NativeEffectWait {
                key,
                occurrence,
                after_resolution_id,
                registered_at,
                closed_at,
                resolution_id,
                outcome_kind,
                resumed_activation_id,
            } => {
                validate_text(key, 128)?;
                occurrence.validate()?;
                optional_id(after_resolution_id)?;
                optional_id(resolution_id)?;
                optional_id(resumed_activation_id)?;
                timestamp(*registered_at)?;
                time(*closed_at)?;
                if resolution_id.is_some() != outcome_kind.is_some()
                    || resolution_id.is_some() != resumed_activation_id.is_some()
                    || (resolution_id.is_some() && closed_at.is_none())
                    || closed_at.is_some_and(|at| at < *registered_at)
                {
                    return Err(invalid("native wake requires one scheduled activation"));
                }
            }
            Self::AdmissionWait {
                subject,
                key,
                occurrence,
                resource,
                deadline,
                registered_at,
                closed_at,
                selection_kind,
                resumed_activation_id,
            } => {
                validate_text(key, 128)?;
                occurrence.validate()?;
                subject.validate_for(occurrence)?;
                crate::admission::name(resource)?;
                timestamp(*deadline)?;
                timestamp(*registered_at)?;
                time(*closed_at)?;
                optional_id(resumed_activation_id)?;
                if *deadline < *registered_at
                    || selection_kind.is_some() != resumed_activation_id.is_some()
                    || (selection_kind.is_some() && closed_at.is_none())
                    || closed_at.is_some_and(|at| at < *registered_at)
                {
                    return Err(invalid("admission wake requires one scheduled activation"));
                }
            }
            Self::AdmissionOperation {
                occurrence,
                resource,
                callable,
                operation_kind,
                operation_version,
                binding_digest,
                source_id,
                accepting_attempt_id,
                accepted_at,
            } => {
                occurrence.validate()?;
                crate::admission::name(resource)?;
                AdmissionOperation {
                    callable: callable.clone(),
                    kind: *operation_kind,
                    version: operation_version.clone(),
                    arguments: serde_json::json!({}),
                }
                .validate()?;
                binding_digest.validate()?;
                validate_text(source_id, 128)?;
                validate_text(accepting_attempt_id, 128)?;
                timestamp(*accepted_at)?;
            }
            Self::ProviderWait {
                key,
                occurrence,
                after_effect_revision,
                registered_at,
                closed_at,
                selected_effect_revision,
                selection_kind,
                resumed_activation_id,
            } => {
                ProviderWait {
                    key: key.clone(),
                    occurrence: occurrence.clone(),
                    after_effect_revision: after_effect_revision.map(|v| v.0),
                }
                .validate()?;
                optional_id(resumed_activation_id)?;
                timestamp(*registered_at)?;
                time(*closed_at)?;
                if selected_effect_revision.is_some() != selection_kind.is_some()
                    || selected_effect_revision.is_some() != resumed_activation_id.is_some()
                    || selected_effect_revision.is_some() && closed_at.is_none()
                    || selected_effect_revision.is_some_and(|v| {
                        v.0 == 0 || after_effect_revision.is_some_and(|after| v.0 <= after.0)
                    })
                    || closed_at.is_some_and(|at| at < *registered_at)
                {
                    return Err(invalid("provider wake requires one scheduled activation"));
                }
            }
        }
        crate::submission::check_encoded_size(self, 32 * 1024, "explorer record")?;
        Ok(())
    }
}
fn keys(values: &[String]) -> Result<()> {
    if values.len() > WORKFLOW_MAX_COMMANDS {
        return Err(invalid("too many explorer members"));
    }
    let mut unique = std::collections::BTreeSet::new();
    for value in values {
        validate_text(value, 128)?;
        if !unique.insert(value) {
            return Err(invalid("duplicate explorer member"));
        }
    }
    Ok(())
}
impl ConsoleRecord for ConsoleExplorerNode {
    fn position(&self) -> ConsolePosition {
        // Entrypoint and child-wait records have no user key. Cursor text is required
        // to be nonempty, so encode their kind as the singleton key; storage
        // decodes it back to the empty record_key before its indexed seek.
        let key = if self.data.key().is_empty() {
            self.data.kind()
        } else {
            self.data.key()
        };
        vec![
            ConsoleKey::Number(self.revision),
            ConsoleKey::Text(self.data.kind().into()),
            ConsoleKey::Text(key.into()),
        ]
    }
    fn validate(&self) -> Result<()> {
        validate_text(&self.activation_id, 128)?;
        validate_text(&self.entrypoint, 128)?;
        self.data.validate()?;
        if self.id.len() > 1024 {
            return Err(invalid("invalid explorer node identity"));
        }
        if self.relations.len() > CONSOLE_EXPLORER_MAX_RELATIONS {
            return Err(invalid("too many explorer relations"));
        }
        let (kind, workflow_id, activation_id, key): (String, String, String, String) =
            serde_json::from_str(&self.id)
                .map_err(|_| invalid("invalid explorer node identity"))?;
        if let ConsoleExplorerData::NativeEffectWait { occurrence, .. }
        | ConsoleExplorerData::ProviderWait { occurrence, .. } = &self.data
            && occurrence.workflow_id != workflow_id
        {
            return Err(invalid("native explorer wait belongs to another workflow"));
        }
        if let ConsoleExplorerData::AdmissionWait { occurrence, .. }
        | ConsoleExplorerData::AdmissionOperation { occurrence, .. } = &self.data
            && occurrence.workflow_id != workflow_id
        {
            return Err(invalid("admission evidence belongs to another workflow"));
        }
        if kind != self.data.kind()
            || activation_id != self.activation_id
            || key != self.data.key()
            || self.relations != self.derive_relations(&workflow_id)?
        {
            return Err(invalid("inconsistent explorer relations or identity"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowExplorer {
    pub workflow: ConsoleWorkflowDetail,
    pub page: ConsolePage<ConsoleExplorerNode>,
    /// Always retained_records_only: unrecorded/previously collected work is not
    /// inferred. next_cursor independently identifies records not loaded yet.
    pub evidence: String,
}
impl ConsoleWorkflowExplorer {
    pub fn validate(
        &self,
        workflow_id: &str,
        page: &ConsolePagination,
        binding: &ConsoleCursorBinding,
    ) -> Result<()> {
        self.workflow.summary.validate()?;
        self.page.validate(page, binding)?;
        if self.workflow.summary.workflow.workflow_id != workflow_id
            || self.evidence != "retained_records_only"
            || self.workflow.observed_at != self.page.observed_at
        {
            return Err(invalid("inconsistent workflow explorer"));
        }
        if usize::from(self.workflow.child_wait.is_some())
            + usize::from(self.workflow.external_wait_key.is_some())
            + usize::from(self.workflow.native_effect_wait.is_some())
            + usize::from(self.workflow.provider_wait.is_some())
            + usize::from(self.workflow.admission_wait.is_some())
            > 1
        {
            return Err(invalid("conflicting current workflow waits"));
        }
        if let Some(wait) = &self.workflow.native_effect_wait {
            wait.validate(workflow_id)?;
            if wait.occurrence.scope != binding.scope {
                return Err(invalid("native wait scope mismatch"));
            }
        }
        if let Some(wait) = &self.workflow.admission_wait {
            wait.validate(workflow_id)?;
            if wait.occurrence.scope != binding.scope {
                return Err(invalid("admission wait scope mismatch"));
            }
        }
        for node in &self.page.items {
            if let ConsoleExplorerData::AdmissionWait { occurrence, .. }
            | ConsoleExplorerData::AdmissionOperation { occurrence, .. } = &node.data
                && occurrence.scope != binding.scope
            {
                return Err(invalid("admission evidence scope mismatch"));
            }
            if let ConsoleExplorerData::NativeEffectWait { occurrence, .. }
            | ConsoleExplorerData::ProviderWait { occurrence, .. } = &node.data
                && occurrence.scope != binding.scope
            {
                return Err(invalid("native explorer wait scope mismatch"));
            }
            let id = serde_json::to_string(&(
                node.data.kind(),
                workflow_id,
                &node.activation_id,
                node.data.key(),
            ))
            .map_err(|_| invalid("invalid explorer identity"))?;
            if node.id != id {
                return Err(invalid("inconsistent explorer node identity"));
            }
        }
        if let Some(wait) = &self.workflow.provider_wait {
            wait.validate(workflow_id)?;
            if wait.occurrence.scope != binding.scope {
                return Err(invalid("provider wait scope mismatch"));
            }
        }
        metadata_size(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowInput {
    pub workflow_id: String,
    pub data: Value,
    pub observed_at: Timestamp,
}
impl ConsoleWorkflowInput {
    pub fn validate(&self, workflow_id: &str) -> Result<()> {
        validate_text(&self.workflow_id, 128)?;
        timestamp(self.observed_at)?;
        if self.workflow_id != workflow_id {
            return Err(invalid("inconsistent workflow input"));
        }
        crate::submission::check_encoded_size(
            &self.data,
            SUBMISSION_DATA_MAX_BYTES,
            "workflow input",
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleAncestor {
    pub execution: ConsoleExecutionIdentity,
    pub program: Option<ProgramRef>,
    pub availability: ConsoleEvidenceAvailability,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleAncestry {
    pub execution: ConsoleExecutionIdentity,
    /// Root first, selected execution last. An unavailable parent ends traversal.
    pub path: Vec<ConsoleAncestor>,
    pub observed_at: Timestamp,
}
impl ConsoleAncestry {
    pub fn validate(&self, execution: &ConsoleExecutionIdentity) -> Result<()> {
        timestamp(self.observed_at)?;
        if self.execution.kind != execution.kind
            || self.execution.id != execution.id
            || self.path.is_empty()
            || self.path.len() > WORKFLOW_MAX_DEPTH as usize + 2
        {
            return Err(invalid("inconsistent execution ancestry"));
        }
        let mut unique = std::collections::BTreeSet::new();
        for item in &self.path {
            validate_text(&item.execution.id, 128)?;
            if !unique.insert((format!("{:?}", item.execution.kind), &item.execution.id)) {
                return Err(invalid("cyclic execution ancestry"));
            }
            if let Some(program) = &item.program {
                program.validate()?;
            }
        }
        for (index, item) in self.path.iter().enumerate() {
            if (index + 1 < self.path.len()
                && item.execution.kind != ConsoleExecutionKind::Workflow)
                || (item.availability == ConsoleEvidenceAvailability::Unavailable
                    && (index != 0 || index + 1 == self.path.len()))
                || (item.availability == ConsoleEvidenceAvailability::Available
                    && item.program.is_none())
            {
                return Err(invalid("inconsistent ancestry availability or ownership"));
            }
        }
        let last = &self.path.last().expect("nonempty").execution;
        if last.kind != execution.kind || last.id != execution.id {
            return Err(invalid("ancestry target mismatch"));
        }
        metadata_size(self)
    }
}
