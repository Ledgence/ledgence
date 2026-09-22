//! One-shot durable completion subscriptions. Transport I/O is outside storage transactions.
use crate::*;
use serde_json::Value;
use std::time::Instant;

pub const COMPLETION_EVENT_MAX_BYTES: usize = 16 * 1024;
pub const COMPLETION_COMMAND_MAX_BYTES: usize = 4096;
pub const COMPLETION_STATUS_MAX_BYTES: usize = 24 * 1024;
pub const MAX_COMPLETION_SUBSCRIPTIONS: u32 = 16;
pub const MAX_COMPLETION_BATCH: u32 = 16;
pub const COMPLETION_MAX_ATTEMPTS: u32 = 8;
pub const COMPLETION_MAX_GENERATION: u32 = 1000;
pub const COMPLETION_LEASE_MS: u64 = 30_000;
pub const COMPLETION_MAX_RETRY_DELAY_MS: u64 = 300_000;
const MAX_TIME: u64 = 253_402_300_799_999;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionTarget {
    Task { id: String },
    Workflow { id: String },
}
impl CompletionTarget {
    pub fn id(&self) -> &str {
        match self {
            Self::Task { id } | Self::Workflow { id } => id,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Task { .. } => "task",
            Self::Workflow { .. } => "workflow",
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(self.id(), 128)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionSubscribeCommand {
    pub scope: Scope,
    pub target: CompletionTarget,
    pub destination: String,
    /// Unique within scope + target, not across unrelated executions.
    pub idempotency_key: String,
}
impl CompletionSubscribeCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.target.validate()?;
        validate_text(&self.destination, 128)?;
        validate_text(&self.idempotency_key, 128)?;
        bounded(self, COMPLETION_COMMAND_MAX_BYTES)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let command: Self = decode_unique_json(bytes, COMPLETION_COMMAND_MAX_BYTES)?;
        command.validate()?;
        Ok(command)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionRetryCommand {
    pub scope: Scope,
    pub subscription_id: String,
    /// The exhausted generation to rearm. This is the durable operation identity.
    /// Replaying an older accepted generation returns current state without rearming.
    pub expected_generation: u32,
}
impl CompletionRetryCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.subscription_id, 128)?;
        if !(1..COMPLETION_MAX_GENERATION).contains(&self.expected_generation) {
            return Err(invalid(
                "completion retry generation is outside supported bounds",
            ));
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let command: Self = decode_unique_json(bytes, COMPLETION_COMMAND_MAX_BYTES)?;
        command.validate()?;
        Ok(command)
    }
}

/// Operator-configured immutable binding. Core does not interpret transport settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionDestination {
    pub scope: Scope,
    pub destination: String,
    pub binding: String,
}
impl CompletionDestination {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.destination, 128)?;
        validate_text(&self.binding, 4096)
    }
}

/// Compact reference-only CloudEvent: no application result copy and no platform `data`.
/// Event identity and bytes remain unchanged across retries and manual redelivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CompletionEvent(Value);
impl CompletionEvent {
    pub fn new(value: Value) -> Result<Self> {
        let event = Self(value);
        event.validate()?;
        Ok(event)
    }
    pub fn value(&self) -> &Value {
        &self.0
    }
    pub fn id(&self) -> &str {
        self.0["id"].as_str().unwrap_or_default()
    }
    pub fn source(&self) -> &str {
        self.0["source"].as_str().unwrap_or_default()
    }
    pub fn trace_context(&self) -> Option<TraceContext> {
        self.0
            .get("traceparent")
            .and_then(Value::as_str)
            .map(|traceparent| TraceContext {
                traceparent: traceparent.to_owned(),
                tracestate: self
                    .0
                    .get("tracestate")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::new(decode_unique_json(bytes, COMPLETION_EVENT_MAX_BYTES)?)
    }
    pub fn validate(&self) -> Result<()> {
        bounded(&self.0, COMPLETION_EVENT_MAX_BYTES)?;
        ledgence_worker_api::validate_cloudevent_context(&self.0)?;
        let object = self
            .0
            .as_object()
            .ok_or_else(|| invalid("completion event must be an object"))?;
        const ALLOWED: &[&str] = &[
            "specversion",
            "id",
            "source",
            "type",
            "subject",
            "time",
            "ldgtenantid",
            "ldgnamespace",
            "ldgstate",
            "ldgresultref",
            "ldgtaskid",
            "ldgrunid",
            "ldgattemptid",
            "ldgworkflowid",
            "ldgactivationid",
            "ldgparentworkflowid",
            "ldgrootworkflowid",
            "ldgcorrelationkey",
            "ldgcorrelationkeyencoding",
            "traceparent",
            "tracestate",
        ];
        if object.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
            return Err(invalid("unsupported completion event attribute"));
        }
        for name in [
            "id",
            "subject",
            "time",
            "ldgtenantid",
            "ldgnamespace",
            "ldgstate",
            "ldgresultref",
        ] {
            validate_text(
                text(&self.0, name)?,
                if name == "ldgresultref" { 2048 } else { 256 },
            )?;
        }
        if self.source() != "urn:ledgence:orchestrator"
            || !matches!(
                text(&self.0, "ldgstate")?,
                "succeeded" | "failed" | "cancelled"
            )
        {
            return Err(invalid("invalid completion source or terminal state"));
        }
        let target = match text(&self.0, "type")? {
            "com.ledgence.task.completed.v1" => {
                validate_text(text(&self.0, "ldgrunid")?, 128)?;
                CompletionTarget::Task {
                    id: text(&self.0, "ldgtaskid")?.to_owned(),
                }
            }
            "com.ledgence.workflow.completed.v1" => {
                if ["ldgtaskid", "ldgrunid", "ldgattemptid", "ldgactivationid"]
                    .iter()
                    .any(|key| object.contains_key(*key))
                {
                    return Err(invalid(
                        "workflow completion must not impersonate a controller task",
                    ));
                }
                CompletionTarget::Workflow {
                    id: text(&self.0, "ldgworkflowid")?.to_owned(),
                }
            }
            _ => return Err(invalid("invalid completion event type")),
        };
        target.validate()?;
        let scope = Scope {
            tenant_id: text(&self.0, "ldgtenantid")?.into(),
            namespace: text(&self.0, "ldgnamespace")?.into(),
        };
        scope.validate()?;
        if self.id() != completion_event_id(&target)
            || text(&self.0, "subject")? != format!("{}s/{}", target.kind(), target.id())
            || text(&self.0, "ldgresultref")? != completion_result_ref(&scope, &target)
        {
            return Err(invalid("completion event reference identity differs"));
        }
        for key in [
            "ldgtaskid",
            "ldgrunid",
            "ldgattemptid",
            "ldgworkflowid",
            "ldgactivationid",
            "ldgparentworkflowid",
            "ldgrootworkflowid",
        ] {
            if let Some(value) = object.get(key) {
                validate_text(
                    value
                        .as_str()
                        .ok_or_else(|| invalid("invalid completion identifier"))?,
                    128,
                )?;
            }
        }
        validate_workflow_lineage(
            object.get("ldgworkflowid").and_then(Value::as_str),
            object.get("ldgparentworkflowid").and_then(Value::as_str),
            object.get("ldgrootworkflowid").and_then(Value::as_str),
        )?;
        if (object.contains_key("ldgactivationid")
            && (!object.contains_key("ldgworkflowid")
                || self.0["ldgactivationid"] != self.0["ldgtaskid"]))
            || (self.0["ldgstate"] == "cancelled" && object.contains_key("ldgattemptid"))
        {
            return Err(invalid("invalid completion lineage or deciding attempt"));
        }
        if let Some(key) = object.get("ldgcorrelationkey") {
            let key = key
                .as_str()
                .ok_or_else(|| invalid("invalid completion correlation key"))?;
            match object.get("ldgcorrelationkeyencoding") {
                None if key.len() <= 512 && !key.chars().any(char::is_control) => {}
                Some(Value::String(encoding)) if encoding == "percent" => {
                    decode_completion_correlation(key)?;
                }
                _ => return Err(invalid("invalid completion correlation encoding or length")),
            }
        } else if object.contains_key("ldgcorrelationkeyencoding") {
            return Err(invalid("correlation encoding requires a key"));
        }
        Ok(())
    }
    pub fn matches(&self, scope: &Scope, target: &CompletionTarget) -> bool {
        self.0["ldgtenantid"] == scope.tenant_id
            && self.0["ldgnamespace"] == scope.namespace
            && self.id() == completion_event_id(target)
            && self.0["type"] == format!("com.ledgence.{}.completed.v1", target.kind())
    }
}

pub fn completion_event_id(target: &CompletionTarget) -> String {
    format!("evt_{}_completed_{}", target.kind(), target.id())
}
pub fn completion_result_ref(scope: &Scope, target: &CompletionTarget) -> String {
    format!(
        "/v1/{}s/result?tenant_id={}&namespace={}&{}_id={}",
        target.kind(),
        percent(&scope.tenant_id),
        percent(&scope.namespace),
        target.kind(),
        percent(target.id())
    )
}
fn percent(value: &str) -> String {
    let mut output = String::new();
    const HEX: &[u8] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            output.push(char::from(byte));
        } else {
            output.push('%');
            output.push(char::from(HEX[(byte >> 4) as usize]));
            output.push(char::from(HEX[(byte & 15) as usize]));
        }
    }
    output
}

/// Encode the rare accepted business strings excluded by CloudEvents metadata.
pub fn encode_completion_correlation(value: &str) -> String {
    percent(value)
}
pub fn decode_completion_correlation(value: &str) -> Result<String> {
    if value.len() > 1536 {
        return Err(invalid("encoded completion correlation exceeds limit"));
    }
    let mut bytes = Vec::with_capacity(value.len());
    let mut iter = value.bytes();
    while let Some(byte) = iter.next() {
        if byte == b'%' {
            let high = iter.next().and_then(|c| char::from(c).to_digit(16));
            let low = iter.next().and_then(|c| char::from(c).to_digit(16));
            match (high, low) {
                (Some(high), Some(low)) => bytes.push((high * 16 + low) as u8),
                _ => return Err(invalid("malformed completion correlation escape")),
            }
        } else {
            bytes.push(byte);
        }
    }
    let decoded = String::from_utf8(bytes).map_err(|_| invalid("correlation is not UTF-8"))?;
    if decoded.len() > 512 || decoded.chars().any(char::is_control) || percent(&decoded) != value {
        return Err(invalid(
            "invalid or noncanonical encoded completion correlation",
        ));
    }
    Ok(decoded)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionState {
    Waiting,
    Pending,
    Delivering,
    Retrying,
    Delivered,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionSubscription {
    pub subscription_id: String,
    pub command: CompletionSubscribeCommand,
    pub state: CompletionState,
    pub generation: u32,
    /// Leased transport attempts in this generation, including uncertain sends.
    pub attempts: u32,
    pub total_attempts: u64,
    pub created_at: Timestamp,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub activated_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_attempt_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub lease_expires_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub delivered_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub exhausted_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub last_failure: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub event: Option<CompletionEvent>,
}
impl CompletionSubscription {
    pub fn validate(&self) -> Result<()> {
        self.command.validate()?;
        validate_text(&self.subscription_id, 128)?;
        if !(1..=COMPLETION_MAX_GENERATION).contains(&self.generation)
            || self.attempts > COMPLETION_MAX_ATTEMPTS
            || self.total_attempts < u64::from(self.attempts)
            || self.total_attempts > u64::from(self.generation * COMPLETION_MAX_ATTEMPTS)
            || self.created_at > MAX_TIME
        {
            return Err(invalid("invalid completion subscription counters or time"));
        }
        for time in [
            self.activated_at,
            self.next_attempt_at,
            self.lease_expires_at,
            self.delivered_at,
            self.exhausted_at,
        ]
        .into_iter()
        .flatten()
        {
            if time < self.created_at || time > MAX_TIME {
                return Err(invalid("invalid completion subscription timestamp"));
            }
        }
        if let Some(reason) = &self.last_failure {
            validate_text(reason, 256)?;
        }
        let active = self.event.is_some() && self.activated_at.is_some();
        let no_terminal = self.delivered_at.is_none() && self.exhausted_at.is_none();
        let valid = match self.state {
            CompletionState::Waiting => {
                !active
                    && self.event.is_none()
                    && self.activated_at.is_none()
                    && self.next_attempt_at.is_none()
                    && self.lease_expires_at.is_none()
                    && no_terminal
                    && self.attempts == 0
                    && self.total_attempts == 0
                    && self.generation == 1
                    && self.last_failure.is_none()
            }
            CompletionState::Pending => {
                active
                    && self.attempts == 0
                    && self.next_attempt_at.is_some()
                    && self.lease_expires_at.is_none()
                    && no_terminal
            }
            CompletionState::Retrying => {
                active
                    && (1..COMPLETION_MAX_ATTEMPTS).contains(&self.attempts)
                    && self.next_attempt_at.is_some()
                    && self.lease_expires_at.is_none()
                    && no_terminal
            }
            CompletionState::Delivering => {
                active
                    && self.attempts > 0
                    && self.next_attempt_at.is_none()
                    && self.lease_expires_at.is_some()
                    && no_terminal
            }
            CompletionState::Delivered => {
                active
                    && self.attempts > 0
                    && self.next_attempt_at.is_none()
                    && self.lease_expires_at.is_none()
                    && self.delivered_at.is_some()
                    && self.exhausted_at.is_none()
            }
            CompletionState::Exhausted => {
                active
                    && self.attempts == COMPLETION_MAX_ATTEMPTS
                    && self.next_attempt_at.is_none()
                    && self.lease_expires_at.is_none()
                    && self.delivered_at.is_none()
                    && self.exhausted_at.is_some()
            }
        };
        if !valid {
            return Err(invalid("inconsistent completion delivery state"));
        }
        if let Some(event) = &self.event {
            event.validate()?;
            if !event.matches(&self.command.scope, &self.command.target) {
                return Err(invalid("completion event target mismatch"));
            }
        }
        bounded(self, COMPLETION_STATUS_MAX_BYTES)
    }
    pub fn matches(&self, command: &CompletionSubscribeCommand) -> bool {
        self.command == *command
    }
}

#[derive(Debug, Clone)]
pub struct CompletionLease {
    pub subscription: CompletionSubscription,
    pub lease_token: String,
    /// Exact committed CloudEvent bytes, reused without regeneration on every send.
    pub event_bytes: Vec<u8>,
}
impl CompletionLease {
    pub fn validate(&self) -> Result<()> {
        self.subscription.validate()?;
        validate_text(&self.lease_token, 128)?;
        if self.subscription.state != CompletionState::Delivering
            || self.subscription.event.as_ref()
                != Some(&CompletionEvent::decode(&self.event_bytes)?)
        {
            return Err(invalid("invalid completion lease event or state"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionDeliveryOutcome {
    Confirmed,
    Retry {
        reason: String,
        retry_after_ms: Option<u64>,
    },
}
#[derive(Debug, Clone)]
pub struct CompletionDeliveryResult {
    pub subscription_id: String,
    pub generation: u32,
    pub lease_token: String,
    pub outcome: CompletionDeliveryOutcome,
}
impl CompletionDeliveryResult {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.subscription_id, 128)?;
        validate_text(&self.lease_token, 128)?;
        if !(1..=COMPLETION_MAX_GENERATION).contains(&self.generation) {
            return Err(invalid("invalid completion generation"));
        }
        if let CompletionDeliveryOutcome::Retry {
            reason,
            retry_after_ms,
        } = &self.outcome
        {
            validate_text(reason, 256)?;
            if retry_after_ms.is_some_and(|delay| delay > COMPLETION_MAX_RETRY_DELAY_MS) {
                return Err(invalid("completion retry delay exceeds bound"));
            }
        }
        Ok(())
    }
}

/// Subscriber-facing operations. Accepted subscriptions survive caller disconnection.
pub trait CompletionService: Send + Sync {
    fn subscribe_completion<'a>(
        &'a self,
        command: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription>;
    fn completion_status<'a>(
        &'a self,
        scope: &'a Scope,
        subscription_id: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription>;
    fn retry_completion<'a>(
        &'a self,
        command: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription>;
}
/// Atomic registration and terminal hooks share the execution row lock. Delivery
/// leasing only locks subscription rows; it must never lock executions afterward.
pub trait CompletionStore: Send + Sync {
    fn configure_completion_destination<'a>(
        &'a self,
        destination: &'a CompletionDestination,
    ) -> ContractFuture<'a, ()>;
    fn subscribe_completion<'a>(
        &'a self,
        command: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription>;
    fn completion_status<'a>(
        &'a self,
        scope: &'a Scope,
        subscription_id: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription>;
    fn retry_completion<'a>(
        &'a self,
        command: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription>;
    fn lease_completions<'a>(
        &'a self,
        destination: &'a CompletionDestination,
        limit: u32,
        deadline: Instant,
    ) -> ContractFuture<'a, Vec<CompletionLease>>;
    fn complete_deliveries<'a>(
        &'a self,
        completions: &'a [CompletionDeliveryResult],
        deadline: Instant,
    ) -> ContractFuture<'a, ()>;
}
pub trait CompletionSender: Send + Sync {
    /// Positive transport acknowledgment only; ambiguous outcomes require retry.
    fn deliver<'a>(
        &'a self,
        lease: &'a CompletionLease,
        deadline: Instant,
    ) -> ContractFuture<'a, CompletionDeliveryOutcome>;
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("missing completion event text attribute"))
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn bounded(value: &impl Serialize, maximum: usize) -> Result<()> {
    crate::submission::check_encoded_size(value, maximum, "completion contract").map_err(Into::into)
}

#[cfg(test)]
mod tests;
