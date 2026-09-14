//! Directly addressed, one-shot workflow events and persisted wait deadlines.
use crate::*;
use ledgence_worker_api::{validate_json_cloudevent, validate_wire_value};
use serde_json::Value;

pub const WORKFLOW_EVENT_MAX_BYTES: usize = 64 * 1024;
pub const WORKFLOW_EVENT_COMMAND_MAX_BYTES: usize = 70 * 1024;
pub const WORKFLOW_MAX_PENDING_EVENTS: usize = 128;
pub const WORKFLOW_PENDING_EVENTS_MAX_BYTES: usize = 256 * 1024;
/// Relative waits are bounded to 365 days. Zero means immediately eligible.
pub const WORKFLOW_MAX_DELAY_MS: u64 = 365 * 24 * 60 * 60 * 1_000;
const MAX_TIMESTAMP: u64 = 253_402_300_799_999;

/// The sender's original JSON CloudEvent. This is an external event profile,
/// without required Ledgence execution identifiers. Routing authority comes
/// from the command's scope/workflow/key, never from event extension attributes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowEvent(Value);
impl WorkflowEvent {
    pub fn new(value: Value) -> Result<Self> {
        let event = Self(value);
        event.validate()?;
        Ok(event)
    }
    pub fn value(&self) -> &Value {
        &self.0
    }
    pub fn id(&self) -> &str {
        self.0.get("id").and_then(Value::as_str).unwrap_or_default()
    }
    pub fn source(&self) -> &str {
        self.0
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }
    pub fn validate(&self) -> Result<()> {
        bounded(&self.0, WORKFLOW_EVENT_MAX_BYTES, "workflow event")?;
        validate_json_cloudevent(&self.0)?;
        validate_text(self.id(), 128)?;
        validate_text(self.source(), 2048)?;
        validate_wire_value(&self.0["data"])?;
        Ok(())
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowEventCommand {
    pub scope: Scope,
    pub workflow_id: String,
    pub key: String,
    pub event: WorkflowEvent,
}
impl WorkflowEventCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.key, 128)?;
        self.event.validate()?;
        bounded(
            self,
            WORKFLOW_EVENT_COMMAND_MAX_BYTES,
            "workflow event command",
        )
    }
}

/// Durable acceptance, not a promise that a controller has processed the event.
/// `accepted_at` is immutable across reconciliation and comes from store time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowEventReceipt {
    pub scope: Scope,
    pub workflow_id: String,
    pub key: String,
    pub event_id: String,
    pub event_source: String,
    pub accepted_at: Timestamp,
    pub already_accepted: bool,
}
impl WorkflowEventReceipt {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.key, 128)?;
        validate_text(&self.event_id, 128)?;
        validate_text(&self.event_source, 2048)?;
        timestamp(self.accepted_at)
    }
    pub fn matches(&self, command: &WorkflowEventCommand) -> bool {
        self.scope == command.scope
            && self.workflow_id == command.workflow_id
            && self.key == command.key
            && self.event_id == command.event.id()
            && self.event_source == command.event.source()
    }
}

/// A single named rendezvous. Keys are one-shot across a workflow, so callbacks
/// from an earlier iteration cannot accidentally satisfy a later wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowWait {
    Event {
        key: String,
        #[serde(deserialize_with = "crate::observation::required_option")]
        timeout_ms: Option<u64>,
    },
    Timer {
        key: String,
        delay_ms: u64,
    },
}
impl WorkflowWait {
    pub fn key(&self) -> &str {
        match self {
            Self::Event { key, .. } | Self::Timer { key, .. } => key,
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(self.key(), 128)?;
        let duration = match self {
            Self::Event { timeout_ms, .. } => *timeout_ms,
            Self::Timer { delay_ms, .. } => Some(*delay_ms),
        };
        if duration.is_some_and(|duration| duration > WORKFLOW_MAX_DELAY_MS) {
            return Err(invalid("workflow wait duration exceeds 365 days"));
        }
        Ok(())
    }
    /// Anchor exactly once when accepting the decision. Scheduler retries must
    /// read the persisted deadline rather than call this with a newer time.
    pub fn deadline(&self, accepted_at: Timestamp) -> Result<Option<Timestamp>> {
        self.validate()?;
        timestamp(accepted_at)?;
        let duration = match self {
            Self::Event { timeout_ms, .. } => *timeout_ms,
            Self::Timer { delay_ms, .. } => Some(*delay_ms),
        };
        duration
            .map(|duration| {
                let deadline = accepted_at
                    .checked_add(duration)
                    .ok_or_else(|| invalid("workflow deadline overflow"))?;
                timestamp(deadline)?;
                Ok(deadline)
            })
            .transpose()
    }
}

/// Immutable next-activation input selected under workflow authority. At an
/// event deadline, only store acceptance strictly before the deadline wins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowWake {
    Event {
        key: String,
        event: WorkflowEvent,
        accepted_at: Timestamp,
    },
    Timeout {
        key: String,
        deadline: Timestamp,
    },
    Timer {
        key: String,
        deadline: Timestamp,
    },
}
impl WorkflowWake {
    pub fn key(&self) -> &str {
        match self {
            Self::Event { key, .. } | Self::Timeout { key, .. } | Self::Timer { key, .. } => key,
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(self.key(), 128)?;
        match self {
            Self::Event {
                event, accepted_at, ..
            } => {
                event.validate()?;
                timestamp(*accepted_at)
            }
            Self::Timeout { deadline, .. } | Self::Timer { deadline, .. } => timestamp(*deadline),
        }
    }
}

fn timestamp(at: Timestamp) -> Result<()> {
    if at > MAX_TIMESTAMP {
        Err(invalid("workflow timestamp exceeds supported range"))
    } else {
        Ok(())
    }
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn bounded(value: &impl Serialize, bytes: usize, label: &str) -> Result<()> {
    crate::submission::check_encoded_size(value, bytes, label).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn event() -> Value {
        json!({"specversion":"1.0","id":"evt_1","source":"urn:billing","type":"invoice.paid","datacontenttype":"application/json","data":{"invoice_id":"INV-1042"}})
    }
    #[test]
    fn external_event_preserves_json_without_fabricated_invocation_ids() {
        let mut value = event();
        value["traceparent"] = json!("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        value["businessid"] = json!("INV-1042");
        let event = WorkflowEvent::new(value.clone()).unwrap();
        assert_eq!(event.value(), &value);
        assert!(event.trace_context().is_some());
        assert!(ledgence_worker_api::CloudEvent::new(value).is_err());
    }
    #[test]
    fn event_profile_rejects_bad_context_and_invalid_payload() {
        for (key, bad) in [
            ("specversion", json!("2.0")),
            ("source", json!("has a space")),
            ("id", json!("")),
            ("datacontenttype", json!("text/plain")),
            ("traceparent", json!("invalid")),
            ("time", json!("yesterday")),
            ("subject", Value::Null),
            ("InvalidName", json!("x")),
            ("dataschema", json!("relative")),
            ("tracestate", json!("a=b")),
        ] {
            let mut value = event();
            value[key] = bad;
            assert!(WorkflowEvent::new(value).is_err(), "{key}");
        }
        let mut missing = event();
        missing.as_object_mut().unwrap().remove("data");
        assert!(WorkflowEvent::new(missing).is_err());
        let mut large = event();
        large["data"] = json!("x".repeat(WORKFLOW_EVENT_MAX_BYTES));
        assert!(WorkflowEvent::new(large).is_err());
        let mut deep = Value::Null;
        for _ in 0..65 {
            deep = json!([deep]);
        }
        let mut value = event();
        value["data"] = deep;
        assert!(WorkflowEvent::new(value).is_err());
    }
    #[test]
    fn wait_shape_duration_and_timestamp_boundaries_are_strict() {
        let zero = WorkflowWait::Timer {
            key: "sleep".into(),
            delay_ms: 0,
        };
        assert_eq!(zero.deadline(42).unwrap(), Some(42));
        let maximum = WorkflowWait::Event {
            key: "approval".into(),
            timeout_ms: Some(WORKFLOW_MAX_DELAY_MS),
        };
        assert_eq!(
            maximum.deadline(42).unwrap(),
            Some(42 + WORKFLOW_MAX_DELAY_MS)
        );
        assert!(maximum.deadline(MAX_TIMESTAMP).is_err());
        assert!(serde_json::from_value::<WorkflowWait>(json!({"kind":"event","key":"a"})).is_err());
        for value in [
            json!({"kind":"timer","key":"a","delay_ms":true}),
            json!({"kind":"timer","key":"a","delay_ms":-1}),
            json!({"kind":"event","key":"a","timeout_ms":null,"extra":1}),
        ] {
            assert!(serde_json::from_value::<WorkflowWait>(value).is_err());
        }
        assert!(
            WorkflowWait::Timer {
                key: "a".into(),
                delay_ms: WORKFLOW_MAX_DELAY_MS + 1
            }
            .validate()
            .is_err()
        );
        assert_eq!(
            WorkflowWait::Event {
                key: "a".into(),
                timeout_ms: None
            }
            .deadline(42)
            .unwrap(),
            None
        );
    }
    #[test]
    fn receipt_identity_binds_scope_workflow_key_and_source_id() {
        let command = WorkflowEventCommand {
            scope: Scope {
                tenant_id: "t".into(),
                namespace: "n".into(),
            },
            workflow_id: "wf_1".into(),
            key: "approval".into(),
            event: WorkflowEvent::new(event()).unwrap(),
        };
        command.validate().unwrap();
        let receipt = WorkflowEventReceipt {
            scope: command.scope.clone(),
            workflow_id: command.workflow_id.clone(),
            key: command.key.clone(),
            event_id: command.event.id().into(),
            event_source: command.event.source().into(),
            accepted_at: 42,
            already_accepted: false,
        };
        receipt.validate().unwrap();
        assert!(receipt.matches(&command));
        for variant in 0..6 {
            let mut changed = receipt.clone();
            match variant {
                0 => changed.scope.tenant_id.push('x'),
                1 => changed.scope.namespace.push('x'),
                2 => changed.workflow_id.push('x'),
                3 => changed.key.push('x'),
                4 => changed.event_id.push('x'),
                _ => changed.event_source.push('x'),
            };
            assert!(!changed.matches(&command));
        }
    }
    #[test]
    fn frozen_wake_shares_input_budget_and_does_not_change_legacy_contexts() {
        let base = json!({"v":1,"workflow_id":"wf_1","activation_id":"task_1","revision":0,
            "continuation":"after","state":null,"inputs":{},"local_steps":[]});
        let mut context: WorkflowActivationContext = serde_json::from_value(base.clone()).unwrap();
        context.validate().unwrap();
        assert_eq!(serde_json::to_value(&context).unwrap(), base);
        context.wake = Some(WorkflowWake::Event {
            key: "approval".into(),
            event: WorkflowEvent::new(event()).unwrap(),
            accepted_at: 42,
        });
        context.validate().unwrap();
        let frozen = serde_json::to_value(&context).unwrap();
        assert_eq!(
            frozen["wake"]["event"]["data"]["invoice_id"],
            json!("INV-1042")
        );
        for bad in [
            json!({"kind":"timeout","key":"a"}),
            json!({"kind":"timer","key":"a","deadline":42,"extra":true}),
            json!({"kind":"event","key":"a","event":null,"accepted_at":42}),
        ] {
            let mut value = base.clone();
            value["wake"] = bad;
            assert!(
                serde_json::from_value::<WorkflowActivationContext>(value.clone()).is_err()
                    || serde_json::from_value::<WorkflowActivationContext>(value)
                        .unwrap()
                        .validate()
                        .is_err()
            );
        }
        context.inputs.insert(
            "large".into(),
            WorkflowChildResult {
                task_id: "child_1".into(),
                state: TaskState::Succeeded,
                outcome: TaskOutcome::Succeeded {
                    output: json!("x".repeat(WORKFLOW_INPUTS_MAX_BYTES - 512)),
                    attempt_id: "att_1".into(),
                    execution_may_have_started: true,
                    quiescence: Quiescence::Confirmed,
                },
            },
        );
        context.wake = None;
        context.validate().unwrap();
        let mut value = event();
        value["data"] = json!("x".repeat(1024));
        context.wake = Some(WorkflowWake::Event {
            key: "approval".into(),
            event: WorkflowEvent::new(value).unwrap(),
            accepted_at: 42,
        });
        assert!(context.validate().is_err());
    }
    #[test]
    fn external_wait_decision_is_explicit_and_preserves_child_wait_semantics() {
        let value = json!({"v":1,"activation_id":"task_1","revision":0,"kind":"wait",
            "continuation":"after","state":null,"commands":[],
            "wait":{"kind":"event","key":"approval","timeout_ms":null}});
        assert!(WorkflowDecision::decode(&value).is_ok());
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("wait");
        assert!(WorkflowDecision::decode(&missing).is_err());
        let mut extra = value;
        extra["until"] = json!([]);
        assert!(WorkflowDecision::decode(&extra).is_err());
    }
    #[test]
    fn event_timestamps_use_the_same_strict_profile_as_python_wakes() {
        for at in [
            "2024-02-29T12:00:00Z",
            "2024-02-29t12:00:00z",
            "2024-01-31T23:59:60Z",
            "2024-02-01T00:59:60+01:00",
            "0000-01-01T00:00:00Z",
        ] {
            let mut value = event();
            value["time"] = json!(at);
            assert!(WorkflowEvent::new(value).is_ok(), "{at}");
        }
        for at in [
            "2024-02-29 12:00:00Z",
            "2024-02-29x12:00:00Z",
            "2024-01-15T12:00:60Z",
            "2024-01-31T23:59:60+01:00",
        ] {
            let mut value = event();
            value["time"] = json!(at);
            assert!(WorkflowEvent::new(value).is_err(), "{at}");
        }
    }
}
