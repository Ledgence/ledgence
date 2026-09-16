use crate::CloudEvent;
use serde::{Deserialize, Serialize};

/// Correlation copied from the validated envelope, never from user-owned data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_workflow_id: Option<String>,
    pub source: String,
    pub event_id: String,
    pub tenant_id: String,
    pub namespace: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_id: Option<String>,
    pub task_id: String,
    pub attempt_id: String,
    pub attempt_no: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

impl From<&CloudEvent> for InvocationIdentity {
    fn from(event: &CloudEvent) -> Self {
        Self {
            parent_workflow_id: event
                .value()
                .get("ldgparentworkflowid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            root_workflow_id: event
                .value()
                .get("ldgrootworkflowid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            source: event.string("source").to_owned(),
            event_id: event.id().to_owned(),
            tenant_id: event.tenant_id().to_owned(),
            namespace: event.namespace().to_owned(),
            run_id: event.string("ldgrunid").to_owned(),
            workflow_id: event
                .value()
                .get("ldgworkflowid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            activation_id: event
                .value()
                .get("ldgactivationid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            task_id: event.task_id().to_owned(),
            attempt_id: event.attempt_id().to_owned(),
            attempt_no: event.value()["ldgattemptno"]
                .as_u64()
                .expect("validated attempt number") as u32,
            traceparent: event.traceparent().map(str::to_owned),
            tracestate: event
                .value()
                .get("tracestate")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        }
    }
}
