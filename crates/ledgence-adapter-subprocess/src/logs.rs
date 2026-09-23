//! Best-effort v2/v3 telemetry. Records carry their creation-time identity; idle or
//! late records never inherit the actor's current invocation.

use ledgence_worker_api::{CloudEvent, TraceContext, validate_wire_value};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const MAX_LOG_FRAME_BYTES: usize = 16 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRecord {
    v: u32,
    #[serde(rename = "type")]
    kind: String,
    time: String,
    severity: String,
    logger: String,
    message: String,
    attributes: Map<String, Value>,
    invocation: Option<LogInvocation>,
    trace_id: Option<String>,
    span_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogInvocation {
    event_id: String,
    attempt_id: String,
    source: String,
    tenant_id: String,
    namespace: String,
    run_id: String,
    workflow_id: Option<String>,
    parent_workflow_id: Option<String>,
    root_workflow_id: Option<String>,
    activation_id: Option<String>,
    task_id: String,
    attempt_no: i32,
}

pub(super) struct LogForwarder {
    pub pid: u32,
    digest: String,
    budget: Arc<AtomicUsize>,
    dropped: u64,
}

impl LogForwarder {
    pub fn new(pid: u32, digest: String, budget: Arc<AtomicUsize>) -> Self {
        Self {
            pid,
            digest,
            budget,
            dropped: 0,
        }
    }

    /// Return true for optional v2/v3 log data, including invalid optional content.
    /// Framing and result/control validation remain the protocol reader's job.
    pub fn accept(&mut self, value: &Value, version: u32) -> bool {
        if !matches!(version, 2 | 3) || value.get("type").and_then(Value::as_str) != Some("log") {
            return false;
        }
        if let Some(record) = validated(value, version) {
            let bytes = serde_json::to_vec(value).map_or(MAX_LOG_FRAME_BYTES + 1, |v| v.len() + 1);
            let allowance = self
                .budget
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    Some(remaining.saturating_sub(bytes))
                })
                .unwrap_or(0);
            if allowance >= bytes {
                let identity = record.invocation.as_ref();
                // The configured subscriber writes via its bounded nonblocking
                // output queue. No IPC reader waits for terminal or exporter I/O.
                macro_rules! emit {
                    ($level:expr) => {{
                    tracing::event!(target: "ledgence::program_log", parent: None, $level,
                    subprocess_pid = self.pid, artifact_digest = %self.digest,
                    stream = "structured", timestamp = %record.time,
                    severity = %record.severity, logger = %record.logger,
                    text = %record.message, attributes = %serde_json::Value::Object(record.attributes),
                    event_id = identity.map(|v| v.event_id.as_str()),
                    event_source = identity.map(|v| v.source.as_str()),
                    tenant_id = identity.map(|v| v.tenant_id.as_str()),
                    namespace = identity.map(|v| v.namespace.as_str()),
                    run_id = identity.map(|v| v.run_id.as_str()),
                    workflow_id = identity.and_then(|v| v.workflow_id.as_deref()),
                    parent_workflow_id = identity.and_then(|v| v.parent_workflow_id.as_deref()),
                    root_workflow_id = identity.and_then(|v| v.root_workflow_id.as_deref()),
                    activation_id = identity.and_then(|v| v.activation_id.as_deref()),
                    task_id = identity.map(|v| v.task_id.as_str()),
                    attempt_id = identity.map(|v| v.attempt_id.as_str()),
                    attempt_no = identity.map(|v| v.attempt_no),
                    trace_id = record.trace_id.as_deref(), span_id = record.span_id.as_deref(),
                    "program log");
                    }};
                }
                match record.severity.as_str() {
                    "CRITICAL" | "ERROR" => emit!(tracing::Level::ERROR),
                    "WARNING" | "WARN" => emit!(tracing::Level::WARN),
                    "DEBUG" => emit!(tracing::Level::DEBUG),
                    "TRACE" => emit!(tracing::Level::TRACE),
                    _ => emit!(tracing::Level::INFO),
                }
                return true;
            }
        }
        self.dropped = self.dropped.saturating_add(1);
        // At most 64 diagnostics over the lifetime of a session.
        if self.dropped.is_power_of_two() {
            tracing::warn!(target: "ledgence::program_log", parent: None,
                subprocess_pid = self.pid, artifact_digest = %self.digest,
                dropped_records = self.dropped, "optional program logs dropped");
        }
        true
    }
}

fn validated(value: &Value, version: u32) -> Option<LogRecord> {
    if serde_json::to_vec(value).ok()?.len() >= MAX_LOG_FRAME_BYTES {
        return None;
    }
    validate_wire_value(value).ok()?;
    let record: LogRecord = serde_json::from_value(value.clone()).ok()?;
    if record.v != version
        || record.kind != "log"
        || record.time.is_empty()
        || record.time.len() > 64
        || record.severity.is_empty()
        || record.severity.len() > 32
        || record.logger.len() > 1024
        || record.message.len() > MAX_LOG_FRAME_BYTES
    {
        return None;
    }
    time::OffsetDateTime::parse(&record.time, &time::format_description::well_known::Rfc3339)
        .ok()?;
    match (&record.trace_id, &record.span_id) {
        (Some(trace), Some(span)) => {
            TraceContext {
                traceparent: format!("00-{trace}-{span}-01"),
                tracestate: None,
            }
            .validate()
            .ok()?;
        }
        (None, None) => {}
        _ => return None,
    }
    if let Some(identity) = &record.invocation {
        // Reuse envelope identity validation without reading user data or looking
        // up whichever invocation happens to be active when this record arrives.
        if version == 2
            && (identity.workflow_id.is_some()
                || identity.activation_id.is_some()
                || identity.parent_workflow_id.is_some()
                || identity.root_workflow_id.is_some())
        {
            return None;
        }
        let mut event = json!({"specversion": "1.0", "type": "ledgence.log",
            "id": identity.event_id, "source": identity.source,
            "ldgtenantid": identity.tenant_id, "ldgnamespace": identity.namespace,
            "ldgrunid": identity.run_id, "ldgtaskid": identity.task_id,
            "ldgattemptid": identity.attempt_id, "ldgattemptno": identity.attempt_no,
            "datacontenttype": "application/json", "time": record.time, "data": null
        });
        for (key, value) in [
            ("ldgworkflowid", &identity.workflow_id),
            ("ldgactivationid", &identity.activation_id),
            ("ldgparentworkflowid", &identity.parent_workflow_id),
            ("ldgrootworkflowid", &identity.root_workflow_id),
        ] {
            if let Some(value) = value {
                if value.is_empty() || value.len() > 128 {
                    return None;
                }
                event[key] = Value::String(value.clone());
            }
        }
        CloudEvent::new(event).ok()?;
    }
    Some(record)
}
