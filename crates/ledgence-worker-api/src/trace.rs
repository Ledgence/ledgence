//! Portable W3C carriers and the narrow bridge used by optional tracing adapters.
use crate::{CloudEvent, Result};
use serde::{Deserialize, Serialize};

/// Origin and processing contexts have the same wire format but different lifetimes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceContext {
    pub traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}
impl TraceContext {
    pub fn validate(&self) -> Result<()> {
        crate::validate_traceparent(&self.traceparent)?;
        if let Some(state) = &self.tracestate {
            crate::validate_tracestate(state)?;
        }
        Ok(())
    }

    /// The immutable event creation context, never an active execution context.
    pub fn from_event(event: &CloudEvent) -> Option<Self> {
        event.traceparent().map(|parent| Self {
            traceparent: parent.to_owned(),
            tracestate: event
                .value()
                .get("tracestate")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
        })
    }
}

/// Connect existing instrumentation to a context provider without exposing its SDK.
/// Set a parent and links before reading/materializing a span's context. `None`
/// explicitly starts a root, independently of any ambient transport span.
/// Hooks must be nonblocking and nonpanicking; returned carriers must validate.
/// Never perform export I/O here: allocation hooks may run inside a transaction.
pub trait TraceBridge: Send + Sync {
    fn set_parent(&self, span: &tracing::Span, parent: Option<&TraceContext>);
    fn add_link(&self, span: &tracing::Span, context: &TraceContext);
    fn context(&self, span: &tracing::Span) -> Option<TraceContext>;
}

/// Disabled tracing retains ordinary diagnostic spans and creates no trace IDs.
#[derive(Debug, Default)]
pub struct NoopTraceBridge;
impl TraceBridge for NoopTraceBridge {
    fn set_parent(&self, _: &tracing::Span, _: Option<&TraceContext>) {}
    fn add_link(&self, _: &tracing::Span, _: &TraceContext) {}
    fn context(&self, _: &tracing::Span) -> Option<TraceContext> {
        None
    }
}
