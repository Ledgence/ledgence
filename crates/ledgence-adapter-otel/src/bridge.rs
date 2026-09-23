use ledgence_worker_api::{TraceBridge, TraceContext};
use opentelemetry::{
    Context,
    propagation::{Extractor, TextMapPropagator},
    trace::{SpanContext, TraceContextExt},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::{OpenTelemetrySpanExt, get_otel_context};
use tracing_subscriber::{Registry, registry::LookupSpan};

/// Context conversion for the independently installed Ledgence telemetry subscriber.
#[derive(Debug, Default)]
pub struct OtelTraceBridge;
impl TraceBridge for OtelTraceBridge {
    fn set_parent(&self, span: &tracing::Span, parent: Option<&TraceContext>) {
        let context = parent
            .filter(|value| value.validate().is_ok())
            .map(extract)
            .unwrap_or_default();
        if let Err(error) = span.set_parent(context) {
            // This is a programming diagnostic, never an execution failure.
            static REPORTED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if !span.is_disabled() && !REPORTED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(target: "ledgence::telemetry", parent: None, error = %error, "trace parent could not be set before span activation");
            }
        }
    }
    fn add_link(&self, span: &tracing::Span, context: &TraceContext) {
        if context.validate().is_ok() {
            span.add_link(extract(context).span().span_context().clone());
        }
    }
    fn context(&self, span: &tracing::Span) -> Option<TraceContext> {
        span.with_subscriber(|(id, dispatch)| context_for_id(id, dispatch))
            .flatten()
    }
}

pub(crate) fn context_for_id(
    id: &tracing::span::Id,
    dispatch: &tracing::Dispatch,
) -> Option<TraceContext> {
    // Diagnostic-only scopes do not create spans, but may enclose exported work.
    if let Some(registry) = dispatch.downcast_ref::<Registry>()
        && let Some(span) = registry.span(id)
    {
        for ancestor in span.scope() {
            if let Some(context) = get_otel_context(&ancestor.id(), dispatch)
                && let Some(context) = portable(context.span().span_context())
            {
                return Some(context);
            }
        }
    }
    get_otel_context(id, dispatch).and_then(|context| portable(context.span().span_context()))
}

fn portable(context: &SpanContext) -> Option<TraceContext> {
    context.is_valid().then(|| TraceContext {
        traceparent: format!(
            "00-{}-{}-{:02x}",
            context.trace_id(),
            context.span_id(),
            context.trace_flags().to_u8() & 1
        ),
        tracestate: (!context.trace_state().header().is_empty())
            .then(|| context.trace_state().header()),
    })
}
fn extract(carrier: &TraceContext) -> Context {
    struct Carrier<'a>(&'a TraceContext);
    impl Extractor for Carrier<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            match key {
                "traceparent" => Some(&self.0.traceparent),
                "tracestate" => self.0.tracestate.as_deref(),
                _ => None,
            }
        }
        fn keys(&self) -> Vec<&str> {
            vec!["traceparent", "tracestate"]
        }
    }
    TraceContextPropagator::new().extract_with_context(&Context::new(), &Carrier(carrier))
}
