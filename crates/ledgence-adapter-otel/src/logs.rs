use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{
        FmtContext, FormatEvent, FormatFields,
        format::{Format, Json, Writer},
    },
    registry::LookupSpan,
};

pub(crate) type DispatchSlot =
    std::sync::Arc<std::sync::OnceLock<tracing::dispatcher::WeakDispatch>>;
pub(crate) struct CaptureDispatch(pub DispatchSlot);
impl<S: Subscriber> tracing_subscriber::Layer<S> for CaptureDispatch {
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        let _ = self.0.set(dispatch.downgrade());
    }
}
/// Standard tracing JSON with active context kept separate from event origins.
pub(crate) struct CorrelatedJson(pub Format<Json>, pub DispatchSlot);
impl<S, N> FormatEvent<S, N> for CorrelatedJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Parentless Python records already carry their immutable snapshot.
        let context = if event.is_root() {
            None
        } else {
            let id = event
                .parent()
                .cloned()
                .or_else(|| ctx.lookup_current().map(|span| span.id()));
            id.and_then(|id| {
                self.1
                    .get()
                    .and_then(tracing::dispatcher::WeakDispatch::upgrade)
                    .and_then(|dispatch| crate::bridge::context_for_id(&id, &dispatch))
            })
        };
        let Some(context) = context else {
            return self.0.format_event(ctx, writer, event);
        };
        // Stream directly to the caller's bounded sink. Only delay the final
        // two characters (the JSON closing brace/newline), never a whole event.
        let tail = {
            let mut streaming = SuffixWriter {
                writer: &mut writer,
                tail: String::new(),
            };
            self.0
                .format_event(ctx, Writer::new(&mut streaming), event)?;
            streaming.tail
        };
        if tail == "}\n" {
            let trace_id = &context.traceparent[3..35];
            let span_id = &context.traceparent[36..52];
            writeln!(
                writer,
                ",\"trace_id\":\"{trace_id}\",\"span_id\":\"{span_id}\"}}"
            )
        } else {
            // Preserve a changed upstream formatter's output rather than
            // generating invalid JSON by assuming a different suffix.
            writer.write_str(&tail)
        }
    }
}

struct SuffixWriter<'a, 'writer> {
    writer: &'a mut Writer<'writer>,
    tail: String,
}
impl fmt::Write for SuffixWriter<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if let Some((split, _)) = text.char_indices().rev().nth(1) {
            // There are at least two complete UTF-8 characters in this write.
            self.writer.write_str(&self.tail)?;
            self.writer.write_str(&text[..split])?;
            self.tail.clear();
            self.tail.push_str(&text[split..]);
        } else {
            // A short write can add at most one character to our two-char tail.
            self.tail.push_str(text);
            if let Some((split, _)) = self.tail.char_indices().rev().nth(1) {
                self.writer.write_str(&self.tail[..split])?;
                self.tail.drain(..split);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write;
    #[test]
    fn suffix_buffer_remains_small_across_unicode_and_large_writes() {
        let mut output = String::new();
        let mut writer = Writer::new(&mut output);
        let mut streaming = SuffixWriter {
            writer: &mut writer,
            tail: String::new(),
        };
        let large = "λ".repeat(1_000_000);
        for chunk in ["{\"", "x", "\":\"", "🙂", large.as_str(), "\"", "}", "\n"] {
            streaming.write_str(chunk).unwrap();
            assert!(streaming.tail.len() <= 8);
            assert!(streaming.tail.capacity() <= 16);
        }
        assert_eq!(streaming.tail, "}\n");
        drop(streaming);
        assert_eq!(output, format!("{{\"x\":\"🙂{large}\""));
    }
}
