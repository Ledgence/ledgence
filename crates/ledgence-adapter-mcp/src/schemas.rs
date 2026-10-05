//! Framework-neutral MCP tool descriptions. Execution remains in Ledgence services.
use ledgence_orchestration_api::APPROVAL_PAGE_MAX_ITEMS;
use serde_json::{Value, json};

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
    pub read_only: bool,
    pub idempotent: bool,
    pub destructive: bool,
}

pub fn tool_definitions(read_only: bool) -> Vec<ToolDefinition> {
    let mut tools = vec![
        tool(
            "ledgence_program_list",
            "List registered programs in the configured installation. Kind is display metadata, not an execution permission. Follow next_cursor with the same filters.",
            object(
                json!({"kind": {"type":"string","enum":["task","workflow","unspecified"]}, "limit":limit(), "cursor":cursor()}),
                &[],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_program_versions",
            "List immutable registered versions of one program. Follow next_cursor with the same program.",
            object(
                json!({"program":identifier(), "limit":limit(), "cursor":cursor()}),
                &["program"],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_program_inspect",
            "Inspect an exact registered program version and its package manifest. This does not execute or download its code.",
            object(
                json!({"program":identifier(), "version":identifier()}),
                &["program", "version"],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_task_submit",
            "Submit one task and return its handle immediately. Supply a stable idempotency_key; retry an uncertain outcome only with the identical key and input. Reusing a key with changed input conflicts. Program execution may cause external effects.",
            submission(),
            false,
            true,
        ),
        tool(
            "ledgence_task_list",
            "Read one bounded page of tasks; results may include workflow activations. Preserve filters when using next_cursor. Times are UTC Unix milliseconds.",
            object(
                json!({"filters":task_filters(),"limit":limit(),"cursor":cursor()}),
                &[],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_task_status",
            "Read current task status without waiting or returning application output.",
            task_reference(),
            true,
            false,
        ),
        tool(
            "ledgence_task_result",
            "Read the current task outcome without waiting. outcome=null means pending; a succeeded outcome with output=null is a completed JSON null result.",
            task_reference(),
            true,
            false,
        ),
        tool(
            "ledgence_task_cancel",
            "Request task cancellation. Active code may still be stopping, and cancellation does not undo external effects. Repeating the request targets the same task.",
            task_reference(),
            false,
            true,
        ),
        tool(
            "ledgence_workflow_submit",
            "Submit one workflow and return its handle immediately. Supply a stable idempotency_key; retry an uncertain outcome only with identical key and input. The workflow may execute tasks or external effects.",
            submission(),
            false,
            true,
        ),
        tool(
            "ledgence_workflow_status",
            "Read the current workflow status, including its active entrypoint activation, without waiting.",
            workflow_reference(),
            true,
            false,
        ),
        tool(
            "ledgence_workflow_result",
            "Read the workflow outcome without waiting. outcome=null means pending; completed JSON null output is represented inside a succeeded outcome.",
            workflow_reference(),
            true,
            false,
        ),
        tool(
            "ledgence_workflow_cancel",
            "Request workflow cancellation, including owned children. Cancellation does not undo external effects or prove that all workers have already stopped.",
            workflow_reference(),
            false,
            true,
        ),
        tool(
            "ledgence_workflow_send_event",
            "Deliver a caller-created JSON CloudEvent to one workflow's one-shot wait key. Preserve its source, id and payload on retry. Acceptance does not mean the workflow has processed it. This cannot decide a durable approval.",
            object(
                json!({"workflow_id":identifier(),"key":identifier(),"event":cloud_event()}),
                &["workflow_id", "key", "event"],
            ),
            false,
            true,
        ),
        tool(
            "ledgence_approval_list",
            "List durable approval requests for a workflow. These are persisted observations; expiration may await coordinator processing. This server cannot approve or reject requests.",
            object(
                json!({"workflow_id":identifier(),"after_key":identifier(),"limit":{"type":"integer","minimum":1,"maximum":APPROVAL_PAGE_MAX_ITEMS,"default":APPROVAL_PAGE_MAX_ITEMS}}),
                &["workflow_id"],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_approval_inspect",
            "Inspect the frozen action and decision for one workflow approval key. This server does not provide approval decisions.",
            object(
                json!({"workflow_id":identifier(),"key":identifier()}),
                &["workflow_id", "key"],
            ),
            true,
            false,
        ),
    ];
    if read_only {
        tools.retain(|tool| tool.read_only);
    }
    tools
}

fn tool(
    name: &'static str,
    description: &'static str,
    input_schema: Value,
    read_only: bool,
    destructive: bool,
) -> ToolDefinition {
    ToolDefinition {
        name,
        description,
        input_schema,
        read_only,
        idempotent: true,
        destructive,
    }
}
fn identifier() -> Value {
    json!({"type":"string","minLength":1,"maxLength":128})
}
fn limit() -> Value {
    json!({"type":"integer","minimum":1,"maximum":100,"default":50})
}
fn cursor() -> Value {
    json!({"type":"string","minLength":1,"description":"Opaque next_cursor from the previous response; do not construct or modify."})
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn task_reference() -> Value {
    object(json!({"task_id":identifier()}), &["task_id"])
}
fn workflow_reference() -> Value {
    object(json!({"workflow_id":identifier()}), &["workflow_id"])
}
fn task_filters() -> Value {
    object(
        json!({
            "state":{"type":"string","enum":["queued","active","succeeded","failed","cancelled"]},
            "queue":identifier(), "correlation_key":{"type":"string","maxLength":512},
            "submitted_from":{"type":"integer","minimum":0,"maximum":253402300799999_u64},
            "submitted_until":{"type":"integer","minimum":0,"maximum":253402300799999_u64}
        }),
        &[],
    )
}
fn submission() -> Value {
    object(
        json!({
            "program":identifier(), "version":identifier(), "queue":identifier(),
            "data":{"description":"Application-owned JSON, at most 1 MiB and depth 64. Never put credentials in persistent inputs."},
            "idempotency_key":{"type":"string","minLength":1,"maxLength":255},
            "correlation_key":{"type":"string","maxLength":512},
            "retry_policy":object(json!({"max_attempts":{"type":"integer","minimum":1,"maximum":1000},"retry_delay_ms":{"type":"integer","minimum":0,"maximum":86400000}}), &["max_attempts","retry_delay_ms"]),
            "attempt_timeout_ms":{"type":"integer","minimum":60000,"maximum":86400000,"default":300000},
            "origin_trace":object(json!({"traceparent":{"type":"string"},"tracestate":{"type":"string"}}), &["traceparent"])
        }),
        &["program", "version", "queue", "data", "idempotency_key"],
    )
}
fn cloud_event() -> Value {
    json!({"type":"object","description":"Complete JSON CloudEvent 1.0, at most 64 KiB. Caller supplies a stable source/id; extensions are preserved.","required":["specversion","source","id","type","datacontenttype","data"],"properties":{
        "specversion":{"const":"1.0"},"source":{"type":"string","minLength":1},"id":identifier(),"type":{"type":"string","minLength":1},"datacontenttype":{"const":"application/json"},"data":{}
    }})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_has_closed_argument_objects_and_no_approval_decision() {
        let all = tool_definitions(false);
        let names: std::collections::HashSet<_> = all.iter().map(|tool| tool.name).collect();
        assert_eq!(names.len(), all.len());
        assert_eq!(all.len(), 15);
        assert!(
            all.iter()
                .all(|tool| tool.input_schema["additionalProperties"] == false)
        );
        assert!(!names.contains("ledgence_approval_decide"));
        assert!(
            tool_definitions(true)
                .iter()
                .all(|tool| tool.read_only && !tool.destructive)
        );
        for tool in all.iter().filter(|tool| tool.name.ends_with("submit")) {
            assert!(
                tool.input_schema["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("idempotency_key"))
            );
            assert!(tool.idempotent && tool.destructive);
        }
    }
}
