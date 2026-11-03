//! Framework-neutral MCP tool descriptions. Execution remains in Ledgence services.
mod admission;
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
            "ledgence_audit_query",
            "Read operator audit events in the configured scope without task/workflow inputs or outputs. Resource policy and admission release events include retained operator reasons. Requires metadata_read and audited credential mode. Filter by one resource or exact approval occurrence. For event or approval_acceptance use limit 2 and no cursor. A successful empty exact result means no verified actor was recorded; an error does not establish absence. Actor attribution identifies the credential holder, including shared services. Pagination is not a cross-request snapshot.",
            object(
                json!({"filter": audit_filter(), "cursor": cursor(), "limit": limit()}),
                &[],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_effect_inbox",
            "Read retained approved-effect metadata in the configured scope, most recently updated first. Requires metadata_read. Keep filters unchanged when following next_cursor; a short page can have a cursor due to the byte limit. Reports and discrepancies do not establish provider truth or authorize another dispatch.",
            object(
                json!({"filter":{"type":"string","enum":["all","unconfirmed","confirmed","discrepant","disputed"],"default":"all"},
                    "provider_filter":{"type":"string","enum":["all","pending","needs_review","succeeded","failed","cancelled","disputed"],"description":"Provider operations only; requires filter=all. Knowledge is adapter-classified; needs_review does not authorize redispatch."},
                    "workflow_id":identifier(),"cursor":effect_cursor(),"limit":limit()}),
                &[],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_effect_inspect",
            "Read the frozen approval binding and whole retained claim/observation entries for an exact effect occurrence. Requires credential mode and sensitive_read; the server durably audits the read before returning evidence. Follow next_cursor with the same identity even on a short page. Worker reports are attributed evidence, not independent verification of an external effect. This tool cannot resolve or redispatch effects.",
            object(
                json!({"workflow_id":identifier(),"activation_id":identifier(),"approval_key":identifier(),
                    "cursor":effect_cursor(),"limit":{"type":"integer","minimum":1,"maximum":4,"default":4}}),
                &["workflow_id", "activation_id", "approval_key"],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_effect_resolve",
            "Append an authenticated operator assessment of one exact approved effect. Requires process opt-in and effect_resolve. Supply the frozen reviewed action, revision, previous assessment, reason and ordered evidence references. Replay an uncertain request only with exactly the same command and stable actor. Acceptance does not assert provider truth, confirm the journal, redispatch work or prove workflow resumption. Revision strings preserve all u64 digits.",
            object(
                json!({"workflow_id":identifier(),"activation_id":identifier(),"approval_key":identifier(),
                    "resolution_id":identifier(),"expected_effect_revision":{"type":"string","pattern":"^[1-9][0-9]{0,19}$","description":"Canonical decimal u64 less than 18446744073709551615; use the inspected effect_revision unchanged."},
                    "supersedes_resolution_id":{"anyOf":[identifier(),{"type":"null"}]},
                    "action":object(json!({"name":{"type":"string","minLength":1,"maxLength":512},"version":identifier(),"arguments":{}}), &["name","version","arguments"]),
                    "assessment":{"oneOf":[
                        object(json!({"kind":{"const":"record_success"},"result":{}}), &["kind","result"]),
                        object(json!({"kind":{"const":"record_failure"},"failure":object(json!({"kind":identifier(),"message":{"type":"string","maxLength":4096}}), &["kind","message"])}), &["kind","failure"]),
                        object(json!({"kind":{"const":"close_unknown"}}), &["kind"])
                    ]},
                    "reason":{"type":"string","minLength":1,"maxLength":4096,"description":"Nonblank, at most 4096 UTF-8 bytes; retained exactly."},
                    "evidence_references":{"type":"array","maxItems":8,"items":{"type":"string","minLength":1,"maxLength":1024},"description":"Ordered references, at most 1024 UTF-8 bytes each. Success/failure requires at least one; close_unknown may use none."}
                }),
                &[
                    "workflow_id",
                    "activation_id",
                    "approval_key",
                    "resolution_id",
                    "expected_effect_revision",
                    "supersedes_resolution_id",
                    "action",
                    "assessment",
                    "reason",
                    "evidence_references",
                ],
            ),
            false,
            true,
        ),
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
            "ledgence_approval_inbox",
            "Find approval summaries across workflows in the configured scope, earliest deadline first. Omit status for pending requests or set it to null for all statuses. Pass next_cursor with the same filters. Use ledgence_approval_inspect with the returned workflow_id and key for the effective action.",
            object(
                json!({
                    "status":{"type":["string","null"],"enum":["pending","approved","rejected","expired","cancelled",null],"default":"pending"},
                    "program_id":identifier(),
                    "due_before":{"type":["integer","null"],"minimum":0,"maximum":253402300799999_u64,"description":"Exclusive deadline cutoff in Unix epoch milliseconds (UTC)."},
                    "cursor":cursor(), "limit":limit()
                }),
                &[],
            ),
            true,
            false,
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
    tools.extend(admission::definitions());
    if read_only {
        tools.retain(|tool| tool.read_only);
    }
    tools
}

fn audit_filter() -> Value {
    json!({"oneOf": [
        object(json!({"kind":{"const":"event"},"event_id":identifier()}), &["kind","event_id"]),
        object(json!({"kind":{"const":"all"}}), &["kind"]),
        object(json!({"kind":{"const":"resource"},
            "resource_kind":{"enum":["task","workflow","resource"]}, "resource_id":identifier()}),
            &["kind","resource_kind","resource_id"]),
        object(json!({"kind":{"const":"approval_acceptance"},
            "workflow_id":identifier(), "key":identifier(), "activation_id":identifier(),
            "revision":{"type":"integer","minimum":0,"maximum":u64::MAX}, "decision_id":identifier()}),
            &["kind","workflow_id","key","activation_id","revision","decision_id"])
    ]})
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
fn effect_cursor() -> Value {
    json!({"type":"string","minLength":2,"maxLength":8192,"pattern":"^(?:[0-9a-f]{2})+$",
        "description":"Opaque next_cursor from the previous effect response; preserve it unchanged."})
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
    fn audit_catalog_covers_each_portable_resource_filter() {
        use ledgence_orchestration_api::{AuditFilter, AuditResourceKind};
        let tools = tool_definitions(true);
        let tool = tools
            .iter()
            .find(|tool| tool.name == "ledgence_audit_query")
            .unwrap();
        let variants = tool.input_schema["properties"]["filter"]["oneOf"]
            .as_array()
            .unwrap();
        let resource = variants
            .iter()
            .find(|v| v["properties"]["kind"]["const"] == "resource")
            .unwrap();
        let advertised = resource["properties"]["resource_kind"]["enum"]
            .as_array()
            .unwrap();
        for resource_kind in [
            AuditResourceKind::Task,
            AuditResourceKind::Workflow,
            AuditResourceKind::Resource,
        ] {
            let filter = AuditFilter::Resource {
                resource_kind,
                resource_id: "model".into(),
            };
            let wire = serde_json::to_value(&filter).unwrap();
            assert!(
                advertised.contains(&wire["resource_kind"]),
                "portable resource is absent from MCP discovery: {wire}"
            );
            assert_eq!(serde_json::from_value::<AuditFilter>(wire).unwrap(), filter);
        }
    }

    #[test]
    fn catalog_has_closed_argument_objects_and_no_approval_decision() {
        let all = tool_definitions(false);
        let names: std::collections::HashSet<_> = all.iter().map(|tool| tool.name).collect();
        assert_eq!(names.len(), all.len());
        assert_eq!(all.len(), 25);
        assert_eq!(tool_definitions(true).len(), 17);
        for name in [
            "ledgence_resource_list",
            "ledgence_resource_inspect",
            "ledgence_admission_inspect",
            "ledgence_resource_put",
            "ledgence_admission_release",
        ] {
            assert!(names.contains(name), "missing admission tool: {name}");
        }
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
