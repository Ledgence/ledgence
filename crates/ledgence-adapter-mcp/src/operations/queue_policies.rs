//! Scope-bound administration of durable queue concurrency.
use super::*;

pub(super) fn is_mutation(name: &str) -> bool {
    matches!(
        name,
        "ledgence_queue_policy_put" | "ledgence_queue_hold_release"
    )
}

pub(super) fn is_tool(name: &str) -> bool {
    is_mutation(name)
        || matches!(
            name,
            "ledgence_queue_policy_inspect" | "ledgence_queue_policy_list" | "ledgence_queue_holds"
        )
}

fn scoped(scope: &Scope, arguments: &Value) -> std::result::Result<Value, ToolError> {
    let mut fields = arguments
        .as_object()
        .ok_or_else(|| ToolError::arguments("expected an object"))?
        .clone();
    if fields.contains_key("scope")
        || fields.contains_key("tenant_id")
        || fields.contains_key("namespace")
    {
        return Err(ToolError::arguments(
            "queue policy scope is fixed by the MCP server",
        ));
    }
    fields.insert("scope".into(), json!(scope));
    Ok(Value::Object(fields))
}

impl ToolDispatcher {
    pub(super) async fn call_queue_policy(
        &self,
        name: &str,
        arguments: &Value,
    ) -> std::result::Result<Value, ToolError> {
        let mut scoped = scoped(&self.scope, arguments)?;
        if matches!(name, "ledgence_queue_policy_list" | "ledgence_queue_holds") {
            let fields = scoped.as_object_mut().expect("scope-bound object");
            fields.entry("after").or_insert(Value::Null);
            fields.entry("limit").or_insert(json!(50));
            if name == "ledgence_queue_holds" {
                fields.entry("include_released").or_insert(json!(false));
            }
        }
        let client = remote(self.tasks.queue_policy_client(), false)?;
        macro_rules! call {
            ($request:ty, $method:ident, $mutation:expr) => {{
                let request: $request = parse(&scoped)?;
                request.validate().map_err(ToolError::arguments)?;
                let reply = remote(client.$method(&request).await, $mutation)?;
                reply
                    .validate_for(&request)
                    .map_err(|_| invalid_reply($mutation))?;
                encode(reply, $mutation)
            }};
        }
        match name {
            "ledgence_queue_policy_inspect" => call!(QueuePolicyQuery, inspect_queue_policy, false),
            "ledgence_queue_policy_list" => call!(QueuePolicyList, list_queue_policies, false),
            "ledgence_queue_holds" => call!(QueueHoldList, list_queue_holds, false),
            "ledgence_queue_policy_put" => call!(QueuePolicyPut, put_queue_policy, true),
            "ledgence_queue_hold_release" => call!(QueueHoldRelease, release_queue_hold, true),
            _ => unreachable!("matched queue policy tool"),
        }
    }
}
