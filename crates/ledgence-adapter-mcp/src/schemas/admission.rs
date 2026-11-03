use super::*;
fn integer(min: u64, max: u64) -> Value {
    json!({"type":"integer","minimum":min,"maximum":max})
}
pub(super) fn definitions() -> Vec<ToolDefinition> {
    let revision = integer(1, 9_007_199_254_740_990);
    let name = json!({"type":"string","minLength":1,"maxLength":128});
    let reason = json!({"type":"string","minLength":1,"maxLength":1024,"description":"Required operator reason, at most 1024 UTF-8 bytes."});
    let rate = object(
        json!({"numerator":integer(1,1_000_000),"period_ms":integer(1,86_400_000),"burst":integer(1,1_000_000)}),
        &["numerator", "period_ms", "burst"],
    );
    let policy = object(
        json!({"enabled":{"type":"boolean"},"max_occupancy":integer(1,1_000_000),"rate":{"anyOf":[rate,{"type":"null"}]},"offer_timeout_ms":integer(1000,300000),"start_timeout_ms":integer(100,30000)}),
        &[
            "enabled",
            "max_occupancy",
            "rate",
            "offer_timeout_ms",
            "start_timeout_ms",
        ],
    );
    vec![
        tool(
            "ledgence_resource_list",
            "List configured resource policies and occupancy in this session's fixed scope. Metadata only; unknown usage remains occupied. Follow next using after.",
            object(json!({"after":name,"limit":limit()}), &[]),
            true,
            false,
        ),
        tool(
            "ledgence_resource_inspect",
            "Inspect resource policy and offered, occupied and unknown counters. Requires metadata_read.",
            object(json!({"name":name}), &["name"]),
            true,
            false,
        ),
        tool(
            "ledgence_admission_inspect",
            "Inspect the original admission occurrence, frozen callback arguments and retained output. Requires sensitive_read and durable audit; use origin_activation_id, not the latest continuation. Start permission alone is not evidence that a remote call happened.",
            object(
                json!({"workflow_id":identifier(),"origin_activation_id":identifier(),"key":identifier()}),
                &["workflow_id", "origin_activation_id", "key"],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_resource_put",
            "Apply a resource policy with a stable caller-supplied request_id and expected_revision (null creates). Requires explicit resource-control opt-in and resource_control. Retry an uncertain response with identical arguments only. Policy updates preserve occupied/unknown usage.",
            object(
                json!({"name":name,"request_id":identifier(),"expected_revision":{"anyOf":[revision,{"type":"null"}]},"policy":policy,"reason":reason}),
                &[
                    "name",
                    "request_id",
                    "expected_revision",
                    "policy",
                    "reason",
                ],
            ),
            false,
            true,
        ),
        tool(
            "ledgence_admission_release",
            "Explicitly release uncertain occupied capacity after operator review. Requires explicit resource-control opt-in and resource_control. This does not stop remote work, prove its outcome or mark the workflow successful. Preserve inspected source_id/revision and the same request_id and arguments when reconciling a lost response.",
            object(
                json!({"workflow_id":identifier(),"origin_activation_id":identifier(),"key":identifier(),"source_id":identifier(),"expected_revision":revision,"request_id":identifier(),"reason":reason}),
                &[
                    "workflow_id",
                    "origin_activation_id",
                    "key",
                    "source_id",
                    "expected_revision",
                    "request_id",
                    "reason",
                ],
            ),
            false,
            true,
        ),
    ]
}
