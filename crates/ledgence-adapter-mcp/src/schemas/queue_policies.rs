use super::*;

pub(super) fn definitions() -> Vec<ToolDefinition> {
    let reason = json!({"type":"string","minLength":1,"maxLength":1024});
    let revision = json!({"type":"string","pattern":"^[1-9][0-9]{0,19}$","description":"Unsigned decimal revision from inspection, transmitted as a string."});
    let wait = object(json!({"mode":{"const":"confirm"}}), &["mode"]);
    let grace = object(
        json!({"mode":{"const":"after_grace"},"grace_ms":{"type":"integer","minimum":1,"maximum":2147483647}}),
        &["mode", "grace_ms"],
    );
    let timeout = object(
        json!({"after_ms":{"type":"integer","minimum":1,"maximum":2147483647},"release":{"oneOf":[wait,grace]}}),
        &["after_ms", "release"],
    );
    let policy = object(
        json!({"max_concurrency":{"anyOf":[{"type":"integer","minimum":0,"maximum":1000000},{"type":"null"}]},
        "timeout":{"anyOf":[timeout,{"type":"null"}]}}),
        &["max_concurrency", "timeout"],
    );
    let queue = identifier();
    let position = json!({"type":["string","null"]});
    let limit = json!({"type":"integer","minimum":1,"maximum":100,"default":50});
    vec![
        tool(
            "ledgence_queue_policy_list",
            "List queue concurrency policies in the configured scope. Occupancy is shared across workers.",
            object(json!({"after":position,"limit":limit}), &[]),
            true,
            false,
        ),
        tool(
            "ledgence_queue_policy_inspect",
            "Inspect a queue policy, its revision, occupied and uncertain slots. Requires metadata_read.",
            object(json!({"queue":queue}), &["queue"]),
            true,
            false,
        ),
        tool(
            "ledgence_queue_holds",
            "Inspect the durable attempt-level capacity holds for a queue. Defaults to outstanding holds. Requires audited sensitive_read.",
            object(
                json!({"queue":queue,"after":position,"limit":limit,"include_released":{"type":"boolean","default":false}}),
                &["queue"],
            ),
            true,
            false,
        ),
        tool(
            "ledgence_queue_policy_put",
            "Create or revise a global queue cap. Zero pauses new attempts; null removes the maximum. A timeout requests stop. An explicit after_grace policy permits more work even if stop is unconfirmed. Requires resource_control and process opt-in. Preserve all arguments on lost reply.",
            object(
                json!({"queue":queue,"request_id":identifier(),"expected_revision":{"anyOf":[revision,{"type":"null"}]},"policy":policy,"reason":reason}),
                &[
                    "queue",
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
            "ledgence_queue_hold_release",
            "Release one inspected uncertain hold with an operator reason. This can admit another attempt before the old one has stopped. Requires resource_control and process opt-in. Retry only the original command.",
            object(
                json!({"queue":queue,"task_id":identifier(),"attempt_id":identifier(),"expected_revision":revision,"request_id":identifier(),"reason":reason}),
                &[
                    "queue",
                    "task_id",
                    "attempt_id",
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
