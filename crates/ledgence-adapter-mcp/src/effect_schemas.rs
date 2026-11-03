//! Schemas name platform integer projections without constraining opaque application JSON.
use serde_json::{Value, json};
fn decimal() -> Value {
    json!({"type":"string","pattern":"^(0|[1-9][0-9]{0,19})$","description":"Canonical decimal u64; do not coerce through a floating-point number."})
}
fn nullable_decimal() -> Value {
    json!({"anyOf":[decimal(),{"type":"null"}]})
}
fn reference() -> Value {
    json!({"type":"object","properties":{"sequence":decimal(),"accepted_revision":decimal(),"basis_revision":decimal(),"accepted_at":{"type":"integer"}}})
}
fn provider_reference() -> Value {
    json!({"type":"object","properties":{"sequence":decimal(),"recorded_revision":decimal(),"recorded_at":{"type":"integer"}}})
}
fn provider_summary() -> Value {
    json!({"type":"object","properties":{"candidate_revision":nullable_decimal(),"selected_report":{"anyOf":[provider_reference(),{"type":"null"}]},"conflicting_reports":{"type":"array","maxItems":2,"items":provider_reference()},"reconciliation":{"type":"object","properties":{"generation":decimal(),"checks_used":{"type":"integer"},"max_checks":{"type":"integer"}}}}})
}
fn entry() -> Value {
    json!({"type":"object","properties":{"sequence":decimal(),"recorded_revision":decimal(),"basis_revision":decimal(),"recorded_at":{"type":"integer"},"generation":decimal(),"source":{"type":"object","properties":{"generation":decimal()}},"completion":{"type":"object","properties":{"report":provider_reference()}},"report":{},"output":{},"assessment":{}}})
}
fn summary() -> Value {
    json!({"type":"object","properties":{"entry_count":decimal(),"effect_revision":decimal(),"created_at":{"type":"integer"},"updated_at":{"type":"integer"},"provider":provider_summary(),"operator_resolution":{"type":"object","properties":{"reference":reference(),"first_conflicting_entry_sequence":nullable_decimal()}}}})
}
pub(super) fn output(name: &str) -> Option<Value> {
    Some(match name {
        "ledgence_effect_inbox" => {
            json!({"type":"object","required":["items","next_cursor"],"properties":{"items":{"type":"array","items":summary()},"next_cursor":{"type":["string","null"]}}})
        }
        "ledgence_effect_inspect" => {
            json!({"type":"object","required":["binding","summary","entries","next_cursor"],"properties":{"binding":{"type":"object","properties":{"approval":{"type":"object","properties":{"revision":decimal(),"provider_binding":{"type":"object","properties":{"program":{"type":"object","properties":{"size":decimal()}}}}}},"program":{"type":"object","properties":{"size":decimal()}}}},"summary":summary(),"entries":{"type":"array","items":entry()},"current_assessment":entry(),"provider":{"type":"object","properties":{"current_report":{"anyOf":[entry(),{"type":"null"}]}}},"next_cursor":{"type":["string","null"]}}})
        }
        "ledgence_effect_resolve" => {
            json!({"type":"object","required":["occurrence","resolution_id","accepted_at","accepted_revision","basis_revision","supersedes_resolution_id","assessment_kind","acceptance_event_id","already_accepted"],"properties":{"accepted_revision":decimal(),"basis_revision":decimal(),"accepted_at":{"type":"integer"},"already_accepted":{"type":"boolean"}}})
        }
        _ => return None,
    })
}
