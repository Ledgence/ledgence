//! Lossless MCP representations of platform counters. Application JSON is opaque.
use ledgence_orchestration_api::*;
use serde_json::Value;

fn decimal(value: &mut Value, names: &[&str]) {
    for name in names {
        if let Some(number) = value.get(*name).and_then(Value::as_u64) {
            value[*name] = Value::String(number.to_string());
        }
    }
}
fn reference(value: &mut Value) {
    decimal(value, &["sequence", "accepted_revision", "basis_revision"]);
}
fn summary(value: &mut Value) {
    decimal(value, &["entry_count", "effect_revision"]);
    if let Some(resolution) = value.get_mut("operator_resolution") {
        decimal(resolution, &["first_conflicting_entry_sequence"]);
        reference(&mut resolution["reference"]);
    }
}
fn entry(value: &mut Value) {
    decimal(value, &["sequence", "recorded_revision", "basis_revision"]);
}
pub(super) fn inbox(page: EffectInboxPage) -> serde_json::Result<Value> {
    let mut value = serde_json::to_value(page)?;
    for item in value["items"].as_array_mut().expect("typed items") {
        summary(item);
    }
    Ok(value)
}
pub(super) fn inspect(detail: EffectEvidenceDetail) -> serde_json::Result<Value> {
    let mut value = serde_json::to_value(detail)?;
    summary(&mut value["summary"]);
    decimal(&mut value["binding"]["approval"], &["revision"]);
    decimal(&mut value["binding"]["program"], &["size"]);
    for item in value["entries"].as_array_mut().expect("typed entries") {
        entry(item);
    }
    if let Some(assessment) = value.get_mut("current_assessment") {
        entry(assessment);
    }
    Ok(value)
}
pub(super) fn receipt(receipt: EffectResolutionReceipt) -> serde_json::Result<Value> {
    let mut value = serde_json::to_value(receipt)?;
    decimal(&mut value, &["accepted_revision", "basis_revision"]);
    Ok(value)
}
pub(super) fn parse_revision(value: &str) -> Result<u64> {
    let number = value.parse::<u64>().map_err(|_| invalid())?;
    if number == 0 || number == u64::MAX || number.to_string() != value {
        return Err(invalid());
    }
    Ok(number)
}
fn invalid() -> ContractError {
    ContractError::InvalidInput(
        "expected_effect_revision must be a canonical positive decimal u64 string below u64::MAX"
            .into(),
    )
}
