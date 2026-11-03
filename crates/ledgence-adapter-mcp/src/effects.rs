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
fn provider_reference(value: &mut Value) {
    decimal(value, &["sequence", "recorded_revision"]);
}
pub(super) fn approval(value: &mut Value) {
    if let Some(binding) = value.get_mut("provider_binding") {
        decimal(&mut binding["program"], &["size"]);
    }
}
pub(super) fn task(value: &mut Value) {
    if let Some(probe) = value.get_mut("provider_probe") {
        decimal(probe, &["generation"]);
    }
}
fn summary(value: &mut Value) {
    decimal(value, &["entry_count", "effect_revision"]);
    if let Some(provider) = value.get_mut("provider") {
        decimal(provider, &["candidate_revision"]);
        decimal(&mut provider["reconciliation"], &["generation"]);
        provider_reference(&mut provider["selected_report"]);
        if let Some(reports) = provider["conflicting_reports"].as_array_mut() {
            for report in reports {
                provider_reference(report);
            }
        }
    }
    if let Some(resolution) = value.get_mut("operator_resolution") {
        decimal(resolution, &["first_conflicting_entry_sequence"]);
        reference(&mut resolution["reference"]);
    }
}
fn entry(value: &mut Value) {
    decimal(value, &["sequence", "recorded_revision", "basis_revision"]);
    match value["kind"].as_str() {
        Some("provider_report") => decimal(&mut value["source"], &["generation"]),
        Some("provider_probe_scheduled" | "provider_probe_source" | "provider_probe_finished") => {
            decimal(value, &["generation"]);
            if let Some(report) = value
                .get_mut("completion")
                .and_then(|v| v.get_mut("report"))
            {
                provider_reference(report);
            }
        }
        _ => {}
    }
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
    approval(&mut value["binding"]["approval"]);
    decimal(&mut value["binding"]["program"], &["size"]);
    for item in value["entries"].as_array_mut().expect("typed entries") {
        entry(item);
    }
    if let Some(provider) = value.get_mut("provider")
        && !provider["current_report"].is_null()
    {
        entry(&mut provider["current_report"]);
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
