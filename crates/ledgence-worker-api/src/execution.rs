//! Portable invocation requests and observations shared by workers and delivery adapters.
//!
//! A report describes local execution; it does not certify durable orchestration
//! acceptance or exactly-once application effects.

use crate::{
    CloudEvent, Digest, Error, InvocationIdentity, ProgramDescriptor, ProgramOutcome, ProgramRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub descriptor: ProgramDescriptor,
    pub event: CloudEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    #[serde(flatten)]
    pub identity: InvocationIdentity,
    pub program: ProgramRef,
    pub digest: Digest,
}
impl From<&ExecutionRequest> for ExecutionContext {
    fn from(request: &ExecutionRequest) -> Self {
        Self {
            identity: InvocationIdentity::from(&request.event),
            program: request.descriptor.program.clone(),
            digest: request.descriptor.digest.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observations: Option<Box<crate::InvocationObservations>>,
    #[serde(flatten)]
    pub context: Box<ExecutionContext>,
    pub process_id: u32,
    pub reused_process: bool,
    pub outcome: ProgramOutcome,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Admission,
    Preparation,
    Startup,
    Execution,
    Cleanup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionFailure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observations: Option<Box<crate::InvocationObservations>>,
    #[serde(flatten)]
    pub context: Box<ExecutionContext>,
    pub error: Error,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_error: Option<Error>,
    pub phase: Phase,
    /// Conservative: a lost response must not be retried inside the runtime.
    pub execution_may_have_started: bool,
}
impl std::fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.phase, self.error)
    }
}
impl std::error::Error for ExecutionFailure {}
pub type ExecutionResult = std::result::Result<ExecutionReport, ExecutionFailure>;

/// Ephemeral runtime input. The carrier belongs to the current execution span;
/// it does not modify the durable request or its immutable CloudEvent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeInvocation {
    pub event: CloudEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_context: Option<crate::TraceContext>,
    /// Optional platform context, separate from the wholly user-owned event data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension: Option<RuntimeExtension>,
}
impl From<CloudEvent> for RuntimeInvocation {
    fn from(event: CloudEvent) -> Self {
        Self {
            event,
            processing_context: None,
            extension: None,
        }
    }
}

/// Capabilities negotiated by the host for this invocation, outside persisted
/// workflow context. The default preserves older runtime and service protocols.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub approved_effect_evidence_v2: bool,
}

/// Extensible features established by connection discovery, separate from the
/// original strict capability object understood by older Python helpers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFeatures {
    pub v: u8,
    pub supported: Vec<String>,
}
pub const EXECUTION_ARTIFACTS_FEATURE: &str = "execution_artifacts.v1";
pub const NATIVE_EFFECT_RESOLUTION_FEATURE: &str = "native_effect_resolution.v1";
pub const PROVIDER_OPERATION_FEATURE: &str = "provider_operation.v1";
pub const PROVIDER_ADMISSION_FEATURE: &str = "provider_admission.v1";
pub const TASK_ADMISSION_FEATURE: &str = "task_admission.v1";
pub const RESOURCE_ADMISSION_FEATURE: &str = "resource_admission.v1";
impl Default for RuntimeFeatures {
    fn default() -> Self {
        Self {
            v: 1,
            supported: Vec::new(),
        }
    }
}
impl RuntimeFeatures {
    pub fn validate(&self) -> crate::Result<()> {
        let mut names = std::collections::BTreeSet::new();
        if self.v != 1
            || self.supported.len() > 16
            || self.supported.iter().any(|name| {
                name.is_empty()
                    || name.len() > 128
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.-".contains(&byte)
                    })
                    || !names.insert(name)
            })
            || serde_json::to_vec(self).map_or(true, |bytes| bytes.len() > 4096)
            || (self.provider_operation() && !self.native_effect_resolution())
            || (self.provider_admission()
                && (!self.provider_operation() || !self.resource_admission()))
        {
            return Err(crate::Error::new(
                crate::ErrorKind::Protocol,
                "invalid runtime feature manifest",
            ));
        }
        Ok(())
    }
    pub fn execution_artifacts(&self) -> bool {
        self.supported
            .iter()
            .any(|name| name == EXECUTION_ARTIFACTS_FEATURE)
    }
    pub fn native_effect_resolution(&self) -> bool {
        self.supported
            .iter()
            .any(|name| name == NATIVE_EFFECT_RESOLUTION_FEATURE)
    }
    pub fn task_admission(&self) -> bool {
        self.supported
            .iter()
            .any(|name| name == TASK_ADMISSION_FEATURE)
    }
    pub fn provider_admission(&self) -> bool {
        self.supported
            .iter()
            .any(|name| name == PROVIDER_ADMISSION_FEATURE)
    }
    pub fn resource_admission(&self) -> bool {
        self.supported
            .iter()
            .any(|name| name == RESOURCE_ADMISSION_FEATURE)
    }
    pub fn provider_operation(&self) -> bool {
        self.supported
            .iter()
            .any(|name| name == PROVIDER_OPERATION_FEATURE)
    }
}

/// Versioned, runtime-neutral context for an opt-in interactive invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeExtension {
    pub schema: String,
    pub payload: Value,
}
impl RuntimeExtension {
    pub fn validate(&self) -> crate::Result<()> {
        validate_name(&self.schema, "runtime extension schema")?;
        crate::validate_runtime_payload(&self.payload, crate::RUNTIME_EXTENSION_MAX_BYTES)
    }
}

/// One invocation-scoped request. The host, not these fields, supplies authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRequest {
    pub id: u64,
    pub operation: String,
    pub payload: Value,
}
impl RuntimeRequest {
    pub fn validate(&self) -> crate::Result<()> {
        if self.id == 0 {
            return Err(Error::new(
                crate::ErrorKind::InvalidInput,
                "runtime request ID must be positive",
            ));
        }
        validate_name(&self.operation, "runtime request operation")?;
        crate::validate_runtime_payload(&self.payload, crate::RUNTIME_REQUEST_MAX_BYTES)
    }
}

/// An acknowledgement for exactly one request; its result schema belongs to the handler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeReply {
    pub id: u64,
    pub result: Value,
}

fn validate_name(value: &str, label: &str) -> crate::Result<()> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(Error::new(
            crate::ErrorKind::InvalidInput,
            format!("{label} must contain 1..=256 bytes without control characters"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod feature_tests {
    use super::*;
    #[test]
    fn versioned_features_are_bounded_and_extensible() {
        let valid = RuntimeFeatures {
            v: 1,
            supported: vec![
                NATIVE_EFFECT_RESOLUTION_FEATURE.into(),
                "future_feature.v2".into(),
            ],
        };
        valid.validate().unwrap();
        assert!(valid.native_effect_resolution());
        for names in [
            vec!["".into()],
            vec!["UPPER".into()],
            vec!["bad/name".into()],
            vec!["x".repeat(129)],
            vec!["same".into(), "same".into()],
            (0..17).map(|i| format!("feature{i}")).collect(),
        ] {
            assert!(
                RuntimeFeatures {
                    v: 1,
                    supported: names
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            RuntimeFeatures {
                v: 2,
                ..RuntimeFeatures::default()
            }
            .validate()
            .is_err()
        );
        for value in [
            serde_json::json!(null),
            serde_json::json!({"v":true,"supported":[]}),
            serde_json::json!({"v":1,"supported":null}),
            serde_json::json!({"v":1,"supported":[],"unknown":true}),
        ] {
            assert!(serde_json::from_value::<RuntimeFeatures>(value).is_err());
        }
    }

    #[test]
    fn provider_operations_require_native_resolution_without_changing_legacy_features() {
        let mut features = RuntimeFeatures {
            v: 1,
            supported: vec![PROVIDER_OPERATION_FEATURE.into()],
        };
        assert!(features.validate().is_err());
        features
            .supported
            .push(NATIVE_EFFECT_RESOLUTION_FEATURE.into());
        features.validate().unwrap();
        assert!(features.provider_operation());
        RuntimeFeatures::default().validate().unwrap();
    }
}

#[cfg(test)]
mod provider_admission_tests {
    use super::*;
    #[test]
    fn composition_is_explicit_and_requires_both_capabilities() {
        let mut features = RuntimeFeatures {
            v: 1,
            supported: vec![
                NATIVE_EFFECT_RESOLUTION_FEATURE.into(),
                PROVIDER_OPERATION_FEATURE.into(),
                RESOURCE_ADMISSION_FEATURE.into(),
            ],
        };
        features.validate().unwrap();
        assert!(!features.provider_admission());
        features.supported.push(PROVIDER_ADMISSION_FEATURE.into());
        features.validate().unwrap();
        for missing in [
            PROVIDER_OPERATION_FEATURE,
            RESOURCE_ADMISSION_FEATURE,
            NATIVE_EFFECT_RESOLUTION_FEATURE,
        ] {
            let mut incomplete = features.clone();
            incomplete.supported.retain(|f| f != missing);
            assert!(incomplete.validate().is_err());
        }
    }
}
