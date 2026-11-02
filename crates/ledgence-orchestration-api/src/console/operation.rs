//! Explicit, on-demand reads of accepted local results. Attempt observations are separate.
use super::*;
use ledgence_worker_api::validate_wire_value;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ConsoleLocalStepLookup {
    pub workflow_id: String,
    pub activation_id: String,
    pub step_key: String,
}

impl ConsoleLocalStepLookup {
    pub fn validate(&self) -> Result<()> {
        for id in [&self.workflow_id, &self.activation_id, &self.step_key] {
            validate_text(id, 128)?;
        }
        Ok(())
    }

    pub fn validate_reply(&self, detail: &ConsoleLocalStepDetail) -> Result<()> {
        if self.workflow_id != detail.step.workflow_id
            || self.activation_id != detail.step.activation_id
            || self.step_key != detail.step.step_key
        {
            return Err(inconsistent(
                "local step inspection returned a different identity",
            ));
        }
        detail
            .validate()
            .map_err(|_| inconsistent("invalid local step inspection"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleOperationKind {
    Local,
    Model,
    Tool,
}

impl ConsoleOperationKind {
    // The Python operation helper prepends its kind to module:qualified_name.
    // Ordinary Python module names may themselves be "tool" or "model".
    fn from_callable(callable: &str) -> Option<Self> {
        let (kind, name) = callable.split_once(':')?;
        let (module, qualified_name) = name.split_once(':')?;
        if module.is_empty() || qualified_name.is_empty() {
            return None;
        }
        match kind {
            "model" => Some(Self::Model),
            "tool" => Some(Self::Tool),
            _ => None,
        }
    }

    fn version(self, input: &Value) -> Option<&str> {
        let binding = input.as_object()?;
        let kind = match self {
            Self::Model => "model",
            Self::Tool => "tool",
            Self::Local => return None,
        };
        if binding.len() != 4
            || binding.get("v")?.as_u64() != Some(1)
            || binding.get("kind")?.as_str() != Some(kind)
            || !binding.get("arguments")?.is_object()
        {
            return None;
        }
        let version = binding.get("version")?.as_str()?;
        validate_text(version, 128).ok()?;
        Some(version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleLocalStepDetail {
    pub step: ConsoleLocalStep,
    pub kind: ConsoleOperationKind,
    pub version: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub arguments: Value,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub result: Value,
}

impl ConsoleLocalStepDetail {
    /// Preserve unknown or older operation encodings as opaque local records.
    pub fn from_record(step: ConsoleLocalStep, mut record: LocalStepRecord) -> Result<Self> {
        if step.step_key != record.key || step.callable != record.callable {
            return Err(inconsistent(
                "local step metadata disagrees with the accepted record",
            ));
        }
        step.validate()?;
        record.validate()?;
        let binding = ConsoleOperationKind::from_callable(&record.callable)
            .and_then(|kind| Some((kind, kind.version(&record.input)?.to_owned())));
        let (kind, version, arguments) = match binding {
            Some((kind, version)) => (kind, Some(version), record.input["arguments"].take()),
            None => (ConsoleOperationKind::Local, None, record.input),
        };
        Ok(Self {
            step,
            kind,
            version,
            arguments,
            result: record.output,
        })
    }

    fn validate(&self) -> Result<()> {
        self.step.validate()?;
        match (self.kind, &self.version) {
            (ConsoleOperationKind::Local, None) => (),
            (kind, Some(version))
                if Some(kind) == ConsoleOperationKind::from_callable(&self.step.callable)
                    && self.arguments.is_object() =>
            {
                validate_text(version, 128)?
            }
            _ => return Err(inconsistent("invalid operation binding")),
        }
        validate_wire_value(&self.arguments)?;
        validate_wire_value(&self.result)?;
        crate::submission::check_encoded_size(
            &(&self.arguments, &self.result),
            WORKFLOW_LOCAL_RECORD_MAX_BYTES,
            "local result",
        )
        .map_err(Into::into)
    }
}
