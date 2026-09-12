use crate::{Error, ErrorKind, Result};
use serde_json::Value;

/// Maximum nested containers in a program result (a scalar has depth zero).
pub const MAX_WIRE_VALUE_DEPTH: usize = 64;

/// Maximum compact JSON data bytes in the submission/delivery profile (1 MiB).
///
/// This is an application-data budget, including JSON quotes and escapes. It
/// does not impose a size validator on arbitrary local [`crate::CloudEvent`]s.
pub const APPLICATION_INPUT_MAX_BYTES: usize = 1024 * 1024;

/// Default complete runtime frame budget, including its newline (2 MiB).
///
/// The delivery profile reserves space beyond [`APPLICATION_INPUT_MAX_BYTES`]
/// for generated CloudEvent metadata and the invocation protocol wrapper. The
/// same bounded budget applies to result frames; their application output is
/// not subject to the submission data limit. Runtime adapters may allow explicit
/// local overrides, which need not be compatible with the delivery profile.
pub const DEFAULT_RUNTIME_FRAME_MAX_BYTES: usize = 2 * 1024 * 1024;

/// Checks the structural limit shared by the Rust and Python result protocol.
///
/// `Value` already guarantees string object keys, Unicode scalar strings and
/// finite signed/unsigned 64-bit or binary64 numbers. The Python helper checks
/// those properties before encoding so unsupported results cannot be coerced.
///
/// ```
/// use ledgence_worker_api::validate_wire_value;
/// let output = serde_json::json!({"invoice_id": "INV-1042", "issued": true});
/// validate_wire_value(&output).unwrap();
/// ```
pub fn validate_wire_value(value: &Value) -> Result<()> {
    fn visit(value: &Value, depth: usize) -> Result<()> {
        if matches!(value, Value::Array(_) | Value::Object(_)) && depth >= MAX_WIRE_VALUE_DEPTH {
            return Err(Error::new(
                ErrorKind::Protocol,
                "program output exceeds 64 nested containers",
            ));
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(value, 0)
}
