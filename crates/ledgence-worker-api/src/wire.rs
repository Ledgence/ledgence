use crate::{Error, ErrorKind, Result};
use serde_json::Value;

/// Maximum nested containers in a program result (a scalar has depth zero).
pub const MAX_WIRE_VALUE_DEPTH: usize = 64;

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
