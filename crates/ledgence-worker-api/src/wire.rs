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

/// Envelope nesting allowance for opt-in interactive runtime payloads. Domain
/// handlers must still validate each application value at the ordinary depth 64.
pub const MAX_RUNTIME_VALUE_DEPTH: usize = 96;
/// Complete extension payload budget (640 KiB).
pub const RUNTIME_EXTENSION_MAX_BYTES: usize = 640 * 1024;
/// Interactive request/reply budget, including room around two 64 KiB values.
pub const RUNTIME_REQUEST_MAX_BYTES: usize = 144 * 1024;

/// Validate interactive envelope nesting and compact size without allocating
/// the encoded payload. Ordinary application values use `validate_wire_value`.
pub fn validate_runtime_payload(value: &Value, max_bytes: usize) -> Result<()> {
    fn visit(value: &Value, depth: usize, remaining: &mut usize) -> Result<()> {
        if *remaining == 0 {
            return Err(Error::new(
                ErrorKind::Protocol,
                "runtime payload exceeds its byte budget",
            ));
        }
        *remaining -= 1;
        if matches!(value, Value::Array(_) | Value::Object(_)) && depth >= MAX_RUNTIME_VALUE_DEPTH {
            return Err(Error::new(
                ErrorKind::Protocol,
                "runtime payload exceeds 96 nested containers",
            ));
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1, remaining)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1, remaining)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0 {
                return Err(std::io::Error::other(
                    "runtime payload exceeds its byte budget",
                ));
            }
            self.0 -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut remaining_nodes = max_bytes;
    visit(value, 0, &mut remaining_nodes)?;
    serde_json::to_writer(Budget(max_bytes), value).map_err(|error| {
        Error::new(
            ErrorKind::Protocol,
            format!("runtime payload exceeds its byte budget: {error}"),
        )
    })
}
