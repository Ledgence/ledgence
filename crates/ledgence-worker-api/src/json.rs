use crate::{Error, ErrorKind, Result};
use serde::de::DeserializeOwned;

/// Decode JSON without silently rounding integer tokens outside i64/u64.
///
/// Use this at transport boundaries before constructing portable contract types.
/// Fractions and exponent-form numbers use finite binary64, as in the Python
/// helper; encode arbitrary-precision decimals or larger integers as strings.
/// JSON syntax, nesting and the target schema are still checked by serde_json.
pub fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    // Only identify unquoted numeric tokens here; do not implement JSON syntax
    // or interpret strings. serde_json performs the complete parse below.
    let mut index = 0;
    let mut quoted = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if quoted => {
                index += 2;
                continue;
            }
            b'"' => quoted = !quoted,
            b'-' | b'0'..=b'9' if !quoted => {
                let start = index;
                while index < bytes.len()
                    && matches!(bytes[index], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                {
                    index += 1;
                }
                let token = &bytes[start..index];
                if !token.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
                    let token = std::str::from_utf8(token).expect("numeric token is ASCII");
                    let representable = if token.starts_with('-') {
                        token.parse::<i64>().is_ok()
                    } else {
                        token.parse::<u64>().is_ok()
                    };
                    if !representable {
                        return Err(Error::new(
                            ErrorKind::InvalidInput,
                            "JSON integer exceeds the signed/unsigned 64-bit range; encode exact larger values as strings",
                        ));
                    }
                }
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    serde_json::from_slice(bytes)
        .map_err(|error| Error::new(ErrorKind::InvalidInput, format!("invalid JSON: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn integer_limits_are_checked_before_lossy_value_construction() {
        for (text, expected) in [
            ("-9223372036854775808", json!(i64::MIN)),
            ("18446744073709551615", json!(u64::MAX)),
            ("9007199254740993", json!(9_007_199_254_740_993_u64)),
        ] {
            assert_eq!(decode_json::<Value>(text.as_bytes()).unwrap(), expected);
        }
        for text in [
            "-9223372036854775809",
            "18446744073709551616",
            "1000000000000000000000000000000000000000",
        ] {
            assert!(decode_json::<Value>(text.as_bytes()).is_err(), "{text}");
            assert!(decode_json::<Value>(format!("{{\"data\":[{text}]}}").as_bytes()).is_err());
        }
    }

    #[test]
    fn strings_and_application_keys_are_opaque_and_syntax_still_validates() {
        let original = json!({"$serde_json::private::Number": "18446744073709551616", "18446744073709551616": "escaped \\\" quote and digits 18446744073709551616", "data":[null,true,2.5,1e100]});
        assert_eq!(
            decode_json::<Value>(&serde_json::to_vec(&original).unwrap()).unwrap(),
            original
        );
        for invalid in ["[1 2]", "01", "--1", "1e", "NaN", "1e400", "\"unterminated"] {
            assert!(
                decode_json::<Value>(invalid.as_bytes()).is_err(),
                "{invalid}"
            );
        }
    }
    #[test]
    fn binary64_tokens_preserve_the_nearest_representable_value() {
        let value =
            decode_json::<Value>(b"[2.291712365432881e-09,-1.527077339613215e-236]").unwrap();
        let numbers = value.as_array().unwrap();
        assert_eq!(numbers.len(), 2);
        for (actual, expected) in numbers
            .iter()
            .zip([2.291712365432881e-09_f64, -1.527077339613215e-236_f64])
        {
            assert_eq!(actual.as_f64().unwrap().to_bits(), expected.to_bits());
        }
        assert_eq!(
            decode_json::<Value>(&serde_json::to_vec(&value).unwrap()).unwrap(),
            value,
        );
    }
}
