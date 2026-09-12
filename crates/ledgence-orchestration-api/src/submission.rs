//! Strict task submission decoding and semantic idempotency comparison.
//!
//! Use [`SubmitTask::decode`] on incoming JSON bytes. Deserializing an already
//! constructed JSON value cannot detect duplicate keys or recover rounded
//! out-of-range integer tokens. This module rejects both at the byte boundary.

use crate::RetryPolicy;
use ledgence_worker_api::{Error, ErrorKind, ProgramRef, Result, decode_json, validate_wire_value};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, DeserializeOwned, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use std::{fmt, io};

/// Maximum compact JSON encoding of the application-owned submission data.
///
/// This includes JSON quotes and escapes; insignificant incoming whitespace
/// does not count toward this limit. The complete incoming request is bounded
/// separately by [`SUBMISSION_MAX_BYTES`].
pub const SUBMISSION_DATA_MAX_BYTES: usize = ledgence_worker_api::APPLICATION_INPUT_MAX_BYTES;

/// Maximum incoming request bytes and normalized submission bytes (2 MiB).
pub const SUBMISSION_MAX_BYTES: usize = 2 * 1024 * 1024;

/// A request to create one logical task, before assigning run or attempt IDs.
///
/// `data` may be any JSON value, with at most 64 nested arrays/objects and a
/// compact serialized size of 1 MiB. Strings, including escaped U+0000, remain
/// application-owned. Integer tokens must fit i64/u64; fractional/exponent
/// tokens use finite binary64, as in the worker protocol.
///
/// Tenant, namespace, and queue must contain 1–128 UTF-8 bytes and no Unicode
/// control characters or Unicode noncharacters, matching the platform identifier
/// rules. Optional correlation contains at most 512 UTF-8 bytes and no control
/// characters. Program identifiers use [`ProgramRef::validate`].
/// The default retry policy is supplied by [`RetryPolicy::default`]; omitted
/// attempt timeout is 300,000 ms and must be between 60,000 and 86,400,000 ms.
/// Unknown submission fields are rejected.
///
/// Use [`Self::semantically_matches`] rather than ordinary JSON equality for
/// idempotency: integer and floating values, and positive/negative floating
/// zero, have intentionally different normalized representations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitTask {
    pub tenant_id: String,
    pub namespace: String,
    pub queue: String,
    pub program: ProgramRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_key: Option<String>,
    pub data: Value,
    #[serde(default)]
    pub retry_policy: RetryPolicy,
    #[serde(default = "default_attempt_timeout_ms")]
    pub attempt_timeout_ms: u64,
}

const fn default_attempt_timeout_ms() -> u64 {
    300_000
}

impl SubmitTask {
    /// Validate a constructed submission, including bounded JSON encoding.
    ///
    /// This cannot detect duplicate keys or preexisting numeric rounding in a
    /// `Value`; incoming transport bytes must pass through [`Self::decode`].
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("tenant_id", self.tenant_id.as_str()),
            ("namespace", self.namespace.as_str()),
            ("queue", self.queue.as_str()),
        ] {
            if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
                return Err(invalid(format!(
                    "{name} must contain 1–128 UTF-8 bytes and no control characters"
                )));
            }
        }
        if self
            .tenant_id
            .chars()
            .chain(self.namespace.chars())
            .chain(self.queue.chars())
            .any(|character| {
                let code = u32::from(character);
                (0xfdd0..=0xfdef).contains(&code) || code & 0xffff >= 0xfffe
            })
        {
            return Err(invalid(
                "tenant_id, namespace, and queue must not contain Unicode noncharacters",
            ));
        }
        if let Some(key) = &self.correlation_key
            && (key.len() > 512 || key.chars().any(char::is_control))
        {
            return Err(invalid(
                "correlation_key must contain at most 512 UTF-8 bytes and no control characters",
            ));
        }
        self.program.validate()?;
        self.retry_policy.validate()?;
        if !(60_000..=86_400_000).contains(&self.attempt_timeout_ms) {
            return Err(invalid(
                "attempt_timeout_ms must be between 60000 and 86400000",
            ));
        }
        validate_wire_value(&self.data)
            .map_err(|_| invalid("submission data must not exceed 64 nested arrays or objects"))?;
        check_encoded_size(&self.data, SUBMISSION_DATA_MAX_BYTES, "submission data")?;
        check_encoded_size(self, SUBMISSION_MAX_BYTES, "submission")
    }

    /// Decode a complete JSON request without duplicate-key collapse or
    /// out-of-range integer rounding, then validate its fields and limits.
    ///
    /// Duplicate keys are rejected at every depth, including inside user data;
    /// keys with equivalent JSON escape spellings are also duplicates. Trailing
    /// bytes, invalid UTF-8, nonfinite numbers, and unknown fields are rejected.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let submission: Self = decode_unique_json(bytes, SUBMISSION_MAX_BYTES)?;
        submission.validate()?;
        Ok(submission)
    }

    /// Return a deterministic JSON encoding for semantic request comparison.
    ///
    /// Object keys are sorted recursively. Array order and string values remain
    /// significant. Missing defaults normalize to their explicit values, and a
    /// missing or null correlation key normalizes to absence. Integer `1` and
    /// float `1.0` differ; equivalent binary64 spellings such as `1e0` and `1.0`
    /// match. Positive and negative floating zero differ. The existing parser
    /// treats the spelling `-0` as floating negative zero, matching `-0.0`.
    ///
    /// This is Ledgence's comparison encoding, not an implementation of RFC
    /// 8785 or arbitrary-precision numeric canonicalization.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let value = serde_json::to_value(self)
            .map_err(|error| invalid(format!("cannot encode submission: {error}")))?;
        canonical_owned_json_bytes(value)
    }

    /// Compare validated submissions using [`Self::canonical_bytes`].
    ///
    /// Every field in this type participates, including scope, correlation,
    /// immutable program identity, and the normalized execution policy.
    pub fn semantically_matches(&self, other: &Self) -> Result<bool> {
        Ok(self.canonical_bytes()? == other.canonical_bytes()?)
    }
}

/// Decode a bounded JSON command without losing duplicate keys or large integers.
///
/// The byte limit applies to the complete incoming request, including whitespace.
/// Duplicate keys are rejected recursively before object insertion, including
/// equivalent escaped spellings. Existing worker numeric-token checks run before
/// constructing any JSON values. The target's serde schema is then applied.
/// Callers must additionally run their command's domain validation; this helper
/// does not impose the submission-specific data, metadata, or execution limits.
pub fn decode_unique_json<T: DeserializeOwned>(bytes: &[u8], max_bytes: usize) -> Result<T> {
    if bytes.len() > max_bytes {
        return Err(invalid(format!(
            "JSON request exceeds its {max_bytes}-byte limit"
        )));
    }
    let StrictValue(value) = decode_json(bytes)?;
    serde_json::from_value(value).map_err(|error| invalid(format!("invalid JSON command: {error}")))
}

/// Deterministically encode an already validated JSON value for comparison.
///
/// Object ordering is ignored recursively; array ordering and strings remain
/// significant. Integer and binary64 number classes remain distinct, as do
/// positive and negative floating zero. Equivalent parsed binary64 values have
/// the same encoding. See [`SubmitTask::canonical_bytes`] for normalization of
/// submission defaults and the JSON spelling `-0`.
///
/// This helper also supports immutable execution-report comparison. It does
/// not impose submission limits on a report or replace the caller's domain
/// validation: callers must first enforce the appropriate depth/byte bounds
/// and decode original JSON without duplicate-key collapse or numeric loss.
pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    canonical_owned_json_bytes(value.clone())
}

fn canonical_owned_json_bytes(mut value: Value) -> Result<Vec<u8>> {
    sort_objects(&mut value);
    serde_json::to_vec(&value)
        .map_err(|error| invalid(format!("cannot encode canonical JSON: {error}")))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, message)
}

fn check_encoded_size(value: &impl Serialize, limit: usize, label: &str) -> Result<()> {
    let mut counter = ByteCounter { written: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| invalid(format!("{label} exceeds its {limit}-byte JSON limit")))
}

struct ByteCounter {
    written: usize,
    limit: usize,
}

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.written {
            return Err(io::Error::other("JSON byte limit exceeded"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn sort_objects(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(sort_objects),
        Value::Object(values) => {
            let mut sorted: Vec<_> = std::mem::take(values).into_iter().collect();
            sorted.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut value) in sorted {
                sort_objects(&mut value);
                values.insert(key, value);
            }
        }
        _ => {}
    }
}

/// A JSON value whose object keys have been checked before insertion.
struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;

        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON value without duplicate object keys")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(value)))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(value.into())))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(value.into())))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                Number::from_f64(value)
                    .map(|number| StrictValue(Value::Number(number)))
                    .ok_or_else(|| E::custom("nonfinite JSON number"))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value.to_owned())))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value)))
            }

            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(Value::Array(values)))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut object: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = object.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON object key"));
                    }
                    let StrictValue(value) = object.next_value()?;
                    values.insert(key, value);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }

        deserializer.deserialize_any(StrictVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(data: &str) -> Vec<u8> {
        format!(
            r#"{{"tenant_id":"acme","namespace":"billing","queue":"python","program":{{"id":"invoice","version":"1.0.0"}},"data":{data}}}"#
        )
        .into_bytes()
    }

    fn submission(data: &str) -> SubmitTask {
        SubmitTask::decode(&request(data)).unwrap()
    }

    #[test]
    fn omitted_optional_settings_use_explicit_defaults() {
        let implicit = submission("null");
        assert_eq!(implicit.correlation_key, None);
        assert_eq!(implicit.retry_policy.max_attempts, 3);
        assert_eq!(implicit.retry_policy.retry_delay_ms, 5_000);
        assert_eq!(implicit.attempt_timeout_ms, 300_000);
        let mut explicit: Value = decode_json(&request("null")).unwrap();
        explicit["correlation_key"] = Value::Null;
        explicit["retry_policy"] = json!({"max_attempts": 3, "retry_delay_ms": 5000});
        explicit["attempt_timeout_ms"] = json!(300_000);
        let explicit = SubmitTask::decode(&serde_json::to_vec(&explicit).unwrap()).unwrap();
        assert!(implicit.semantically_matches(&explicit).unwrap());
    }

    #[test]
    fn duplicate_keys_are_rejected_before_collapse_at_every_depth() {
        for data in [
            r#"{"x":1,"x":2}"#,
            r#"[{"outer":{"x":1,"\u0078":1}}]"#,
            r#"{"\u0000":null,"\u0000":true}"#,
        ] {
            let error = SubmitTask::decode(&request(data)).unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidInput);
            assert!(error.message.contains("duplicate JSON object key"));
        }
        let duplicated = String::from_utf8(request("null")).unwrap().replacen(
            "\"tenant_id\":\"acme\"",
            "\"tenant_id\":\"acme\",\"tenant_id\":\"acme\"",
            1,
        );
        assert!(SubmitTask::decode(duplicated.as_bytes()).is_err());
        let duplicate_program = String::from_utf8(request("null")).unwrap().replace(
            "\"id\":\"invoice\"",
            "\"id\":\"invoice\",\"id\":\"invoice\"",
        );
        assert!(SubmitTask::decode(duplicate_program.as_bytes()).is_err());
        assert!(submission(r#"[{"x":1},{"x":2}]"#).validate().is_ok());
    }

    #[test]
    fn integer_limits_and_exact_large_integer_roundtrip() {
        let accepted = submission("[-9223372036854775808,18446744073709551615,9007199254740993]");
        assert_eq!(accepted.data[0].as_i64(), Some(i64::MIN));
        assert_eq!(accepted.data[1].as_u64(), Some(u64::MAX));
        assert_eq!(accepted.data[2].as_u64(), Some(9_007_199_254_740_993));
        let roundtrip = SubmitTask::decode(&accepted.canonical_bytes().unwrap()).unwrap();
        assert!(accepted.semantically_matches(&roundtrip).unwrap());
        for rejected in ["-9223372036854775809", "18446744073709551616"] {
            assert!(SubmitTask::decode(&request(rejected)).is_err());
        }
    }

    #[test]
    fn user_strings_and_keys_preserve_nul_and_numeric_looking_text() {
        let task = submission(
            r#"{"\u0000":"left\u0000right","$serde_json::private::Number":"18446744073709551616"}"#,
        );
        assert_eq!(task.data["\0"], json!("left\0right"));
        let roundtrip = SubmitTask::decode(&task.canonical_bytes().unwrap()).unwrap();
        assert!(task.semantically_matches(&roundtrip).unwrap());
    }

    #[test]
    fn binary64_spellings_normalize_without_erasing_number_kinds_or_signed_zero() {
        for (left, right) in [("1e0", "1.0"), ("2.5e2", "250.0"), ("-0", "-0.0")] {
            assert!(
                submission(left)
                    .semantically_matches(&submission(right))
                    .unwrap()
            );
        }
        for (left, right) in [("1", "1.0"), ("0", "0.0"), ("0.0", "-0.0"), ("0", "-0")] {
            assert!(
                !submission(left)
                    .semantically_matches(&submission(right))
                    .unwrap()
            );
        }
        let task = submission("[2.291712365432881e-09,-1.527077339613215e-236,-0.0]");
        let restored = SubmitTask::decode(&task.canonical_bytes().unwrap()).unwrap();
        for (actual, expected) in restored.data.as_array().unwrap().iter().zip([
            2.291712365432881e-09_f64,
            -1.527077339613215e-236_f64,
            -0.0_f64,
        ]) {
            assert_eq!(actual.as_f64().unwrap().to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn comparison_ignores_object_order_but_preserves_arrays_and_values() {
        let left = submission(r#"{"b":[{"z":0,"a":1},2],"a":"same"}"#);
        let reordered = submission(r#"{"a":"same","b":[{"a":1,"z":0},2]}"#);
        assert!(left.semantically_matches(&reordered).unwrap());
        assert!(
            !left
                .semantically_matches(&submission(r#"{"a":"same","b":[2,{"a":1,"z":0}]}"#))
                .unwrap()
        );
        assert!(
            !left
                .semantically_matches(&submission(r#"{"a":"different","b":[{"a":1,"z":0},2]}"#))
                .unwrap()
        );
    }

    #[test]
    fn shared_json_comparison_preserves_nested_report_number_classes() {
        let left: Value =
            decode_json(br#"{"outcome":{"z":-0.0,"a":9007199254740993},"attempt":1}"#).unwrap();
        let reordered: Value =
            decode_json(br#"{"attempt":1,"outcome":{"a":9007199254740993,"z":-0e0}}"#).unwrap();
        assert_eq!(
            canonical_json_bytes(&left).unwrap(),
            canonical_json_bytes(&reordered).unwrap()
        );
        for changed in [
            br#"{"attempt":1,"outcome":{"a":9007199254740993,"z":0.0}}"#.as_slice(),
            br#"{"attempt":1.0,"outcome":{"a":9007199254740993,"z":-0.0}}"#.as_slice(),
        ] {
            let changed: Value = decode_json(changed).unwrap();
            assert_ne!(
                canonical_json_bytes(&left).unwrap(),
                canonical_json_bytes(&changed).unwrap()
            );
        }
    }

    #[test]
    fn all_submission_settings_participate_in_comparison() {
        let original = submission("null");
        let mut changed = Vec::new();
        let mut task = original.clone();
        task.program.version = "2.0.0".into();
        changed.push(task);
        let mut task = original.clone();
        task.retry_policy.max_attempts = 2;
        changed.push(task);
        let mut task = original.clone();
        task.retry_policy.retry_delay_ms += 1;
        changed.push(task);
        let mut task = original.clone();
        task.correlation_key = Some("invoice:1".into());
        changed.push(task);
        let mut task = original.clone();
        task.attempt_timeout_ms += 1;
        changed.push(task);
        let mut task = original.clone();
        task.tenant_id = "other".into();
        changed.push(task);
        let mut task = original.clone();
        task.namespace = "other".into();
        changed.push(task);
        let mut task = original.clone();
        task.queue = "other".into();
        changed.push(task);
        for task in changed {
            assert!(!original.semantically_matches(&task).unwrap());
        }
    }

    #[test]
    fn metadata_uses_utf8_byte_limits_and_rejects_controls() {
        let mut task = submission("null");
        task.tenant_id = "é".repeat(64);
        task.correlation_key = Some("é".repeat(256));
        assert!(task.validate().is_ok());
        task.tenant_id.push('é');
        assert!(task.validate().is_err());
        task.tenant_id = "acme".into();
        task.correlation_key.as_mut().unwrap().push('é');
        assert!(task.validate().is_err());
        task.correlation_key = Some("business\u{85}reference".into());
        assert!(task.validate().is_err());
        task.correlation_key = None;
        for value in ["", "line\nbreak", "nul\0value"] {
            task.queue = value.into();
            assert!(task.validate().is_err());
        }
    }

    #[test]
    fn platform_identifiers_reject_noncharacters_before_the_task_can_be_accepted() {
        for character in [
            '\u{fdd0}',
            '\u{fdef}',
            '\u{fffe}',
            '\u{ffff}',
            '\u{1fffe}',
            '\u{10ffff}',
        ] {
            let mut task = submission("null");
            task.tenant_id = format!("tenant{character}");
            assert!(task.validate().is_err());
            task.tenant_id = "acme".into();
            task.namespace = format!("namespace{character}");
            assert!(task.validate().is_err());
            task.namespace = "billing".into();
            task.queue = format!("queue{character}");
            assert!(task.validate().is_err());
        }
        let mut task = submission("null");
        task.correlation_key = Some("reference\u{10ffff}".into());
        assert!(task.validate().is_ok());
    }

    #[test]
    fn generic_command_decoder_rejects_nested_duplicate_keys_and_respects_limits() {
        let bytes = br#"{"report":{"output":{"value":1,"\u0076alue":2}}}"#;
        assert!(decode_unique_json::<Value>(bytes, bytes.len()).is_err());
        let valid = br#"{"report":{"output":9007199254740993}}"#;
        assert!(decode_unique_json::<Value>(valid, valid.len() - 1).is_err());
        let decoded: Value = decode_unique_json(valid, valid.len()).unwrap();
        assert_eq!(
            decoded["report"]["output"].as_u64(),
            Some(9_007_199_254_740_993)
        );
        assert!(
            decode_unique_json::<Value>(br#"{"report":{"output":18446744073709551616}}"#, 1000)
                .is_err()
        );
    }

    #[test]
    fn data_size_counts_compact_json_bytes_including_string_escapes() {
        let mut task = submission("null");
        task.data = Value::String("x".repeat(SUBMISSION_DATA_MAX_BYTES - 2));
        assert!(task.validate().is_ok());
        task.data = Value::String("x".repeat(SUBMISSION_DATA_MAX_BYTES - 1));
        assert!(task.validate().is_err());
        task.data = Value::String("\0".repeat(SUBMISSION_DATA_MAX_BYTES / 6 + 1));
        assert!(task.validate().is_err());
    }

    #[test]
    fn whole_request_limit_applies_to_incoming_whitespace() {
        let mut bytes = request("null");
        bytes.resize(SUBMISSION_MAX_BYTES, b' ');
        assert!(SubmitTask::decode(&bytes).is_ok());
        bytes.push(b' ');
        assert!(SubmitTask::decode(&bytes).is_err());
    }

    #[test]
    fn data_depth_64_is_allowed_and_65_is_rejected() {
        let mut data = Value::Null;
        for _ in 0..64 {
            data = Value::Array(vec![data]);
        }
        let encoded = serde_json::to_string(&data).unwrap();
        assert!(SubmitTask::decode(&request(&encoded)).is_ok());
        data = Value::Array(vec![data]);
        let encoded = serde_json::to_string(&data).unwrap();
        assert!(SubmitTask::decode(&request(&encoded)).is_err());
    }

    #[test]
    fn attempt_timeout_endpoints_are_inclusive() {
        let mut task = submission("null");
        for valid in [60_000, 86_400_000] {
            task.attempt_timeout_ms = valid;
            assert!(task.validate().is_ok());
        }
        for invalid in [0, 1, 59_999, 86_400_001] {
            task.attempt_timeout_ms = invalid;
            assert!(task.validate().is_err());
        }
    }

    #[test]
    fn invalid_schema_syntax_and_nonfinite_values_are_rejected() {
        for data in ["1e400", "NaN", "[1 2]", "\"\\ud800\""] {
            assert!(SubmitTask::decode(&request(data)).is_err());
        }
        let mut value: Value = decode_json(&request("null")).unwrap();
        value["unexpected"] = json!(true);
        assert!(SubmitTask::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        value.as_object_mut().unwrap().remove("unexpected");
        value["program"]["version"] = json!("../invalid");
        assert!(SubmitTask::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}
