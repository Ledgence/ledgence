//! Lossless persisted payloads and checked relational snapshot reconstruction.
//!
//! NUMERIC counters must be selected as `trunc(column)::text AS column_text`.
//! Decode failures indicate unavailable/corrupt storage, not invalid requests.

use ledgence_orchestration_api::*;
use ledgence_worker_api::{CloudEvent, ProgramDescriptor};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{Decode, Postgres, Row, Type, postgres::PgRow};

// The core formats RFC3339 dates with a four-digit year, through 9999 inclusive.
const MAX_TIMESTAMP_MS: u64 = 253_402_300_799_999;

fn corrupt(context: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid PostgreSQL record: {context}"))
}

trait Record {
    fn row(&self) -> &PgRow;
    fn column(&self, name: &str) -> String;
}

impl Record for PgRow {
    fn row(&self) -> &PgRow {
        self
    }

    fn column(&self, name: &str) -> String {
        name.to_owned()
    }
}

struct PrefixedRow<'a> {
    row: &'a PgRow,
    prefix: &'a str,
}

impl Record for PrefixedRow<'_> {
    fn row(&self) -> &PgRow {
        self.row
    }

    fn column(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

fn get<T>(row: &impl Record, column: &str) -> Result<T>
where
    for<'r> T: Decode<'r, Postgres> + Type<Postgres>,
{
    row.row()
        .try_get(row.column(column).as_str())
        .map_err(|_| corrupt(column))
}

/// Use the same comparison encoding as the contract; do not route through JSONB.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(|_| corrupt("JSON serialization"))?;
    canonical_json_bytes(&value).map_err(|_| corrupt("JSON serialization"))
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    decode_unique_json(bytes, SETTLEMENT_MAX_BYTES).map_err(|_| corrupt("JSON payload"))
}

pub(crate) fn label<T: Serialize>(value: &T) -> Result<String> {
    match serde_json::to_value(value).map_err(|_| corrupt("enum serialization"))? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(corrupt("enum serialization")),
    }
}

fn enum_value<T: DeserializeOwned>(value: String) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value))
        .map_err(|_| corrupt("unknown state or transition"))
}

/// Bind timestamp milliseconds without narrowing or extending the core's range.
pub(crate) fn ms(value: u64) -> Result<i64> {
    if value > MAX_TIMESTAMP_MS {
        return Err(corrupt("timestamp outside RFC3339 range"));
    }
    i64::try_from(value).map_err(|_| corrupt("timestamp overflow"))
}

fn timestamp(value: i64) -> Result<u64> {
    let value = u64::try_from(value).map_err(|_| corrupt("negative timestamp"))?;
    ms(value)?;
    Ok(value)
}

fn time(row: &impl Record, column: &str) -> Result<u64> {
    timestamp(get(row, column)?)
}

fn optional_time(row: &impl Record, column: &str) -> Result<Option<u64>> {
    get::<Option<i64>>(row, column)?.map(timestamp).transpose()
}

fn count(row: &impl Record, column: &str) -> Result<u32> {
    u32::try_from(get::<i64>(row, column)?).map_err(|_| corrupt(column))
}

/// The SQL projection removes NUMERIC scale; accept only integer decimal digits.
pub(crate) fn u64_text(value: &str) -> Result<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(corrupt("unsigned counter representation"));
    }
    value.parse().map_err(|_| corrupt("unsigned counter range"))
}

fn positive_sequence(value: &str) -> Result<u64> {
    match u64_text(value)? {
        0 => Err(corrupt("operation sequence starts at one")),
        value => Ok(value),
    }
}

fn identifier(row: &impl Record, column: &str, maximum: usize) -> Result<String> {
    let value: String = get(row, column)?;
    validate_text(&value, maximum).map_err(|_| corrupt(column))?;
    Ok(value)
}

pub(crate) fn task(row: &PgRow) -> Result<TaskSnapshot> {
    let command = SubmitCommand {
        idempotency_key: identifier(row, "idempotency_key", 255)?,
        input: decode(&get::<Vec<u8>>(row, "input_bytes")?)?,
        origin_trace: get::<Option<Vec<u8>>>(row, "origin_trace_bytes")?
            .map(|bytes| decode(&bytes))
            .transpose()?,
    };
    ledgence_orchestration_core::validate_submission(&command)
        .map_err(|_| corrupt("submission binding"))?;
    let descriptor: ProgramDescriptor = decode(&get::<Vec<u8>>(row, "descriptor_bytes")?)?;
    descriptor.validate().map_err(|_| corrupt("descriptor"))?;
    if descriptor.program != command.input.program
        || get::<String>(row, "tenant_id")? != command.input.tenant_id
        || get::<String>(row, "namespace")? != command.input.namespace
        || get::<String>(row, "queue")? != command.input.queue
        || get::<Option<String>>(row, "correlation_key")? != command.input.correlation_key
    {
        return Err(corrupt("indexed submission differs from immutable binding"));
    }
    let task = TaskSnapshot {
        task_id: identifier(row, "task_id", 128)?,
        run_id: identifier(row, "run_id", 128)?,
        idempotency_key: command.idempotency_key,
        input: command.input,
        descriptor,
        origin_trace: command.origin_trace,
        state: enum_value(get(row, "state")?)?,
        submitted_at: time(row, "submitted_at_ms")?,
        available_at: time(row, "available_at_ms")?,
        terminal_at: optional_time(row, "terminal_at_ms")?,
        current_attempt_id: get(row, "current_attempt_id")?,
        attempt_count: count(row, "attempt_count")?,
        cancel_requested_at: optional_time(row, "cancel_requested_at_ms")?,
    };
    if let Some(id) = &task.current_attempt_id {
        validate_text(id, 128).map_err(|_| corrupt("current attempt identity"))?;
    }
    let next_expiry = optional_time(row, "next_expiry_ms")?;
    if (task.state == TaskState::Active) != task.current_attempt_id.is_some()
        || (task.state == TaskState::Active) != next_expiry.is_some()
        || task.state.is_terminal() != task.terminal_at.is_some()
        || task.attempt_count > task.input.retry_policy.max_attempts
        || (task.state == TaskState::Active && task.attempt_count == 0)
    {
        return Err(corrupt("task scheduling state"));
    }
    Ok(task)
}

/// Hydrate `a.*` plus the optional accepted-settlement columns. In particular,
/// the caller must select the requested historical attempt, not only the current one.
pub(crate) fn attempt(row: &PgRow, task: &TaskSnapshot) -> Result<AttemptSnapshot> {
    attempt_prefixed(row, task, "")
}

/// Aliased fields permit one coherent task/previous-attempt snapshot SELECT.
pub(crate) fn attempt_prefixed(
    row: &PgRow,
    task: &TaskSnapshot,
    prefix: &str,
) -> Result<AttemptSnapshot> {
    let row = &PrefixedRow { row, prefix };
    let event: CloudEvent = decode(&get::<Vec<u8>>(row, "event_bytes")?)?;
    let owner = LeaseOwner {
        scope: task.scope(),
        task_id: identifier(row, "task_id", 128)?,
        attempt_id: identifier(row, "attempt_id", 128)?,
        lease_id: identifier(row, "lease_id", 128)?,
        generation: count(row, "generation")?,
        worker_session_id: identifier(row, "worker_session_id", 128)?,
        consumer_id: count(row, "consumer_id")?,
    };
    if owner.task_id != task.task_id
        || owner.generation == 0
        || owner.generation > task.attempt_count
        || event.task_id() != task.task_id
        || event.attempt_id() != owner.attempt_id
        || event.value()["ldgattemptno"].as_u64() != Some(u64::from(owner.generation))
        || event.value()["ldgrunid"].as_str() != Some(task.run_id.as_str())
        || event.tenant_id() != task.input.tenant_id
        || event.namespace() != task.input.namespace
        || get::<String>(row, "event_id")? != event.id()
        || event.value()["source"].as_str() != Some(get::<String>(row, "event_source")?.as_str())
        || canonical_json_bytes(&event.value()["data"]).map_err(|_| corrupt("event data"))?
            != canonical_json_bytes(&task.input.data).map_err(|_| corrupt("submission data"))?
    {
        return Err(corrupt(
            "attempt differs from task or indexed event identity",
        ));
    }
    let last_renewal = match (
        get::<Option<String>>(row, "last_renew_sequence_text")?,
        get::<Option<String>>(row, "last_renew_intent")?,
    ) {
        (None, None) => None,
        (Some(sequence), Some(intent)) => Some(RenewCommand {
            owner: owner.clone(),
            sequence: positive_sequence(&sequence)?,
            intent: enum_value(intent)?,
        }),
        _ => return Err(corrupt("incomplete renewal command")),
    };
    let settlement = match (
        get::<Option<Vec<u8>>>(row, "accepted_command")?,
        get::<Option<i64>>(row, "accepted_at")?,
        get::<Option<String>>(row, "accepted_operation_id")?,
    ) {
        (None, None, None) => None,
        (Some(bytes), Some(accepted_at), Some(operation_id)) => {
            let command = SettleCommand::decode(&bytes)
                .map_err(|_| corrupt("accepted settlement command"))?;
            if command.owner != owner || command.operation_id != operation_id {
                return Err(corrupt("accepted receipt identity"));
            }
            Some(AcceptedSettlement {
                command,
                receipt: SettlementReceipt {
                    operation_id,
                    task_id: owner.task_id.clone(),
                    attempt_id: owner.attempt_id.clone(),
                    accepted_at: timestamp(accepted_at)?,
                },
            })
        }
        _ => return Err(corrupt("incomplete accepted settlement")),
    };
    let attempt = AttemptSnapshot {
        event,
        descriptor: task.descriptor.clone(),
        lease: Lease {
            owner,
            expires_at: time(row, "expires_at_ms")?,
        },
        deadline: time(row, "deadline_ms")?,
        authority_deadline: time(row, "authority_deadline_ms")?,
        state: enum_value(get(row, "state")?)?,
        execution_may_have_started: get(row, "execution_may_have_started")?,
        last_renewal,
        quiescence: enum_value(get(row, "quiescence")?)?,
        settlement,
        finished_at: optional_time(row, "finished_at_ms")?,
    };
    if (attempt.state == AttemptState::Active) != attempt.finished_at.is_none()
        || (attempt.quiescence == Quiescence::Confirmed && attempt.settlement.is_none())
    {
        return Err(corrupt("attempt lifecycle state"));
    }
    if let Some(accepted) = &attempt.settlement {
        if accepted.command.quiescence == Quiescence::Confirmed
            && attempt.quiescence != Quiescence::Confirmed
        {
            return Err(corrupt("accepted cleanup confirmation was lost"));
        }
        ledgence_orchestration_core::validate_report(&attempt, &accepted.command)
            .map_err(|_| corrupt("accepted report binding"))?;
    }
    Ok(attempt)
}

pub(crate) fn session(row: &PgRow) -> Result<WorkerSession> {
    let session = WorkerSession {
        id: identifier(row, "session_id", 128)?,
        scope: Scope {
            tenant_id: identifier(row, "tenant_id", 128)?,
            namespace: identifier(row, "namespace", 128)?,
        },
        queue: identifier(row, "queue", 128)?,
        concurrency: count(row, "concurrency")?,
        expires_at: time(row, "expires_at_ms")?,
    };
    if session.concurrency == 0 {
        return Err(corrupt("zero session concurrency"));
    }
    Ok(session)
}

pub(crate) fn cursor(row: &PgRow, session: &WorkerSession) -> Result<Option<ConsumerCursor>> {
    let session_id: String = get(row, "session_id")?;
    let consumer_id = count(row, "consumer_id")?;
    if session_id != session.id || consumer_id >= session.concurrency {
        return Err(corrupt("cursor does not belong to registered consumer"));
    }
    let assignment = match (
        get::<Option<String>>(row, "task_id")?,
        get::<Option<String>>(row, "attempt_id")?,
    ) {
        (None, None) => None,
        (Some(task_id), Some(attempt_id)) => {
            validate_text(&task_id, 128).map_err(|_| corrupt("cursor task identity"))?;
            validate_text(&attempt_id, 128).map_err(|_| corrupt("cursor attempt identity"))?;
            Some(AttemptRef {
                task_id,
                attempt_id,
            })
        }
        _ => return Err(corrupt("incomplete cursor assignment")),
    };
    let Some(sequence) = get::<Option<String>>(row, "sequence_text")? else {
        return if assignment.is_none() {
            Ok(None)
        } else {
            Err(corrupt("uncommitted cursor has assignment"))
        };
    };
    Ok(Some(ConsumerCursor {
        command: AcquireCommand {
            scope: session.scope.clone(),
            queue: session.queue.clone(),
            worker_session_id: session_id,
            consumer_id,
            sequence: positive_sequence(&sequence)?,
        },
        assignment,
    }))
}

pub(crate) fn history(row: &PgRow) -> Result<RecordedHistoryEvent> {
    let attempt_id: Option<String> = get(row, "attempt_id")?;
    if let Some(id) = &attempt_id {
        validate_text(id, 128).map_err(|_| corrupt("history attempt identity"))?;
    }
    Ok(RecordedHistoryEvent {
        sequence: positive_sequence(&get::<String>(row, "sequence_text")?)?,
        event: HistoryEvent {
            task_id: identifier(row, "task_id", 128)?,
            attempt_id,
            at: time(row, "at_ms")?,
            reason: enum_value(get(row, "reason")?)?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn unsigned_counter_codec_keeps_full_range_and_rejects_coercion() {
        assert_eq!(u64_text("0").unwrap(), 0);
        assert_eq!(u64_text("9223372036854775808").unwrap(), 1_u64 << 63);
        assert_eq!(u64_text("18446744073709551615").unwrap(), u64::MAX);
        for text in [
            "",
            "-1",
            "+1",
            "1.0",
            "1e0",
            " 1",
            "18446744073709551616",
            "NaN",
        ] {
            assert!(matches!(u64_text(text), Err(ContractError::Unavailable(_))));
        }
        assert!(positive_sequence("0").is_err());
    }

    #[test]
    fn timestamp_codec_has_checked_core_range() {
        assert_eq!(timestamp(0).unwrap(), 0);
        assert_eq!(
            timestamp(ms(MAX_TIMESTAMP_MS).unwrap()).unwrap(),
            MAX_TIMESTAMP_MS
        );
        assert!(timestamp(-1).is_err());
        assert!(ms(MAX_TIMESTAMP_MS + 1).is_err());
        assert!(ms(u64::MAX).is_err());
    }

    #[test]
    fn stored_json_preserves_numeric_kinds_bits_and_application_strings() {
        let value = json!({
            "u64": u64::MAX,
            "integer": 1,
            "float": 1.0,
            "negative_zero": -0.0,
            "precise": 1.2345678901234567_f64,
            "nul": "\u{0}",
            "$serde_json::private::Number": "application-owned",
        });
        let bytes = encode(&value).unwrap();
        let restored: Value = decode(&bytes).unwrap();
        assert_eq!(encode(&restored).unwrap(), bytes);
        assert_eq!(restored["u64"].as_u64(), Some(u64::MAX));
        assert!(restored["integer"].as_number().unwrap().is_u64());
        assert!(restored["float"].as_number().unwrap().is_f64());
        assert_eq!(
            restored["negative_zero"].as_f64().unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            restored["precise"].as_f64().unwrap().to_bits(),
            1.2345678901234567_f64.to_bits()
        );
        assert_eq!(restored["nul"], "\u{0}");
        assert_eq!(
            restored["$serde_json::private::Number"],
            "application-owned"
        );
    }

    #[test]
    fn malformed_storage_is_unavailable_and_never_coerced() {
        for bytes in [
            br#"{"key":1,"key":2}"#.as_slice(),
            br#"{"key":1,"\u006bey":2}"#.as_slice(),
            b"18446744073709551616".as_slice(),
            b"NaN".as_slice(),
        ] {
            assert!(matches!(
                decode::<Value>(bytes),
                Err(ContractError::Unavailable(_))
            ));
        }
        assert!(enum_value::<TaskState>("unknown".into()).is_err());
        assert!(label(&42).is_err());
        assert_eq!(label(&RenewIntent::KeepAlive).unwrap(), "keep_alive");
    }
}
