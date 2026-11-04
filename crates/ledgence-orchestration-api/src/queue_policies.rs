//! Durable queue policy management. Capacity is scoped to a logical queue, not to a worker.
use crate::{console::ConsoleU64, *};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const QUEUE_POLICY_COMMAND_MAX_BYTES: usize = 16 * 1024;
pub const QUEUE_POLICY_PAGE_MAX_BYTES: usize = 1024 * 1024;
pub const QUEUE_POLICY_PAGE_LIMIT: u32 = 100;
pub const QUEUE_CONCURRENCY_MAX: u32 = 1_000_000;

fn invalid_queue(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn revision_after(expected: u64) -> Result<u64> {
    expected
        .checked_add(1)
        .ok_or_else(|| invalid_queue("queue revision exhausted"))
}
fn positive_revision(n: u64) -> Result<()> {
    if n == 0 {
        return Err(invalid_queue("queue revision must be positive"));
    }
    Ok(())
}
fn page_input(after: Option<&str>, limit: u32) -> Result<()> {
    if let Some(cursor) = after {
        validate_text(cursor, 128)?;
    }
    if !(1..=QUEUE_POLICY_PAGE_LIMIT).contains(&limit) {
        return Err(invalid_queue("queue policy page limit must be 1..100"));
    }
    Ok(())
}
fn timestamp(t: Timestamp) -> Result<()> {
    if t > console::CONSOLE_MAX_TIMESTAMP {
        return Err(invalid_queue("queue timestamp exceeds supported range"));
    }
    Ok(())
}
pub(crate) mod decimal {
    use super::*;
    pub fn serialize<S: Serializer>(v: &u64, s: S) -> std::result::Result<S::Ok, S::Error> {
        ConsoleU64(*v).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
        Ok(ConsoleU64::deserialize(d)?.0)
    }
}
pub(crate) mod optional_decimal {
    use super::*;
    pub fn serialize<S: Serializer>(v: &Option<u64>, s: S) -> std::result::Result<S::Ok, S::Error> {
        v.map(ConsoleU64).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Option<u64>, D::Error> {
        Ok(Option::<ConsoleU64>::deserialize(d)?.map(|v| v.0))
    }
}

fn validate_next(next: Option<&str>, last: Option<&str>) -> Result<()> {
    if next.is_some() && next != last {
        return Err(invalid_queue("queue page cursor disagrees with last item"));
    }
    Ok(())
}

mod policy;
pub use policy::*;
mod holds;
pub use holds::*;
mod ports;
pub use ports::*;
#[cfg(test)]
mod tests;
