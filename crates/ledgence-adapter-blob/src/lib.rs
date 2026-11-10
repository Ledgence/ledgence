//! Execution-artifact byte storage and bounded local transfer spools.
//!
//! Catalog authority and integrity acceptance remain in the orchestration
//! service. Filesystem storage is for a single orchestrator on a persistent
//! local volume. The optional S3 adapter supports a separately qualified object
//! service; it does not make third-party endpoints interchangeable by default.

mod filesystem;
#[cfg(feature = "s3")]
mod s3;
mod spool;

pub use filesystem::FileExecutionArtifactStore;
#[cfg(feature = "s3")]
pub use s3::{S3ArtifactOptions, S3ExecutionArtifactStore};
pub use spool::FileArtifactSpoolFactory;

use ledgence_orchestration_api::{ContractError, Result};
use std::time::Instant;

const MAX_BYTES: u64 = 64 * 1024 * 1024;
const CHUNK_BYTES: usize = 256 * 1024;

fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}
fn random_id() -> Result<String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| unavailable("artifact randomness unavailable"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(unavailable("artifact transfer deadline exceeded"))
    } else {
        Ok(())
    }
}
fn chunk_limit(max_bytes: usize) -> Result<()> {
    if max_bytes == 0 || max_bytes > CHUNK_BYTES {
        Err(invalid("artifact chunk must be between 1 and 262144 bytes"))
    } else {
        Ok(())
    }
}

fn transfer_deadline(requested: Instant) -> Instant {
    requested.min(Instant::now() + std::time::Duration::from_secs(120))
}
