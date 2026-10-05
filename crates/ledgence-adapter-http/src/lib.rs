//! Version-one HTTP/JSON transport for the portable task service.
//!
//! Enable `client` and/or `server` explicitly. Each client call performs one
//! bounded exchange; durable retries belong to the delivery driver or caller.

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "completion")]
pub mod completion;
#[cfg(feature = "client")]
mod response;
#[cfg(feature = "client")]
pub use client::{ExchangeMetadata, HttpProgramCatalogService, HttpTaskService};
#[cfg(feature = "server")]
pub mod server;
#[cfg(any(feature = "client", feature = "server"))]
mod wire;

/// Maximum successful response, including an accepted settlement and event.
/// See `docs/http-orchestration.md` for the current snapshot size derivation.
pub const RESPONSE_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Compact task status never includes application payloads or reports.
pub const STATUS_MAX_BYTES: usize = ledgence_orchestration_api::TASK_STATUS_MAX_BYTES;
/// Fork receipts contain up to 65 identifiers of 128 UTF-8 bytes each.
/// JSON escaping can double their size, exceeding the compact status budget.
pub const WORKFLOW_FORK_RECEIPT_MAX_BYTES: usize = 32 * 1024;
/// A malformed or larger error response cannot establish a domain rejection.
pub const ERROR_MAX_BYTES: usize = 64 * 1024;
