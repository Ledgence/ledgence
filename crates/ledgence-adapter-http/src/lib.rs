//! Version-one HTTP/JSON transport for the portable task service.
//!
//! Enable `client` and/or `server` explicitly. Each client call performs one
//! bounded exchange; durable retries belong to the delivery driver or caller.

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
mod response;
#[cfg(feature = "client")]
pub use client::{ExchangeMetadata, HttpTaskService};
#[cfg(feature = "server")]
pub mod server;
#[cfg(any(feature = "client", feature = "server"))]
mod wire;

/// Maximum successful response, including an accepted settlement and event.
/// See `docs/http-orchestration.md` for the current snapshot size derivation.
pub const RESPONSE_MAX_BYTES: usize = 16 * 1024 * 1024;
/// A malformed or larger error response cannot establish a domain rejection.
pub const ERROR_MAX_BYTES: usize = 64 * 1024;
