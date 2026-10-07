# ledgence-orchestration-api

Portable Rust contracts for Ledgence task orchestration and worker delivery:
submission, acquisition and leases, durable settlement, observation, completion
callbacks, retention, resumable workflows with child tasks and subworkflows,
events, and durable action approvals.

This README describes the current source. Check the
[registry package guide](https://github.com/Ledgence/ledgence/blob/develop/docs/registry-packages.md)
for verified published versions; a development checkout can contain contracts
that are not available from a registry yet. To develop an adapter against this
checkout:

```toml
[dependencies]
ledgence-orchestration-api = { path = "/absolute/path/to/ledgence/crates/ledgence-orchestration-api" }
```

```rust
use ledgence_orchestration_api::RetryPolicy;

let retries = RetryPolicy { max_attempts: 3, retry_delay_ms: 5_000 };
assert!(retries.validate().is_ok());
```

The contracts use `ledgence-worker-api` from the same release series. Adapters
must preserve the documented transactional, replay and ownership semantics before
acknowledging an operation. This crate provides no database, HTTP service, broker
or worker implementation; those components remain in the Ledgence source
repository and release bundles.

Requires Rust 1.98 or newer. The pre-1.0 API may change between minor versions.
See the [published API documentation](https://docs.rs/ledgence-orchestration-api),
[delivery contract](https://github.com/Ledgence/ledgence/blob/develop/docs/delivery-contract.md)
and [release reference](https://docs.ledgence.com/reference/releases).
The [source repository](https://github.com/Ledgence/ledgence) contains the
implementations and integration examples.

Ledgence-owned code is MIT licensed; the included LICENSE contains the notice.
This source crate does not bundle third-party dependency sources. Cargo obtains
those dependencies separately, with their own licenses and redistribution
notices. This license does not require users to disclose their applications.
