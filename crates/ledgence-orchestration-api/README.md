# ledgence-orchestration-api

Portable Rust contracts for Ledgence task orchestration and worker delivery:
submission, acquisition and leases, durable settlement, observation, completion
callbacks, retention, and resumable workflows with child tasks and events.

```toml
[dependencies]
ledgence-orchestration-api = "0.1.1"
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
See the [API documentation](https://docs.rs/ledgence-orchestration-api),
[delivery contract](https://github.com/Ledgence/ledgence/blob/main/docs/delivery-contract.md)
and [source repository](https://github.com/Ledgence/ledgence).

Ledgence-owned code is MIT licensed; the included LICENSE contains the notice.
This source crate does not bundle third-party dependency sources. Cargo obtains
those dependencies separately, with their own licenses and redistribution
notices. This license does not require users to disclose their applications.
