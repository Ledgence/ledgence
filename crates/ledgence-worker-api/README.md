# ledgence-worker-api

Portable Rust contracts for Ledgence worker adapters: immutable program identity,
artifact preparation and cache leases, CloudEvents, cancellation and monotonic
deadlines, reusable execution sessions, runtime requests, metrics and tracing.

```toml
[dependencies]
ledgence-worker-api = "0.1.1"
```

```rust
use ledgence_worker_api::RunControl;
use std::time::Duration;

let control = RunControl::try_new(Duration::from_secs(30))?;
control.check()?;
# Ok::<(), ledgence_worker_api::Error>(())
```

Use these contracts to implement adapters. This crate does not launch workers,
retrieve programs, provide a process sandbox or implement durable orchestration.
The Ledgence executables and adapters are distributed from the source repository
and release bundles. Initial program execution assumes operator-trusted code.

Requires Rust 1.98 or newer. The pre-1.0 API may change between minor versions;
matching Ledgence components should use the same release series. See the
[API documentation](https://docs.rs/ledgence-worker-api),
[program package contract](https://github.com/Ledgence/ledgence/blob/main/docs/program-packages.md)
and [source repository](https://github.com/Ledgence/ledgence).

Ledgence-owned code is MIT licensed; the included LICENSE contains the notice.
This source crate does not bundle third-party dependency sources. Cargo obtains
those dependencies separately, with their own licenses and redistribution
notices. This license does not require users to disclose their applications.
