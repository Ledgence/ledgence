![Ledgence](https://ledgence.com/brand/ledgence-logo-v1.png)

# ledgence-worker-api

Portable Rust contracts for Ledgence worker adapters: immutable program identity,
artifact reading and preparation, publication, CloudEvents, cancellation and
monotonic deadlines, reusable execution sessions, runtime requests, metrics and
tracing. Concrete storage, transport and process implementations live in separate
adapter crates.

This README describes the current source. Check the
[registry package guide](https://github.com/Ledgence/ledgence/blob/develop/docs/registry-packages.md)
for verified published versions; a development checkout can contain contracts
that are not available from a registry yet. To develop an adapter against this
checkout:

```toml
[dependencies]
ledgence-worker-api = { path = "/absolute/path/to/ledgence/crates/ledgence-worker-api" }
```

```rust
use ledgence_worker_api::RunControl;
use std::time::Duration;

let control = RunControl::try_new(Duration::from_secs(30))?;
control.check()?;
# Ok::<(), ledgence_worker_api::Error>(())
```

## Adapter contracts

| Contract | Responsibility |
| --- | --- |
| `ProgramStore` | Resolve an immutable program reference and fetch its archive. |
| `ArtifactCache` / `PreparedArtifact` | Verify and prepare an archive for local use, retaining a lease while sessions use it. Cache publication does not publish a program to an external store. |
| `ProgramArtifactPublisher` | Validate and persist an immutable program archive through a separate write port. |
| `ExecutionRuntime` / `ExecutionSession` | Start, reuse and close execution sessions while retaining process ownership until cleanup completes. |
| `InvocationAdmission` / `InvocationPermit` | Optionally authorize one invocation after its session is ready; check the monotonic send deadline immediately before writing the request. |
| `RuntimeRequestHandler` | Handle invocation-scoped runtime requests, with durable acknowledgement and stale-attempt fencing supplied by the implementation. |
| `TraceBridge` and `metrics` | Connect worker observations to optional telemetry adapters. |

A publication implementation must verify the supplied program identity, actual
length and digest, archive contents and limits before making a descriptor
resolvable. It exposes the complete durable blob before the descriptor and
reconciles identical bytes across processes; different bytes for an existing
program/version conflict. Once a write may have become visible, an unconfirmed
result is `OutcomeUnknown`, not proof of rollback. Accepted persistence work and
its admission permit remain owned until completion, including after a request
timeout. `PublicationCapabilities`, `PublicationLimits`, `PublishArtifactResult`
and typed `PublicationError` values describe the exchange without concrete HTTP,
Docker or storage SDK types.

Use these contracts to implement adapters. This crate does not launch workers,
retrieve programs, provide a process sandbox or implement durable orchestration.
The unified `ledgence` executable and concrete adapters are distributed from the
source repository and release bundles. Program execution assumes
operator-trusted code.

Requires Rust 1.98 or newer. The pre-1.0 API may change between minor versions;
matching Ledgence components should use the same release series. See the
[published API documentation](https://docs.rs/ledgence-worker-api),
[program package contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-packages.md),
[publication contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-publication.md)
and [release reference](https://docs.ledgence.com/reference/releases).
The [source repository](https://github.com/Ledgence/ledgence) contains the
implementations and integration examples.

Ledgence-owned code is MIT licensed; the included LICENSE contains the notice.
This source crate does not bundle third-party dependency sources. Cargo obtains
those dependencies separately, with their own licenses and redistribution
notices. This license does not require users to disclose their applications.

### Admission feature negotiation in development

Matching development runtimes can negotiate `resource_admission.v1` for registered
callbacks and the distinct `provider_admission.v1` for atomic protected provider
dispatch. The latter requires both resource admission and provider recovery;
those capabilities alone do not imply composed support. Advertise it only when
the runtime and complete orchestration port implement the contract. Existing
operator-trusted programs and provider integrations remain application-owned.
[Admission behavior and limits](https://github.com/Ledgence/ledgence/blob/develop/docs/resource-admission.md).
The independent `task_admission.v1` capability governs a plain task's entrypoint
without adding a Python helper. Its worker prepares the program, then requests
one start grant. A replayed receipt or lost reply does not authorize invocation.
The subprocess adapter checks the grant again immediately before sending the
invocation frame. Preparation imports the trusted Python module; admission does
not guard import-time side effects. Quiescence covers the invocation and managed
local cleanup, not detached work or remote effects. A custom runtime opts in through `ExecutionRuntime::supports_admitted_invocation`.
The default is false: its workers can still run ordinary tasks and leave governed
ones available for capable workers. A prepared session must confirm support
again before requesting a grant.

These additions are not included in the published 0.8.0 crate.
