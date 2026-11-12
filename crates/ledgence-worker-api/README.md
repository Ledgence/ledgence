![Ledgence](https://ledgence.com/brand/ledgence-logo-v1.png)

# ledgence-worker-api

Portable Rust contracts for Ledgence worker adapters: immutable program identity,
artifact reading and preparation, publication, CloudEvents, cancellation and
monotonic deadlines, reusable execution sessions, runtime requests, metrics and
tracing. Concrete storage, transport and process implementations live in separate
adapter crates.

This README documents `ledgence-worker-api` 0.10.0. Use matching Ledgence 0.10
components. See [GitHub releases](https://github.com/Ledgence/ledgence/releases)
for verified release availability. For a local source build, use the checkout
path shown below.

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

`ProgramPackageProfile` selects either `Legacy4096` or the opt-in
`Entries8192V1` contract. `PublicationLimits::default()` retains the 4,096-member
legacy limit; the expanded profile permits 8,192 raw ZIP members, with the same
byte limits. `ProgramPackageInspector` verifies the retained archive without
executing it. Profile selection is transport-local and does not add fields to the
manifest or capabilities JSON. See the [package profile contract](https://github.com/Ledgence/ledgence/blob/v0.10.0/docs/program-publication.md#adapter-boundary-and-evidence).

`RuntimeFeatures` includes negotiation for `execution_artifacts.v1`; its default
advertises no optional features. The feature enables invocation-scoped execution
file requests through a capable host. Storage, authorization and durable
acceptance belong to the separate orchestration ports, not this feature flag or
the program archive cache. See the [execution artifact contract](https://github.com/Ledgence/ledgence/blob/v0.10.0/docs/execution-artifacts.md).

Use these contracts to implement adapters. This crate does not launch workers,
retrieve programs, provide a process sandbox or implement durable orchestration.
The unified `ledgence` executable and concrete adapters are distributed from the
source repository and release bundles. Program execution assumes
operator-trusted code.

Requires Rust 1.98 or newer. The pre-1.0 API may change between minor versions;
matching Ledgence components should use the same release series. See the
[0.10.0 API documentation](https://docs.rs/ledgence-worker-api/0.10.0/ledgence_worker_api/),
[program package contract](https://github.com/Ledgence/ledgence/blob/v0.10.0/docs/program-packages.md),
[publication contract](https://github.com/Ledgence/ledgence/blob/v0.10.0/docs/program-publication.md)
and [GitHub releases](https://github.com/Ledgence/ledgence/releases).
The [0.10.0 source](https://github.com/Ledgence/ledgence/tree/v0.10.0) contains the
implementations and integration examples.

Ledgence-owned code is MIT licensed; the included LICENSE contains the notice.
This source crate does not bundle third-party dependency sources. Cargo obtains
those dependencies separately, with their own licenses and redistribution
notices. This license does not require users to disclose their applications.

<span id="admission-feature-negotiation-in-development"></span>

### Admission feature negotiation

Matching 0.9.0 runtimes can negotiate `resource_admission.v1` for registered
callbacks and the distinct `provider_admission.v1` for atomic protected provider
dispatch. The latter requires both resource admission and provider recovery;
those capabilities alone do not imply composed support. Advertise it only when
the runtime and complete orchestration port implement the contract. Existing
operator-trusted programs and provider integrations remain application-owned.
[Admission behavior and limits](https://github.com/Ledgence/ledgence/blob/v0.10.0/docs/resource-admission.md).
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

Use version 0.10.0 of the worker API crate when targeting Ledgence 0.10.
