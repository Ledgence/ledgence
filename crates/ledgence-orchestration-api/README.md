![Ledgence](https://ledgence.com/brand/ledgence-logo-v1.png)

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

In development, `WorkflowStore::approved_effects` and
`WorkflowService::approved_effects` expose the optional `ApprovedEffects` port.
An adapter must implement both `claim_approved_effect` and
`confirm_approved_effect` to enable protected external writes.
Bind the one-use dispatch grant to the saved action and its read-only lookup
callback. Confirm the matching journal entry and the effect outcome in one
transaction. An exact retry of a confirmation can acknowledge the existing
result. An unresolved grant keeps the associated workflow evidence available
during retention. [Protected external writes](https://github.com/Ledgence/ledgence/blob/develop/docs/workflow-approvals.md#protect-an-approved-external-write)

Development builds expose `CredentialVerifier`, `AccessAudience` and
`VerifiedPrincipal` for installation-scoped authentication. The HTTP adapter
checks each route's required `AccessCapability` before dispatching to services.
The local opaque-credential implementation lives in a separate adapter.
[Instance access contract](https://github.com/Ledgence/ledgence/blob/develop/docs/instance-access.md)

A workflow adapter can implement `WorkflowStore::approval_inbox` to list scoped
approval summaries across workflows. Console adapters can implement
`ConsoleQueryStore::explore_approval` to start a graph page at its recorded
approval wait. Subsequent pages use the ordinary workflow explorer cursor.
The optional `ConsoleQueryStore::inspect_local_step` method reads a retained
accepted result by workflow, activation and step key within the bound scope.
It carries the exact result, effective arguments and accepting attempt.
Returned model and tool records use the versioned durable operation binding.
Existing adapters return `ContractError::Unavailable` for these optional reads
until they implement them.

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


Development builds add the grouped `OperatorAccess` capability for verified
approval decisions, durable sensitive-response observations and scoped audit
queries. All three operations are required by native credential mode.
`VerifiedPrincipal` and `OperatorRequest` enter through trusted adapters.
Existing approval commands and worker continuation schemas remain unchanged.
`AuditReader` is the separate read-only client port for authenticated remote
queries. [Operator audit contract](https://github.com/Ledgence/ledgence/blob/develop/docs/operator-audit.md).
