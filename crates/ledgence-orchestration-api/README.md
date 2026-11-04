![Ledgence](https://ledgence.com/brand/ledgence-logo-v1.png)

# ledgence-orchestration-api

Portable Rust contracts for Ledgence task orchestration and worker delivery:
submission, acquisition and leases, durable settlement, observation, completion
callbacks, retention, resumable workflows with child tasks and subworkflows,
events, and durable action approvals.

Version 0.9.0 is available from crates.io. Pin it in an adapter targeting Ledgence 0.9.0. For a local source build, use the checkout path shown below. [Release reference](https://docs.ledgence.com/reference/releases).

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

`WorkflowStore::approved_effects` and
`WorkflowService::approved_effects` expose the optional `ApprovedEffects` port.
An adapter must implement both `claim_approved_effect` and
`confirm_approved_effect` to enable protected external writes.
Bind the one-use dispatch grant to the saved action and its read-only lookup
callback. Confirm the matching journal entry and the effect outcome in one
transaction. An exact retry of a confirmation can acknowledge the existing
result. An unresolved grant keeps the associated workflow evidence available
during retention. [Protected external writes](https://github.com/Ledgence/ledgence/blob/develop/docs/workflow-approvals.md#protect-an-approved-external-write)

`CredentialVerifier`, `AccessAudience` and
`VerifiedPrincipal` provide installation-scoped authentication. The HTTP adapter
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

Standalone task admission uses the grouped
`TaskAdmissionStore` and `TaskAdmissionClient` ports. An adapter must bind
the authenticated worker to its session, grant at most one executable Start,
retain uncertain occupancy, and accept source-bound cleanup independently
of ordinary result settlement. It must implement the corresponding transactional
submission, acquisition, maintenance, cancellation, retention and recovery paths
before advertising this capability. Generic acquire/renew replies never grant
permission for a governed invocation. [Standalone task contract](https://github.com/Ledgence/ledgence/blob/develop/docs/resource-admission.md#standalone-task-admission)

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


The grouped `OperatorAccess` capability supports verified
approval decisions, durable sensitive-response observations and scoped audit
queries. All three operations are required by native credential mode.
`VerifiedPrincipal` and `OperatorRequest` enter through trusted adapters.
Existing approval commands and worker continuation schemas remain unchanged.
`AuditReader` is the separate read-only client port for authenticated remote
queries. [Operator audit contract](https://github.com/Ledgence/ledgence/blob/develop/docs/operator-audit.md).

The optional grouped `ApprovedEffectEvidence` port supports
verified claims, confirmation and separate observations. Implement all three
methods together. `EffectEvidenceClient` carries the portable commands across
a remote boundary. `EffectEvidenceReader` provides bounded inbox and inspection
queries over retained occurrences and immutable entries. Existing workflow-store
adapters remain source-compatible through default unavailable capability hooks.
The retained ledger preserves claims and reported results after execution
history is collected. Its identity, replay and legacy-proof rules are described
in the [evidence contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md).

`EffectResolution` supports authenticated operator
assessments and `EffectResolutionStore` provides the complete native-wait coordinator
operation. `EffectResolutionClient` is the command-only remote port. Native waits
carry the original effect occurrence and an immutable selected wake, without
creating a provider result or another dispatch grant. Implementations must also
provide retained evidence, its reader and operator audit before enabling this
capability. [Native effect resolution](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md#record-an-operator-assessment).

`ProviderOperationStore` adds the complete optional provider-recovery contract:
request-time binding, one mutation grant, typed reports, finite read-only probe
scheduling and provider-aware waits. `ProviderOperations` accepts verified worker
identity out of band; `ProviderOperationClient` carries remote commands. Recovery
requires explicit installation policy and the preceding evidence, resolution and
audit capabilities. Existing A/B reports remain opaque and are not reclassified
as provider facts. No vendor SDK is part of these portable types.
[Provider operations](https://github.com/Ledgence/ledgence/blob/develop/docs/provider-operations.md).

Adapters receiving `WorkflowActivationContext` should use `validate_for_owner`
to bind the context and all scoped wake variants to the current lease, then
check the expected workflow and parent/root lineage from their assignment.
The application service applies complete-capability checks to direct evidence
and resolution calls as well as discovery and native-wait admission.

### Resource and protected provider admission

This crate exposes portable resource policies, durable admission waits
and one-shot Start/Finish contracts. `ProviderAdmissionStore` is a separate
complete capability: its implementation must commit provider dispatch permission,
capacity accounting and recovery obligation atomically. Implementing the separate
provider and resource ports does not imply this guarantee. Default capability
hooks remain unavailable until the composed store is supplied.

Version-2 provider bindings freeze the reviewed resource requirement; ungoverned
version-1 bindings preserve their encoding. A linked provider report distinguishes
call completion from business knowledge, so a pending asynchronous operation can
outlive its original call's occupancy. Read-only probes are outside that resource
policy. Select the 0.9 API for these contracts.
[Admission contract](https://github.com/Ledgence/ledgence/blob/develop/docs/resource-admission.md).

Resource admission adapters opt into weighted offers through `supports_resource_fair_sharing()`, which defaults to false. An adapter advertising support must reserve capacity, advance the scoped resource cursor and retain its decision atomically, and implement scope-bound paginated reads. FIFO-only adapters can continue serving policies without `fair_sharing`. The service rejects a weighted policy or decision query when the active adapter lacks support. [Weighted offer contract](https://github.com/Ledgence/ledgence/blob/develop/docs/resource-admission.md#weighted-offers).

The crate exposes `QueuePolicyClient` for authenticated management of
queue concurrency. A policy can set a shared maximum, pause new assignments with
zero or explicitly remove the maximum. Inspecting a hold shows the policy revision
and timeout schedule that applied to its original attempt. `QueuePolicyOperations`
binds each request to a verified principal, and `QueuePolicyStore` implements the
transactional authority. Existing adapters can explicitly omit this capability.
A close receipt is bound to the full worker source, including the original lease,
without exposing that lease in operator inspection. Mutation retries reuse the
same request ID, expected revision and arguments.

Run budget support is an explicit adapter capability. A capable store must derive root membership from ownership and commit each delegation or call decision with the state change it authorizes. The `WorkflowStore::budgets()` and `TaskStore::supports_run_budgets()` defaults leave this capability unavailable. `RunBudgetClient` exposes scope-bound metadata. A custom worker may opt into fresh native grants with `HttpTaskService::with_budget_dispatch_fence` only when it enforces the returned send window at callback invocation. An adapter without the capability must reject budgeted submissions. [Contract and limitations](https://github.com/Ledgence/ledgence/blob/develop/docs/run-budgets.md).
