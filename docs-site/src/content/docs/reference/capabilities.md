---
title: Capabilities and availability
description: Find published Ledgence capabilities, setup guides and operating limits.
---

**0.8.0 is the current published release.** This page maps its capabilities to
user guides and operating limits. The installer, native bundles, Python client,
worker helper, Rust API crates, container images, and local kit belong to the
same release.

For a new local installation, start with [Choose your installation](/getting-started/installation),
then [Run your first task](/tutorials/first-task). This page is a feature map.

## Program authoring

The 0.8.0 CLI creates functional task and workflow projects with both
`ledgence-worker` and `ledgence-client` pinned as development dependencies.

| Capability | Where to start |
| --- | --- |
| Scaffold a Python task or workflow with `ledgence program new`, including code, tests, development dependencies and explicit build configuration. | [Create a project](/how-to/develop-python-programs#create-a-project-with-the-cli) and [CLI contract](/reference/cli#create-a-python-program) |

Scaffolding runs offline. Installing dependencies, choosing the worker target,
building, publishing and execution remain explicit operations.

## Execution and composition

| Capability in 0.8.0 | Where to start |
| --- | --- |
| Package Python code with prepared dependencies; fetch and cache immutable artifacts on demand. | [Register a program](/how-to/register-agent) |
| Reuse a bounded process pool across programs, with one concurrency setting. | [Execution model](/concepts/execution-model) |
| Submit tasks and workflows, inspect status/results, and request cancellation. | [Python client](/reference/python-client) and [CLI](/reference/cli) |
| Find tasks by state, queue, business correlation and submission time, using bounded pagination. | [Task discovery contract](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/task-discovery.md) |
| Compose workflows using typed entrypoints, explicit local steps, distributed tasks and owned subworkflows. | [Your first workflow](/tutorials/first-workflow) and [workflow context](/reference/workflow-context) |
| Fork independently checkpointed branches, perform local work, and join selected branches. | [Mix local work and branches](/how-to/fork-workflow-branches) |
| Suspend for tasks, timers or external events and resume without retaining a waiting worker process. | [Wait for an event](/how-to/wait-for-event) |
| Reuse acknowledged model/tool results with stable operation keys and explicit bindings. | [Recover model and tool calls](/how-to/recover-agent-calls) |
| Bind durable human approval to the exact persisted action and effective arguments. | [Require approval](/how-to/require-approval) |
| Deliver terminal result notifications to a configured HTTP receiver after the client disconnects. | [Receive results](/how-to/receive-results) |

## Inspection and integrations

| Capability in 0.8.0 | Where to start |
| --- | --- |
| Browse Programs, Executions, Approvals, Effects and Workers in the self-hosted Console. | [Explore Console](/tutorials/use-console) |
| Inspect a workflow's recorded graph or trace view, and navigate into child executions. | [Console reference](/reference/console) |
| Inspect and decide persisted approvals in Console, through the client or with the CLI. | [Require approval](/how-to/require-approval) |
| Connect an MCP host through stdio. With audit configured, the native catalog has 19 tools by default, 14 in read-only mode or 20 with explicit effect resolution. | [Connect an MCP client](/how-to/connect-mcp) |
| Export optional OTLP HTTP/protobuf traces and metrics, and write correlated Python logs. | [Configure observability](/how-to/configure-observability) |
| Use integrated HTTP acquisition or the optional SQS Standard / ElasticMQ adapter. | [Queue delivery](/concepts/queue-delivery) |
| Implement custom Rust ports and adapters without exposing provider SDK types in core contracts. | [Rust API packages](/reference/releases#rust-adapter-contracts) |
| Preview and apply scoped, bounded retention cleanup after at least 90 days of terminal retention. | [Retention contract](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/retention.md) |

Provider clients and agent frameworks belong to application packages. The
[example catalog](https://github.com/Ledgence/ledgence/blob/develop/examples/README.md)
includes framework-free workflow patterns, a Google ADK/Gemini support agent,
Codex workflows, durable approvals and operation recovery. Model access is a
prerequisite of the corresponding live example, not of Ledgence self-hosting.

### Durable operation inspection

From a workflow graph or trace, open a local step to see the arguments, accepted
result and recording attempt. Model and tool steps also show the version saved
with the call. [Inspect an accepted operation](/reference/console#inspect-an-accepted-operation).

### Approval inbox

Find pending actions across workflow runs within the instance or configured
scope. Console, CLI, Python and MCP share the same deadline-ordered listing.
[Find pending requests across workflows](/how-to/require-approval#find-pending-requests-across-workflows).

<span id="in-development-approved-external-writes"></span>

## Approved external writes

A workflow using the 0.8.0 helper can declare a protected action before
approval, authorize one dispatch, restrict later attempts to receipt lookup and
keep unresolved evidence past the ordinary retention period. Its saved contract
rejects execution through the ordinary approval helper.
[Protect an approved write](/how-to/require-approval#protect-an-approved-write-in-development).

<span id="instance-access-in-development"></span>

## Instance access

The optional self-hosted credential adapter authenticates operators, service
clients and workers across HTTP, CLI, Python, MCP and Console. Capabilities
separate metadata, sensitive payloads, submission, control, publication, approval
and worker execution. Console binds cached data and pending commands to the
current actor. Existing operator-trusted deployments keep their access boundary.
[Configure instance access](/how-to/configure-instance-access).

<span id="external-effect-evidence-in-development"></span>

## External-effect evidence

Matching 0.8.0 components retain approved effect occurrences, authenticated
worker claims and reported results independently of execution history. Inspect
unconfirmed or discrepant cases through the API, CLI, Python, MCP and Console. Sensitive
inspection requires credentials and durable audit. Historical imports retain
the provenance actually available at upgrade.
[Inspect external effects](/how-to/inspect-effects).

<span id="native-effect-resolution-in-development"></span>

## Native effect resolution

An operator can record success, failure or an unknown outcome against an exact
evidence revision and approved action. Saved commands preserve their content for
explicit retry. Workflows can wait durably for the assessment, release their
worker slot and resume at a registered entrypoint with a typed outcome. Console
shows retained evidence and links native waits to the original effect occurrence.
Later conflicting worker reports remain visible, including after execution
records are collected. Matching 0.8.0 components, credentials, operator
audit and database migrations are required.
[Inspect and resolve effects](/how-to/inspect-effects).

<span id="provider-recovery-in-development"></span>

## Provider recovery

Matching 0.8.0 components support approval-bound provider operations with
business and provider deduplication identities, same-package registered adapters,
a single dispatch grant, bounded read-only probes and typed workflow resumption.
The Effects view exposes provider knowledge, recovery budget and operator
handling. An explicit policy file controls dispatch and declared-restore
quarantine. Use a dedicated matching-worker probe queue.
[Recover provider operations](/how-to/recover-provider-operations).

## Installation channels

| Method | Availability |
| --- | --- |
| One-line installer with persistent shell setup. | **Published 0.8.0.** [Install the CLI](/how-to/install-native#one-line-installation). |
| Native archives for Linux x86_64/glibc and macOS Apple silicon. | **Published 0.8.0.** [Manual installation](/how-to/manual-native-installation). |
| Python client and worker authoring helper on PyPI and two Rust API crates on crates.io. | **Published 0.8.0.** [Package reference](/reference/releases). These do not install the server stack. |
| Docker Hub images for Linux amd64 and arm64, with a matching digest-pinned local kit. | **Published 0.8.0.** [CLI-managed installation](/how-to/install-native) or [direct Compose](/how-to/install-compose). |
| Build the CLI from the released source tag and use the published runtime kit. | **Published 0.8.0 source.** [Compile the CLI](/how-to/build-cli-from-source). |
| Build the local Docker Compose stack from source. | **Published 0.8.0 source.** [Source tutorial](/tutorials/run-locally). |
| Install `ledgence.worker` in an application's environment for imports, editor support and local business-logic tests. | **Published 0.8.0 on PyPI.** [Develop Python programs](/how-to/develop-python-programs). Add `ledgence-worker` as a development dependency matching the worker. |

## Program preparation and publication

Ledgence 0.8.0 includes explicit Docker preparation for Linux worker
runtimes, immutable HTTP artifact uploads and saved retry receipts. Optional
subsequent registration is bound to the exact uploaded descriptor. The existing
filesystem publication path remains supported. See [Build and publish Python
programs](/how-to/build-and-publish-programs) for prerequisites, configuration,
limits and interrupted-operation recovery.

These features require matching 0.8.0 CLI/server components and explicit server
write enablement. They are not silently added to existing saved local kits. Docker belongs to the local builder; prepared ZIP uploads need no
Docker dependency on the server. Program identity remains global within each
store, with one artifact per program ID and version; there is no multiarchitecture
variant selection or automatic remote storage configuration.

## Boundaries that matter

Tasks are leaf execution units. Workflows compose work; internal Python code
becomes a durable local node only when registered explicitly. A graph comes
from persisted Ledgence identities and relationships; optional OTel spans add
diagnostics and are not the execution ledger.

Ledgence resumes explicit checkpoints and entrypoints, not an arbitrary Python
stack. A provider call or external effect can repeat after an uncertain reply;
applications need idempotency or reconciliation for those effects. Running code
is operator-trusted. The self-hosted Console binds to one configured instance,
without a tenant-administration interface.

PostgreSQL remains the authority even with an external queue. Queue priorities,
cost-based worker placement, Kafka/Kinesis/RabbitMQ adapters, hostile-code
isolation, and checkpointing a provider's partial token stream are not built-in
capabilities in this release. Capacity depends on the deployed workload and must
be measured; these features do not establish a production throughput guarantee.


<span id="operator-audit-in-development"></span>

## Operator audit

Matching 0.8.0 components add verified approval acceptance and durable sensitive-response observations, exposed through API, CLI, Python, MCP and Console. Pending result polling creates no observation rows.
[Setup, queries and operating limits](/how-to/audit-operators).

## In development: durable resource admission

Matching development components add shared capacity and token-bucket policies
for registered workflow callbacks. Workflows park without a worker slot, then
resume under a one-shot Start permission. Accepted results survive worker loss;
uncertain usage remains occupied. CLI, Python, HTTP and MCP expose policy and
inspection operations, and Console C14 renders the durable wait/result. Protected
provider dispatch can share a resource through a combined one-shot grant; its
call completion remains separate from the provider's business outcome. Recovery
probes are outside that resource limit. Standalone tasks can require the same
resource before handler invocation. Resources can give eligible queue groups weighted turns at admission. Console shows committed offers with their reasons. Run budgets bound governed calls and delegations across a root and its owned executions. [Budget guide](/how-to/share-run-budgets). This is not in the 0.8.0 packages. [Guide and limits](/how-to/control-resource-capacity).

Matching development builds let operators edit named-resource policies in Console, inspect decisions and find outstanding sources using the resource name. Aggregate inspection uses `metadata_read`. Listing individual sources requires `sensitive_read` and records a response audit.


## In development: worker poll shares

[Configure a worker to poll multiple integrated or SQS Standard queues](/how-to/share-worker-polls)
with one concurrency budget. Choose deterministic Stride or weighted lottery,
then adjust the weights without interrupting active tasks.

## Queue concurrency in development

Limit outstanding attempts across worker replicas, independently of each worker’s local poll weights. Inspect occupied and uncertain slots and edit a limit from Console, CLI, HTTP, Python or MCP. [Limit a queue across workers](/how-to/limit-queue-concurrency).
