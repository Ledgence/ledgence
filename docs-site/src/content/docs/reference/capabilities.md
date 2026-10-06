---
title: Capabilities and availability
description: Find Ledgence 0.4.0 execution, integration, observability, and installation capabilities.
---

**0.4.0 is the current published release.** This page maps its capabilities to
user guides and operating limits. The installer, native bundles, Python client,
Rust API crates, container images, and local kit belong to the same release.
Development-only additions are identified separately below.

## Execution and composition

| Capability in 0.4.0 | Where to start |
| --- | --- |
| Package Python code with prepared dependencies; fetch and cache immutable artifacts on demand. | [Register a program](/how-to/register-agent) |
| Reuse a bounded process pool across programs, with one concurrency setting. | [Execution model](/concepts/execution-model) |
| Submit tasks and workflows, inspect status/results, and request cancellation. | [Python client](/reference/python-client) and [CLI](/reference/cli) |
| Find tasks by state, queue, business correlation and submission time, using bounded pagination. | [Task discovery contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/task-discovery.md) |
| Compose workflows using typed entrypoints, explicit local steps, distributed tasks and owned subworkflows. | [Your first workflow](/tutorials/first-workflow) and [workflow context](/reference/workflow-context) |
| Fork independently checkpointed branches, perform local work, and join selected branches. | [Mix local work and branches](/how-to/fork-workflow-branches) |
| Suspend for tasks, timers or external events and resume without retaining a waiting worker process. | [Wait for an event](/how-to/wait-for-event) |
| Reuse acknowledged model/tool results with stable operation keys and explicit bindings. | [Recover model and tool calls](/how-to/recover-agent-calls) |
| Bind durable human approval to the exact persisted action and effective arguments. | [Require approval](/how-to/require-approval) |
| Deliver terminal result notifications to a configured HTTP receiver after the client disconnects. | [Receive results](/how-to/receive-results) |

## Inspection and integrations

| Capability in 0.4.0 | Where to start |
| --- | --- |
| Browse Programs, Executions and Workers in the self-hosted Console. | [Explore Console](/tutorials/use-console) |
| Inspect a workflow's recorded graph or trace view, and navigate into child executions. | [Console reference](/reference/console) |
| Inspect and decide persisted approvals in Console, through the client or with the CLI. | [Require approval](/how-to/require-approval) |
| Connect an MCP host through 15 stdio tools, with an optional read-only mode. | [Connect an MCP client](/how-to/connect-mcp) |
| Export optional OTLP HTTP/protobuf traces and metrics, and write correlated Python logs. | [Configure observability](/how-to/configure-observability) |
| Use integrated HTTP acquisition or the optional SQS Standard / ElasticMQ adapter. | [Queue delivery](/concepts/queue-delivery) |
| Implement custom Rust ports and adapters without exposing provider SDK types in core contracts. | [Rust API packages](/reference/releases#rust-adapter-contracts) |
| Preview and apply scoped, bounded retention cleanup after at least 90 days of terminal retention. | [Retention contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/retention.md) |

Provider clients and agent frameworks belong to application packages. The
[example catalog](https://github.com/Ledgence/ledgence/tree/v0.4.0/examples)
includes framework-free workflow patterns, a Google ADK/Gemini support agent,
Codex workflows, durable approvals and operation recovery. Model access is a
prerequisite of the corresponding live example, not of Ledgence self-hosting.

## Installation channels

| Method | Availability |
| --- | --- |
| One-line installer with persistent shell setup. | **Published 0.4.0.** [Install the CLI](/how-to/install-native#one-line-installation). |
| Native archives for Linux x86_64/glibc and macOS Apple silicon. | **Published 0.4.0.** [Manual installation](/how-to/install-native#before-you-start). |
| Python client on PyPI and two Rust API crates on crates.io. | **Published 0.4.0.** [Package reference](/reference/releases). These do not install the server stack. |
| Docker Hub images for Linux amd64 and arm64, with a matching digest-pinned local kit. | **Published 0.4.0.** [Manage the stack with `ledgence local`](/how-to/run-local-distribution). |
| Build the local Docker Compose stack from source. | **Published 0.4.0 source.** [Source tutorial](/tutorials/run-locally). |
| Install `ledgence.worker` in an application's environment for imports, editor support and local business-logic tests. | **Development source only.** [Develop Python programs](/how-to/develop-python-programs). The local `ledgence-worker` package is absent from the existing `v0.4.0` tag and has no PyPI release. |

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
