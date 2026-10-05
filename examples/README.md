# Ledgence examples

Start with a Python program, explore durable workflow patterns, or run an agent
with an optional provider integration. This catalog also includes client and
transport examples. Follow each linked setup before running the code; commands
normally run from the repository root. The 0.3.1 examples use one
`ledgence` executable, built with `cargo build -p ledgence-cli --locked`. The
[CLI guide](../docs/cli.md) maps commands from earlier releases. Example tooling
accepts `--binaries DIRECTORY` pointing to the directory containing `ledgence`.

## Programs and workflows

| Example | What it shows | Setup |
| --- | --- | --- |
| [Python program](python/README.md) | A synchronous handler, CloudEvent input, contextual logs, and the subprocess PID. | Host CPython and a worker; start with [local execution](../README.md#run-a-program-from-source) and the [package contract](../docs/program-packages.md). No application dependencies. |
| [Python application spans](python-otel/README.md) | An application span inherits the worker's processing context and records into an in-memory buffer. | Package the reviewed OpenTelemetry dependencies listed in the example. See [worker tracing](../docs/observability.md); this example does not export Python spans over the network. |
| [Checkpoint workflow](checkpoint-workflow/README.md) | Typed entrypoints, bounded concurrent page fetches, acknowledged local results, and explicit summary-task failure handling. | Rust, host CPython 3.11+, PostgreSQL 18, and the local HTTP server described in the guide. Completes with worker concurrency one. |
| [Durable action approval](durable-approval/README.md) | Review effective arguments before a simulated refund, suspend without a worker slot, and reconcile the exact decision across retries. | Build matching components and client from 0.3.1 source with all migrations. Includes an offline check; PostgreSQL acceptance is documented in the [approval guide](../docs/workflow-approvals.md). |
| [Owned subworkflows](owned-subworkflows/README.md) | A typed parent joins a nested page-processing workflow and an ordinary task, then handles failed or cancelled children. | Build on the checkpoint example's published packages, orchestrator, page server, and worker. |
| [Mixed local and distributed workflow](mixed-workflow/README.md) | Validated input, typed entrypoints, acknowledged same-package forks, local work, an independent branch timer, and terminal-outcome handling after a durable join. | Matching 0.3.1 orchestrator and workers with all migrations applied; reuse the checkpoint example's setup. Completes with one worker slot; extra capacity permits overlap. |

Typed entrypoints and forks have been available since Ledgence 0.2.0. Use matching 0.3.1 components
and migrations for this checkout.
The historical 0.1 releases do not include these APIs; check
[their contract](../docs/workflow-entrypoints.md) before upgrading.

## Agent workflows

The change-review example proposes and validates a code change. The two support
examples research bundled Ledgence documentation, draft a reply, and wait for a
person to approve or reject that draft. The support examples do not send an
email or external reply.

| Example | Integration | Setup |
| --- | --- | --- |
| [Agent call recovery](agent-recovery/README.md) | Per-model and per-tool durable calls, explicit turn checkpoints, and recovery after a lost acknowledgment. Uses a scripted model and order service to isolate durability. | 0.3.1 source and CPython 3.11+ for the offline check; PostgreSQL and a worker for process recovery tests. No provider credentials. |
| [Codex change review](codex-change-review/README.md) | Fix an empty-page pagination bug with Codex, fork tests/review/release-note branches, compare behavior locally, and wait for candidate-bound human approval. Includes an animated workflow replay, code snippets and a recording guide. | 0.3.1 source, PostgreSQL 18, CPython 3.13, and a host Codex CLI. Includes an offline acceptance gate and a real-provider mode. |
| [Codex support agent](codex-support-agent/README.md) | Codex CLI with ChatGPT sign-in and access to the configured model. | 0.3.1 source, Rust, PostgreSQL 18, and CPython 3.13 on macOS arm64 or Linux x86_64. The application uses the Python standard library; Codex CLI is supplied separately. |
| [Google ADK support agent](support-agent/README.md) | Google ADK and Gemini Developer API. | 0.3.1 source, Rust, PostgreSQL 18, CPython 3.13, and a Gemini Developer API key. Reviewed dependency locks target macOS arm64 or Linux x86_64 with glibc 2.28+. |

Provider integrations are optional application dependencies, separate from the
Ledgence platform and Python client. Offline tests substitute external model
responses; explicitly enabled live checks use the actual provider.

## Data and agent workflows

These examples connect data preparation to an agent investigation and a reviewed
output. Their bundled sources are synthetic; a fixture agent is labelled
separately from an optional real-provider run.

| Example | What it shows | Setup |
| --- | --- | --- |
| [Fulfillment investigator](fulfillment-investigator/README.md) | Distinguish missing warehouse data from a delivery-delay signal. Ingest four sources, wait for the missing batch, investigate one verified snapshot in four branches, verify an agent report, and approve its local publication. | 0.3.1 components, CPython 3.11+, and a dedicated PostgreSQL database. Standard-library application; offline scripted-agent checks need no provider. Optional Codex adapter. One worker slot is sufficient; more slots permit overlap. |

This example uses durable operations and action approvals added in Ledgence
0.3.1. Its artifacts remain in a separate
shared local directory; it does not require a cloud account or configure a
remote lakehouse.

## Client and transport

| Example | What it shows | Setup |
| --- | --- | --- |
| [Installed Python client](local-compose-client.py) | Task and workflow submission, warm-process reuse, and persisted completion callbacks through an installed client wheel. | Start and publish the [local Compose stack](../docs/local-deployment.md#run-the-installed-python-client-example), install the client in a host environment, and keep worker concurrency at one for its reuse assertion. |
| [Local SQS delivery configuration](delivery-sqs-local.json) | Route dispatch references through a local SQS-compatible queue while PostgreSQL retains task authority. | Follow [SQS configuration](../docs/dispatch-delivery.md#executable-sqs-configuration): build with optional `sqs` support and create a dedicated Standard queue on the local broker. The file uses the HTTP quickstart's `tenant_example/demo/python-demo` binding; match the worker and adjust the queue URL. |
