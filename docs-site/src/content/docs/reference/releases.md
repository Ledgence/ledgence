---
title: Releases and packages
description: Ledgence 0.4.0 distribution channels, supported native targets, upgrade requirements, and historical releases.
---

Ledgence 0.4.0 adds a one-line CLI installer, persistent shell setup, published container images, and a local stack managed with `ledgence local`. Source, native bundles, the local kit, the Python client, and Rust API crates use version **0.4.0**. Public APIs may evolve before 1.0; pin versions and review the [upgrade guide](/how-to/upgrade-to-0-4).

## Distributions

| Distribution | Version and scope |
| --- | --- |
| [Source tag](https://github.com/Ledgence/ledgence/tree/v0.4.0) | `v0.4.0`: platform, Console, Compose deployment, examples, and contracts. |
| [Native bundles](https://github.com/Ledgence/ledgence/releases/tag/v0.4.0) | `0.4.0`: Linux x86_64/glibc and macOS arm64, with Console. |
| [Installer and local kit](https://github.com/Ledgence/ledgence/releases/tag/v0.4.0) | `install.sh` and `ledgence-0.4.0-local.tar.gz`; the matching kit also ships in each native bundle. |
| [Container runtime on Docker Hub](https://hub.docker.com/r/ledgence/ledgence) | `0.4.0`: Linux amd64 and arm64, CPython 3.14, pinned by digest in the local kit. |
| [Python client on PyPI](https://pypi.org/project/ledgence-client/0.4.0/) | `ledgence-client==0.4.0` |
| [Worker contracts on crates.io](https://crates.io/crates/ledgence-worker-api/0.4.0) | `ledgence-worker-api = "=0.4.0"` |
| [Orchestration contracts on crates.io](https://crates.io/crates/ledgence-orchestration-api/0.4.0) | `ledgence-orchestration-api = "=0.4.0"` |

The Linux native target is `x86_64-unknown-linux-gnu`, built and qualified on Ubuntu 24.04. The macOS target is `aarch64-apple-darwin`. Archive provenance records actual dynamic-library requirements; Linux qualification is not a claim of compatibility with every distribution. The source-based [Compose tutorial](/tutorials/run-locally) builds its own Linux container image and does not require a published Ledgence container image.

## Install and run locally

[Install the CLI](/how-to/install-native#one-line-installation) with:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Open a new Bash/Zsh terminal, or source the setup file in the current shell:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

With Docker running Linux containers and Compose 2.23.1+, start the matching
stack with `ledgence local up`. The CLI uses the bundled kit, waits for readiness,
and prints the Console URL. No source checkout, Rust, Node, or host Python is
needed for this container stack. See [local lifecycle and data preservation](/how-to/run-local-distribution).

The [0.4.0 upgrade guide](/how-to/upgrade-to-0-4) distinguishes CLI installation
from service upgrades. Existing saved stacks retain their image and settings.

## Unified CLI

The native bundle contains one executable, `bin/ledgence`, with `program`, `worker`, `orchestrator`, `task`, `approval`, `mcp`, and `local` command groups. Inspect it with `ledgence --help` and `ledgence --version`. Worker and orchestrator still run as separate processes. Build from source with `cargo build --locked -p ledgence-cli`; see the [CLI reference and migration table](/reference/cli).

## Console

Console assets ship under `console/` in the native bundles and are built by the source Compose deployment. The Rust orchestrator serves them alongside its API; Node is only needed to build frontend source. Use matching Console contract **5** server and assets, and retain the server's immutable instance binding. Follow [Explore Ledgence Console](/tutorials/use-console) or [install native tools](/how-to/install-native).

## Durable agent operations

[Approval requests](/how-to/require-approval) bind a human decision to a persisted effective action. [Operation recovery](/how-to/recover-agent-calls) reuses acknowledged model/tool results with the same logical key and binding. The [MCP server](/how-to/connect-mcp) exposes tools over stdio through an existing HTTP API. Providers and agent frameworks remain application integrations; no provider account is required for self-hosting.

## Workflow entrypoints and forks

Register Python `Workflow` handlers with typed entrypoints and use acknowledged `fork` / `join` operations for independently checkpointed branches in the same immutable package. Upgrade orchestrator and workers together and apply all database migrations. Runtime protocol **3** remains the workflow package contract; existing `workflow_context()` controllers and string continuations remain supported.

[Mix local work and workflow branches](/how-to/fork-workflow-branches) shows the example and its execution limits. Public submissions select a workflow's default entrypoint. Branch creation and resume decisions can select named handlers.

## Python client

```sh
python3 -m pip install "ledgence-client==0.4.0"
```

Import `from ledgence.client import AsyncClient`. The client requires Python 3.11 or newer; the supported qualification matrix covers Python 3.11–3.14 on Linux x86_64/glibc and macOS arm64. The optional `otel` extra integrates tracing. This client communicates with an existing service; it does not install a server, upload packages, or provide `ledgence.worker`.

## Worker helper for development

The separate [local worker helper package](/how-to/develop-python-programs)
provides `ledgence.worker` for application development from updated `develop`
source. It is not published to PyPI and is absent from the existing `v0.4.0`
source tag; its local version `0.4.1` is being prepared for release and does not
claim registry availability.

## Rust adapter contracts

The two API crates expose integration interfaces and require Rust 1.98 or newer:

```toml
[dependencies]
ledgence-worker-api = "=0.4.0"
ledgence-orchestration-api = "=0.4.0"
```

Read the [worker API](https://docs.rs/ledgence-worker-api/0.4.0/ledgence_worker_api/) and [orchestration API](https://docs.rs/ledgence-orchestration-api/0.4.0/ledgence_orchestration_api/) documentation. These are libraries, not `cargo install` packages. Recompile custom adapters against the 0.4 crates. The approval variants and service operations introduced in 0.3 remain part of the contracts.

## Historical releases

**0.3.1** introduced durable human approvals, model/tool operation recovery and
MCP. It remains the complete published 0.3 release. Its native bundles have no
`ledgence local` command or local kit; see the [historical upgrade guide](/how-to/upgrade-to-0-3).

`v0.3.0` remains a historical source tag. Its native bundles, Python client and
Rust API crates were never publicly published. Use **0.3.1** for the 0.3 series;
the earlier tag is preserved without being moved or republished.

**0.2.0** introduced the unified CLI, Console contract 4, typed entrypoints and durable forks. It did not include the approval ledger, operation helper or MCP server. Use matching historical binaries and documentation when operating that version.

The first native release, **0.1.0**, shipped only for macOS arm64 with three executables: `ledgence`, `ledgence-worker`, and `ledgence-orchestrator`. Its Python client wheel also remains 0.1.0. The later **0.1.1** source and registry packages did not include a native archive. Both releases predate Console, typed entrypoints, and durable forks. Their original archives and tags are unchanged; use their own bundled documentation when operating them.

The `ledgence.worker` import namespace was already present in 0.1.1. The move from the older `ledgence_worker` spelling was not a new 0.3.1 migration. Example versions such as `1.0.0` and `1.0.1` identify application packages independently of platform releases.

## Operating scope

Native service deployments require separately supplied CPython, PostgreSQL,
brokers, and host libraries. The local container distribution supplies its
CPython runtime and PostgreSQL service. Ledgence executes operator-trusted code; reusable subprocesses do not isolate hostile programs. Checkpoints and retries do not provide exactly-once external effects. Preserve tested backups before upgrading a durable deployment.

Optional HTTP completion notifications, integrated or SQS-compatible queue
delivery, OTLP tracing/metrics, execution retention and task discovery are also
part of 0.4.0. See [results and callbacks](/how-to/receive-results),
[queue delivery](/concepts/queue-delivery), [observability](/how-to/configure-observability)
and the [capability map](/reference/capabilities) for their setup and limits.

Ledgence-owned code is MIT licensed and supports self-hosting without a mandatory vendor account. Applications can remain proprietary; third-party components retain their licenses and notices.

**Source:** [0.4.0 release notes](https://github.com/Ledgence/ledgence/blob/ae6734a2dfa58d931c3e3fcfa0e791382bfe15bf/docs/releases/0.4.0.md) · [Release history](https://github.com/Ledgence/ledgence/releases) · [Registry contract](https://github.com/Ledgence/ledgence/blob/ae6734a2dfa58d931c3e3fcfa0e791382bfe15bf/docs/registry-packages.md)
