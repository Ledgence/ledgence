---
title: Releases and packages
description: Ledgence 0.2.0 distribution channels, supported native targets, upgrade requirements, and historical releases.
---

Ledgence 0.2.0 adds the unified CLI, self-hosted Console, and typed workflow entrypoints with durable forks. Source, native bundles, the Python client, and Rust API crates use version **0.2.0**. Public APIs may evolve before 1.0; pin versions and review the [upgrade guide](/how-to/upgrade-to-0-2).

## Distributions

| Distribution | Version and scope |
| --- | --- |
| [Source tag](https://github.com/Ledgence/ledgence/tree/v0.2.0) | `v0.2.0`: platform, Console, Compose deployment, examples, and contracts. |
| [Native bundles](https://github.com/Ledgence/ledgence/releases/tag/v0.2.0) | `0.2.0`: Linux x86_64/glibc and macOS arm64, with Console. |
| [Python client on PyPI](https://pypi.org/project/ledgence-client/0.2.0/) | `ledgence-client==0.2.0` |
| [Worker contracts on crates.io](https://crates.io/crates/ledgence-worker-api/0.2.0) | `ledgence-worker-api = "=0.2.0"` |
| [Orchestration contracts on crates.io](https://crates.io/crates/ledgence-orchestration-api/0.2.0) | `ledgence-orchestration-api = "=0.2.0"` |

The Linux native target is `x86_64-unknown-linux-gnu`, built and qualified on Ubuntu 24.04. The macOS target is `aarch64-apple-darwin`. Archive provenance records actual dynamic-library requirements; Linux qualification is not a claim of compatibility with every distribution. The source-based [Compose tutorial](/tutorials/run-locally) builds its own Linux container image and does not require a published Ledgence container image.

## Unified CLI

The native bundle contains one executable, `bin/ledgence`, with `program`, `worker`, `orchestrator`, and `task` command groups. Inspect it with `ledgence --help` and `ledgence --version`. Worker and orchestrator still run as separate processes. Build from source with `cargo build --locked -p ledgence-cli`; see the [CLI reference and migration table](/reference/cli).

## Console

Console assets ship under `console/` in the native bundles and are built by the source Compose deployment. The Rust orchestrator serves them alongside its API; Node is only needed to build frontend source. Use matching Console contract **4** server and assets, and retain the server's immutable instance binding. Follow [Explore Ledgence Console](/tutorials/use-console) or [install native tools](/how-to/install-native).

## Workflow entrypoints and forks

Register Python `Workflow` handlers with typed entrypoints and use acknowledged `fork` / `join` operations for independently checkpointed branches in the same immutable package. Upgrade orchestrator and workers together and apply all database migrations. Runtime protocol **3** remains the workflow package contract; existing `workflow_context()` controllers and string continuations remain supported.

[Mix local work and workflow branches](/how-to/fork-workflow-branches) shows the example and its execution limits. Public submissions select a workflow's default entrypoint. Branch creation and resume decisions can select named handlers.

## Python client

```sh
python3 -m pip install "ledgence-client==0.2.0"
```

Import `from ledgence.client import AsyncClient`. The client requires Python 3.11 or newer; the supported qualification matrix covers Python 3.11–3.14 on Linux x86_64/glibc and macOS arm64. The optional `otel` extra integrates tracing. This client communicates with an existing service; it does not install a server, upload packages, or provide `ledgence.worker`.

## Rust adapter contracts

The two API crates expose integration interfaces and require Rust 1.98 or newer:

```toml
[dependencies]
ledgence-worker-api = "=0.2.0"
ledgence-orchestration-api = "=0.2.0"
```

Read the [worker API](https://docs.rs/ledgence-worker-api/0.2.0/ledgence_worker_api/) and [orchestration API](https://docs.rs/ledgence-orchestration-api/0.2.0/ledgence_orchestration_api/) documentation. These are libraries, not `cargo install` packages. Recompile custom adapters against the 0.2 contracts and account for the new workflow and observation interfaces.

## Historical releases

The first native release, **0.1.0**, shipped only for macOS arm64 with three executables: `ledgence`, `ledgence-worker`, and `ledgence-orchestrator`. Its Python client wheel also remains 0.1.0. The later **0.1.1** source and registry packages did not include a native archive. Both releases predate Console, typed entrypoints, and durable forks. Their original archives and tags are unchanged; use their own bundled documentation when operating them.

The `ledgence.worker` import namespace was already present in 0.1.1. The move from the older `ledgence_worker` spelling is not a new 0.2.0 migration. Example versions such as `1.0.0` and `1.0.1` identify application packages independently of platform releases.

## Operating scope

CPython, PostgreSQL, brokers, and host libraries are supplied separately. Ledgence executes operator-trusted code; reusable subprocesses do not isolate hostile programs. Checkpoints and retries do not provide exactly-once external effects. Preserve tested backups before upgrading a durable deployment.

Ledgence-owned code is MIT licensed and supports self-hosting without a mandatory vendor account. Applications can remain proprietary; third-party components retain their licenses and notices.

**Source:** [0.2.0 release notes](https://github.com/Ledgence/ledgence/blob/v0.2.0/docs/releases/0.2.0.md) · [Release history](https://github.com/Ledgence/ledgence/releases) · [Registry contract](https://github.com/Ledgence/ledgence/blob/v0.2.0/docs/registry-packages.md)
