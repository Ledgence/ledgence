---
title: Releases and packages
description: Published Ledgence versions, installation channels, supported platforms, and the scope of each distribution.
---

Ledgence 0.1 is publicly available as source, a native release, a Python client, and Rust adapter contracts. Choose the distribution for the work you need; their currently published versions differ.

## Available distributions

| Distribution | Version |
| --- | --- |
| [Source tag](https://github.com/Ledgence/ledgence/tree/v0.1.1) | `v0.1.1` |
| [Native bundle](https://github.com/Ledgence/ledgence/releases/tag/v0.1.0) | `0.1.0` |
| [Python client on PyPI](https://pypi.org/project/ledgence-client/0.1.1/) | `0.1.1` |
| [Worker contracts on crates.io](https://crates.io/crates/ledgence-worker-api/0.1.1) | `0.1.1` |
| [Orchestration contracts on crates.io](https://crates.io/crates/ledgence-orchestration-api/0.1.1) | `0.1.1` |

The source distribution includes the platform code, local Compose deployment, examples, and detailed contracts.

The `v0.1.1` tag does not include a downloadable native bundle. The published native target is `aarch64-apple-darwin`; use the [source-based Compose tutorial](/tutorials/run-locally) on Linux or macOS. Compose builds an image locally; these instructions do not depend on a published Ledgence container image.

## Console in the current source tree

The self-hosted Console is implemented after the releases listed above. It is not included in the `v0.1.1` source tag or the native `0.1.0` archive. Do not use those artifacts for a Console setup.

[Explore Ledgence Console](/tutorials/use-console) starts from an existing source checkout containing `console/` and the updated Compose deployment. The [Console reference](/reference/console) covers the operator interface, single-instance configuration, and matching asset requirements. These source features do not change the independently published client or crate versions above.

## Native tools

The 0.1.0 archive contains three executables:

| Executable | Purpose |
| --- | --- |
| `ledgence` | Submit and inspect tasks through the HTTP API. |
| `ledgence-orchestrator` | Run the PostgreSQL-backed orchestration service and explicit migrations. |
| `ledgence-worker` | Publish packages, execute local fixtures, or connect a reusable worker to the service. |

CPython 3.11–3.14, PostgreSQL, brokers, and host system libraries are supplied separately. The release contains the small worker helper under `runtime/ledgence/worker/`; select a CPython interpreter matching each program's declared Python major/minor and OS/architecture.

The [native installation guide](/how-to/install-native) verifies both the archive checksum and its internal inventory before running an example. Keep the bundled source provenance and third-party legal notices with the installation.

## Python client

```sh
python3 -m pip install "ledgence-client==0.1.1"
```

Import it with `from ledgence.client import AsyncClient`. The client requires Python 3.11 or newer. Its qualified test matrix covers Python 3.11–3.14 on Linux x86_64/glibc and macOS arm64. An optional `otel` extra provides tracing API integration.

The client communicates with an existing service. It does not install a server, upload program packages, or provide `ledgence.worker`. See [Python client](/reference/python-client) for API contracts.

## Rust adapter contracts

The two published crates expose the interfaces used to integrate Ledgence with other Rust applications. They require Rust 1.98 or newer. Add the contracts your integration uses:

```toml
[dependencies]
ledgence-worker-api = "=0.1.1"
ledgence-orchestration-api = "=0.1.1"
```

Read their generated API documentation on docs.rs: [worker API 0.1.1](https://docs.rs/ledgence-worker-api/0.1.1/ledgence_worker_api/) and [orchestration API 0.1.1](https://docs.rs/ledgence-orchestration-api/0.1.1/ledgence_orchestration_api/).

These are libraries, not `cargo install` packages. Implementation crates and service binaries are built from source or obtained from the native bundle.

## Version and operating scope

The release tutorials and client references describe source and client version **0.1.1**, except where a page explicitly targets the native **0.1.0** release. Console pages explicitly target the newer current source implementation. Example programs with version `1.0.0` use their own application version; that number is separate from the Ledgence platform version.

The 0.1 API can evolve. Pin the versions you use and review release changes before upgrading. Use matching platform components and explicit database migrations, and preserve a tested backup before changing a durable deployment.

Ledgence runs operator-trusted programs. The local Compose setup uses loopback networking and demonstration credentials. Process reuse, durable checkpoints, and retries do not provide isolation for hostile code or exactly-once external effects; see [How work runs](/concepts/execution-model) and [Checkpoints and recovery](/concepts/checkpoints-and-recovery).

Ledgence-owned code is MIT licensed. Self-hosting requires no vendor account, and applications can remain proprietary. Third-party components retain their licenses and required notices.

**Source:** [Release history](https://github.com/Ledgence/ledgence/releases) · [Registry distribution contract](https://github.com/Ledgence/ledgence/blob/v0.1.1/docs/registry-packages.md)
