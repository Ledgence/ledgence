---
title: Upgrade to 0.6.0
description: Install the CLI with Python project scaffolding, update matching components, and preserve existing programs and local data.
---

Ledgence 0.6.0 adds `ledgence program new` for functional Python tasks and
workflows, fixes Docker build cleanup and Console execution inspection, and
updates the TLS and ZIP dependencies. Both Python packages and the two Rust API
crates are published at 0.6.0.

There are **no new PostgreSQL migrations relative to 0.5.0**. Console contract
**5**, workflow runtime protocol **3**, and the public Rust and Python API
contracts are unchanged. For older installations, review the
[0.5 upgrade guide](/how-to/upgrade-to-0-5) and its earlier migration requirements.

## Install the matching CLI

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.6.0/install.sh | sh -s -- --version 0.6.0
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

New Bash/Zsh terminals load the configured PATH automatically. A piped installer
cannot change its parent shell; source the environment file as shown. Manual
installation and source builds remain available in [Install the CLI](/how-to/install-native).

Native targets remain Linux x86_64/glibc and macOS arm64. Container runtimes
support Linux amd64 and arm64 with CPython 3.14. Choose a program target matching
the worker's architecture, Python major/minor and native libraries.

## Create new programs

For the development and test commands below, install Python 3.11 or newer and uv.

```sh
ledgence program new invoice-issuer
cd invoice-issuer
uv sync
uv run python -m unittest discover -s tests -v
```

Add `--kind workflow` when creating a project to generate a workflow with an
enum-based default entrypoint. Both starters implement a working greeting and
pin `ledgence-worker` and `ledgence-client` as development dependencies to the
CLI's version. The destination must be new; scaffolding runs offline and does
not install dependencies, build, publish or execute anything.

Program creation is consolidated on `program new`. Scripts that previously
relied on the legacy program-fixture generator must prepare their package and
invocation fixture explicitly, or use the new-project build and publication
flow. A generated source project is not an already prepared package or task
file. See [Python authoring](/how-to/develop-python-programs) and
[build and publication](/how-to/build-and-publish-programs).

Existing program artifacts are not rewritten. Continue using their immutable
identities. Keep publication receipts with their saved ZIP to reconcile an
uncertain upload; publish changed archive bytes under a new application version.
Application versions such as `1.0.0` are independent of platform versions.

## Update a retained service deployment

1. Retain tested backups of PostgreSQL, the program store and the server-owned
   instance configuration, together with the previous bundle and service options.
2. Plan the restart, stop old writers and replace the orchestrator, workers,
   runtime helper and Console assets with matching 0.6.0 components. Update
   explicit `--runner` or `--console-dir` paths if configured.
3. Preserve the database, artifact store, queues and immutable instance binding.
   Run `ledgence orchestrator migrate` against the intended database; an
   up-to-date 0.5.0 schema needs no new migrations. The server still verifies it.
4. Restore services and verify readiness, a known task or workflow, its retained
   history, and Console checkpoint navigation and Trace rendering.

Existing publication-writer settings remain explicit. A CLI update does not
enable uploads, configure artifact replication or restart services.

## Preserve saved local installations

A CLI-managed installation retains its original kit, pinned image, Docker
context, port, concurrency, volumes and project identity after installation of
a new CLI. Do not edit checksummed kit files or delete volumes as an upgrade.

Evaluate 0.6.0 in a **new directory** with a free port:

```sh
ledgence local up --directory "$HOME/.local/share/ledgence/local-0.6.0" --port 8086
ledgence local status --directory "$HOME/.local/share/ledgence/local-0.6.0"
ledgence local down --directory "$HOME/.local/share/ledgence/local-0.6.0"
```

Repeat that directory for subsequent commands. `local down` preserves data.
The new stack has separate data; it does not import an earlier installation.
Moving retained data requires an explicit backup, restore and compatibility
plan. No automatic local upgrade or restore command is supplied.

## Update application and adapter environments

Use the matching client in applications that submit work, and the matching
helper as a development dependency when authoring programs:

```sh
uv add "ledgence-client==0.6.0"
uv add --dev "ledgence-worker==0.6.0"
```

The generated starter already declares both under its development group; use
`uv sync` there. Existing projects can use their own environment manager and
the equivalent pinned pip packages. The deployed Rust worker supplies its
helper. Prepare actual runtime dependencies separately for the target worker.

Recompile custom Rust adapters against the 0.6.0 API crates. Their public
contracts have not changed since 0.5.0. Keep matching component versions and
run your application checks before adopting the new deployment.

**Source:** [0.6.0 release notes](https://github.com/Ledgence/ledgence/blob/develop/docs/releases/0.6.0.md) · [Release artifacts](https://github.com/Ledgence/ledgence/releases/tag/v0.6.0)
