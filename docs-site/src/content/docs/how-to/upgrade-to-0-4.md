---
title: Upgrade to 0.4.0
description: Install the 0.4.0 CLI, update matching services, and keep existing deployment data separate from a new local stack.
---

Ledgence 0.4.0 adds the one-line installer, installed-resource discovery, and a
local stack built from published images. There are **no new PostgreSQL migrations
relative to 0.3.1**. Console contract **5**, runtime protocol **3**, and the
`ledgence.client` / `ledgence.worker` imports remain unchanged.

For deployments older than 0.3.1, first review the [0.3 upgrade
requirements](/how-to/upgrade-to-0-3). Those requirements still apply when adopting
the current schema from an older release.

## Install the CLI

Install this release explicitly:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.4.0/install.sh | sh -s -- --version 0.4.0
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The installer keeps the complete bundle and legal notices, checks it before
activation, and configures supported shell profiles. Existing manually installed
executables are preserved; check `command -v ledgence` if an older version still
runs. Installing a CLI does not restart or upgrade services.

Native targets remain Linux x86_64/glibc and macOS arm64. The container runtime
supports Linux amd64 and arm64, with CPython 3.14. Program packages still need to
match the worker's actual architecture and exact Python major/minor.

## Update an existing deployment

1. Retain tested backups of PostgreSQL, the immutable program store, and the
   server-owned instance configuration. Preserve the current service configuration
   and complete old bundle for recovery planning.
2. Plan the service restart, stop old writers, and replace the orchestrator,
   workers, Python runtime helper, and Console assets with the matching 0.4.0
   components. Do not mix frontend assets or worker helpers across versions.
3. Retain the intended `DATABASE_URL`, program store, queues, and instance file.
   Run `ledgence orchestrator migrate` explicitly against the intended database
   before starting services. For an already current 0.3.1 schema there are no new
   migrations to apply; the server still verifies the schema on startup.
4. Restore the service processes with their existing options and inspect
   readiness, registered programs, workers, and a known task or workflow result.
   Keep checkpoints, attempt history, and Console navigation available after the
   restart.

With a complete bundle, the CLI finds its Python helper automatically. The
orchestrator finds bundled Console assets only when `--instance-config` is
supplied. Explicit `--runner` and `--console-dir` values continue to take
precedence; update any such paths when replacing a manual installation.

Update application environments to `ledgence-client==0.4.0`. Recompile custom
Rust adapters against the 0.4.0 API crates. The Python client remains separate
from the native CLI and worker helper; see [releases and packages](/reference/releases).

## Keep new local stacks separate

`ledgence local up` initializes a new deployment from the bundled kit. It does
not import an existing source-Compose database, program store, or volumes. If an
existing stack uses port 8080, stop it or initialize the new local installation
with a free `--port` and its own `--directory`.

Existing CLI-managed installations retain their saved kit, image, Docker context,
port, concurrency, and project identity. Installing a newer CLI does not change
that configuration or migrate its database. Keep the original state directory
and named volumes; `local down` preserves them, and `local up` starts them again.
There is no automatic local upgrade, restore, or database rollback command.

Use a separate directory and free port to evaluate a new stack. Move production
or retained example data only through an explicit backup, restoration, and
compatibility plan. See [local configuration and data
preservation](/how-to/run-local-distribution#preserve-configuration-and-data).

**Source:** [0.4.0 release notes](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/releases/0.4.0.md) · [Container distribution contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/container-distribution.md)
