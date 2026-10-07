---
title: Upgrade to 0.5.0
description: Update matching services and Python authoring packages, enable program publication explicitly, and preserve existing local data.
---

Ledgence 0.5.0 adds program preparation, immutable HTTP uploads, saved publication
receipts and the separately installable `ledgence-worker` authoring package.
There are **no new PostgreSQL migrations relative to 0.4.0**. Console contract
**5**, runtime protocol **3**, and the `ledgence.client` / `ledgence.worker`
imports remain unchanged.

For releases older than 0.4.0, first review the [0.4 upgrade guide](/how-to/upgrade-to-0-4)
and its earlier migration requirements. Installing a new CLI does not restart
services, migrate data or replace a saved local distribution.

## Install the matching CLI

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.5.0/install.sh | sh -s -- --version 0.5.0
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

New Bash/Zsh terminals load the configured PATH automatically. A piped installer
cannot change the current parent shell; source the environment file as shown.
Manual installation and source builds remain available in [Install native tools](/how-to/install-native).

Native targets remain Linux x86_64/glibc and macOS arm64. Container runtimes
support Linux amd64 and arm64 with CPython 3.14. A program must match the worker's
actual architecture and exact Python major/minor; the runtime image must also
supply compatible native libraries.

## Update a retained deployment

1. Retain tested backups of PostgreSQL, the immutable program store and the
   server-owned instance configuration. Keep the previous complete bundle and
   service options for recovery planning.
2. Plan the restart, stop old writers and replace the orchestrator, workers,
   Python runtime helper and Console assets with matching 0.5.0 components.
   Update any explicit `--runner` or `--console-dir` paths.
3. Keep the intended database, program store, queues and instance binding.
   Run `ledgence orchestrator migrate` explicitly against that database. An
   up-to-date 0.4.0 schema has no new migrations; the server still verifies it.
4. Restore services, inspect readiness and registered programs, and verify a
   known task or workflow plus its retained history after restart.

To enable uploads, add `--allow-program-publication` to `orchestrator serve`
with `--instance-config` and a writable filesystem `--store`. Give that
orchestrator access to the same store workers read. An HTTPS read-only store
does not become writable. Keep the operator-controlled access boundary; upload
is an administrative capability for trusted code.

## Preserve saved local installations

An existing CLI-managed installation keeps its original kit, image, Docker
context, port, concurrency, volumes and project identity. Neither the installer
nor a later `local up` replaces it with 0.5.0. Keep its state directory and named
volumes; `local down` preserves both. Do not modify checksummed kit files or
remove volumes as an upgrade shortcut.

Evaluate 0.5.0 in a **new directory** and a free port, for example:

```sh
ledgence local up --directory "$HOME/.local/share/ledgence/local-0.5.0" --port 8085
ledgence local status --directory "$HOME/.local/share/ledgence/local-0.5.0"
```

Use a path that does not already contain an installation and an available port.
Repeat that same `--directory` for logs and shutdown:

```sh
ledgence local down --directory "$HOME/.local/share/ledgence/local-0.5.0"
```

The new stack has separate data and starts without application programs. It does
not import an older source-Compose or CLI-managed deployment. Moving retained
data requires an explicit backup, restore and compatibility plan; no automatic
local upgrade or restore command is supplied. The 0.5.0 kit explicitly enables
its orchestrator's publication writer while keeping worker mounts read-only.

## Update application and adapter environments

Update the client where applications submit work, and install the helper as a
development dependency where they author programs:

```sh
uv add "ledgence-client==0.5.0"
uv add --dev "ledgence-worker==0.5.0"
```

The pip alternatives are `python -m pip install "ledgence-client==0.5.0"` and
`python -m pip install "ledgence-worker==0.5.0"` in the appropriate activated
environments. The worker still supplies its helper at runtime; keep it out of
application production requirements. Rebuild actual application dependencies
for the selected worker image when necessary.

Recompile Rust adapters against the 0.5.0 API crates. `RegisterProgram` struct
literals must set `expected_descriptor: None` or supply the descriptor to bind;
its HTTP JSON field is optional. The new `ProgramArtifactPublisher` write port
is separate from `ProgramStore` reading and local cache preparation.

For a program build, copy the exact worker image from
`ledgence local status --directory "$HOME/.local/share/ledgence/local-0.5.0"`
for the installation shown above, or its saved `distribution.json`. Choose its
platform explicitly.
Do not reuse another target's program version for different archive bytes.
Keep publication receipts with their saved ZIP for recovery instead of rebuilding
an uncertain upload. See [Build and publish programs](/how-to/build-and-publish-programs).

**Source:** [0.5.0 release](https://github.com/Ledgence/ledgence/releases/tag/v0.5.0) · [Program publication contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-publication.md)
