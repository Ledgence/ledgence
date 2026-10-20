---
title: Manage your local installation
description: Inspect, stop and restart a CLI-managed local stack, and understand how its configuration and data are retained.
---

Use this guide **after** installing Ledgence with the CLI. For a first setup,
start with [Choose your local installation](/getting-started/installation), then
[Your first task](/tutorials/first-task). If you chose direct Compose, use that
method's [Compose lifecycle commands](/how-to/install-compose#stop-restart-or-inspect-the-services)
instead; the CLI does not adopt a separately created Compose project.

A CLI-managed installation uses a verified, versioned Docker Compose kit.
The kit pins the runtime image by digest and includes PostgreSQL, a migrator,
the orchestrator with Console, and one worker. Ledgence 0.6.0 supports
`linux/amd64` and `linux/arm64` containers with CPython 3.14.

## Before you start

Docker must be running Linux containers, with Compose **2.23.1 or newer**.
Use a named **local** Docker context; the CLI rejects remote endpoints. If you
have set `DOCKER_HOST`, choose the intended named local context explicitly with
`--context` when creating an installation. The selected engine must support a
platform declared in the kit.

The [CLI installer](/how-to/install-native) includes a qualified kit. A
[source-built CLI](/how-to/build-cli-from-source) needs a downloaded kit of the
same version for a new installation. The checkout's `deploy/distribution`
directory is a packaging template, not a runnable kit.

## Start your local stack

To start or resume the default installation:

```sh
ledgence local up
ledgence local status
```

`up` verifies and saves the kit when creating an installation, then waits for
startup readiness. PostgreSQL starts before the migrator; the orchestrator
starts after migrations succeed. The orchestrator verifies its schema rather
than applying migrations itself.

The output identifies the API and Console URLs, worker platform, image and
Docker context. Defaults are port **8080**, concurrency **1**, tenant **`acme`**,
namespace **`demo`**, and queue **`demo`**. A new installation has no published
application programs or execution history.

To create an independent stack, choose a new state directory and a free port:

```sh
ledgence local up --directory "$HOME/.local/share/ledgence/local-preview" --port 8086 --concurrency 1
```

The `--directory` value is where Ledgence saves this stack's configuration.
`$HOME` is your home directory. This example creates a separate installation;
it does not change the default one's port or reuse its database.

Use the same directory for every later operation on that installation:

```sh
ledgence local status --directory "$HOME/.local/share/ledgence/local-preview"
ledgence local down --directory "$HOME/.local/share/ledgence/local-preview"
ledgence local up --directory "$HOME/.local/share/ledgence/local-preview"
```

## Inspect, stop, and restart

For the default installation:

```sh
ledgence local logs --tail 100 --service worker
ledgence local down
ledgence local up
```

`logs` prints recent service output. Add `--follow` to stream it; use Ctrl+C to
stop following logs without stopping the service. `--service` accepts
`postgres`, `migrate`, `orchestrator` or `worker`, and `--tail` accepts 1–10000.
For a custom installation, add its original `--directory` to each command.

`down` allows 65 seconds for shutdown and preserves named volumes. The next
`up` uses the saved kit and settings. If startup failed after state was saved,
inspect `status` and logs, resolve the reported cause and retry `up`. Do not
remove state or data volumes to work around a startup error.

## Preserve configuration and data

The default state directory is `~/.local/share/ledgence/local`. If you configured
`XDG_DATA_HOME`, it is instead `XDG_DATA_HOME/ledgence/local`. The directory holds
the copied kit and `.ledgence-state.json`, recording the Compose project,
original absolute path, Docker context and endpoint, image, port and concurrency.
Keep it at its original location; relocation is rejected.

Choose `--port`, `--concurrency` (1–1024), and any `--context` on the **first**
`up`. Later starts reuse those values. A newer CLI does not replace the saved
kit, runtime image or schema, and different settings are rejected for existing
state. Use a separate directory for independent experiments.

The database, program store and worker cache live in **Docker named volumes**,
not inside the state directory. Back up the database and immutable program store
together, plus the saved configuration; the cache can be recreated. Stop writers
and use PostgreSQL backup tools. Copying a live database volume is not a
consistent backup, and copying only the state directory does not back up programs
or execution history.

There is no `local restore`, automatic stack upgrade, database rollback, or
migration to another Docker engine. Validate a compatible restore before an
explicit version upgrade. See [Upgrade to 0.6.0](/how-to/upgrade-to-0-6).

## Programs, examples, and direct Compose

The local worker runs CPython **3.14**. Program packages must match its actual
Linux architecture and exact Python major/minor; multiarchitecture images do
not make one program ZIP portable across both architectures. The
[first task tutorial](/tutorials/first-task) shows how to select that target,
prepare a program and publish it.

The 0.6.0 kit enables HTTP program publication and gives the orchestrator write
access to the shared program store. The worker reads that store. An older saved
kit retains its original behavior even after you update the CLI.

For a walkthrough of prewritten examples and callbacks, the kit's `README.md`
describes optional `compose.examples.yaml` commands. These use a **separate
Compose project** named `ledgence-local`; they do not populate or operate the
CLI-managed stack. Stop the CLI installation before using the same port:

```sh
ledgence local down
```

Open the README in the saved state directory and follow its startup, publication,
example and shutdown commands, retaining the same project name and Compose file
list. Its process-reuse assertion requires concurrency 1. When finished, stop
that example project with `down --timeout 65` before returning to
`ledgence local up`. Both projects retain their own separate volumes.

For a new direct Compose installation, use
[Install with Docker Compose](/how-to/install-compose). For a platform image
built from a checkout, use the [advanced source walkthrough](/tutorials/run-locally).
These are separate lifecycle methods; do not mix their commands against one
installation.

**Source:** [Local CLI](https://github.com/Ledgence/ledgence/blob/v0.6.0/crates/ledgence-cli/src/local.rs) ·
[Distribution contract](https://github.com/Ledgence/ledgence/blob/develop/docs/container-distribution.md)
