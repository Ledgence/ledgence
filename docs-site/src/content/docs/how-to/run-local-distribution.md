---
title: Run the local image distribution
description: Start and manage a pinned local container stack with the development CLI while preserving its configuration and data.
---

The `ledgence local` commands manage a versioned Docker Compose kit containing
PostgreSQL, a schema migrator, the orchestrator with Console, and one worker.
The runtime image is pinned by immutable registry digest. A prepared kit runs
without a source checkout, Rust, Node, or host Python.

**Available on `develop`: published 0.3.1 does not contain these commands or a
local distribution kit.** No version number is assigned here to the next
release. For a complete released setup today, use the
[source Compose tutorial](/tutorials/run-locally). The
[native installation guide](/how-to/install-native) separates the released
archives from the upcoming one-line installer.

## Before you start

Install and start Docker Engine or Docker Desktop with **Compose 2.23.1 or
newer**. Use Linux containers and a named local Docker context. The CLI rejects
remote endpoints. If `DOCKER_HOST` is set, select a named local context
explicitly with `--context` on first startup.

You need a development CLI containing `local` and a matching qualified kit.
The kit includes `distribution.json`, its Compose files, and `SHA256SUMS`.
Its manifest identifies the version, immutable runtime image, CPython version,
and qualified container platforms. An unprocessed `deploy/distribution`
directory in a checkout is a packaging template, not a runnable kit.

Native CLI targets and container platforms are separate. Published native
bundles target Linux x86_64/glibc and macOS Apple silicon. A kit can support
`linux/amd64`, `linux/arm64`, or both; the CLI checks the actual Docker engine
against that kit's declared platforms. Do not infer native Linux ARM64 or
Windows support from an image's platform list.

## Start a prepared development kit

From a matching development checkout, build the CLI:

```sh
cargo build --locked --package ledgence-cli --all-features
./target/debug/ledgence local --help
```

Then select your extracted, qualified kit and an empty installation directory:

```sh
./target/debug/ledgence local up \
  --distribution /absolute/path/to/qualified-local-kit \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --port 8086 --concurrency 1
```

Replace the kit path with the one you have prepared or received. A new
installation requires the kit's version to match the compiled CLI. Development
builds can retain the current package version, so `local --help`, rather than
`--version` alone, establishes whether this command exists.

The CLI verifies and copies the kit, saves its project identity and settings,
then waits for Compose readiness. PostgreSQL becomes healthy before the
migrator runs; the orchestrator starts after migration succeeds. The server
verifies its schema rather than applying migrations itself.

On success, the CLI prints the API and Console URLs, scope, runtime image,
Docker context, and worker platform. With the example above, open
[Console on port 8086](http://127.0.0.1:8086/console/). The scope is tenant
`acme`, namespace `demo`, with queue `demo`. The base stack contains no
published application programs or callback receiver.

When a future release includes the CLI commands and a qualified `local/` kit
inside the native bundle, a complete installation will support:

```sh
ledgence local up
ledgence local status
ledgence local logs --follow --service worker
ledgence local down
```

This sequence uses port 8080, concurrency 1, and the default state directory.
It is not available with the released 0.3.1 executable.

## Inspect, stop, and restart

Use the same directory for every operation on a nondefault installation:

```sh
./target/debug/ledgence local status \
  --directory "$HOME/.local/share/ledgence/local-preview"
./target/debug/ledgence local logs \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --tail 100 --service worker
./target/debug/ledgence local down \
  --directory "$HOME/.local/share/ledgence/local-preview"
./target/debug/ledgence local up \
  --directory "$HOME/.local/share/ledgence/local-preview"
```

`down` allows 65 seconds for shutdown and preserves named volumes. A subsequent
`up` reuses the saved kit, image and settings. `logs` accepts `--follow` and
`--tail 1..10000`; select `postgres`, `migrate`, `orchestrator`, or `worker`
with `--service`. Following logs leaves other lifecycle commands available.

If a port conflict occurs before initialization, retry with a free `--port`.
If startup fails after state is saved, inspect `status` and `logs`, resolve the
reported cause, and retry `up` in that directory. Keep its state and volumes.

## Preserve configuration and data

The default state directory is `$XDG_DATA_HOME/ledgence/local`, or
`$HOME/.local/share/ledgence/local` when `XDG_DATA_HOME` is unset. It retains the
verified kit and `.ledgence-state.json`, including the unique Compose project,
original absolute directory, Docker context and endpoint, image, port, and
concurrency. Keep this directory at its original path; relocation is rejected.

Choose `--port`, `--concurrency` (1–1024), and any explicit `--context` on the
first `up`. They are retained for that installation. A newer CLI installation
does not update its image, kit, or schema. Different options or a different kit
are rejected for existing state. Use a separate directory and free port for an
independent stack.

Database, program store, and worker cache are Docker named volumes. Saving the
state directory alone does not back up those volumes. Preserve the database
and immutable program store together according to application requirements,
along with the saved kit and project configuration. Cache can be recreated.
Stop writers, use PostgreSQL backup tools, and validate restoration before
changing versions. Copying a live database volume is not a consistent backup.

There is no `local restore`, automatic upgrade, database rollback, or migration
to another engine. Restore compatible data and matching project configuration
through a planned operator procedure. A forward schema migration does not make
older binaries compatible with that schema.

## Programs, examples, and direct Compose

The initial runtime image supplies **CPython 3.14**. Programs must target the
worker's actual Linux architecture and exact Python major/minor, with application
dependencies already prepared for that environment. Image support for multiple
architectures does not make one program package portable across them.

The copied kit's `README.md` describes direct Compose use and an optional
`compose.examples.yaml` for publishing sample programs, running workflows, and
observing callbacks. The example uses concurrency 1 to check process reuse.
For direct Compose, consistently use the same explicit project name and file
list for startup, logs, and shutdown. Follow that route as a separate project;
the local CLI manages its saved base project and does not adopt another Compose
deployment.

Source Compose builds an image from a checkout and uses `LEDGENCE_HTTP_PORT`.
A distribution kit pulls its pinned image and uses `LEDGENCE_PORT` for direct
Compose, or `--port` through the CLI. Both bind the API to loopback and use local
demo credentials for operator-trusted code.

**Development source:** [Local CLI](https://github.com/Ledgence/ledgence/blob/develop/crates/ledgence-cli/src/local.rs) · [Kit guide](https://github.com/Ledgence/ledgence/blob/develop/deploy/distribution/README.md) · [Distribution contract](https://github.com/Ledgence/ledgence/blob/develop/docs/container-distribution.md)
