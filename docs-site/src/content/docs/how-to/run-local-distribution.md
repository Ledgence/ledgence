---
title: Run the local image distribution
description: Start and manage a pinned local container stack with the released CLI while preserving its configuration and data.
---

The `ledgence local` commands manage a versioned Docker Compose kit containing
PostgreSQL, a schema migrator, the orchestrator with Console, and one worker.
The runtime image is pinned by immutable registry digest. A prepared kit runs
without a source checkout, Rust, Node, or host Python.

**Available in Ledgence 0.4.0.** [Install the CLI](/how-to/install-native), then
start the bundled distribution with `ledgence local up`. The image is published
in [Docker Hub](https://hub.docker.com/r/ledgence/ledgence); the kit records the
exact digest rather than following a moving image tag. Building from source
remains available through the [source Compose tutorial](/tutorials/run-locally).

**Development addition:** updated source templates opt the orchestrator into
HTTP program publication and mount its shared program store writable while the
worker remains read-only. This requires a newly qualified matching kit and server;
it does not change any existing 0.4.0 installation. Follow [Build and publish
programs](/how-to/build-and-publish-programs) for the explicit build/upload path,
and use a separate state directory to evaluate it without migrating existing data.

## Before you start

Install and start Docker Engine or Docker Desktop with **Compose 2.23.1 or
newer**. Use Linux containers and a named local Docker context. The CLI rejects
remote endpoints. If `DOCKER_HOST` is set, select a named local context
explicitly with `--context` on first startup.

A complete 0.4.0 native installation includes its matching qualified kit.
The kit includes `distribution.json`, its Compose files, and `SHA256SUMS`.
Its manifest identifies the version, immutable runtime image, CPython version,
and qualified container platforms. An unprocessed `deploy/distribution`
directory in a checkout is a packaging template, not a runnable kit.

Native CLI targets and container platforms are separate. Published native
bundles target Linux x86_64/glibc and macOS Apple silicon. The 0.4.0 kit supports
`linux/amd64` and `linux/arm64`; the CLI checks the actual Docker engine against
the kit's declared platforms. Do not infer native Linux ARM64 or
Windows support from an image's platform list.

## Start your local stack

With Docker running, use the installed CLI:

```sh
ledgence local up
ledgence local status
```

The CLI verifies and copies the bundled kit, saves its project identity and
settings, then waits for Compose readiness. PostgreSQL becomes healthy before
the migrator runs; the orchestrator starts after migration succeeds. The server
verifies its schema rather than applying migrations itself.

On success, the CLI prints the API and Console URLs, scope, runtime image,
Docker context, and worker platform. Open the printed Console URL, normally
[http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/). Defaults are port
8080, concurrency 1, tenant `acme`, namespace `demo`, and queue `demo`.
The base stack contains no published application programs or callback receiver.

To create an independent installation on another port, select an empty directory
on its first startup:

```sh
ledgence local up \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --port 8086 --concurrency 1
```

A source-built CLI can use `--distribution /absolute/path/to/qualified-local-kit`
instead of bundled resources. The extracted kit must be complete and match the
CLI version for a new installation. An unprocessed `deploy/distribution` source
directory is not a qualified kit.

## Inspect, stop, and restart

For the default installation:

```sh
ledgence local logs --tail 100 --service worker
ledgence local down
ledgence local up
```

Use the same directory for every operation on a nondefault installation:

```sh
ledgence local status \
  --directory "$HOME/.local/share/ledgence/local-preview"
ledgence local logs \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --tail 100 --service worker
ledgence local down \
  --directory "$HOME/.local/share/ledgence/local-preview"
ledgence local up \
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
verified kit files directly in that directory, alongside `.ledgence-state.json`.
The state records the unique Compose project,
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

The CLI and direct Compose instructions create **separate projects with separate
data volumes**. The example README uses project `ledgence-local` and port 8080,
while the CLI generates and saves its own project identity. Stop the CLI project
before starting that example on the same port:

```sh
ledgence local down
cd "${XDG_DATA_HOME:-$HOME/.local/share}/ledgence/local"
```

For a custom installation, pass its original `--directory` to `down` and change
into that directory instead. The kit's README and Compose files live directly
there, with no extra kit subdirectory. Set `LEDGENCE_CONCURRENCY=1` for the
example assertions. If the CLI installation uses a custom Docker context, pass
the same `docker --context NAME` before `compose` for every direct command.
Then follow its example commands, keeping
`--project-name ledgence-local` and both `--file` options identical for startup,
logs, and shutdown. If keeping the CLI stack running, export a free
`LEDGENCE_PORT`, such as `8086`, for every direct Compose command instead.

Stop the example project when finished, preserving its volumes:

```sh
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml down --timeout 65
```

You can then restart the original CLI project with `ledgence local up`, using the
same `--directory` if customized. The CLI does not adopt the example project's
volumes or manage its lifecycle. Neither project imports data from an existing
source-Compose deployment.

Source Compose builds an image from a checkout and uses `LEDGENCE_HTTP_PORT`.
A distribution kit pulls its pinned image and uses `LEDGENCE_PORT` for direct
Compose, or `--port` through the CLI. Both bind the API to loopback and use local
demo credentials for operator-trusted code.

**Source:** [Local CLI](https://github.com/Ledgence/ledgence/blob/v0.4.0/crates/ledgence-cli/src/local.rs) · [Kit guide](https://github.com/Ledgence/ledgence/blob/v0.4.0/deploy/distribution/README.md) · [Distribution contract](https://github.com/Ledgence/ledgence/blob/ae6734a2dfa58d931c3e3fcfa0e791382bfe15bf/docs/container-distribution.md)
