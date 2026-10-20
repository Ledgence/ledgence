---
title: Build the full stack from source
description: An advanced walkthrough for building Ledgence's container image from a release checkout and exercising its bundled examples.
---

Use this walkthrough when you want to build the **Ledgence platform image** from
source and exercise its bundled integration examples. It is intended for
contributors and operators investigating the platform.

For your first application, choose [a local installation](/getting-started/installation)
and start with [Hello Ledgence](/tutorials/first-task). Those routes use released
runtime images and teach you to write your own program.

Here you will build the `v0.6.1` source release in Docker. Its Compose deployment
includes a database, orchestrator, Console, worker and callback receiver. It is
a separate project from an installation managed by `ledgence local` or the
published kit. Stop another stack before reusing port 8080.

## Before you start

You need Git and Docker Engine or Docker Desktop with Compose **2.23.1 or newer**,
running Linux containers on `amd64` or `arm64`. Rust, Node and Python are supplied
by the container build; they do not need to be installed on your host.

Allow additional time and disk space for the first build. It downloads pinned
base images, Rust dependencies and frontend dependencies. Use a fresh deployment
for this walkthrough; follow [Upgrade to 0.6.1](/how-to/upgrade-to-0-6-1) before
adopting retained data from another version.

## 1. Get the source

In the directory where you keep projects:

```sh
git clone --branch v0.6.1 --depth 1 https://github.com/Ledgence/ledgence.git ledgence-source
cd ledgence-source
```

This checks out the published release tag. Git's detached-HEAD notice is expected.
Use a separate clone if you already have work in progress. Keep running the
remaining terminal commands from this repository root.

## 2. Save the build settings

Open your editor and create **`ledgence-source.env` next to the repository**,
not inside it. The resulting layout is:

```text
projects/
  ledgence-source.env
  ledgence-source/
    deploy/
    crates/
    ...
```

For this unmodified release checkout, put these settings in the file:

```dotenv
# Source commit recorded in the built image and Console.
LEDGENCE_SOURCE_REVISION=b63c40edcb1e3f52035bcf87392129ed5f9132f2
# This walkthrough starts from an unmodified release checkout.
LEDGENCE_SOURCE_DIRTY=false
# The API listens on this port on your computer.
LEDGENCE_HTTP_PORT=8080
# One consumer/process slot demonstrates process reuse in the example.
LEDGENCE_CONCURRENCY=1
```

Compose reads this file when passed `--env-file`. Saving these settings once
keeps later starts and stops consistent without repeated terminal exports.
`git rev-parse HEAD` shows the source commit; if you change the platform source,
update that value to the selected commit and set `LEDGENCE_SOURCE_DIRTY=true`
while it has uncommitted changes.

## 3. Build and start the stack

```sh
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml build
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml up --detach --wait --wait-timeout 120
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml ps
```

`--project-name ledgence-source` keeps this deployment's containers and data
separate from the published kit's example project. PostgreSQL starts first,
then the migrator applies the schema. The migrator exits successfully; the
long-running services remain running. The command waits for startup readiness.

The API binds to `127.0.0.1:8080`. The deployment uses local demo credentials and
trusted code; it is not a recipe for exposing a public service.

## 4. Publish and run the bundled examples

```sh
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml run --rm --no-deps publish
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml run --rm --no-deps demo
```

`publish` prepares and registers `invoice-issuer` and `workflow-summary`, both
application version `1.0.0`, plus `workflow-example` version `1.0.1`. The worker
retrieves their immutable packages when needed. `demo` then submits invoice tasks,
runs the workflow, and checks completion callbacks.

Its final JSON includes:

```json
{
  "passed": true,
  "workflow_output": {
    "page_count": 4,
    "summary": {
      "characters": 84,
      "pages": 4
    }
  }
}
```

Generated execution and process IDs differ between runs. `passed: true` means
the example checked that two invoice tasks reused a healthy process, the
workflow completed, and the callback receiver accepted its notifications. The
workflow fetches four short pages, releases its worker slot while a summary task
runs, then resumes to return the result.

Open [Console](http://127.0.0.1:8080/console/) to inspect the registered programs
and recorded executions. Repeating `demo` uses fresh submission keys; publishing
identical package bytes again is safe.

This walkthrough builds a local image tagged `ledgence:local`. That mutable tag
is **not** a valid digest-pinned `program build` target. The beginner tutorials
use the published runtime image instead; do not substitute this local tag into
their program configuration.

## 5. Keep or stop the source stack

Stop the services from the same checkout:

```sh
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml down --timeout 65
```

This preserves the database, published programs, worker cache and callback
records in named volumes. Start again with the same `up` command from step 3.
Keep the environment file and project name consistent; the native CLI does not
manage or upgrade this source-Compose deployment.

## If the example does not finish

```sh
docker compose --env-file ../ledgence-source.env --project-name ledgence-source --file deploy/local/compose.yaml logs --tail 100 orchestrator worker
```

Check that publication succeeded and port 8080 is available. To use a different
port, stop this stack, change `LEDGENCE_HTTP_PORT` in `ledgence-source.env`, and
start it again; use the new port when opening Console or connecting a client.
Keep concurrency 1 for the process-reuse assertion.

The demo receiver stores at most 256 callback events. Repeated demonstrations
can fill it; it is not a production callback service. The
[source deployment reference](https://github.com/Ledgence/ledgence/blob/develop/docs/local-deployment.md)
covers more options, including SQS-compatible delivery through ElasticMQ.

**Source:** [Compose configuration](https://github.com/Ledgence/ledgence/blob/v0.6.1/deploy/local/compose.yaml) ·
[Example assertions](https://github.com/Ledgence/ledgence/blob/v0.6.1/deploy/local/demo.py)
