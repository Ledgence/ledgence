---
title: Run Ledgence locally
description: Start a local stack, publish its example agents, and observe real task and workflow results.
---

Run a complete Ledgence 0.2.0 stack on your machine and watch it execute a task and a checkpoint workflow. You will use the repository's Compose example, which includes a database, an orchestrator serving Console, a worker, and a small callback receiver.

This tutorial uses the `v0.2.0` source release. Its invoice and summary programs use application version `1.0.0`; its typed workflow controller uses `1.0.1`. Use a fresh deployment, or follow [Upgrade to 0.2.0](/how-to/upgrade-to-0-2) before adopting existing data.

Allow extra time for the first image build. You do not need a hosted account, Rust, or Python installed on your computer for this tutorial.

## Before you start

You need Git and Docker Engine or Docker Desktop with Compose 2.23.1 or newer supporting `up --wait` and inline configs. Docker must run Linux containers on `amd64` or `arm64`. The first build downloads its pinned images, Rust dependencies, and frontend dependencies.

The example binds its API to `127.0.0.1:8080`. It uses local demo credentials and runs trusted code; use it on your own machine rather than exposing it as a public service.

## 1. Get the source

In a directory where you keep projects:

```sh
git clone --branch v0.2.0 --depth 1 https://github.com/Ledgence/ledgence.git
cd ledgence
```

This checks out the published `v0.2.0` source tag. Git may report a detached HEAD; that is expected when following a release tag. Compose builds the image locally from this version. Run the remaining commands from this repository root. If you already have a checkout, use a separate clone to follow along without changing work in progress.

## 2. Start the stack

```sh
export LEDGENCE_SOURCE_REVISION="$(git rev-parse HEAD)"
export LEDGENCE_SOURCE_DIRTY="$(test -z "$(git status --porcelain)" && echo false || echo true)"
docker compose -f deploy/local/compose.yaml build
docker compose -f deploy/local/compose.yaml up -d --wait --wait-timeout 120
```

Compose starts PostgreSQL, applies the schema migration, then starts the orchestrator and worker. The command returns when the long-running services are ready. The migration service exits successfully after doing its work.

Check the services:

```sh
docker compose -f deploy/local/compose.yaml ps
```

Keep the example's default worker concurrency of **one**. The next step uses it to demonstrate process reuse.

## 3. Publish the example packages

```sh
docker compose -f deploy/local/compose.yaml run --rm --no-deps publish
```

This prepares three immutable packages for the container's Python version and platform: `invoice-issuer`, `workflow-example`, and `workflow-summary`.

Publishing makes their code available in the shared program store and registers the verified references in Console. Open [Console](http://127.0.0.1:8080/console/) to inspect **Programs** and **Executions**. The worker fetches and verifies a package when it needs to execute it.

## 4. Run the example

```sh
docker compose -f deploy/local/compose.yaml run --rm --no-deps demo
```

The command submits two invoice tasks, runs a workflow, and checks their completion callbacks. Its final JSON includes:

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

The complete output also contains generated task, workflow, and subscription IDs, plus a process ID. Those values will differ on your machine.

`passed: true` means the example checked that both invoice tasks reused the same healthy process, the workflow finished, and the callback receiver accepted the notifications. The workflow fetched four short pages, released its worker slot while a separate summary task ran, then resumed to return the result.

You can run `demo` again: each invocation uses fresh submission keys. Publishing the same package bytes again is also safe.

## 5. Keep the stack for the next tutorial

Continue with [your first workflow](/tutorials/first-workflow) to submit the published workflow yourself and understand its two typed entrypoints. Use [Explore Ledgence Console](/tutorials/use-console) to inspect its recorded graph, trace, input, and output.

When you finish, stop the local services:

```sh
docker compose -f deploy/local/compose.yaml down --timeout 65
```

This preserves the database, published packages, worker cache, and callback records in named volumes. Start them again with the same `up` command from step 2.

## If the image build reports “Patches were modified”

The `v0.2.0` Dockerfile can hit `ERR_PNPM_VERIFY_DEPS_BEFORE_RUN` when Docker reuses its dependency-install layer, then copies Console files with newer patch timestamps. Add this line in `deploy/local/Dockerfile`, immediately after `COPY LICENSE /src/LICENSE` in the `console-builder` stage:

```dockerfile
RUN pnpm install --offline --frozen-lockfile --ignore-scripts
```

This revalidates the copied files against the lockfile using packages already in the image. Keep the original install step and dependency verification enabled. Recompute the source metadata and rebuild from step 2; the edited checkout should now report `LEDGENCE_SOURCE_DIRTY=true`. No cache or volume deletion is needed.

## If the example does not finish

Read the service logs:

```sh
docker compose -f deploy/local/compose.yaml logs --tail 100 orchestrator worker
```

Confirm that the publication command completed and that no other application uses port 8080. To choose a different port, set `LEDGENCE_HTTP_PORT` before starting the stack and use that port for client connections.

The sample callback receiver has a capacity of 256 events. It is a bounded demonstration receiver; repeated testing can fill it. The [local deployment guide](https://github.com/Ledgence/ledgence/blob/v0.2.0/docs/local-deployment.md) covers its lifecycle and deployment options.

**Source:** [Compose configuration](https://github.com/Ledgence/ledgence/blob/v0.2.0/deploy/local/compose.yaml) · [Example assertions](https://github.com/Ledgence/ledgence/blob/v0.2.0/deploy/local/demo.py)
