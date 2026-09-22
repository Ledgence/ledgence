---
title: Run Ledgence locally
description: Start a local stack, publish its example agents, and observe real task and workflow results.
---

Run a complete Ledgence stack on your machine and watch it execute a task and a checkpoint workflow. You will use the repository's Compose example, which includes a database, an orchestrator, a worker, and a small callback receiver.

Allow extra time for the first image build. You do not need a hosted account, Rust, or Python installed on your computer for this tutorial.

## Before you start

You need Git and Docker Engine or Docker Desktop with Compose v2 supporting `up --wait`. Docker must run Linux containers on `amd64` or `arm64`. The first build downloads its pinned images and Rust dependencies.

The example binds its API to `127.0.0.1:8080`. It uses local demo credentials and runs trusted code; use it on your own machine rather than exposing it as a public service.

## 1. Get the source

In a directory where you keep projects:

```sh
git clone --branch develop https://github.com/Ledgence/ledgence.git
cd ledgence
git checkout 82d2173862a0a379c467b46977b91327258aee15
```

This checkout matches the product revision used by these tutorials. Run the remaining commands from this repository root. If you already have a checkout, use a separate clone to follow along without changing work in progress.

## 2. Start the stack

```sh
export LEDGENCE_SOURCE_REVISION="$(git rev-parse HEAD)"
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

Publishing makes their code available in the shared program store. The worker fetches and verifies a package when it needs to execute it.

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
      "characters": 80,
      "pages": 4
    }
  }
}
```

The complete output also contains generated task, workflow, and subscription IDs, plus a process ID. Those values will differ on your machine.

`passed: true` means the example checked that both invoice tasks reused the same healthy process, the workflow finished, and the callback receiver accepted the notifications. The workflow fetched four short pages, released its worker slot while a separate summary task ran, then resumed to return the result.

You can run `demo` again: each invocation uses fresh submission keys. Publishing the same package bytes again is also safe.

## 5. Keep the stack for the next tutorial

Continue with [your first workflow](/tutorials/first-workflow) to submit the published workflow yourself and understand its two continuations.

When you finish, stop the local services:

```sh
docker compose -f deploy/local/compose.yaml down --timeout 65
```

This preserves the database, published packages, worker cache, and callback records in named volumes. Start them again with the same `up` command from step 2.

## If the example does not finish

Read the service logs:

```sh
docker compose -f deploy/local/compose.yaml logs --tail 100 orchestrator worker
```

Confirm that the publication command completed and that no other application uses port 8080. To choose a different port, set `LEDGENCE_HTTP_PORT` before starting the stack and use that port for client connections.

The sample callback receiver has a capacity of 256 events. It is a bounded demonstration receiver; repeated testing can fill it. The [local deployment guide](https://github.com/Ledgence/ledgence/blob/develop/docs/local-deployment.md) covers its lifecycle and deployment options.

**Source:** [Compose configuration](https://github.com/Ledgence/ledgence/blob/develop/deploy/local/compose.yaml) · [Example assertions](https://github.com/Ledgence/ledgence/blob/develop/deploy/local/demo.py)
