---
title: Mix local work and workflow branches
description: Register typed entrypoints, start durable branches in the same package, and join their outcomes after local work.
---

Use a fork when parts of the same workflow package need independent execution, retries, and checkpoints while the parent continues local work. Each branch is an owned workflow. Register its entrypoint with a Python decorator and explicitly save the state needed after a durable wait.

**Availability:** typed `Workflow` entrypoints and `branch`, `fork`, and `join` have been available since Ledgence 0.2.0. This guide uses 0.3.1. Use matching orchestrator and worker versions and all database migrations. Runtime protocol **3** remains the package contract. See [Upgrade to 0.3.1](/how-to/upgrade-to-0-3) before changing an existing deployment.

## Prepare the source deployment

Start with the 0.3.1 [checkpoint workflow setup](https://github.com/Ledgence/ledgence/blob/v0.3.1/examples/checkpoint-workflow/README.md). It builds the `ledgence` executable, applies database migrations, starts PostgreSQL-backed orchestration and a connected worker, and installs the client from the same checkout. The instructions define `LEDGENCE_PYTHON` and `workflow_demo`; retain those values for the commands below.

The source checkout must contain migration `20260928000000_workflow_forks.sql`, and the deployment must have applied all migrations. Upgrade workers and orchestrators together. The host supplies CPython 3.11 or newer; use the same Python major/minor and target platform when preparing and running the package.

The example works with worker concurrency **1**. The worker and client use the setup's fixed `acme` / `demo` compatibility binding and the `workflows` queue.

## Register the handlers

The [complete mixed workflow example](https://github.com/Ledgence/ledgence/blob/v0.3.1/examples/mixed-workflow/README.md) has one local computation and two distributed branches. Its double branch saves a timer checkpoint before completing; its triple branch completes directly. The parent collects both terminal outcomes.

```python
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    MAIN = "main"
    DOUBLE = "double"
    DOUBLE_READY = "double_ready"
    TRIPLE = "triple"
    COLLECT = "collect"


workflow = Workflow(Entry)


def input_values(data):
    if type(data) is not dict:
        raise ValueError("data must be an object")
    values = data.get("values")
    if (type(values) is not list or not 1 <= len(values) <= 1000
            or any(type(value) is not int or abs(value) > 1_000_000 for value in values)):
        raise ValueError("values must contain 1 to 1000 integers between -1000000 and 1000000")
    return values


def summarize(values):
    return {"count": len(values), "sum": sum(values)}


@workflow.entrypoint(Entry.MAIN, default=True)
async def main(event, ctx):
    data = event.get("data")
    try:
        values = input_values(data)
    except ValueError as error:
        return ctx.fail("invalid_input", str(error))
    queue = data.get("queue")
    if (type(queue) is not str or not 1 <= len(queue) <= 128
            or any(ord(char) < 32 or ord(char) >= 127 for char in queue)):
        return ctx.fail("invalid_input", "queue must contain 1 to 128 printable ASCII characters")
    branch_data = {"values": values}
    branches = await ctx.fork("calculations:0", branches=[
        ctx.branch("double:0", entrypoint=Entry.DOUBLE,
                   queue=queue, data=branch_data),
        ctx.branch("triple:0", entrypoint=Entry.TRIPLE,
                   queue=queue, data=branch_data),
    ])
    # Registration has committed, so workers can execute both branches while
    # this activation does local work. One worker slot is sufficient to finish.
    local = await ctx.local("summary", summarize, values=values)
    return ctx.join(branches, resume=Entry.COLLECT, state={"local": local})


@workflow.entrypoint(Entry.DOUBLE)
def double(event, ctx):
    # This child owns its timer and checkpoint independently of the parent.
    return ctx.sleep("ready:0", 100, continuation=Entry.DOUBLE_READY,
                     state={"sum": sum(event["data"]["values"])})


@workflow.entrypoint(Entry.DOUBLE_READY)
def double_ready(event, ctx):
    return ctx.complete(2 * ctx.state["sum"])


@workflow.entrypoint(Entry.TRIPLE)
def triple(event, ctx):
    return ctx.complete(3 * sum(event["data"]["values"]))


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    outcomes = ctx.inputs
    for key in ("double:0", "triple:0"):
        if outcomes[key]["state"] == "failed":
            return ctx.fail("branch_failed", key + " failed; inspect its terminal outcome")
        if outcomes[key]["state"] == "cancelled":
            return ctx.fail("branch_cancelled", key + " was cancelled")
    return ctx.complete({"local": ctx.state["local"],
                         "double": ctx.get_result("double:0"),
                         "triple": ctx.get_result("triple:0")})


handle = workflow.build()
```

The default entrypoint validates the request before registering any branches: `values` contains 1–1000 integers between -1000000 and 1000000, and `queue` contains 1–128 printable ASCII characters. Invalid input returns a terminal `invalid_input` decision. Unexpected runtime exceptions use the activation's retry policy.

`Workflow(Entry)` registers one handler per enum value. `build()` validates and freezes the registry and returns the package's asynchronous `handle`. Individual handlers may be synchronous or asynchronous and receive `(event, ctx)`. Public workflow submissions start at the default handler, here `Entry.MAIN`; public submission does not take an initial entrypoint override. Branch creation and explicit resume decisions select named entrypoints.

Use members of this exact enum for every branch and continuation target. Plain strings and another enum's members are rejected in a registered workflow. If the enum contains the wire value `"start"`, that member must be the default. Keep enum values stable: they are durable addresses within an immutable package version.

## Understand the two commit boundaries

| Operation | What it does | Parent capacity |
| --- | --- | --- |
| `ctx.branch(...)` | Builds an immutable branch specification; schedules nothing. | Same activation. |
| `await ctx.fork(...)` | Atomically registers the branches and their durable scheduling obligations. Returns after acknowledgment, without awaiting branch completion. | Same activation, revision, and process. |
| `await ctx.local(...)` | Executes in the parent and returns after its result is durably acknowledged. | Holds the parent's invocation slot. |
| `return ctx.join(...)` | Saves parent state and waits for all branches to become terminal. | Returns from the invocation and releases its slot. |

After fork registration commits, matching workers can start the branches while the parent performs local work. The acknowledgment does not guarantee that a child has already started. With one worker slot, the parent finishes its local work and returns the join before children use that slot. More available capacity permits overlap; it does not guarantee simultaneous execution.

Each branch uses the parent's **exact pinned program descriptor**, with no new lookup or dependency installation. It receives the explicit `data` from its specification. Entrypoint selection and execution identifiers stay outside user-owned CloudEvent `data`. A branch has its own workflow ID, checkpoints, local journal, and event/timer waits.

Python variables and stack frames do not survive the join. `state={"local": local}` saves the summary explicitly. A later invocation runs `Entry.COLLECT` with that state and frozen child outcomes; it can use another process. A child that finishes before the join is registered is still observed.

For a separately named program, use `ctx.task(...)` or `ctx.workflow(...)`. Those operations stage commands until the returned checkpoint commits; see [Run tasks in parallel](/how-to/parallel-tasks). Keep orchestration in the controller: functions passed to `ctx.local` cannot create branches, fork, join, stage children, or issue their own durable waits.

## Publish and run the example

From the repository root, with the source deployment above still running:

```sh
"$LEDGENCE_PYTHON" examples/mixed-workflow/prepare.py "$workflow_demo/mixed/program"
./target/debug/ledgence program publish \
  --source "$workflow_demo/mixed/program" --store "$workflow_demo/store"
```

Preparation writes a protocol 3 manifest with `handler: "program:handle"` for the current interpreter and target. Use a fresh output directory; preparation refuses to overwrite an existing one. The commands publish application version `mixed-workflow@1.0.1`; that application version is separate from the Ledgence platform version. Publish a new immutable application version when changing its code.

Submit through the client environment created during setup:

```sh
"$workflow_demo/client/bin/python" - <<'PYTHON'
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:8080", tenant="acme", namespace="demo") as client:
        run = await client.workflows.submit(
            program="mixed-workflow", version="1.0.1", queue="workflows",
            data={"values": [1, 2, 3], "queue": "workflows"},
            idempotency_key="mixed-workflow:1.0.1:1",
        )
        print(run.id)
        print(await run.result(timeout=60))

asyncio.run(main())
PYTHON
```

The result is:

```json
{"local": {"count": 3, "sum": 6}, "double": 12, "triple": 18}
```

Repeating the same submission key and input returns the same workflow. The 60-second client timeout limits observation; it neither cancels nor resubmits the workflow.

## Handle retries and terminal outcomes

The fork key is stable across the parent workflow, and branch keys share its child-key namespace. Retrying the exact ordered branch registration reuses its children. Changing membership, order, entrypoint, queue, data, or execution policy conflicts. Use iteration-qualified keys for new work in loops; a new fork cannot adopt a previously staged child or another fork's branch.

A crash after accepted registration does not roll back the children. Retrying the parent activation reconciles the same fork and reuses acknowledged local results. An uncertain fork acknowledgment prevents a successful final decision. External side effects can still repeat before their local result is recorded; use application idempotency or reconciliation where required.

`join` waits for **every terminal outcome**, including failure and cancellation. It does not fail fast or cancel siblings automatically. The example checks each state before calling `get_result`; application code can choose another recovery policy. Parent cancellation, intentional failure, or exhausted activation retries drain owned descendants. Cancellation cannot undo external effects.

A `ForkRef` belongs to its activation and cannot be stored in JSON state. For a later activation, reconcile the original fork binding or wait using its durable child keys with `suspend`. A later explicit join can read retained terminal results again without re-executing the children. Automatic pending-input delivery through `continue_` consumes each input only once.

## Stay within the bounds

One fork accepts **1–64 branches** and at most **128 KiB** of compact JSON for its complete request. A logical activation, including retries, may record at most **64 forks / 256 KiB** and register at most **64 new children** across forks and staged decisions. Each parent may have at most **64 live owned workflows**; nested depth is at most **16**.

Checkpoint state is limited to **64 KiB**. Existing journal, result, and activation-context limits still apply; see [Workflow context](/reference/workflow-context#inline-limits) for all bounds. Keep large payloads in application-controlled storage and pass references.

A durable join or timer retains no sleeping invocation or database connection. One worker concurrency setting bounds N consumers and at most N managed subprocesses across all programs, including warm sessions. Local async calls share their invocation's slot. These bounded mechanisms do not promise unbounded fan-out, exactly-once effects, or a particular throughput.

**Release contracts:** [Typed entrypoints and forks](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflow-entrypoints.md) · [Checkpoint workflows](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflows.md)
