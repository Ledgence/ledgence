---
title: Run tasks in parallel
description: Stage independent child tasks, wait for their terminal outcomes, and collect successful results.
---

Use distributed child tasks when work should run as independently scheduled executions, each with its own attempts and lease. Stage the children together, then return a checkpoint that waits for all of them.

For concurrent I/O inside one controller invocation, use `ctx.gather` with `ctx.local` instead. The [execution model](/concepts/execution-model) explains the difference.

## Prerequisites

You need a running orchestrator and workers connected to the queues you choose. Publish the child program before submitting the controller, and package the controller with runtime protocol **3**.

The example below uses the real `workflow-summary` program from [your first workflow](/tutorials/first-workflow). Its input is `{"pages": [...]}` and its output contains `pages` and `characters`. If using the local Compose stack, keep the queue as `demo`.

## Stage the children and save their keys

Use this as the controller's handler. Publish it as a new program or version; it is not already included in the Compose example.

```python
from ledgence.worker.workflow import workflow_context

async def handle(event):
    ctx = workflow_context()

    if ctx.continuation == "start":
        batches = event["data"]["batches"]
        if len(batches) > 64:
            return ctx.fail("too_many_batches", "Use at most 64 batches")

        keys = [f"summary:{index}" for index in range(len(batches))]
        children = [
            ctx.task(
                key,
                program="workflow-summary",
                version="1.0.0",
                queue="demo",
                data={"pages": pages},
            )
            for key, pages in zip(keys, batches)
        ]
        return ctx.suspend(
            continuation="collect",
            state={"keys": keys},
            until=children,
        )

    if ctx.continuation == "collect":
        keys = ctx.state["keys"]
        inputs = ctx.inputs
        unsuccessful = [
            key for key in keys
            if inputs[key]["outcome"]["kind"] != "succeeded"
        ]
        if unsuccessful:
            return ctx.fail("batch_failed", "A summary task did not succeed")
        return ctx.complete({
            "summaries": [ctx.get_result(key) for key in keys],
        })

    return ctx.fail("unknown_continuation", ctx.continuation)
```

For example, submit the controller with:

```json
{
  "batches": [
    ["First page", "Second page"],
    ["Another page"]
  ]
}
```

After packaging and publication, submit it through `client.workflows.submit(...)` using your controller's program ID and version. See the [Python client reference](/reference/python-client) for the submission fields and the [package contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-packages.md) for publication.

## Handle every terminal outcome

`until=children` waits for **all** listed children to become terminal. Failed and cancelled children also satisfy the wait. Inspect `ctx.inputs` before reading successful output with `ctx.get_result`.

The example fails the whole workflow when a batch does not succeed. Your controller can instead store partial results or schedule a recovery task using a new child key.

## Choose capacity deliberately

Tasks become eligible for independent scheduling after the checkpoint is accepted. They execute simultaneously only when matching workers have spare capacity. A worker with concurrency one runs them sequentially; the controller's durable wait does not occupy its slot.

A checkpoint can stage at most 64 combined task/subworkflow commands and wait for at most 64 children. Its encoded decision must also fit within 256 KiB, so large page bodies can reach the byte limit earlier. Use references to application-managed storage for large inputs.

Child keys belong to the whole workflow. Reuse a key only for the same child binding; use an iteration suffix such as `summary:round-2:0` when a loop should create new work.

**Related:** [Workflow context reference](/reference/workflow-context) · [Checkpoint contracts](https://github.com/Ledgence/ledgence/blob/develop/docs/workflows.md)
