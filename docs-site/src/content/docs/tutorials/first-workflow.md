---
title: Run your first workflow
description: Follow a real workflow from concurrent local steps to a distributed task and a resumed continuation.
---

Submit the workflow published in [Run Ledgence locally](/tutorials/run-locally), then follow the code that produced its result. You will see how local work, a distributed task, and a checkpoint fit together—even with a single worker slot.

## Before you start

Complete the local tutorial and leave its stack running. The `publish` command must have succeeded so that `workflow-example` and `workflow-summary` version `1.0.0` are available.

For this tutorial, also install CPython 3.11 or newer on your host. The commands below use `python3`; check its version first. Keep your terminal at the Ledgence repository root.

## 1. Prepare the client

Create a virtual environment outside the checkout and install the client from this source tree:

```sh
python3 --version
export LEDGENCE_TUTORIAL_DIR="$(mktemp -d)"
python3 -m venv "$LEDGENCE_TUTORIAL_DIR/client"
"$LEDGENCE_TUTORIAL_DIR/client/bin/python" -m pip install ./sdk/python-client
```

The SDK talks to the local API. It does not upload packages or execute the workflow in this client process.

## 2. Submit the workflow

Run this complete script in the same terminal:

```sh
"$LEDGENCE_TUTORIAL_DIR/client/bin/python" - <<'PYTHON'
import asyncio
import json
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        workflow = await client.workflows.submit(
            program="workflow-example",
            version="1.0.0",
            queue="demo",
            data={
                "urls": ["http://receiver:8091/page.txt"] * 4,
                "queue": "demo",
            },
            idempotency_key="docs:first-workflow:1",
            correlation_key="docs:first-workflow",
        )
        print("Workflow:", workflow.id)
        output = await workflow.result(timeout=60)
        print(json.dumps(output, indent=2))

asyncio.run(main())
PYTHON
```

If you selected a different API port in the local tutorial, update the client URL. The page URL stays unchanged: `receiver` is the hostname the **worker container** uses on the Compose network.

You should receive a workflow ID and:

```json
{
  "page_count": 4,
  "summary": {
    "characters": 80,
    "pages": 4
  }
}
```

## 3. Follow the first continuation

Open `examples/checkpoint-workflow/controller/program.py`. Its handler begins with:

```python
ctx = workflow_context()
if ctx.continuation == "start":
    pages = await ctx.gather(*[
        ctx.local(f"page-{index}", fetch, url=url)
        for index, url in enumerate(event["data"]["urls"])
    ])
```

The four asynchronous fetches overlap inside one invocation. Each `ctx.local` call gives a fetch a stable key and records its returned page durably before the await completes.

The next lines stage a separate task:

```python
child = ctx.task(
    "summarize",
    program="workflow-summary",
    version="1.0.0",
    queue=event["data"]["queue"],
    data={"pages": pages},
)
return ctx.suspend(
    continuation="collect",
    state={"page_count": len(pages)},
    until=[child],
)
```

Returning this decision commits the next continuation and its state. The controller invocation ends, freeing the worker slot. Once the checkpoint is accepted, the summary task can be dispatched.

## 4. Follow the resumed continuation

The summary program counts the pages and their characters. When it finishes, Ledgence schedules another controller activation with `ctx.continuation` set to `"collect"`.

The controller returns:

```python
return ctx.complete({
    "page_count": ctx.state["page_count"],
    "summary": ctx.get_result("summarize"),
})
```

The saved JSON state supplies the page count. The child result supplies the summary. Python variables from the earlier invocation are not restored.

This example takes the successful path. In your own workflow, inspect failed or cancelled child outcomes before calling `get_result`; [Run tasks in parallel](/how-to/parallel-tasks) shows that pattern.

## 5. Observe the same execution again

Run the submission script again unchanged. The same key and input return the existing workflow, so you see the same workflow ID and result. Change the key to `docs:first-workflow:2` when you intentionally want a new execution.

The 60-second timeout limits how long the client observes the result. It does not cancel or submit the workflow again. Save the workflow ID to reconnect through `client.workflows.handle(workflow_id)` later.

You have now followed a complete checkpoint: local results, a distributed child, a saved continuation, and a final output. Read [Checkpoints and recovery](/concepts/checkpoints-and-recovery) to see what happens when an activation stops unexpectedly.

**Source:** [Controller and summary example](https://github.com/Ledgence/ledgence/tree/develop/examples/checkpoint-workflow) · [Python client](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md)
