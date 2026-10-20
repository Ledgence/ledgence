---
title: Run your first workflow
description: Compose the greeting task from the previous lesson and resume the workflow when its result is ready.
---

You already have a task that says hello. Now create a workflow that asks that
same task to do the work, waits durably for its outcome, and returns the greeting.
You will not copy the greeting logic into the workflow.

A **task** performs work. A **workflow** composes tasks and other workflows and
coordinates when to continue. Our workflow has two entrypoints: **start** and
**finish**.

## Before you start

Complete [Run your first task](/tutorials/first-task). Leave the local **0.6.1**
stack running and `hello-ledgence@0.1.0` published. Keep your editor open; you
will create a second project beside the first one.

The sequence will be:

1. The `start` entrypoint requests one greeting task.
2. Ledgence saves the checkpoint and runs that task.
3. The `finish` entrypoint receives its outcome and completes the workflow.

## 1. Create the workflow project

If your terminal is still inside `hello-ledgence`, run:

```sh
cd ..
ledgence program new hello-workflow --kind workflow
cd hello-workflow
uv sync
```

Open `hello-workflow` in your editor. The `--kind workflow` option creates a
workflow scaffold and marks the program as a workflow in `ledgence.toml`.
`uv sync` installs both Python SDKs into this project's `.venv`.

In **`ledgence.toml`**, replace its generated `[target]` section with the
**entire `[target]` section from your previous `hello-ledgence/ledgence.toml`**.
Both programs will run on the same worker, so their platform, Python version,
protocol, and runtime image must match. Keep this new project's program ID
`hello-workflow`, version `0.1.0`, and kind `workflow` unchanged.

## 2. Write the workflow

Replace **`hello-workflow/program.py`** with:

```python title="program.py"
"""A workflow reuses the greeting task and completes when it succeeds."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    START = "start"
    FINISH = "finish"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    # Describe a child task. The checkpoint makes its creation durable.
    greeting = ctx.task(
        "greeting",
        program="hello-ledgence",
        version="0.1.0",
        queue="demo",
        data=event["data"],
    )
    # End this activation and resume at FINISH when the child is terminal.
    return ctx.suspend(continuation=Entry.FINISH, state={}, until=[greeting])


@workflow.entrypoint(Entry.FINISH)
async def finish(event, ctx):
    # A terminal child may have failed or been cancelled; check before reading.
    if ctx.inputs["greeting"]["state"] != "succeeded":
        return ctx.fail("greeting_failed", "The greeting task did not succeed.")
    return ctx.complete(ctx.get_result("greeting"))


handle = workflow.build()
```

Read the code in three parts:

- **Entry points.** `Entry` names the supported places to begin or resume.
  The decorators register their functions; `default=True` selects `start` for
  a new workflow. `workflow.build()` validates the registry and exposes the
  `handle` function the worker calls.
- **Request and wait.** `ctx.task` describes a child task using the program
  published in the previous lesson. `"greeting"` is a stable key for this
  operation within the workflow. Returning `ctx.suspend` commits the request
  and the continuation, then ends this entrypoint activation.
- **Resume and finish.** Ledgence invokes `finish` after the child reaches a
  terminal state. `ctx.inputs` tells you whether it succeeded, failed, or was
  cancelled. A successful child's saved output is available through
  `ctx.get_result("greeting")`.

The workflow releases its execution slot while waiting. Its original Python
variables are not kept alive; Ledgence resumes through the registered
entrypoint and saved state. We use `state={}` because this example needs no
additional values between entrypoints. The greeting task remains a leaf: the
workflow is responsible for orchestration.

## 3. Update and run the tests

The generated tests refer to the scaffold's original `greet` function, which
this workflow no longer has. Replace **`tests/test_program.py`** with:

```python title="tests/test_program.py"
"""Test registration and outcome handling; orchestration needs a running stack."""
import inspect
import unittest
from unittest.mock import Mock

from program import finish, handle, workflow


class ProgramTests(unittest.IsolatedAsyncioTestCase):
    def test_workflow_registration(self):
        # build() validates the enum entries and exactly one default entrypoint.
        self.assertIs(workflow.build(), handle)
        self.assertTrue(inspect.iscoroutinefunction(handle))

    async def test_unsuccessful_child_does_not_read_a_result(self):
        for state in ("failed", "cancelled"):
            with self.subTest(state=state):
                ctx = Mock()
                ctx.inputs = {"greeting": {"state": state}}
                await finish({}, ctx)
                ctx.fail.assert_called_once_with(
                    "greeting_failed", "The greeting task did not succeed."
                )
                ctx.get_result.assert_not_called()
                ctx.complete.assert_not_called()

    async def test_success_returns_the_child_output(self):
        ctx = Mock()
        ctx.inputs = {"greeting": {"state": "succeeded"}}
        ctx.get_result.return_value = {"message": "Hello, Ledgence!"}
        await finish({}, ctx)
        ctx.complete.assert_called_once_with({"message": "Hello, Ledgence!"})
        ctx.fail.assert_not_called()


if __name__ == "__main__":
    unittest.main()
```

Run them from `hello-workflow`:

```sh
uv run python -m unittest discover -s tests -v
```

All three tests should pass. They check registration and ensure that failed or
cancelled children are handled without trying to read a successful result.
The mock context is only for testing this decision logic. It does not run a
workflow or prove that checkpoints survive a restart; we will exercise the
real orchestrator next.

## 4. Build and publish the workflow

```sh
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
```

You now have two published programs: `hello-ledgence@0.1.0` and
`hello-workflow@0.1.0`. The first is a task program; the second composes it.
You do not need to rebuild or republish the task from the earlier lesson.

## 5. Submit the workflow

Create **`hello-workflow/submit.py`** and paste:

```python title="submit.py"
"""Submit the workflow that calls the previously published greeting task."""
import asyncio

from ledgence.client import AsyncClient


async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        workflow = await client.workflows.submit(
            program="hello-workflow",
            version="0.1.0",
            queue="demo",
            data={"name": "Ledgence"},
            idempotency_key="hello-workflow-1",
        )
        print("Workflow:", workflow.id)
        output = await workflow.result(timeout=60)
        print(output["message"])


if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```sh
uv run python submit.py
```

Expect a workflow ID and **Hello, Ledgence!**. Keep the workflow ID for the next
lesson. This workflow creates a **new child task**; it does not reconnect to the
standalone task you submitted earlier.

Repeat the script unchanged to retrieve the same workflow through its
idempotency key. To request another workflow, use a new key such as
`hello-workflow-2`. If you change the input name, change the key too.

As with a task, `result(timeout=60)` only limits the client's wait. It does not
cancel or resubmit the workflow. You can reconnect using
`client.workflows.handle(workflow_id)`; [Receive results](/how-to/receive-results)
explains how.

## 6. Understand what you built

The final output is the same greeting as the task lesson, but the execution
history now also records how the work was composed: a starting entrypoint, an
owned child task, the wait for its outcome, and a resumed entrypoint. Those
records let you inspect the execution in Console.

**Next:** [Explore Ledgence Console](/tutorials/use-console) to find both your
standalone task and this workflow, then follow the child task from its graph.

After that, [Checkpoints and recovery](/concepts/checkpoints-and-recovery)
explains persistence and [Run tasks in parallel](/how-to/parallel-tasks) extends
the same pattern to several children.

**Example files:** [Hello Ledgence](https://github.com/Ledgence/ledgence/tree/develop/examples/hello-ledgence).
