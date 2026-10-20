---
title: Run tasks in parallel
description: Reuse the greeting task for several names, wait durably for every child, and collect their results.
---

The [first workflow](/tutorials/first-workflow) requests one greeting task.
This guide extends that pattern: submit several names, create one child task
for each name, and collect the greetings when all children finish.

Use distributed tasks when each operation should have its own execution,
attempts, and scheduling. For concurrent I/O inside one workflow activation,
use `ctx.gather` with `ctx.local` instead. See the
[execution model](/concepts/execution-model) for that distinction.

## Before you start

Complete the task and workflow tutorials. Leave your local **0.6.1** stack
running with `hello-ledgence@0.1.0` published and a worker consuming `demo`.
This example reuses that task's `{"name": "Ledgence"}` input and
`{"message": "Hello, Ledgence!"}` output.

From the folder containing your tutorial projects, create another workflow:

```sh
ledgence program new parallel-greetings --kind workflow
cd parallel-greetings
uv sync
```

Open this project in your editor. Copy the entire `[target]` section from
`hello-workflow/ledgence.toml` into this project's `ledgence.toml`, replacing
the generated target. Both programs run on the same worker. Keep the new
program ID `parallel-greetings`, application version `0.1.0`, and kind
`workflow`.

## Request several children

Replace **`parallel-greetings/program.py`** with:

```python title="program.py"
"""Request one greeting per name and collect the children in input order."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    START = "start"
    COLLECT = "collect"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    data = event["data"]
    names = data.get("names") if isinstance(data, dict) else None
    if not isinstance(names, list) or not 1 <= len(names) <= 64:
        return ctx.fail("invalid_input", "Provide a list of 1 to 64 names.")
    if any(
        not isinstance(name, str) or not name.strip() or len(name) > 200
        for name in names
    ):
        return ctx.fail(
            "invalid_input", "Each name must contain 1 to 200 nonblank characters."
        )

    # Stable keys identify these logical children if this activation is retried.
    keys = [f"greeting:{index}" for index in range(len(names))]
    children = [
        ctx.task(
            key,
            program="hello-ledgence",
            version="0.1.0",
            queue="demo",
            data={"name": name},
        )
        for key, name in zip(keys, names)
    ]
    # Save the keys explicitly; Python variables do not survive a durable wait.
    return ctx.suspend(
        continuation=Entry.COLLECT,
        state={"keys": keys},
        until=children,
    )


@workflow.entrypoint(Entry.COLLECT)
async def collect(event, ctx):
    keys = ctx.state["keys"]
    inputs = ctx.inputs
    # Failed and cancelled children also satisfy the wait.
    if any(inputs[key]["state"] != "succeeded" for key in keys):
        return ctx.fail("greeting_failed", "A greeting task did not succeed.")
    return ctx.complete({
        "messages": [ctx.get_result(key)["message"] for key in keys],
    })


handle = workflow.build()
```

`ctx.task` describes each child; it does not wait for that child's result.
Returning `ctx.suspend` saves the commands, child keys, and continuation in one
checkpoint. Ledgence schedules the children and releases the parent's
execution slot while it waits. `COLLECT` runs after **all** listed children
reach a terminal state.

Saving the keys in input order makes the result order predictable, even if the
children finish in a different order. Failed and cancelled children are
checked before `ctx.get_result`, which only reads successful output.

## Test the workflow decisions

The scaffold's generated tests exercise its original greeting implementation.
Replace **`tests/test_program.py`** with tests for these handlers:

```python title="tests/test_program.py"
"""Check workflow decisions locally before trying the real orchestrator."""
import unittest
from unittest.mock import Mock

from program import Entry, collect, start


class ProgramTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_input_stages_no_children(self):
        invalid_inputs = [
            None,
            {},
            {"names": []},
            {"names": ["Ada"] * 65},
            {"names": [42]},
            {"names": [" "]},
            {"names": ["x" * 201]},
        ]
        for data in invalid_inputs:
            with self.subTest(data=data):
                ctx = Mock()
                await start({"data": data}, ctx)
                ctx.fail.assert_called_once()
                self.assertEqual(ctx.fail.call_args.args[0], "invalid_input")
                ctx.task.assert_not_called()
                ctx.suspend.assert_not_called()

    async def test_checkpoint_keeps_every_child_key(self):
        for count in (1, 3, 64):
            with self.subTest(count=count):
                names = [f"Person {index}" for index in range(count)]
                keys = [f"greeting:{index}" for index in range(count)]
                ctx = Mock()
                # Simple references let us inspect the saved wait decision.
                ctx.task.side_effect = lambda key, **kwargs: key
                await start({"data": {"names": names}}, ctx)
                self.assertEqual(ctx.task.call_count, count)
                for call, key, name in zip(ctx.task.call_args_list, keys, names):
                    self.assertEqual(call.args, (key,))
                    self.assertEqual(call.kwargs, {
                        "program": "hello-ledgence",
                        "version": "0.1.0",
                        "queue": "demo",
                        "data": {"name": name},
                    })
                ctx.suspend.assert_called_once_with(
                    continuation=Entry.COLLECT, state={"keys": keys}, until=keys
                )
                ctx.fail.assert_not_called()

    async def test_unsuccessful_child_does_not_read_results(self):
        for state in ("failed", "cancelled"):
            with self.subTest(state=state):
                ctx = Mock()
                ctx.state = {"keys": ["greeting:0", "greeting:1"]}
                ctx.inputs = {
                    "greeting:0": {"state": "succeeded"},
                    "greeting:1": {"state": state},
                }
                await collect({}, ctx)
                ctx.fail.assert_called_once_with(
                    "greeting_failed", "A greeting task did not succeed."
                )
                ctx.get_result.assert_not_called()
                ctx.complete.assert_not_called()

    async def test_results_follow_saved_key_order(self):
        ctx = Mock()
        ctx.state = {"keys": ["greeting:0", "greeting:1"]}
        ctx.inputs = {
            "greeting:1": {"state": "succeeded"},
            "greeting:0": {"state": "succeeded"},
        }
        outputs = {
            "greeting:0": {"message": "Hello, Ada!"},
            "greeting:1": {"message": "Hello, Linus!"},
        }
        ctx.get_result.side_effect = outputs.__getitem__
        await collect({}, ctx)
        ctx.complete.assert_called_once_with({
            "messages": ["Hello, Ada!", "Hello, Linus!"],
        })
        ctx.fail.assert_not_called()


if __name__ == "__main__":
    unittest.main()
```

Run the four tests from `parallel-greetings`:

```sh
uv run python -m unittest discover -s tests -v
```

These tests check input bounds, checkpoint contents, and result handling.
The mock context does not schedule tasks or persist checkpoints. The next
section exercises that behavior through your running Ledgence installation.

## Publish and try it

Build and publish from the `parallel-greetings` directory, as in the workflow
tutorial:

```sh
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
```

Create **`parallel-greetings/submit.py`**:

```python title="submit.py"
"""Ask the workflow to greet three people using the existing task program."""
import asyncio

from ledgence.client import AsyncClient


async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        workflow = await client.workflows.submit(
            program="parallel-greetings",
            version="0.1.0",
            queue="demo",
            data={"names": ["Ledgence", "Ada", "Linus"]},
            idempotency_key="parallel-greetings-1",
        )
        print("Workflow:", workflow.id)
        output = await workflow.result(timeout=60)
        for message in output["messages"]:
            print(message)


if __name__ == "__main__":
    asyncio.run(main())
```

Run the file:

```sh
uv run python submit.py
```

After the workflow ID, expect:

```text
Hello, Ledgence!
Hello, Ada!
Hello, Linus!
```

Open the workflow in [Console](/tutorials/use-console) to inspect its three
child tasks. Repeating this submission unchanged returns the same workflow.
Choose a new idempotency key when changing the names or requesting another
run. The 60-second timeout limits the client's wait; it does not cancel or
resubmit the workflow.

## Choose capacity deliberately

Children become eligible for scheduling after the checkpoint is accepted.
They execute simultaneously only when matching workers have spare capacity.
A worker with concurrency one runs them sequentially; the parent's durable
wait does not occupy its slot.

A checkpoint can stage at most 64 combined task/subworkflow commands and wait
for at most 64 children. Its encoded decision must also fit within 256 KiB.
Use references to application-managed storage for larger inputs.

Child keys belong to the whole workflow. Reuse a key only for the same child
binding. If a loop should create new work, include an iteration in its keys,
such as `greeting:round-2:0`.

This example fails the workflow if any greeting fails or is cancelled. A
different application can collect partial results or schedule recovery work
under new child keys.

**Next:** [Mix local work and workflow branches](/how-to/fork-workflow-branches)
for branches that use the parent's package and can start while it continues
local work.

**Related:** [Workflow context reference](/reference/workflow-context) · [Checkpoint contracts](https://github.com/Ledgence/ledgence/blob/v0.6.1/docs/workflows.md)
