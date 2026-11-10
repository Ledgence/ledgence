---
title: Reopen an execution's progress
description: Follow committed task or workflow transitions and resume with a saved cursor.
---

**Development source for the next release; not available in 0.9.0.** Use the
server and Python client from the same development checkout. The released
container stack does not yet expose this endpoint.

When an investigation runs for several minutes, you can close its observer and
return later. A saved cursor identifies the last page you processed. Reopening
it reads the remaining retained transitions without starting the work again.

## Follow from your editor

With your development virtual environment active, install the client from the
repository root:

```sh
python -m pip install ./sdk/python-client
```

Create `watch_execution.py` in your editor. Replace `TASK_ID` with a task you
already submitted, and match the server and scope to your installation:

```python
import asyncio
from pathlib import Path
from ledgence.client import AsyncClient, ProgressTimeout

async def main():
    checkpoint = Path("task-progress.cursor")
    cursor = checkpoint.read_text().strip() if checkpoint.exists() else None
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo",
    ) as client:
        task = client.tasks.handle("TASK_ID")
        try:
            async for page in task.follow_progress(cursor=cursor, timeout=60):
                for event in page.events:
                    print(event.sequence, event.reason, flush=True)
                # Save only after handling every event in this page.
                temporary = checkpoint.with_suffix(".tmp")
                temporary.write_text(page.next_cursor)
                temporary.replace(checkpoint)
        except ProgressTimeout:
            print("The observer stopped. The task continues; rerun to resume.")

asyncio.run(main())
```

Run `python watch_execution.py`. Rerun the same file to continue from its cursor.
For a workflow, use `client.workflows.handle("WORKFLOW_ID")` and a separate
checkpoint file. A credential-protected installation also needs
`credentials_file="access/reader.json"` in `AsyncClient`, with `metadata_read`.

A crash between printing and saving can display a page twice. If you update a
database or another system, deduplicate by scope, execution kind/ID and event
sequence. Printing once is not an exactly-once delivery guarantee.

## Choose how long to observe

The follower reads up to 100 events per page. After catching up it allows the
server to wait up to 20 seconds, returning sooner when an event arrives. Observers
at the same position share server reads. Set `wait_ms=0` for immediate polling;
`poll_interval` sets a minimum request cycle interval and defaults to one second. It stops after draining a terminal snapshot by default. Late worker
cleanup can still add history; reopening the saved cursor reads it.

Use `stop_on_terminal=False` to keep observing, with `timeout` to bound the wait.
Timeout and Ctrl-C stop observation; they do not cancel or resubmit the task.
Use `await task.progress(cursor=cursor, limit=50)` for one immediate page.
Your existing `task.subscribe(...)` remains a terminal webhook subscription.

The CLI supports the same read:

```sh
ledgence task progress --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --task TASK_ID --follow
```

Its output is JSONL pages. Save a returned `next_cursor` and pass `--cursor` to
resume. For a workflow, use `workflow progress --workflow WORKFLOW_ID`.
The MCP tool `ledgence_execution_progress` returns one bounded page per call.

## What this history includes

These are committed lifecycle transitions, not application outputs or streaming
model tokens. A workflow's history covers its own coordination; inspect its
children separately. Each sequence identifies one retained fact, and timestamps
do not imply dependencies between parallel branches.

Retained executions keep their complete history. After execution retirement,
reads return `NotFound`; resetting a cursor cannot restore deleted evidence.
## Follow in Console

In matching development Console, open a task and choose **Trace → Lifecycle**,
or open a workflow and choose **General → Lifecycle**.
The live timeline shows committed lifecycle events. It keeps a bounded window,
resumes its cursor after transient connection failures and offers earlier history
explicitly. Reloading the page rebuilds from retained history; it does not save
progress or credentials in browser storage. Leaving the view stops observation.
The graph and trace views continue to show their existing execution relationships.
A server without the progress capability keeps the original paginated history.

[Runnable example](https://github.com/Ledgence/ledgence/tree/develop/examples/execution-progress)
· [API, cursor and retention contract](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-progress.md)
