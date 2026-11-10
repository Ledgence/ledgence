# Reopen an execution's progress

**Development source for the next release; not included in 0.9.0.** Use a server
and Python client built from this checkout. No model account, new program or
additional Python dependency is required. Start the matching source-built stack
using the [HTTP deployment guide](../../docs/http-orchestration.md).

In an activated development virtual environment, install this checkout's client:

```sh
python -m pip install ./sdk/python-client
```

Open [`follow.py`](follow.py) in your editor. It reads an existing task or workflow,
prints bounded lifecycle pages and atomically replaces a cursor file **after**
processing each page. It never submits, retries or cancels execution.

From the repository root, replace `TASK_ID` with the ID of a task you already
submitted:

```sh
python examples/execution-progress/follow.py task TASK_ID --checkpoint task-progress.cursor
```

Use `workflow WORKFLOW_ID` to observe a workflow, and choose a different cursor
file. Match `--server`, `--tenant` and `--namespace` to your installation. For
credential-protected installations, pass `--credentials-file access/reader.json`
with `metadata_read` permission. Credentials are not written to the checkpoint.

The default observation timeout is 60 seconds. Rerun the same command to resume
from its saved cursor. A crash after printing but before saving can print records
again: deduplicate by scope, execution kind/ID and sequence when applying updates
to another system. Do not share a checkpoint file between executions or processes.

The default follower ends after it has drained a terminal snapshot. Late cleanup
records can still arrive; reopening the cursor reads them. Application outputs
remain in `result()`. This history does not flatten every child execution, stream
model tokens or contain every local step.

[Complete contract and operating limits](../../docs/execution-progress.md).
