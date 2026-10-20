---
title: Explore Ledgence Console
description: Inspect the greeting task and workflow you just ran, then follow the workflow's child task from its graph.
next:
  label: Run tasks in parallel
  link: /how-to/parallel-tasks
---

Use Console to see **the executions you created in the previous two lessons**.
You already have everything you need; there is no new image to build, program
to download, or example service to start.

## Before you start

Complete [Run your first task](/tutorials/first-task) and
[Run your first workflow](/tutorials/first-workflow), and leave the stack
running. Keep the task ID and workflow ID printed by your `submit.py` files.

Open [http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/).
The orchestrator serves Console as part of your local installation. Its main
sections are **Programs**, **Executions**, and **Workers**.

## 1. Find your published programs

Open **Programs**. You should see:

| Program | Version | Kind |
| --- | --- | --- |
| `hello-ledgence` | `0.1.0` | Task |
| `hello-workflow` | `0.1.0` | Workflow |

Open `hello-ledgence` and its version `0.1.0`. Its details describe the package
you built: its content digest, Python version, platform, and handler. This is
the **program**, not an individual execution of it.

If a program is absent, check that its `program publish --register` command
succeeded. Publication stores executable bytes; registration makes that exact
reference discoverable in this list.

## 2. Inspect the standalone task

Open **Executions**, set **Type** to **Task**, and choose **Apply filters**.
Find the task ID printed by the first lesson. **More filters** includes an
execution ID filter if your instance already has many executions.

Open the task:

- **General → Input** shows the submitted `name`.
- **General → Output** shows the greeting when the task has succeeded.
- **Trace** shows its attempt and recorded history.

For the first submission, the output is:

```json
{"message": "Hello, Ledgence!"}
```

If you opened the second task from the optional input change, it says
`Hello, Ada!` instead. The task's status should be **Succeeded**. These examples
finish quickly; you do not need to catch them while they are running.

## 3. Follow the workflow's actual graph

Return to **Executions**, set **Type** to **Workflow**, choose **Apply filters**,
and open the workflow ID from the second lesson.

Open **Graph**. You should be able to identify the `start` entrypoint, the
`greeting` child task, and the resumed `finish` entrypoint. These are records of
what actually ran. The graph does not predict future work or infer dependencies
from timestamps.

Select the **greeting** node. Its details link to the child execution through
**Open task**. Follow that link to inspect the child's input and output. This
child has its own task ID, distinct from the standalone task in the first
lesson. Use the browser's **Back** button to return to the workflow and continue
exploring.

Switch to **Trace** to see the same durable execution records arranged by time
and status. **Graph** emphasizes how operations relate; **Trace** helps you
inspect when they ran. Optional OpenTelemetry instrumentation can add diagnostic
data, but an external tracing backend is not required for this graph.

Under **General → Output**, the workflow has the same greeting object as its
child. The workflow returned it through `ctx.complete`.

For larger workflows, **Fit** brings the loaded graph into view, and
**Full screen** gives you more space. A subworkflow opens its own graph when you
navigate into it; a canvas shows one workflow level at a time.

## 4. Submit a new greeting from Console

You can also request work without writing another client script. Open
**Executions → New execution** and enter:

| Field | Value |
| --- | --- |
| Program ID | `hello-ledgence` |
| Exact version | `0.1.0` |
| Queue | `demo` |
| JSON input | `{"name":"Console"}` |

Choose **Verify reference**, then **Submit execution**. Console opens the new
task. Once it succeeds, open **General → Output** and expect:

```json
{"message": "Hello, Console!"}
```

This is a real submission to your local instance. **Run again** requests new
work; it does not merely reopen the earlier result.

## 5. Inspect the worker

Open **Workers** and select the worker session. You can inspect its configured
capacity, reported processes, and execution links when available. A process
may remain warm after a task completes so the worker can reuse it for compatible
work.

These are reported observations. Check their timestamps before interpreting
an old report as the worker's current state. A single worker can execute
different programs; it is not permanently assigned to the greeting task.

You have now connected published code, individual executions, and workflow
composition. Continue with [Run tasks in parallel](/how-to/parallel-tasks) or
use the [Console reference](/reference/console) for the complete interface.

When you are finished, return to the stop command in your chosen
[installation guide](/getting-started/installation). Stopping the local stack
normally preserves its data so you can return to these executions later.
