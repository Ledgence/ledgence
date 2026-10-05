---
title: How work runs
description: Understand programs, tasks, workers, and workflows—and choose where work belongs.
---

Ledgence separates the code you publish, the work you request, and the capacity that executes it. This lets the same application code run repeatedly on available workers while execution state lives outside the Python process.

An agent is your application. Ledgence packages its entry point and dependencies as a **program**, submits a **task** to invoke it, and uses a **workflow** when that application needs durable coordination across multiple steps.

## From a published package to a result

1. **Publish the program.** A prepared package contains application code, dependencies, and a manifest. Its program/version identifies immutable bytes by content digest.
2. **Submit work.** The orchestrator records the request in PostgreSQL and binds the execution to a program package.
3. **Acquire an assignment.** A worker receives execution authority through a lease. Queue delivery and durable ownership remain separate concerns.
4. **Prepare and execute.** The worker fetches and verifies the package, uses its cache, and runs the handler in a managed Python subprocess.
5. **Settle the outcome.** The delivery driver reconciles the result with durable orchestration state. A client can inspect it without owning the execution.

The handler receives a complete CloudEvent. Its `data` belongs to your application; task, run, attempt, and tracing identifiers stay in the envelope or invocation context.

## Packages and runtime are separate

A package contains application code and its prepared dependencies. The worker supplies a compatible host CPython interpreter and the small Ledgence helper. It does not run `pip` or install application dependencies when a task arrives.

The manifest declares an exact Python major/minor and an operating system/architecture. Immutable packaging makes the code binding explicit; it does not make a native dependency portable across incompatible platforms.

## Workers reuse bounded process capacity

A worker's concurrency setting **N** creates N consumers and permits at most N managed subprocesses across all programs. Starting, warm, running, and retiring processes count toward that limit.

A healthy warm process can be reused when its artifact digest and compatibility scope match. In the current [self-hosted instance model](/concepts/self-hosted-console), that scope is fixed by the server; operators do not select tenants. Reuse avoids starting Python and importing the same package for every task. It also means module globals and scratch files can persist between invocations.

Each process handles one invocation at a time. Application code must finish its background work and manage state that should not leak between invocations. The current runtime executes operator-trusted code with the worker's OS permissions; the subprocess boundary is lifecycle management, not a hostile-code sandbox.

## Choose a unit of work

| Form | Where it runs | What recovery remembers |
| --- | --- | --- |
| Ordinary function call | Current invocation | No separate durable result; it may repeat with the continuation. |
| `ctx.local(...)` | Current controller process and package | An individually acknowledged local result within the activation. |
| `ctx.task(...)` | Independently scheduled task | A child task with its own attempts, lease, and outcome. |
| `ctx.workflow(...)` | Independently scheduled owned workflow | A child workflow's checkpoints and terminal outcome. |
| `await ctx.fork(..., branches=[ctx.branch(...)])` | Independently scheduled workflows in the parent's exact package | Acknowledged branch registration, then each branch's own checkpoints and outcome. |

Local asynchronous work is useful for overlapping I/O without creating a distributed task for every request. Its invocation still occupies a worker slot. A synchronous local function does not become parallel merely by wrapping it in `ctx.local`.

Distributed children are useful when work needs separate scheduling, a different package, or independent attempts. `task` and `workflow` stage commands for the next accepted checkpoint. A `fork` registers its branches before returning, so the parent can continue local work while matching workers execute those branches. Available worker capacity determines actual overlap; one slot can run the same workflow sequentially. See [Mix local work and branches](/how-to/fork-workflow-branches).

Keep coordination in workflows. A task runs its handler again on retry; journaled local operations belong to the workflow controller that invoked them. A branch is a child workflow with its own identity, state, and entrypoint invocations.

## Workflows release capacity while waiting

A workflow controller runs as a leased task. Its registered entrypoint handler performs local work, coordinates children, and returns a decision describing what should happen next.

A checkpoint can wait for children, an external event, or a timer. Once that decision is accepted, the invocation has ended. The durable wait holds no coroutine, worker reservation, or database connection. A warm subprocess may remain in the ordinary reusable pool.

When the condition is satisfied, Ledgence schedules a new activation at the saved entrypoint with JSON state and selected outcomes. Each activation has its own identity; revisiting the same entrypoint in a loop creates another invocation. Retrying an activation keeps its logical identity and its accepted local results. Python stacks and local variables are not restored.

Console's [workflow graph](/reference/console) follows these recorded executions. It shows one workflow level at a time; open a branch to inspect that child workflow's entrypoints. It does not need a separate graph declaration in your application.

This is why the [first workflow tutorial](/tutorials/first-workflow) can run a controller and its summary child with worker concurrency one.

## Client time and execution time differ

A client's result timeout ends its observation. It does not stop a task, undo effects, or submit another attempt. Execution remains in the orchestration service, so another client can reconnect using the same identity and scope.

Similarly, a logical outcome and physical process cleanup are separate facts. Cancellation cannot undo an external API call that already succeeded. [Checkpoints and recovery](/concepts/checkpoints-and-recovery) explains how to design around retries and uncertain effects.

**Source:** [Architecture](https://github.com/Ledgence/ledgence/blob/v0.3.0/docs/architecture.md) · [Package contract](https://github.com/Ledgence/ledgence/blob/v0.3.0/docs/program-packages.md) · [Worker delivery](https://github.com/Ledgence/ledgence/blob/v0.3.0/docs/worker-delivery.md)
