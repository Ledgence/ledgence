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

A healthy warm process can be reused when its artifact digest, tenant, and namespace match. Reuse avoids starting Python and importing the same package for every task. It also means module globals and scratch files can persist between invocations.

Each process handles one invocation at a time. Application code must finish its background work and manage state that should not leak between invocations. The current runtime executes operator-trusted code with the worker's OS permissions; the subprocess boundary is lifecycle management, not a hostile-code sandbox.

## Choose a unit of work

| Form | Where it runs | What recovery remembers |
| --- | --- | --- |
| Ordinary function call | Current invocation | No separate durable result; it may repeat with the continuation. |
| `ctx.local(...)` | Current controller process and package | An individually acknowledged local result within the activation. |
| `ctx.task(...)` | Independently scheduled task | A child task with its own attempts, lease, and outcome. |
| `ctx.workflow(...)` | Independently scheduled owned workflow | A child workflow's checkpoints and terminal outcome. |

Local asynchronous work is useful for overlapping I/O without creating a distributed task for every request. Its invocation still occupies a worker slot. A synchronous local function does not become parallel merely by wrapping it in `ctx.local`.

Distributed children are useful when work needs separate scheduling, a different package, or independent attempts. Staging children does not mean unlimited parallelism: their actual concurrency depends on available matching workers.

## Workflows release capacity while waiting

A workflow controller runs as a leased task. It performs local work, stages children, and returns a decision describing what should happen next.

A checkpoint can wait for children, an external event, or a timer. Once that decision is accepted, the invocation has ended. The durable wait holds no coroutine, worker reservation, or database connection. A warm subprocess may remain in the ordinary reusable pool.

When the condition is satisfied, Ledgence schedules a new activation with the saved continuation, JSON state, and selected outcomes. The workflow continues from an explicit label, not a suspended Python stack.

This is why the [first workflow tutorial](/tutorials/first-workflow) can run a controller and its summary child with worker concurrency one.

## Client time and execution time differ

A client's result timeout ends its observation. It does not stop a task, undo effects, or submit another attempt. Execution remains in the orchestration service, so another client can reconnect using the same identity and scope.

Similarly, a logical outcome and physical process cleanup are separate facts. Cancellation cannot undo an external API call that already succeeded. [Checkpoints and recovery](/concepts/checkpoints-and-recovery) explains how to design around retries and uncertain effects.

**Source:** [Architecture](https://github.com/Ledgence/ledgence/blob/develop/docs/architecture.md) · [Package contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-packages.md) · [Worker delivery](https://github.com/Ledgence/ledgence/blob/develop/docs/worker-delivery.md)
