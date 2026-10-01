# Typed entrypoints and durable forks

Write workflow control flow in Python and register each entrypoint with a
decorator. A workflow has exactly one default entrypoint. A branch, timer,
callback, or join resumes at a registered handler; no string-routing `if` chain,
graph declaration, or serialized Python stack is required.

This capability is available in Ledgence 0.2.0. Upgrade the orchestrator and
workers together and apply all migrations, including
`20260928000000_workflow_forks.sql`; follow the [upgrade guide](upgrading-to-0.2.md).
The historical 0.1 releases do not include it. Runtime protocol 3 remains the
package contract.

## Register handlers

```python
from enum import StrEnum
from ledgence.worker.workflow import Workflow

class Entry(StrEnum):
    START = "start"
    DOUBLE = "double"
    TRIPLE = "triple"
    COLLECT = "collect"

workflow = Workflow(Entry)

def summarize(values):
    return {"count": len(values), "sum": sum(values)}

@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    data = event["data"]
    branches = await ctx.fork("calculations:0", branches=[
        ctx.branch("double:0", entrypoint=Entry.DOUBLE,
                   queue=data["queue"], data={"values": data["values"]}),
        ctx.branch("triple:0", entrypoint=Entry.TRIPLE,
                   queue=data["queue"], data={"values": data["values"]}),
    ])
    local = await ctx.local("summary", summarize, values=data["values"])
    return ctx.join(branches, resume=Entry.COLLECT, state={"local": local})

@workflow.entrypoint(Entry.DOUBLE)
def double(event, ctx):
    return ctx.complete(2 * sum(event["data"]["values"]))

@workflow.entrypoint(Entry.TRIPLE)
def triple(event, ctx):
    return ctx.complete(3 * sum(event["data"]["values"]))

@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    for key in ("double:0", "triple:0"):
        if ctx.inputs[key]["state"] != "succeeded":
            return ctx.fail("branch_failed", key + " did not succeed")
    return ctx.complete({
        "local": ctx.state["local"],
        "double": ctx.get_result("double:0"),
        "triple": ctx.get_result("triple:0"),
    })

handle = workflow.build()
```

The [runnable mixed workflow](../examples/mixed-workflow/README.md) also shows a
branch checkpointing to a timer before it completes. One worker slot is enough
to finish it; with more available slots, distributed branches can overlap the
parent's local processing. Queue choice controls which workers can receive them.

`build()` validates and freezes the registry. Every enum value needs exactly one
handler, aliases are rejected, and exactly one handler must be the default.
Handlers receive `(event, ctx)` and may be synchronous or asynchronous. The
exported `handle` is asynchronous. Use this workflow's exact enum members in
`branch(entrypoint=...)`, `join(resume=...)`, and the `continuation` argument of
`continue_`, `suspend`, `wait_event`, and `sleep`. Plain strings and members of
another enum are rejected in a registered workflow. `ctx.entrypoint` exposes
the selected enum member.

Public workflow submissions start at the default handler. Named entrypoints
are currently selected by branch creation and explicit resume decisions; the
submission API does not expose an initial-entrypoint override. The wire value
`start` selects the default on initial activation. A declared enum value `start`
must therefore be that default. Other default names, such as `main`, work too.
Existing controllers using `workflow_context()` and string continuations remain
supported. Enum values are durable addresses: keep them stable for a package
version, and publish a new immutable package for code changes.

## Registration and waiting are separate operations

1. `ctx.branch(...)` builds an immutable specification. It schedules nothing.
2. `await ctx.fork(key, branches=[...])` registers all branch workflows and their
   dispatch obligations in one transaction. Only the acknowledgment completes
   the await. The parent keeps the same activation, revision, and process and
   can immediately perform local work. Dispatch can begin after commit; the
   acknowledgment does not guarantee that any child has already started.
3. `return ctx.join(group, resume=..., state=...)` checkpoints the parent and
   waits for every branch to become terminal. The parent invocation returns and
   releases its worker slot. Healthy pooled processes remain reusable.
4. The orchestrator invokes the resume handler with frozen child outcomes and
   the saved state, even if the children finished before the join was installed.

Each branch is an owned workflow with its own ID, attempts, checkpoints, local
results, and event/timer waits. It starts at its selected entrypoint using the
**exact pinned program descriptor** of its parent. No new program lookup occurs.
Its `data` is the explicit user value in its branch specification. Execution
metadata and entrypoint selection remain outside CloudEvent `data`.

Use `ctx.task(...)` or `ctx.workflow(...)` for a separately named program; those
commands launch when the returned checkpoint decision commits. A same-package
branch can itself stage either kind of child. Ordinary Python calls and
`ctx.local(...)` execute in their current invocation. Calling orchestration
operations from inside a durable local function is not allowed.

## Recovery and outcomes

Fork keys are stable across the parent workflow. Branch keys share its existing
child-key namespace. Exact retries reuse the original registration and children;
changed membership, order, entrypoint, queue, data, or execution policy conflicts.
A new fork cannot adopt a child belonging to another fork or staged command.
Use iteration-qualified keys for new work in loops.

A crash after registration does not roll back accepted children. Retrying the
activation calls the same fork again, reconciles its receipt, and reuses already
acknowledged local results. The original accepting lease can reconcile an exact
receipt after expiry; any other attempt must hold current live authority. New
registrations require a live, uncancelled activation. An uncertain acknowledgment
prevents the SDK from committing a successful final decision.

A `ForkRef` belongs to the current context. Do not put it in checkpoint state.
If a later activation needs the same group, call the same fork with its original
binding, or use `suspend(until=[...])` with its durable child keys. The sealed
membership is immutable. An explicit join can read retained terminal results
again in a later activation; it does not re-execute the children. Automatic
pending-input delivery through `continue_` consumes each input only once.

Joins collect **all terminal outcomes**, including failure and cancellation.
They do not fail fast or automatically cancel siblings when one child fails.
Inspect `ctx.inputs` to recover, or return `ctx.fail(...)` to fail the parent.
Parent cancellation, intentional failure, or exhausted activation retries drain
the owned descendants. Cancellation cannot undo external effects. Local-result
checkpointing and fork registration do not provide exactly-once business effects.

## Portable contract and bounds

The runtime operation is `workflow.fork` with `{key, branches}`. The worker adds
its `LeaseOwner` and processing trace and calls `WorkflowService::fork_workflow`
through `POST /v1/workflows/forks`. The store commits the ledger, owned links,
child runs, first activation tasks, and dispatch obligations atomically. The
receipt contains `{key, branch_keys, already_accepted}`; the worker validates the
ordered keys and acknowledges `{committed: true, key, branch_keys}` to the runtime.
Trace context controls causality and is not part of the immutable fork binding.
Rust contracts contain no Python, PostgreSQL, or broker SDK types. Adapters that
have not implemented this optional operation reject it definitively.

| Resource | Bound |
| --- | --- |
| Branches in one fork / members in one join | 1–64 / at most 64 |
| Complete fork request / command with owner and trace | 128 KiB / 144 KiB |
| Accepted fork ledger per logical activation, including retries | 64 forks / 256 KiB |
| Newly registered children per logical activation across forks and staged decisions | 64 |
| Live owned workflows per parent / maximum child depth | 64 / 16 |

Existing state, local journal, result, and activation-context bounds also apply.
Fork registrations add one acknowledged durable transaction per fork rather
than one extra parent activation. The join still commits a checkpoint. There is
no per-wait process, database connection, or polling loop. Forks reuse the
existing child lifecycle and bounded coordinator batches; they do not establish
unbounded fan-out or a production throughput guarantee.

`tools/check-workflows.py` exercises the public example, live local/remote
overlap, lost registration responses, activation crashes, orchestrator restart,
duplicate event reconciliation, branch failure, and tree cancellation. Its
normal local PostgreSQL and ElasticMQ modes use the same scenarios.
