# Checkpoint workflows

Ledgence workflows execute Python continuations as ordinary leased tasks. A
continuation can perform useful local async work, persist individual local
results, and explicitly schedule distributed tasks. When it needs only their
results, it saves a checkpoint and returns. The orchestrator stores the wait;
no workflow invocation, coroutine, worker reservation, or database connection
remains allocated for that wait. A healthy subprocess may remain in the normal
warm pool, available for reuse or replacement.

This first workflow slice provides durable local steps, distributed child tasks,
sealed all-terminal waits, explicit continuations, workflow results and
cancellation. [External events and durable timers](workflow-events.md) add
one-shot callback waits and persisted deadlines to the same checkpoint model.
[Owned subworkflows](subworkflows.md) add nested workflow composition and
parent/child lifecycle coordination. Administrative redrive, automatic code
upgrades, and large externalized checkpoint/result payloads are later capabilities.

## Application model

Use a prepared Python package with runtime protocol **3**. The manifest schema
stays at version 1; the runtime interpreter and dependency packaging requirements
are the same as [ordinary programs](program-packages.md). Protocol 1 and 2
synchronous programs retain their existing behavior, including as distributed
children of a workflow.

```python
from ledgence_worker.workflow import workflow_context

async def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        user_id = event["data"]["user_id"]
        profile, orders = await ctx.gather(
            ctx.local("profile", fetch_profile, user_id=user_id),
            ctx.local("orders", fetch_orders, user_id=user_id),
        )
        analysis = ctx.task(
            "analysis",
            program="large-analysis",
            version="1.0.0",
            queue="analysis",
            data={"profile": profile, "orders": orders},
        )
        return ctx.suspend(
            continuation="after_analysis",
            state={"user_id": user_id},
            until=[analysis],
        )
    if ctx.continuation == "after_analysis":
        outcome = ctx.inputs["analysis"]["outcome"]
        if outcome["kind"] != "succeeded":
            return ctx.fail("analysis_failed", "The analysis child did not succeed")
        return ctx.complete({
            "user_id": ctx.state["user_id"],
            "analysis": ctx.get_result("analysis"),
        })
    return ctx.fail("unknown_continuation", ctx.continuation)
```

`fetch_profile` and `fetch_orders` are application functions in the same prepared
package. Their arguments must include every input that determines their durable
binding. The handler always receives the original user-owned CloudEvent `data`.
Ledgence supplies the checkpoint, continuation, and child outcomes through the
separate invocation context. Python locals, stacks, sockets and coroutine state
are not serialized.

Execution choices have different recovery behavior:

| Choice | Placement | Durable recovery unit |
| --- | --- | --- |
| Ordinary Python call | Current invocation | May repeat with its continuation |
| `await ctx.local(key, function, **inputs)` | Current invocation/package | Individual acknowledged local result |
| `ctx.task(key, ...)` | Independently admitted child task | Child task with its own attempts and leases |
| `ctx.workflow(key, ...)` | Independently scheduled owned workflow | Child workflow with its own checkpoints and terminal outcome |

`ctx.task` stages a command. It does not launch the child before the returned
checkpoint decision is accepted. `ctx.continue_(continuation=..., state=...)`
commits a checkpoint and schedules the next activation immediately, while owned
children may continue running. `ctx.suspend(..., until=[...])` waits for every
listed child to become terminal, including failed or cancelled children; the
continuation decides what those outcomes mean. Empty membership is immediately
ready. Strings can reference children registered in an earlier continuation.

Local-step keys are scoped to a logical activation and remain stable through
that activation's task attempts. Bindings include the callable name, explicit
JSON inputs, and the workflow's pinned package. Child keys are scoped to the
whole workflow. Reusing a child key with the same normalized submission reuses
its original task and package digest; changing its binding conflicts. Include an
explicit iteration identifier in keys when a loop should create new work.

`ctx.complete(output)` cannot discard staged launches or finish while an owned
task or workflow child is still nonterminal. `ctx.fail(kind, message)` is an intentional workflow
decision. Unexpected controller exceptions, runtime failures and timeouts use
the activation task's retry policy instead. Exhausted activation retries fail
the workflow and drain its owned tasks.

## Persistence and recovery

Workflow submission atomically records the immutable submission/program binding
and its first activation task. Its idempotency keys are scoped by tenant and
namespace, independently of ordinary task submission keys. Successful submission
reconciliation does not contact the program store again.

During an activation, every completed local step is sent through an
invocation-scoped runtime request to the orchestration service. Its `await`
resolves only after the store commits the record and the acknowledgment returns.
Concurrent application I/O can proceed while these bounded commit requests are
serialized. Sequential dependent steps each pay their acknowledgment latency.
On an activation retry, already committed local results are loaded into the new
context and returned after binding checks; those functions are not deliberately
executed again. Ordinary code around those steps re-executes from the latest
explicit continuation.

An external effect can succeed before its local record commits. A crash in that
window leaves the operation unconfirmed and a retry can repeat it. Business
idempotency keys or application reconciliation remain necessary for such effects.
Checkpointing does not provide exactly-once external execution.

A controller's final decision follows the existing task settlement and cleanup
boundary. Only a real terminal task transition creates a durable completion
obligation. Cancellation and workflow failure separately create durable drain
obligations to stop and settle remaining owned tasks. The coordinator validates the accepted controller result, then commits
the checkpoint revision, consumed input batch, child registrations, wait, and
required task/dispatch obligations atomically. It does not require the old worker
lease to remain active after the controller has settled.

Completion obligations are recoverable after orchestrator restart. Child
completion is checked from durable state when a wait is installed, so a child
that finishes before wait registration is not lost. Child events arriving during
an activation do not change that activation's frozen inputs or checkpoint
revision. A stale worker cannot add new local results or replace an accepted
controller outcome; exact accepted receipts can be reconciled after expiry.

The PostgreSQL adapter bounds retries of failed completion-obligation application:
once that obligation is at least 24 hours old, a further transient application
failure fails the workflow and starts draining its owned tasks. Age is measured
from obligation creation, so coordinator downtime counts. This is a retry cutoff,
not a timer that terminates healthy long-running workflows or prevents an older
obligation from being applied successfully. Drain obligations keep retrying until
the owned-task terminal boundaries can be established.

Cancellation stops new workflow decisions from scheduling children and drains
owned work in bounded batches. Workflow states `cancelling` and `failing` remain
nonterminal until owned tasks reach their logical terminal boundaries. The
existing [task cleanup and lease guarantees](task-results.md) still apply; an
external side effect cannot be undone by cancelling its caller.

## Interfaces and adapters

The portable `WorkflowStore` and `WorkflowService` ports are separate from
ordinary `TaskStore`/`TaskService`. `ApplicationService::with_workflows` enables
the capability. An embedding application must supply a workflow store that
shares the required task/scheduling authority, then supply the optional workflow
service to its HTTP router and `DeliveryDriver::with_workflows`.

The packaged orchestrator/connected worker wire these ports together. PostgreSQL
implements the initial workflow persistence authority. Explicitly apply migration
`20260915000000_workflows.sql` before serving the updated executable. The optional
SQS-compatible adapter continues to transport compact task references for both
activations and children. Workflow state is never stored in queue receipts, and
the core API contains no SQS or PostgreSQL types.

| Endpoint | Operation |
| --- | --- |
| `POST /v1/workflows` | Submit a workflow using the existing `SubmitCommand` JSON shape |
| `GET /v1/workflows/status` | Read compact workflow status |
| `GET /v1/workflows/result` | Read status and an optional terminal outcome |
| `POST /v1/workflows/events` | Accept or reconcile a directly addressed external CloudEvent |
| `POST /v1/workflows/cancel` | Request cancellation with `{scope, workflow_id}` |
| `POST /v1/workflows/activations/context` | Worker read using its exact `LeaseOwner` |
| `POST /v1/workflows/local-results` | Commit `{owner, record}` for a local step |

GET requests require `tenant_id`, `namespace`, and `workflow_id` query parameters.
Status/result reads do not acquire execution authority. Waiting in the Python
client is an observation timeout; it neither resubmits nor cancels the workflow.
See the [Python client](../sdk/python-client/README.md) for its workflow handle.

`workflow_id` identifies the workflow execution; each activation/child retains
its existing task, run and attempt identities. CloudEvents add `ldgworkflowid`,
and controller events add `ldgactivationid`. The activation ID equals its stable
controller task ID; retries receive new attempt IDs. Runtime context uses schema
`ledgence.workflow.activation.v1`, and its checkpoint decision carries `v: 1`,
`activation_id`, and `revision`. An optional `wake` adds the frozen external event/timer result; it is
omitted on ordinary child continuations. Old contexts without `wake` remain valid.
Revision advances on committed control decisions,
independently of local journal progress and incoming child completions.

## Bounds and performance

The one worker concurrency setting still governs N consumers and at most N
managed subprocesses globally. Concurrent local coroutines use their owning
invocation's slot. Applications control their own connection pools and request
fan-out; N does not count individual HTTP requests. A workflow awaiting active
local I/O retains that invocation. A durable distributed wait returns from it.

The first inline contract enforces these compact JSON bounds:

| Resource | Limit |
| --- | --- |
| Checkpoint state | 64 KiB |
| One local record (binding and output) | 128 KiB |
| Local ledger per logical activation | 128 records / 256 KiB combined |
| One decision, including staged commands | 256 KiB |
| Commands and all-terminal wait members per decision | 64 each |
| Frozen child inputs per activation | 64 entries / 256 KiB combined |
| Complete activation context | 640 KiB |

Use application-controlled object references for larger payloads. Exceeding an
inline bound is an error; the system does not silently drop results or wait members.
Ordinary application values retain the documented JSON number and depth rules.

The coordinator uses bounded batches and a 50 ms idle scan interval, with bounded
catch-up and error backoff. Durable waits require no per-wait polling, heartbeat,
timer, or connection. Standalone task terminalization skips workflow SQL entirely.
The first adapter still serializes decisions for an individual workflow and is
not a sharded, unbounded-fan-out implementation. Throughput and overhead must be
measured for the deployment's program, payload, storage and queue configuration;
local I/O eliminates per-operation distributed task delivery but still incurs
local-result durability traffic.


## Running and verifying the slice

The [checkpoint workflow example](../examples/checkpoint-workflow/README.md)
includes package preparation, publication, worker startup, and Python client
submission. It runs with worker concurrency 1.

The [workflow acceptance gate](performance.md#workflow-placement-measurements)
checks real Python execution, individually acknowledged locals, lost local-result
acknowledgments, worker-crash recovery, orchestrator restart while suspended,
depth-64 application values, and the public example with integrated or local
ElasticMQ delivery. PostgreSQL tests separately cover transaction rollback,
stale attempt fencing, completion-before-wait, completion during activation,
cleanup boundaries, cancellation, bounded journals, and permanent decision
failure. Offline unit tests alone do not establish these storage/transport claims.
