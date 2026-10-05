# Durable workflow approvals

A workflow can persist an immutable action, release its worker slot, and wait
for a review decision. The decision names the exact request and effective
arguments. An approved continuation can execute those saved arguments as a
durable local step. No model SDK or hosted service is required.

This capability is included in Ledgence 0.3.0. Use matching worker,
orchestrator, Python client, and Console versions and apply all migrations with
`ledgence orchestrator migrate`. The matching Console contract is version 5.
Follow the [0.3 upgrade guide](upgrading-to-0.3.md) before replacing a 0.2 deployment.

## Request and execute an action

```python
from enum import StrEnum
from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, Workflow

class Entry(StrEnum):
    REQUEST = "request"
    APPLY = "apply"

workflow = Workflow(Entry)

def simulate_refund(*, amount, currency="USD"):
    return {"simulated": True, "amount": amount, "currency": currency}

@workflow.entrypoint(Entry.REQUEST, default=True)
def request(event, ctx):
    proposed = event["data"]["amount"]
    effective = min(proposed, 50)  # Apply domain validation/normalization here.
    action = ApprovalAction.for_callable(
        simulate_refund, version="1", arguments={"amount": effective},
    )
    return ctx.request_approval(
        "refund", action=action, proposed_arguments={"amount": proposed},
        timeout_ms=86_400_000, resume=Entry.APPLY, state={},
    )

@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    if ctx.approval.status is not ApprovalStatus.APPROVED:
        return ctx.complete({"status": ctx.approval.status.value, "executed": False})
    result = await ctx.approved_local(simulate_refund, version="1")
    return ctx.complete(result)

handle = workflow.build()
```

`for_callable` freezes the callable identity, explicit version, normalized JSON
keyword arguments, and keyword defaults. Here the effective amount is 50 and
currency is USD. `proposed_arguments` is optional audit context; it never grants
authority to execute. The complete runnable example also validates its input:
[durable action approval](../examples/durable-approval/README.md).

`approved_local` takes no replacement arguments. It verifies the callable's
module, qualified name and declared version, and obtains its arguments from a
private copy of the saved approval wake. Changing a public view cannot alter the
operation. Repeated calls in one activation share the same local-step identity;
a retry returns a previously acknowledged local result. The workflow's immutable
program descriptor also pins the executing package.

The callable is operator-trusted Python code and remains responsible for what
it does internally. Ledgence binds the arguments passed into that callable;
it does not prove what a downstream service executed. An external effect can
succeed before its result is committed. Use an external idempotency key or
reconcile that uncertainty; this API does not guarantee exactly-once effects.

## Review with the client or Console

Once the request has committed, `await run.approval("refund")` returns its
snapshot; `await run.approvals(limit=10)` lists requests by key. There is no
approval before a stored request exists. Open **Approvals** in the workflow's
Console details to inspect the proposed arguments, effective action, server
status, deadline and decision record. The same request appears as an approval
wait in the execution graph.

```python
import json
from pathlib import Path
from ledgence.client import ApprovalDecisionUncertain

run = client.workflows.handle(workflow_id)
approval = await run.approval("refund")
print(approval.action.to_dict())

command = run.prepare_approval_decision(
    approval, decision_id="refund-review:1", decision="approve",
    reviewer="operator", reason="Reviewed the effective amount",
)
# Keep this in application-owned storage before dispatch.
Path("decision.json").write_text(json.dumps(command.to_dict()))
try:
    receipt = await run.decide_approval(command)
except ApprovalDecisionUncertain as error:
    # Decide when to retry; retain exactly this command.
    receipt = await run.decide_approval(error.command)
```

The client does not retry automatically. After restarting the client, restore
with `run.restore_approval_decision(json.loads(Path("decision.json").read_text()))`.
Use the same endpoint, workflow, scope and saved command. Do not create a new
`decision_id` to resolve an uncertain response. Identical commands reconcile
after expiry or workflow completion while the retained record exists; different
decisions, reviewers, reasons, request bindings or arguments conflict.

`reviewer` is caller-supplied attribution, not an authenticated identity. Access
to inspect and decide endpoints belongs behind the installation's authenticated
application boundary. The Console uses the installation's fixed scope and
existing allowed-Origin checks; an Origin check is not user authentication.

## CLI and HTTP

```sh
ledgence approval list --server http://localhost:8080 \
  --tenant acme --namespace billing --workflow WORKFLOW_ID --limit 10
ledgence approval inspect --server http://localhost:8080 \
  --tenant acme --namespace billing --workflow WORKFLOW_ID --key refund
ledgence approval decide --server http://localhost:8080 --file decision.json
```

The decision file above is also accepted by the CLI. It contains `scope`,
`workflow_id`, `key`, `activation_id`, `revision`, `action`, `decision_id`,
`decision`, `reviewer`, and nullable `reason`. The CLI sends once; repeat the
identical file to reconcile an uncertain response. Pass a list response's
`next_cursor` as `--after-key` for the next page.

All three endpoints are POST with JSON bodies:

| Endpoint | Request |
| --- | --- |
| `/v1/workflows/approvals/inspect` | `scope`, `workflow_id`, `key` |
| `/v1/workflows/approvals/list` | `scope`, `workflow_id`, nullable `after_key`, `limit` (1–10) |
| `/v1/workflows/approvals/decide` | The complete frozen decision command |

Console routes use `/v1/console/approvals/{inspect,list,decide}`, omit `scope`,
and represent `revision` as a canonical decimal string. The public API uses an
unsigned 64-bit JSON number; use lossless JSON decoding. Effective argument
identity distinguishes integers, floats, negative zero and booleans and ignores
object member order. Approvals cannot be submitted as generic workflow events.

## Durable lifecycle and limits

1. The workflow checkpoints its state and creates its immutable proposal in the
   same transaction that installs the wait. The request key is one-shot within
   that workflow; a changed action requires a new key.
2. Decision acceptance, deadline checks, cancellation and resumption serialize
   under existing workflow authority. The database owns time. A new decision at
   or after the deadline is too late; an identical accepted decision still
   reconciles. Inspection and list responses report persisted status: a request
   can remain `pending` after its deadline until the coordinator commits expiry.
   The deadline still rejects new decisions during that interval.
3. An accepted decision schedules the existing durable wait coordinator. It
   releases database locks before any program executes. No worker process or
   open connection stays assigned to the paused workflow.
4. Approval, rejection and expiry create one logical resumed activation with a
   frozen approval wake. Attempts of that activation share its identity and
   local result journal. The grant is not inherited by later entrypoints or
   child workflows. Each concurrently waiting branch owns its own request.
5. Workflow cancellation closes a pending request without creating a resumed
   activation. Approved decisions remain audit evidence if cancellation wins
   before execution. Retention removes approval records with their workflow;
   reconciliation is unavailable after retention.

Action name: at most 512 UTF-8 bytes; version: 128. The effective action and
optional original arguments each have a 32 KiB limit. Request keys and decision
IDs have a 128-byte limit. Reviewer is at most 128 bytes; reason 4096. Every wait
has a finite timeout from zero (immediate expiry) through 365 days. A snapshot
is bounded to 96 KiB and a page to ten entries. These bounded records reuse the
current coordinator and queue delivery architecture; no throughput claim is
inferred from functional acceptance tests.

## Design references

The design was checked against primary documentation and issue history on
2026-10-04:

- [PydanticAI #6968](https://github.com/pydantic/pydantic-ai/issues/6968)
  describes approval of model arguments before a transformation changes the
  effective tool call. Ledgence persists both views and only authorizes the
  final action. Domain normalization happens before requesting approval.
- [PydanticAI #6452](https://github.com/pydantic/pydantic-ai/issues/6452) and
  [#5536](https://github.com/pydantic/pydantic-ai/issues/5536) discuss provenance
  and approval binding in their respective trust models. A Ledgence request
  must already exist server-side; a reviewer label does not establish identity.
- [Temporal message handling](https://docs.temporal.io/handling-messages)
  distinguishes validated updates from asynchronous signals. Ledgence returns
  durable decision acceptance separately from eventual workflow execution and
  supplies explicit command replay identity.
- [Prefect interactive workflows](https://docs.prefect.io/v3/advanced/interactive)
  support rescheduled suspension and typed input. Ledgence also releases the
  executor while waiting, and additionally binds its approval to an immutable
  effective action.
- [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
  use checkpoints and reenter interrupted nodes. Ledgence resumes a registered
  entrypoint; acknowledged local results survive attempts, while external
  effects still require idempotency across a commit gap.
- [Restate external events](https://docs.restate.dev/develop/python/external-events)
  offer durable promises. Ledgence reuses its existing durable wait machinery
  with a distinct one-shot action-review contract.

These products already support durable execution and human interaction. The
comparison motivates precise semantics rather than a claim that competitors
lack the capability. PydanticAI's dynamic capability/toolset issue
[#5253](https://github.com/pydantic/pydantic-ai/issues/5253) was resolved by
[#6623](https://github.com/pydantic/pydantic-ai/pull/6623); it is not an open gap
that this feature claims to fix. Per-model-call and per-tool-call agent recovery,
dynamic tool discovery and framework adapters are separate work.

## Verification

```sh
python3.13 examples/durable-approval/check.py
cargo test -p ledgence-orchestration-api --lib workflow_approvals --locked
cargo test -p ledgence-adapter-http --all-features --test contract workflow::approvals --locked
# An owned disposable PostgreSQL 18 server is required for this command.
LEDGENCE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1:5432/ledgence \
  python3.13 tools/check-workflows.py --scenario approvals --scenario approval-example \
  --psql /path/to/psql --binaries target/debug --evidence /path/to/new-evidence
```

The process gate checks unchanged proposals through an orchestrator restart,
lost decision responses and identical reconciliation, changed-action rejection,
rejection/expiry/cancellation, worker-slot release, CLI interoperability, and an
activation retry after its approved local result has committed. Adding
`--endpoint http://127.0.0.1:9324` runs against a disposable local ElasticMQ queue.
