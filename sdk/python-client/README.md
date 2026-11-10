![Ledgence](https://ledgence.com/brand/ledgence-logo-v1.png)

# Ledgence Python client

An MIT-licensed, asynchronous Python client for submitting tasks and workflows,
observing execution, and registering durable completion notifications with a
self-hosted Ledgence service. The distribution is `ledgence-client`; its public
import is `ledgence.client`. Python 3.11–3.14 is tested on Linux x86_64 and macOS
arm64. Public APIs may evolve before version 1.0.

Ledgence 0.9.0 is available on PyPI. Use matching server support. [Release reference](https://docs.ledgence.com/reference/releases).

## Installation

Use Python 3.11 or newer and an activated virtual environment.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ledgence-client
```

This selects an available registry version. For an existing deployment, select
the client version matching your service and pin that exact version in your
application's dependency configuration. Review release notes before upgrading.
Native bundles and registry packages are published separately.

For an unreleased checkout or a locally qualified build, install its selected
wheel instead. Replace the example path with that wheel's actual filename.

```sh
python -m pip install /path/to/your/client.whl
```

The client connects to an existing Ledgence service. Follow the
[installation guide](https://docs.ledgence.com/getting-started/installation) to start one. Then [publish the greeting task](https://docs.ledgence.com/tutorials/first-task). The [client reference](https://docs.ledgence.com/reference/python-client)
provides a shorter guide to everyday operations; the contracts below cover their
behavior in detail.

## Submit a task

After publishing `hello-ledgence@0.1.0` in the first task tutorial, save this as
`submit.py` and run it in the environment where you installed the client:

```python
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        task = await client.tasks.submit(
            program="hello-ledgence", version="0.1.0", queue="demo",
            data={"name": "Ledgence"},
            idempotency_key="client-readme:hello:1",
        )
        print(task.id)
        output = await task.result(timeout=60)
        print(output)

asyncio.run(main())
```

The caller owns its event loop. Keep one client open across calls and use it on
that loop. Programs receive the complete CloudEvent and execute in the Rust worker.
Protocol v3 programs can use synchronous or asynchronous Python handlers. The client neither uploads
packages nor imports handlers. It is separate from the dependency-free
`ledgence.worker` runtime helper and does not package that helper or CPython.
Both use the native `ledgence` namespace: neither distribution owns a root
`ledgence/__init__.py`, and the client keeps its typing marker in `ledgence/client/`.
Programs use `Workflow` or `workflow_context` from `ledgence.worker.workflow`; the old
`ledgence_worker` imports must be updated before running on current workers.

## Task references and observations

Save the task ID, tenant, namespace and server location to reconnect. Constructing
`client.tasks.handle(task_id)` makes no network call. Handles retain their scoped
client; use a new client's handle after that client's lifetime ends.

| Operation | Result |
| --- | --- |
| `await task.status()` | Compact `TaskStatus` with scheduling and attempt metadata |
| `await task.outcome()` | `TaskResult`; `.outcome` is `None` exactly while queued/active |
| `await task.wait(timeout=60)` | Full terminal `TaskResult`, including failures/cancellation as values |
| `await task.result(timeout=60)` | User JSON output, or `TaskFailed` / `TaskCancelled` |
| `await task.cancel()` | Actual acknowledged `TaskState`; it can still be `active` |

States and outcome kinds compare naturally to strings. Successful JSON `null`
returns Python `None`; it is distinct from a pending `TaskResult.outcome`.
Failures retain structured application, execution or lost-attempt details. They
never instantiate arbitrary remote exception classes. `TaskFailed.result` and
`TaskCancelled.result` retain the full observation.

Logical success does not imply exactly-once external effects or confirmed
physical cleanup. Check `result.outcome.quiescence` when cleanup evidence matters;
`result()` returns a succeeded task's output even when quiescence is unconfirmed.
Cancellation outcomes have no deciding attempt or output. The status's
`latest_attempt_id` is only a diagnostic reference to earlier work.

<a id="credentials-development"></a>

## Credentials

For a credential-enabled instance, keep the private credential file supplied by
your administrator outside application source and pass its path:

```python
from pathlib import Path
from ledgence.client import AsyncClient, Forbidden, Unauthenticated

async with AsyncClient(
    "https://ledgence.example.com",
    tenant="acme",
    namespace="demo",
    credentials_file=Path.home() / ".config/ledgence/operator.json",
) as client:
    try:
        status = await client.tasks.handle("task_id").status()
    except Unauthenticated:
        print("The credential is missing, expired, revoked, or invalid.")
    except Forbidden:
        print("This credential does not permit the requested operation.")
```

The file contains exactly `server` and `token`. Its normalized server endpoint
must match the client, including any base path. On POSIX, it must be a regular
file owned by the current user without group/other permissions (normally mode
`0600`); symlinks are rejected. Reads are bounded. HTTPS is required except for
trusted loopback HTTP development. Authentication does not follow redirects.

Applications using a secret manager can instead pass `token=credential` as a
programmatic argument. Never supply both options. Do not put reusable tokens in
source, shell arguments, logs, or task data. The client reads a file at
construction; create a new client after replacing the credential. No credential
is included in prepared commands or saved approval decisions.

`Unauthenticated` and `Forbidden` are definitive `ServiceError` subclasses.
A matching 401/403 response does not become submission or approval uncertainty,
and the client does not automatically retry it. Existing clients without a
credential continue to support operator-trusted deployments. Credential options
require matching server support. The published 0.7.0 client predates this
authentication boundary.

## Workflow references and observations

A workflow starts a published controller program that returns explicit checkpoint
decisions. First publish `hello-workflow@0.1.0` using the
[workflow tutorial](https://docs.ledgence.com/tutorials/first-workflow).
Inside the same `async with AsyncClient(...) as client` block, replace the task
submission with this fragment:

```python
workflow = await client.workflows.submit(
    program="hello-workflow", version="0.1.0", queue="demo",
    data={"name": "Ledgence"},
    idempotency_key="docs:workflow:1", correlation_key="docs:workflow",
)
print(workflow.id)
output = await workflow.result(timeout=60)
```

This workflow calls the greeting task from the earlier lesson and uses the same
`acme/demo` client scope. Application package versions are independent of the
client version.

Save the workflow ID and reconnect with `client.workflows.handle(workflow_id)`.
`status()` returns `WorkflowStatus`; `outcome()` returns `WorkflowResult` with
`.outcome=None` while work is pending. `wait()` returns the terminal result;
`result()` returns successful JSON or raises `WorkflowFailed` / `WorkflowCancelled`.
`cancel()` returns the acknowledged `WorkflowStatus`, which can be `cancelling`
while active work drains. Failure and cancellation remain nonterminal in the
`failing` and `cancelling` states.

Each owned subworkflow has its own workflow ID. Reconnect to that ID with the same
`client.workflows.handle(...)` API to inspect its status/result, send an event, or
cancel it independently. `WorkflowStatus.parent_workflow_id` is null for roots;
`root_workflow_id` is the root's own ID for a root and the ancestor root ID for a
nested workflow. These fields are immutable across observations. A terminal child
controller task does not imply that its workflow is complete.

The public `submit()` endpoint starts root workflows. Controllers create owned
children using the worker helper's `ctx.workflow(...)`. Parent cancellation and
failure drain the owned tree before reaching a terminal status. Same-package
branches use `await ctx.fork(...)` with registered entrypoints and share that
owned lifecycle. See
[`docs/subworkflows.md`](https://github.com/Ledgence/ledgence/blob/main/docs/subworkflows.md) for composition and result
semantics.

`WorkflowWaitTimeout` is also a `WaitTimeout`. It retains `.workflow`, `.last_status`
and `.last_error`; observation timeout never cancels or resubmits the workflow.
Observe the saved handle again to continue waiting. For durable external notification
without keeping the client running, register a completion subscription below.

For an uncertain submission, `SubmissionUncertain.submission` retains a frozen
`WorkflowSubmission` from `client.workflows.prepare(...)`. Resend that command
explicitly to the workflow endpoint with the same idempotency key. Task and workflow
commands are distinct types to prevent accidentally replaying one as the other.
`WorkflowCancellationUncertain` similarly retains the workflow for reconciliation.

Controller authoring and durability semantics are described in
[`docs/workflows.md`](https://github.com/Ledgence/ledgence/blob/main/docs/workflows.md).

## Durable completion subscriptions

Register notification delivery to an operator-configured destination alias:

```python
from ledgence.client import CompletionSubscriptionUncertain

command = task.prepare_subscribe(
    destination="billing-results", idempotency_key="invoice-completion",
)
try:
    subscription = await task.subscribe(command)
except CompletionSubscriptionUncertain as uncertain:
    # Persist this command and reconcile under your application's retry policy.
    saved_command = uncertain.command.to_dict()
    raise

print(subscription.id)
print((await subscription.status()).state)
```

`await task.subscribe(destination="billing-results", idempotency_key="invoice-completion")`
is the convenience form; `workflow.prepare_subscribe()` and `workflow.subscribe()`
use the same contract. The workflow form observes the complete workflow, including
owned-child draining, rather than a controller activation. `client.completions.prepare()`
accepts an explicit `CompletionTarget("task", task_id)` or
`CompletionTarget("workflow", workflow_id)`. `client.completions.subscribe(command)`
reconciles either prepared command.

Registration is one HTTP operation, not a background listener in Python. After its
durable acceptance the caller can disconnect. Registration also works after the
execution finishes. The guarantee begins when registration commits: submitting
work and registering its subscription are separate operations. Save the task or
workflow identity and retry registration if the client stops between these calls.
The idempotency key is scoped to the execution; changing its destination conflicts.

Save the subscription ID and reconnect with
`client.completions.handle(subscription_id)`. `status()` returns a bounded
`CompletionSubscription` with its immutable command, delivery state, generation,
attempt counters, timestamps, last failure, and nullable completion CloudEvent.
States are `waiting`, `pending`, `delivering`, `retrying`, `delivered`, and `exhausted`.
`waiting` means the execution is still nonterminal; `delivered` confirms receiver
acceptance, not completion of business effects at the receiver.

For explicit redelivery after exhaustion:

```python
from ledgence.client import CompletionRetryUncertain

status = await subscription.status()
if status.state == "exhausted":
    command = subscription.prepare_retry(expected_generation=status.generation)
    try:
        status = await subscription.retry(command)
    except CompletionRetryUncertain as uncertain:
        saved_command = uncertain.command.to_dict()
        raise
```

`await subscription.retry(expected_generation=status.generation)` is the convenience
form. Reuse the same prepared command when acceptance is uncertain. Repeating an
accepted retry observes the current generation without rearming delivery again;
it never reruns the task or workflow. Like other mutations, the SDK does not retry
subscription or redelivery requests automatically. Caller cancellation remains
`asyncio.CancelledError`; prepare and persist commands before awaiting if they must
survive that cancellation.

`to_dict()` saves the wire command, not the server URL. Persist that URL alongside
the command and reconnect to the same endpoint and scope. To reconstruct a saved
subscription after a caller restart, first verify its saved `scope` matches the
new client's scope, then prepare the same binding:

```python
from ledgence.client import CompletionTarget

command = client.completions.prepare(
    target=CompletionTarget(**saved_command["target"]),
    destination=saved_command["destination"],
    idempotency_key=saved_command["idempotency_key"],
)
subscription = await client.completions.subscribe(command)
```

For a saved redelivery command, reconnect using its `subscription_id` and call
`prepare_retry(expected_generation=saved_command["expected_generation"])` on that
handle. Preserve that generation even if a later status read has advanced.
Generation 1000 is the final generation and cannot be rearmed.

The initial notification is a reference-only CloudEvent. Execution IDs, terminal
state, business correlation, result reference, and trace context live in its
envelope; it has no `data` field and does not copy user output. Fetch the result
through the existing scoped task/workflow API. Receivers must deduplicate using
`(source, id)` and durably accept a notification before acknowledging it. Notification
retries and explicit redelivery preserve the event identity. See
[completion notifications](https://github.com/Ledgence/ledgence/blob/main/docs/completion-notifications.md) for delivery
semantics, destination configuration, limits, and receiver behavior.

## Finding tasks

```python
page = await client.tasks.list(
    state="failed", correlation_key="INV-1042", limit=50,
)
for task in page.items:
    print(task.task_id, task.state, task.latest_attempt_id)
if page.next_cursor is not None:
    page = await client.tasks.list(
        state="failed", correlation_key="INV-1042", limit=50,
        cursor=page.next_cursor,
    )
```

`list()` returns one immutable `TaskPage` containing a tuple of compact
`TaskStatus` observations and a nullable `next_cursor`. Each call has the client's
existing request deadline. It does not fetch results or automatically traverse
more pages. Filters always apply inside the client's tenant and namespace.

Optional `state` accepts one state string or `TaskState`; `queue` and
`correlation_key` match exactly. An omitted correlation filter matches any key;
`correlation_key=""` matches only an explicitly empty key. `submitted_from` is
inclusive and `submitted_until` exclusive, in Unix epoch milliseconds from zero
through `253402300799999`. If both are present, `submitted_from` must be smaller.
`limit` defaults to 50 and must be an integer from 1 through 100.

Tasks are ordered by immutable `(submitted_at, task_id)`, descending. Treat the
cursor as opaque, repeat the same filters and use it with the same scope; changing
the page limit is allowed. A null cursor ends this traversal. Each page reads
committed state when queried. Multiple pages are not a frozen snapshot: tasks may
change state or become visible between requests, and concurrent changes can make
matching tasks enter or leave the remaining traversal. Start again without a
cursor to refresh. Use a task handle's status or result for subsequent observation.

## External workflow events

Send the complete original CloudEvent to a one-shot wait key:

```python
from ledgence.client import WorkflowEventUncertain

workflow = client.workflows.handle(saved_workflow_id)
command = workflow.prepare_event("payment:1", event={
    "specversion": "1.0", "id": "payment-1042", "source": "/billing/payments",
    "type": "invoice.paid", "datacontenttype": "application/json",
    "data": {"invoice_id": "INV-1042", "status": "paid"},
})
try:
    receipt = await workflow.send_event(command)
except WorkflowEventUncertain as error:
    # Application policy decides when to resend these unchanged bytes.
    saved_command = error.command.to_dict()
    raise
```

`await workflow.send_event(key="payment:1", event=original_event)` is the
convenience form. Preparation freezes the endpoint, scope, workflow, key and full
event; it generates no event ID and changes no CloudEvent fields. Store the
prepared command before an await when caller cancellation may require later
reconciliation. `to_dict()` returns an independent JSON copy. After a caller
restart, reconnect to the same endpoint/scope/workflow and call
`prepare_event(saved_command["key"], event=saved_command["event"])`.

`WorkflowEventReceipt` contains `scope`, `workflow_id`, `key`, `event_id`,
`event_source`, `accepted_at` (Unix milliseconds), and `already_accepted`.
Acceptance means durable storage, not that the workflow already consumed the
event. An event may arrive before the controller registers its wait. Source and
ID identify an event within the workflow run, and each wait key accepts one event.
Resending an identical command returns its receipt, including after workflow
completion. Changed bindings raise `Conflict`; closed/late events produce
`ServiceError` with `code="obsolete_operation"`.

Event POSTs never automatically retry. An uncertain response raises
`WorkflowEventUncertain` with `.command`, `.cause`, and an optional `.request_id`.
Explicitly resend `.command` to reconcile. `asyncio.CancelledError` still
propagates; cancellation does not prove that acceptance failed. Known
pre-dispatch deadline expiration remains `RequestTimeout(dispatched=False)`.

Events use the common CloudEvents JSON profile: version1.0; nonempty `id`,
`source`, and `type`; `datacontenttype="application/json"`; and a required `data`
field (which may be null). Optional time/subject/schema/tracing and scalar context
extensions are validated. Execution IDs are not required; forwarded context
fields remain intact. Complete encoded events are capped at 64 KiB, commands at
70 KiB, and application data at depth64. Preparation uses the strict Python-encoded size, so a float-format boundary may
be rejected conservatively even if the Rust encoding would fit. Wait keys are one-shot for the whole run;
use new iteration keys when the controller waits again.

Generic events supply application input and cannot decide a durable approval.
Use the [approval API](#workflow-approvals) to review an immutable effective action.

External event IDs are limited to 128 UTF-8 bytes and sources to 2,048 UTF-8
bytes, in addition to the complete event budget. Sources must be valid URI
references; encode non-ASCII URI characters with percent escapes.

## Submission uncertainty

An explicit idempotency key is required. Optional `RetryPolicy`,
`attempt_timeout_ms`, `correlation_key` and `origin_trace` map to the existing
server fields; omitted scheduling settings retain server defaults.

```python
from ledgence.client import SubmissionUncertain

submission = client.tasks.prepare(
    program="invoice-issuer", version="1.0.0", queue="billing",
    data={"invoice_id": "INV-1042"}, idempotency_key="issue:INV-1042",
)
try:
    task = await client.tasks.submit(submission)
except SubmissionUncertain as error:
    # The application chooses when to retry this same immutable command.
    saved_command = error.submission.to_dict()
    raise
```

`prepare()` is synchronous and local. It freezes the endpoint, scope, command and
origin context (including absence) before transmission; later changes to the
caller's data cannot change it. `to_dict()` returns an independent copy suitable
for application-owned persistence. Reconstruct with the same key and semantic
input after a caller restart. Sending a prepared submission through a different
endpoint or scope is rejected locally.

POST submission and cancellation have no automatic retries. A lost, malformed or
unavailable response—including a valid server `503 unavailable`—may follow a
commit. `SubmissionUncertain` retains the frozen command; `CancellationUncertain`
retains `.task`. Both expose `.cause` and any server `.request_id`. Definitive
`NotFound`, `Conflict` and `ServiceError` reject this exchange; they cannot prove
that another concurrent exchange with the same key never committed.

Cancelling a Python await propagates `asyncio.CancelledError` and never sends
remote cancellation. If submission was in flight, use the prepared command or
original stable key/input to reconcile; cancellation does not prove rejection.
Closing the client likewise does not cancel remote tasks.

## Deadlines and ownership

`request_timeout` defaults to 30 seconds and must be positive, finite and at most
30. `wait`/`result` have a separate positive finite observation timeout, default
60; there is no unbounded or zero mode. They poll compact status at most once per
second after each observation, then fetch the result within the same observation
budget. Transient read failures are retried during that budget. Protocol errors
and definitive rejections are immediate. A request deadline includes admission,
network/body transfer, decoding and validation. Convenience submission includes
local preparation too; an already prepared command was encoded before its later
exchange budget begins.

`RequestTimeout.dispatched` says whether network dispatch began. Known
pre-dispatch expiration is not mutation uncertainty. `WaitTimeout` contains
`.task`, optional `.last_status`, and optional `.last_error`; it never means task
failure, absence or cancellation. The SDK checks deadlines before each logical
exchange and after decoding, rejecting late success.

The client admits at most eight active HTTP exchanges and two codec jobs.
Caller-created concurrent requests wait for admission within their own deadlines. Waiting tasks sleep outside network admission. A timed-out codec
waiter does not free its job's slot while the work is still running. Client close
cancels owned exchanges and joins remaining codec work. If the close await itself
is cancelled, the owned close continues; call `await client.close()` again to join
it. CPU validation and Python's GIL are cooperative, so this is not a hard
real-time deadline or a promise to forcibly terminate native calls.

The explicitly selected threaded resolver uses the host's DNS behavior. One
connector coalesces same-host resolution while cancelled waiters detach; an
already running OS DNS call may continue, and event-loop/default-executor
shutdown can wait for it. The selected aiohttp release may transparently retry a
GET connection failure once under the same timer. POST is excluded. This SDK
does not alter private retry flags or pretend every GET is one wire request.

TLS uses the host Python SSL context and trust roots. Redirects and compressed
responses are rejected; system proxy configuration is not implicitly adopted.
No vendor service or account is required. Error responses are capped at 64 KiB,
status responses at 16 KiB, task pages at 2 MiB, and other responses at the
existing 16 MiB bound.
Client I/O admission does not alter worker execution concurrency.

## JSON and optional tracing

User data stays user-owned JSON. The client preserves signed i64/unsigned u64
integers, finite binary64, integer/float distinction, negative floating zero,
Unicode scalar strings and escaped U+0000. Objects require string keys; tuples
normalize to arrays. Data has the existing 1 MiB compact JSON/64-container-depth
limit. Results retain current server/report limits. Oversized integers, nonfinite
numbers, Decimal/custom objects, cycles, lone surrogates and duplicate response
keys are rejected. Encode exact decimal quantities or larger identifiers as
strings. No Pydantic coercion, pickle or arbitrary remote Python objects are used.

```python
from ledgence.client.otel import enable_context

enable_context()  # Requires the optional opentelemetry-api dependency.
```

The application owns any OTel provider/exporter. Context capture and HTTP client
spans are explicitly enabled; the base client imports no OTel dependency.
Preparation snapshots the application's active valid context once; an explicit
`TraceContext` wins and explicit `origin_trace=None` freezes absence. Replaying a
prepared submission does not replace that durable context, while later transport
spans use the currently active context. No input, output or idempotency key is
recorded as a span attribute. Task/run/event/attempt IDs and per-exchange request
IDs remain distinct from tracing IDs.

## Workflow approvals

Matching clients and servers provide a scope-wide inbox. By default it lists
pending requests, earliest deadline first. Pass an exact `program_id` to filter,
or `status=None` to include every persisted status. Read the full effective
action before preparing a decision.

```python
from datetime import datetime, timezone

cutoff = datetime(2026, 11, 6, 17, tzinfo=timezone.utc)
page = await client.workflows.approval_inbox(program_id="refunds", due_before=cutoff, limit=20)
for item in page.items:
    workflow = client.workflows.handle(item.workflow_id)
    approval = await workflow.approval(item.key)
    print(approval.status, approval.action.name, approval.action.arguments)
```

Pass `page.next_cursor` as `cursor` with the same filters to continue when it
is present. Each page reflects the current database state. A request can change
status between discovery and inspection.

`await workflow.approval(key)` inspects a committed request.
`await workflow.approvals(after_key=None, limit=10)` returns a bounded, ordered
`ApprovalPage`; pass its `next_cursor` as `after_key` for the next page. Read
`approval.action.arguments` for the effective action and
`approval.proposed_arguments` for optional original input. These views return
independent JSON copies and expose typed `ApprovalStatus` values.

Requests created with the worker's
`ctx.request_effect_approval(...)` expose
`approval.protection is ApprovalProtection.EXTERNAL_EFFECT`. The prepared
decision preserves this protection, and receipt validation rejects a changed
or missing marker. Import `ApprovalProtection` from `ledgence.client`. Generic
and historical requests have `protection is None`, and their saved commands
omit the field. Use matching client, CLI and worker versions for protected
requests. The 0.7.0 client predates this optional response field.

```python
from ledgence.client import ApprovalStatus, ApprovalDecisionUncertain

approval = await workflow.approval("refund")
if approval.status is ApprovalStatus.PENDING:
    command = workflow.prepare_approval_decision(
        approval, decision_id="review-1", decision="approve", reviewer="operator",
        reason="Reviewed the stored effective action",
    )
    # Save command.to_dict() in application-owned storage before dispatch.
    receipt = await workflow.decide_approval(command)
```

Choose `approve` or `reject` only after reviewing the exact stored action. The
command binds its scope, workflow, request activation/revision, full action,
and any requested external-effect protection.
`reviewer` is a caller-supplied label. It does not establish the actor who accepted
the decision. Credential-enabled servers record verified acceptance
separately, as described below. No pending request is created by deciding.

`ApprovalDecisionUncertain` retains the command when the response cannot prove
acceptance. No automatic resend occurs. Explicitly resend that command, or use
`workflow.restore_approval_decision(saved_dict)` after a client restart. An
identical decision returns its original receipt; changed decisions or bindings
conflict. Restoring a command performs validation only; it never grants approval.

<a id="operator-audit-development"></a>

## Operator audit

Matching clients and credential-enabled native servers expose a scoped metadata
audit feed.
The credential needs `metadata_read`. Operator-trusted deployments and adapters
without the audit capability report audit as unavailable.

```python
from ledgence.client import ApprovalStatus, AuditFilter

# Inspect the original acceptance without changing the approval or decision.
approval = await client.workflows.handle(workflow_id).approval("refund")
if approval.status in (ApprovalStatus.APPROVED, ApprovalStatus.REJECTED):
    accepted_by = await client.audit.approval_acceptance(approval)
    if accepted_by is None:
        print("No verified actor recorded")
    else:
        print(accepted_by.actor.subject, accepted_by.actor.issuer)
else:
    print("Approval status", approval.status.value)

# Read one bounded page of metadata directly associated with a workflow.
page = await client.audit.list(
    filter=AuditFilter.resource("workflow", workflow_id), limit=50,
)
for event in page.items:
    print(event.recorded_at, event.actor.subject, event.operation.kind)
```

`approval_acceptance` requires an approved or rejected observation from this
client's endpoint and scope. It validates the full request occurrence, decision
ID, decision and acceptance time. The actor is the original verified operator or
service, even when the current reader or credential has changed. Its
`credential_id` identifies the original credential; it is not a bearer token.
Only a successful empty exact query returns `None`. Authentication, permission,
unavailable and invalid-response errors propagate; they must not be presented as
historical absence.

`client.audit.list()` reads the current scope. A resource filter includes directly
owned observations, not a traversal into child tasks or subworkflows. Continue
with `page.next_cursor`, the same filter and limit. Cursors are opaque; do not
construct them or infer a total execution order from event timestamps. The feed
contains typed resource IDs, verified actor metadata and observation times,
without action arguments, reviewer labels, reasons or payloads. A sensitive
response event records authorization to return that response. Delivery may
still fail. Audit queries themselves create no audit events.

Returned audit events describe recorded facts. The server supplies authorization
through the verified principal and its durable acceptance linkage.
Decisions and restored commands retain their existing wire format. For an audited
decision, a deliberate identical retry by the same verified actor can recover the
original receipt after credential rotation; a changed actor cannot take ownership
of that receipt. Historical decisions accepted without audit remain unattributed
after an otherwise valid identical replay.

<a id="retained-effect-evidence-development"></a>

## Retained effect evidence

Matching clients and servers expose `client.effects` to inspect retained
evidence from approved
external-effect execution, including records whose original execution has been
collected. Reads do not authorize execution. The assessment API below records an operator decision without redispatching or confirming the journal.

With an open, scoped `AsyncClient`, find an occurrence and inspect its original
effect activation:

```python
from ledgence.client import EffectFilter, EffectObservationEntry

# Metadata only: unconfirmed, confirmed, discrepant, disputed, or all (default).
page = await client.effects.inbox(filter=EffectFilter.UNCONFIRMED, limit=20)
for item in page.items:
    print(item.occurrence.workflow_id, item.journal_state, item.has_discrepancy)

if page.items:
    occurrence = page.items[0].occurrence
    evidence = await client.effects.inspect(
        workflow_id=occurrence.workflow_id,
        activation_id=occurrence.activation_id,
        approval_key=occurrence.approval_key,
    )
    print(evidence.binding.approval.action.name)
    for entry in evidence.entries:
        if isinstance(entry, EffectObservationEntry):
            print(entry.sequence, entry.source.kind, entry.output)
```

`inbox(filter=EffectFilter.ALL, workflow_id=None, cursor=None, limit=50)` returns
an `EffectInboxPage`. `limit` ranges from 1 through 100. It uses the client's
scope, optionally restricts one workflow and orders by most recent update, then
byte-ordered occurrence identity. `discrepant` selects differing reports; it is
independent of the journal being confirmed. Metadata reads need `metadata_read`
in credential mode; the trusted deployment mode can also read this metadata.

`inspect(workflow_id=..., activation_id=..., approval_key=..., cursor=None,
limit=4)` returns an `EffectEvidenceDetail` with the frozen binding, current
summary and up to four whole entries, ordered by sequence. Supply the occurrence
from the inbox, including its original effect activation. Sensitive inspection
requires credential mode, `sensitive_read`, and a working durable audit sink.
The server records `AuditReadKind.EFFECT_INSPECT` with an `AuditEffect` resource
before returning evidence. Trusted mode without audit cannot expose the detail.
Authentication, permission, missing-record and unavailable errors propagate;
none is an empty result.

Both response types have a 1 MiB bound. A byte-limited page can contain fewer
than `limit` items and still have `next_cursor`. Pass that opaque cursor with the
same filters or occurrence until it is `None`. Do not use page length to decide
whether reading is complete. Inbox pagination is a current view, not a frozen
snapshot across requests; concurrent updates may move occurrences.

Returned models are frozen. `.output` supplies an independent copy of each
reported application JSON value, preserving integers through `u64::MAX` without
converting them to floats. Claims and `verified_worker_claim` sources identify
the authenticated worker credential, callback and ingress (`live_confirm` or
`late_observe`); this is worker attribution, not provider verification. Historical
`legacy_attempt`, `legacy_fence` and `legacy_journal` sources remain explicitly
unattributed. A confirmed journal is Ledgence's recorded execution result, not
proof of exactly-once provider effects. Private lease owners and frozen worker
commands are never returned. See the [effect evidence contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md).

<a id="operator-effect-assessments-development"></a>

## Operator effect assessments

Matching components support native operator assessment and typed workflow
resumption. The resolver needs the
explicit `effect_resolve` capability; this is separate from `approval_decide`,
execution control and sensitive reads. A credential limited to `effect_resolve`
can submit a previously reviewed command without receiving application evidence.
`access init` grants the new capability to newly created operator credentials;
existing credentials do not gain it automatically.

After inspecting an occurrence, freeze your assessment and save it **before**
sending. This example assumes `evidence` came from the inspection above:

```python
from pathlib import Path
from ledgence.client import EffectResolutionAssessment, EffectResolutionUncertain

command = client.effects.prepare_resolution(
    evidence,
    resolution_id="support-case-1042-assessment-1",  # Stable identity for this decision.
    assessment=EffectResolutionAssessment.record_success({"receipt": "refund-1042"}),
    reason="Matched the original action against the provider receipt.",
    evidence_references=["support:case:1042", "provider:refund:1042"],
)
# Application-owned storage: this file contains sensitive action/result data.
saved = Path("effect-assessment.json")
saved.write_text(command.to_json(), encoding="utf-8")
try:
    receipt = await client.effects.resolve(command)
    print(receipt.resolution_id, receipt.already_accepted)
except EffectResolutionUncertain:
    print("Acceptance is unknown. Keep this saved command for reconciliation.")
```

For a documented failure use
`EffectResolutionAssessment.record_failure("declined", "Provider rejected the action")`.
Use `close_unknown()` to record that the outcome remains unknown. Success and
failure require at least one evidence reference; closing unknown permits none.
Reasons are nonblank and limited to 4096 UTF-8 bytes. Up to eight ordered
references of 1024 UTF-8 bytes each are retained exactly. Success results are
JSON, including `None`, with a 128 KiB bound. The full command is at most 256 KiB.

`prepare_resolution` freezes the original occurrence, approved action, reviewed
`effect_revision` and prior assessment ID from the detail header. The header's
`current_assessment` is available even when that entry is outside the current
page. Preparing a command does not claim that every evidence page was read.
A newer revision makes a new command conflict; inspect again and make a new
assessment with a new resolution ID. Do not silently refresh a saved command.

`command.to_json()` stores a versioned envelope with the canonical endpoint and
exact request-body text. `command.to_dict()` is a detached wire view for inspection;
it is **not** the saved format. Restore after restarting the application:

```python
# Use the original endpoint, scope and stable actor (rotated credentials are allowed).
command = client.effects.restore_resolution(saved.read_text(encoding="utf-8"))
receipt = await client.effects.resolve(command)
```

Restore validates locally without contacting the server, rejects a different
endpoint or scope, and preserves the original bytes. The saved envelope is
bounded to 1 MiB and contains no credential. There are no automatic mutation
retries. `EffectResolutionUncertain` retains `.command` and `.cause`; an invalid
success response, dispatched timeout or unavailable response can mean acceptance
already happened. A definite permission or conflict error remains that error.
Cancellation propagates; keep the saved command because acceptance may precede
response cancellation.

An identical command by the same verified actor returns the original receipt,
including after supersession or execution collection. A changed command or actor
conflicts. Acceptance is append-only and durably audited as `AuditEffectResolved`;
the metadata receipt includes `acceptance_event_id`, not proof of workflow resumption.
`await client.audit.event(receipt.acceptance_event_id)` reads that exact scoped
event or returns `None` after a successful empty lookup; errors stay errors.
The equivalent `AuditFilter.event(event_id)` query requires `limit=2` and no cursor.

`EffectFilter.DISPUTED` selects a current success assessment contradicted by a
later unequal JSON report. `DISCREPANT` separately describes differing historical
reports. The new `operator_resolution` summary and `current_assessment` detail
are independent of journal state. Reports are opaque: a failure or unknown
assessment cannot be contradicted merely by interpreting a report's `state` key.
An assessment does not grant dispatch, assert provider truth, or rewrite a prior
workflow wake. Workflows opt into native rendezvous with
`ctx.wait_effect_resolution(...)`; cancellation and collection never revive work.

## Local verification

The repository's [package verification gate](https://github.com/Ledgence/ledgence/blob/main/tools/check-python-client.py)
builds a source distribution, rebuilds its wheel, and tests the installed client
outside the checkout using the reviewed dependency inventory. Run it from the
repository root to retain the verified distributions:

```sh
python tools/check-python-client.py --dist-dir /path/to/new-dist-directory
```

See its `--help` for offline wheelhouse and retained-environment options. Source
tests also run from the repository root with:

```sh
PYTHONPATH=sdk/python-client/src python -m unittest discover -s sdk/python-client/tests -v
```

The selected interpreter must already have the pinned dependencies. Optional
trace tests run when the reviewed OTel API is installed; no exporter is used.
The source distribution includes its tests and JSON fixture corpus. After
installing the package and dependencies, run `python -m unittest discover -s tests -v`
from its extracted directory. `LEDGENCE_JSON_FIXTURES` can explicitly select an
alternative JSON corpus; the repository gate checks the bundled corpus against
the shared Rust/Python fixtures. Dependency versions, wheel/source hashes,
licenses and notices are retained under `third_party`; normal wheel installation
pins the reviewed runtime closure. The gate verifies those pins and distributed
legal files. No dependencies are downloaded during program execution.

<a id="typed-provider-operations-development"></a>

## Typed provider operations

Matching client, API and worker builds support approved asynchronous
provider operations. The client
continues to use `client.effects`; no provider SDK or model service is required.

```python
from ledgence.client import ProviderFilter, ProviderSucceeded

page = await client.effects.inbox(provider_filter=ProviderFilter.NEEDS_REVIEW)
for item in page.items:
    occurrence = item.occurrence
    detail = await client.effects.inspect(
        workflow_id=occurrence.workflow_id,
        activation_id=occurrence.activation_id,
        approval_key=occurrence.approval_key,
    )
    binding = detail.binding.approval.provider_binding
    print(binding.business.key, detail.summary.provider.knowledge)
    report = detail.provider.current_report
    if report and isinstance(report.report.knowledge, ProviderSucceeded):
        print(report.report.knowledge.result)  # A copy; JSON null is a valid success.
```

`provider_filter` accepts `ProviderFilter.ALL`, `.PENDING`, `.NEEDS_REVIEW`,
`.SUCCEEDED`, `.FAILED`, `.CANCELLED` and `.DISPUTED`. It requires the ordinary
`filter` to remain `EffectFilter.ALL`. Omit it for the existing mixed effect inbox.
A provider cursor is bound to that provider filter, scope and workflow selector;
it cannot be reused with another filter. Follow `next_cursor` even on short pages.
`NEEDS_REVIEW` excludes historical disputes covered by the current clean operator
assessment; `DISPUTED` retains that history. New conflicting evidence against the
current assessment makes the occurrence need review again.

`ApprovalProtection.PROVIDER_OPERATION` requires a `ProviderBinding`. Its frozen
business identity, adapter contract, target, optional provider deduplication
domain/key/horizon, credential **reference**, reconciliation policy and pinned
program are reviewed together with `action.arguments`. Preparing and restoring
an approval decision preserves and echoes that full binding. It never embeds a
provider credential or invokes the provider from the client.

`EffectEvidenceSummary.provider` separates `knowledge`, `reconciliation`,
`handling`, `selected_report`/`conflicting_reports` and `candidate_revision`.
`EffectEvidenceDetail.provider.current_report` is a bounded typed header, even
when its timeline entry is on another page. Typed entries include
`ProviderDispatchEntry`, `ProviderProbeScheduledEntry`, `ProviderProbeSourceEntry`,
`ProviderReportEntry`, `ProviderProbeFinishedEntry` and `ProviderRecoveryStoppedEntry`.
A report's `knowledge` is one of `ProviderUnknown`, `ProviderPending`,
`ProviderRequiresAction`, `ProviderSucceeded`, `ProviderFailed` or
`ProviderCancelled`. Disputed aggregate knowledge retains conflicting report or
provider-reference facts. A reference conflict can arise before any terminal
outcome; `disputed` is not a seventh individual report outcome.

The SDK keeps exact integer metadata and immutable models; returned application
JSON is copied. `TaskStatus.provider_probe` identifies a managed read-only check
and its original occurrence; a successful check task alone does not establish
provider success. Inspect the typed report instead.

Provider reports remain adapter-classified evidence. Journal state stays
`unconfirmed`; operator handling and workflow wake selection are independent.
The existing frozen operator-resolution command and permission are unchanged.
For typed provider evidence a later terminal report can also dispute a recorded
failure. Closing an unknown outcome does not assert provider truth or authorize
redispatch. A selected workflow wake is never rewritten.

See the [provider recovery guide](https://docs.ledgence.com/how-to/recover-provider-operations/)
and [source contract](../../docs/provider-operations.md).

## Resource policies and admission inspection

Matching 0.9.0 servers expose `client.resources` and `client.admissions`.
These provider-neutral APIs require configured credentialed access; installing a
new client does not enable the server capability. Published availability remains
in the release reference above.

An operator creates a named, scoped resource with a concurrency limit and an
optional token-bucket rate. `occupied` includes outstanding offers, running
callbacks and unresolved starts; `unknown` is the unresolved subset.

```python
from ledgence.client import ResourcePolicy, ResourceRate

# Inside an authenticated AsyncClient context with resource_control permission:
receipt = await client.resources.put(
    "reply-service",
    policy=ResourcePolicy(
        max_occupancy=4,
        rate=ResourceRate(numerator=10, period_ms=1_000, burst=10),
    ),
    request_id="reply-service:create:1",
    reason="Limit concurrent callbacks and accepted starts",
)
print(receipt.resource.revision)
print(await client.resources.get("reply-service"))
page = await client.resources.list(limit=50)
```

`expected_revision=None` creates a policy; pass the last reviewed revision for a
compare-and-swap update. The `request_id` belongs to the immutable command. If an
acknowledgement is uncertain, preserve and resend all the same arguments. A
changed command with the same ID conflicts. Calls do not automatically retry.
Disabling a resource pauses new admission; it does not prove existing work ended.

To inspect an occurrence, use its original workflow activation and logical key,
not the most recent resume activation. Inspection includes sensitive arguments
and results, requires `sensitive_read`, and records an audit event:

```python
from ledgence.client import AdmissionOccurrence

occurrence = AdmissionOccurrence(
    client.scope, workflow_id="workflow-id",
    origin_activation_id="original-activation-id", key="reply:1",
)
detail = await client.admissions.inspect(occurrence)
print(detail.state, detail.revision)
```

After independently reviewing an unresolved source, an operator with
`resource_control` can call `client.admissions.release(occurrence,
source_id=detail.source["source_id"], expected_revision=detail.revision,
request_id="review:1", reason="Explain the reviewed evidence")`. This explicit,
audited override releases capacity. It does not report successful execution or
invoke the callback again. Preserve the exact command when reconciling an
uncertain response. A later completed result may still be retained with its
original source; release and completion must not release occupancy twice.

Worker Start/Finish calls are deliberately absent from this application client
surface. The runtime owns their lease identity and execution permission. Use
`Workflow.operation`, `ctx.request_admission` and `ctx.admitted_operation` from
[`ledgence.worker`](../python/README.md#durable-resource-admission-development-source)
for governed execution.


### Protected provider admission

Admission inspection also accepts governed provider bindings: version 2 includes
`admission.resource`; version 1 omits it and retains its original encoding.
`ProviderBinding.admission` is an optional `ProviderAdmissionRequirement`.
`AdmissionSnapshot.provider` exposes an immutable `ProviderAdmissionRecord`;
`record.call_completion` is `ProviderCallCompletion.COMPLETED`, `UNKNOWN`, or
`None` when no report has been accepted. `record.to_dict()` returns a fresh copy
of the linked ticket, source, and report receipt.

These records describe the dispatch and its capacity settlement. They do not
assert business success: a completed call may have returned a pending provider
operation. Continue inspecting the protected provider operation for its business
outcome. A ticket grants no authority to invoke the provider again. These models require a matching 0.9 server with provider admission enabled.

### Standalone task admission

Use `from ledgence.client import TaskAdmission` and pass
`admission=TaskAdmission(resource="support-model")` to `client.tasks.prepare` or
`submit`. The optional requirement is frozen with the submission; it never changes
CloudEvent `data`. `TaskStatus.admission_deferrals` preserves the difference
between total assignment generations and retry budget consumed. Inspect a source
with `await client.task_admissions.inspect(task.id, generation=1)`; release only
a reviewed source with `client.task_admissions.release(inspected, request_id=...,
reason=...)`. No automatic retry of a mutation. See the [task admission
contract](../../docs/resource-admission.md#standalone-task-admission).

### Queue concurrency

Use `client.queue_policies.get`, `list`, `holds`, `put` and `release` with a matching 0.9.0 server and a scoped credential. Reconcile an uncertain write by submitting the same arguments and request ID. Release takes an uncertain hold inspected at the same server and scope. [Configure a queue and inspect a hold](../../docs/queue-concurrency.md).

## Find outstanding resource sources

With a credential that has `sensitive_read`, find the task generations, workflow
operations or provider approvals behind a resource's unknown occupancy.
Every page requires a committed response audit.

```python
from ledgence.client import ResourceSourceState

cursor = None
while True:
    page = await client.resources.sources(
        "support-model", state=ResourceSourceState.UNKNOWN_OCCUPIED, cursor=cursor
    )
    for source in page.items:
        print(source.subject, source.revision, source.updated_at)
    cursor = page.next_cursor
    if cursor is None:
        break
```

Omit `state` to include accepted Starts still classified as `possibly_dispatched`.
Inspect the returned workflow occurrence with `client.admissions.inspect` or
the task ID and generation with `client.task_admissions.inspect` for its retained
source evidence. Each inspection has its own audit record.

## Weighted resource offers

Pass `ResourceFairSharing` and its `ResourceFairGroup` values to `ResourcePolicy(fair_sharing=...)` to weight offers between queues. Read a page with `await client.resources.decisions("support-model", limit=20)`, then pass `before=page.next` for older offers when `page.next` is present. [Policy, example and compatibility](https://github.com/Ledgence/ledgence/blob/develop/docs/resource-admission.md#weighted-offers).

### Shared run budgets

Pass `budget=RunBudgetPolicy(max_calls=1, max_delegations=2)` when submitting a workflow. Import `RunBudgetPolicy` from `ledgence.client`. For a standalone task, supply its resource admission requirement as well. Read `await client.budgets.inspect(workflow_id=run.id)` and `await client.budgets.entries(workflow_id=run.id, limit=20)`. An execution subject always resolves to its server-derived root. [Runnable example](https://github.com/Ledgence/ledgence/tree/develop/examples/shared-run-budget).

## Execution progress (development source)

This checkout adds `await handle.progress(cursor=None, limit=100)` and
`async for page in handle.follow_progress(cursor=None, poll_interval=1.0, timeout=None, stop_on_terminal=True, limit=100)`
for task and workflow handles. A page contains typed state, immutable lifecycle
events, an observed high-watermark and `next_cursor`. Process a complete page,
then persist that cursor; deduplicate repeated deliveries by scope/target/sequence.
The iterator has no background buffer and polls only when caught up.

`ProgressTimeout` includes the execution, `last_page` and `next_cursor`; it never
cancels or resubmits execution. Other transport, permission and protocol errors
propagate. Default completion means a terminal snapshot was drained, not that no
late cleanup event can arrive. `subscribe()` remains terminal webhook registration.
These methods require a matching development server and are not in published
0.9.0 packages. [Contract](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-progress.md)
and [runnable example](https://github.com/Ledgence/ledgence/tree/develop/examples/execution-progress).
