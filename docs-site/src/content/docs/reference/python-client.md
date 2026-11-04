---
title: Python client
description: Core submission, observation, task discovery, and external-event APIs in ledgence.client.
---

The `ledgence-client` package exposes `ledgence.client` for asynchronous submission and observation. It is separate from the worker-supplied `ledgence.worker` helper.

This reference covers `ledgence-client` **0.8.0**, published on PyPI. It requires Python 3.11 or newer; the qualified test matrix covers Python 3.11–3.14 on Linux x86_64/glibc and macOS arm64. The pre-1.0 API can evolve, so pin versions and review changes before upgrading.

## Installation

A project created with `ledgence program new` already declares the client;
run `uv sync` in that project. For an existing uv application, add it with:

```sh
uv add "ledgence-client==0.8.0"
```

If you use pip instead, run `python -m pip install "ledgence-client==0.8.0"`
inside your application's activated virtual environment. Choose the command
for your environment manager; do not run both. The optional `otel` extra is
`ledgence-client[otel]==0.8.0` when your application needs tracing integration.

The client does not include a server, worker, or Python worker helper. Start a
complete stack through [local installation](/getting-started/installation), then
use the [first task tutorial](/tutorials/first-task) for a complete `submit.py` file.
The snippets below show API calls **inside** an asynchronous function and open
client; they are reference fragments rather than standalone scripts.

The current self-hosted Console operates one instance. Older client scope fields remain for compatibility; they must match the server binding and do not provide tenant administration. The Compose example uses `acme/demo`; a new instance with an omitted binding uses `default/default`. See [One self-hosted instance](/concepts/self-hosted-console).

## `AsyncClient`

```python
from ledgence.client import AsyncClient

async with AsyncClient(
    "http://127.0.0.1:8080",
    tenant="acme",
    namespace="demo",
    request_timeout=30.0,
) as client:
    task = client.tasks.handle(saved_task_id)
    status = await task.status()
```

| Argument | Meaning |
| --- | --- |
| `base_url` | HTTP API endpoint. |
| `tenant` | Required compatibility value; match the self-hosted instance binding. |
| `namespace` | Required compatibility value; match the self-hosted instance binding. |
| `request_timeout` | Positive finite per-request deadline in seconds, at most `30.0`; defaults to `30.0`. |

Open the client with `async with`, reuse it across calls, and keep it on its owning event loop. It exposes `tasks`, `workflows`, and `completions`. Handles retain their scoped client; reconnect through a new client after the original closes.

<span id="credentials-in-development"></span>

## Credentials

The 0.8.0 client accepts `credentials_file="access/operator.json"` or
`token=` in `AsyncClient`. Choose one credential source. Files bind the credential
to the configured server URL. HTTPS is required for remote access, with loopback
HTTP supported for development. `Unauthenticated` and `Forbidden` describe
rejected operations. These options require matching 0.8.0 components.
[Configure instance access](/how-to/configure-instance-access).

## Submit a task or root workflow

`client.tasks.prepare(...)` returns a frozen `Submission`. `client.workflows.prepare(...)` returns a distinct `WorkflowSubmission`. Neither sends a request.

Both accept:

| Argument | Contract |
| --- | --- |
| `program`, `version` | Required published program ID and version. |
| `queue` | Required execution queue. |
| `data` | Required JSON application input, at most 1 MiB. |
| `idempotency_key` | Required submission identity, at most 255 UTF-8 bytes. |
| `correlation_key` | Optional application lookup key, at most 512 UTF-8 bytes. |
| `retry_policy` | Optional `RetryPolicy(max_attempts=..., retry_delay_ms=...)`. |
| `attempt_timeout_ms` | Optional integer from `60000` through `86400000`. |
| `origin_trace` | Optional `TraceContext`; explicit `None` freezes absence. |

`RetryPolicy.max_attempts` ranges from 1 through 1000 and `retry_delay_ms` from 0 through 86400000. Defaults omitted by the client are supplied by the service.

Send a prepared command with `await client.tasks.submit(submission)` or `await client.workflows.submit(submission)`. The convenience form accepts the same keyword arguments directly:

```python
workflow = await client.workflows.submit(
    program="hello-workflow",
    version="0.1.0",
    queue="demo",
    data={"name": "Ledgence"},
    idempotency_key="docs:workflow:1",
)
```

This uses the `hello-workflow@0.1.0` program published in [your first workflow](/tutorials/first-workflow). Application package versions are independent of the Ledgence client version.

The returned handle has an `id`. Construct a handle for an existing execution with `client.tasks.handle(task_id)` or `client.workflows.handle(workflow_id)`; construction makes no network request.

The client does not publish packages. Public workflow submission creates a root workflow; controllers create owned children with [the workflow context](/reference/workflow-context).

## Observe execution

| Method | Task handle | Workflow handle |
| --- | --- | --- |
| `await status()` | Compact `TaskStatus`. | Compact `WorkflowStatus`. |
| `await outcome()` | `TaskResult`; `.outcome` is `None` while pending. | `WorkflowResult`; `.outcome` is `None` while pending. |
| `await wait(timeout=60.0)` | Terminal result, including failure or cancellation as a value. | Terminal result, including failure or cancellation as a value. |
| `await result(timeout=60.0)` | Successful JSON output, or `TaskFailed` / `TaskCancelled`. | Successful JSON output, or `WorkflowFailed` / `WorkflowCancelled`. |
| `await cancel()` | Acknowledged `TaskState`; may still be `active`. | Acknowledged `WorkflowStatus`; may still be `cancelling`. |

`wait` and `result` use positive finite observation deadlines in **seconds**; there is no zero or unbounded mode. A timeout neither cancels execution nor submits it again. `WaitTimeout` retains the task, last status, and last transient read error; `WorkflowWaitTimeout` also subclasses it and retains the workflow. Observe the saved handle again to continue waiting.

A successful JSON `null` becomes Python `None`; distinguish it from an absent `.outcome` on a pending result. Failure exceptions retain their full result as `.result`.

For applications that should disconnect instead of polling, follow
[Receive results without keeping a client connected](/how-to/receive-results).
It covers server-side callback destinations, subscription acceptance and delivery.

Task success does not prove exactly-once effects or physical process cleanup. Inspect `result.outcome.quiescence` when cleanup evidence matters. Workflow `failing` and `cancelling` are nonterminal states while owned work drains.

## Find tasks

```python
page = await client.tasks.list(
    state="failed",
    queue="demo",
    correlation_key="docs:workflow",
    limit=50,
)
for item in page.items:
    print(item.task_id, item.state)
```

Filters apply inside the client's tenant and namespace. Optional fields are `state`, `queue`, `correlation_key`, `submitted_from`, `submitted_until`, and `cursor`.

`limit` defaults to 50 and ranges from 1 through 100. Submission times are Unix milliseconds: `submitted_from` is inclusive and `submitted_until` exclusive. If both are supplied, the lower bound must be smaller.

Results are ordered by `(submitted_at, task_id)` descending. Use the opaque `page.next_cursor` with the same filters for the next page; `None` ends the traversal. Pages are current observations, not a frozen snapshot. This API lists tasks, not workflow roots.

## Send an external workflow event

`workflow.prepare_event(key, *, event)` freezes the event command without sending it. `await workflow.send_event(command)` sends it. The convenience form is `await workflow.send_event(key="callback:1", event=event)`.

The CloudEvent requires `specversion="1.0"`, nonempty `id`, `source`, and `type`, `datacontenttype="application/json"`, and `data` (which may be null).

The returned `WorkflowEventReceipt` contains scope, workflow ID, key, event identity, `accepted_at`, and `already_accepted`. It confirms storage rather than processing. See [Wait for an external event](/how-to/wait-for-event) for the complete pattern.

## Reconcile uncertain mutations

The SDK does not automatically retry submission, cancellation, event, approval-decision, or completion-subscription mutations. A transport failure after dispatch can leave acceptance uncertain.

For submission, `SubmissionUncertain.submission` preserves the frozen task or workflow command. Resend it explicitly through the corresponding collection with the same endpoint and scope. For external events, `WorkflowEventUncertain.command` preserves the command to resend.

Prepare and persist commands before awaiting if they must survive caller cancellation. `asyncio.CancelledError` remains cancellation of the caller; it does not prove the server rejected the mutation.

Saved `to_dict()` values contain the wire command, not the base URL. Keep the original endpoint and scope with your application record. On restart, reconnect there and reconstruct the original preparation arguments; the [complete client contract](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md) documents each mutation's reconstruction path. Preserve any original trace context or its explicit absence when reconstructing a submission.

## Completion notifications

Both task and workflow handles expose `prepare_subscribe(destination=..., idempotency_key=...)` and `await subscribe(...)`. The destination is an operator-configured alias. Registration is separate from submission; its guarantee starts after acceptance.

Save the returned subscription ID and reconnect with `client.completions.handle(subscription_id)`. `await subscription.status()` observes delivery; after `exhausted`, `subscription.prepare_retry(expected_generation=status.generation)` prepares an explicit new delivery generation for `await subscription.retry(command)`. This retries the notification, not the task or workflow. See [the complete subscription API](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md#durable-completion-subscriptions) for status fields and uncertainty handling.

## Discover approvals across workflows

Call `await client.workflows.approval_inbox(program_id="durable-approval")`
to find pending requests in the client's scope. Use `status=None` for all
statuses. Pass `due_before` as UTC epoch milliseconds or a timezone-aware
Python `datetime` to select deadlines strictly earlier than that instant. Each summary supplies `workflow_id` and `key` for reading the
stored action. Pass the returned `next_cursor` as `cursor` with the same
filters to read another page.

## Durable approval decisions

`await workflow.approval(key)` returns a committed request; `await workflow.approvals(after_key=None, limit=10)` lists requests in key order. Pass the page's `next_cursor` as `after_key`. Inspect the typed status and exact `approval.action.arguments` before deciding; `proposed_arguments` is optional audit context.

Use `workflow.prepare_approval_decision(approval, decision_id=..., decision="approve", reviewer=...)` to freeze the decision, and save `command.to_dict()` before `await workflow.decide_approval(command)`. Use `"reject"` for rejection. `ApprovalDecisionUncertain` retains the same command for explicit reconciliation; the client does not automatically resend it. Restore a saved command with `workflow.restore_approval_decision(saved_dict)` after restarting the client. Identical decisions return the original receipt; changed bindings conflict.

A decision cannot create a pending request. The reviewer string is claimed attribution; deployment authentication must establish the actual approver. Generic external workflow events cannot grant approval. See [Require approval before a tool call](/how-to/require-approval) and the [complete client contract](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md#workflow-approvals).

**Source:** [Client contract](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md) · [Typed implementation](https://github.com/Ledgence/ledgence/tree/v0.8.0/sdk/python-client/src/ledgence/client)


<span id="operator-audit-in-development"></span>

## Operator audit

Matching client builds add `client.audit.list()` and `client.audit.approval_acceptance(approval)`. The exact helper returns a typed event or `None` after a successful empty lookup. Errors remain errors.
[Setup, queries and operating limits](/how-to/audit-operators).

<span id="retained-effect-evidence-in-development"></span>

## Retained effect evidence

Matching 0.8.0 client and server builds provide `client.effects`. Start with an
open `AsyncClient` using the instance's
scope and credentials:

```python
from ledgence.client import EffectFilter

page = await client.effects.inbox(filter=EffectFilter.UNCONFIRMED, limit=20)
if page.items:
    occurrence = page.items[0].occurrence
    evidence = await client.effects.inspect(
        workflow_id=occurrence.workflow_id,
        activation_id=occurrence.activation_id,
        approval_key=occurrence.approval_key,
    )
    print(evidence.summary.journal_state, evidence.summary.has_discrepancy)
```

`inbox` accepts `filter` (`all`, `unconfirmed`, `confirmed`, `discrepant`, `disputed`), optional
`workflow_id` and `cursor`, and `limit` from 1–100 (default 50). Metadata reads
require `metadata_read` in credential mode and also work in trusted mode.
`inspect` uses the original effect activation from that occurrence, accepts
`cursor` and `limit` from 1–4 (default 4), and returns the immutable binding and
whole claim, observation and assessment entries. It requires credential mode, `sensitive_read`
and a durable audit observation. Permission or audit errors never become empty
evidence.

Both responses are bounded to 1 MiB. Follow `next_cursor` with the same filters
or occurrence until it is `None`, even when a page contains fewer than `limit`
entries. Returned models are frozen; `EffectObservationEntry.output` returns
an independent JSON value and preserves exact large integers. No implicit
retries or background polling occur.

Evidence is retained independently of execution collection. Verified worker
sources identify who reported an outcome, not whether the provider performed
it. Legacy sources remain unattributed. Inspection does not approve, resolve or
retry an effect. See the [complete client contract](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md#retained-effect-evidence-development)
and [effect evidence contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md).


<span id="operator-effect-assessments-in-development"></span>

## Operator effect assessments

Use `client.effects.prepare_resolution(evidence, resolution_id=..., assessment=...,
reason=..., evidence_references=[...])` to freeze a reviewed effect's original
occurrence, action, revision and prior assessment. Factories are
`EffectResolutionAssessment.record_success(result)`, `.record_failure(kind, message)`
and `.close_unknown()`. Save `command.to_json()` to an application-owned file before
`await client.effects.resolve(command)`. The saved file contains sensitive action
and result data, but no credential.

A restarted client restores that exact file with
`client.effects.restore_resolution(saved_text)`. Endpoint and scope must match;
no revision or action is refreshed. `.to_dict()` is an inspection view, not the
saved format. `EffectResolutionUncertain` means acceptance may have committed;
reconcile with the same saved command and verified actor. Credentials may rotate.
No automatic retry occurs. Changed commands or actors conflict.

Resolution requires `effect_resolve`, independently of `sensitive_read` used for
inspection. Receipts are metadata only and carry `acceptance_event_id`; they do
not claim the workflow resumed. Use `await client.audit.event(receipt.acceptance_event_id)`
for the exact scoped audit event; a successful absence is `None`, while errors propagate. `current_assessment` accompanies detail even when
its entry is outside the selected page. `EffectFilter.DISPUTED` selects current
assessment conflict; `DISCREPANT` preserves historical disagreement. An operator
assessment never redispatches, confirms the journal or claims provider truth.

See the [complete assessment API and copyable file-based example](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md#operator-effect-assessments-development).
These additions require matching 0.8.0 components.

<span id="typed-provider-evidence-in-development"></span>

## Typed provider evidence

Matching 0.8.0 components add `ProviderFilter` to the existing
`client.effects.inbox(provider_filter=ProviderFilter.NEEDS_REVIEW)` call. Choices
are `ALL`, `PENDING`, `NEEDS_REVIEW`, `SUCCEEDED`, `FAILED`, `CANCELLED` and
`DISPUTED`; the ordinary effect `filter` must remain `ALL`. Omit the new argument
for the mixed inbox. Preserve the filter while following `next_cursor`.

An approval with `ApprovalProtection.PROVIDER_OPERATION` includes a frozen
`provider_binding`, covering business identity, adapter contract, target,
deduplication policy, credential reference, reconciliation policy and pinned
program. Review it together with `action.arguments`. The existing decision
preparation and restore methods retain that complete binding.

`detail.summary.provider` separates knowledge, scheduling and operator handling.
`detail.provider.current_report` is a typed report header independent of the
selected timeline page. Test `isinstance(report.report.knowledge,
ProviderSucceeded)` before reading its copied `.result`; explicit JSON null can
be a successful result. `TaskStatus.provider_probe` identifies managed checks,
whose task success does not itself establish external success.

See the
[provider recovery guide](/how-to/recover-provider-operations) and the
[complete SDK models and example](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md#typed-provider-operations-development).

## Resource administration in development

The development client adds `client.resources.put/get/list`, typed `ResourcePolicy`
and `ResourceRate`, and `client.admissions.inspect/release`. Resource metadata
requires metadata access; arguments/results require audited sensitive access;
mutations require operator `resource_control`. Preserve command identity after
a lost response. [Guide](/how-to/control-resource-capacity).

An inspected admission can carry a typed `ProviderAdmissionRecord` instead of a
generic operation result. `snapshot.provider.call_completion` distinguishes a
completed original call from uncertain completion; it does not describe business
success. Inspect the linked effect ticket for provider knowledge and recovery.
Version-2 provider approvals also expose the reviewed resource requirement.
All of these extensions require the matching development client and server.

In matching development builds, `TaskAdmission(resource="support-model")` is an
optional `client.tasks.submit` / `prepare` argument. Use
`client.task_admissions.inspect(task.id, generation=1)` for audited evidence.
[Control shared resource capacity](/how-to/control-resource-capacity#apply-the-resource-to-a-single-task).

## Queue concurrency in development

Limit outstanding attempts across worker replicas, independently of each worker’s local poll weights. Inspect occupied and uncertain slots and edit a limit from Console, CLI, HTTP, Python or MCP. [Limit a queue across workers](/how-to/limit-queue-concurrency).

### Weighted offers (development)

Use `ResourceFairGroup` and `ResourceFairSharing` from `ledgence.client` to configure `ResourcePolicy.fair_sharing`. Use `client.resources.decisions(name, before=page.next)` to read the next page of offer history when a cursor is present. [Configure queue groups and inspect offers](/how-to/share-resource-offers).
