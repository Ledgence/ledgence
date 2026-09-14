# Ledgence Python client

An MIT-licensed, asynchronous client for submitting and observing existing
Ledgence programs. Python 3.11–3.14 is the configured test matrix. The package is
`ledgence-client`; its public import is `ledgence.client`. This is an initial
version without a stable-release compatibility promise or a published-registry
claim. Install the locally built wheel; building and testing instructions follow.

```python
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient(
        "http://localhost:8080", tenant="acme", namespace="billing"
    ) as client:
        task = await client.tasks.submit(
            program="invoice-issuer", version="1.0.0", queue="billing",
            data={"invoice_id": "INV-1042"},
            idempotency_key="issue:INV-1042",
            correlation_key="INV-1042",
        )
        print(task.id)
        output = await task.result(timeout=60)
        print(output)

asyncio.run(main())
```

The caller owns its event loop. Keep one client open across calls and use it on
that loop. Programs remain ordinary synchronous Python handlers receiving the
complete CloudEvent, executed by the Rust worker. The client neither uploads
packages nor imports handlers. It is separate from the dependency-free
`ledgence_worker` runtime helper and does not package that helper or CPython.

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
status responses at 16 KiB, and other responses at the existing 16 MiB bound.
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

## Local verification

The repository's `tools/check-python-client.py` builds and tests the
installed wheel in isolation, using the reviewed dependency inventory. See its
`--help` for supported environments. Source tests also run with:

```sh
PYTHONPATH=sdk/python-client/src python -m unittest discover -s sdk/python-client/tests -v
```

The selected interpreter must already have the pinned dependencies. Optional
trace tests run when the reviewed OTel API is installed; no exporter is used.
`LEDGENCE_JSON_FIXTURES` can select the shared Rust/Python JSON fixture file when
tests are copied outside the checkout. Dependency versions, wheel/source hashes,
licenses and notices are retained under `third_party`; normal wheel installation
pins the reviewed runtime closure. The gate verifies those pins and distributed
legal files. No dependencies are downloaded during program execution.
