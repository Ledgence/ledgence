# Durable completion notifications

External applications can subscribe to the terminal result of one task or
workflow, disconnect, and receive a compact CloudEvent later. Subscriptions,
notification bytes, retry schedules, and delivery state are durable in
PostgreSQL. The first delivery adapter uses HTTP webhooks; the core contracts
contain no HTTP client or broker types.

The guarantee starts when the subscription is accepted. Task submission and
subscription are separate commands: persist/reconcile both if your application
can stop between them. Registration works before or after completion while the
execution remains retained. Registration and terminalization use the same
execution lock, so their race cannot miss a committed terminal outcome.

## Configure a destination

Save this operator-owned configuration as `completion.json`:

```json
{
  "destinations": [
    {
      "scope": {"tenant_id": "acme", "namespace": "billing"},
      "destination": "billing-results",
      "url": "http://127.0.0.1:9000/completions"
    }
  ]
}
```

Apply the explicit migrations, then start the orchestrator using the same
program store and database as the [HTTP quickstart](http-orchestration.md):

```sh
cargo build -p ledgence-orchestrator --locked
target/debug/ledgence-orchestrator migrate
target/debug/ledgence-orchestrator serve \
  --store /absolute/path/to/program-store \
  --bind 127.0.0.1:8080 \
  --completion-config /absolute/path/to/completion.json
```

`DATABASE_URL` is required as in the quickstart. Run the program workers
separately. The application supplies a POST receiver at the configured URL;
Ledgence does not start that receiver.

Configuration accepts 1..16 destinations and at most 64 KiB of JSON, rejects
unknown/duplicate fields, and requires unique aliases within tenant/namespace.
HTTP and HTTPS URLs may include a query, but no URL user information or fragment;
the URL limit is 2048 bytes. Destination URLs and queries are not included in
request failure diagnostics or trace attributes. This first adapter has no
custom authorization headers or webhook-signature protocol; deploy it within
the existing operator-trusted scope.

Startup validates configuration, prepares the sender, and registers each
canonical immutable destination binding before admitting HTTP requests. The
same scoped alias cannot silently acquire another URL on restart. Use a new
alias for a different destination. Successful binding registrations remain
committed if later startup work fails; replaying the same configuration is safe.

At least one orchestrator with the matching completion configuration must remain
active to deliver that destination's backlog. Other replicas can serve
subscription requests against the shared database without configuring a local
sender. Removing all matching dispatchers leaves obligations retained; it does
not turn off or acknowledge subscriptions.

## Subscribe from Python

```python
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="billing"
    ) as client:
        task = await client.tasks.submit(
            program="invoice-issuer",
            version="1.0.0",
            queue="billing",
            data={"invoice_id": "INV-1042"},
            idempotency_key="issue:INV-1042",
            correlation_key="INV-1042",
        )
        subscription = await task.subscribe(
            destination="billing-results",
            idempotency_key="invoice-result-v1",
        )
        print(subscription.id)
        print(await subscription.status())
        # No wait, local background coroutine, or open subscription connection
        # remains after this client context exits.

asyncio.run(main())
```

A workflow handle supports the same `await workflow.subscribe(...)` method.
Its notification represents the workflow's final result, including owned-child
drain, rather than an intermediate activation/controller-task result. A task
subscription represents that task's logical terminal state: an attempt failure
eligible for another execution attempt produces no completion notification.

The idempotency key is scoped to tenant, namespace, and target execution.
Repeating exactly the same command returns its existing subscription; changing
the destination under that key returns conflict. A target can retain at most
16 subscriptions. A different key intentionally creates another subscription,
even if its destination matches an existing one.

Use `task.prepare_subscribe(...)` or `workflow.prepare_subscribe(...)` when the
frozen command must survive uncertainty. `CompletionSubscriptionUncertain`
contains the exact command to reconcile. Reuse that command and key; do not
create another subscription merely because a response was lost. The equivalent
explicit API is `client.completions.subscribe(...)` with a `CompletionTarget`.

## Event envelope and receiver behavior

A successful task produces a notification shaped like this:

```json
{
  "specversion": "1.0",
  "id": "evt_task_completed_task_invoice_1042",
  "source": "urn:ledgence:orchestrator",
  "type": "com.ledgence.task.completed.v1",
  "subject": "tasks/task_invoice_1042",
  "time": "2026-09-22T14:00:00.000Z",
  "ldgtenantid": "acme",
  "ldgnamespace": "billing",
  "ldgstate": "succeeded",
  "ldgtaskid": "task_invoice_1042",
  "ldgrunid": "run_invoice_1042",
  "ldgcorrelationkey": "INV-1042",
  "ldgresultref": "/v1/tasks/result?tenant_id=acme&namespace=billing&task_id=task_invoice_1042"
}
```

`ldgstate` is `succeeded`, `failed`, or `cancelled`. Workflow notifications use
`com.ledgence.workflow.completed.v1`, a `workflows/...` subject, `ldgworkflowid`,
and the workflow result endpoint. Known workflow lineage is included. Task
completion envelopes currently omit attempt IDs because terminalization clears
the active attempt pointer; the result API provides success/failure attempt
details. Correlation and trace context are optional. An accepted correlation key
containing Unicode noncharacters is percent-encoded with
`ldgcorrelationkeyencoding: "percent"`. The SDK validates and preserves this raw
envelope; decode the value explicitly only when that encoding is present:

```python
from urllib.parse import unquote

correlation_key = event.get("ldgcorrelationkey")
if event.get("ldgcorrelationkeyencoding") == "percent":
    correlation_key = unquote(correlation_key, encoding="utf-8", errors="strict")
```

`data` is absent. Application output and failure details remain in the existing
result API rather than being copied into each notification. Resolve
`ldgresultref` against your configured Ledgence API endpoint, or use
`client.tasks.handle(event["ldgtaskid"]).outcome()` / the equivalent workflow
handle. An API read can fail transiently even though the notification is valid;
the receiver should retain and retry its own processing.

Delivery uses `Content-Type: application/cloudevents+json` and the exact stored
event bytes. Headers `Ledgence-Subscription-Id`, `Ledgence-Delivery-Generation`,
and `Ledgence-Delivery-Attempt` identify the subscription and current transport
attempt. They are distinct from task execution attempts and event identity.

Receivers should durably accept the event into their own inbox, then return
`2xx`. Deduplicate by `(source, id)` before applying business effects. If one
receiver intentionally handles separate subscriptions independently, include
the subscription ID in that consumer's delivery bookkeeping. There is no
cross-execution ordering guarantee. Duplicates are possible after lost replies,
process restarts, lease expiry, or deliberate redelivery.

## Retry, inspection, and redelivery

`subscription.status()` returns the accepted command, delivery counters,
timestamps, latest compact failure reason, and the immutable event once active.
Status reads do not lease work or advance delivery. States are:

| State | Meaning |
| --- | --- |
| `waiting` | The target has not reached a logical terminal state. |
| `pending` | Notification delivery is eligible in this generation. |
| `delivering` | A dispatcher holds an expiring delivery lease. |
| `retrying` | A failed/uncertain attempt has a persisted retry time. |
| `delivered` | The endpoint acknowledged acceptance with `2xx`. |
| `exhausted` | All eight leased attempts in this generation were consumed. |

Every generation allows eight leased transport attempts, including the first.
A crash after leasing can consume an attempt even if delivery was never observed.
Expired leases become eligible for recovery; generation and lease-token checks
prevent late replies from overwriting newer work. An expired eighth lease
becomes exhausted when the dispatcher next processes it.

All non-`2xx` statuses and ambiguous exchange failures retry. Redirects are not
followed. Backoff starts at five seconds, doubles, applies deterministic jitter,
and caps at five minutes. A single numeric `Retry-After` header in seconds can
increase that delay up to five minutes. HTTP-date, duplicate, and malformed
values are ignored. A notification failure never retries the underlying task or
workflow and never changes its outcome.

After fixing the receiver, rearm an exhausted subscription explicitly:

```python
subscription = client.completions.handle(subscription_id)
observed = await subscription.status()
if observed.state == "exhausted":
    command = subscription.prepare_retry(expected_generation=observed.generation)
    updated = await subscription.retry(command)
```

Reusing the same accepted retry command returns current state without rearming
again. `CompletionRetryUncertain` carries the command to reconcile after a lost
reply. A future generation, a current non-exhausted generation, or trying to
advance beyond generation 1000 is rejected. Redelivery preserves the completion
event identity and bytes; it starts a new delivery generation, not new execution.

## Operation, bounds, and limitations

Each configured orchestrator permits at most sixteen active deliveries globally
and two per destination. These are control-plane budgets independent of worker
execution concurrency. Dispatch rotates destinations; a slow receiver can hold
only its own two slots. Waiting subscriptions live only in storage. The idle
probe interval is one second; full batches catch up without a fixed pause.
Multiple orchestrators may dispatch concurrently, so these process-local bounds
add across replicas.

Delivery leases last thirty seconds. A batch has a twenty-five-second budget;
HTTP requests use at most ten seconds and connections at most five. Storage
transactions are released before sending. The adapter reuses an HTTP/1.1 pool
with at most two idle connections per host. It acknowledges successful headers
without reading the response body. The application rejects response headers
over 16 KiB or 64 entries; the underlying HTTP parser may buffer more before
this check, within its own finite parser bounds.

Receiver outages are delivery failures, and do not make the task API unready.
Storage failures in the dispatcher degrade readiness. An unexpected dispatcher
exit triggers supervised service drain. Ordinary shutdown stops HTTP admission,
drains accepted requests, stops new delivery leasing, and retains current
bounded delivery/settlement operations before closing the database pool.

An event's trace context preserves the accepted execution origin when present.
Each delivery creates a separate HTTP transport span linked to that context;
transport trace headers never replace the persisted CloudEvent's context.
Tracing remains optional and is not the durable delivery record.

This milestone does not introduce a general event stream, subscription
cancellation/deletion, per-destination rate policies, signatures, arbitrary
headers, or archival. [Retention maintenance](retention.md) now preserves active
subscriptions, pending/retry work, and at least 90 days after the latest terminal
delivery activity; expired subscription IDs and manual retries return `NotFound`.
Exactly-once receiver business effects and production throughput are not implied
by local functional tests.


## Verification

Run the ordinary Rust/Python gates and the [PostgreSQL gate](postgres.md#verification)
before separate-process acceptance. Set `LEDGENCE_POSTGRES_URL` to an owned
disposable PostgreSQL server whose role can create databases, and set
`LEDGENCE_PYTHON` to the supported host CPython interpreter used by workers.
Install the reviewed client wheel into a new environment, then run:

```sh
cargo build --workspace --bins --locked
python3 tools/check-python-client.py \
  --venv-dir /absolute/path/to/new-client-environment \
  --evidence /absolute/path/to/client-package-evidence.json
/absolute/path/to/new-client-environment/bin/python tools/check-completions.py \
  --psql /absolute/path/to/psql \
  --binaries /absolute/path/to/ledgence/target/debug \
  --evidence /absolute/path/to/new-completion-evidence
```

The completion gate requires an already built binaries directory. It uses the
installed SDK, starts real orchestrator and worker processes plus a loopback
callback receiver, and creates/drops a unique test database. Evidence includes
process logs and a `results.json` record. It covers uncertain registration,
duplicate webhook bytes, late registration, scope isolation, exhaustion,
idempotent redelivery, sender crash/lease recovery, workflow suspension followed
by terminal delivery, and failed/cancelled task notifications. One exhaustion
scenario explicitly advances persisted retry due times to keep the test bounded;
actual attempts, leases, receiver responses, and acknowledgments are exercised.
This does not measure wall-clock backoff over an extended outage.

The PostgreSQL tests separately exercise registration/terminalization races and
stale-token settlement. HTTP adapter loopback tests cover request deadlines,
redirect behavior, retry headers, exact bytes, transport trace separation, and
connection reuse. Dispatcher tests cover global/per-destination bounds, fair
progress during receiver stalls, storage failure readiness, fatal-drain health,
and retained shutdown operations. Run `python3 tools/check-http-features.py` to
check dependency isolation for the no-feature, client, server, and completion
adapter builds. CI configuration describes checks to execute; it does not by
itself establish a completed hosted run or production capacity.
