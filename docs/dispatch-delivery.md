# Dispatch delivery

Ledgence separates transport delivery from task execution authority. An external
queue carries compact references to tasks already accepted by the task service.
Programs still receive their complete CloudEvent, and its `data` remains entirely
application-owned. Broker receipt handles and publication identities are not
program input or permission to execute.

## Worker composition

The default `DeliveryDriver::new(worker, service, config)` uses integrated service
acquisition. Custom compositions can supply an individual-ack queue adapter:

```rust,ignore
let source = Arc::new(BrokerAcquisitionSource::new(queue, service.clone())?);
let driver = DeliveryDriver::new(worker, service, config)?
    .with_acquisition_source(source);
let handle = driver.start();
```

`queue` implements the portable `AckQueue` contract. Provider SDKs, credentials,
and connection configuration remain inside adapters. A separate
`DispatchPublisher` interface supports publication. The task service must support
`claim_dispatch`; unsupported implementations reject explicitly rather than
falling back to an unrestricted queue scan.

If an empty logical queue changes from integrated to external delivery, an old
completed integrated operation still replays. A new integrated operation receives
the explicit `external_dispatch_required` rejection, which certifies no authority
or cursor mutation for that operation. A timely confirmed rejection stops the
old worker cleanly. A timeout or malformed error remains an unknown outcome.

The source takes its scope, logical queue, worker-session identity, and N directly
from the session registered by the driver. One broker client is shared across N
logical consumers. Each consumer reserves worker capacity before receiving a
record. The first implementation receives and acknowledges one record per
reservation; receive and acknowledgment batching are not yet optimized.
Publication batching is an independent concern.

## Durable handoff

```mermaid
sequenceDiagram
    participant API as Task service
    participant DB as PostgreSQL
    participant Publisher
    participant Queue as SQS / ElasticMQ
    participant Worker
    API->>DB: Commit task and delivery intent together
    Publisher->>DB: Lease due intents
    Publisher->>Queue: Publish task references in batches
    Publisher->>DB: Record confirmation; retain repair obligation
    Worker->>Queue: Receive within a reserved consumer slot
    Worker->>API: Claim exact task, generation and operation
    API->>DB: Commit attempt, cursor and claim receipt together
    API-->>Worker: Assignment or confirmed nonauthority disposition
    Worker->>Queue: Acknowledge after validating the handoff
    opt Assignment with current authority
        Worker->>API: Authorize dispatch
        Worker->>Worker: Fetch/cache program and execute in the process pool
        Worker->>API: Settle result and cleanup state
        API->>DB: Commit result and lifecycle transition together
    end
```

The acknowledgment does not wait for program completion. After handoff, the
existing Ledgence lease and expiry machinery owns recovery; broker visibility
is no longer the execution lease.

## Handoff and replay

A dispatch identifies scope, logical queue, task, and readiness generation. A
publication identity distinguishes one send operation from a deliberate repair
publication. Retries of an uncertain send preserve the publication identity.

Each slot retains its already-issued receive future across cancellation of an
acquisition call, with the original receive deadline. A new call for that slot
polls the same future instead of issuing a competing receive. This prevents a
healthy abandoned long poll from invisibly receiving tasks after worker restart.
A graceful stop drains this bounded receive, which can take the configured
acquisition exchange budget (30 seconds by default). A second shutdown signal
still forces exit. Real network loss or receive timeout remains uncertain and
uses broker redelivery and durable dispatch repair.

For each selected record, the source retains its dispatch, receipt, and immutable
claim command. A timeout or dropped future never replaces that record under the
same consumer sequence. The source reconciles the original command through the
task service and validates the echoed identity, attempt, owner, lease, and event
before acknowledging the broker record.

A new claim or the caller's exact replay can return an assignment. A claim already
handed off to another consumer returns no execution authority. Terminal,
superseded, and durably deferred records also grant no authority. These confirmed
nonauthority dispositions consume the sequence and release the unused reservation
without an empty-queue delay. An empty broker poll consumes no durable sequence.

After successful handoff, the source attempts acknowledgment. An error, missing
result, or mismatched receipt is logged as unconfirmed acknowledgment; it cannot
reverse the durable handoff. Execution may proceed, and later broker redelivery
is reconciled against task state. If the acquisition future itself is cancelled
during acknowledgment, the original command and receipt remain available for
reconciliation. Replayed assignments obtain current authority from the service;
the source does not cache a lease time-to-live.

The ordinary worker dispatch-authorization step still runs before user code.
Acknowledging the broker record does not release worker capacity. Preparation,
execution, settlement reconciliation, and required cleanup retain the same
reservation and the same global process limit N.

## Bounds and failure handling

The source retains at most N selected records, with identifier-only dispatch
bodies capped at 16 KiB and opaque receipts capped at 16 KiB. It never stores
worker ownership references or execution-session handles in its registry. The
adapter must separately bound network response allocation and SDK prefetch;
validating an already allocated response is not a network memory bound.

Malformed, oversized, unexpected-batch, or wrong-route records stop new broker
admission visibly and remain unacknowledged. This is a fail-stop policy, not a
durable quarantine implementation. Operators must correct the route or inspect
the offending record before restarting. Already uncertain claims continue their
original reconciliation; already executing work follows normal shutdown and
cleanup. Do not repeatedly restart unchanged poison input and assume it has been
handled. The SQS adapter reports positively identified receive-protocol failures
as `InvalidQueueDelivery`, including a decoded body/receipt bound, overfull batch,
or the connector's wire-body cap. The source consumes that rejection only from
its receive stage, before any task claim. Network failures and timeouts remain
uncertain and retryable; the same error coming from a claim cannot abandon its
original operation.

The driver closes the source session only after its consumers stop. Session
expiry or process loss leaves task recovery to durable orchestration state.
Removing an in-memory receipt does not delete its task or certify local cleanup.

## Adapter boundaries

`QueueLimits` declares batch and message limits, with portable batch maxima of
100. A backend may advertise lower limits. These values do not promise ordering,
scheduling, replay, native retries, transactions, or exactly-once execution.

`AckQueue` represents individual acknowledgment. A Kafka or Kinesis prefix
checkpoint cannot safely implement arbitrary receipt deletion; checkpointed
streams require a separate source model with bounded ordered handoff tracking.
Implementing these queue interfaces alone does not claim stream support or a
measured daily execution capacity.

## Executable SQS configuration

Build optional SQS support explicitly; the default executable builds have no AWS SDK dependency:

```sh
cargo build --workspace --bins --features ledgence-orchestrator/sqs,ledgence-worker/sqs --locked
```

Both `ledgence-orchestrator serve` and `ledgence-worker connect` accept `--delivery-config FILE`. They reject this option explicitly when built without `sqs`. Omitting it preserves integrated PostgreSQL acquisition. The file contains one route and one SQS Standard queue configuration, bounded to 16 KiB including whitespace. Unknown and duplicate fields are rejected. Worker tenant, namespace, and logical queue must match the file exactly before it registers a session.

The committed [local configuration](../examples/delivery-sqs-local.json) matches the HTTP quickstart's `tenant_example/demo/python-demo` scope and expects an **already created**, dedicated Standard queue named `ledgence` on a local SQS-compatible server at port 9324. Adjust its queue URL to the server's actual returned URL. Ledgence does not create queues or start a broker.

Add the same configuration file to the two HTTP quickstart commands:

```sh
./target/debug/ledgence-orchestrator serve \
  --bind 127.0.0.1:8080 --store "$demo_dir/store" \
  --delivery-config "$PWD/examples/delivery-sqs-local.json"

./target/debug/ledgence-worker connect \
  --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --queue python-demo \
  --store "$demo_dir/store" --cache "$demo_dir/cache" \
  --python "$LEDGENCE_PYTHON" \
  --runner "$PWD/sdk/python/ledgence_worker/bootstrap.py" --concurrency 2 \
  --delivery-config "$PWD/examples/delivery-sqs-local.json"
```

For AWS, supply the real Standard queue URL and region, omit `endpoint_url`, and leave `local_credentials` false or omitted. Credentials use the adapter's supported environment/profile/role chain, outside this file. Fixed local test credentials are available only with an explicit loopback endpoint. The adapter verifies queue capabilities at startup: Standard queue, zero default delivery delay, and capacity for a 16 KiB message. It does not promise ordering or exactly-once effects. The physical queue must be dedicated to this one logical route; Ledgence does not manage external queue ownership or routing tags.

`operation_timeout_ms` defaults to 5,000 and accepts whole milliseconds from 1 through 30,000. `visibility_timeout_seconds` defaults to 60 and accepts whole seconds from 30 through 43,200. Visibility protects transport handoff, independently of Ledgence execution leases. These settings do not add worker concurrency parameters.

The orchestrator validates SQS before durably activating the route. Initial activation requires an empty logical queue, and another logical route cannot reuse its destination alias. Route activation persists across restarts. **At least one orchestrator configured as the publisher for that destination must remain active.** Additional HTTP-only orchestrators may share the database. Removing all publisher configuration does not revert externally routed tasks to PostgreSQL acquisition; their durable intents remain pending until a matching publisher returns.

## Publication supervision and readiness

Each configured orchestrator owns one bounded publication loop. It leases up to the adapter's publish batch limit (10 for SQS), publishes compact dispatch references outside database transactions, and conditionally records per-record completion. A whole iteration has a 25-second budget, shorter than the PostgreSQL adapter's 30-second publication lease. Deadline expiry or uncertain send/completion leaves durable leases/intents recoverable. Confirmed publication still retains a repair obligation until a task generation is durably handed off or invalidated.

The loop validates lease route/identity and every returned publication ID. Missing, duplicate, unknown, or malformed confirmation sets cause the batch to be retried; no acknowledgment is inferred from an incomplete result. Valid mixed results preserve per-record confirmations. Full batches continue immediately with a cooperative yield. Empty or partial batches wait 100 ms to bound idle database traffic and allow small batches to accumulate. Transient failures use 1–5 second backoff. Multiple configured orchestrators can publish the same destination; durable leases arbitrate their work.

Publication failures degrade readiness while the process remains live and its durable work remains recoverable. An empty due-intent poll cannot prove the broker recovered. Readiness restores after a fully confirmed publication batch, or after an actual bounded SQS configuration probe while an outage is followed by empty polling. Recovery probes run at most once every five seconds and reuse the existing client. Attribute checks establish current queue/configuration access; they do not establish future send capacity or publication permission for every record. Readiness also requires the existing fresh expiry-recovery progress.

Unexpected publisher termination or panic drains HTTP and recovery and returns an explicit service failure. During graceful shutdown, publication and recovery continue while previously admitted HTTP operations finish. They then stop between bounded operations, retaining the current operation until its budget completes. The process closes the database pool after all three supervised tasks finish. A second signal retains its existing force-exit behavior; uncertain publication and task mutations require reconciliation after restart.
