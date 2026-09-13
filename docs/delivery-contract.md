# Delivery contract

This feature implements portable orchestration types, deterministic state transitions, and worker capacity reservations. PostgreSQL and HTTPS/JSON long polling are the selected direction for the next adapters. There is no database adapter, server, production poller, automatic remote retry loop, or OTLP exporter in this feature. The local `run` command still consumes a fixture.

## Boundaries

`ledgence-orchestration-api` defines submission, acquisition, lease, report, receipt, inspection, and service contracts. It depends on worker-api's portable CloudEvent, program descriptor, and execution report types. `ledgence-orchestration-core` decides transitions over supplied records; it performs no I/O, ID allocation, or clock reads. `TaskService` is implemented by the Rust application service over atomic storage ports. The [PostgreSQL adapter](postgres.md) supplies durable transactions; HTTP worker transport remains future work.

Each core transition returns proposed task/attempt updates, history, and a reply. **A returned transition is not a durable acknowledgement.** A store must lock the relevant records, obtain fresh authoritative time, run the transition, commit every update atomically, and only then expose the reply. Input snapshots remain unchanged even when validation fails.

The PostgreSQL adapter enforces uniqueness for scoped submission keys, task/run IDs, attempt IDs/numbers, event source/ID, worker session IDs, and consumer cursor keys. It must lock the task and consumer cursor consistently during claim, and serialize renewal, cancellation, settlement, and expiry through the same task row. `SKIP LOCKED` is suitable for competing claimers but does not promise strict FIFO. Lease time is sampled after lock acquisition; PostgreSQL transaction-start `now()` is insufficient after a long lock wait. [PostgreSQL row locking](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE), [time functions](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT).

## Submission and JSON

The public submission payload contains client-owned settings and data; the service supplies execution identities. HTTP mapping is future adapter work. A valid submission is:

```json
{
  "tenant_id": "tenant_acme",
  "namespace": "billing",
  "queue": "python-billing",
  "program": {"id": "invoice-issuer", "version": "1.2.0"},
  "correlation_key": "invoice:INV-1042",
  "data": {"invoice_id": "INV-1042", "amount_minor": 12500}
}
```

`SubmitCommand` combines this input with a required submission idempotency key and an optional origin trace context. Store lookup/replay happens before external program resolution. On first acceptance, resolve the immutable descriptor and bind it to the task; concurrent submission uniqueness chooses one accepted binding. Retrying a submission does not replace its input, descriptor, or origin context. Reusing its key with different normalized input is a conflict.

`data` remains wholly application-owned and may be any supported JSON value. Invocation extensions are generated in the CloudEvent envelope. `correlation_key` is optional, non-unique business lookup metadata scoped by tenant/namespace. It is not automatically copied into an event extension and does not deduplicate business effects.

Use `SubmitTask::decode`, `SubmitCommand::decode`, `SettleCommand::decode`, or `decode_unique_json` on original request bytes. Plain deserialization of an already constructed `Value` cannot detect duplicate keys or recover rounded numbers. New orchestration requests reject duplicate keys at every depth, including escaped aliases inside data. The existing local event interface is unchanged. Unknown submission and outer control fields are rejected by their schemas.

Normalization sorts object keys recursively, preserves arrays and strings, and applies explicit defaults. Integer `1` differs from float `1.0`; equivalent finite binary64 spellings such as `1e0` and `1.0` match. Positive and negative floating zero differ. The existing parser treats `-0` as floating negative zero, matching `-0.0`. Missing/null optional correlation normalizes to absence. This comparison encoding is not RFC 8785 and does not narrow the existing i64/u64/binary64 numeric contract.

Store authoritative validated serialized events and results as bytes; index platform fields separately. PostgreSQL `jsonb` rejects escaped U+0000 and rewrites numeric formatting, so it is unsuitable as the automatic authoritative representation for this payload contract. [PostgreSQL JSON types](https://www.postgresql.org/docs/current/datatype-json.html).

## Task, attempt, and identity

Task states are `queued`, `active`, `succeeded`, `failed`, and `cancelled`. Delayed retries are queued with `available_at`. Attempt states are `active`, `succeeded`, `failed`, `cancelled`, and `lost`. `active` includes package preparation and startup. Without an accepted report, expired ownership produces a lost attempt: it does not prove no external effect happened.

A retry preserves run ID, task ID, input, and descriptor/digest. It allocates a new attempt ID/number, lease identity/generation, and invocation CloudEvent ID. Redelivery of one assignment keeps its original event, including its trace context. Origin and worker processing trace contexts are distinct; an accepted report can carry the latter without rewriting the event. The trace field contracts exist here; tracing activation/export is future work. [CloudEvents tracing extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md).

Request IDs belong to individual transport exchanges. Submission keys identify repeatable submissions; acquisition and renewal use ordered operation sequences. Settlement has its own immutable operation ID scoped to the attempt. No generic `parent_id` combines task ancestry, event causation, and span parentage.

## Sessions and bounded acquisition

The service allocates non-reused worker session IDs. A session records scope, queue, and the worker's configured N. Opening a session does not create another independent concurrency parameter. Unknown/expired sessions cannot acquire or renew; registration never revives a caller-supplied old ID. A live session can be extended; each attempt still requires its own lease renewal.

Each consumer index is below N. Its durable cursor keeps only the latest completed acquisition sequence, starting at 1, and either Empty or a task/attempt reference:

- Same latest sequence and command returns the same disposition. Empty remains Empty even if new work has arrived.
- A replayed assignment retains identity but returns freshly sampled remaining authority. Expired or completed ownership returns `OwnershipLost`.
- Older sequences are obsolete, sequence gaps are rejected, and changed scope/queue/consumer identity conflicts.
- The next sequence can close only after Empty or after the previous attempt no longer owns authority. It cannot create extra work while the previous claim is live.
- Waiting for work is adapter behavior. A wait is not a committed Empty. Each completed poll atomically updates its cursor and any claimed task/attempt.

Cursors for a live session must not be deleted/reset. A cursor's referenced task/attempt must remain resolvable: retain their minimal tombstones after payload retention expires until the cursor advances or its session expires. Otherwise an idle consumer could be stranded when it next tries to advance. Once a session expires, its cursor records may be removed after outstanding ownership is reconciled; an unknown session still rejects old commands. This avoids an indefinitely growing receipt history for idle polls. Renewals similarly retain one ordered command, not a history row per heartbeat.

## Lease and execution deadlines

Lease owners bind scope, task, attempt, generation, lease ID, worker session, and consumer. Ownership requires time strictly before expiry; equality is expired even if the scanner has not run. Renewal accepts the next sequence once. Same-sequence replay does not extend twice, older sequences cannot refresh authority, and a sequence reused with a changed intent conflicts.

A pre-dispatch renewal records dispatch intent and `execution_may_have_started=true` before returning permission. This is conservative: the worker may disappear after permission without invoking Python. Worker phase observations never turn missing evidence into proof that execution did not start. The worker still dispatches at most once through its single-use reservation.

`Authority` carries both remaining lease time and remaining execution time. Cleanup/reporting can retain a lease after the user-work deadline. `LeaseTracker` derives conservative local permission from the monotonic **request-start** time and both durations, with a safety margin; database wall-clock timestamps are diagnostic, not directly converted to local deadlines. The fixed execution deadline cannot be extended by later replies. Old/replayed responses cannot refresh or restore permission; cancellation, ownership loss, and local expiry stop it permanently.

The tracker controls starting/continuing user work. Stopping work does not stop cleanup, lease communication, or settlement reconciliation. Those continue until resolved, bounded by the server's remaining ownership horizon. Clock discipline and stopping after a discontinuity/suspension are deployment obligations. A lease fences Ledgence state, not external systems.

## Results, cleanup, retries, and cancellation

An accepted settlement stores the immutable command and receipt with its lifecycle changes. A matching accepted receipt is replayed **before** checking current lease liveness, even when a later attempt is already active. Changed report content or operation identity conflicts; new stale reports cannot mutate scheduling. The PostgreSQL adapter emits bounded attributable operation logs for rejected late reports without replacing the accepted outcome.

`Quiescence::Confirmed` means the invocation and its required cleanup have finished; a healthy warm subprocess may remain. It cannot be inferred solely from a missing `cleanup_error`. A confirmed report finalizes immediately. An unconfirmed report is durably recorded while the task/attempt remains active, and the reply exposes that state. `confirm_quiescence` is a separate idempotent transition that finalizes from the stored report without changing it. If ownership expires first, an already accepted outcome still determines success/failure; an accepted success is not retried merely because stop confirmation is unavailable. Cleanup remains explicitly unconfirmed. Without an accepted report, expiry records lost ownership.

Application-reported failures are terminal by default. Invalid input, missing packages, integrity, incompatibility, and protocol failures are terminal. Worker unavailability, I/O, capacity, runtime interruption, timeout, and local cancellation are retryable within the task's bounded policy. The original typed failure and cleanup error remain observable; retry classification is not based on text matching. A server cancellation overrides retries. External effects still require application idempotency.

An already terminal task stays terminal. Cancellation of initial queued work or a delayed retry is immediately terminal. Cancellation of active work persists intent, prevents further claims/dispatch/retries, and caps the cleanup authority window. It becomes terminal after confirmed cleanup or expiry. A later observed success remains in the report but does not change the cancelled scheduling outcome. At expiry with unconfirmed stop, the attempt remains `lost`; this does not claim the process stopped. Duplicate cancellation and expiry create no additional transition.

## Worker reservation

`Worker::reserve_consumer(control).await` obtains a `ConsumerReservation` from the same semaphore as ordinary execution. Reserve before remote acquisition. `reservation.execute(request, control).await` is single-use. Keep the reservation through unresolved settlement, then release it. Dropping it does not certify remote settlement.

Retained invocation supervisors share that same permit. Caller cancellation, an early fetch timeout, or release after a report cannot free capacity while the underlying local operation is still owned. Quarantined cleanup and unrecoverable starts retain their process slots and relevant consumer ownership. Healthy warm processes keep their globally counted process slot without retaining a completed consumer's reservation.

N is per worker instance, across all its programs and versions. `is_cancellation_requested()` lets a future delivery driver observe shutdown while it owns an acquisition or report. Shutdown retries available quarantined cleanup while external reservations remain held, so those owners can observe local completion and reconcile their reports. Shutdown remains incomplete until external owners resolve and release their reservations. This feature supplies that ownership primitive, not a production delivery driver.

`is_quiescent()` observes whether the reservation still has supervised local work or cleanup outstanding. A successful returned report can coexist with a healthy warm process and a quiescent reservation; an early fetch-timeout report can coexist with a non-quiescent reservation until its retained fetch finishes. The observation does not confirm remote acceptance or absence of arbitrary application side effects.

The reservation keeps its execution cancellation control attached while supervised work or required cleanup remains. Once that local lifetime is complete, releasing the reservation does not cancel the finished execution's control, including when other invocations share it. Early reports and unconfirmed cleanup do not end that lifetime.

## Initial limits and retention contract

| Setting | Initial value |
| --- | --- |
| Application submission data | 1 MiB compact encoded JSON; 64 nested containers |
| Full submission/control request | 2 MiB incoming/normalized submission; settlement has its own limit |
| Default subprocess protocol frame | 2 MiB in either direction, including the complete envelope and newline |
| Settlement command including output/context | 8 MiB; output retains the 64-container value limit |
| Program attempts | 3 total by default; configurable 1–1,000 |
| Fixed retry delay | 5 seconds by default; configurable 0–24 hours |
| Attempt execution budget including preparation/startup | 5 minutes by default; configurable 1 minute–24 hours |
| Lease / renewal cadence / safety margin | 60 seconds / 15 seconds / 5 seconds |
| Cleanup/report authority after execution deadline or cancellation | Up to 30 seconds; existing shorter lease remains authoritative until renewed |
| Long-poll wait / request deadline | 20 seconds / 30 seconds; future proxy configuration must accommodate them |
| Session validity from creation/extension | 24 hours; expired sessions cannot be extended |
| Task history, accepted reports/receipts, submission deduplication | Entire active life plus 90 days after terminal task state |

The subprocess frame budget includes generated CloudEvent metadata and the protocol wrapper in addition to application data. The default accommodates the full submission data limit with the largest supported generated identifiers and trace context. Local applications may override frame limits; smaller frames can reject otherwise valid submissions, and larger output frames require checking the settlement/report budget. A future delivery adapter must check its configured runtime limits against these contracts.

The minimum execution budget exceeds the initial control request deadline and safety margin. Long-poll request latency is charged conservatively, so a fresh authority exchange may still be needed before starting work. These initial values establish bounded behavior, not throughput promises. Execution duration, leases, cleanup, and byte limits are distinct from the sole concurrency parameter. Existing local-worker timeouts retain their previous behavior.

Retention cleanup is not implemented in this feature. The future store must preserve accepted settlement receipts for the entire attempt-history lifetime and keep submission identity while the task is active. At the documented 90-day terminal expiry, inspection may return NotFound and a reused submission key may create a new task. Such submission deduplication is not permanent business idempotency. Per-consumer cursor compaction cannot turn an old sequence into a new assignment, even after session record removal.

## Validation and next adapter

Deterministic tests exercise duplicate/obsolete sequences, loss of permission, receipt replay during retries, cancellation ordering, cleanup progression, payload preservation, and transition errors without input mutation. Worker tests prove capacity retention with gated preparation/cleanup and caller cancellation.

These tests establish in-process contracts. The PostgreSQL feature must additionally prove concurrent claimers, row-lock waits crossing expiry, commit/reply loss, restart recovery, schema migrations, retained outcomes, and missed wakeups against PostgreSQL itself. It must not substitute SQLite or in-memory tests for those guarantees.
