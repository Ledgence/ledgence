# Acquisition waits

Workers use the existing `POST /v1/acquisitions` route with `wait_ms: 20000` by default. Omitting the field means immediate completion. `ledgence-worker connect --acquire-wait-ms 0` selects immediate operation, including compatibility with older servers that reject additional fields. There is still one execution concurrency parameter: N consumers and at most N managed subprocesses per worker.

An acquisition key is scope, queue, session, consumer and sequence. Wait preference is separate from that identity. A latest completed Empty stays Empty after another task arrives. Replaying an assignment returns its immutable event and descriptor with fresh authority; it never returns a cached TTL. An older sequence becomes obsolete after its successor completes.

## Waiting and finalization

`TaskStore::probe_acquisition(command, finish_empty, deadline)` owns a short atomic operation. It validates the session and cursor before selecting a candidate. If no candidate is available and finalization was not requested, it explicitly rolls back and returns Pending. A first cursor placeholder is inserted and locked before allocation to serialize concurrent requests across replicas; rollback removes it. Pending commits no cursor, attempt, event, lease or history. Rolled-back inserts can still generate WAL and dead-tuple work.

The service registers interest before probing. Each physical request validates independently, including duplicate subscribers. Waiting releases the connection, transaction, row locks and probe permit. A fixed deadline triggers a final database probe which can claim work, replay a completed cursor, commit Empty, or fail. An error, dropped connection, expired request or overload never becomes synthetic Empty. The request's original identity remains necessary for reconciliation after commit uncertainty.

The server budget is at most 30 seconds from handler entry through response encoding, including request transfer, decoding, admission, permit waits and database contention/retries. The client independently enforces its original exchange deadline through response transfer and decoding. The maximum wait is 20 seconds, limited further by the remaining exchange budget with 10 seconds reserved for finalization/response. Wakeups, reconnects, session extension and internal retries do not restart that budget. The reserve is not a guarantee that a blocked final transaction will finish.

## Queue coordination

The application service owns the coordinator and knows no SQLx types. Optional `AcquisitionWake` hints carry queue or acquisition identities, never cached results or authority. A periodic-only adapter can implement the same storage contract without providing notifications.

Initial and final probes, as well as nominated wake probes, share four service acquisition permits. Pending consumers rotate locally within their queue. Each active queue gets one nominated fallback scan per second. Queue hints open a scan epoch; actual new claims can expand its backlog scan to at most four nominated turns. A Pending result at the current epoch closes it, and older successful probes cannot reopen it. Queue rotation and FIFO permit admission allow other queues and finalizations to progress. This is local fairness, with no distributed FIFO guarantee; PostgreSQL `SKIP LOCKED` can defer a locked candidate until a later scan.

The service permits at most 4096 registered physical requests, with at most two subscribers per acquisition key. Logical keys and queue entries disappear with their last registration. Exceeding a registration bound returns Unavailable. Cancelled requests drop their registration; there is no detached acquisition loop. Resource statistics expose current and peak registrations, keys, queues, nominations and probes to a Rust host; metrics export remains separate work.

## PostgreSQL wake transport

The orchestrator enables optional PostgreSQL notifications by default. Every confirmed relevant commit first wakes matching local interests without network I/O and then queues a remote hint. Queue hints follow new submissions and transitions that queue retries. Acquisition completion hints follow newly committed Assigned and Empty, allowing another replica to discover a receipt even when the queue is empty. Replays do not republish completion hints.

The publisher uses a separate connection after the lifecycle commit. Publication failure cannot change the committed mutation's result. Do not put NOTIFY in the lifecycle transaction: PostgreSQL notification queue exhaustion can fail that transaction's commit. Hints are transient; periodic checks recover missed work and newly due retries. A final cursor probe rechecks completion even when its hint was lost; a database failure or expired exchange leaves the outcome uncertain. [PostgreSQL NOTIFY](https://www.postgresql.org/docs/18/sql-notify.html)

One fixed versioned channel carries strict identity-only payloads of at most 2 KiB. The publisher coalesces at most 1024 keys, including any in flight. Overflow/error drops optional hints and records aggregate diagnostics. The listener subscribes before rescanning active queues on startup/reconnect. Bounded setup failure leaves a durable-store-ready server available through fallback while reconnection continues. [PostgreSQL LISTEN](https://www.postgresql.org/docs/18/sql-listen.html)

The auxiliary pool has at most two additional connections per orchestrator: one listener and one publisher. It does not permanently consume the lifecycle pool's default eight connections. The listener requires a direct or session-affine endpoint; PgBouncer transaction pooling does not support LISTEN. Use `LEDGENCE_POSTGRES_NOTIFICATION_URL` to supply a separate suitable connection URL, or `LEDGENCE_POSTGRES_NOTIFICATIONS=off` for periodic-only operation. The default is `on`, using `DATABASE_URL`. [PgBouncer feature matrix](https://www.pgbouncer.org/features.html)

Ledgence bounds its own interest/hint containers. SQLx and the operating system also buffer network traffic, so these counts are not an unconditional whole-process memory bound. The listener is dedicated to subscription and receive; it does not execute acquisition or publisher queries.

## Execution and shutdown

An acquired assignment establishes identity but initially remains `AwaitingAuthority`. No program download, preparation or execution starts before a valid fresh Dispatch renewal. That renewal's request-start time and fresh remaining budget establish the first local execution bound. Time spent waiting before claim therefore does not consume the initial local execution budget. Public task timeout bounds remain unchanged; the controlled short-authority regression tests this worker behavior separately. Initial confirmation has a fixed 30-second limit, and later responses cannot revive stopped work. Sent renewal sequences and intents remain unchanged through uncertainty.

A stopping worker retains its reservation while reconciling the same acquisition sequence with zero wait. An assignment obtained during stopping is reconciled without dispatch. The orchestrator rejects new requests and wakes already accepted waits into authoritative finalization. It keeps the coordinator and local hints alive through HTTP/recovery drain, then stops notification tasks and closes their pool before closing the lifecycle pool. Cleanup observation intervals do not abandon ownership; the second explicit signal remains the force path.

See [HTTP verification](http-orchestration.md#verification), [PostgreSQL verification](postgres.md#verification) and the service/worker tests for the corresponding race, replay, deadline, notification and process scenarios. Performance depends on queue sharing, database latency, workload and deployment; no latency SLA is implied by the wait duration.
