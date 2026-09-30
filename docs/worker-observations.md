# Worker observations

Connected workers report their current process pool for Ledgence Console. The
driver sends a snapshot when reporting starts, then approximately every five
seconds with jitter. Each exchange has a two-second deadline, and only one
publication runs at a time. An unavailable observation endpoint does not block
task acquisition, lease renewal, settlement, or worker shutdown. Shutdown requests
one final best-effort publication without waiting for the endpoint.

Add `--display-name "Billing worker"` to `ledgence worker connect` to set an
optional visible name. The name is limited to 128 UTF-8 bytes. No hostname, cache
path, environment, command arguments, or task payload is included in snapshots.
Without an explicit name, the Console uses the session identity.

## Slots, processes, and consumers

The same pool registry controls execution capacity and provides observations.
One concurrency setting defines N consumers and at most N occupied process slots
across programs. Starting, warm, executing, retiring, cleanup-pending, and unknown
ownership all occupy capacity. A consumer reservation and a process slot are
separate quantities: waiting for an assignment can occupy a consumer without
starting a process, and a warm process can remain after its consumer is released.

A slot ID is stable from zero through N minus one. A process instance ID changes
when a new process replaces the previous occupant; reusing a healthy warm process
preserves that identity. The tuple of session ID, slot ID, and process instance ID
identifies the observed process. PID is optional diagnostic information and may
be reused by the operating system. Unknown or unresolved cleanup does not turn
an occupied slot into an empty one.

The current task and attempt are linked only after the orchestrator verifies that
they belong to the reported session and configured scope. A mismatch produces a
diagnostic without those links. A process observation cannot change task outcome
or attempt authority. Connected reporting also hides metadata from another scope
when an embedded worker shares one pool across scopes, while preserving global
occupancy counts.

## Freshness and pagination

The server assigns `received_at`; `observed_at` accompanies query pages. Reports
are fresh through 15 seconds, stale after 15 seconds, and marked as having no
recent report after 60 seconds. Session expiry is reported separately. These
labels describe the available evidence and do not prove that a process is dead
or that its capacity is free. A worker without reporting remains visible with
observation unavailable and no invented slot grid.

Worker lists read compact summaries. Detail pages contain at most 100 slots
(50 by default), drawn from the latest coherent snapshot together with its
summary and sequence. The cursor advances by stable slot ID and remains valid
when a newer snapshot arrives; pages do not represent a frozen historical view.

Snapshots are limited to 2 MiB and 1024 detailed slots. The CLI's concurrency
limit is 1024. Embedders using a larger session capacity retain the true N and
publish a summary with `detail_state=unsupported_capacity`. If detail exceeds
the byte limit, the reporter publishes `detail_state=unavailable` with a coherent
summary. Neither case presents a partial array as complete telemetry.

## Storage and retention

PostgreSQL stores only the latest normalized snapshot for each existing session.
Replaying an identical sequence and normalized payload returns the original
receipt time. Reusing that sequence with different content or sending an older
sequence is rejected. A report never creates or extends a session or lease.

Observation cleanup uses the existing [bounded retention maintenance](retention.md)
and its configured batch size and maintenance interval. Expired-session cleanup
deletes the observation as one counted row before removing the remaining session
records. A session with an active attempt remains protected, including its last
observation. There is no separate telemetry TTL that bypasses that protection,
and no growing history of snapshots.
