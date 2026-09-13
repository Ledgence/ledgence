# HTTP orchestration

Ledgence provides three Rust executables: `ledgence-orchestrator` serves the durable task API and runs expiry recovery, `ledgence-worker connect` executes assignments, and `ledgence` submits and inspects tasks. PostgreSQL 18 stores orchestration state. Program packages remain in a separate filesystem or HTTPS store and are downloaded into each worker's verified cache on demand.

This version uses immediate HTTP/JSON acquisition. An empty poll completes immediately; the worker waits one second and sends the next acquisition sequence. Server-side long polling, gRPC, a package upload API, OpenTelemetry span activation/export, and retention deletion remain later work. The API is versioned under `/v1` but has no stable-release compatibility promise yet.

## Run a task

Build from the repository root with the pinned Rust toolchain and CPython 3.11 or newer. The example uses CPython 3.12 and a local program store shared by the two processes. Supply a PostgreSQL 18 database using `DATABASE_URL`; `migrate` applies the schema explicitly, while `serve` only verifies it.

```sh
export LEDGENCE_PYTHON="$(command -v python3.12)"
export DATABASE_URL='postgres://USER:PASSWORD@127.0.0.1:5432/ledgence'
cargo build --workspace --bins --locked

demo_dir="$(mktemp -d)"
./target/debug/ledgence-worker example \
  --directory "$demo_dir/example" --python "$LEDGENCE_PYTHON"
./target/debug/ledgence-worker publish \
  --source "$demo_dir/example/program" --store "$demo_dir/store"
./target/debug/ledgence-orchestrator migrate
./target/debug/ledgence-orchestrator serve \
  --bind 127.0.0.1:8080 --store "$demo_dir/store"
```

Keep the server running. In another terminal, use the same absolute `demo_dir` path and interpreter, then start a worker:

```sh
./target/debug/ledgence-worker connect \
  --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --queue python-demo \
  --store "$demo_dir/store" --cache "$demo_dir/cache" \
  --python "$LEDGENCE_PYTHON" \
  --runner "$PWD/sdk/python/ledgence_worker/bootstrap.py" --concurrency 2
```

In a third terminal, save this complete submission as `submit.json`:

```json
{
  "idempotency_key": "hello:example:1",
  "input": {
    "tenant_id": "tenant_example",
    "namespace": "demo",
    "queue": "python-demo",
    "program": {"id": "hello", "version": "1.0.0"},
    "correlation_key": "example:1",
    "data": {"message": "Hello"}
  }
}
```

```sh
./target/debug/ledgence task submit \
  --server http://127.0.0.1:8080 --file submit.json
./target/debug/ledgence task inspect \
  --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --task TASK_ID
./target/debug/ledgence task history \
  --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --task TASK_ID --after 0
./target/debug/ledgence task attempt \
  --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --task TASK_ID --attempt ATTEMPT_ID
./target/debug/ledgence task cancel \
  --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --task TASK_ID
```

Replace `TASK_ID` with the submitted snapshot's `task_id`. Obtain an attempt ID from `current_attempt_id` while active, or a `claimed` history record after completion. Task inspection returns scheduling state and input; attempt inspection returns the accepted result, when present. History returns up to 100 records; request the next page using the last record's `sequence`. An empty page only means no later records exist at that moment.

`ledgence` writes one JSON result to stdout and request diagnostics to stderr. Exit `0` means the operation was accepted, `2` means input/usage rejection, and `1` means a service or transport failure. Successful submission does not mean successful execution. Each command makes one bounded exchange. If submission has an uncertain outcome, resubmit the same file/key; do not generate a replacement key. Matching replays preserve the first accepted input, descriptor, and origin context. Changed normalized input under the same scoped key conflicts. The [delivery contract](delivery-contract.md) defines normalization and deduplication scope.

The submission is a command, not the invocation event. Python receives the generated [CloudEvent](events.md), with the exact logical application `data` value and platform identifiers in the envelope. Programs must make their external effects idempotent across attempts.

Use the existing [package publication layout](program-packages.md) for remote stores. The orchestrator resolves descriptors at submission; workers receive that bound descriptor in each assignment and need the corresponding immutable blob on a cache miss. They may use different URLs exposing the same published contents. The listener defaults to `127.0.0.1:8080` and serves HTTP/1.1; an external reverse proxy can terminate HTTPS. HTTP clients validate HTTPS certificates. No vendor account is required.

## Routes and representation

Requests and responses use UTF-8 `application/json`; an optional UTF-8 charset parameter is accepted. Bodies are uncompressed. Each successful service operation returns `200` with its portable reply. API responses carry `Cache-Control: no-store` and a diagnostic `Request-Id`.

| Method and path | Input | Reply |
| --- | --- | --- |
| `POST /v1/tasks` | `SubmitCommand` | `TaskSnapshot` |
| `GET /v1/tasks/inspect` | `tenant_id`, `namespace`, `task_id` query values | `TaskSnapshot` |
| `GET /v1/attempts/inspect` | Same scope/task values plus `attempt_id` | `AttemptSnapshot` |
| `GET /v1/tasks/history` | Scope/task values; optional `after_sequence`, default `0` | Up to 100 `RecordedHistoryEvent` values |
| `POST /v1/tasks/cancel` | `{"scope": Scope, "task_id": string}` | `TaskState` JSON string |
| `POST /v1/worker-sessions` | `{"scope": Scope, "queue": string, "concurrency": u32}` | `WorkerSession` |
| `POST /v1/worker-sessions/extend` | `{"worker_session_id": string}` | `WorkerSession` |
| `POST /v1/acquisitions` | `AcquireCommand` | `AcquireReply` |
| `POST /v1/renewals` | `RenewCommand` | `Authority` |
| `POST /v1/settlements` | `SettleCommand` | `SettleReply` |
| `POST /v1/quiescence-confirmations` | `LeaseOwner` | `TaskState` JSON string |

`Scope` is `{"tenant_id": string, "namespace": string}`. Full command/reply definitions live in `ledgence-orchestration-api`. Fixed inspection paths keep identifiers out of path normalization; query encoding preserves valid Unicode, spaces, slashes, plus and percent signs, including identifiers `.` and `..`. Duplicate/unknown query fields, malformed encoding, and bodies on GET requests are rejected. Submission keys stay in JSON; there is no competing `Idempotency-Key` header mapping. There is no task-list/search endpoint.

`AcquireReply::Empty` is `200 {"disposition":"empty","sequence":1}`. An acquisition's `ownership_lost` disposition also returns `200`, with its sequence and assignment reference. Neither is a request-level error or a synthetic `204` response.

| Status | Domain error codes |
| --- | --- |
| `400` | `invalid_input` |
| `404` | `not_found`, `unknown_session` |
| `409` | `conflict`, `ownership_lost`, `obsolete_operation`, `out_of_order`, `busy` |
| `410` | `session_expired` |
| `413` | `invalid_input` for an oversized request |
| `415` | `invalid_input` for unsupported media type/encoding |
| `503` | `unavailable` |

Error JSON uses the existing tagged representation, such as `{"code":"conflict"}` or `{"code":"invalid_input","message":"..."}`. Unknown routes/methods instead return transport codes `route_not_found`/`method_not_allowed` with `404`/`405`. The client accepts only known domain codes paired with their allowed HTTP status. Proxy HTML, redirects, incomplete/oversized bodies, wrong media types, malformed JSON, unknown codes, mismatched status/code, and socket/deadline failures become `Unavailable`. That uncertainty never proves a task is absent, a transaction rolled back, or ownership was lost.

## Limits and JSON fidelity

| Value | Limit |
| --- | --- |
| Submission and other control request bodies | 2 MiB raw JSON |
| Settlement request body | 8 MiB raw JSON |
| Submitted application `data` | 1 MiB compact JSON; 64 nested arrays/objects |
| Successful response body | 16 MiB |
| Error response consumed by client | 64 KiB |
| One HTTP exchange | At most 30 seconds |

Streaming reads enforce limits even without `Content-Length`. Decoding starts from original bytes, rejects duplicate keys at every depth and out-of-range integer tokens, and preserves signed/unsigned 64-bit integers, finite binary64 values, negative floating zero, and escaped U+0000. Unknown-field behavior follows each portable type; not every response type rejects additional fields. See [event numeric semantics](events.md).

The 16 MiB response cap accommodates the current service's largest response, `AttemptSnapshot`: one accepted command of at most 8 MiB, one generated event containing at most 1 MiB of data, and bounded metadata. Outside those two payloads, the attempt contains fewer than 64 variable strings, each at most 512 UTF-8 bytes (identifiers are at most 128; trace state at most 512). Valid metadata excludes control characters; JSON escaping therefore adds at most a factor of two. Allowing 64 KiB for these strings and another 64 KiB for fixed keys, punctuation, numeric fields, and fixed event text gives a conservative total below **9 MiB + 128 KiB**. The accepted command already includes its report context and processing trace; those are not counted again. A descriptor's two program names are each at most 128 ASCII bytes and its digest is exactly 71 bytes.

`TaskSnapshot` has at most a 2 MiB validated input plus the same generous 128 KiB metadata allowance. An assignment has at most a 1 MiB event payload plus that allowance. A history record contains two bounded identifiers and fixed scalar metadata; even a conservative 2 KiB per record keeps its 100-record page below 200 KiB. Session, authority, and settlement replies are smaller. These bounds describe records generated through the current lifecycle/service contract, not arbitrary extensions supplied by a custom `TaskService`. Custom services must honor the HTTP response cap. Changing response fields, envelope extensions, or domain limits requires reviewing the derivation and fixtures. Oversized responses fail; reports are never silently truncated.

Client encode/decode and server decode/encode run in bounded blocking jobs so large JSON values do not block async lease timers. The four internal JSON-job permits per client/server instance bound CPU work; they do not change the worker's single execution concurrency parameter. A job retains its permit until completion even if its HTTP waiter times out. The client budget includes queueing, encoding, network wait, body reading, and decoding, with an elapsed-time check before returning decoded authority.

## Identity, retries, and observability

`HttpTaskService` pools connections and makes one exchange per call, with redirects and automatic retries disabled. The delivery driver owns acquisition/renewal/settlement retries. It preserves the complete durable operation identity and does not advance a consumer after an uncertain acquisition. A completed Empty is replayed as Empty. Renewal replay cannot extend authority twice; accepted settlement receipts remain replayable after expiry. Cleanup confirmation is a separate operation and never rewrites an accepted report. Session opening has no replay key: losing its successful response can leave an unused session until expiry.

The worker retains request-start monotonic accounting: connection, transfer, and decoding time consume authority. Assignment delivery still requires a dispatch renewal before package preparation/execution. Late responses cannot revive locally stopped work. One `--concurrency N` (default `4`, range `1–1024`) controls N consumers and at most N managed subprocesses globally within that worker. Multiple workers each have their own N and service session; N is not a cluster-wide limit.

Every API exchange gets a new `Request-Id`, including retries of the same durable command. Request logs include route, method, status, elapsed time and error code; operation logs add available scope and durable command identities. Application input/output is not copied into these request logs. Missing request-ID headers do not invalidate otherwise accepted results. Health routes are separate executable endpoints and do not provide an API request ID.

Submission `origin_trace`, invocation event trace context, and settlement `processing_trace` retain separate meanings. HTTP tracing headers do not overwrite those fields; this adapter does not currently propagate or activate exchange trace headers. The connected driver does not synthesize a processing context. There is no OpenTelemetry exporter or metrics backend yet. Use durable task/run/attempt IDs and the optional submission `correlation_key` for application correlation; trace span IDs and per-exchange request IDs are not business parent IDs.

## Recovery, readiness, and shutdown

Serving verifies that every expected SQLx migration is present, clean and checksum-matched and rejects missing, dirty, changed, or newer migration sets. It also checks database connectivity before opening the listener. It does not migrate automatically. See [PostgreSQL setup](postgres.md).

Each orchestrator runs one supervised expiry loop. It scans up to 100 shortlisted tasks per batch, with a one-second normal cadence. A full shortlist permits ten additional catch-up batches, yielding between them. Skipped locks and zero expired tasks do not prove exhaustion. Unavailable scans retry after 1, 2, 4, then at most 5 seconds; successful batches reset backoff. Each call has a 30-second budget. Already committed transitions survive a later batch error. PostgreSQL arbitrates concurrent scanners; no in-memory leader is required.

`GET /health/live` returns `200` with `{"status":"ok","reasons":[]}` while the listener is serving. `GET /health/ready` returns `200` or `503`, with `status`, bounded `reasons`, and `recovery_last_success_age_ms` (null before a successful scan). Readiness requires startup checks and a successful recovery pass, and becomes false on observed recovery failure, shutdown, or progress older than 40 seconds. That freshness budget is 30 seconds for a scan + 5 seconds maximum backoff + 5 seconds scheduling allowance. It is an operational readiness signal, not a real-time execution guarantee. An unexpected scanner exit or panic initiates server drain and a nonzero process result.

The first SIGINT/SIGTERM makes the orchestrator unready, closes new request admission, and drains accepted HTTP operations. Recovery continues during that drain, then stops between bounded batches before the database pool closes. A 35-second observation interval logs pending shutdown while retaining the same operations; it is not a forced-exit deadline. A second explicit signal permits a nonzero forced exit. Individual HTTP deadlines or disconnects still leave transaction outcomes uncertain; clients reconcile their durable operation identities.

The connected worker's first signal stops acquisition and requests retained delivery/process cleanup. Five-second waits report pending without discarding the driver. Reconciliation can remain pending during an unavailable service. A second signal forces a nonzero exit with unresolved work. Normal signal-driven worker shutdown also exits nonzero to report interruption, and emits a final JSON delivery-status record. `settled_attempts` counts accepted reports, including failures, not successful business outcomes. Retrieve durable results with attempt inspection.

Unfinished preparation or runtime cleanup also initiates a worker drain: it retains the pending operation and report until reconciliation and local cleanup finish, then exits. A process supervisor can start a replacement with a fresh session. The draining worker does not resume acquisition after releasing those resources. A preparation result that arrives after its deadline cannot start a handler or publish a late cache result.

Orchestrator logs use a bounded nonblocking writer with 64 queued records, at most 256 KiB each and a five-second delivery deadline. Lost records or output failure make the final process result nonzero. A paused pipe reader does not block recovery or signals. Writer ownership and signal handling persist through output drain; an uninterruptible filesystem write can still require the explicit second-signal force path.

A worker crash loses its in-memory cursor and pending report. A replacement opens a fresh session, and expiry plus the task retry policy recovers unfinished attempts. This is at-least-once task execution; leases, event IDs, and receipts do not guarantee exactly-once external effects. The initial runtime remains for operator-trusted programs under the worker's OS permissions.

## Verification

Run the ordinary workspace gates and the [PostgreSQL gate](postgres.md#verification), then the separate-process HTTP acceptance gate against a disposable PostgreSQL server whose role can create databases:

```sh
python3 tools/check-http-features.py
python3 tools/check-http.py --psql /path/to/psql \
  --evidence /absolute/path/to/new-evidence-directory
```

`LEDGENCE_POSTGRES_URL` supplies the disposable server connection and `LEDGENCE_PYTHON` selects Python. The HTTP gate creates/drops its own unique database and starts separate orchestrator, worker, and CLI binaries, a program server, and a fault proxy. It builds binaries unless `--binaries DIRECTORY` is supplied. Use `--scenario NAME` to select a case; omit it for the full gate. The proxy drops responses after consuming upstream committed replies, allowing tests to check real socket uncertainty and durable replay. Runtime fault tests and controlled cleanup tests remain separate from deployment tests; they are not substitutes for one another.

The feature gate checks the HTTP adapter with no features, client only, and server only, and verifies that client dependencies do not pull in Axum and server dependencies do not pull in reqwest or SQLx. The Linux PostgreSQL CI job runs network acceptance after the database gate. CI configuration describes required checks; it is not evidence of a completed hosted run. Long-poll wakeups and database failover remain outside this version's validation scope.
