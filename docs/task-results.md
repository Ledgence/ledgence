# Task status and results

`GET /v1/tasks/status` returns compact scheduling metadata. `GET /v1/tasks/result` returns that metadata and a nullable terminal outcome from one database snapshot. Both take exactly `tenant_id`, `namespace`, and `task_id` query fields. These are immediate reads; they never recover expired leases or change task state.

The equivalent Rust CLI commands are:

```sh
ledgence task status --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --task TASK_ID
ledgence task result --server http://127.0.0.1:8080 \
  --tenant tenant_example --namespace demo --task TASK_ID
```

A found task returns HTTP 200 even while pending or after failure/cancellation. Wrong scope or missing task returns NotFound. Backend failure and inconsistent stored observations return Unavailable. All replies retain `Request-Id` diagnostics and `Cache-Control: no-store`. The existing 30-second server exchange deadline applies.

## Status

`TaskStatus` contains `scope`, `task_id`, `run_id`, `queue`, nullable `correlation_key`, `state`, `attempt_count`, nullable `current_attempt_id` and `latest_attempt_id`, and the existing `submitted_at`, `available_at`, nullable `terminal_at` and `cancel_requested_at` timestamps in UTC milliseconds since the Unix epoch.

`queued` includes delayed retries. `active` includes preparation, execution, reporting, and cleanup. An expired lease remains active until recovery commits. `current_attempt_id` exists only while active; `latest_attempt_id` identifies the last allocated attempt and remains diagnostic after completion. Neither field implies why a queued task was cancelled.

The status endpoint reads indexed scalar metadata, without loading application input, output, CloudEvent, package descriptor, or settlement bytes. Its encoded body is limited to 16 KiB. The lookup joins the existing unique `(task_id, generation)` index at `generation = attempt_count`, without scanning task history.

## Results

`TaskResult` has two required fields: `task` containing a complete `TaskStatus`, and `outcome`. For queued/active work, `outcome` is null. Terminal variants are:

| `outcome.kind` | Additional fields |
| --- | --- |
| `succeeded` | `attempt_id`, `quiescence`, `execution_may_have_started`, `output` |
| `failed` | `attempt_id`, `quiescence`, `execution_may_have_started`, `failure` |
| `cancelled` | None |

Successful JSON null is therefore `{ "kind": "succeeded", ..., "output": null }`, distinct from pending `outcome: null`. Output may be any supported JSON value; its truthiness does not determine completion.

Failure details use a second tagged structure:

| `failure.kind` | Additional fields |
| --- | --- |
| `application` | `error` with the original program error `kind` and `message` |
| `execution` | Original worker `error`, `phase`, nullable `cleanup_error` |
| `attempt_lost` | None; no invented worker error or timeout |

Success/failure references the latest allocated attempt. Retry-eligible failures remain pending at the task level. Cancelled outcomes have no deciding attempt ID: cancelling a queued task between retries cannot attribute that cancellation to the earlier failed attempt. Use `task.latest_attempt_id` and existing attempt inspection for historical diagnostics.

## Cleanup and cancellation

The committed task state decides the logical outcome. An accepted successful report remains pending while the task is active. Separate cleanup confirmation can finalize it; expiry recovery can also finalize an accepted success with **unconfirmed** quiescence. Success confirms the accepted logical output, not physical cleanup or exactly-once external effects.

The result uses the current stored attempt's `quiescence` and `execution_may_have_started` evidence. An immutable accepted settlement may still say unconfirmed after separate cleanup confirmation. Cancellation committed before finalization wins over a retained successful report. Cancellation after an already terminal success/failure leaves that outcome unchanged.

The PostgreSQL adapter selects the task, latest attempt, and settlement in one SQL statement snapshot. Malformed JSON, missing expected attempts, and contradictory state/report combinations fail the read. It preserves original stored byte payloads rather than converting through JSONB. Result responses keep the existing 16 MiB HTTP bound and validate the stored settlement's 8 MiB bound and application value limits. Worker runtime output limits are unchanged.

Install the Python client from the repository root in your application's virtual environment:

```sh
python -m pip install ./sdk/python-client
```

Its import is `from ledgence.client import AsyncClient`. Program packages are published separately; installing the client does not start a worker or upload a program.

See the [Python client](../sdk/python-client/README.md) for asynchronous submission, bounded waiting, typed task errors, and reconciliation of uncertain mutations. Detailed invocation IDs, traces, process IDs, and settlement receipts remain available through [attempt inspection](http-orchestration.md). Use [task discovery](task-discovery.md) to find tasks by state, queue, submission time, or business correlation. No result retention policy change or stable-release compatibility guarantee is introduced.
