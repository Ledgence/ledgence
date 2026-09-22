# Operational metrics

Ledgence exports optional OpenTelemetry metrics over OTLP HTTP/protobuf. Metrics work independently of trace sampling and log verbosity, with no mandatory collector or hosted account. Run the worker and orchestrator with the default `otel` feature and set:

```sh
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://127.0.0.1:4318/v1/metrics
```

Use the collector's actual reachable hostname when running containers. The full endpoint path is required. `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`, when supplied, must equal `http/protobuf`. The generic `OTEL_EXPORTER_OTLP_PROTOCOL` applies to both signals. `OTEL_SDK_DISABLED=true` disables both. The trace endpoint remains separately optional. Builds without `otel` reject either endpoint rather than silently accepting unused configuration.

The exporter uses a fixed ten-second collection interval, two-second HTTP timeout, and cumulative temporality. The existing combined three-second outer shutdown budget covers traces, metrics and exporter destruction. Interval, timeout, header, compression and other unsupported `OTEL_*` overrides fail startup. No export request occurs in an execution or database transaction. An unavailable collector cannot fail a task or grow a telemetry retry queue. The next successful cumulative export includes observations still held by the running process; crashes and abandoned shutdown can lose observations.

## Instruments and meaning

Durations are explicit-bucket histograms in seconds; their `count` is observed operation throughput. Counters are cumulative; occupancy is a cumulative nonmonotonic sum. These are process observations, not authoritative task accounting. Aggregate by service and deployment environment while preserving service instance identity for resets. Metrics do not deduplicate accepted API replays or reconstruct observations after a crash.

| Instrument | Type | Meaning and fixed `outcome` values |
| --- | --- | --- |
| `ledgence.http.request.duration` | Histogram | Completed API handler duration, including long polling; `ok`, `client_error`, `server_error`, `cancelled`. Repeated requests count separately; this is not logical task throughput. |
| `ledgence.database.operation.duration` | Histogram | PostgreSQL adapter `run`/`run_until` operation duration, including pool wait, internal retry/backoff and commit; `ok`, `failed`, `cancelled`. Migration/startup/readiness queries outside this wrapper are excluded. |
| `ledgence.database.operation.retry` | Counter | Internal retryable database errors before another attempt; no labels. |
| `ledgence.worker.preparation.duration` | Histogram | Artifact preparation, including same-digest waiting, lookup and necessary download/publication; `ok`, `failed`, `cancelled`. |
| `ledgence.worker.cache.lookup` | Counter | Completed initial artifact-cache lookup, `hit` or `miss`. Failed lookups appear in preparation failures. |
| `ledgence.worker.process.selection` | Counter | Successfully selected session, `started` or `reused`. Selection can precede cancellation before invocation. |
| `ledgence.worker.execution.duration` | Histogram | Runtime invocation duration, `ok`, `failed` (program-reported), `runtime_error`, or `cancelled` (abandoned future). Separate attempts and workflow activations are separate observations; local steps inside an invocation do not create additional runtime invocations. |
| `ledgence.worker.execution.active` | Nonmonotonic sum | Currently outstanding runtime calls. This measures active program execution, including waiting on program I/O, rather than CPU utilization. No labels. |
| `ledgence.worker.consumer.occupied` | Nonmonotonic sum | Actual semaphore ownership. Includes reservations for idle polling, settlement and retained unresolved cleanup. Use `execution.active` to distinguish running programs from reserved consumers. No labels. |
| `ledgence.task.claim.queue_age` | Histogram | Confirmed new claim time minus the task's current `available_at`; `integrated` or `external`. Excludes intentional retry delay; replays do not resample. Uses already loaded database timestamps, not an extra query. |
| `ledgence.recovery.scan.duration` | Histogram | Expiry recovery batch duration, `ok`, `failed`, `cancelled`. |
| `ledgence.recovery.expired` | Counter | Expirations reported by successful recovery batches. A partly committed failed batch can leave unobserved work; this is not a durable total. No labels. |
| `ledgence.completion.delivery.duration` | Histogram | Each attempted callback transport call, `ok` (confirmed transport reply), `retry`, or `cancelled`. Does not imply result-settlement commit or unique receiver effects. |
| `ledgence.completion.delivery.age` | Histogram | Time since subscription activation, sampled when a delivery lease is confirmed. Includes retry/backoff time, not time the execution spent running or time an inactive subscription waited. No labels. |

`cancelled` is the observation when an operation future ends without reporting an explicit outcome, including unwinding; it does not change the durable task state. A runtime's explicit cancellation/timeout error appears as `runtime_error`. HTTP `ok` means status below 400, not program success. Callback `ok` means the sender reported confirmed delivery; a subsequent storage failure can still cause another delivery.

The histogram buckets are 1, 5, 10, 25, 50, 100, 250 and 500 milliseconds; 1, 2.5, 5, 10, 30, 60, 300, 900 and 3600 seconds; and overflow. Delays above an hour remain represented in count/sum and the overflow bucket.

## Bounds and interpretation

There are fourteen instruments and at most four fixed outcome values per instrument. Each SDK stream also has an explicit cardinality cap of 32. There are no tenant, namespace, queue, program, destination, task, business, trace or worker IDs in metric labels. Those belong in logs, traces and inspection APIs. Resource attributes retain the existing bounded service name/version/instance/environment configuration. User-supplied fields on metric events are ignored by the adapter.

Metrics aggregate in memory rather than storing individual events. Export acknowledgements have the existing 64 KiB body limit. Malformed or oversized responses are failures; partial rejections and collector warnings are counted in `Telemetry::metrics_statistics()` and rate-limited diagnostics. A failed export is not retried immediately; the next fixed-interval cumulative export is a new observation. This does not promise exactly-once metric ingestion. Metric events are excluded from executable JSON logs, even under a trace log filter and in builds without OpenTelemetry.

The current metrics intentionally do not report global queue depth, oldest unclaimed work, per-queue costs, durable task totals, exact callback backlog or workflow-resume latency. Claim/delivery age describes work that was actually admitted, so an entirely stalled queue can produce no samples. Pair metrics with readiness checks, native queue monitoring and execution inspection; absence of samples is not proof of an empty queue. No per-execution database scans or writes were added for instrumentation.

## Validation and local capture

```sh
cargo test -p ledgence-adapter-otel -p ledgence-worker-core --locked
cargo build -p ledgence-adapter-otel --example otlp-capture --locked
target/debug/examples/otlp-capture 127.0.0.1:4318
```

The capture fixture accepts `/v1/traces` and `/v1/metrics`, decodes actual protobuf and writes JSON lines. Metric records have `signal: "metrics"`, cumulative temporality `2`, fixed attributes, histogram buckets or sum values, and resource identity. It is a bounded local verification receiver, not a production collector. Use port `0` for an ephemeral endpoint reported on stderr. [Telemetry adapter details](../crates/ledgence-adapter-otel/README.md) describe lifecycle and trace limits.
