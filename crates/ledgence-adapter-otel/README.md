# Optional telemetry adapter

This crate provides a W3C context bridge, JSON log correlation, and bounded OTLP HTTP/protobuf trace and operational metrics export. It does not install a global provider or subscriber. Executable composition owns enablement and shutdown; execution contracts expose only `ledgence_worker_api::TraceContext` and `TraceBridge`.

Construct `Telemetry::from_env(service_name, version)` before starting an async runtime. Install `telemetry.subscriber(existing_bounded_log_writer, log_filter)` and inject `telemetry.bridge()` into adapters and services. The `RUST_LOG` filter applies to logs independently from exported Ledgence spans. Diagnostic spans with target `ledgence::context` retain logging context without generating extra OTel operations. Their children should set explicit phase parents before entering or reading the span. `otel.kind` and `otel.status_code` fields follow the pinned tracing bridge's case-insensitive names.

Set parent and link context before the first span entry, child creation, or context read, which can materialize the SDK span and sampling decision. The bridge reads actual sampled or unsampled span contexts. Disabled mode returns no context and creates no synthetic IDs. JSON log `trace_id`/`span_id` identify the active span, separately from copied event-origin fields. Explicit `parent: None` records, including Python log snapshots, receive no ambient enrichment.

Supported environment settings:

| Setting | Behavior |
| --- | --- |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Independently enables metrics; full absolute HTTP(S) URL including `/v1/metrics`. Ten-second cumulative collection. |
| `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` | If supplied, must be `http/protobuf`. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Enables tracing; absolute HTTP(S) URL, including the traces path. No default collector. |
| `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | If supplied, must be `http/protobuf`. |
| `OTEL_SERVICE_NAME` | Overrides executable service name. |
| `OTEL_RESOURCE_ATTRIBUTES` | Comma-separated `service.instance.id=value` and/or `deployment.environment.name=value`. No escaping; values up to 256 bytes. |
| `OTEL_TRACES_SAMPLER` | `parentbased_traceidratio` (default) or `parentbased_always_on`. |
| `OTEL_TRACES_SAMPLER_ARG` | Finite ratio 0–1, default 1; must equal 1 for `parentbased_always_on`. |
| `OTEL_SDK_DISABLED` | `true` disables export even with an endpoint; `false` is the default. |

Unsupported `OTEL_*` settings fail startup with a named configuration error. The adapter does not silently accept unsupported protocols, header credentials, compression, exporter timeouts, or SDK queue overrides. Supplied resource service/version/instance/environment values are bounded. Export configuration does not overwrite accepted submission origin or any CloudEvent.

The ordinary SDK batch processor runs blocking reqwest HTTP on its own OS thread. Completed spans, including the in-flight batch, share a 1,024-span budget; batches contain at most 256 spans and the scheduled interval is one second. Each HTTP exchange has a two-second timeout. No failed-batch retry or resend backlog is added. Acknowledgement bodies are limited to 64 KiB. Malformed acknowledgements are export failures; collector partial rejections are counted without retrying an already partially accepted batch.

SDK span limits are 32 attributes, four events with eight attributes each, and eight links with four attributes each. Before completed spans enter the queue, names/keys are bounded to 128 UTF-8 bytes and string values/status descriptions to 256 bytes. Arrays contain at most 16 elements; string array elements use 16 bytes each. Attribute vectors and array/string backing allocations are compacted. Conservative accounting of the pinned Rust representations, including maximum string-array slots and W3C tracestate storage, budgets at most 128 KiB of retained span payload per completed span (128 MiB for the full queue). Tests check this accounting against the actual type sizes; allocator rounding, SDK/HTTP serialization copies and active-span allocations are additional. These limits bound retained completed-span data and serialization; they do not claim a process-wide RSS bound or prevent temporary allocation while a caller formats a field. Active application operations, the allocator, TLS/HTTP machinery and Python logs have separate ownership and bounds. Task data and reports are not automatically recorded as attributes.

`statistics()` reports locally dropped completed spans, failed export batches/spans, and collector-reported rejections/warnings. Queue overflow emits one initial diagnostic; export/collector warnings are aggregated at most once per 30 seconds and nonzero totals are reported on shutdown. Diagnostic records are not exported as spans or span events. Exporter and HTTP-library spans are excluded to prevent recursion.

After application cleanup, move `Telemetry` to a dedicated OS thread and call its consuming `shutdown()`. Observe that thread's result while retaining first/second signal handling. An outer three-second deadline covers the SDK drain and provider/exporter destruction. This remains bounded even if the SDK acknowledges before joining an exporter thread whose destructor is blocked; timeout abandons unresolved telemetry. Do not put this call in a Tokio `spawn_blocking` task whose runtime destructor might then wait for it. Dropping `Telemetry` starts zero-wait abandonment on an independent thread, so an ordinary async drop does not synchronously destroy the blocking client's private runtime. Crash, forced stop, queue overflow, timeout and collector failure can lose telemetry; durable task state remains authoritative.

## Local wire capture

Build the acceptance receiver with `cargo build -p ledgence-adapter-otel --example otlp-capture --locked`, then run `target/debug/examples/otlp-capture 127.0.0.1:4318`. It prints its endpoint to stderr and decoded span JSON lines to stdout. Port 0 selects an available ephemeral port. Point worker/orchestrator trace endpoints at its `/v1/traces` path. The fixture verifies actual HTTP/protobuf bytes and parent IDs without a hosted service. It is a bounded verification receiver, not an upstream OpenTelemetry Collector or production telemetry backend.

## Operational metrics

See [metrics and exact counting semantics](../../docs/metrics.md). The fixed-vocabulary Ledgence event carrier is consumed by a separate optional aggregation layer, independent from log filters and span sampling. No vendor SDK enters worker or orchestration contracts. Metrics have a dedicated periodic exporter thread and bounded response handling. Both providers share the outer shutdown deadline; metrics failures are available through `metrics_statistics()`. The local capture fixture also prints `OTLP_CAPTURE_METRICS_ENDPOINT` and emits decoded metric JSON lines with `signal: "metrics"`.
