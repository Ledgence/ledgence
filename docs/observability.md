# Observability

Ledgence exports optional platform traces through OTLP/HTTP with protobuf and writes correlated JSON logs to stderr. The endpoint belongs to the operator; no hosted account is required. Task, attempt, lease, and settlement records remain authoritative when traces are unsampled, dropped, or unavailable.

## Enable tracing

The executables include the optional `otel` Cargo feature by default. Tracing is off until an explicit traces endpoint is configured:

```sh
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
# Start the orchestrator and worker using the HTTP quickstart.
```

This is the full endpoint, including its path; Ledgence does not append `/v1/traces`. Each process configures its own provider. Supported settings are deliberately explicit:

| Setting | Behavior |
| --- | --- |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Enable tracing to an absolute HTTP(S) URL, at most 2048 bytes; no URL user information or fragment. |
| `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`, `OTEL_EXPORTER_OTLP_PROTOCOL` | If supplied, must be `http/protobuf`. |
| `OTEL_SDK_DISABLED` | `true` disables the provider; `false` is the default. |
| `OTEL_SERVICE_NAME` | Override `ledgence-worker`, `ledgence-orchestrator`, or `ledgence-cli`. |
| `OTEL_RESOURCE_ATTRIBUTES` | Accepts `service.instance.id` and `deployment.environment.name`, comma-separated `key=value`. Instance ID is otherwise generated per process. |
| `OTEL_TRACES_SAMPLER` | `parentbased_traceidratio` (default) or `parentbased_always_on`. |
| `OTEL_TRACES_SAMPLER_ARG` | Finite root ratio from 0 to 1, default 1; must be 1 for `parentbased_always_on`. |
| `RUST_LOG` | Controls diagnostic logs independently; `error` does not disable trace creation or propagation. |

Unknown `OTEL_` settings, unsupported protocols, and invalid configuration fail startup clearly. An unavailable collector after startup does not change task outcomes or existing contexts. This first adapter does not support exporter authentication headers, custom certificates, metrics export, or OTLP log export. Use an operator-managed collector for routing and backend credentials.

To compile an executable without any OpenTelemetry dependency, use `cargo build -p ledgence-worker --no-default-features --locked` (likewise for `ledgence-orchestrator` or `ledgence-cli`). Such a build rejects a configured traces endpoint. `tools/check-otel-features.py` checks all three dependency graphs and compilation paths.

## Identity and causal relationships

For a submitted task with caller origin S, the expected tree is:

```text
accepted submission origin S
  invocation creation P1
    worker processing W1
      program preparation
      fresh runtime startup (only when needed)
      program execution E1
        optional application spans
      settlement and its HTTP exchanges
      required cleanup
  invocation creation P2 (a later retry)
    worker processing W2
      ...
```

P is created only for a new invocation, after acquisition replay has been excluded. Pending/Empty probes and wake hints create no producer span. W begins when the worker creates its processing span; its reported duration excludes the preceding acquisition wait. P's context is stored in that invocation's CloudEvent. A later retry creates a sibling producer under the accepted origin. Without an origin, each producer starts a new root; durable task/run IDs join attempts across traces. An acquisition HTTP context is a diagnostic link, never the producer's causal parent. Known rollback and unconfirmed commit outcomes remain distinct; a lost acknowledgment can leave an actual durable event even when its producer span reports uncertainty.

W starts before dispatch/preparation and remains open through settlement and required cleanup. Its context is captured once into `SettleCommand.processing_trace`, including report normalization and every transport retry. The separate `RuntimeInvocation.processing_context` carries E into Python. E ends when the local execution result is known, before report reconciliation. Healthy warm processes do not extend a completed invocation's execution span.

The CloudEvent source, event ID, envelope, and user-owned `data` are unchanged on redelivery. Processing context is never placed inside user data or substituted for the event's creation context. Existing persisted events remain valid, including events that predate producer instrumentation.

| Orchestrator tracing | Worker tracing | Event and processing context |
| --- | --- | --- |
| Off | Off | Event keeps accepted origin or absence; no SDK-created processing IDs. |
| On | Off | Event carries P; processing context is absent. |
| Off | On | W parents to the event context, or starts a root; E parents to W. |
| On | On | S → P → W → E, with P a root when S is absent. |

Enabled unsampled spans still have valid context and respect the upstream sampling decision. They are different from disabled instrumentation. No historical producer or dead-worker completion is reconstructed on replay/recovery. A crash before settlement may leave no durable W pointer; the event's trace ID and durable execution history remain available.

HTTP request IDs identify individual exchanges and are generated by the server. A lost response can leave the client without that ID. HTTP tracing uses W3C headers independently of accepted body context; malformed transport context cannot replace or invalidate an otherwise valid submission body. Durable body trace fields retain strict validation. No generic `parent_id` is added.

## Span and log fields

Canonical operations are `ledgence.task.submit`, `ledgence.invocation.create` (PRODUCER), `ledgence.attempt.process` (CONSUMER), `ledgence.program.prepare`, `ledgence.runtime.start`, `ledgence.program.execute`, `ledgence.attempt.settle`, `ledgence.attempt.cleanup`, and `ledgence.recovery.scan`, plus HTTP CLIENT/SERVER exchanges. Internal worker coordination supplies diagnostic context without an additional exported operation.

Ledgence's v1 attribute mapping uses `ledgence.tenant.id`, `ledgence.namespace`, `ledgence.run.id`, `ledgence.task.id`, `ledgence.attempt.id`, `ledgence.attempt.number`, `ledgence.worker.session.id`, `ledgence.consumer.id`, `ledgence.program.id`, `ledgence.program.version`, `ledgence.program.digest`, `ledgence.request.id`, and `ledgence.business.correlation_key` where already known. No extra database query obtains attributes. Payloads, program outputs, and idempotency keys are not automatically exported. HTTP/CloudEvents names follow the meanings used by their conventions; this mapping is versioned by Ledgence because some upstream conventions remain in development.

Resources contain service name, version, instance, and optional deployment environment. Execution identifiers belong on spans/logs. Current `trace_id` and `span_id` in JSON logs refer to the active span; copied event `traceparent` remains separate. Python v2 log records retain the context captured when emitted, including records forwarded after the invocation ended. Raw stderr remains process-level output because arbitrary bytes cannot be assigned reliably to one invocation.

Measured `ledgence.duration_ms` fields use local monotonic clocks. SDK span timestamps use wall-clock time. Do not subtract timestamps across hosts to infer authoritative duration.

## Python programs

Use protocol v2 or v3 packages for the separate processing carrier and structured logging. Existing immutable v1 packages remain executable. See the [Python helper](../sdk/python/README.md) and [package protocol](program-packages.md).

```python
from ledgence_worker import get_logger

log = get_logger(__name__)

def handle(event):
    invoice = event["data"]
    log.info("Invoice issued", extra={"attributes": {"invoice.id": invoice["invoice_id"]}})
    return {"invoice_id": invoice["invoice_id"], "accepted": True}
```

The default helper uses only the Python standard library. Applications may explicitly call `ledgence_worker.otel.enable_context()` with a separately supplied OTel API/provider. This attaches E for custom child spans and detaches it after the handler. It does not install a provider or bundle a network exporter. With no processing carrier, it starts from an empty OTel context. Optional provider shutdown callbacks execute before closing acknowledgment within the worker's existing process shutdown grace.

## Bounds and failure behavior

Completed Rust spans, including the in-flight batch, are capped at 1024; batches contain at most 256 spans. Batch interval is one second, HTTP timeout two seconds, response bound 64 KiB, and best-effort provider shutdown budget three seconds. Completed span data is bounded to 32 attributes, 4 events, 8 links, and 256-byte string values, with smaller limits on event/link attributes and arrays. Sampling, attribute limits, and exporter queue capacity are separate from execution concurrency. Failed batches are discarded with aggregated diagnostics; there is no application resend backlog.

The Python v2 writer has at most 64 queued optional log records and 1 MiB of queued log bytes. Each log frame fits the smaller of 16 KiB and the configured output-frame limit, including newline. Result/control traffic has a reserved priority slot; one bounded log may already be in flight. Rust continuously drains log frames during execution and warm idle, yielding fairly under a flood. Optional invalid or oversized log content can be dropped; invalid control identity and broken framing still fail the protocol. Rust JSON sinks are separately bounded. Dropping optional logs does not rewrite a result or durable lifecycle state.

Normal shutdown finishes owned execution/runtime work, drains telemetry within its budget, then drains output. Signal subscriptions remain active so an explicit second signal can abandon outstanding optional telemetry. Crash, timeout, or force can lose spans and logs; traces are not a durable audit log.

## Verification receiver

For a local capture without a hosted service:

```sh
cargo run -p ledgence-adapter-otel --example otlp-capture --locked -- 127.0.0.1:4318 > traces.jsonl
```

The example receives actual OTLP HTTP/protobuf and writes decoded spans with parent IDs, attributes, status, and resource identity. It is a bounded verification fixture, not a production OpenTelemetry Collector or persistent tracing backend. Point the processes at its `/v1/traces` endpoint and run the HTTP quickstart. Adapter tests use actual OTLP bytes for parentage, sampling, bounds, and stalled-exporter checks; database tests separately verify replay and ambiguous commit behavior. The separate-process observability gate checks the composed platform against this receiver.


## Workflow correlation

Controller and child CloudEvents carry `ldgworkflowid`; controller events also
carry `ldgactivationid`, equal to their stable task ID. Task inspection/status
preserves those relationships. Worker processing/execution spans and structured
logs add `ledgence.workflow.id` and `ledgence.activation.id` when applicable;
protocol 3 Python log records retain the same workflow identity across forwarding.
Normal warm reuse clears the previous invocation's context.

Workflow children and later activations currently retain the workflow's accepted
submission origin for invocation creation. The first slice does not export a
dedicated span per durable local step or reconstruct an uninterrupted span through
a suspended wait. Existing per-attempt trace carriers and durable workflow/task
IDs provide correlation; traces remain optional, lossy observations. Durable
workflow state, checkpoints, and result records establish execution outcomes.
