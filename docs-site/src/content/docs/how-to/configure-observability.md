---
title: Configure traces, metrics and logs
description: Export optional OpenTelemetry diagnostics and correlate Python logs with durable Ledgence execution identities.
---

Ledgence 0.3.1 exports traces and metrics over **OTLP HTTP/protobuf**. Choose an
operator-managed collector or compatible receiver. Self-hosting and Console do
not require a telemetry provider account.

## Enable export for each process

With a collector listening on the same host, set these variables in the
environment of each worker and orchestrator process before starting it:

```sh
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://127.0.0.1:4318/v1/metrics
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=local
```

Endpoints must include the signal path. Configure either signal alone if that
is all you need. The default `ledgence` build includes the optional `otel`
feature; instrumentation remains off without an explicit endpoint.

For Docker, pass the variables to the actual services through your deployment
configuration and use the collector hostname reachable **from those containers**.
Exporting variables in the host shell does not automatically inject them into
Compose services or into `ledgence local` containers.

Useful supported settings include:

| Setting | Meaning |
| --- | --- |
| `OTEL_SERVICE_NAME` | Override the default worker, orchestrator or CLI service name for that process. |
| `OTEL_RESOURCE_ATTRIBUTES` | Accepts `service.instance.id` and `deployment.environment.name`. |
| `OTEL_TRACES_SAMPLER` | `parentbased_traceidratio` or `parentbased_always_on`. |
| `OTEL_TRACES_SAMPLER_ARG` | Root sampling ratio from 0 to 1; use 1 for `parentbased_always_on`. |
| `OTEL_SDK_DISABLED=true` | Disable both signals. |
| `RUST_LOG` | Control Rust diagnostic logging independently of sampling. |

Unknown or unsupported `OTEL_*` options fail startup. This adapter does not
support gRPC export, generic endpoint path expansion, exporter authentication
headers, custom certificates or OTLP log export. Route through your collector
when the destination needs additional credentials or transport settings.

## Add correlated program logs

Use runtime protocol 2 or 3 and the worker helper supplied with Ledgence:

```python
from ledgence.worker import get_logger

log = get_logger(__name__)

def handle(event):
    invoice_id = event["data"]["invoice_id"]
    log.info("Invoice accepted", extra={"attributes": {"invoice.id": invoice_id}})
    return {"invoice_id": invoice_id, "accepted": True}
```

Structured logs retain task, attempt, program and available trace context.
Workflow invocations also retain workflow and activation identity. Arbitrary
raw stderr remains process-level output; it cannot reliably be attributed to an
invocation after the fact.

For custom application spans, supply the optional Python OTel dependencies in
the program package and use the helper's `ledgence.worker.otel.enable_context()`
integration with an application-owned provider. The helper does not install an
exporter or fetch dependencies during execution. See the
[Python helper contract](https://github.com/Ledgence/ledgence/blob/v0.3.1/sdk/python/README.md#optional-opentelemetry-api-bridge).

## Interpret execution and capacity

The metrics include HTTP/database duration, program preparation, cache hits,
process starts/reuse, active execution, occupied consumers, claim queue age,
recovery and completion delivery. Occupied consumers include reserved slots
waiting for work or settlement; this is different from actively running
programs or CPU utilization.

Metrics describe observations in each process. They do not reconstruct durable
totals after a crash or expose global queue depth, per-queue cost, or a complete
backlog. Task IDs and business correlation belong in traces/logs and inspection
APIs, not unbounded metric labels.

## Keep the execution graph authoritative

Console builds Graph and Trace from durable execution identities, relationships
and observations. OTel complements that history with per-attempt spans and
instrumented internal calls. Missing or unsampled spans do not remove graph
nodes or change execution results.

Do not infer dependencies by comparing timestamps across workers. Replays keep
the same logical operation; retries have separate attempts. The stored workflow
relationships establish invocation, waits and resume edges.

An unavailable collector after startup does not fail tasks. Telemetry queues,
export deadlines and shutdown time are bounded, so diagnostics can be dropped.
Retain the execution records needed for authoritative investigation.

**Source:** [Tracing and log contract](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/observability.md) · [Metric instruments and limits](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/metrics.md)
