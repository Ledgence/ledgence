# Optional Python application spans

This protocol 2 example proves that an application child span uses the worker's
execution context as its parent. It exports only to an application-owned in-memory
buffer, which is cleared after each invocation. The worker exports its own spans
independently; this example does not provide Python network export.

Prepare the artifact in a build environment matching the worker, using the same
manifest layout as [Python packages](../../docs/program-packages.md). Select
`runtime.protocol: 2` and `handler: "program:handle"`. Vendor the reviewed set:

```text
opentelemetry-api==1.44.0
opentelemetry-sdk==1.44.0
opentelemetry-semantic-conventions==0.65b0
typing_extensions==4.16.0
```

Retain those packages' copyright and license files in the artifact. The three
OpenTelemetry packages use Apache-2.0; typing_extensions uses PSF-2.0. Ledgence
neither installs them at runtime nor bundles a Python exporter. The regular
[Python example](../python/README.md) requires none of these dependencies.

When a child is recorded and the worker's processing carrier is present, the
example checks its parentage. Unsampled parents propagate without recording. When tracing is disabled, the application-owned SDK may still record a
root child span locally; it does not reconstruct a worker span from the event
origin. `register_shutdown` requests provider shutdown before graceful closing;
forced process retirement can lose application telemetry.
