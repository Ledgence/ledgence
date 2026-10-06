# CI qualification

CI validates source behavior and distributable artifacts separately. A passing
package job alone does not qualify a release. Check every applicable workflow for
the exact commit being promoted; previous commits, skipped jobs, local tests and
published artifacts are different evidence.

## Independent suites

The `CI` workflow runs the Rust/Python and installed-client matrices on Linux and
macOS with Python 3.11–3.14, plus dependency policy checks. Database and delivery
acceptance is split into independent jobs:

| Job or acceptance suite | Coverage |
| --- | --- |
| PostgreSQL 18 transactions and query metadata | Online SQLx metadata comparison and all explicitly selected database tests; isolated container for restart tests |
| SQS adapter conformance | The existing ignored adapter acceptance test against ElasticMQ |
| `http` | Separate-process delivery using the default-feature CLI, including worker crashes and connectivity failures |
| `sqs` | PostgreSQL, HTTP and Python delivery through ElasticMQ with the default acquisition wait and production lease durations |
| `api` | OTel, MCP, installed Python client, durable completion callbacks, and the fulfillment workflow example |
| `workflows-integrated` | Complete workflow checkpoint, ownership, fork/join, approval and agent recovery suite with integrated delivery |
| `workflows-sqs` | The same complete workflow suite with ElasticMQ delivery |
| `workflows-tracing` | Events, owned workflow trees and mixed forks, with assertions on their exported trace relationships |

Acceptance suites have separate PostgreSQL services; the two SQS suites also
have separate ElasticMQ services. The PostgreSQL adapter tests that restart their
container cannot interrupt other suites. Existing fault-injection assertions,
real lease-expiry tests, feature boundaries and OS/Python combinations remain
part of qualification. ElasticMQ results do not establish real AWS SQS capacity.

A shared build job compiles the default-feature CLI, all-feature CLI and OTLP
capture executable once each. It retains a tar archive that preserves executable
permissions, source SHA and checksums. Acceptance jobs download that artifact from
the same workflow run and verify it before execution. Retrying a failed acceptance
job uses the successful build job's artifact output, even if the run attempt has
changed. These are test executables, not release bundles.

The Cargo cache contains downloaded registry archives, indexes and Git dependency
objects, keyed by operating system,
architecture, toolchain and lockfile. It does not contain credentials, compiled
workspace outputs or generated SQLx metadata. A cache hit never replaces a build
or test. PostgreSQL qualification still cleans the adapter before generating fresh
query metadata against the database.

## Timing evidence

HTTP and workflow acceptance retain `timings.jsonl` beside `results.json` and
fixture logs. Each timing row records `kind` (`scenario` or `process-stop`),
`name`, `result` and `elapsed_seconds`; failures also record `error_type`.
Scenario duration includes worker drain. Nested stop durations are already part
of scenario duration, so do not add both when calculating elapsed suite time.
The new timing rows omit exception messages, command arguments and environment
values. Fixture evidence can contain synthetic task inputs and results.

Workflow ElasticMQ functional acceptance uses `--acquire-wait-ms 1000` to avoid
paying a mostly idle 20-second receive during repeated fixture teardown. This is
an explicit test option requiring `--endpoint`; omitting it retains the worker's
20,000 ms default. `resources.json` records the effective value. The separate
SQS delivery suite continues to exercise the default wait. Lease durations,
recovery assertions and product defaults are unchanged.

Each acceptance job uploads its available evidence on success or failure for
seven days. Look for `acceptance-<suite>-<sha>-<attempt>` in the workflow artifacts.
A failure before fixture creation may have no evidence directory; use the job
logs in that case. Compare individual scenario/stop times separately from build,
artifact-transfer, dependency-installation and GitHub queue times. Test elapsed
time is not a production throughput benchmark.

## Pushes and publication

On a push, standalone source workflows run according to their own triggers.
Registry packages runs its source identity and package artifact gates when its
paths change; it does not launch a second copy of CI, documentation, Console or
examples. Expected skipped reusable jobs in that push workflow are not successful
validation and must not be counted as such.

Every manual Registry packages dispatch, including qualification with
`publish=false`, runs all four source workflows and both package gates for the
selected source. Publishing requires all of them to pass in that dispatch and
revalidates the annotated tag and `main` ancestry before uploading. Push
qualification and manual publication have separate concurrency groups, so a new
push cannot cancel a running publication. See [registry packages](registry-packages.md)
and [release bundles](releasing.md) for the release procedures.

Implementation references: [GitHub reusable workflows](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations)
and [workflow artifacts versus dependency caches](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflow-artifacts).
