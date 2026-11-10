# CI qualification

CI validates source behavior and distributable artifacts separately. A passing
package job alone does not qualify a release. Check every applicable workflow for
the exact commit being promoted; previous commits, skipped jobs, local tests and
published artifacts are different evidence.

## Independent suites

The `CI` workflow runs the Rust/Python and installed Python package matrices on
Linux and macOS with Python 3.11–3.14, plus dependency policy checks. The package
matrix checks `ledgence-client` and the development distribution `ledgence-worker`
outside the checkout, including their shared namespace. The worker gate reuses
the client gate's hash-verified dependency wheelhouse offline. Each job retains
its available package evidence for seven days. This qualifies local artifacts;
it does not publish the worker helper to PyPI. Database and delivery acceptance
is split into independent jobs:

| Job or acceptance suite | Coverage |
| --- | --- |
| PostgreSQL 18 transactions and query metadata | Online SQLx metadata comparison and all explicitly selected database tests; isolated container for restart tests |
| SQS adapter conformance | The existing ignored adapter acceptance test against ElasticMQ |
| `http` | Separate-process delivery using the default-feature CLI, including worker crashes and connectivity failures |
| `sqs` | PostgreSQL, HTTP and Python delivery through ElasticMQ with the default acquisition wait and production lease durations |
| `api` | OTel, MCP, installed Python client, instance credentials, operator audit, retained-effect recovery, native effect resolution and worker-free waits, durable completion callbacks, and the fulfillment workflow example |
| `artifacts` | Framework-neutral file exchange with real workers, installed clients, CLI/MCP, filesystem and owned S3 fixture, recovery faults and retained-byte verification |
| `provider` | Typed provider recovery, finite read-only checks, actual predecessor-worker compatibility, retained evidence and an owned database dump/restore |
| `provider-sqs` | Provider pending/probe/wake delivery and frozen probe-report retries with both logical queues routed through ElasticMQ |
| `workflows-integrated` | Complete workflow checkpoint, ownership, fork/join, approval and agent recovery suite with integrated delivery |
| `workflows-sqs` | The same complete workflow suite with ElasticMQ delivery |
| `workflows-tracing` | Events, owned workflow trees and mixed forks, with assertions on their exported trace relationships |

Acceptance suites have separate PostgreSQL services. The three SQS suites also
have separate ElasticMQ services. The PostgreSQL adapter tests that restart their
container cannot interrupt other suites. Existing fault-injection assertions,
real lease-expiry tests, feature boundaries and OS/Python combinations remain
part of qualification. ElasticMQ results do not establish real AWS SQS capacity.

A shared build job compiles the current default-feature CLI, all-feature CLI and
OTLP capture executable once each. After preserving those binaries, it builds the
reviewed predecessor `3a5cc0d37cb70baeb300d0f77be3659f8c2ef48f` from a separate
Git archive. The predecessor uses its own helper source, not the current helper.
Compilation shares dependency outputs only within this job; it does not replace
the current checkout or preserved binaries. The tar archive retains executable
permissions, both source SHAs and checksums for every executable and helper file.
Acceptance jobs download that artifact from
the same workflow run and verify it before execution. Retrying a failed acceptance
job uses the successful build job's artifact output, even if the run attempt has
changed. These are test executables, not release bundles.

The Cargo cache contains downloaded registry archives, indexes and Git dependency
objects, keyed by operating system,
architecture, toolchain and lockfile. It does not contain credentials, compiled
workspace outputs or generated SQLx metadata. A cache hit never replaces a build
or test. PostgreSQL qualification still cleans the adapter before generating fresh
query metadata against the database.

The Rust/Python matrix builds several isolated feature selections in each hosted
job. Its development and test builds, and the shared acceptance build, disable incremental compilation and use
`line-tables-only` debug information to reduce temporary disk use. Backtraces
retain source filenames and line numbers. The normal debug assertions, integer
overflow checks and complete test/feature matrix still run. Job logs report free
disk before compilation and testing, plus the final target-directory size when
the runner reaches cleanup. These settings are scoped to CI qualification jobs.
[Cargo profile settings](https://doc.rust-lang.org/cargo/reference/profiles.html).

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

After promotion to `main`, let its applicable validation workflows finish even
when the same commit already passed on `develop`. Cancelling those runs leaves
the branch without completed checks; cancellation is not a passing result.
Record the branch, commit, run ID and attempt when reporting validation.
A later rerun may detect newly published dependency advisories against an
unchanged lockfile. Review and test compatible dependency fixes through the
normal integration and release process; do not suppress the audit or replace
previously published artifacts to make an old commit pass.

Every manual Registry packages dispatch, including qualification with
`publish=false`, runs all four source workflows and both package gates for the
selected source. Publishing requires all of them to pass in that dispatch and
revalidates the annotated tag and `main` ancestry before uploading. Push
qualification and manual publication have separate concurrency groups, so a new
push cannot cancel a running publication. See [registry packages](registry-packages.md)
and [release bundles](releasing.md) for the release procedures.

Implementation references: [GitHub reusable workflows](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations)
and [workflow artifacts versus dependency caches](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflow-artifacts).

## Native effect resolution

The API acceptance job exercises `tools/check-effect-resolution.py` with an
installed Python client, the all-feature executable from the current workflow
and an owned disposable PostgreSQL database. Four scenario families cover
worker capacity while parked, resolve-before-park, competing late worker
evidence, exact retry after a lost reply with same-actor credential rotation,
cancellation, real retention collection and receipt replay after collection.
An independent synthetic provider counts dispatches. The gate records its binary
digest and verifies cleanup and credential removal in retained evidence.
Console also tests frozen assessments, permission boundaries, exact audit lookup
and graph navigation in Chromium and WebKit.

## Provider-operation recovery in development

The independent `provider` acceptance suite runs all twelve scenario families in
`tools/check-provider-operations.py`, with an installed current client and the
same-run all-feature executable. It has its own PostgreSQL service and timeout;
these checks do not extend the API suite's execution time. Scenarios cover pending
capacity, lost mutation/grant/report replies, expired provider keys without a
second mutation, unavailable lookup, typed terminal/action-required outcomes,
bounded probe timeouts, concurrent business/domain identities, disputes,
cancellation, worker replacement, actual predecessor execution, retention and
declared restore quarantine. The provider is a real loopback HTTP fixture with
an independent request/effect census; no external provider or account is used.

The separate `provider-sqs` suite runs two focused scenario families with
`--scenario sqs --endpoint http://127.0.0.1:9324`. Both the workflow queue and
the reconciliation queue require external delivery through ElasticMQ. The gate
checks a pending operation through its probe and resumed workflow, then loses a
probe-report acknowledgement and verifies the exact frozen report is retried
without another provider lookup or probe generation. It uses the current binary
and installed client, with no predecessor worker or database restore. The full
twelve-family suite remains on integrated delivery. These local checks establish
the provider recovery path through the SQS adapter without making AWS service
availability or throughput claims.

The predecessor is a reviewed B development commit, not a published release.
Its executable and complete helper inventory are bound to the recorded source
SHA. Consumers reject altered, missing or unlisted artifact files before starting
services. The gate also compares the selected executable/helper bytes after use.
PostgreSQL 18 tools from the pinned service container stream a custom-format
snapshot through host-owned files; `pg_dump` and `pg_restore` share that container's
version. The fixture restores only its owned disposable database and removes the
snapshot during cleanup. The test demonstrates explicit operator-declared
quarantine, not automatic database rollback detection.

The Rust/Python matrix runs the provider harness and runnable example unit tests.
The separate PostgreSQL gate discovers every ignored database regression,
including ordinary-path provider-table lock checks and indexed reconciliation
query tests. Those structural bounds and synthetic scenarios do not establish
production provider throughput or universal idempotency guarantees.

## Execution files in development

Beyond stable 0.9.0, the independent `artifacts` acceptance suite checks isolated
feature combinations, then runs `tools/check-execution-artifacts.py` with the
same-run all-feature executable and installed Python client. Its framework-neutral
example uses separate producer/consumer workers and a workflow. Checks include
empty/multichunk files, producer interruption, server restart, cross-root denial,
lost upload acknowledgement, explicit reconciliation, CLI file operations and
MCP metadata-only discovery. A second run with `--s3-owned` uses the shared pinned
external fixture and verifies the composed path across a persistent-store restart.
No real model provider, AWS account or user workload is involved.

Blob tests separately cover conditional-write conflicts, full checksum/length
validation, denied/corrupt/truncated responses, lost or late PUT replies, delete
faults, spool deadlines, cancellation, retained blocking IO and failed removal.
Console contract 16 covers metadata-only access, explicit verified downloads,
actor/navigation cancellation and one active browser download without a queue.

The manual Linux Console candidate gate additionally rehearses the actual
published 0.9.0 bundle upgrade alongside the required 0.6.1, 0.7.0 and 0.8.0
baselines. It verifies immutable inventories before/after, interrupted migration
rollback, repeated migration, old-binary rejection, fresh database restore and
resumed historical work before exercising new artifacts. Its separate production
S3 adapter gate requires owned-fixture restart proof tied to the clean candidate
SHA. See [candidate qualification](releasing.md). These are required checks, not
claims that an uncommitted development tree already passed GitHub qualification.
