# Ledgence Console

Console is the self-hosted operator UI for one Ledgence instance. It serves static
files from the Rust orchestrator and calls the same instance's APIs. Production
needs no Node process, CDN, vendor account or hosted web service.

## Open the local deployment

Follow [local deployment](local-deployment.md), including the explicit publication
command, then open [Console](http://127.0.0.1:8080/console/). The four sections use
persisted executions, recorded workflows, registered programs and actual worker
observations. The supplied demo uses one compatibility binding configured on the
server. There is no workspace, tenant or namespace selector in the browser.

- **Executions**: filter by supported exact values, inspect input/result/attempts
  and history, submit a program, run it again as new work, or request cancellation.
- **Workflows**: inspect activations and recorded relationships, local steps,
  child and external waits; start a registered controller, send an event to an
  actual wait, or request cancellation. Recorded work is not a future DAG.
- **Agents**: inspect registered packages, exact opaque versions, digests and
  runtime requirements. Registration is explicit and independent of execution.
- **Workers**: inspect server-received observations, stable process slots and
  validated execution links. Fresh/stale/no-recent-report describes the age of
  information; it does not establish process death or task authority.

Lists are live keyset pages, not a transaction frozen across navigation. There
are no synthetic fleet totals. Inputs and terminal results load on demand; normal
refreshes read bounded metadata. Hidden/offline pages pause polling. The browser
validates instance identity and contract version before accepting responses.

## Native startup

Build assets explicitly with the pinned toolchain in [Console build guide](https://github.com/Ledgence/ledgence/blob/develop/console/README.md),
or use the `console/` directory from a qualified native bundle. A regular Cargo
build/test does not run frontend tooling. Create a server-only instance file:

```json
{
  "instance_id": "local",
  "name": "Local instance",
  "suggested_queues": ["default"]
}
```

For new installations the omitted compatibility scope is `default/default`.
Legacy SDK and worker calls must match it. For existing data, configure its actual
binding before starting; see [binding and migration](self-hosted-instance.md).
With `DATABASE_URL` set for the intended database, from a source checkout:

```sh
cargo run -p ledgence-orchestrator -- migrate
cargo run -p ledgence-orchestrator -- serve --store /path/to/program-store \
  --bind 127.0.0.1:8080 --instance-config /path/to/instance.json \
  --console-dir console/dist
```

For an extracted native bundle use `bin/ledgence-orchestrator` and
`--console-dir /absolute/path/to/bundle/console`. Keep `--instance-config` on every
startup, even if assets are disabled by omitting `--console-dir`. A missing,
modified or incompatible static asset fails startup before coordinators begin.
The console source manifest and retained notices accompany the assets.

## Register immutable programs

Publish a prepared package into the configured store first. Register its exact
reference against the running instance:

```sh
ledgence program register --server http://127.0.0.1:8080 \
  --program invoice-issuer --version 1.0.0 --kind task
ledgence program register --server http://127.0.0.1:8080 \
  --program invoice-workflow --version release-september --kind workflow
```

Registration fetches and verifies the archive/descriptor/manifest without executing
it. Packages for another supported host platform can be registered; a worker must
still satisfy their actual runtime requirements. The catalog does not enumerate
private stores or upload/compile code. Package versions are immutable and are not
assumed to be SemVer. Declare `task`, `workflow` or `unspecified` explicitly; the
runtime protocol does not classify business intent.

Registration of identical metadata and bytes is idempotent. Use
`--update-metadata true` deliberately to change descriptive metadata for an
existing reference. Changing its digest is a conflict and needs a new version.
If publication succeeds but registration fails, retain the immutable artifact and
repeat registration to reconcile. Do not delete it as a rollback. The Compose
publish command performs these two steps in this order.

## Commands and observation

Transport timeouts can occur after a mutation commits. Console never automatically
retries a write. Its explicit retry retains the exact in-memory body and identity;
editing the intended submission requires a separate action. Navigating away can
lose that in-memory command. **Run again** creates a new submission and new key.
Business effects remain at-least-once; application idempotency is still necessary.

User JSON preserves signed/unsigned 64-bit integers and floating categories,
including successful `null`, `1.0` and `-0.0`. Platform metadata stays outside
user `data`. A pending result is distinct from a completed null result.
Cancellation is a request: active cleanup or draining children can remain pending.
An event accepted by the server is distinct from a workflow that has resumed.

Worker observation uses a separate bounded reporter. A missing or stale report
does not renew a lease, release capacity, change recovery or stop program execution.
A healthy reused process keeps its identity; replacement gets a new identity.
Capacity above 1024 remains supported by execution, while detailed slot observation
is explicitly unavailable at that size. See [worker observations](worker-observations.md)
and [query contracts](console-query-model.md).

## Reverse proxy and upgrades

Protect both `/console/` and `/v1/` with the same access-controlled TLS proxy if
remote access is required. The instance binding is not authentication. Preserve
these paths and serve assets/API from the same public origin. Configure that
exact origin in the server's `allowed_origins`, for example
`["https://ledgence.example.com"]`; the server does not infer it from proxy headers.
CLI/worker requests without an Origin remain supported. Loopback listeners infer
their local HTTP origins when the configured list is empty.

Console's CSP allows local scripts only, without inline JavaScript or eval. Inline
CSS is allowed for the reviewed accessible dialog scroll-lock behavior. Do not
rewrite unknown assets or API errors into HTML. The orchestrator already handles
supported SPA deep links and exposes Notices through the UI.

Upgrade all writers together, back up PostgreSQL and immutable artifacts, run
explicit migrations, and start with matching instance configuration and assets.
Mixed historical bindings fail startup and require an offline operational decision.
No migration deletes or reassigns them. Rollback to an older binary that ignores
the persisted binding requires an offline plan; never run it concurrently against
the bound database. Catalog retention is independent of execution-history cleanup.

## Qualification

The reproducible native acceptance gate uses only uniquely owned disposable
resources and requires a PostgreSQL server permitting database creation:

```sh
LEDGENCE_POSTGRES_URL=postgres://postgres:local-test@127.0.0.1:5432/ledgence \
LEDGENCE_PYTHON=/path/to/python3.14 \
python3 tools/check-console.py --binaries target/debug --console-dist console/dist \
  --evidence /tmp/ledgence-console-evidence \
  --browser-command pnpm --dir console test:live
```

Component fixtures supplement this real HTTP/PostgreSQL/worker gate. Container
recreation is checked by `tools/check-deployment.py`; relocated native bundles by
[release verification](releasing.md). Preserve their separate evidence and actual
platform/source identity. Running a mocked browser suite alone does not qualify a
deployment, and local compatibility tests do not certify AWS or cluster capacity.
