# Upgrade to Ledgence 0.2.0

This guide upgrades an existing 0.1 deployment to Ledgence 0.2.0 **with Console**.
The CLI changes apply to every deployment; adopting Console also binds its
database to one instance. Plan a coordinated maintenance window and use matching
orchestrator, worker helper, workers, and Console assets. Recompile Rust adapters
against the 0.2.0 API crates.

Headless operation without an instance configuration remains supported for a
database that has never been bound. It does not require selecting one scope or
splitting a mixed-scope database merely to upgrade to 0.2.0. Once an instance is
bound, its configuration is required on every start, including headless starts.

## 1. Stop writers and preserve a recovery point

Stop external producers first, then drain and stop workers and orchestrators.
Stop other database writers, including retention jobs and older services. Back
up PostgreSQL and the immutable program store, and preserve the existing
configuration, package references, and binaries. Check that the backups can be
restored before applying migrations.

To adopt Console, an existing database must contain a single compatible tenant/namespace binding.
The new startup audit rejects mixed scopes instead of deleting or reassigning
records. A mixed-scope installation needs separate databases or a planned offline
migration before adoption.

## 2. Replace executable names

The native archive now contains only `bin/ledgence`. Update service managers,
container commands, scripts, and operational procedures:

| Previous command | 0.2.0 command |
| --- | --- |
| `ledgence-worker example` | `ledgence program example` |
| `ledgence-worker publish` | `ledgence program publish` |
| `ledgence-worker run` | `ledgence worker run` |
| `ledgence-worker connect` | `ledgence worker connect` |
| `ledgence-orchestrator migrate` | `ledgence orchestrator migrate` |
| `ledgence-orchestrator serve` | `ledgence orchestrator serve` |
| `ledgence-orchestrator retain` | `ledgence orchestrator retain` |

Existing `ledgence task ...` commands retain their task-administration behavior.
`ledgence program register ...` uses the same spelling as in the earlier Console
source builds. Worker and orchestrator remain separate processes. Run
`ledgence --version` and the relevant group's `--help` to check the installation.
Build source with `cargo build --locked -p ledgence-cli`; the worker and
orchestrator crates are internal libraries without binary targets.

## 3. Configure the instance and migrate explicitly

Create a server-owned JSON configuration using the existing scope:

```json
{
  "instance_id": "production",
  "name": "Production instance",
  "scope": { "tenant_id": "acme", "namespace": "production" },
  "suggested_queues": ["default"],
  "allowed_origins": []
}
```

Replace the example identity and scope with your installation's values. Omitting
`scope` selects `default/default`, which is suitable only if it matches the data
and clients. The instance ID and scope become immutable once bound. Client, CLI,
and worker scope fields must match; the browser offers no scope selector.

With the intended PostgreSQL connection configured and all writers stopped:

```sh
ledgence orchestrator migrate
ledgence orchestrator serve \
  --store /absolute/path/to/program-store \
  --instance-config /absolute/path/to/instance.json \
  --console-dir /absolute/path/to/ledgence-0.2.0/console
```

Retain all other deployment options, store settings, and environment variables
from your service configuration. `serve` verifies the schema; it never performs
implicit migration. Supply `--instance-config` on every subsequent start,
including headless operation. Assets must match Console contract **4**. The
migration set includes instance binding, catalog and observations, workflow
forks, execution discovery, explorer records, and entrypoint persistence.

Start the new orchestrator, then matching workers, and verify readiness and the
instance identity before resuming producers. Check a representative execution,
its retained result, and any completion subscriptions used by your application.
Console assets need no production Node process. Preserve the bundle's licenses
and legal material with the installation.

## 4. Keep application identities and checkpoints stable

The Python worker namespace remains `ledgence.worker`, already present in 0.1.1;
client imports remain `ledgence.client`. Workflow runtime protocol **3** is
unchanged. Existing `workflow_context()` controllers with string continuations
remain supported. Typed entrypoints and `fork` / `join` require the new platform
components and migrations; publish changed application code under a new immutable
program version instead of replacing existing package bytes.

For example, the release's `workflow-example` controller is `1.0.1`, while
`invoice-issuer` and `workflow-summary` remain `1.0.0`. Application versions are
independent of platform 0.2.0. Retained workflows continue to identify their pinned
program descriptor.

If upgrading an earlier development Console, refresh server and assets together.
Contract 4 rejects older Explorer cursors; restart that traversal from its first
page. C3-to-C4 changes do not add a database migration or change existing node
IDs. C2 Explorer records have a transactional migration. Budget maintenance time
for its write locks.

## Rollback

Do not start an older binary against the upgraded database or mix old and new
writers. Older binaries reject the newer schema. If Console was adopted, an older server
also cannot honor the persisted instance binding. Rollback requires stopping the new services
and restoring a matching pre-upgrade database backup, artifact store, binaries,
and configuration. There are no automatic down migrations. Work accepted after
the backup needs an explicit reconciliation plan.

See [instance binding](self-hosted-instance.md), [Console operation](console.md),
and [CLI reference](cli.md) for the detailed contracts.
