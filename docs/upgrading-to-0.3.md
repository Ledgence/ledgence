# Upgrade to Ledgence 0.3.0

This guide covers a **0.2.0 → 0.3.0** deployment. For an older installation, read
[the 0.2 upgrade guide](upgrading-to-0.2.md) first. Keep historical releases and
backups available; an upgraded database must not be opened by old binaries.

## What must match

| Component | 0.3.0 requirement |
| --- | --- |
| CLI, orchestrator and worker | The same 0.3.0 distribution/source |
| Python runtime helper | The helper supplied with that worker |
| Python HTTP client | `ledgence-client==0.3.0` |
| Console | Contract **5** assets and server; 0.2 used contract 4 |
| PostgreSQL | Explicitly apply `20261004000000_workflow_approvals.sql` through the migrator |
| Custom Rust adapters | Rebuild against the 0.3 API crates and handle new approval variants |

Task/workflow IDs, attempt IDs, registered immutable program descriptors and
persisted checkpoints retain their identities. Python workflow protocol **3**
continues to apply. Existing event/timer waits and `workflow_context()` programs
remain supported. The `ledgence.client` and `ledgence.worker` imports do not change.

## Upgrade an installation

1. Pause producers, callback/event senders and administrative writers. Gracefully
   stop workers and orchestrators before migrating. Preserve the instance
   configuration, completion/delivery configuration and immutable program store.
2. Take and verify a restorable PostgreSQL backup plus the deployment's required
   program/configuration backup. Record the old binary/helper/Console versions.
   Keep the backup outside the installation being replaced.
3. Install the matching 0.3.0 executable, runtime helper, Python client and Console
   assets. Prepare them before stopping services when possible. Do not combine
   an old server with new assets, or old workers with new approval behavior.
4. With the deployment's normal `DATABASE_URL` set, run the new executable:

   ```sh
   ledgence orchestrator migrate
   ```

   The migrator applies missing migrations transactionally and checks historical
   checksums. The new migration extends wait kinds and adds the approval ledger.
   It validates existing wait rows and takes PostgreSQL table locks; allow a
   maintenance window sized for the installation. Never edit released migration
   SQL to bypass a checksum mismatch. `serve` verifies schema; it does not migrate.
5. Start the new orchestrators with the preserved instance and delivery bindings,
   then workers with the matching runtime helper. Confirm readiness, registered
   programs, representative retained results and pending workflow waits.
6. Verify one task and workflow, resume a retained wait, and check callback
   delivery/reconciliation. If using approvals, verify a pending request and its
   exact effective action before deciding. Refresh old Console history/graph
   pages from their first page, then resume producers.

Compose deployments follow the same order: stop old writers, preserve named
volumes, build from `v0.3.0`, apply migrations and recreate matching services.
Do not remove volumes to work around an upgrade failure. See
[local deployment](local-deployment.md).

## Application and adapter changes

`ctx.operation` and durable approval helpers require the new worker helper.
Application code owns provider clients, credentials, dynamic tools and external
idempotency. A saved result prevents deliberate re-execution only after durable
acknowledgment; retries cannot guarantee exactly-once external side effects.

Rust adapters with exhaustive matches must handle approval variants of
`WorkflowWait`, `WorkflowWake` and related workflow/Console contracts. Implement
new optional service operations when the adapter supports approvals; an adapter
that rejects an unsupported operation must preserve its documented error and
uncertainty behavior. Rebuild and run adapter contract tests before deployment.

MCP is enabled by default in the CLI and can be omitted through Cargo features.
It uses stdio and the HTTP API; it creates no extra network listener. See
[MCP setup](mcp.md) for fixed scope and read-only mode.

## Recovery and rollback

A failed transactional migration rolls back its own changes. Investigate and
retry with the same reviewed source once the cause is corrected. Do not resume
old writers against a schema that has successfully upgraded.

Rollback restores the verified pre-upgrade database and matching program,
configuration and 0.2 components together. Reconcile any external effects that
occurred after the backup. There is no automatic down-migration or supported
rolling mix of 0.2 and 0.3 services.

## Upgrade regression

The PostgreSQL gate includes a populated historical-schema regression. It pins
the 18 released 0.2 migration checksums, preserves representative task results,
workflow checkpoints and event/timer waits, resumes old work after migration,
and exercises new approval behavior. It also checks repeat migration. Run
`tools/check-postgres.py` only against its explicitly owned disposable database
and container as documented in [PostgreSQL verification](postgres.md).
This automated fixture complements an installation-specific backup/restore and
upgrade rehearsal; it is not a rolling-upgrade claim.
