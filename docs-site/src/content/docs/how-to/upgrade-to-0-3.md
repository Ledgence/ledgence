---
title: Upgrade to 0.3.1
description: Upgrade a 0.2 deployment with preserved execution state, an explicit approval migration, and matching Console assets.
---

Ledgence 0.3.1 adds durable approvals, model/tool operation recovery and MCP.
It requires a database migration and Console contract **5**. Upgrade the
orchestrator, worker, Python runtime helper, client and Console together.
For an older installation, read [the 0.2 upgrade guide](/how-to/upgrade-to-0-2) first.

The earlier `v0.3.0` source tag had no published distribution. A deployment
built from that source can follow this guide; 0.3.1 adds no further database
migration or protocol change, and the migrator only applies missing migrations.

## Prepare and migrate

1. Pause producers and other writers. Gracefully stop workers and orchestrators.
   Preserve the immutable program store, instance binding and delivery/callback
   configuration.
2. Take and verify a restorable PostgreSQL backup and the required deployment
   backup. Keep the matching old binaries/helper/assets available.
3. Install matching **0.3.1** components. Set the deployment's `DATABASE_URL` and
   run the new executable:

   ```sh
   ledgence orchestrator migrate
   ```

   Migration `20261004000000_workflow_approvals.sql` adds the approval ledger and
   extends wait kinds. Existing rows are validated under PostgreSQL table locks;
   plan maintenance time for your database size. Do not edit historical migrations.
4. Start matching services and Console assets. Verify readiness, catalog, retained
   results, resumed waits and callback delivery. Refresh old history/graph pages
   from the first page, then resume producers.

`serve` verifies the schema and never migrates it automatically. Do not remove
Compose volumes to resolve a migration problem or run old writers against the
upgraded schema. Native installation and source commands are in
[Install native tools](/how-to/install-native) and [Run locally](/tutorials/run-locally).

## Compatibility

Execution identities and immutable programs remain in use. Existing event/timer
waits and workflow protocol **3** remain supported. The Python namespaces stay
`ledgence.client` and `ledgence.worker`; install `ledgence-client==0.3.1`.

Custom Rust adapters must rebuild against the 0.3 API crates and handle the new
approval-related workflow enum variants and operations. The new helper APIs
require the matching worker helper. No provider framework becomes a platform dependency.

MCP uses stdio with the existing HTTP service; it does not open an additional
listener. See [Connect an MCP client](/how-to/connect-mcp).

## Roll back as a matching deployment

Restore the verified pre-upgrade database and matching programs/configuration
with the old components. Reconcile external effects after the backup. There is
no automatic down-migration or supported mixed-version rolling deployment.

The release's populated-database regression verifies historical checksums,
retained checkpoints/results, old event/timer resumption, new approval behavior
and repeat migration. Also rehearse backup/restore with representative local data.

**Source:** [Full upgrade contract](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/upgrading-to-0.3.md) · [Release notes](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/releases/0.3.1.md)
