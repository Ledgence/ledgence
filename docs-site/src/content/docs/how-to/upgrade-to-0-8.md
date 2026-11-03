---
title: Upgrade to 0.8.0
description: Upgrade from 0.7.0 while preserving execution evidence and explicitly enabling new capabilities.
---

**Ledgence 0.8.0 is available.** Use its verified release artifacts and matching
components. Rehearse the migration on a restored copy before upgrading production.
[Distributions and checksums](/reference/releases).

The upgrade preserves ordinary tasks, workflows and immutable program identities.
It adds retained effect evidence, optional credentials and audit, native operator
waits and typed provider recovery. Mixed old and new database writers are not a
supported upgrade mode.

## Choose an installation strategy

A separate local installation is the easiest place to evaluate 0.8.0.
Use a new directory and unused port. It has a new database and program store.
Updating the CLI leaves an existing `ledgence local` installation on its saved
kit and image. Reusing its directory starts that saved configuration.

To retain existing data, rehearse the maintenance process below. For a CLI-managed
stack, use a tested restore into a replacement deployment with the original instance
identity. Keep checksummed kit files intact.
[Local installation lifecycle](/how-to/run-local-distribution).

## Rehearse and preserve a recovery set

Record the database, immutable program store, instance ID and scope, service
arguments, queues, delivery routes, completion destinations and component versions.
For Docker, include the context, Compose project and actual volume names.
Retain the old bundles and configuration.

Restore a copy in an isolated environment. Keep its workers stopped until external
endpoints are isolated for testing. A copied workflow can still contain production
destinations. Preserve published program artifacts and their original digests.
Use the selected release's explicit executable, Console and runtime helper paths.

## Stop all writers

Pause ingress and scheduled retention. Drain and stop workers while their API is
available, then stop every orchestrator, publisher, coordinator and other database
writer. Keep PostgreSQL available. Back up the stopped database, program store and
configuration together and preserve the external queues.

A first SIGINT/SIGTERM requests normal drain. A second signal forces exit and can
leave uncertain operations to reconcile. A provider's accepted write is not rolled
back by stopping a worker or restoring the database.

## Apply the complete migration

From the extracted 0.8.0 bundle, use its executable. Configure `DATABASE_URL`
through your existing deployment secret mechanism to select the retained database.
`LEDGENCE_POSTGRES_URL` belongs to test harnesses and is not the service setting.

```sh
./bin/ledgence --version
./bin/ledgence orchestrator migrate
```

The selected executable must report `ledgence 0.8.0`. Migration defaults to ten
minutes after connection. For a thirty-minute maintenance budget, use
`./bin/ledgence orchestrator migrate --timeout-ms 1800000`.

For direct Compose, run the new image's `migrate` service with writers stopped.
Verify the image, project and volume bindings first. A different project name may
create an empty database instead of upgrading the original one.

The command applies all embedded migrations and required backfills. Do not execute
a subset of SQL files manually. Retained evidence backfill commits batches of at
most 16 approvals and preserves original accepted bytes. Earlier completed steps
survive an interruption. Keep writers stopped, correct the reported error and rerun
the same command. Do not bypass the migration ledger or backfill checks.

Historical approved actions are conservatively lookup-only when adopted into
protected execution. A still-pending approval may authorize its first protected
dispatch after approval. Migration does not invent provider facts or verified actors.
Missing historical proof or cleanup that already deleted unresolved evidence may
require restoring the backup. Deleted records cannot be reconstructed.
[Detailed migrations and rehearsal](https://github.com/Ledgence/ledgence/blob/develop/docs/postgres.md#upgrade-from-070-to-080).

## Restart and verify matching components

Start 0.8.0 servers against the same database, program store, instance binding
and delivery configuration. Restart embedded connection pools. Then start matching
workers and their runtime helper. Use Console contract **7** assets and refresh
old browser tabs. Replacing the CLI alone does not replace served Console assets.

`serve` verifies the exact schema and completed backfills before listening and
starting coordinators. It does not migrate. Resolve startup errors rather than
pointing an old binary at the upgraded schema.

Check `/health/ready` on every serving instance. Inspect a known task, workflow,
approval and accepted result against your pre-upgrade record, then execute a small
ordinary task before reopening ingress. Old decisions must retain their original
bytes and must not acquire a fabricated verified actor.

## Keep ordinary setup or enable new capabilities

Without `--access-config`, an installation that has never enabled credentials keeps
its operator-trusted model and existing network access boundary. Without
`--provider-config`, provider recovery and its scheduler remain disabled.
The packaged local kit keeps this ordinary setup.

To enable credentials, follow [Configure instance access](/how-to/configure-instance-access)
using the original instance ID and scope. Provision private credential files with
`ledgence access init`, then restart every replica with the same `--access-config`.
Supply operator or worker `--credentials-file` values to the appropriate clients.
Once enabled, the authority cannot be omitted or replaced on a later startup.
Use `access issue` to rotate credentials while preserving the original authority.
Existing access files do not gain new capabilities automatically.

Provider recovery additionally needs a matching external policy file, explicit
`--provider-config`, new program code and a dedicated reconciliation queue consumed
by compatible workers. For SQS-compatible delivery, both logical queues need their
own configured publisher and matching workers. The
[provider guide](/how-to/recover-provider-operations) explains the contract and
links the synthetic example. Missing policy is not restore quarantine.

Install matching client and authoring packages for the deployment. For 0.8.0,
use `ledgence-client==0.8.0` and `ledgence-worker==0.8.0`.
Recompile custom Rust adapters against the matching contracts and expose a new
capability only when its complete store and service ports are implemented.

## Plan retention and rollback

Independent effect evidence, key reservations and acceptance events have no automatic
expiry. Include them in backups and capacity planning. Unresolved effects can hold
execution history beyond its normal age. Confirmation, an assessment or terminal
typed provider knowledge can release the relevant hold without deleting evidence.
[Effect handling](/how-to/inspect-effects) · [Operator audit](/how-to/audit-operators).

Rollback requires stopping new writers and restoring the tested database, program
store and configuration before using old binaries. Replacing an executable does
not reverse schema changes. Reconcile external writes made since the backup before
resuming work.

For a restored 0.8 database with provider operations, configure
`declared_restore_quarantine` outside the database before restarting. It denies new
mutation grants while allowing read-only recovery of known operations. Apply it
consistently to all replicas. Ledgence cannot detect an undisclosed old snapshot,
and old 0.7 binaries cannot enforce this policy.
