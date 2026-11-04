---
title: Upgrade a self-hosted installation from 0.8 to 0.9
description: Preserve existing executions, receipts and external effects while introducing shared resources and run budgets.
---

Use the [verified 0.9.0 distributions](/reference/releases) and a coordinated maintenance window. Keep the 0.8 installation, its verified native bundle and a recoverable copy of its state until you finish the checks below. A fresh local stack is useful for a tutorial; upgrading a production installation requires preserving its database, program artifacts, queues and instance identity.

## Prepare the recovery set

Record the exact orchestrator, worker, Console, Python runtime and client versions. Save the instance and access configuration, provider policy, program store location and digests, broker routes and completion destinations. Keep security-sensitive configuration in its normal protected location.

Rehearse on an isolated copy with a synthetic provider. A restored task can still contain an external destination. The original provider, broker and completion receiver exist independently of the database. Preserve their receipts and your ability to look up an operation by its original identity.

## Stop writers and back up

Pause new submissions, scheduled retention and independent consumers of the same queues. Drain and stop workers while the API is still available. Then stop all orchestrators, publishers, coordinators and other database writers. Capture the database and matching program/configuration state using your tested backup procedure. Keep broker messages and external receipts.

Size the window on a representative restore. The 0.9 migrations add resource, task admission, queue-policy and budget tables and build indexes on existing task/attempt paths. Index creation can block writes. An accepted provider mutation may still finish during this window.

## Apply the migrations and verify the original work

Use the new version's executable from the extracted 0.9 bundle. Configure `DATABASE_URL` through the deployment's secret mechanism so the command selects the existing database. From that bundle, run:

```sh
./bin/ledgence --version
./bin/ledgence orchestrator migrate
```

Expect `ledgence 0.9.0` from the first command. The migrator uses a bounded lock/statement budget. For a longer rehearsed window, for example, run `./bin/ledgence orchestrator migrate --timeout-ms 1800000`. If interrupted or timed out, reconcile the migration ledger by running the same command again. A completed migration stays recorded with its checksum.

Start all 0.9 orchestrators with the original instance, scope and authority, and the matching 0.9 Console assets. Use the matching runtime helper and worker binaries before enabling new controls. Inspect a previously submitted task, a pending approval, a completed provider operation and an unresolved operation. Repeat a known idempotent submission or a saved approval decision with its original arguments. Check for the original ID, receipt and action before resuming ingress. A retry with a new ID is a new request.

Existing runs have no implicit run budget. A standalone task or workflow root opts in at submission. Use an operator credential explicitly authorized for `resource_control` to configure resource or queue policies; existing credentials keep their original permissions. Upgrade every orchestrator before creating budgeted roots. An old worker may still run ordinary compatible programs, but budgeted provider and approved-effect grants require a worker that can enforce the send deadline. [Shared run budgets](/how-to/share-run-budgets) · [Resource admission](/how-to/control-resource-capacity)

## If you restore a backup after external work occurred

An offline backup taken immediately before migration differs from restoring that same backup after a provider has accepted new work. In the second case, the provider effect can still exist even though the restored database lacks its run, budget membership, idempotency key, or dispatch receipt.

Restore into a fresh isolated database following your restoration procedure. Keep workers and other writers stopped. Apply the target version's migrations. Set `dispatch` to `quarantined` in the admission configuration and to `declared_restore_quarantine` in the provider configuration on **every** replica before restarting governed dispatch. Both settings are external to the restored database; missing configuration does not declare a restore. Take particular care with ordinary application code, completion receivers and direct network requests outside these controls.

Review unknown reservations and compare original provider/account/business keys and trusted external receipts. Read-only recovery can record the outcome of an already issued provider operation while fresh governed sends remain fenced. Reconcile outstanding usage and absent evidence before re-enabling the installation. Keep the scope quarantined if the safe disposition is unknown. Restoring the 0.8 database to roll back a deployment requires the same external reconciliation.

## Reproduce the native upgrade rehearsal

The source checkout includes a check using the original 0.8 binary/helper and a candidate 0.9 binary/helper. Verify both release archive checksums before extracting. Use an explicitly disposable PostgreSQL 18 parent server with matching `psql`, `pg_dump` and `pg_restore` clients. The check creates two temporary databases on that server. It preserves an independent synthetic provider across a real dump/restore, and removes its test databases and temporary credentials afterward.

From the source checkout, substitute your extracted bundle paths. `LEDGENCE_POSTGRES_URL` is a test-harness setting identifying the disposable parent, independent of production service configuration.

```sh
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' \
  python3 tools/check-resource-upgrade.py \
    --previous-bundle /path/to/verified-0.8-bundle \
    --binaries /path/to/verified-0.9-bundle/bin \
    --console-dist /path/to/verified-0.9-bundle/console \
    --candidate-runner /path/to/verified-0.9-bundle/runtime/ledgence/worker/bootstrap.py \
    --evidence /tmp/ledgence-upgrade-0-9
```

Read `summary.json` in the new evidence directory. It records predecessor and candidate provenance, applied migrations, preserved receipt digests, the outcome of a budgeted provider operation and the provider's actual POST count after restore. The task and approval replay checks use the original request identities.
