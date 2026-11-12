---
title: Prepare an upgrade from 0.9 to 0.10
description: Rehearse the 0.10 candidate migration while preserving execution files, receipts, instance identity and external effects.
---

**Candidate source, not yet a verified public distribution.** The verified release
remains 0.9.0. Use matching 0.10.1 candidate components for this rehearsal; do not
substitute a source version bump for verified native bundles, packages, images or
a local kit. The [candidate release notes](https://github.com/Ledgence/ledgence/blob/develop/docs/releases/0.10.1.md)
describe lifecycle progress, execution files, optional framework adapters and the
expanded program-package profile.

This guide's retained-data rehearsal starts from 0.9.0. If your installation is
older, also review the earlier [migration guidance](/reference/releases#historical-releases)
and rehearse with a restored copy of your actual starting version. The
0.9-to-0.10 rehearsal does not establish compatibility for other upgrade paths.

## Record the installation and recovery set

Keep the verified 0.9.0 bundle and record the candidate's exact source commit and
archive checksum. Save the original instance, scope, credential authority and
explicit capabilities, provider/admission policies, program digests, queue routes
and completion destinations. Keep protected configuration in its normal secure
location. Updating a CLI does not upgrade an existing saved local installation:
its kit, image, project identity, volume layout and settings remain unchanged.

Plan a coordinated maintenance window. Rehearse against an isolated copy and a
synthetic provider. External provider operations, broker messages and completion
receipts exist independently of PostgreSQL; retain their original identities and
evidence so restoration cannot be mistaken for proof that a write never occurred.

## Stop writers and take a consistent backup

Pause submissions, retention maintenance and independent queue consumers. Drain
and stop workers while the API remains available, then stop every orchestrator,
publisher, coordinator and other database writer. Use your tested PostgreSQL
backup procedure; copying a live database volume is not consistent. Preserve the
matching immutable program store, configuration and any configured execution-file
store. Worker caches and artifact spools are transient and are not recovery copies.

Published 0.9.0 has no execution-file store. New candidate local kits configure
filesystem storage in `execution-artifacts`, separate from `programs` and the
transient `artifact-spool` volume. If rehearsing or restoring a candidate that
already accepted files, copy the **complete stopped store**, including hidden
`.ledgence-artifact-store`, with the matching database and unchanged configured
`store_id`.

Restore that copy into an empty target **before any server or retention process
first opens it**. Opening an empty target generates a new marker which the
restored catalog rejects. The filesystem directory path may change; marker bytes
and `store_id` must not. For S3, endpoint, region, bucket and prefix bind the store
identity. Copying objects to another bucket or prefix is not a transparent
restore. See [execution-file storage](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-artifacts.md#retention-and-backup).

## Apply the migration and reconcile the ledger

The new migration is `20261110000000_execution_artifacts.sql`. It adds the store
binding, immutable artifact catalog, upload generations and durable cleanup
evidence. Allow the rehearsed maintenance window for DDL and locks; this is an
offline upgrade. From the verified candidate bundle, set `DATABASE_URL` through
the deployment's secret mechanism to select the stopped installation's database:

```sh
./bin/ledgence --version
./bin/ledgence orchestrator migrate
./bin/ledgence orchestrator migrate
```

Expect `ledgence 0.10.1`. The second migration run verifies an already completed
ledger without reapplying changes. If the bounded lock/statement budget is too
short for the rehearsed operation, use an explicit larger `--timeout-ms`, for
example `1800000`. After interruption or timeout, rerun the same migrator and
reconcile its recorded versions and checksums before starting services. Do not
edit the ledger to hide a failure. The previous binary rejects the new schema.

Start the candidate with the original instance, authority and scope, matching
Console contract **16** assets, worker binaries, bundled helpers and Python SDKs.
Inspect previously accepted tasks, local results, owned forks, callbacks, queue
and resource state. Repeat saved idempotent submissions or approval decisions
using their original identities and confirm their original receipts. Start a
progress observer on a preserved execution, save its cursor and reconnect with
that cursor; retention errors must remain visible.

## Enable files and framework profiles deliberately

Execution files are optional. Pass a strict `--artifact-config` separately from
the program store's `--store` configuration and retain it for maintenance. The
filesystem backend supports one orchestrator with persistent local storage.
The optional `s3-artifacts` build has a local RustFS qualification profile; it
does not establish AWS qualification. Verify accepted metadata and downloaded
digests after restart and after an artifact-bearing backup restore.

Existing credentials keep their explicit capabilities. File upload and recovery
need `artifact_write`, metadata reads need `metadata_read`, and downloads need
`sensitive_read`. Use `ledgence access issue --config` with the **existing authority
configuration**, then deploy its replacement verifier configuration to every
replica. Do not run `access init` as an upgrade step: it creates a new authority.
[Issue and rotate credentials](https://github.com/Ledgence/ledgence/blob/develop/docs/instance-access.md#add-and-rotate-credentials).

The three [framework profiles](/how-to/agent-frameworks) require their exact
optional SDK versions and matching runtime helper. PydanticAI and Google ADK use
bounded operation replay; Claude Agent SDK uses accepted whole-run result replay.
These profiles do not imply live-provider, native-CLI or session-restore
qualification. Applications own their providers and credentials.

Upgrade **every eligible worker** before publishing packages using the negotiated
8,192-entry profile. Legacy 4,096-entry negotiation remains available. Existing
compatible immutable packages need no forced rebuild; retain their digests and
respect platform, runtime and exact CPython major/minor requirements. The
[program publication contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-publication.md)
defines rejection before PUT when a peer supports only legacy limits.

Use each kit's configured shutdown grace. New candidate kits allow 65 seconds for
the worker and 155 seconds for the orchestrator; `down` must not apply a blanket
timeout. Old saved kits retain their original configuration. Do not overwrite
saved kits or delete volumes to work around an upgrade error.

## Restore or roll back without repeating external effects

Roll back into a **fresh isolated database** restored from the original 0.9.0
backup, with matching old binaries, helper, Console, program store and
configuration. Do not run the old binary on the migrated database or invent down
migrations. Candidate artifact recovery instead needs its matching candidate
database and full byte store with the preserved identity described above.

If external work occurred after the backup, keep workers stopped and reconcile
the independent provider/account/business keys, callbacks, broker state and
trusted receipts. On every replica, use admission `dispatch: "quarantined"` and
provider `dispatch: "declared_restore_quarantine"` before resuming governed
dispatch. These declarations live outside the restored database. Read-only
recovery can establish an existing outcome; a missing restored row does not
authorize a new write. Direct application network calls and completion receivers
also require reconciliation. Keep writes quarantined while safe disposition is
unknown. [Restore and governed dispatch](/how-to/upgrade-to-0-9#if-you-restore-a-backup-after-external-work-occurred).

## Reproduce the native upgrade rehearsal

The source harness uses the actual verified 0.9.0 bundle and selected candidate
binary, helper and Console. Check archive inventories before extraction. Supply a
disposable PostgreSQL 18 parent server and matching `psql`, `pg_dump` and
`pg_restore` clients; the gate creates and cleans up its own temporary databases.
From the candidate source checkout, substitute the real extracted bundle paths:

```sh
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' \
  python3 tools/check-artifact-upgrade.py \
    --previous-bundle /path/to/verified-0.9-bundle \
    --binaries /path/to/selected-0.10-candidate/bin \
    --console-dist /path/to/selected-0.10-candidate/console \
    --candidate-runner /path/to/selected-0.10-candidate/runtime/ledgence/worker/bootstrap.py \
    --evidence /tmp/ledgence-upgrade-0-10
```

Require the candidate's passing receipt for interrupted DDL rollback, two
migration runs, old-binary rejection, preserved predecessor rows and receipts,
fresh database restore, and accepted artifact bytes after restart and a separate
artifact-bearing restore. Check the recorded source and executable identities,
store identity and cleanup result. The guide is a procedure, not evidence that
those gates have passed for a particular candidate. Publication and public
documentation deployment require their separate verification.
