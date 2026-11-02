---
title: Upgrade to 0.7.0
description: Plan a coordinated upgrade from Ledgence 0.6.1, migrate PostgreSQL, and verify retained workflows and approvals.
---

Ledgence 0.7.0 adds an [approval inbox](/how-to/require-approval#find-pending-requests-across-workflows)
and an [inspector for accepted workflow operations](/reference/console#inspect-an-accepted-operation).
The upgrade from 0.6.1 requires two PostgreSQL migrations. Console uses
contract **5**, and Python workflows use runtime protocol **3**. If you are
starting from an older version, also consult the [previous upgrade guides](/reference/releases#historical-releases).

## Install the CLI

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.7.0/install.sh | sh -s -- --version 0.7.0
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

Expect `ledgence 0.7.0`. The second line makes the command available in the
current Bash or Zsh session. See [manual downloads](/how-to/manual-native-installation)
for supported native platforms and checksum instructions.

## Start a new local installation

Choose [one installation method](/getting-started/installation). With the CLI
and Docker Compose, you can try the release alongside an older installation by
selecting an unused directory and port:

```sh
ledgence local up --directory "$HOME/.local/share/ledgence/local-0.7.0" --port 8087
ledgence local status --directory "$HOME/.local/share/ledgence/local-0.7.0"
```

This starts a new database and program store. Open the Console URL printed
by the CLI. To stop this stack and keep its data, run
`ledgence local down --directory "$HOME/.local/share/ledgence/local-0.7.0"`.
For direct Compose, follow [Install with Docker Compose](/how-to/install-compose)
in a new directory and use a separate project for a separate installation.

## Upgrade a deployment with existing data

Updating the CLI leaves a saved local installation on its original kit and
image. Reusing its directory with `ledgence local up` starts that saved
configuration. Plan a coordinated maintenance window to migrate a deployment
whose data you want to retain.

1. **Rehearse on a restored copy.** Record the database, program store,
   immutable instance configuration, queue and delivery settings, service
   options and existing component versions. For Compose, record the Docker
   context, project name and actual volume names. Restore the database and
   program store in a separate environment, then try the steps below there.
   Allow time for the approval backfill based on your retained history.
2. **Quiesce the deployment and take a final backup.** Pause ingress, drain
   and stop workers, stop all orchestrators and other writers, then back up
   PostgreSQL, the immutable program store and the instance configuration
   together. Preserve the previous bundles, kit and configuration for recovery.
   Keep any external queue and its settings.
3. **Migrate the retained database once.** With the 0.7.0 CLI and the service's
   existing `DATABASE_URL` configured, run:

   ```sh
   ledgence orchestrator migrate
   ```

   For direct Compose with the 0.7.0 kit, the equivalent is its `migrate`
   service. Confirm the Docker context, project name, database volume and
   instance configuration still identify the original deployment, then run
   the migration with the other services stopped. For example, if you originally
   used the project name `ledgence-local` and a file named `compose.yaml`:

   ```sh
   docker compose --project-name ledgence-local --file compose.yaml run --rm migrate
   ```

   The first migration, `20261102000000_workflow_resolution_progress.sql`,
   adds a checkpoint for incomplete child-program resolution. The second,
   `20261102010000_approval_inbox.sql`, backfills routing fields from retained
   approval records and creates indexes for inbox queries. Writes can be
   locked during this work. If migration is interrupted, keep old writers
   stopped, inspect the error, and rerun the same migration command after
   correcting it. Server startup checks the schema.
4. **Start matching services.** Use 0.7.0 orchestrators, workers, their runtime
   helper and Console assets. Point them at the same migrated database and
   program store, with the same instance configuration, queues and delivery
   settings. For direct Compose, start the stack with the same project and
   settings used for the migration. For a CLI-managed installation, use a tested backup/restore plan
   to a replacement deployment with the original instance identity. Keep
   checksummed kit files intact and retain the old volumes until verification.
5. **Verify and reopen ingress.** Check API readiness, open a known workflow
   and its retained results, then run a small task. If you have a pending
   request, find it in **Approvals** and compare its workflow, request key and
   effective arguments to your pre-upgrade record. Open an accepted local
   operation in **Graph** or **Trace** to check its recorded result.

If you must roll back, stop the 0.7.0 services and restore your tested
pre-upgrade database, store and configuration before starting the previous
version. Replacing executables alone does not reverse these schema changes.

## Match application dependencies

For applications using the client and workflow helpers:

```sh
uv add "ledgence-client==0.7.0"
uv add --dev "ledgence-worker==0.7.0"
```

For projects scaffolded by the 0.7.0 CLI, run `uv sync` to install the pinned
development group. Recompile custom adapters against the matching Rust API
crates and test the optional inbox and local-step inspection operations your
deployment exposes. Published program versions and their immutable artifacts
remain independently identified. [Release distributions](/reference/releases).
