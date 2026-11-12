# Ledgence local distribution

This directory is a versioned installation kit. `distribution.json` names its
runtime image by immutable registry digest. Docker Engine or Docker Desktop
must run Linux containers with Compose 2.23.1 or newer. No Git checkout, Rust,
Node, or host Python installation is needed to run the stack.

The `ledgence local` commands manage the copied kit and persistent project
identity. For direct Compose use, choose and keep an explicit project name:

```sh
docker compose --project-name ledgence-local --file compose.yaml pull
docker compose --project-name ledgence-local --file compose.yaml up --no-build --detach --wait --wait-timeout 120
```

Open `http://127.0.0.1:8080/console/`. Set `LEDGENCE_PORT` before invoking Compose
to select another host port. `LEDGENCE_CONCURRENCY` controls worker consumers and
its subprocess pool (default 1). This kit runs one worker; separate workers need
separate cache directories. Scope and queue are `acme/demo` and `demo`.

## Publish your own program

This kit enables `--allow-program-publication` on the orchestrator
and gives that service write access to the shared program volume. The worker
continues to mount `/programs` read-only. With matching CLI/server
components and an explicit `ledgence.toml` targeting the same runtime image:

```sh
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
```

Building requires local Docker; uploading a prepared package does not. Programs
are still identified globally within this store by ID and exact version, with
one immutable artifact per version. Keep the publication receipt and its ZIP for
`ledgence program publish --resume RECEIPT.json` after an uncertain result.
Publication and registration do not execute the program.

This capability does not change previously saved kits. An existing CLI-managed
installation keeps its verified copied kit, image,
project identity and volumes. A newer CLI does not migrate that installation.
Use a separate directory with a matching qualified kit to evaluate the
feature; do not overwrite its saved files or bypass checksum validation.
See the [program publication guide](https://github.com/Ledgence/ledgence/blob/develop/docs/program-publication.md)
for the configuration, dependency requirements, limits and recovery contract.

## Optional examples

For optional example programs, workflows and completion callbacks, include both
files consistently when operating the project:

```sh
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml up --no-build --detach --wait --wait-timeout 120
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml run --rm --no-deps publish
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml run --rm --no-deps demo
```

The example checks process reuse and requires concurrency 1. It prepares programs
inside the worker-compatible image, publishes them dynamically and registers
them. Repeating publication of identical bytes is safe. Changed code requires a
new program version. The optional receiver is a bounded demonstration, not a
production webhook service.

Programs must target the worker's Linux architecture and exact CPython 3.14
major/minor. Vendor dependencies for that environment, including required
native libraries. Image multiarchitecture support does not make program ZIPs
portable or select compatible workers automatically.

## Preserve and restore data

`down` preserves named volumes and lets Compose use each service's configured
grace period: 65 seconds for the worker and 155 seconds for the orchestrator in
this development kit. Older saved kits keep their own configuration. A newer CLI
does not rewrite it; do not force one timeout over all services.

New development kits configure filesystem execution storage in the durable
`execution-artifacts` volume. Published 0.9.0 and older saved kits do not gain this
capability by updating the CLI. PostgreSQL, immutable programs, execution files
and saved configuration form a recovery set. Worker cache and `artifact-spool`
are transient and are not recovery copies.

Stop writers and retention maintenance before backing up. Use PostgreSQL backup
tools; copying a live database volume is not consistent. Copy the complete artifact
store, including hidden `.ledgence-artifact-store`, and retain the configured
`store_id`. Restore it to an empty target before a server or retention process
first opens the store. Opening an empty target creates a different marker that
the restored database rejects. The path may change; the marker bytes and
`store_id` must not. Verify file metadata and downloaded digests against the
matching restored database before resuming writers. S3 storage has different
identity constraints; see the [artifact recovery contract](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-artifacts.md#retention-and-backup).

Never use `down --volumes` to fix a startup problem: it deletes this project's
database, programs, execution files, cache and spool (and receiver state when the
examples override is used). Plan and rehearse each version/schema upgrade. The
migrator runs before the server; the server only verifies its schema. This kit
does not perform implicit upgrades or offer automatic rollback of migrated data.

The host API binds to loopback. Fixed credentials and internal HTTP belong to
this local operator-trusted deployment. Images and included third-party software
retain their own license terms and notices under `/opt/ledgence/legal` and the
base image's `/usr/share/doc` and `/usr/share/common-licenses`.

Public image releases also provide the matching `corresponding-source.tar.gz`,
`source-manifest.json` and checksums for their included Debian and CPython runtime
components. Those sources and original notices retain their upstream licenses;
they are separate from Ledgence's MIT source and from your application programs.
The image inventory identifies the exact installed binary/source versions.

During image qualification, an unchanged runtime can reuse the corresponding sources from a published release of the same architecture. The workflow pins the archive SHA-256 and the exact runtime inventory. The collector revalidates every source file and rebuilds the manifest for the current image. When the inventory changes or the release asset cannot be downloaded, it collects the exact upstream sources. An archive that fails integrity checks blocks qualification.
