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

`down --timeout 65` preserves named volumes. PostgreSQL and the program store
hold durable data; cache can be recreated. Never use `down --volumes` to fix a
startup problem. Back up data and stop writers before applying an explicitly
planned version/schema upgrade. The migrator runs before the server; the server
only verifies its schema. This kit does not perform implicit upgrades or offer
automatic rollback of migrated data.

The host API binds to loopback. Fixed credentials and internal HTTP belong to
this local operator-trusted deployment. Images and included third-party software
retain their own license terms and notices under `/opt/ledgence/legal` and the
base image's `/usr/share/doc` and `/usr/share/common-licenses`.

Public image releases also provide the matching `corresponding-source.tar.gz`,
`source-manifest.json` and checksums for their included Debian and CPython runtime
components. Those sources and original notices retain their upstream licenses;
they are separate from Ledgence's MIT source and from your application programs.
The image inventory identifies the exact installed binary/source versions.
