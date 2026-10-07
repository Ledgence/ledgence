---
title: Build and publish Python programs
description: Prepare explicit application inputs for a worker runtime, upload immutable bytes, and recover interrupted publication without rebuilding.
---

**Available in Ledgence 0.5.0.** Use matching CLI and orchestrator components
and explicitly enable the server's publication writer. Earlier saved local kits
retain their image and configuration; a new CLI does not upgrade those services.
[Filesystem publication and separate registration](/how-to/register-agent)
remain available.

Prepare a program for the worker's runtime, upload it to that installation, and
optionally register it in Console. Building, publishing and registering do not
execute the program. To author workflows with local imports and editor support,
start with [Develop Python programs](/how-to/develop-python-programs).

## Before you start

You need:

- The 0.5.0 `ledgence` CLI and local Docker running Linux containers.
- The worker's explicit platform, exact CPython major/minor and digest-pinned
  runtime image. A Mac's architecture or a server URL cannot select these for you.
- A matching 0.5.0 server with `--allow-program-publication`, an instance configuration
  and a writable filesystem program store. An HTTPS read-only store cannot
  enable this writer.

The 0.5.0 Compose templates enable the server's writer and leave the
worker's program mount read-only. An already saved kit keeps its original image,
settings and data; installing a newer CLI does not upgrade it. Use a separate
state directory with a matching 0.5.0 kit for evaluation. Do not edit
copied kit files to bypass their checksum checks. See
[local distribution operations](/how-to/run-local-distribution).

On separate hosts, workers must still access the same published artifacts
through a shared filesystem or configured HTTPS reader. Uploading to an
orchestrator does not configure remote storage or workers. Use HTTPS and an
operator-controlled access boundary remotely; a digest is integrity data, not
publisher authentication.

## Describe the application

The [program publication example](https://github.com/Ledgence/ledgence/blob/develop/examples/program-publication/README.md)
uses an HTML report workflow with a small native dependency. Copy its project
and edit its runtime image and target to match your worker. Its example image
placeholder must be replaced with the worker's actual digest. For the local
distribution, `ledgence local status` reports the saved image; `distribution.json`
in the saved installation records the same digest. Installing a newer CLI does
not change that saved image.

A minimal configuration for `src/invoice.py` looks like this. Replace
`REPLACE_WITH_WORKER_RUNTIME_DIGEST` with the actual 64-character lowercase image
digest. Values are literal TOML; shell variables are not expanded.

```toml
schema_version = 1

[program]
id = "invoice-issuer"
version = "1.0.0"
handler = "invoice:handle"
kind = "task"

[source]
root = "src"
include = ["invoice.py"]

[target]
platform = "linux/amd64"
python = "3.14"
protocol = 3
image = "ledgence/ledgence@sha256:REPLACE_WITH_WORKER_RUNTIME_DIGEST"
```

Save this as `ledgence.toml` in the project root. For example, the handler can be:

```python
def handle(event):
    return {"invoice_id": event["data"]["invoice_id"], "accepted": True}
```

`source.root` is relative to the configuration directory. `source.include` lists
explicit files or directories relative to that root; it does not accept glob
patterns. Only selected inputs are copied. Hidden files, environments, caches,
credential/secrets paths and bytecode are excluded; explicit excluded selections,
symlinks, traversal and destination collisions fail the build. Keep secrets
outside the application selections; exclusions do not scan file contents.

For additional regular files and production dependencies, append:

```toml
[[files]]
source = "LICENSE"
destination = "LICENSE"

[dependencies]
requirements = "requirements.txt"
```

These source paths are relative to the configuration file. The requirements must
pin every active dependency and transitive dependency with `==` and SHA-256
hashes. The adapter installs compatible wheels from public PyPI in a fresh target
directory. It neither compiles sdists nor rewrites the project's lock file.
Missing wheels, missing transitive pins or incorrect hashes fail explicitly.
Retain dependency notices. Omit `[dependencies]` for a standard-library-only
program; keep `ledgence-worker` as a development dependency because the worker
supplies its own runtime helper.

The initial Docker adapter supports `linux/amd64` and `linux/arm64`, normalized
to manifest `x86_64` and `aarch64`. The runtime inside Docker must match the
requested platform and Python version. A different image with similar labels
can have incompatible native libraries; use the worker's image. Source/URL
requirements, private indexes and remote Docker contexts are outside this
adapter's scope. You can still publish a package prepared by another pipeline.

## Build once

Run from the project directory:

```sh
ledgence program build
```

The defaults are `--config ledgence.toml` and `--output .ledgence/prepared`.
A relative output is resolved from the configuration directory. Use a fresh
output name for another build; existing output or receipt files are never
overwritten. Optional flags are `--context NAME` and `--timeout-seconds N`
(default 600, range 1–3600). A cancelled or expired build waits for its owned
cleanup and running filesystem verification before returning; this can extend
elapsed time beyond the deadline. The selected Docker context must be local; the CLI
never changes the global context. If `DOCKER_HOST` is set, select a named local
context explicitly.

The build verifies the generated manifest and ZIP without importing the handler.
It prints JSON with the prepared directory, descriptor and adjacent build
receipt. The receipt records input and prepared-file hashes and the runtime
image outside the package. Packaging the same prepared files and permissions
produces the same ZIP; this does not promise that every dependency build pipeline
is universally reproducible.

## Upload and register

With a compatible server running:

```sh
ledgence program publish --server http://127.0.0.1:8080 --register
```

This uses `.ledgence/prepared`, packages it once and retains the exact ZIP before
uploading. It does not rebuild dependencies. The matching build receipt provides
`kind`, display name and description; explicit `--kind`, `--display-name` and
`--description` override those values. Without `--register`, publication leaves
the catalog unchanged.

The server recomputes the digest and size, checks every ZIP member and verifies
that the manifest matches the requested program identity. It exposes the
immutable descriptor only after the complete verified blob exists. The CLI
checks the response against its retained ZIP before registering, then verifies
that the catalog reference resolves to that exact descriptor.

Open **Programs** in Console after successful registration. Execution remains a
separate task or workflow submission. A registered program can target another
platform; a worker still needs to satisfy its runtime requirements.

The filesystem alternative remains:

```sh
ledgence program publish --source .ledgence/prepared --store /path/to/program-store
```

`--store` and `--server` cannot be combined. `--store` keeps its original
descriptor-only JSON and rejects `--register`; register separately as in
[Register an agent](/how-to/register-agent). A host directory is not automatically
a Docker volume.

## Resume after interruption

The CLI prints the publication receipt path on stderr and includes it in its
JSON report. The report also carries the last available HTTP `request_id`,
a structured `error`, and `publication_outcome` (`confirmed`, `unknown`, or
`unconfirmed`). Upload confirmation and catalog registration remain distinct. By default, the receipt and exact `artifact.zip` live under
`.ledgence/publications/publication-…`. Keep both files.

```sh
ledgence program publish --resume /absolute/path/to/receipt.json
```

Resume uses the recorded server and metadata, verifies the saved ZIP and retries
only unconfirmed stages. It works after the original project or prepared
directory changes; it does not run another build. Do not combine `--resume` with
other flags. A changed or missing retained ZIP is rejected.

An HTTP timeout is an unknown outcome: persistence may still finish. HTTP
publication makes one PUT without automatic retries. Resend the retained bytes
to reconcile. If upload succeeded and registration failed, the command returns a
nonzero exit status with phase `published`, the descriptor and a
`register_command` argument array. Resume retries registration only. Never delete
a shared artifact as rollback; filesystem publication and catalog registration
are separate operations.

Use a new program version when code, dependencies, permissions or target change.
The store identifies a program globally by ID and exact version, regardless of
catalog tenant/namespace. One version has one immutable artifact; there is no
multiarchitecture selection beneath that identity. Repeating identical bytes is
idempotent; different bytes return an immutable conflict.

## Limits and operating scope

The default package limits are 64 MiB compressed, 256 MiB expanded, 64 MiB per
file, 4,096 entries, a 64 KiB manifest and a 16 KiB descriptor. The service admits
two uploads at a time before reading their bodies, with a 120-second transfer
and persistence budget. A timeout cannot release capacity while its admitted
write is still running. JSON control operations keep their existing deadline.

`GET /v1/programs/publication-capabilities` reports whether uploads and catalog
registration are enabled, their limits and immutable mode. An older server is
reported as incompatible; use `--store` or an explicit server upgrade.

The [full configuration, binary API, error and recovery contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-publication.md)
describes typed failures, descriptor binding, temporary files and adapter
requirements. Docker belongs to the local builder; accepting a prepared ZIP and
running it need no Docker installation or vendor account on the server itself.
