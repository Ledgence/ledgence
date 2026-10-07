# Build and publish Python programs

These commands require matching CLI and orchestrator components with program
publication support. The server writer must be enabled explicitly; the CLI
checks capability discovery before uploading. Earlier saved local kits keep
their original image and configuration. Existing filesystem publication and
separate registration remain available. For verified release availability, see
[the release reference](https://docs.ledgence.com/reference/releases).

A program moves through three distinct operations:

1. **Build** prepares selected application files and dependencies for an explicit
   worker runtime. Docker is a local CLI adapter; it does not run on the server.
2. **Publish** verifies and stores an immutable ZIP, identified by program ID,
   exact version and SHA-256. It does not execute the handler.
3. **Register** records the verified reference and descriptive metadata in the
   installation's existing catalog. It does not install or execute the program.

The [HTML report example](../examples/program-publication/README.md) provides a
workflow and reviewed native wheel requirements for this path.

From the directory containing `ledgence.toml`:

```sh
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
```

The default build output is `.ledgence/prepared`; publication selects that
prepared directory and never builds implicitly. A configured kind and display
metadata are read from its matching build receipt. For an already prepared
package, continue to use `program publish --source DIR --store DIR`, or use
`--server URL` for a compatible server. Docker is not needed to publish an
already prepared package.

## Explicit build configuration

`ledgence.toml` is a versioned build configuration, separate from the generated
runtime manifest `ledgence-program.json`. The following example assumes
`src/invoice.py`, `LICENSE`, and `requirements.txt` exist. Replace the image
placeholder with the **same digest-pinned runtime image used by your worker**;
TOML values are literal and do not expand shell variables.

```toml
schema_version = 1

[program]
id = "invoice-issuer"
version = "1.0.0"
handler = "invoice:handle"
kind = "task"
display_name = "Invoice issuer"
description = "Issue an invoice from an approved request."

[source]
root = "src"
include = ["invoice.py"]

[[files]]
source = "LICENSE"
destination = "LICENSE"

[dependencies]
requirements = "requirements.txt"

[target]
platform = "linux/amd64"
python = "3.14"
protocol = 3
image = "ledgence/ledgence@sha256:REPLACE_WITH_WORKER_RUNTIME_DIGEST"
```

| Setting | Contract |
| --- | --- |
| `schema_version` | Exactly `1`; unknown configuration fields are rejected. |
| `program` | Exact ID, version and `module:function` handler; optional `kind` is `task`, `workflow` or `unspecified`, with optional display name and description. |
| `source.root` | Application import root, relative to the configuration file's directory. |
| `source.include` | Nonempty explicit files or directories relative to `source.root`; paths, not glob patterns. |
| `files` | Optional list of regular source files relative to the configuration directory and their package-relative destinations. |
| `dependencies.requirements` | Optional production requirements file relative to the configuration directory. Omit the section for standard-library-only programs. |
| `target.platform` | `linux/amd64` maps to manifest `linux/x86_64`; `linux/arm64` maps to `linux/aarch64`. |
| `target.python` | Exact CPython major/minor, at least `3.11`; checked inside the builder. |
| `target.protocol` | Runtime protocol `1`, `2` or `3`; workflows require `3`. |
| `target.image` | Worker-compatible image pinned with `@sha256:` plus 64 lowercase hexadecimal digits. The adapter invokes its `python3`. |

All selected inputs stay under the configuration directory, without symlinks or
parent traversal. A relative `--output` is also resolved from that directory,
not the shell's working directory. The default config path is `ledgence.toml` in
the working directory. `--source` for publication remains an ordinary path from
the working directory; pass it explicitly when building with a config elsewhere.

The CLI never copies the whole project by default. Recursive selections skip
hidden paths, environments, caches, credentials/secrets directories, SSH private
key names, and bytecode. Explicitly selecting an excluded path fails before its
contents are read. Destination collisions, case collisions, links, special files,
nonportable names and input/output overlap are rejected. Keep secrets outside
application selections: filename exclusions are not a secret scanner. A manifest
already present at the project root or included as the package manifest must
agree with the configuration.

Choose the target deliberately. The CLI does not infer it from the developer's
Mac, a server URL, or the first observed worker. Matching OS, architecture and
Python alone cannot prove compatible native system libraries; use the same
runtime image and qualify real imports and execution there.

## Dependency and Docker requirements

Use production requirements with exact `==` versions, SHA-256 hashes and every
active transitive dependency. The adapter uses pip's hash checking with wheels
only, `--no-compile`, a fresh target directory and public PyPI. Environment
markers and multiple hashes for compatible wheels are supported. Nested
requirements, index directives, URLs, editable/local packages and unpinned
requirements are not supported by this adapter. A missing compatible wheel or
incorrect hash fails the build; there is no implicit source compilation or
installation of a compiler. Keep the application lock file under your own
package manager's control. Keep `ledgence-worker` as a development dependency;
the deployed Rust worker supplies its runtime helper.

A project needing another preparation pipeline can supply a prepared directory
and use `program publish --source DIR`. Retain dependency license notices inside
the package. Ledgence does not rewrite dependencies' license terms.

Start a local Docker Engine or Docker Desktop running Linux containers. The CLI
uses the selected Docker context without changing the global context:

```sh
ledgence program build --config ledgence.toml \
  --output .ledgence/prepared-1.0.0 --context desktop-linux \
  --timeout-seconds 600
```

Use an existing context name from your machine. The default build timeout is
600 seconds, with a supported range of 1–3600. Cancellation or expiry prevents
publication of a pending build; the command waits for its owned container cleanup
and any running filesystem verification to finish before returning, so cleanup
can extend elapsed time beyond the deadline. `DOCKER_CONTEXT` is honored;
when `DOCKER_HOST` is set, select a named local context explicitly. Remote Docker
endpoints are rejected because local bind mounts cannot supply a remote daemon's
files. Docker must support the chosen platform, natively or through an
operator-configured emulation facility. The builder neither changes the Docker
context nor installs emulation. It mounts selected staging files, not the project
or Docker socket, and starts no privileged container.

Build completion returns JSON with `prepared_directory`, `receipt`, `manifest`,
`metadata` and `descriptor`. The adjacent `prepared.build.json` records input
hashes, prepared file hashes, target and image digest outside the package. Output
is staged and synchronized before publication. Existing outputs or receipts are
never overwritten; choose a new `--output` for another build. Preserve completed
outputs until their publication is confirmed. A partial or interrupted build
must be rebuilt into a fresh output directory.

Packaging the same prepared files, empty directories and executable permissions
produces the same ZIP bytes, independent of modification times. This does not
promise universal reproducibility of dependency preparation across tools or
images. The application handler is not imported or executed during build,
publication or registration.

## Enable publication on an installation

The orchestrator's upload capability is off by default. Add
`--allow-program-publication` to an explicitly configured instance with a
writable filesystem program store:

```sh
ledgence orchestrator serve --store /absolute/path/to/program-store \
  --instance-config /absolute/path/to/instance.json \
  --allow-program-publication
```

Keep the existing database, binding and Console asset options required by that
installation; source builds may also need `--console-dir`. See
[instance configuration](self-hosted-instance.md). The publication writer uses
the same canonical filesystem store that the orchestrator resolves. An HTTPS
read-only store remains supported, but cannot enable this filesystem writer.
The server and worker do not need Docker or a provider account to accept and
execute an already prepared package.

The matching local Compose templates give the orchestrator write access to
`/programs` and leave the worker's mount read-only. Both see the same volume.
**Existing saved kits are not rewritten or upgraded.** Installing a newer CLI
cannot enable uploads in an older stack. Use a separate state directory and a
matching qualified kit for evaluation, or plan an explicit
operator-managed upgrade with backups. Do not edit a copied kit to bypass its
checksum checks, silently replace its image, or delete persistent volumes.

On separate worker hosts, operators must expose the same verified artifacts
through a shared filesystem or the existing HTTPS reader. Upload does not set
up replication, S3, credentials, a remote worker or a distribution service.
Use HTTPS and the existing operator-controlled perimeter remotely. Uploading
code is an administrative capability for trusted operators; a hash, reviewer
name or `--server` is not authentication.

## Publish, register and recover

```sh
ledgence program publish --source .ledgence/prepared \
  --server http://127.0.0.1:8080 --register --kind task
```

`--store` and `--server` are mutually exclusive. `--register` and catalog metadata
flags require `--server`; filesystem publication returns its original
**descriptor-only JSON** and requires separate registration. Without
`--register`, HTTP publication leaves the catalog unchanged. `--kind`,
`--display-name` and `--description` override matching build-receipt metadata.
A kind is never inferred from runtime protocol. `--update-metadata true` is
required to replace existing descriptive metadata; this never changes the
immutable artifact.

Before sending the ZIP, the CLI saves `artifact.zip` and `receipt.json` beneath
`publications/publication-…` beside the prepared directory. The default location
is `.ledgence/publications/`. The receipt owns the exact descriptor, destination,
registration command and last confirmed state. Stdout reports JSON; progress
and the receipt location go to stderr. HTTP publication makes one PUT and does
not automatically retry. Its response is accepted only if identity, digest and
compressed size match that retained ZIP.

Resume a saved operation after a connection error, timeout, CLI interruption or
registration failure:

```sh
ledgence program publish --resume /absolute/path/to/receipt.json
```

Do not combine `--resume` with other options. It uses the saved destination and
registration intent, verifies the retained archive, and retries only the stages
not yet confirmed. It does not read the original project, rerun Docker or
rebuild dependencies. Preserve the receipt **and** its sibling ZIP; a missing or
changed archive is rejected. A timeout means the publication outcome is unknown,
not that the server rolled it back.

The HTTP result contains `descriptor`, `request_id`, `phase`, `receipt`,
`publication`, `registration`, `resume`, `register_command`,
`publication_outcome` and `error`. `request_id` is the last observed HTTP request
ID, when available. `error` is null or a structured object with `code` and,
when supplied by that error variant, `message`. Typed publication failures are
distinguished from catalog/local failures.
`publication_outcome` is `confirmed` after upload confirmation, `unknown` after
an uncertain upload/storage result, or `unconfirmed` otherwise. It is independent
of whether registration succeeded. Phases are `prepared`,
`published` and `registered`; a publish without registration ends at `published`.
If upload succeeds but registration fails, the command returns nonzero with the
confirmed descriptor and phase. Resume the receipt to retry registration only,
or use its `register_command` argument array. Separate registration can bind
both `--expected-digest sha256:HEX` and `--expected-size BYTES`; these flags must
be supplied together. The backend rejects a reference resolving to different
bytes, and the client verifies the returned registered descriptor. There is no
filesystem/database transaction and no blob deletion on registration failure.

Program identity is global **within the store**, keyed by `(program_id, version)`.
Catalog tenant/namespace scope does not partition that identity. Different
content, including another target's build, needs a new ID or version. One
program version has one artifact, without multiarchitecture variant selection.
Identical publication is idempotent; changing bytes under an existing identity
returns `immutable_conflict`.

## Binary API and bounds

Discover support before sending a body:

```text
GET /v1/programs/publication-capabilities
PUT /v1/programs/{program_id}/{version}/artifact
Content-Type: application/zip
X-Ledgence-Archive-Sha256: <64 lowercase hexadecimal characters>
```

The PUT body is the ZIP itself. The SHA header occurs exactly once; HTTP content
encoding is not accepted. IDs use the existing `ProgramRef` path-safe grammar.
The server computes the hash and actual length, verifies the manifest identity,
all member checksums, expansion, paths and ZIP profile. It never imports Python.
Neither a supplied digest nor `Content-Length` bypasses those checks.

Capabilities report `enabled`, `registration_enabled`, `mode` (`immutable` when
enabled), `limits`, `max_concurrent_uploads` and `transfer_timeout_ms`. An older
server's absent route is reported by the CLI as unsupported publication, with
`--store` as the existing alternative.

| Limit | Default |
| --- | --- |
| Compressed ZIP | 64 MiB |
| Expanded bytes | 256 MiB |
| Single file | 64 MiB |
| Entries, including directories | 4,096 |
| Manifest | 64 KiB |
| Descriptor | 16 KiB |
| Concurrent uploads per service instance | 2 |
| Transfer plus verification/persistence budget | 120 seconds |

Admission happens before reading the body. Transfers without a declared length
are bounded as bytes arrive. A request's timeout or disconnection does not free
its permit while admitted persistence still runs; shutdown waits for that
operation. Blocking ZIP/filesystem work stays on the adapter's executor. The
normal JSON control deadline remains 30 seconds. These bounds apply per
orchestrator process and do not establish a cluster-wide upload quota.

Capability and upload HTTP exchanges use the existing request-ID and optional
W3C trace propagation contract; subsequent registration uses the same configured
trace bridge. The CLI initializes its normal telemetry/logging and drains them
before exit. Optional tracing does not replace the retained receipt or determine
publication success. See [observability](observability.md).

A new descriptor returns `201` with `{ "descriptor": {…},
"already_published": false }`; an identical existing descriptor returns `200`
with `already_published: true`. A preexisting deduplicated blob with a new
descriptor is still a new publication. Failures use `{ "code": "…",
"message": "…" }` without reflecting source contents or store paths:

| HTTP | Code | Meaning |
| --- | --- | --- |
| 404 | `publication_disabled` | Upload is not enabled; an older server can return its ordinary missing-route response. |
| 400 | `invalid_artifact` | Request or package verification failed. |
| 413 | `artifact_too_large` | An enforced package/body limit was exceeded. |
| 409 | `immutable_conflict` | Same identity, different immutable content. |
| 429 | `publication_saturated` | Admission is full; retry the saved artifact later. |
| 503 | `publication_storage` | Storage unavailable; reconcile using the identical archive. |
| 504 | `publication_outcome_unknown` | The result is uncertain; the operation may still finish. |

The writer synchronizes directories and the verified blob before exposing the
descriptor through a no-clobber filesystem publication. Concurrent processes
therefore agree on one immutable identity. A crash can leave owned staging files
or unreferenced immutable blobs, but never a descriptor to a partially written
blob. There is no automatic destructive garbage collector; leave shared blobs
intact when reconciling a failure.

## Adapter boundary and evidence

`ProgramArtifactPublisher` is a separate portable write port. `ProgramStore` and
`ArtifactCache` remain worker reading/preparation contracts. The filesystem
adapter supplies `pack_directory`, `persist_archive` and
`FileProgramArtifactPublisher`; a custom writer must honor the same immutable
identity and completion-ownership rules. No Docker, filesystem or HTTP SDK types
appear in the portable publication request/result/error contract.

The package and API tests cover CRC/digest/identity errors, archive limits,
reserved namespace rejection, identical and conflicting writers across real
processes, interruption around blob/descriptor publication, timeout admission
retention, and catalog descriptor binding. Deployment acceptance must also
execute a native dependency in the actual target runtime; fake builders alone
do not prove platform compatibility.

The preparation choices follow [pip hash-checking mode](https://pip.pypa.io/en/stable/topics/secure-installs/)
and [Docker's platform model](https://docs.docker.com/build/building/multi-platform/).
Idempotent PUT behavior depends on the immutable writer described here; it is
not supplied solely by using the method name. See [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2).
