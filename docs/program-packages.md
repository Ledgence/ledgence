# Python program packages, version 1

A deployment is a ZIP archive containing application code, vendored application dependencies, and one root `ledgence-program.json`. The worker provides CPython and its small Ledgence helper. It does not install dependencies or download an interpreter at task time.

Example package root:

```text
ledgence-program.json
program.py
my_dependency/
  __init__.py
```

```json
{
  "schema_version": 1,
  "program": {"id": "invoice-issuer", "version": "1.2.0"},
  "runtime": {"kind": "python", "python": "3.12", "protocol": 2},
  "handler": "program:handle",
  "platform": {"os": "linux", "arch": "x86_64"}
}
```

The manifest schema remains version 1. Runtime protocol 2 adds invocation-local
processing trace context and best-effort contextual logging. Runtime protocol 1
remains supported; the helper import migration below applies independently of
the protocol. Changing a manifest to protocol 2
requires publishing a new program version and digest; the worker never silently
falls back to another protocol after dispatch. See the [Python helper](../sdk/python/README.md).

Runtime protocol 3 opts into [checkpoint workflows](workflows.md), including
async workflow handlers and durable local-result request/acknowledgment frames.
Protocol 1 and 2 synchronous packages remain supported. A workflow
package uses the same digest-pinned prepared dependency environment, and its
local steps share that environment. The worker does not import independently
packaged programs into a workflow interpreter.

Program IDs and versions are nonempty, lowercase portable path components, at most 128 bytes. The runtime declaration requires an exact CPython major/minor of at least 3.11. Supported target labels are `linux`/`macos` and `x86_64`/`aarch64`. A worker rejects a package for another target. Publishing a prepared package for a different target is allowed.

Build dependencies for the declared target. Native extensions require compatible OS libraries, architecture, and Python ABI; a ZIP alone cannot make them portable. Deployments should pin and vendor their complete dependency set, including its required legal notices. For example, in a build environment matching the worker:

```sh
python3.12 -m pip install --target ./program -r requirements.lock
```

This is a build-time operation; Ledgence's runtime does not invoke pip. Requirements files, hashes, and reproducible application builds remain the program publisher's responsibility. Publication preserves empty directories and regular-file executable bits. The prepared cache strips write and special permission bits, retaining read permissions plus those executable bits. The initial archive profile supports ZIP32 with stored or deflated regular files and directories, portable ASCII paths, and no symlinks, special files, encrypted entries, or ZIP64.

## Python import namespace

Application-side client imports use `ledgence.client`; programs use
`ledgence.worker`, including `from ledgence.worker.workflow import workflow_context`.
The standard-library-only worker helper is supplied by the worker and does not
require the client SDK or its HTTP dependencies. The shared `ledgence` root is a
native namespace package: vendored packages contributing to it must not include
`ledgence/__init__.py`.

The pre-MVP `ledgence_worker` import path has been removed. Programs using that
path must update their imports and publish a new immutable program version and
digest before running on an updated worker. Updating the worker does not rewrite
existing artifacts. Artifacts bundling an earlier client SDK with a regular
`ledgence/__init__.py` must be rebuilt with the updated namespace-compatible
client SDK. Handler modules under `ledgence.*` must move to an application-specific
namespace because the bootstrap now preloads the `ledgence` parent. Publish any
changed artifact under a new program version and digest.

Programs already using the new imports, compatible namespace packages, and
application-specific handler names need no other migration changes. The manifest
schema and runtime protocols are unchanged.

## Publication and identity

`ledgence-worker publish --source DIR --store DIR` creates deterministic archive bytes and publishes:

```text
programs/<program-id>/<version>/descriptor.json
blobs/<sha256-hex>.zip
```

The descriptor contains `program`, `digest` (`sha256:<64 lowercase hex characters>`), and the compressed archive `size`. The immutable descriptor is published after the blob. Republishing identical content succeeds; changing content under the same program/version is an integrity error. Upload a new version to deploy new bytes. Publication is local filesystem based in this milestone; the same completed layout can be served over HTTPS.

The HTTPS adapter validates response status, declared identity, and bounded size. Cache preparation verifies the downloaded bytes against the descriptor digest before extraction. It disallows redirects and non-HTTPS origins, except loopback HTTP for local testing. It currently has no authenticated registry protocol or credentials configuration. Digests verify bytes against a trusted descriptor; they do not establish publisher authenticity by themselves.

## Cache and process lifetime

The cache has one exclusive owner, a retained lock, and pins that prevent eviction while a prepared artifact or process still uses it. The last cache handle or artifact pin explicitly releases that ownership lock, so a temporarily inherited file descriptor cannot extend its lifetime. Separate worker processes need separate cache directories. Materialization verifies archive bytes before extraction, validates paths and the manifest, and publishes the completed materialization atomically. Program content and stored metadata files are read-only; the cache's private wrapper directory remains owner-writable so publication and eviction can rename it on macOS as well as Linux. Reopening an older cache normalizes only that wrapper's permissions after content verification. Directories and files are synced during publication. Reopening the cache verifies persisted bytes, topology, and executable bits; it does not trust mere directory presence. Eviction first renames a victim into a private deletion directory and syncs that rename before deleting content. Failed deletion retains its remaining quota charge and is retried before new publication or on reopen, so partial deletion never appears as a live digest entry. Staging reserves its planned file bytes before writes begin. If publication and rollback fail, the cache retains the remaining staging charge (or the previous reservation if it cannot measure the remainder) and retries cleanup before another publication. Existing cache lookups and pinned programs remain usable while this cleanup is pending; recovery does not require restarting the cache.

The default limits are 64 MiB compressed, 256 MiB expanded, 64 MiB per file, 4,096 entries, a 64 KiB manifest, and a 1 GiB cache content quota. Quota accounting includes compressed archives, extracted regular-file bytes, and descriptors. Publication reserves quota for staged artifact content before writing it. Filesystem allocation overhead, downloaded memory buffers, and process scratch space are outside that content quota. Archive bytes are buffered in memory with bounded size; streaming cache publication is future work.

Warm and active sessions retain cache pins. Unpinned entries can be evicted; the core can retire idle processes when their pins prevent an otherwise valid package fitting. Oversized or invalid packages fail directly. Each session receives a separate temporary working directory, preserved between its invocations and removed after confirmed cleanup. Access package resources relative to the module's `__file__`.

The package model follows the deployment/runtime separation described in [AWS's Python package documentation](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html), without requiring AWS services or SDKs.

Handler modules and their package parents must originate inside the artifact. Names already loaded by the bootstrap (for example `json`, `os`, or `ledgence`) are rejected as handler module names before readiness. Use an application-specific module name. Ordinary imports do not write Python bytecode into the artifact, even if its filesystem permissions allow writes.
