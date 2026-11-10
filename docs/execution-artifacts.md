# Execution files and artifact references

**Development source for the next release; not available in 0.9.0.** Use matching
server, worker, CLI and Python components from this checkout. Program ZIP
publication and the worker's program cache are separate capabilities.

Execution artifacts let a task, workflow entrypoint or authorized client keep a
file outside execution JSON and pass a small, immutable reference to it. This
works with application-owned PydanticAI, Google ADK, Anthropic SDKs or ordinary
Python: Ledgence does not import an agent framework or select a model provider.

## Configure storage

Apply the explicit database migrations before enabling this feature. Configure a
fixed instance with `--instance-config` and add `--artifact-config artifacts.json`
to `ledgence orchestrator serve`. Without artifact configuration the capability
is unavailable; neither installing the client nor registering a program enables it.

Create `artifacts.json` next to your instance configuration:

```json
{
  "schema": "ledgence.artifact-store.v1",
  "store_id": "local-execution-files",
  "backend": {"kind": "filesystem", "directory": "execution-files"},
  "spool_directory": "artifact-spool"
}
```

Relative paths resolve against this file's directory, not the process working
directory. Keep the byte-store directory on persistent storage. Its durable
marker is part of its identity: copying only object files into a new empty store
does not preserve that identity. The database binds `store_id` and fingerprint;
a conflicting binding fails startup. Do not edit the marker or change storage
location as a way to bypass that check. Configuration is strict JSON, at most
16 KiB, with no credentials embedded.

The filesystem mode supports one orchestrator with a persistent local volume.
It uses synchronized staging and atomic publication without replacing existing
bytes. Concurrent retention maintenance can open this store without removing
another process's live staging. Arbitrary network filesystems are not qualified.

An optional binary built with `s3-artifacts` accepts this backend instead:

```json
{
  "kind": "s3",
  "bucket": "my-execution-files",
  "prefix": "ledgence",
  "region": "us-east-1",
  "endpoint_url": null,
  "force_path_style": false,
  "local_credentials": false
}
```

This is the `backend` value, not the whole configuration. Create the bucket and
provision the server's normal AWS credential chain separately. Custom endpoints
must be HTTPS origins. `local_credentials: true` is exclusively for an explicit
literal-loopback local fixture endpoint, using fixed nonsecret credentials; it
is never the default cloud credential behavior. The adapter requires conditional
single-object creation and checksum behavior compatible with its tested profile.
The pinned local RustFS acceptance gate verifies that profile, including restart;
it does not qualify AWS or every S3-compatible service. The external test fixture
is not bundled with Ledgence.

## Reference and ownership

A successful upload returns a JSON-compatible reference:

```json
{
  "schema": "ledgence.artifact.v1",
  "id": "art_example",
  "digest": "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
  "size": 5,
  "media_type": "text/plain"
}
```

This illustrative reference describes the five bytes `hello`; it is not an
existing downloadable object. References contain no storage URL, local path or
credential and do not grant access. Keep CloudEvent `data` application-owned:
explicitly put the reference in your result or another task's input. Ledgence
does not automatically move large JSON values into storage.

The catalog records the canonical root and logical producer. A worker producer
is its task ID or workflow activation ID, derived from its current durable lease.
An external upload is attributed to its verified actor or trusted fixed instance.
A credential rotation preserving actor identity preserves that producer binding.
A child can read references within the same root; passing one to an unrelated
root does not grant worker access. Tasks remain leaves: file IO is not task
orchestration.

The immutable logical key is `(scope, canonical root, producer, key)`. Repeating
that key with identical digest, size, media type and display filename reuses an
accepted reference. Different metadata conflicts. A worker retry retains its
logical producer; a new workflow activation is a different producer. Use stable
keys for the same operation and new keys for genuinely new iterations. If an
agent can generate different bytes on retry, persist its accepted result through
the existing durable-operation helpers before uploading. An attempt ID is not a
substitute for this decision.

## Use from Python

Inside an active protocol-3 task or workflow entrypoint:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from ledgence.worker import artifacts

async def handle(event):
    # Write in scratch space rather than inside the immutable program package.
    with TemporaryDirectory() as directory:
        report = Path(directory) / "report.txt"
        report.write_text("hello", encoding="utf-8")
        reference = await artifacts.put_file(
            report, key="report", media_type="text/plain", filename="report.txt",
        )
    return {"report": reference}
```

The host negotiates `execution_artifacts.v1`, binds the real invocation lease and
clips transfers to its remaining deadline. The helper accepts no storage
credentials or caller-provided producer. Another task in the same workflow root
can call `await artifacts.download(event["data"]["report"], destination)`; the
destination must not already exist. The Rust host verifies bytes before atomic
publication of that local file.

Outside a worker, `AsyncClient.artifacts` provides `put_file`, `inspect`, `list`,
`download`, `handle`, `upload_status`, `reconcile` and `replace`. Uploads name
exactly one existing `task_id` or `workflow_id`. Client references are typed;
use `reference.to_dict()` when placing one in JSON. See the
[client documentation](../sdk/python-client/README.md) and the
[file-sharing guide](../docs-site/src/content/docs/how-to/share-execution-files.md).

## Authorization and inspection

| Operation | Native credential capability |
| --- | --- |
| External prepare/upload/status/reconcile/replace | `artifact_write` |
| Inspect or list accepted metadata | `metadata_read` |
| Download contents | `sensitive_read` |
| Worker file operations | `worker_execute` plus a current matching lease |

The server checks current authority and retained root state. Download prepares a
verified spool, performs required conditional audit, then checks authority again
before releasing response bytes. Revocation cannot retract bytes already sent.
An artifact reference never replaces these checks.

Console contract 16 exposes `capabilities.execution_artifacts`. The General tab
lists files for the execution's canonical root, including those produced by its
other descendants. Metadata names the root and producer so this is explicit.
Metadata inspection does not fetch bytes. Download is a separate sensitive action;
there is no inline preview. Console's manifest projection omits the private scope.
MCP exposes only `ledgence_artifact_inspect` and `ledgence_artifact_list`, with
configured scope injection. It has no file upload, download or replacement tools.

## Uncertain uploads and recovery

A prepared generation has a fixed 120-second expiry. Read-only status and repeated
prepare calls do not extend it. At most 128 nonterminal upload generations may be
retained per canonical root. Pre-write expiry can safely release this bound;
writing or uncertain generations cannot be forgotten merely because time passed.

Only one durable transition grants the physical write for a generation. The S3
adapter sends one conditional PUT with exact length and whole-object SHA-256;
SDK write retries are disabled even if environment settings request retries.
An ETag is never treated as the content digest. A connection failure after
possible dispatch is uncertain, not proof that no bytes were stored.

Keep the returned selection/upload identity when a transfer is uncertain:

1. Inspect `upload_status` without sending bytes again.
2. Explicitly `reconcile` the selection to verify stored bytes and, if still
   selected and authorized, accept them. Reconciliation does not retransmit.
3. If replacement is eligible, use `replace` with the observed revision. This
   compare-and-set operation creates one successor. Replaying it returns that
   same successor, even after later replacement; an old generation never becomes
   selected again.

Replacement eligibility is not evidence that an old remote PUT stopped. An absent
object likewise does not prove a delayed write cannot appear. `inspect` and
`list` show accepted retained manifests only, so an empty list is not proof that
no upload is unresolved. Failed prepare responses may lack a selection; repeating
the same logical preparation is the lookup path, not permission for a blind PUT.

## Bounds, disk use and shutdown

Files may be empty and are limited to 64 MiB. Chunks are at most 256 KiB;
references at most 1 KiB, manifests 4 KiB, and pages 512 KiB with at most 100 items.
Binary transfer IO and local ownership have an absolute deadline of at most
120 seconds; ordinary JSON request budgets are unchanged. A timed-out client does not resubmit an execution.

One orchestrator spool factory admits two uploads and four downloads before
creating files: at most 384 MiB of spool data. Filesystem atomic staging can add
128 MiB, for **512 MiB of temporary file data**, plus bookkeeping. These bounds
apply per composed instance and add across replicas. They are separate from the
worker's execution concurrency and program cache. Browser download verification
buffers up to one file in memory; server transfers use bounded chunks.

Transfers traverse worker/client → orchestrator → storage and the reverse path.
Allow for those network hops and local disk IO. This is bounded file exchange,
not a zero-copy transport or a bulk-dataset system.

An independent deadline expires local download ownership and starts spool cleanup,
even when HTTP stops polling the body. It does not impose a connection-level write
timeout: a client that stops reading can keep HTTP graceful shutdown waiting after
the temporary file has been removed. The existing [shutdown policy](http-orchestration.md)
retains in-flight HTTP connections; a second signal explicitly forces exit.
Dropped futures and expired deadlines do not prove blocking disk IO ended:
permits remain owned through IO and file removal. Failed removal retains capacity
and retries cleanup.
Shutdown closes admission and drains service transfers, retained blob writes and
spools. A drain timeout reports incomplete cleanup; the process keeps its runtime
alive until ownership can be released. Operators must repair the underlying IO
failure rather than treat that warning as successful shutdown.

## Retention and backup

Artifacts follow the canonical root's retention, not an individual child task.
Removing a child does not remove its files while the root remains retained.
Root retirement hides artifacts from new reads; already authorized responses can
finish. Back up the PostgreSQL catalog and durable byte store together, preserving
the store marker or S3 identity. Spools are transient and are not recovery copies.

Pass the same `--artifact-config` to `ledgence orchestrator retain --apply` for
physical maintenance. Preview remains read-only. Maintenance uses a separately
locked, stable `retain` spool namespace, so it can coexist with serving and clean
its own recognized crash leftovers. A second concurrent maintainer cannot share
that namespace. There is no automatic artifact deletion daemon.

A durable cleanup ledger survives execution-history deletion. Physical deletion
requires retained evidence that an old write is quiescent, plus permanent
retirement/deselection. Expiry or an absent-object probe alone cannot authorize
it. Pending/uncertain cleanup remains visible in maintenance progress. No public
arbitrary artifact-delete operation bypasses these rules.

## Verification

The feature has separate contract, PostgreSQL, subprocess, Python client, HTTP,
CLI/MCP, Console and blob-adapter tests. `tools/check-artifact-s3.py` exercises the
production S3 adapter against the pinned external local fixture, including
conditional creation, exact size/digest checks and restart. See the
[adapter runbook](../crates/ledgence-adapter-blob/README.md). Local results do not
establish GitHub candidate qualification or publication; release gates remain
required for the exact candidate.
