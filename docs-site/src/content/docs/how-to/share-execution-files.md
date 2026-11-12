---
title: Share files between execution steps
description: Save an execution file, pass its immutable reference, and download it deliberately.
---

**Development source for the next release; not available in 0.9.0.** Use matching
server, worker, CLI and Python packages from this checkout. The released local
stack does not yet enable this feature. Your operator must configure a fixed
instance and an execution file store; configuration is explained below.

Suppose a task produces a report that another step or a person needs. Keep the
file in the execution store and return a small reference. The reference records
its ID, SHA-256, size and media type. It contains no storage credentials or URL.

## Produce a small report

In an existing Python task project, use this handler in your editor. Keep the
handler path registered by your program manifest:

```python title="handler.py"
from pathlib import Path
from tempfile import TemporaryDirectory
from ledgence.worker import artifacts

async def handle(event):
    with TemporaryDirectory() as directory:
        report = Path(directory) / "report.txt"
        report.write_text("Hello, Ledgence!\n", encoding="utf-8")
        # This key identifies the same logical report across worker retries.
        reference = await artifacts.put_file(
            report, key="report", media_type="text/plain", filename="report.txt",
        )
    # Only this small reference becomes part of the task's JSON result.
    return {"report": reference}
```

Build, publish and submit the task using the [program development workflow](/how-to/develop-python-programs).
The host supplies execution identity and authorization. You do not configure an
object-store SDK or credential in the program. This helper works with ordinary
Python and application-owned agent frameworks alike.

When a workflow gives that reference to another task in the **same workflow
root**, the receiving handler can download it:

```python title="read_report.py"
from pathlib import Path
from tempfile import TemporaryDirectory
from ledgence.worker import artifacts

async def handle(event):
    with TemporaryDirectory() as directory:
        destination = Path(directory) / "report.txt"
        await artifacts.download(event["data"]["report"], destination)
        return {"text": destination.read_text(encoding="utf-8")}
```

The workflow passes the first result's `report` field in the second task's
`data`. See [your first workflow](/tutorials/first-workflow) for composition.
Tasks themselves do not launch other tasks. A reference from an unrelated root
does not grant a worker permission to read it.

## Download a report from your application

Create this file beside your normal client scripts. Replace `ARTIFACT_ID` with
the ID inside the completed task's `report` reference:

```python title="download_report.py"
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo",
    ) as client:
        manifest = await client.artifacts.inspect("ARTIFACT_ID")
        await client.artifacts.download(manifest.reference, "downloaded-report.txt")
        print("Saved downloaded-report.txt")

asyncio.run(main())
```

Run `python download_report.py`. The destination must not exist: download verifies
length and SHA-256 before publishing the local file and never overwrites it.
A credential-protected installation needs a credential with `metadata_read` for
inspection and `sensitive_read` for the download, supplied through the existing
`credentials_file` client option.

In Console, open an execution's **General → Artifacts** section. The list covers
its entire root, including files produced by sibling tasks. Open metadata to see
the producer, root and digest; click **Download** to fetch bytes. Metadata browsing
does not load file contents. There are no inline previews.

## Keep retries intentional

The same producer and `key` with identical bytes and metadata reuse the accepted
reference. Changed contents under the same key conflict. For a model-generated
report, persist the accepted model result with [durable call recovery](/how-to/recover-agent-calls)
before uploading so a retry does not silently regenerate different contents.

A network timeout can mean an upload succeeded without its acknowledgement
arriving. Preserve its upload/selection information, inspect status, and explicitly
reconcile it. Reconciliation verifies stored bytes without resending them. Explicit
revision-bound replacement is available when eligible; an old request timing out
does not prove that request stopped. See the [full recovery contract](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-artifacts.md#uncertain-uploads-and-recovery).

## Configure the instance

An operator creates `artifacts.json`:

```json title="artifacts.json"
{
  "schema": "ledgence.artifact-store.v1",
  "store_id": "local-execution-files",
  "backend": {"kind": "filesystem", "directory": "execution-files"},
  "spool_directory": "artifact-spool"
}
```

Apply database migrations, then add `--artifact-config artifacts.json` to the
existing `ledgence orchestrator serve` command alongside its `--instance-config`.
Paths resolve next to the JSON file. Store bytes on persistent local storage;
this filesystem mode supports one orchestrator. The normal program `--store` is
separate and does not enable execution files. An optional `s3-artifacts` build
supports a server-owned S3-compatible store; see [storage configuration](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-artifacts.md#configure-storage).

Files are limited to 64 MiB, with at most 120 seconds for transfer IO and shorter
limits when the worker's attempt is ending. One orchestrator admits two uploads
and four downloads. Provision up to 512 MiB of temporary file data for the
filesystem mode, plus bookkeeping, independently of program caches. Multiple
instances add their own budgets.

Files follow the root execution's retention. Removing a completed child does not
remove its files while its root remains retained. Physical cleanup runs through
explicit retention maintenance and preserves uncertain write obligations. Back
up the complete stopped durable file store together with PostgreSQL and
configuration. Include hidden `.ledgence-artifact-store` and preserve its bytes
and configured `store_id`. Restore the whole filesystem copy into an empty target
before the server first opens it; an early open generates a marker rejected by
the restored catalog. Its directory path may change while that identity remains
fixed. Temporary spools and worker caches are not backups. S3 identity binds
endpoint, region, bucket and prefix, so a cross-bucket copy is not a transparent
restore. Read the [operational contract](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-artifacts.md#retention-and-backup)
and [0.10 upgrade procedure](/how-to/upgrade-to-0-10) before enabling this in an
existing installation.

A transfer deadline releases local download ownership and starts temporary-file
cleanup. It does not force a network connection closed. A client that stops
reading can prolong the server's graceful shutdown; see the
[HTTP shutdown contract](https://github.com/Ledgence/ledgence/blob/develop/docs/http-orchestration.md)
for the separate force-exit procedure.

## Try the complete example

The [report workflow example](https://github.com/Ledgence/ledgence/tree/develop/examples/execution-artifacts)
contains the producer, consumer and workflow together. It uses separate worker
processes and requires no model API key or framework-specific integration.
