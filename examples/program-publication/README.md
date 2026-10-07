# Build and publish a Python workflow

Use the **Ledgence 0.5.0** CLI and a matching server with program publication
enabled. This example renders a title as HTML using a native Python wheel, and
records the operation
as a local workflow step. No model, cloud credentials or application database
is needed.

1. Start a **new** local installation using the matching 0.5.0 kit. Keep existing
   installations unchanged; use a separate `--directory` and free `--port` if
   needed. See the
   [build and publication guide](../../docs/program-publication.md). If you use a
   different port, use the printed API URL in both the publication command and
   Python `AsyncClient` example below.
2. In `ledgence.toml`, replace `REPLACE_WITH_WORKER_IMAGE_DIGEST` with the exact
   `repository@sha256:...` image used by that worker. Set `target.platform` to
   `linux/arm64` or `linux/amd64`. This example requires CPython 3.14. A different
   architecture produces different bytes: use a different program version when
   publishing it to the same store.
3. From this example directory, prepare and publish:

   ```sh
   ledgence program build
   ledgence program publish --server http://127.0.0.1:8080 --register
   ```

The builder includes `src/html_report`, `LICENSE` and the hash-pinned wheels.
The prepared directory is `.ledgence/prepared`; the build receipt is beside it.
The publication receipt and exact ZIP are under `.ledgence/publications/`.
Metadata comes from the validated build receipt. Neither command executes the
workflow. Docker is used only by the local builder; the server and worker need
no Docker API to accept or execute a prepared program.

Submit it with the matching Python client. In an existing uv application project,
install it with `uv add "ledgence-client==0.5.0"`, save the following code as
`submit.py`, and run `uv run python submit.py`. With an activated virtual
environment instead, use `python -m pip install "ledgence-client==0.5.0"`
and `python submit.py`.

```python
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:8080", tenant="acme", namespace="demo") as client:
        run = await client.workflows.submit(
            program="html-report", version="1.0.0", queue="demo",
            data={"title": "Ledgence & Python"},
            idempotency_key="html-report-first-run",
        )
        print(await run.result(timeout=60))

asyncio.run(main())
```

The result contains `<h1>Ledgence &amp; Python</h1>`, Linux architecture and the
loaded native extension filename. Use another idempotency key for a new logical
execution. A client result timeout does not cancel the workflow or resubmit it.

After an interrupted upload, run the exact `resume` argument array printed by
the CLI, or `ledgence program publish --resume PATH/receipt.json`. It uses the
saved ZIP. If upload succeeded but registration failed, the same command retries
only registration. Keep the receipt and archive together; do not rebuild to
recover an uncertain publication. See the [dependency review](third_party/README.md).

The distribution acceptance gate builds this example twice, verifies identical
artifacts, publishes and registers through the native CLI, executes it in the
Linux worker, restarts the stack and checks persistence plus another execution.
It uses fresh owned resources and cleans up only its own test project.
