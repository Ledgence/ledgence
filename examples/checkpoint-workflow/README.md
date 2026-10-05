# Checkpoint workflow example

The controller fetches four pages concurrently in one Python process, durably
records each response, and stages a distributed summary task. It then returns a
checkpoint. The child uses the released worker slot; a later controller
activation reads its result. The controller registers typed `START` and `COLLECT`
entrypoints instead of routing continuation strings manually. Worker concurrency
can remain **1** throughout. Use matching 0.3.0 workers and orchestrator with all
database migrations installed; the historical 0.1 releases lack this API.

From the repository root, build and prepare both packages. Use CPython 3.11 or
newer and a disposable PostgreSQL 18 database:

```sh
export LEDGENCE_PYTHON="$(command -v python3.12)"
export DATABASE_URL='postgres://USER:PASSWORD@127.0.0.1:5432/ledgence'
cargo build -p ledgence-cli --locked
export workflow_demo="$(mktemp -d)"
printf '%s\n' "$workflow_demo"
./target/debug/ledgence program example --directory "$workflow_demo/controller" --python "$LEDGENCE_PYTHON"
./target/debug/ledgence program example --directory "$workflow_demo/child" --python "$LEDGENCE_PYTHON"

"$LEDGENCE_PYTHON" - <<'PY'
import json, os, shutil
from pathlib import Path

demo = Path(os.environ["workflow_demo"])
for folder, name, version, protocol in [
    ("controller", "workflow-example", "1.0.1", 3),
    ("child", "workflow-summary", "1.0.0", 2),
]:
    package = demo / folder / "program"
    manifest = package / "ledgence-program.json"
    value = json.loads(manifest.read_text())
    value["program"] = {"id": name, "version": version}
    value["runtime"]["protocol"] = protocol
    manifest.write_text(json.dumps(value))
    shutil.copyfile(Path("examples/checkpoint-workflow") / folder / "program.py", package / "program.py")
(demo / "pages").mkdir()
(demo / "pages" / "page.txt").write_text("Hello from Ledgence!\n")
PY

./target/debug/ledgence program publish --source "$workflow_demo/controller/program" --store "$workflow_demo/store"
./target/debug/ledgence program publish --source "$workflow_demo/child/program" --store "$workflow_demo/store"
./target/debug/ledgence orchestrator migrate
./target/debug/ledgence orchestrator serve --bind 127.0.0.1:8080 --store "$workflow_demo/store"
```

Leave the orchestrator running. In separate terminals, reuse the printed absolute
`workflow_demo` path and the same interpreter to start the page server and worker:

```sh
"$LEDGENCE_PYTHON" -m http.server 8091 --bind 127.0.0.1 --directory "$workflow_demo/pages"
```

```sh
./target/debug/ledgence worker connect \
  --server http://127.0.0.1:8080 --tenant acme --namespace demo --queue workflows \
  --store "$workflow_demo/store" --cache "$workflow_demo/cache" \
  --python "$LEDGENCE_PYTHON" --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

Install the Python client into a virtual environment, then run this program:

```sh
"$LEDGENCE_PYTHON" -m venv "$workflow_demo/client"
"$workflow_demo/client/bin/python" -m pip install ./sdk/python-client
"$workflow_demo/client/bin/python" - <<'PY'
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient("http://localhost:8080", tenant="acme", namespace="demo") as client:
        workflow = await client.workflows.submit(
            program="workflow-example", version="1.0.1", queue="workflows",
            data={"urls": ["http://127.0.0.1:8091/page.txt"] * 4, "queue": "workflows"},
            idempotency_key="checkpoint-example:1.0.1:1",
            correlation_key="checkpoint-example",
        )
        print(workflow.id)
        print(await workflow.result(timeout=60))

asyncio.run(main())
PY
```

The result contains `page_count: 4` and a summary of the four response bodies.
Repeating the same submission key/input returns the same workflow. The result
timeout only limits observation; it does not resubmit or cancel execution.
For a new run or a changed program version, choose a new submission key; keep
the original key and exact input when reconciling an uncertain submission.

The input accepts one to four plain HTTP URLs, each at most 2048 ASCII characters
without whitespace, credentials or a fragment, plus a queue name of 1–128
printable ASCII characters. The helper requires an uncompressed `Content-Length`
response, bounds headers to 16 KiB and each UTF-8 body to 8 KiB, and imposes a
10-second request deadline. These page bounds leave room for JSON escaping and
record bindings in the combined local journal and child command.

Invalid input returns an explicit workflow failure. Network, timeout and protocol
errors propagate so the activation's retry policy applies; acknowledged page
results are reused on retry. The collect entrypoint handles a failed or cancelled
summary task explicitly, instead of retrying a controller against the same terminal
child result. Inspect the child outcome for its underlying error.

An application can package an async HTTP client and its prepared dependencies
instead. Create event-loop-bound clients inside the invocation; the process is
reused but each async invocation owns its event loop. See
[workflow contracts and limitations](../../docs/workflows.md).
