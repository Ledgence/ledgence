# Checkpoint workflow example

The controller fetches four pages concurrently in one Python process, durably
records each response, and stages a distributed summary task. It then returns a
checkpoint. The child uses the released worker slot; a later controller
activation reads its result. Worker concurrency can remain **1** throughout.

From the repository root, build and prepare both packages. Use CPython 3.11 or
newer and a disposable PostgreSQL 18 database:

```sh
export LEDGENCE_PYTHON="$(command -v python3.12)"
export DATABASE_URL='postgres://USER:PASSWORD@127.0.0.1:5432/ledgence'
cargo build --workspace --bins --locked
export workflow_demo="$(mktemp -d)"
./target/debug/ledgence-worker example --directory "$workflow_demo/controller" --python "$LEDGENCE_PYTHON"
./target/debug/ledgence-worker example --directory "$workflow_demo/child" --python "$LEDGENCE_PYTHON"

"$LEDGENCE_PYTHON" - <<'PY'
import json, os, shutil
from pathlib import Path

demo = Path(os.environ["workflow_demo"])
for folder, name, protocol in [
    ("controller", "workflow-example", 3),
    ("child", "workflow-summary", 2),
]:
    package = demo / folder / "program"
    manifest = package / "ledgence-program.json"
    value = json.loads(manifest.read_text())
    value["program"] = {"id": name, "version": "1.0.0"}
    value["runtime"]["protocol"] = protocol
    manifest.write_text(json.dumps(value))
    shutil.copyfile(Path("examples/checkpoint-workflow") / folder / "program.py", package / "program.py")
(demo / "pages").mkdir()
(demo / "pages" / "page.txt").write_text("Hello from Ledgence!\n")
PY

./target/debug/ledgence-worker publish --source "$workflow_demo/controller/program" --store "$workflow_demo/store"
./target/debug/ledgence-worker publish --source "$workflow_demo/child/program" --store "$workflow_demo/store"
./target/debug/ledgence-orchestrator migrate
./target/debug/ledgence-orchestrator serve --bind 127.0.0.1:8080 --store "$workflow_demo/store"
```

Leave the orchestrator running. In separate terminals, reuse the printed absolute
`workflow_demo` path and the same interpreter to start the page server and worker:

```sh
"$LEDGENCE_PYTHON" -m http.server 8091 --bind 127.0.0.1 --directory "$workflow_demo/pages"
```

```sh
./target/debug/ledgence-worker connect \
  --server http://127.0.0.1:8080 --tenant acme --namespace demo --queue workflows \
  --store "$workflow_demo/store" --cache "$workflow_demo/cache" \
  --python "$LEDGENCE_PYTHON" --runner "$PWD/sdk/python/ledgence_worker/bootstrap.py" \
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
            program="workflow-example", version="1.0.0", queue="workflows",
            data={"urls": ["http://127.0.0.1:8091/page.txt"] * 4, "queue": "workflows"},
            idempotency_key="checkpoint-example:1",
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

The example's small HTTP helper deliberately accepts only plain HTTP responses
with `Content-Length`, bounded to 64 KiB. An application can package an async HTTP
client and its prepared dependencies instead. Create event-loop-bound clients
inside the invocation; the process is reused but each async invocation owns its
event loop. See [workflow contracts and limitations](../../docs/workflows.md).
