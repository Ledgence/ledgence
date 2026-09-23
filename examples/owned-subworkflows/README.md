# Owned subworkflow example

This parent starts the page-processing workflow from the [checkpoint example](../checkpoint-workflow/README.md), alongside an ordinary metadata task. It checkpoints once and waits for both terminal outcomes. The page workflow may execute several controller activations before the parent resumes. All work can use the same worker with `--concurrency 1`.

Prepare and publish the checkpoint example's `workflow-example` and `workflow-summary` packages first, then start its orchestrator, page server and worker. Reuse its exported `workflow_demo` directory and `LEDGENCE_PYTHON` interpreter. From the repository root, publish this parent:

```sh
./target/debug/ledgence-worker example --directory "$workflow_demo/owned" --python "$LEDGENCE_PYTHON"
"$LEDGENCE_PYTHON" - <<'PYTHON'
import json, os, shutil
from pathlib import Path
package = Path(os.environ["workflow_demo"]) / "owned" / "program"
manifest = package / "ledgence-program.json"
value = json.loads(manifest.read_text())
value["program"] = {"id": "owned-example", "version": "1.0.0"}
value["runtime"]["protocol"] = 3
manifest.write_text(json.dumps(value))
shutil.copyfile("examples/owned-subworkflows/program.py", package / "program.py")
PYTHON
./target/debug/ledgence-worker publish --source "$workflow_demo/owned/program" --store "$workflow_demo/store"
```

Submit the parent with the Python client, using the checkpoint example's `acme` tenant and `demo` namespace:

```python
workflow = await client.workflows.submit(
    program="owned-example", version="1.0.0", queue="workflows",
    data={"urls": ["http://127.0.0.1:8091/page.txt"] * 4, "queue": "workflows"},
    idempotency_key="owned-example:1",
)
print(await workflow.result(timeout=60))
```

`ctx.workflow()` starts an owned workflow; `ctx.task()` starts an ordinary task. Their keys share one namespace within the parent. Reusing the same key and binding reuses the original child; use a new key for a new iteration. The child keeps its pinned program package and has its own workflow ID, checkpoint state, retries, and terminal outcome.

Cancelling or failing the parent cancels its owned descendants and drains outstanding execution before the parent becomes terminal. A parent cannot complete successfully while an owned child is unfinished. The result timeout above only limits observation and does not cancel the tree. This example creates no detached workflows.
