# Owned subworkflow example

This parent starts the page-processing workflow from the [checkpoint example](../checkpoint-workflow/README.md), alongside an ordinary metadata task. It checkpoints once and waits for both terminal outcomes. The page workflow may execute several controller activations before the parent resumes. The parent uses typed `START` and `COLLECT` entrypoints. All work can use the same worker with `--concurrency 1`.

Prepare and publish the checkpoint example's `workflow-example` 1.0.1 and `workflow-summary` 1.0.0 packages first, then start its orchestrator, page server and worker. Reuse its exported `workflow_demo` directory and `LEDGENCE_PYTHON` interpreter. From the repository root, prepare this existing standard-library parent for the same host/interpreter and publish it. The output directory must be new:

```sh
"$LEDGENCE_PYTHON" - <<'PYTHON'
import json, os, platform, shutil, sys
from pathlib import Path
package = Path(os.environ["workflow_demo"]) / "owned" / "program"
assert sys.implementation.name == "cpython" and sys.version_info >= (3, 11)
os_name = {"Darwin": "macos", "Linux": "linux"}[platform.system()]
arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}[platform.machine()]
package.mkdir(parents=True)
value = {
    "schema_version": 1, "program": {"id": "owned-example", "version": "1.0.1"},
    "runtime": {"kind": "python", "python": f"{sys.version_info.major}.{sys.version_info.minor}", "protocol": 3},
    "handler": "program:handle", "platform": {"os": os_name, "arch": arch},
}
(package / "ledgence-program.json").write_text(json.dumps(value))
shutil.copyfile("examples/owned-subworkflows/program.py", package / "program.py")
PYTHON
./target/debug/ledgence program publish --source "$workflow_demo/owned/program" --store "$workflow_demo/store"
```

Submit the parent with the Python client, using the checkpoint example's `acme` tenant and `demo` namespace:

```python
workflow = await client.workflows.submit(
    program="owned-example", version="1.0.1", queue="workflows",
    data={"urls": ["http://127.0.0.1:8091/page.txt"] * 4, "queue": "workflows"},
    idempotency_key="owned-example:1.0.1:1",
)
print(await workflow.result(timeout=60))
```

`ctx.workflow()` starts an owned workflow; `ctx.task()` starts an ordinary task. Their keys share one namespace within the parent. Reusing the same key and binding reuses the original child; use a new key for a new iteration. The child keeps its pinned program package and has its own workflow ID, checkpoint state, retries, and terminal outcome.

Cancelling or failing the parent cancels its owned descendants and drains outstanding execution before the parent becomes terminal. A parent cannot complete successfully while an owned child is unfinished. The result timeout above only limits observation and does not cancel the tree. This example creates no detached workflows.

The parent accepts the checkpoint example's bounded URL list and queue name. The
page workflow owns URL validation and HTTP retries. Once both children are
terminal, the parent handles failed or cancelled outcomes explicitly before
reading successful results. It returns `child_failed` or `child_cancelled` instead
of retrying the controller against a child that cannot change. Unexpected runtime
exceptions still use the activation's retry policy.
