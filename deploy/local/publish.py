"""Publish target-correct programs dynamically into the shared immutable store."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

TASK = """import os
count = 0
def handle(event):
    global count
    count += 1
    return {"invoice_id": event["data"]["invoice_id"], "pid": os.getpid(), "invocation": count}
"""
with tempfile.TemporaryDirectory(prefix="ledgence-publish-") as temporary:
    for name, version, protocol, source in [
        ("invoice-issuer", "1.0.0", 2, None),
        ("workflow-example", "1.0.1", 3, "controller"),
        ("workflow-summary", "1.0.0", 2, "child"),
    ]:
        directory = Path(temporary) / name
        subprocess.run(["ledgence", "program", "example", "--directory", directory,
                        "--python", "/usr/local/bin/python3"], check=True)
        package = directory / "program"
        manifest = package / "ledgence-program.json"
        value = json.loads(manifest.read_text())
        value["program"] = {"id": name, "version": version}
        value["runtime"]["protocol"] = protocol
        manifest.write_text(json.dumps(value))
        if source:
            shutil.copyfile(Path("/opt/ledgence/examples/checkpoint-workflow") / source / "program.py", package / "program.py")
        else:
            (package / "program.py").write_text(TASK)
        subprocess.run(["ledgence", "program", "publish", "--source", package, "--store", "/programs"], check=True)

        # Artifact publication is immutable and precedes catalog registration.
        # A failed registration leaves a valid artifact; rerun this command to
        # reconcile it instead of deleting or rolling back the program store.
        subprocess.run(["ledgence", "program", "register", "--server", "http://orchestrator:8080",
                        "--program", name, "--version", version, "--kind",
                        "workflow" if name == "workflow-example" else "task"], check=True)
