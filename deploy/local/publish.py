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
    for name, protocol, source in [
        ("invoice-issuer", 2, None),
        ("workflow-example", 3, "controller"),
        ("workflow-summary", 2, "child"),
    ]:
        directory = Path(temporary) / name
        subprocess.run(["ledgence-worker", "example", "--directory", directory,
                        "--python", "/usr/local/bin/python3"], check=True)
        package = directory / "program"
        manifest = package / "ledgence-program.json"
        value = json.loads(manifest.read_text())
        value["program"] = {"id": name, "version": "1.0.0"}
        value["runtime"]["protocol"] = protocol
        manifest.write_text(json.dumps(value))
        if source:
            shutil.copyfile(Path("/opt/ledgence/examples/checkpoint-workflow") / source / "program.py", package / "program.py")
        else:
            (package / "program.py").write_text(TASK)
        subprocess.run(["ledgence-worker", "publish", "--source", package, "--store", "/programs"], check=True)
