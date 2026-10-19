"""Publish target-correct programs dynamically into the shared immutable store."""
from pathlib import Path
import subprocess
import tempfile

from program_fixture import prepare_package

EXAMPLES = Path("/opt/ledgence/examples/checkpoint-workflow")

TASK = """import os
count = 0
def handle(event):
    global count
    count += 1
    return {"invoice_id": event["data"]["invoice_id"], "pid": os.getpid(), "invocation": count}
"""

def main():
    with tempfile.TemporaryDirectory(prefix="ledgence-publish-") as temporary:
        for name, version, protocol, source in [
            ("invoice-issuer", "1.0.0", 2, None),
            ("workflow-example", "1.0.1", 3, "controller"),
            ("workflow-summary", "1.0.0", 2, "child"),
        ]:
            code = (EXAMPLES / source / "program.py").read_text() if source else TASK
            package = prepare_package(Path(temporary) / name / "program", name, version, code,
                                      python="/usr/local/bin/python3", protocol=protocol)
            subprocess.run(["ledgence", "program", "publish", "--source", package, "--store", "/programs"], check=True)

            # Artifact publication is immutable and precedes catalog registration.
            # A failed registration leaves a valid artifact; rerun this command to
            # reconcile it instead of deleting or rolling back the program store.
            subprocess.run(["ledgence", "program", "register", "--server", "http://orchestrator:8080",
                            "--program", name, "--version", version, "--kind",
                            "workflow" if name == "workflow-example" else "task"], check=True)


if __name__ == "__main__":
    main()
