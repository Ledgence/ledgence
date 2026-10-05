#!/usr/bin/env python3
"""Exercise the relocated candidate executable and runtime helper on this host."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from console_bundle import smoke as console_smoke


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args()
    bundle = args.directory.resolve()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    # Exporter availability is not needed for a portable program smoke.
    for name in list(env):
        if name.startswith(("OTEL_", "LEDGENCE_OTEL_", "LEDGENCE_METRICS_")):
            env.pop(name)
    def run(*command):
        return subprocess.check_output(list(map(str, command)), cwd=temporary, env=env, timeout=60, text=True)
    with tempfile.TemporaryDirectory(prefix="ledgence-bundle-smoke-") as directory:
        temporary = Path(directory)
        assert {path.name for path in (bundle / "bin").iterdir()} == {"ledgence"}
        executable = bundle / "bin/ledgence"
        assert "Ledgence" in run(executable, "--help")
        assert "ledgence" in run(executable, "--version").lower()
        for group in ("program", "worker", "orchestrator", "task", "approval", "mcp"):
            assert group in run(executable, group, "--help")
        run(executable, "program", "example", "--directory", temporary / "example", "--python", sys.executable)
        run(executable, "program", "publish", "--source", temporary / "example/program", "--store", temporary / "store")
        output = run(executable, "worker", "run", "--tasks", temporary / "example/tasks.json", "--store", temporary / "store",
                     "--cache", temporary / "cache", "--python", sys.executable,
                     "--runner", bundle / "runtime/ledgence/worker/bootstrap.py", "--concurrency", "1")
        reports = [json.loads(line) for line in output.splitlines()]
        assert len(reports) == 2, reports
        assert reports[0]["report"]["process_id"] == reports[1]["report"]["process_id"], reports
        assert reports[1]["report"]["reused_process"] is True, reports
        for report in reports:
            assert report["report"]["outcome"]["status"] == "success", report
        console_smoke(bundle, temporary / "store", temporary)
    print("Relocated binary, dynamic publication/cache, helper and warm process reuse passed")


if __name__ == "__main__":
    main()
