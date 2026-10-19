#!/usr/bin/env python3
"""Exercise the relocated candidate executable and runtime helper on this host."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from program_fixture import prepare_package
from console_bundle import smoke as console_smoke


def task_fixture(program):
    """Two independent tasks exercise the generated starter and warm reuse."""
    return [{
        "program": program,
        "event": {
            "specversion": "1.0", "id": f"evt_smoke_{index}", "source": "urn:ledgence:release-smoke",
            "type": "com.ledgence.task.invocation.requested.v1", "subject": f"tasks/task_smoke_{index}",
            "time": "2026-09-12T14:00:00Z", "datacontenttype": "application/json",
            "traceparent": f"00-0af7651916cd43dd8448eb211c80319c-{index:016x}-01",
            "ldgtenantid": "tenant_smoke", "ldgnamespace": "qualification", "ldgrunid": "run_smoke",
            "ldgtaskid": f"task_smoke_{index}", "ldgattemptid": f"att_smoke_{index}_1", "ldgattemptno": 1,
            "data": {"name": name},
        },
    } for index, name in enumerate(("Ada", "Grace"), start=1)]


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
        created = json.loads(run(executable, "program", "new", temporary / "smoke-task"))
        # Release qualification runs on the selected native host. Prepare only
        # the dependency-free starter's source; production cross-target builds
        # use the explicit target configured in ledgence.toml.
        package = prepare_package(temporary / "package", created["program"]["id"],
                                  created["program"]["version"],
                                  (Path(created["directory"]) / "program.py").read_text(),
                                  python=sys.executable, protocol=3)
        tasks = task_fixture(created["program"])
        task_path = temporary / "tasks.json"
        task_path.write_text(json.dumps(tasks))
        run(executable, "program", "publish", "--source", package, "--store", temporary / "store")
        output = run(executable, "worker", "run", "--tasks", task_path, "--store", temporary / "store",
                     "--cache", temporary / "cache", "--python", sys.executable,
                     "--runner", bundle / "runtime/ledgence/worker/bootstrap.py", "--concurrency", "1")
        reports = [json.loads(line) for line in output.splitlines()]
        assert len(reports) == 2, reports
        assert reports[0]["report"]["process_id"] == reports[1]["report"]["process_id"], reports
        assert reports[1]["report"]["reused_process"] is True, reports
        for report, task in zip(reports, tasks):
            assert report["report"]["outcome"] == {
                "status": "success", "output": {"message": f"Hello, {task['event']['data']['name']}!"},
            }, report
        console_smoke(bundle, temporary / "store", temporary)
    print("Relocated binary, dynamic publication/cache, helper and warm process reuse passed")


if __name__ == "__main__":
    main()
