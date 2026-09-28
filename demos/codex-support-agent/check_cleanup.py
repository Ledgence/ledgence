#!/usr/bin/env python3
"""Offline regression of Codex failure cleanup through the real Rust worker.

The executable named Codex below is a local fixture. It never authenticates or
contacts a model. Two sequential tasks prove cleanup happens before slot reuse,
not merely when the worker shuts down at the end of the check.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
from prepare import target


def fixture_source(directory):
    return "#!" + sys.executable + "\n" + '''
import json, os, pathlib, subprocess, sys
base = pathlib.Path(''' + repr(str(directory)) + ''')
if sys.argv[1:] == ['--version']:
    print('codex-cli 0.158.0-alpha.2.1')
    raise SystemExit(0)
sys.stdin.read()
path = base / 'records.json'
records = json.loads(path.read_text()) if path.exists() else []
record = {'helper_pid': os.getppid(), 'group': os.getpgrp()}
if records:
    observed = subprocess.run(['/bin/ps', '-p', str(records[0]['descendant_pid']), '-o', 'stat='],
                              capture_output=True, text=True, timeout=2)
    if observed.returncode not in (0, 1) or observed.stderr:
        raise RuntimeError('could not inspect the owned fixture descendant')
    state = observed.stdout.strip()
    # A killed orphan may remain a zombie until PID 1 reaps it, especially in
    # Linux containers. It cannot execute and is not a surviving model process.
    record['previous_descendant_running'] = bool(state) and not state.startswith('Z')
else:
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    record['descendant_pid'] = child.pid
records.append(record)
path.write_text(json.dumps(records))
raise SystemExit(7)
'''


def task(number):
    return {"program": {"id": "codex-support-agent", "version": "1.0.0"}, "event": {
        "specversion": "1.0", "id": f"evt_codex_cleanup_{number}", "source": "urn:ledgence:test",
        "type": "com.ledgence.task.invocation.requested.v1", "datacontenttype": "application/json",
        "ldgtenantid": "test", "ldgnamespace": "demo", "ldgrunid": "run_cleanup",
        "ldgtaskid": f"task_cleanup_{number}", "ldgattemptid": f"att_cleanup_{number}", "ldgattemptno": 1,
        "data": {"ticket_id": f"TEST-{number}", "question": "Synthetic offline cleanup test"},
    }}


def check(binaries):
    operating_system, architecture = target()
    binary = binaries.resolve() / "ledgence-worker"
    if not binary.is_file():
        raise ValueError("build ledgence-worker first")
    with tempfile.TemporaryDirectory(prefix="ledgence-codex-retirement-") as temporary:
        scratch = Path(temporary)
        package = scratch / "package"
        shutil.copytree(HERE / "agent", package, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copy2(ROOT / "LICENSE", package / "LEDGENCE-LICENSE")
        manifest = {"schema_version": 1, "program": task(1)["program"],
                    "runtime": {"kind": "python", "python": "3.13", "protocol": 2},
                    "handler": "program:handle", "platform": {"os": operating_system, "arch": architecture}}
        (package / "ledgence-program.json").write_text(json.dumps(manifest))
        fake = scratch / "fake-codex"
        fake.write_text(fixture_source(scratch))
        fake.chmod(0o700)
        tasks = scratch / "tasks.json"
        tasks.write_text(json.dumps([task(1), task(2)]))
        environment = {name: value for name, value in os.environ.items()
                       if name in {"HOME", "PATH", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL"}}
        environment.update(LEDGENCE_CODEX_BIN=str(fake), PYTHONDONTWRITEBYTECODE="1")
        publication = subprocess.run([str(binary), "publish", "--source", str(package),
                                      "--store", str(scratch / "store")], env=environment,
                                     capture_output=True, timeout=20)
        if publication.returncode:
            raise ValueError("offline fixture publication failed")
        try:
            result = subprocess.run([
                str(binary), "run", "--tasks", str(tasks), "--store", str(scratch / "store"),
                "--cache", str(scratch / "cache"), "--python", sys.executable,
                "--runner", str(ROOT / "sdk/python/ledgence/worker/bootstrap.py"),
                "--concurrency", "1", "--timeout-ms", "10000",
            ], env=environment, capture_output=True, text=True, timeout=35)
            records = json.loads((scratch / "records.json").read_text())
            reports = [json.loads(line) for line in result.stdout.splitlines()]
            complete = len(records) == len(reports) == 2
            summary = {"worker_exit_code": result.returncode, "generation_attempts": len(records),
                       "helper_reused_after_failure": records[0]["helper_pid"] == records[1]["helper_pid"] if complete else None,
                       "descendant_running_before_second_attempt": records[1].get("previous_descendant_running") if complete else None}
            summary["passed"] = bool(
                result.returncode == 1 and complete
                and all(record["group"] == record["helper_pid"] for record in records)
                and all(report.get("failure", {}).get("phase") == "execution" for report in reports)
                and summary["helper_reused_after_failure"] is False
                and summary["descendant_running_before_second_attempt"] is False
            )
            return summary
        finally:
            record_path = scratch / "records.json"
            if record_path.exists():
                for record in json.loads(record_path.read_text()):
                    group = record["group"]
                    for pid in (record["helper_pid"], record.get("descendant_pid")):
                        if pid is None:
                            continue
                        try:
                            if group == record["helper_pid"] and os.getpgid(pid) == group:
                                os.killpg(group, signal.SIGKILL)
                                break
                        except ProcessLookupError:
                            pass


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    args = parser.parse_args(argv)
    try:
        summary = check(args.binaries)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        summary = {"passed": False, "failure_type": type(error).__name__}
    print(json.dumps(summary))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
