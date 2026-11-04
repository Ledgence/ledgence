#!/usr/bin/env python3
"""Qualify poll shares through a real authenticated worker connect and PostgreSQL (MIT).

Set LEDGENCE_POSTGRES_URL to an explicitly disposable PostgreSQL server.
The worker and orchestrator use the binary from --binaries. All resources are local.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback
import uuid

from http_acceptance.harness import timed
from postgres_fixture import create_owned_database, owned_database_url
from worker_poll_plan.fixture import PollDeployment
from worker_poll_plan.scenarios import refresh_and_replay, secondary_admission, shared_capacity, stable


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path, required=True, help="new local evidence directory")
    parser.add_argument("--scenario", action="append", choices=("stride", "lottery", "refresh", "burst", "admission"))
    args = parser.parse_args()
    if not __debug__:
        parser.error("run with assertions enabled")
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent:
        parser.error("set LEDGENCE_POSTGRES_URL to an explicitly disposable server")
    binaries = args.binaries.resolve()
    if not (binaries / "ledgence").is_file():
        parser.error("--binaries must contain the built ledgence executable")
    database = "ledgence_poll_plan_" + uuid.uuid4().hex
    try:
        url = owned_database_url(parent, database)
    except ValueError as error:
        parser.error(str(error))
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[1]
    record = {"passed": False, "database": database, "cleanup_complete": False,
              "credential_scan_clean": None, "binary_sha256": hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest(),
              "scenarios": {}}
    def admin(sql):
        result = subprocess.run([args.psql, "--dbname", parent, "-X", "-v", "ON_ERROR_STOP=1", "-c", sql],
                                capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned database administration failed")
    try:
        create_owned_database(admin, database)
        try:
            d = PollDeployment(root, evidence, binaries, os.environ.get("LEDGENCE_PYTHON", sys.executable), url, args.psql)
            try:
                d.command("ledgence", ["orchestrator", "migrate"])
                d.provision()
                d.publish("poll-fixture", "1.0.0", program_source=(root / "tools/worker_poll_plan/program.py").read_text())
                scenarios = {"stride": lambda: stable(d, "stride"), "lottery": lambda: stable(d, "lottery"),
                             "refresh": lambda: refresh_and_replay(d), "burst": lambda: shared_capacity(d),
                             "admission": lambda: secondary_admission(d)}
                selected = args.scenario or scenarios
                ordinary = [name for name in selected if name != "admission"]
                for group in (ordinary, ["admission"] if "admission" in selected else []):
                    if not group:
                        continue
                    if group == ["admission"]:
                        d.enable_admission()
                    server = d.start_server()
                    for name in group:
                        with timed(evidence, "scenario", name):
                            record["scenarios"][name] = scenarios[name]()
                    server.stop()
            finally:
                try:
                    d.close()
                finally:
                    record["credential_scan_clean"] = d.remove_credentials_and_scan()
        finally:
            admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            record["cleanup_complete"] = True
        assert record["credential_scan_clean"], "credential scan failed"
        record["passed"] = True
    except Exception as exc:
        # Database and credential values do not belong in retained diagnostics.
        record["failure"] = {"type": type(exc).__name__, "frames": [
            {"file": Path(f.filename).name, "line": f.lineno, "function": f.name} for f in traceback.extract_tb(exc.__traceback__)]}
    finally:
        (evidence / "summary.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps({key: record.get(key) for key in ("passed", "cleanup_complete", "credential_scan_clean", "failure")}), flush=True)
    return 0 if record["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
