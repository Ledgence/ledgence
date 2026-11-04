#!/usr/bin/env python3
"""Measure FIFO and 3:1 sharing with authenticated native workers and a disposable PostgreSQL database (MIT)."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import traceback
import uuid
from pathlib import Path

from http_acceptance.harness import timed
from postgres_fixture import create_owned_database, owned_database_url
from queue_capacity.fixture import CapacityDeployment
from resource_fairness.mixed import mixed_callers
from resource_fairness.public_surfaces import surfaces
from resource_fairness.scenario import CAP, prewarm, scenario


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--psql", default="psql")
    parser.add_argument(
        "--mode", choices=("compare", "fifo", "weighted"), default="compare"
    )
    parser.add_argument(
        "--weighted-first",
        action="store_true",
        help="Reverse paired run order to check for order effects",
    )
    parser.add_argument(
        "--python-client",
        type=Path,
        help="Interpreter with the candidate ledgence-client wheel installed",
    )
    args = parser.parse_args()
    if args.mode != "fifo" and (
        args.python_client is None or not args.python_client.is_file()
    ):
        parser.error(
            "--python-client must select the installed candidate wheel for weighted qualification"
        )
    if args.weighted_first and args.mode != "compare":
        parser.error("--weighted-first applies to --mode compare")
    modes = ("weighted", "fifo") if args.weighted_first else ("fifo", "weighted")
    modes = modes if args.mode == "compare" else (args.mode,)
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not __debug__ or not parent:
        parser.error(
            "enable assertions and supply the disposable LEDGENCE_POSTGRES_URL"
        )
    bins = args.binaries.resolve()
    binary = bins / "ledgence"
    if not binary.is_file():
        parser.error("--binaries must contain ledgence")
    db = "ledgence_resource_fair_" + uuid.uuid4().hex
    try:
        url = owned_database_url(parent, db)
    except ValueError as error:
        parser.error(str(error))
    tools = Path(__file__).resolve().parent
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    with binary.open("rb") as executable:
        digest = hashlib.file_digest(executable, "sha256").hexdigest()
    report = {
        "passed": False,
        "binary_sha256": digest,
        "database": db,
        "scenarios": {},
        "paired_run_order": modes,
        "database_dropped": False,
        "deployment_closed": False,
        "credential_scan_clean": None,
    }

    def admin(statement):
        result = subprocess.run(
            [
                args.psql,
                "--dbname",
                parent,
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                statement,
            ],
            check=False,
            capture_output=True,
            timeout=40,
        )
        if result.returncode:
            raise RuntimeError("test database administration failed")

    try:
        create_owned_database(admin, db)
        try:
            d = CapacityDeployment(
                tools.parent,
                evidence,
                bins,
                os.environ.get("LEDGENCE_PYTHON", sys.executable),
                url,
                args.psql,
            )
            try:
                d.command("ledgence", ["orchestrator", "migrate"])
                d.provision()
                d.enable_admission()
                d.sql((tools / "resource_fairness/observers.sql").read_text())
                d.publish(
                    "poll-fixture",
                    "1.0.0",
                    program_source=(tools / "worker_poll_plan/program.py").read_text(),
                )
                if args.mode != "fifo":
                    d.publish(
                        "resource-admission",
                        "1.0.0",
                        runtime_protocol=3,
                        program_source=(
                            tools.parent / "examples/resource-admission/program.py"
                        ).read_text(),
                    )
                d.start_server()
                for queue in ("batch", "support"):
                    d.consumer(queue, queue, CAP)
                prewarm(d)
                for mode in modes:
                    with timed(evidence, "scenario", mode):
                        report["scenarios"][mode] = scenario(d, mode)
                if args.mode != "fifo":
                    with timed(evidence, "scenario", "mixed-callers"):
                        report["scenarios"]["mixed_callers"] = mixed_callers(d)
                    with timed(evidence, "scenario", "public-surfaces"):
                        report["scenarios"]["public_surfaces"] = surfaces(
                            d, args.python_client.absolute()
                        )
            finally:
                try:
                    d.close()
                    report["deployment_closed"] = True
                finally:
                    report["credential_scan_clean"] = d.remove_credentials_and_scan()
        finally:
            admin(f'DROP DATABASE "{db}" WITH (FORCE)')
            report["database_dropped"] = True
        assert report["credential_scan_clean"]
        report["passed"] = True
    except Exception as error:  # noqa: BLE001 -- retain only sanitized failure frames
        report["failure"] = {
            "type": type(error).__name__,
            "frames": [
                {"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
                for f in traceback.extract_tb(error.__traceback__)
            ],
        }
    finally:
        report["cleanup_complete"] = (
            report["deployment_closed"] and report["database_dropped"]
        )
        (evidence / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report.get(key)
                for key in (
                    "passed",
                    "cleanup_complete",
                    "credential_scan_clean",
                    "failure",
                )
            }
        ),
        flush=True,
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
