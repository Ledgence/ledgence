#!/usr/bin/env python3
"""Exercise per-queue concurrency through real workers, PostgreSQL, and optional ElasticMQ (MIT).

Set LEDGENCE_POSTGRES_URL to an explicitly disposable PostgreSQL server.
The worker and orchestrator use the binary from --binaries. All resources are local.
"""

import argparse
import asyncio
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
from queue_capacity.fixture import CapacityDeployment
from queue_capacity.scenarios import (
    workers_and_limits,
    surfaces,
    timeout_modes,
    installed_client,
)
from runpy import run_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument(
        "--evidence", type=Path, required=True, help="new local evidence directory"
    )
    parser.add_argument("--endpoint", help="explicit local ElasticMQ endpoint")
    parser.add_argument("--console-dist", type=Path)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=("workers", "surfaces", "timeouts", "sdk", "browser"),
    )
    parser.add_argument("--browser-command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    selected = args.scenario or ["workers", "surfaces", "timeouts"]
    if "browser" in selected and not (
        args.browser_command and args.console_dist and args.console_dist.is_dir()
    ):
        parser.error("browser requires --console-dist and --browser-command")
    if not __debug__:
        parser.error("run with assertions enabled")
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent:
        parser.error("set LEDGENCE_POSTGRES_URL to an explicitly disposable server")
    binaries = args.binaries.resolve()
    if not (binaries / "ledgence").is_file():
        parser.error("--binaries must contain the built ledgence executable")
    database = "ledgence_queue_cap_" + uuid.uuid4().hex
    try:
        url = owned_database_url(parent, database)
    except ValueError as error:
        parser.error(str(error))
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[1]
    record = {
        "passed": False,
        "database": database,
        "cleanup_complete": False,
        "credential_scan_clean": None,
        "binary_sha256": hashlib.sha256(
            (binaries / "ledgence").read_bytes()
        ).hexdigest(),
        "scenarios": {},
    }

    def admin(sql):
        result = subprocess.run(
            [args.psql, "--dbname", parent, "-X", "-v", "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True,
            timeout=40,
        )
        if result.returncode:
            raise RuntimeError("owned database administration failed")

    try:
        create_owned_database(admin, database)
        try:
            d = CapacityDeployment(
                root,
                evidence,
                binaries,
                os.environ.get("LEDGENCE_PYTHON", sys.executable),
                url,
                args.psql,
            )
            try:
                d.command("ledgence", ["orchestrator", "migrate"])
                d.provision()
                d.publish(
                    "poll-fixture",
                    "1.0.0",
                    program_source=(
                        root / "tools/worker_poll_plan/program.py"
                    ).read_text(),
                )
                if args.endpoint:
                    d.configure_broker(args.endpoint)
                if args.console_dist:
                    d.console_dist = args.console_dist.resolve()
                d.start_server()

                def browser():
                    environment = dict(
                        os.environ,
                        LEDGENCE_CONSOLE_URL=d.server_url + "/console/",
                        LEDGENCE_QUEUE_OPERATOR_FILE=str(d.operator),
                        LEDGENCE_QUEUE_READER_FILE=str(d.reader),
                        LEDGENCE_CONSOLE_LIVE_SUITE="queue-policies",
                    )
                    run_path(str(root / "tools/check-console.py"))[
                        "run_browser_command"
                    ](args.browser_command, root, environment, evidence)
                    return {"credential_bound_browser_suite": True}

                scenarios = {
                    "workers": lambda: workers_and_limits(d),
                    "surfaces": lambda: surfaces(d),
                    "timeouts": lambda: timeout_modes(d),
                    "sdk": lambda: asyncio.run(installed_client(d)),
                    "browser": browser,
                }
                for name in selected:
                    with timed(evidence, "scenario", name):
                        record["scenarios"][name] = scenarios[name]()
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
        record["failure"] = {
            "type": type(exc).__name__,
            "frames": [
                {"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
                for f in traceback.extract_tb(exc.__traceback__)
            ],
        }
    finally:
        (evidence / "summary.json").write_text(json.dumps(record, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: record.get(key)
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
    return 0 if record["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
