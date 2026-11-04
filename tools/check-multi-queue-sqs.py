#!/usr/bin/env python3
"""Qualify multi-queue SQS with native workers and disposable loopback infrastructure (MIT).

The benchmark records individual samples, not a throughput promise. Set
LEDGENCE_POSTGRES_URL to an explicitly disposable server that permits fixture
database creation and use --endpoint for an already running local ElasticMQ.
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
from multi_queue_sqs.fixture import MultiDeployment
from multi_queue_sqs.publication import publisher_outage
from multi_queue_sqs.scenarios import compare, rebound
from postgres_fixture import create_owned_database, owned_database_url
from provider_operations.sqs import loopback_endpoint


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--psql", default="psql")
    parser.add_argument(
        "--scenario",
        action="append",
        choices=("compare", "rebound", "startup", "publication"),
    )
    args = parser.parse_args()
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not __debug__ or not parent:
        parser.error(
            "enable assertions and supply the disposable LEDGENCE_POSTGRES_URL"
        )
    bins = args.binaries.resolve()
    if not (bins / "ledgence").is_file():
        parser.error("--binaries must contain ledgence built with the sqs feature")
    db = "ledgence_multi_sqs_" + uuid.uuid4().hex
    try:
        url = owned_database_url(parent, db)
        endpoint = loopback_endpoint(args.endpoint)
    except ValueError as error:
        parser.error(str(error))
    root = Path(__file__).resolve().parents[1]
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    with (bins / "ledgence").open("rb") as program:
        digest = hashlib.file_digest(program, "sha256").hexdigest()
    record = {
        "passed": False,
        "binary_sha256": digest,
        "database": db,
        "scenarios": {},
        "cleanup_complete": False,
        "deployment_closed": False,
        "database_dropped": False,
        "credential_scan_clean": None,
    }

    def admin(sql):
        value = subprocess.run(
            [args.psql, "--dbname", parent, "-X", "-v", "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True,
            timeout=40,
        )
        if value.returncode:
            raise RuntimeError("test database administration failed")

    try:
        create_owned_database(admin, db)
        try:
            d = MultiDeployment(
                root,
                evidence,
                bins,
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
                d.provision_routes(endpoint)
                d.start_server()
                scenarios = {
                    "startup": d.mismatched_startup,
                    "publication": lambda: publisher_outage(d),
                    "compare": lambda: compare(d),
                    "rebound": lambda: rebound(d),
                }
                for name in args.scenario or scenarios:
                    with timed(evidence, "scenario", name):
                        record["scenarios"][name] = scenarios[name]()
            finally:
                try:
                    d.close()
                    record["deployment_closed"] = True
                finally:
                    record["credential_scan_clean"] = d.remove_credentials_and_scan()
        finally:
            admin(f'DROP DATABASE "{db}" WITH (FORCE)')
            record["database_dropped"] = True
        assert record["credential_scan_clean"]
        record["passed"] = True
    except Exception as error:
        record["failure"] = {
            "type": type(error).__name__,
            "frames": [
                {"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
                for f in traceback.extract_tb(error.__traceback__)
            ],
        }
    finally:
        record["cleanup_complete"] = record["deployment_closed"] and record["database_dropped"]
        (evidence / "summary.json").write_text(json.dumps(record, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: record.get(k)
                for k in (
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
