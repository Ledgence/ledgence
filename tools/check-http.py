"""Run separate orchestrator/worker/CLI binaries against an owned disposable database.

Requires LEDGENCE_POSTGRES_URL (a test server whose role can create databases),
psql, and CPython >=3.11. Creates/drops a unique database; does not restart the
database server. Full contract/fault coverage also includes Rust adapter tests.
"""

import argparse
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.parse
import uuid

from http_acceptance.harness import Deployment
from http_acceptance.scenarios import SCENARIOS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--binaries", type=Path, help="directory with built debug binaries")
    parser.add_argument("--evidence", type=Path, help="retain test files/logs in a new directory")
    parser.add_argument("--scenario", action="append", choices=[f.__name__ for f in SCENARIOS])
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name a disposable test PostgreSQL server")
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries
    if binaries is None:
        subprocess.run(["cargo", "build", "--workspace", "--bins", "--locked"], cwd=root, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], cwd=root))
        binaries = Path(metadata["target_directory"]) / "debug"
    binaries = binaries.resolve()
    for name in ("ledgence", "ledgence-worker", "ledgence-orchestrator"):
        if not (binaries / name).is_file():
            parser.error(f"missing executable {binaries / name}")
    temporary = None
    succeeded = False
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        directory = args.evidence.resolve()
    else:
        temporary = tempfile.mkdtemp(prefix="ledgence-http-acceptance-")
        directory = Path(temporary)
    database = "ledgence_http_" + uuid.uuid4().hex
    parsed = urllib.parse.urlsplit(parent_url)
    database_url = urllib.parse.urlunsplit(parsed._replace(path="/" + database))

    def admin(sql):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1",
                                 "--command", sql], capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned test database setup/cleanup failed")

    deployment = None
    created = False
    try:
        admin(f'CREATE DATABASE "{database}"')
        created = True
        deployment = Deployment(root, directory, binaries, python, database_url, args.psql)
        # Migration command may emit operational logs rather than a JSON result.
        result = subprocess.run([str(binaries / "ledgence-orchestrator"), "migrate"],
                                env=deployment.environment, capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("explicit migration command failed: " + result.stderr.decode(errors="replace")[-3000:])
        deployment.server, _ = deployment.start_server()
        results = []
        for scenario in SCENARIOS:
            if args.scenario and scenario.__name__ not in args.scenario:
                continue
            print(f"RUN {scenario.__name__}", flush=True)
            detail = scenario(deployment)
            results.append({"scenario": scenario.__name__, "result": "passed", "detail": detail})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"PASS {scenario.__name__}: {detail}", flush=True)
        deployment.server.stop()
        print(f"HTTP separate-process acceptance passed: {len(results)} scenarios", flush=True)
        succeeded = True
        return 0
    except (AssertionError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"HTTP acceptance failed: {error}", file=sys.stderr)
        print(f"Evidence: {directory}", file=sys.stderr)
        return 1
    finally:
        try:
            if deployment:
                deployment.close()
        finally:
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            if temporary and succeeded:
                shutil.rmtree(temporary)


if __name__ == "__main__":
    sys.exit(main())
