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
import traceback
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
        def diagnostic_text(value):
            text = str(value)
            # SQL subprocess errors include argv. Keep fixture URLs useful while
            # redacting caller-supplied PostgreSQL credentials from diagnostics.
            for url in (parent_url, database_url):
                address = urllib.parse.urlsplit(url)
                if "@" in address.netloc or address.query:
                    host = address.netloc.rsplit("@", 1)[-1]
                    redacted = urllib.parse.urlunsplit(address._replace(
                        netloc="REDACTED@" + host if "@" in address.netloc else host,
                        query="REDACTED" if address.query else ""))
                    text = text.replace(url, redacted)
            return text

        failure = diagnostic_text(traceback.format_exc())
        print(diagnostic_text(f"HTTP acceptance failed: {type(error).__name__}: {error}"), file=sys.stderr)
        print(failure, file=sys.stderr, end="")
        try:
            (directory / "failure-traceback.txt").write_text(failure)
        except OSError as diagnostic_error:
            print(diagnostic_text(f"Could not save failure traceback: {diagnostic_error}"), file=sys.stderr)
        # These are fixture-owned subprocess logs, not request headers or the
        # caller's environment. Keep CI output bounded and retain the full files.
        for process in deployment.processes[-3:] if deployment else []:
            print(f"Fixture {process.label} exit={process.process.poll()}", file=sys.stderr)
            for log in (process.stdout_path, process.stderr_path):
                print(f"--- {log.name} (last 20 lines, up to 6000 characters) ---", file=sys.stderr)
                try:
                    lines = log.read_text(errors="replace").splitlines()[-20:]
                except OSError as diagnostic_error:
                    print(diagnostic_text(f"Could not read fixture log: {diagnostic_error}"), file=sys.stderr)
                else:
                    print(diagnostic_text("\n".join(lines))[-6000:], file=sys.stderr)
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
