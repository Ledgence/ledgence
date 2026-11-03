#!/usr/bin/env python3
"""Verify real CLI/API/worker/MCP access against an owned disposable database.

Requires LEDGENCE_POSTGRES_URL naming a test server with CREATE DATABASE rights,
psql, built ledgence (including mcp), and CPython >=3.11. No cloud/provider calls.
An optional --python-client points at an interpreter with the current client
wheel installed; -I prevents source-tree/PYTHONPATH imports. Credentials exist
only in ephemeral fixture bundles and are removed before evidence is retained.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid

from http_acceptance.harness import timed
from instance_access.harness import AccessDeployment
from instance_access import scenarios
from postgres_fixture import create_owned_database, owned_database_url


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--python-client", type=Path, help="interpreter with installed current Python client wheel")
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name an explicitly disposable PostgreSQL server")
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries.resolve()
    if not (binaries / "ledgence").is_file():
        parser.error("--binaries must contain the built ledgence executable")
    database = "ledgence_access_" + uuid.uuid4().hex
    try:
        database_url = owned_database_url(parent_url, database)
    except ValueError as error:
        parser.error(str(error))
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        directory = args.evidence.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix="ledgence-instance-access-"))
    directory.chmod(0o700)
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    deployment, created, passed, clean, cleanup = None, False, False, True, True
    results = []
    failure = None
    binary_digest = hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest()

    def admin(sql):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1",
                                 "--command", sql], capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned database setup/cleanup failed")

    def record(name, result):
        results.append({"scenario": name, "result": "passed", "detail": result})
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        return result

    def run(name, action):
        with timed(directory, "scenario", name):
            result = action()
        return record(name, result)

    try:
        create_owned_database(admin, database)
        created = True
        deployment = AccessDeployment(root, directory, binaries, python, database_url, args.psql)
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.start_server()
        assert deployment.request("GET", "/v1/access")[2] == {"mode": "operator_trusted"}
        deployment.server.stop()
        deployment.provision()
        deployment.start_server()
        (directory / "resources.json").write_text(json.dumps({
            "database": database, "scope": deployment.scope,
            "binary_sha256": binary_digest,
            "access_config_sha256": hashlib.sha256(deployment.config.read_bytes()).hexdigest(),
            "python_client": str(args.python_client) if args.python_client else None,
            "headless_server": True, "external_provider": False,
        }, indent=2) + "\n")
        run("anonymous-denial", lambda: scenarios.anonymous_denial(deployment))
        run("publication-resume", lambda: scenarios.publication(deployment))
        run("console-session", lambda: scenarios.console_session(deployment))
        with timed(directory, "scenario", "worker-cli-observations"):
            worker, task_id, detail = scenarios.worker_and_cli(deployment)
        record("worker-cli-observations", detail)
        run("mcp-and-approval", lambda: scenarios.mcp_and_approval(deployment, task_id))
        if args.python_client:
            # Resolving a venv's Python symlink would select the base interpreter
            # and lose its installed packages. Make the path absolute only.
            run("installed-python-client", lambda: scenarios.python_client(deployment, args.python_client.absolute()))
        else:
            results.append({"scenario": "installed-python-client", "result": "not_requested"})
        worker.stop()
        run("restart-and-rotation", lambda: scenarios.restart_and_rotation(deployment))
        deployment.server.stop()
        assert hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest() == binary_digest, "binary changed during acceptance"
        passed = True
    except Exception as error:
        # Frame-only diagnostics deliberately exclude exception values/locals:
        # a regression must not turn the gate into a credential disclosure.
        failure = {"type": type(error).__name__, "frames": [
            {"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
            for frame in traceback.extract_tb(error.__traceback__)]}
    finally:
        if deployment:
            try:
                deployment.close()
            except Exception:
                cleanup = False
            try:
                clean = deployment.remove_credentials_and_scan()
            except Exception:
                clean = False
        if created:
            try:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            except Exception:
                cleanup = False
        summary = {"passed": passed and clean and cleanup, "database": database,
                   "credential_scan_clean": clean, "cleanup_complete": cleanup,
                   "scenarios": results, "failure": failure}
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"passed": summary["passed"], "credential_scan_clean": clean,
                          "cleanup_complete": cleanup, "evidence": str(directory), "failure": failure}), flush=True)
        if summary["passed"] and not args.evidence:
            shutil.rmtree(directory)
    return 0 if passed and clean and cleanup else 1


if __name__ == "__main__":
    sys.exit(main())
