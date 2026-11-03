#!/usr/bin/env python3
"""Verify native retained approved-effect evidence against an owned disposable PostgreSQL DB.

Requires LEDGENCE_POSTGRES_URL with CREATE DATABASE rights and built ledgence
including MCP. --python-client is an interpreter with the current wheel
installed. Uses no external providers. Retained evidence contains no credentials.
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
from effect_evidence.harness import EffectDeployment
from effect_evidence import scenarios
from postgres_fixture import create_owned_database, owned_database_url


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--python-client", type=Path, required=True)
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name an explicitly disposable PostgreSQL server")
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries.resolve()
    if not (binaries / "ledgence").is_file():
        parser.error("--binaries must contain ledgence")
    database = "ledgence_effect_evidence_" + uuid.uuid4().hex
    database_url = owned_database_url(parent_url, database)
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        directory = args.evidence.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix="ledgence-effect-evidence-"))
    directory.chmod(0o700)
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    deployment, created, passed, clean, cleanup = None, False, False, True, True
    results, failure = [], None
    binary_digest = hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest()

    def admin(sql):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1",
                                 "--command", sql], capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned database setup/cleanup failed")

    def run(name, action):
        with timed(directory, "scenario", name):
            result = action()
        results.append({"scenario": name, "result": "passed", "detail": result})
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        return result

    try:
        create_owned_database(admin, database)
        created = True
        deployment = EffectDeployment(root, directory, binaries, python, database_url, args.psql)
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.provision()
        deployment.start_server()
        access = deployment.checked("GET", "/v1/access", credentials=deployment.workers["crash-original"])
        assert access["runtime_capabilities"]["approved_effect_evidence_v2"] is True
        assert access["class"] == "worker" and access["subject"] == "crash-original"
        deployment.publish("effect-refund", "1.0.0", runtime_protocol=3,
            program_source=(root / "tools/workflow_acceptance/refund_program.py").read_text())
        (directory / "resources.json").write_text(json.dumps({"database": database,
            "scope": deployment.scope, "binary_sha256": binary_digest,
            "access_config_sha256": hashlib.sha256(deployment.config.read_bytes()).hexdigest(),
            "external_provider": False, "headless_native_server": True,
            "worker_subjects": sorted(deployment.workers)}, indent=2) + "\n")
        run("lost-claim-zero-provider-posts", lambda: scenarios.lost_claim(deployment))
        # Private lease commands stay in memory for exact receipt replay; they
        # are never exported as public operator evidence.
        with timed(directory, "scenario", "killed-worker-distinct-subject-recovery-lost-confirm"):
            recovery = scenarios.worker_replacement(deployment)
        public = scenarios.public_recovery(recovery)
        results.append({"scenario": "killed-worker-distinct-subject-recovery-lost-confirm", "result": "passed", "detail": public})
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        run("independent-observe-after-cancellation", lambda: scenarios.late_observation(deployment))
        run("api-console-cli-python-mcp-audited-reads", lambda: scenarios.read_surfaces(
            deployment, recovery["occurrence"], args.python_client.absolute()))
        run("retained-evidence-after-real-collection", lambda: scenarios.retention(deployment, recovery))
        assert hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest() == binary_digest
        passed = True
    except Exception as error:
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
