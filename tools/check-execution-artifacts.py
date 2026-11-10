#!/usr/bin/env python3
"""Qualify execution artifacts against owned PostgreSQL/HTTP, real workers and an installed client (MIT)."""
import argparse
import hashlib
import json
import os
import runpy
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback
import uuid

from execution_artifacts.harness import ArtifactDeployment
from execution_artifacts.scenarios import CASES
from http_acceptance.harness import timed
from postgres_fixture import create_owned_database, owned_database_url


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--python-client", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--s3-owned", action="store_true", help="Use an owned pinned RustFS container with persistent-volume restart")
    args = parser.parse_args()
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent:
        parser.error("LEDGENCE_POSTGRES_URL must name an explicitly disposable PostgreSQL server")
    root = Path(__file__).resolve().parents[1]
    binary = args.binaries.resolve() / "ledgence"
    if not binary.is_file() or not args.python_client.is_file():
        parser.error("candidate binary and installed client interpreter must exist")
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    directory = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix="ledgence-artifacts-"))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    directory.chmod(0o700)
    database = "ldg_artifacts_" + uuid.uuid4().hex
    url = owned_database_url(parent, database)
    created = passed = False
    cleanup = clean = True
    deployment = failure = fixture = None
    fixture_image = None
    results = []

    def admin(sql):
        value = subprocess.run([args.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1", "--command", sql],
                               capture_output=True, timeout=40)
        assert value.returncode == 0, "owned artifact database setup/cleanup failed"

    try:
        create_owned_database(admin, database); created = True
        deployment = ArtifactDeployment(root, directory, binary.parent,
            os.environ.get("LEDGENCE_PYTHON", sys.executable), url, args.psql)
        deployment.python_client = str(args.python_client.absolute())
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.provision()
        if args.s3_owned:
            helpers = runpy.run_path(str(root / "tools/check-artifact-s3.py"))
            fixture_image = helpers["IMAGE"]
            fixture = helpers["Fixture"](directory)
            endpoint = fixture.initialize()
            bucket = "ledgence-artifact-" + uuid.uuid4().hex[:16]
            helpers["create_bucket"](endpoint, bucket)
            deployment.configure_s3(endpoint, bucket, fixture)
        deployment.start_server()
        deployment.publish_example()
        provenance = {"binary_sha256": digest,
                      "helper_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                        for path in sorted(deployment.runner.parent.glob("*.py"))},
                      "examples_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                          for path in sorted((root / "examples/execution-artifacts").glob("*.py"))}}
        (directory / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
        for case in CASES:
            print("RUN " + case.__name__, flush=True)
            with timed(directory, "scenario", case.__name__):
                detail = case(deployment)
            results.append({"scenario": case.__name__, "passed": True, "detail": detail})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == digest, "binary changed during acceptance"
        assert provenance["helper_sha256"] == {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                               for path in sorted(deployment.runner.parent.glob("*.py"))}
        passed = True
    except Exception as error:
        failure = {"type": type(error).__name__, "frames": [{"file": Path(frame.filename).name,
                   "line": frame.lineno, "function": frame.name} for frame in traceback.extract_tb(error.__traceback__)]}
        if deployment:
            try:
                rows = json.loads(deployment.sql(
                    "SELECT coalesce(json_agg(json_build_object('task_id',task_id,'state',state,'attempt_count',attempt_count)),'[]'::json) FROM tasks"))
                for row in rows[:20]:
                    if row["attempt_count"]:
                        try:
                            row["attempt"] = deployment.attempt(row["task_id"])
                        except Exception:
                            pass
                (directory / "task-diagnostics.json").write_text(json.dumps(rows, indent=2) + "\n")
            except Exception:
                pass
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
        if fixture is not None:
            try:
                cleanup = not fixture.close() and cleanup
            except Exception:
                cleanup = False
        if created:
            try:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            except Exception:
                cleanup = False
        summary = {"passed": passed and cleanup and clean, "binary_sha256": digest, "database": database,
                   "cleanup_complete": cleanup, "credential_scan_clean": clean, "scenarios": results, "failure": failure,
                   "storage": "owned-local-s3" if args.s3_owned else "filesystem",
                   "fixture_image": fixture_image,
                   "qualification_scope": "local S3-compatible composition, no AWS qualification" if args.s3_owned else "single-orchestrator persistent filesystem"}
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({key: summary[key] for key in ("passed", "cleanup_complete", "credential_scan_clean", "failure")}), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
