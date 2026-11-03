#!/usr/bin/env python3
"""Qualify durable admission with real workers and an owned local synthetic provider.

Requires a disposable PostgreSQL parent URL via LEDGENCE_POSTGRES_URL.
--endpoint additionally runs the same scenarios through an owned ElasticMQ queue.
No external model/provider is contacted. Evidence excludes temporary credentials.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback
import uuid

from postgres_fixture import create_owned_database, owned_database_url
from http_acceptance.harness import timed
from resource_admission.harness import AdmissionDeployment, classifier
from resource_admission.scenarios import CASES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--python-client", type=Path, required=True, help="Interpreter containing the installed candidate ledgence-client package")
    parser.add_argument("--endpoint")
    parser.add_argument("--provider", action="store_true", help="Qualify composed protected provider dispatch admission")
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent:
        parser.error("LEDGENCE_POSTGRES_URL must name an explicitly disposable PostgreSQL server")
    if args.endpoint:
        from provider_operations.sqs import loopback_endpoint
        loopback_endpoint(args.endpoint)
    root = Path(__file__).resolve().parents[1]
    binary = args.binaries.resolve() / "ledgence"
    if not binary.is_file():
        parser.error("--binaries must contain ledgence")
    binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    database = "ledgence_admission_" + uuid.uuid4().hex
    url = owned_database_url(parent, database)
    directory = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix="ledgence-admission-"))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    directory.chmod(0o700)
    created, passed, cleanup, clean = False, False, True, True
    deployment, provider, failure = None, None, None
    results = []
    provenance = {}

    def admin(sql):
        result = subprocess.run([args.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1", "--command", sql], capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned database setup/cleanup failed")

    try:
        create_owned_database(admin, database); created = True
        if args.provider:
            from resource_admission.protected import ProtectedAdmissionDeployment
            from resource_admission.protected_scenarios import CASES as cases
            deployment_type = ProtectedAdmissionDeployment
        else:
            deployment_type, cases = AdmissionDeployment, CASES
        deployment = deployment_type(root, directory, binary.parent, os.environ.get("LEDGENCE_PYTHON", sys.executable), url, args.psql)
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.python_client = str(args.python_client.absolute())
        deployment.provision()
        if args.endpoint:
            deployment.configure_delivery(args.endpoint)
        deployment.start_server()
        deployment.publish_example()
        provenance = {
            "helper_sha256": {str(path.relative_to(deployment.runner.parent)): hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in sorted(deployment.runner.parent.rglob("*.py"))},
            "program_source_sha256": hashlib.sha256((root / ("examples/provider-admission/program.py" if args.provider else "examples/resource-admission/program.py")).read_bytes()).hexdigest(),
            "artifacts_sha256": deployment.store_hashes(),
        }
        (directory / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
        if args.provider:
            from provider_operations.harness import provider_class
            provider = provider_class(root)(directory / "synthetic-provider", reply_timeout=30).__enter__()
        else:
            provider = classifier(root)
        for case in cases:
            print("RUN " + case.__name__, flush=True)
            with timed(directory, "scenario", case.__name__):
                detail = case(deployment, provider)
            results.append({"scenario": case.__name__, "passed": True, "detail": detail})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        assert provenance["helper_sha256"] == {str(path.relative_to(deployment.runner.parent)): hashlib.sha256(path.read_bytes()).hexdigest()
                                              for path in sorted(deployment.runner.parent.rglob("*.py"))}
        assert provenance["artifacts_sha256"] == deployment.store_hashes()
        (directory / "delivery-census.json").write_text(json.dumps(deployment.delivery_census(), indent=2) + "\n")
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == binary_digest, "candidate binary changed during acceptance"
        passed = True
    except Exception as error:
        failure = {"type": type(error).__name__, "frames": [{"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name} for frame in traceback.extract_tb(error.__traceback__)]}
    finally:
        if provider:
            try:
                (directory / "provider-census.json").write_text(json.dumps(provider.census(), indent=2) + "\n")
            except Exception:
                passed = False
            try:
                if args.provider:
                    provider.__exit__(None, None, None)
                else:
                    provider.close()
            except Exception:
                cleanup = False
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
        summary = {"passed": passed and clean and cleanup, "operation_kind": "provider" if args.provider else "generic", "delivery": "elasticmq" if args.endpoint else "integrated-http",
                   "binary_sha256": binary_digest, "database": database,
                   "credential_scan_clean": clean, "cleanup_complete": cleanup, "scenarios": results, "failure": failure}
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({key: summary[key] for key in ("passed", "delivery", "credential_scan_clean", "cleanup_complete", "failure")}), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
