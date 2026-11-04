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
    parser.add_argument("--metrics-capture", type=Path, help="Local OTLP capture executable for the run-budget metric gate")
    parser.add_argument("--previous-bundle", type=Path, help="Verified predecessor native bundle for the optional protected dispatch compatibility check")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--provider", action="store_true", help="Qualify composed protected provider dispatch admission")
    mode.add_argument("--run-budget", action="store_true", help="Qualify root and descendant budgets across restart and replay")
    mode.add_argument("--task", action="store_true", help="Qualify standalone task admission at the subprocess invocation boundary")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--console-dist", type=Path, help="Built Console for budget browser qualification")
    parser.add_argument("--browser-command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.console_dist or args.browser_command is not None:
        if not args.run_budget or not args.console_dist or not args.browser_command or not (args.console_dist/"console-manifest.json").is_file():
            parser.error("budget browser qualification requires --run-budget, --console-dist and --browser-command")
    if args.previous_bundle and (not args.provider or not all((args.previous_bundle / path).is_file() for path in ("bin/ledgence", "runtime/ledgence/worker/bootstrap.py"))):
        parser.error("--previous-bundle requires --provider and an extracted native bundle")
    if args.metrics_capture and (not args.run_budget or not args.metrics_capture.is_file() or not os.access(args.metrics_capture, os.X_OK)):
        parser.error("--metrics-capture requires --run-budget and an executable local capture")
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
        if args.task:
            from resource_admission.task import TaskAdmissionDeployment
            from resource_admission.task_scenarios import CASES as cases
            deployment_type = TaskAdmissionDeployment
        elif args.run_budget:
            from resource_admission.budgets import BudgetDeployment, CASES as cases
            deployment_type = BudgetDeployment
        elif args.provider:
            from resource_admission.protected import ProtectedAdmissionDeployment
            from resource_admission.protected_scenarios import CASES as cases
            deployment_type = ProtectedAdmissionDeployment
        else:
            deployment_type, cases = AdmissionDeployment, CASES
        deployment = deployment_type(root, directory, binary.parent, os.environ.get("LEDGENCE_PYTHON", sys.executable), url, args.psql)
        deployment.command("ledgence", ["orchestrator", "migrate"])
        if args.previous_bundle:
            from resource_admission.legacy_budget import prior_worker
            deployment.previous_bundle = args.previous_bundle.resolve()
            cases = [*cases, prior_worker]
        deployment.python_client = str(args.python_client.absolute())
        if args.console_dist:
            deployment.console_dist = args.console_dist.resolve()
            deployment.browser_command = args.browser_command
            cases = [case for case in cases if case.__name__ == "last_call"]
        deployment.provision()
        if args.endpoint:
            deployment.configure_delivery(args.endpoint)
        deployment.start_server()
        deployment.publish_example()
        if args.previous_bundle:
            deployment.publish_reference()
        if args.task:
            from http_acceptance.harness import PROGRAM
            program_source = PROGRAM.encode()
        else:
            example = "examples/provider-admission/program.py" if args.provider else "examples/resource-admission/program.py"
            program_source = (root / example).read_bytes()
        provenance = {
            "helper_sha256": {str(path.relative_to(deployment.runner.parent)): hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in sorted(deployment.runner.parent.rglob("*.py"))},
            "program_source_sha256": hashlib.sha256(program_source).hexdigest(),
            "artifacts_sha256": deployment.store_hashes(),
        }
        if args.metrics_capture:
            with args.metrics_capture.open("rb") as stream:
                provenance["metrics_capture_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        if args.previous_bundle:
            old = deployment.previous_bundle / "bin/ledgence"
            provenance["previous_binary_version"] = subprocess.check_output([old, "--version"], text=True).strip()
            with old.open("rb") as stream:
                provenance["previous_binary_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
            previous_helper = deployment.previous_bundle / "runtime/ledgence/worker"
            provenance["previous_helper_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                                    for path in sorted(previous_helper.glob("*.py"))}
        if args.run_budget:
            provenance["budget_example_sha256"] = hashlib.sha256((root / "examples/shared-run-budget/program.py").read_bytes()).hexdigest()
        if args.task:
            provenance["workflow_example_sha256"] = hashlib.sha256((root / "examples/resource-admission/program.py").read_bytes()).hexdigest()
            provenance["hello_task_sha256"] = hashlib.sha256((root / "examples/hello-ledgence/task/program.py").read_bytes()).hexdigest()
            provenance["task_submit_example_sha256"] = hashlib.sha256((root / "examples/resource-admission/submit_task.py").read_bytes()).hexdigest()
        (directory / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
        if args.provider:
            from provider_operations.harness import provider_class
            provider = provider_class(root)(directory / "synthetic-provider", reply_timeout=30).__enter__()
        elif not args.task:
            provider = classifier(root)
        for case in cases:
            if args.task:
                # Each scenario owns independent policy/rate history on its own key.
                deployment.resource_name = "task-" + case.__name__.replace("_", "-")
            print("RUN " + case.__name__, flush=True)
            with timed(directory, "scenario", case.__name__):
                detail = case(deployment, provider)
            results.append({"scenario": case.__name__, "passed": True, "detail": detail})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        if args.metrics_capture:
            from resource_admission.metrics import verify_metrics
            with timed(directory, "scenario", "admission-budget-metrics"):
                detail = verify_metrics(deployment, provider, args.metrics_capture.resolve())
            results.append({"scenario": "admission-budget-metrics", "passed": True, "detail": detail})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        assert provenance["helper_sha256"] == {str(path.relative_to(deployment.runner.parent)): hashlib.sha256(path.read_bytes()).hexdigest()
                                              for path in sorted(deployment.runner.parent.rglob("*.py"))}
        assert provenance["artifacts_sha256"] == deployment.store_hashes()
        if args.previous_bundle:
            with (deployment.previous_bundle / "bin/ledgence").open("rb") as stream:
                assert provenance["previous_binary_sha256"] == hashlib.file_digest(stream, "sha256").hexdigest()
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
        summary = {"passed": passed and clean and cleanup, "operation_kind": "task" if args.task else "provider" if args.provider else "generic", "delivery": "elasticmq" if args.endpoint else "integrated-http",
                   "binary_sha256": binary_digest, "database": database,
                   "credential_scan_clean": clean, "cleanup_complete": cleanup, "scenarios": results, "failure": failure}
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({key: summary[key] for key in ("passed", "delivery", "credential_scan_clean", "cleanup_complete", "failure")}), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
