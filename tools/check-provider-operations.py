#!/usr/bin/env python3
"""Verify typed provider operations and durable worker-free asynchronous recovery against an owned disposable PostgreSQL DB.

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
from provider_operations.harness import ProviderDeployment
from provider_operations import scenarios
from postgres_fixture import create_owned_database, owned_database_url


def helper_sources(runner):
    """Freeze the actual helper sources invoked by the historical binary."""
    return {
        str(path.relative_to(runner.parent)): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in sorted(runner.parent.rglob("*.py"))
        if "__pycache__" not in path.parts
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--python-client", type=Path, required=True)
    parser.add_argument("--scenario", choices=("all", "demo", "sqs"), default="all")
    parser.add_argument(
        "--endpoint",
        help="explicit owned loopback ElasticMQ origin; requires --scenario sqs",
    )
    parser.add_argument("--old-binary", type=Path)
    parser.add_argument("--old-runner", type=Path)
    args = parser.parse_args()
    if bool(args.endpoint) != (args.scenario == "sqs"):
        parser.error("--endpoint and --scenario sqs must be selected together")
    if args.endpoint:
        from provider_operations.sqs import loopback_endpoint

        try:
            args.endpoint = loopback_endpoint(args.endpoint)
        except ValueError as error:
            parser.error(str(error))
    if args.scenario == "all" and (args.old_binary is None or args.old_runner is None):
        parser.error(
            "full qualification requires --old-binary and --old-runner from the preserved prior release"
        )
    old_binary_digest = None
    old_helper_sources = None
    if args.old_binary is not None:
        args.old_binary = args.old_binary.resolve(strict=True)
        if args.old_binary.name != "ledgence":
            parser.error("--old-binary must name the actual ledgence executable")
        old_binary_digest = hashlib.sha256(args.old_binary.read_bytes()).hexdigest()
    if args.old_runner is not None:
        args.old_runner = args.old_runner.resolve(strict=True)
        old_helper_sources = helper_sources(args.old_runner)
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error(
            "LEDGENCE_POSTGRES_URL must name an explicitly disposable PostgreSQL server"
        )
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries.resolve()
    if not (binaries / "ledgence").is_file():
        parser.error("--binaries must contain ledgence")
    database = "ledgence_provider_operations_" + uuid.uuid4().hex
    database_url = owned_database_url(parent_url, database)
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        directory = args.evidence.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix="ledgence-provider-operations-"))
    directory.chmod(0o700)
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    deployment, created, passed, clean, cleanup = None, False, False, True, True
    results, failure = [], None
    binary_digest = hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest()

    def admin(sql):
        result = subprocess.run(
            [
                args.psql,
                "--dbname",
                parent_url,
                "-X",
                "--set",
                "ON_ERROR_STOP=1",
                "--command",
                sql,
            ],
            capture_output=True,
            timeout=40,
        )
        if result.returncode:
            raise RuntimeError("owned database setup/cleanup failed")

    def run(name, action, *, public=None):
        with timed(directory, "scenario", name):
            result = action()
        detail = public(result) if public is not None else result
        results.append({"scenario": name, "result": "passed", "detail": detail})
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        return result

    try:
        create_owned_database(admin, database)
        created = True
        if args.scenario == "sqs":
            from provider_operations.sqs import ProviderSqsDeployment

            deployment_type = ProviderSqsDeployment
        else:
            deployment_type = ProviderDeployment
        deployment = deployment_type(
            root, directory, binaries, python, database_url, args.psql
        )
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.provision()
        if args.endpoint:
            deployment.configure_delivery(args.endpoint)
        deployment.start_server()
        deployment.publish_reference()
        if args.scenario == "sqs":
            flow_proxy = deployment.authenticated_proxy()
            probe_proxy = deployment.authenticated_proxy()
            deployment.worker("late-origin", flow_proxy)
        else:
            flow, probe, flow_proxy, probe_proxy = deployment.pair()
        (directory / "resources.json").write_text(
            json.dumps(
                {
                    "database": database,
                    "scope": deployment.scope,
                    "binary_sha256": binary_digest,
                    "external_provider": False,
                    "worker_concurrency": 1,
                    "old_binary_sha256": old_binary_digest,
                    "old_helper_sources": old_helper_sources,
                },
                indent=2,
            )
            + "\n"
        )
        if args.scenario == "sqs":
            from provider_operations.sqs import SCENARIOS, run_case

            for name, lose_report in zip(SCENARIOS, (False, True), strict=True):
                run(
                    name,
                    lambda lose_report=lose_report: run_case(
                        deployment,
                        probe_proxy,
                        lose_report=lose_report,
                        interpreter=args.python_client.absolute(),
                    ),
                )
        else:
            run(
                "pending-capacity-auth-public-surfaces",
                lambda: scenarios.pending_capacity(
                    deployment, args.python_client.absolute()
                ),
            )
        if args.scenario == "all":
            run(
                "lost-provider-and-report-replies-expired-key",
                lambda: scenarios.loss_pending_expiry(
                    deployment, flow_proxy, probe_proxy
                ),
            )
            run(
                "lost-claim-and-no-lookup",
                lambda: scenarios.lost_claim_and_no_lookup(deployment, flow_proxy),
            )
            run(
                "typed-provider-terminal-and-human-action",
                lambda: scenarios.typed_outcomes(deployment),
            )
            run(
                "bounded-probe-timeout-and-capacity",
                lambda: scenarios.bounded_probe_timeout(deployment),
            )
            second = deployment.worker("lost-grant", flow_proxy)
            run(
                "concurrent-business-and-provider-domains",
                lambda: scenarios.concurrent_business(deployment),
            )
            second.stop()
            run(
                "typed-dispute-before-selection",
                lambda: scenarios.typed_dispute_before_selection(
                    deployment, flow_proxy
                ),
            )
            # Time the actual cancellation and independent late delivery. Private
            # replay input stays in memory; only the whitelist enters evidence.
            retained = run(
                "cancelled-original-report-no-revival",
                lambda: scenarios.cancelled_retained_source(deployment, flow_proxy),
                public=scenarios.public_cancelled,
            )
            run(
                "real-worker-kill-and-replacement",
                lambda: scenarios.worker_replacement(deployment),
            )
            run(
                "prior-worker-before-provider-callback",
                lambda: scenarios.old_worker_before_provider_callback(
                    deployment, args.old_binary.absolute(), args.old_runner.absolute()
                ),
            )
            run(
                "cancelled-retained-after-collection",
                lambda: scenarios.retained_after_collection(deployment, retained),
            )
            run(
                "actual-database-restore-quarantine",
                lambda: scenarios.declared_restore(deployment),
            )
        assert (
            hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest()
            == binary_digest
        )
        if args.old_binary is not None:
            assert (
                hashlib.sha256(args.old_binary.read_bytes()).hexdigest()
                == old_binary_digest
            )
        if args.old_runner is not None:
            assert helper_sources(args.old_runner) == old_helper_sources
        passed = True
    except Exception as error:
        failure = {
            "type": type(error).__name__,
            "frames": [
                {
                    "file": Path(frame.filename).name,
                    "line": frame.lineno,
                    "function": frame.name,
                }
                for frame in traceback.extract_tb(error.__traceback__)
            ],
        }
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
        (directory / "declared-restore.dump").unlink(missing_ok=True)
        if created:
            try:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            except Exception:
                cleanup = False
        summary = {
            "passed": passed and clean and cleanup,
            "scenario": args.scenario,
            "full_qualification": args.scenario == "all",
            "database": database,
            "credential_scan_clean": clean,
            "cleanup_complete": cleanup,
            "scenarios": results,
            "failure": failure,
        }
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(
            json.dumps(
                {
                    "passed": summary["passed"],
                    "credential_scan_clean": clean,
                    "cleanup_complete": cleanup,
                    "evidence": str(directory),
                    "failure": failure,
                }
            ),
            flush=True,
        )
        if summary["passed"] and not args.evidence:
            shutil.rmtree(directory)
    return 0 if passed and clean and cleanup else 1


if __name__ == "__main__":
    sys.exit(main())
