#!/usr/bin/env python3
"""Qualify the local cost and correctness of shared resource and run-budget admission (MIT)."""

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

from http_acceptance.harness import eventually
from resource_admission.load import (
    MODES,
    SLOTS,
    Deployment,
    parked,
    skew,
    throttle,
    timing_arm,
    warm,
)
from resource_admission.load_provider import Provider

POSTGRES = "postgres:18.6@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
ROOT = Path(__file__).resolve().parents[1]


def command(args):
    return subprocess.check_output(
        args, text=True, stderr=subprocess.DEVNULL, timeout=90
    ).strip()


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


@contextmanager
def database(report, psql):
    # An ephemeral loopback port, a uniquely owned container and its volume. This
    # keeps cluster-wide WAL and pg_stat_statements evictions isolated per run.
    ident = uuid.uuid4().hex
    name = "ledgence-resource-load-" + ident
    owner = "ledgence.qualifier=" + ident
    pgid = None
    volumes = []
    try:
        pgid = command(
            [
                "docker",
                "create",
                "--name",
                name,
                "--label",
                owner,
                "--publish",
                "127.0.0.1::5432",
                "--env",
                "POSTGRES_PASSWORD=load-local",
                "--env",
                "POSTGRES_DB=ledgence",
                POSTGRES,
                "postgres",
                "-c",
                "shared_preload_libraries=pg_stat_statements",
                "-c",
                "pg_stat_statements.max=10000",
            ]
        )
        mounts = json.loads(
            command(["docker", "inspect", "--format", "{{json .Mounts}}", pgid])
        )
        volumes = [m["Name"] for m in mounts if m["Type"] == "volume"]
        if len(volumes) != 1:
            raise RuntimeError("expected one owned Postgres data volume")
        command(["docker", "start", pgid])
        address = command(["docker", "port", pgid, "5432/tcp"])
        if not address.startswith("127.0.0.1:") or not address[10:].isdecimal():
            raise ValueError("expected a private loopback PostgreSQL port")
        url = "postgres://postgres:load-local@" + address + "/ledgence"

        def ready():
            # The image briefly serves on a Unix socket during initialization.
            # Use the actual host TCP route which the orchestrator will use.
            return (
                subprocess.run(
                    [psql, url, "-XAtc", "SELECT 1"],
                    capture_output=True,
                    timeout=8,
                    check=False,
                ).returncode
                == 0
            )

        eventually(ready, timeout=50, description="qualifier PostgreSQL ready")
        report["postgres"] = {
            "image": POSTGRES,
            "container": pgid,
            "port": int(address[10:]),
        }
        yield url
    finally:
        # Covers an interrupted create whose response was lost. Check our label
        # before deleting the uniquely named fixture or its anonymous volume.
        found = command(
            ["docker", "ps", "-aq", "--no-trunc", "--filter", "label=" + owner]
        )
        if found:
            assert not pgid or found == pgid
            command(["docker", "rm", "-fv", found])
        remaining = set(command(["docker", "volume", "ls", "-q"]).splitlines())
        assert not remaining.intersection(volumes), (
            "owned PostgreSQL data volume survived cleanup"
        )
        report["database_removed"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--psql", default="psql")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="small functional check; not a release timing result",
    )
    parser.add_argument(
        "--scenario", choices=("all", "timing", "pressure"), default="all"
    )
    parser.add_argument(
        "--candidate-sha", help="exact clean committed source revision for measurement"
    )
    parser.add_argument(
        "--binary-sha256",
        help="verified SHA-256 from optimized candidate build provenance",
    )
    a = parser.parse_args()
    if not __debug__:
        parser.error("assertions must be enabled")
    binary = a.binaries.resolve() / "ledgence"
    sha = digest(binary)
    commit = command(["git", "-C", str(ROOT), "rev-parse", "HEAD"])
    dirty = bool(command(["git", "-C", str(ROOT), "status", "--porcelain"]))
    if not a.smoke and (a.candidate_sha != commit or a.binary_sha256 != sha or dirty):
        parser.error(
            "measurement requires exact clean --candidate-sha and verified optimized --binary-sha256"
        )
    evidence = a.evidence.resolve()
    evidence.mkdir(parents=True, mode=0o700, exist_ok=False)
    helper = ROOT / "sdk/python/ledgence/worker"
    helpers = {p.name: digest(p) for p in sorted(helper.glob("*.py"))}
    report = {
        "passed": False,
        "measurement": not a.smoke,
        "scenario": a.scenario,
        "source_commit": commit,
        "source_dirty": dirty,
        "binary_sha256": sha,
        "binary_version": command([str(binary), "--version"]),
        "helper_sha256": helpers,
        "system": platform.platform(),
        "machine": platform.machine(),
        "slots": SLOTS,
        "timing_arms": [],
        "database_removed": False,
        "deployment_closed": False,
        "credential_scan_clean": False,
    }
    try:
        with database(report, a.psql) as url:
            d = Deployment(
                ROOT,
                evidence,
                binary.parent,
                os.environ.get("LEDGENCE_PYTHON", sys.executable),
                url,
                a.psql,
            )
            p = None
            try:
                d.environment["RUST_LOG"] = "warn"
                d.command("ledgence", ["orchestrator", "migrate"])
                report["postgres"]["server_version"] = d.sql("SHOW server_version")
                d.sql("CREATE EXTENSION pg_stat_statements")
                d.provision()
                d.enable_admission()
                d.publish_programs()
                d.start_server()
                for name in ["load-hot", *(f"load-{i}" for i in range(8))]:
                    d.configure(name)
                d.consumer("load-main", d.queue, SLOTS)
                p = Provider()
                warm(d, p)
                if a.scenario in ("all", "timing"):
                    rounds = 1 if a.smoke else 3
                    total = 12 if a.smoke else 96
                    for i in range(rounds):
                        for mode in MODES if i % 2 == 0 else reversed(MODES):
                            report["timing_arms"].append(
                                timing_arm(d, p, mode, f"round{i}-{mode}", total)
                            )
                if a.scenario in ("all", "pressure"):
                    report["parked"] = parked(d, p, 16 if a.smoke else 512)
                    report["late_interactive"] = skew(d, p)
                    report["retry_after"] = throttle(d, p)
                residual = int(
                    d.sql(
                        "SELECT count(*) FROM admission_resources WHERE offered<>0 OR started<>0 OR unknown<>0"
                    )
                )
                assert residual == 0
                report["occupied_resources_after_drain"] = residual
                all_expected = SLOTS + sum(
                    r["physical_calls"] for r in report["timing_arms"]
                )
                if a.scenario in ("all", "pressure"):
                    all_expected += (
                        report["parked"]["ordinary_witness"]
                        + report["parked"]["completed_after_restart"]
                    )
                    all_expected += report["late_interactive"]["physical_sends"] + sum(
                        len(t["requests"]) for t in report["retry_after"]
                    )
                assert len(p.rows) == all_expected
                report["provider_records"] = len(p.rows)
                assert digest(binary) == sha and helpers == {
                    f.name: digest(f) for f in sorted(helper.glob("*.py"))
                }
            finally:
                try:
                    if p:
                        p.close()
                finally:
                    try:
                        d.close()
                        report["deployment_closed"] = True
                    finally:
                        report["credential_scan_clean"] = (
                            d.remove_credentials_and_scan()
                        )
        assert (
            report["database_removed"]
            and report["deployment_closed"]
            and report["credential_scan_clean"]
        )
        if not a.smoke:
            assert command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]) == commit
            assert not command(["git", "-C", str(ROOT), "status", "--porcelain"])
        report["passed"] = True
    finally:
        (evidence / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "passed",
                    "measurement",
                    "database_removed",
                    "credential_scan_clean",
                )
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
