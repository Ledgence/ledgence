"""Owned native upgrade lifecycle shared by release rehearsals (MIT)."""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
from pathlib import Path
from postgres_fixture import create_owned_database, owned_database_url
from .upgrade_bundles import inputs

def main(*, predecessor, deployment, exercise, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--previous-bundle",
        required=True,
        type=Path,
        help="extracted published predecessor archive verified against its checksum",
    )
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--console-dist", required=True, type=Path)
    parser.add_argument("--candidate-runner", required=True, type=Path)
    parser.add_argument(
        "--psql",
        default="psql",
        help="PostgreSQL 18 psql; pg_dump and pg_restore must also be available",
    )
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if not __debug__:
        parser.error("enable assertions for qualification")
    root = Path(__file__).resolve().parents[2]
    old, bins, console = (
        args.previous_bundle.resolve(),
        args.binaries.resolve(),
        args.console_dist.resolve(),
    )
    runner = args.candidate_runner.resolve()
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent:
        parser.error(
            "supply an explicitly disposable PostgreSQL server with LEDGENCE_POSTGRES_URL"
        )
    psql = shutil.which(args.psql)
    if not psql:
        parser.error("psql is required")
    pgtools = {}
    for name in ("pg_dump", "pg_restore"):
        sibling = Path(psql).with_name(name)
        executable = str(sibling) if sibling.is_file() else shutil.which(name)
        if not executable:
            parser.error(f"{name} is required")
        pgtools[name] = executable
    provenance = inputs(old, bins, console, runner, predecessor=predecessor)
    names = ["ldg_upgrade_" + uuid.uuid4().hex for _ in range(2)]
    urls = [owned_database_url(parent, name) for name in names]
    directory = args.evidence.resolve()
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    report = {
        "passed": False,
        "provenance": provenance,
        "databases": names,
        "database_cleanup": False,
        "process_cleanup": False,
        "credential_scan_clean": None,
    }

    def admin(sql):
        result = subprocess.run(
            [psql, "--dbname", parent, "-X", "-v", "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True,
            timeout=60,
            check=False,
        )
        if result.returncode:
            raise RuntimeError("owned database administration failed")

    owned = []
    exercise_passed = False
    try:
        for name in names:
            # CREATE may commit before its reply is lost, and its immediate
            # compensating DROP may fail. Retain the attempted name for the
            # final cleanup even when create_owned_database raises.
            owned.append(name)
            create_owned_database(admin, name)
        d = deployment(
            root,
            directory,
            old / "bin",
            os.environ.get("LEDGENCE_PYTHON", sys.executable),
            urls[0],
            psql,
        )
        try:
            with tempfile.TemporaryDirectory(
                prefix="ledgence-owned-upgrade-"
            ) as scratch:
                report["exercise"] = exercise(
                    d,
                    old,
                    bins,
                    console,
                    runner,
                    urls[1],
                    Path(scratch) / "backup.dump",
                    pgtools,
                )
        finally:
            try:
                d.close()
                report["process_cleanup"] = True
            finally:
                report["credential_scan_clean"] = d.remove_credentials_and_scan()
        assert report["credential_scan_clean"]
        exercise_passed = True
    except Exception as exc:  # noqa: BLE001 - report frames without connection or credential values.
        report["failure_type"] = type(exc).__name__
        report["frames"] = [
            {"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
            for f in traceback.extract_tb(exc.__traceback__)
        ]
    finally:
        try:
            report["provenance_after"] = inputs(old, bins, console, runner, predecessor=predecessor)
            if report["provenance_after"] != provenance:
                raise ValueError("bundle bytes changed during rehearsal")
        except Exception as error:
            exercise_passed = False
            report["integrity_failure"] = type(error).__name__
        cleanup_errors = []
        for name in reversed(owned):
            try:
                admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
                cleanup_errors.append(
                    {"database": name, "failure_type": type(exc).__name__}
                )
        report["database_cleanup"] = not cleanup_errors
        if cleanup_errors:
            report["cleanup_errors"] = cleanup_errors
        report["passed"] = exercise_passed and report["database_cleanup"]
        (directory / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: report[k]
                for k in (
                    "passed",
                    "database_cleanup",
                    "process_cleanup",
                    "credential_scan_clean",
                )
            }
        ),
        flush=True,
    )
    return 0 if report["passed"] and report["database_cleanup"] else 1

