#!/usr/bin/env python3
"""Rehearse 0.8.0 → 0.9.0 migration and externally declared restore quarantine (MIT)."""

import argparse
import hashlib
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
from resource_admission.upgrade import UpgradeDeployment, exercise


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def bundle_inputs(previous, binaries, console, runner):
    old, new = previous / "bin/ledgence", binaries / "ledgence"
    provenance = json.loads((previous / "provenance.json").read_text())
    if (
        provenance.get("release_tag") != "v0.8.0"
        or provenance.get("release_commit")
        != "e95f0baac01d443e0e72c3ffa5010ef5a28c6e6b"
    ):
        raise ValueError("expected the published 0.8.0 release bundle")
    manifest = dict(
        line.split("  ", 1)[::-1]
        for line in (previous / "SHA256SUMS").read_text().splitlines()
    )
    verified = {}
    for path in [old, *sorted((previous / "runtime/ledgence/worker").glob("*.py"))]:
        relative = str(path.relative_to(previous))
        checksum = digest(path)
        if manifest.get(relative) != checksum:
            raise ValueError("predecessor executable or helper checksum mismatch")
        verified[relative] = checksum
    if "runtime/ledgence/worker/bootstrap.py" not in verified:
        raise ValueError("missing predecessor Python helper")
    versions = [
        subprocess.check_output([binary, "--version"], text=True).strip()
        for binary in (old, new)
    ]
    if versions != ["ledgence 0.8.0", "ledgence 0.9.0"] or not runner.is_file():
        raise ValueError(
            "expected distinct 0.8.0 and 0.9.0 binaries and an installed candidate helper"
        )
    console_manifest = json.loads((console / "console-manifest.json").read_text())
    if console_manifest["console_version"] != "0.9.0":
        raise ValueError("candidate Console version does not match the native release")
    return {
        "old_release_commit": provenance["release_commit"],
        "old_manifest_sha256": digest(previous / "SHA256SUMS"),
        "old_verified_files": verified,
        "new_binary_sha256": digest(new),
        "versions": versions,
        "new_helper": {p.name: digest(p) for p in sorted(runner.parent.glob("*.py"))},
        "console_manifest_sha256": digest(console / "console-manifest.json"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--previous-bundle",
        required=True,
        type=Path,
        help="extracted 0.8.0 archive verified against its published checksum",
    )
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--console-dist", required=True, type=Path)
    parser.add_argument("--candidate-runner", type=Path)
    parser.add_argument(
        "--psql",
        default="psql",
        help="PostgreSQL 18 psql; pg_dump and pg_restore must also be available",
    )
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if not __debug__:
        parser.error("enable assertions for qualification")
    root = Path(__file__).resolve().parents[1]
    old, bins, console = (
        args.previous_bundle.resolve(),
        args.binaries.resolve(),
        args.console_dist.resolve(),
    )
    runner = (
        args.candidate_runner or root / "sdk/python/ledgence/worker/bootstrap.py"
    ).resolve()
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
    provenance = bundle_inputs(old, bins, console, runner)
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
            create_owned_database(admin, name)
            owned.append(name)
        d = UpgradeDeployment(
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
        assert provenance == bundle_inputs(old, bins, console, runner)
        exercise_passed = True
    except Exception as exc:  # noqa: BLE001 - report frames without connection or credential values.
        report["failure_type"] = type(exc).__name__
        report["frames"] = [
            {"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
            for f in traceback.extract_tb(exc.__traceback__)
        ]
    finally:
        cleanup_errors = []
        for name in reversed(owned):
            try:
                admin(f'DROP DATABASE "{name}" WITH (FORCE)')
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


if __name__ == "__main__":
    sys.exit(main())
