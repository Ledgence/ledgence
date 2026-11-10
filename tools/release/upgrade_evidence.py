"""Require complete, bundle-bound predecessor rehearsals before candidate upload (MIT)."""
import argparse
import json
import sys
from pathlib import Path
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release.upgrade_bundles import digest, inputs


def verify(directory):
    directory = Path(directory)
    root = Path(__file__).resolve().parents[2]
    version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    expected = "ledgence " + version
    candidate = directory / "candidate"
    candidate_hash = digest(candidate / "bin/ledgence")
    for previous in ("0.6.1", "0.7.0"):
        evidence = directory / "evidence" / previous / "results.json"
        report = json.loads(evidence.read_text())
        old_hash = digest(directory / f"previous-{previous}/bin/ledgence")
        if (report.get("previous_version") != "ledgence " + previous
                or report.get("candidate_version") != expected
                or report.get("executable_sha256") != {"previous": old_hash, "candidate": candidate_hash}
                or old_hash == candidate_hash):
            raise ValueError(f"{evidence} does not qualify the selected executables")
    for previous in ("0.8.0", "0.9.0"):
        evidence = directory / "evidence" / previous / "summary.json"
        report = json.loads(evidence.read_text())
        if any(report.get(key) is not True for key in ("passed", "database_cleanup", "process_cleanup", "credential_scan_clean")):
            raise ValueError(f"{evidence} is incomplete")
        selected = inputs(directory / ("previous-" + previous), candidate / "bin", candidate / "console",
                          candidate / "runtime/ledgence/worker/bootstrap.py", predecessor=previous, execute=False)
        if report.get("provenance") != selected or report.get("provenance_after") != selected:
            raise ValueError(f"{evidence} does not qualify the selected immutable bundles")
        if previous == "0.9.0":
            exercise = report.get("exercise", {})
            required = {"double_migration": True, "fresh_restore": True, "restored_rows_unchanged": True}
            if any(exercise.get(key) is not value for key, value in required.items()):
                raise ValueError("artifact upgrade omitted migration or restore proof")
            groups = {
                "artifacts": ("old_credential_denied_artifact_write", "preserved_root_accepts_artifact", "same_key_replays_reference", "restart_verified_bytes"),
                "interrupted_migration": ("observed_transactional_ddl", "nonzero_exit", "artifact_migration_rolled_back", "historical_rows_unchanged"),
                "old_binary_rejection": ("nonzero_exit", "schema_error_verified", "readiness_never_opened", "historical_rows_unchanged"),
                "resumed": ("queued_task_completed_once", "parked_admission_completed_once", "fork_join_completed_once",
                            "local_step_not_reexecuted", "callback_delivered", "submission_and_queue_receipts_replayed"),
            }
            for group, fields in groups.items():
                if any(exercise.get(group, {}).get(key) is not True for key in fields):
                    raise ValueError("artifact upgrade omitted " + group + " proof")
            if ("20261110000000" not in exercise.get("applied_migrations", [])
                    or not exercise.get("old_rows", {}).get("sha256") or not exercise.get("backup_sha256")):
                raise ValueError("artifact upgrade omitted migration, byte preservation or backup evidence")
    print("Verified 0.6.1, 0.7.0, 0.8.0 and 0.9.0 upgrade evidence for " + expected)


def verify_s3(evidence, candidate):
    report = json.loads((Path(evidence) / "summary.json").read_text())
    source = json.loads((Path(candidate) / "provenance.json").read_text())["source_commit"]
    harness = Path(__file__).resolve().parents[1] / "check-artifact-s3.py"
    if (any(report.get(key) is not True for key in ("passed", "restart_validated", "cleanup_complete"))
            or report.get("source_dirty") is not False or report.get("source_sha") != source
            or report.get("harness_sha256") != digest(harness) or report.get("phases") != ["write", "read"]
            or report.get("fixture_image") != "docker.io/rustfs/rustfs:1.0.4@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2"):
        raise ValueError("artifact S3 gate lacks clean candidate-bound production adapter and restart evidence")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--s3", type=Path)
    args = parser.parse_args()
    verify(args.directory)
    if args.s3:
        verify_s3(args.s3, args.directory / "candidate")
