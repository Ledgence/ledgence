"""Require complete, bundle-bound predecessor rehearsals before candidate upload (MIT)."""
import argparse
import hashlib
import re
import json
import sys
from pathlib import Path
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release.upgrade_bundles import digest, inputs



ARTIFACT_RESTORE_PHASES = ["quiesced", "database_dumped", "store_copied", "fresh_database_restored",
    "missing_marker_rejected", "foreign_marker_rejected", "original_store_started",
    "original_reference_downloaded", "same_key_replayed"]


def verify_artifact_restore(artifacts):
    proof = artifacts.get("backup_restore", {})
    if (proof.get("phases") != ARTIFACT_RESTORE_PHASES or any(proof.get(key) is not True for key in
            ("all_writers_stopped", "fresh_database", "new_store_path", "fresh_empty_spool", "catalog_restored_unchanged",
             "same_key_replays_reference", "no_new_generation"))):
        raise ValueError("artifact upgrade omitted quiesced artifact-bearing backup/restore proof")
    for key in ("database_dump_sha256", "store_inventory_sha256", "restored_inventory_sha256", "marker_sha256", "download_sha256"):
        if not isinstance(proof.get(key), str) or not re.fullmatch(r"[a-f0-9]{64}", proof[key]):
            raise ValueError("artifact restore omitted actual backup, inventory or byte digests")
    inventory = proof.get("store_inventory", {})
    if not isinstance(inventory, dict):
        raise ValueError("artifact restore omitted full store inventory")
    for name, item in inventory.items():
        if not name or name.startswith("/") or any(part in ("", ".", "..") for part in name.split("/")):
            raise ValueError("invalid artifact store inventory path")
        if item.get("kind") == "directory":
            if item != {"kind": "directory"}:
                raise ValueError("invalid artifact store directory inventory")
        elif (set(item) != {"kind", "size", "sha256"} or item.get("kind") != "file"
              or type(item.get("size")) is not int or item["size"] < 0
              or not isinstance(item.get("sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", item["sha256"])):
            raise ValueError("invalid artifact store file inventory")
    marker = inventory.get(".ledgence-artifact-store")
    if (marker != {"kind": "file", "size": 32, "sha256": proof["marker_sha256"]}
            or not any(name.startswith("objects/") and item["kind"] == "file" for name, item in inventory.items())):
        raise ValueError("artifact backup omitted hidden store identity or object bytes")
    actual = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if proof["store_inventory_sha256"] != actual or proof["restored_inventory_sha256"] != actual:
        raise ValueError("artifact restore inventory does not match the backup")
    binding = proof.get("binding", {})
    reference = artifacts.get("reference", {})
    if (not binding.get("store_id") or not isinstance(binding.get("fingerprint"), str)
            or not re.fullmatch(r"sha256:[a-f0-9]{64}", binding["fingerprint"])
            or not reference or proof.get("same_reference") != reference
            or reference.get("digest") != "sha256:" + proof["download_sha256"]):
        raise ValueError("artifact restore lost the original binding, reference or byte identity")
    for case in ("missing_marker", "foreign_marker"):
        rejected = proof.get("rejected_copies", {}).get(case, {})
        if any(rejected.get(key) is not True for key in
                ("nonzero_exit", "readiness_never_opened", "catalog_unchanged", "different_marker_verified", "fresh_empty_spool")):
            raise ValueError("artifact restore omitted " + case + " startup rejection proof")


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
            verify_artifact_restore(exercise["artifacts"])
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
