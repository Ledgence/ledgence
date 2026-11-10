"""Inert native bundle/evidence fixtures for qualification-tool regression tests (MIT)."""
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

from release.upgrade_bundles import PREDECESSORS

ROOT = Path(__file__).resolve().parents[2]


def bundle_fixture(directory, label):
    directory = Path(directory)
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"] if label == "current" else label
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    release_commit, commit, contract = PREDECESSORS.get(label, (head, head, 16))
    payload = {"bin/ledgence": f"{label}:bin/ledgence".encode(),
               "runtime/ledgence/worker/bootstrap.py": f"{label}:runtime/ledgence/worker/bootstrap.py".encode(),
               "runtime/ledgence/worker/workflow.py": f"{label}:workflow".encode(),
               "console/index.html": f"{label}:console/index.html".encode(),
               "console/notices/index.html": b"Notices", "console/notices/LEDGENCE-LICENSE.txt": b"MIT"}
    assets = []
    for name, data in payload.items():
        if name.startswith("console/"):
            assets.append({"path": name.removeprefix("console/"), "sha256": hashlib.sha256(data).hexdigest(),
                           "size_bytes": len(data), "content_type": "text/html; charset=utf-8" if name.endswith(".html") else "text/plain; charset=utf-8"})
    manifest = {"schema_version": 1, "console_contract_version": contract, "console_version": version,
                "source_revision": commit, "source_dirty": False, "toolchain": {"node": "24.21.0", "pnpm": "11.27.1"},
                "lockfile_sha256": "b" * 64, "assets": assets}
    payload["console/console-manifest.json"] = json.dumps(manifest).encode()
    provenance = {"source_commit": commit, "source_tree_clean": True, "package_version": version,
                  "console": {"mode": "static", "manifest_sha256": hashlib.sha256(payload["console/console-manifest.json"]).hexdigest()}}
    if label != "current":
        provenance.update(release_tag="v" + label, release_commit=release_commit)
    payload["provenance.json"] = json.dumps(provenance).encode()
    for name, data in payload.items():
        target = directory / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(data)
    reseal(directory)


def reseal(directory):
    (directory / "SHA256SUMS").write_text("".join(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.relative_to(directory).as_posix() + "\n"
        for path in sorted(directory.rglob("*")) if path.is_file() and path.name != "SHA256SUMS"))


def artifact_exercise_fixture():
    return {"double_migration": True, "fresh_restore": True, "restored_rows_unchanged": True,
            "interrupted_migration": {key: True for key in ("observed_transactional_ddl", "nonzero_exit", "artifact_migration_rolled_back", "historical_rows_unchanged")},
            "old_binary_rejection": {key: True for key in ("nonzero_exit", "schema_error_verified", "readiness_never_opened", "historical_rows_unchanged")},
            "artifacts": {key: True for key in ("old_credential_denied_artifact_write", "preserved_root_accepts_artifact", "same_key_replays_reference", "restart_verified_bytes")},
            "resumed": {key: True for key in ("queued_task_completed_once", "parked_admission_completed_once", "fork_join_completed_once", "local_step_not_reexecuted", "callback_delivered", "submission_and_queue_receipts_replayed")},
            "applied_migrations": ["20261110000000"], "old_rows": {"sha256": {"tasks": "a" * 64}}, "backup_sha256": "b" * 64}
