"""Checksum-bound native upgrade inputs; never substitute a source-tree helper (MIT)."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tomllib

from .console_bundle import validate, unique

PREDECESSORS = {
    "0.8.0": ("e95f0baac01d443e0e72c3ffa5010ef5a28c6e6b", "cf1804ea98d7a18c4608b9b3082646fa41c505c4", 7),
    "0.9.0": ("c5589aaae95d1dd944372beaf27847702a96e13c", "c5589aaae95d1dd944372beaf27847702a96e13c", 14),
}


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def inventory(directory):
    """Verify the entire bundle, not just its binary or an attacker-selected subset."""
    directory = Path(directory).resolve()
    manifest = directory / "SHA256SUMS"
    expected = {}
    if manifest.is_symlink() or not manifest.is_file() or manifest.stat().st_size > 2 * 1024**2:
        raise ValueError("missing or oversized native bundle checksum manifest")
    for line in manifest.read_text().splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  (.+)", line)
        if match is None:
            raise ValueError("invalid native bundle checksum record")
        checksum, name = match.groups()
        pure = PurePosixPath(name)
        if (pure.is_absolute() or any(p in ("", ".", "..") for p in name.split("/"))
                or "\\" in name or any(ord(c) < 32 for c in name)
                or name in expected or name == "SHA256SUMS"):
            raise ValueError("invalid or duplicate native bundle checksum path")
        expected[name] = checksum
    actual = {}
    for file in directory.rglob("*"):
        if file.is_symlink() or not (file.is_dir() or file.is_file()):
            raise ValueError("native bundle contains a nonregular entry")
        if file.is_file() and file != manifest:
            actual[file.relative_to(directory).as_posix()] = digest(file)
    if not expected or actual != expected:
        raise ValueError("native bundle inventory or checksum mismatch")
    required = {"bin/ledgence", "runtime/ledgence/worker/bootstrap.py", "provenance.json", "console/console-manifest.json"}
    if not required <= actual.keys() or {p for p in actual if p.startswith("bin/")} != {"bin/ledgence"}:
        raise ValueError("native bundle binary, helper or Console is missing")
    return actual


def bundle(directory, *, predecessor=None, execute=True):
    directory = Path(directory).resolve()
    files = inventory(directory)
    record = json.loads((directory / "provenance.json").read_text(), object_pairs_hook=unique)
    qualified = record
    if "candidate-provenance.json" in files:
        qualified = json.loads((directory / "candidate-provenance.json").read_text(), object_pairs_hook=unique)
    version, commit = record.get("package_version"), record.get("source_commit")
    if (not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", version)
            or not isinstance(commit, str) or not re.fullmatch(r"[a-f0-9]{40}", commit)
            or record.get("source_tree_clean") is not True
            or qualified.get("package_version") != version
            or qualified.get("source_commit") != commit
            or qualified.get("source_tree_clean") is not True):
        raise ValueError("native bundle provenance is not clean and version-bound")
    contract = 16
    if predecessor is not None:
        expected_release, expected_commit, contract = PREDECESSORS[predecessor]
        if (version != predecessor or commit != expected_commit
                or record.get("release_tag") != "v" + predecessor
                or record.get("release_commit") != expected_release):
            raise ValueError("expected the pinned published predecessor release")
    console = validate(directory / "console", source_commit=commit, version=version, expected_contract=contract)
    console_record = qualified.get("console", {})
    if (console_record.get("mode") != "static"
            or console_record.get("manifest_sha256") != digest(directory / "console/console-manifest.json")):
        raise ValueError("Console payload differs from native provenance")
    if execute:
        output = subprocess.check_output([directory / "bin/ledgence", "--version"], text=True, timeout=20).strip()
        if output != "ledgence " + version:
            raise ValueError("native executable version differs from provenance")
    return {"version": version, "source_commit": commit, "release_commit": record.get("release_commit"), "console_contract": console["console_contract_version"],
            "manifest_sha256": digest(directory / "SHA256SUMS"), "verified_files": files}


def source_clean(source):
    return not subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"], cwd=source, text=True, timeout=10
    ).strip()


def inputs(previous, binaries, console, runner, *, predecessor, execute=True):
    previous, binaries, console, runner = map(lambda p: Path(p).resolve(), (previous, binaries, console, runner))
    candidate = binaries.parent
    if (binaries != candidate / "bin" or console != candidate / "console"
            or runner != candidate / "runtime/ledgence/worker/bootstrap.py" or candidate == previous):
        raise ValueError("upgrade requires distinct extracted bundles and their bundled helpers")
    old = bundle(previous, predecessor=predecessor, execute=execute)
    new = bundle(candidate, execute=execute)
    source = Path(__file__).resolve().parents[2]
    expected = tomllib.loads((source / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    source_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True, timeout=10).strip()
    if not source_clean(source):
        raise ValueError("upgrade qualification requires a clean source checkout for the candidate and harness")
    if new["source_commit"] != source_commit or new["version"] != expected or old["verified_files"]["bin/ledgence"] == new["verified_files"]["bin/ledgence"]:
        raise ValueError("candidate version or binary does not match selected upgrade")
    return {"old_release_commit": old["release_commit"], "old_source_commit": old["source_commit"], "old_manifest_sha256": old["manifest_sha256"],
            "old_verified_files": old["verified_files"], "new_manifest_sha256": new["manifest_sha256"],
            "new_verified_files": new["verified_files"], "new_source_commit": new["source_commit"], "new_source_clean": True,
            "new_binary_sha256": new["verified_files"]["bin/ledgence"],
            "versions": ["ledgence " + old["version"], "ledgence " + new["version"]],
            "console_contract": new["console_contract"],
            "console_manifest_sha256": new["verified_files"]["console/console-manifest.json"]}
