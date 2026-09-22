#!/usr/bin/env python3
"""Repackage a selected candidate offline; never build, tag, push or publish."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib

from package import archive_tree
from notices import ROOT
from verify import checksum, extract_archive, verify as verify_archive, verify_files

CHANGED_METADATA = frozenset(("README.md", "provenance.json", "SHA256SUMS"))
ADDED_METADATA = frozenset(("candidate-provenance.json", "promotion-payload-sha256.json"))


def git(repository, *arguments):
    return subprocess.check_output(["git", *arguments], cwd=repository, text=True).strip()


def clean_release(repository, release_ref, source_commit):
    if git(repository, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("release checkout must be clean, including untracked files")
    release_commit = git(repository, "rev-parse", "--verify", "--end-of-options", release_ref + "^{commit}")
    if git(repository, "rev-parse", "HEAD") != release_commit:
        raise ValueError("release ref must identify the clean checkout's HEAD")
    source_tree = git(repository, "rev-parse", "--verify", source_commit + "^{tree}")
    if git(repository, "rev-parse", release_commit + "^{tree}") != source_tree:
        raise ValueError("release commit tree differs from the candidate build source")
    lineage = subprocess.run(["git", "merge-base", "--is-ancestor", source_commit, release_commit], cwd=repository)
    if lineage.returncode:
        raise ValueError("candidate build source must be an ancestor of the release commit")
    return release_commit, source_tree


def inventory(directory):
    return {str(path.relative_to(directory)): {"sha256": checksum(path), "mode": f"{path.stat().st_mode & 0o7777:04o}"}
            for path in sorted(directory.rglob("*")) if path.is_file()}


def candidate_identity(directory, repository, version):
    original = (directory / "provenance.json").read_bytes()
    provenance = json.loads(original)
    if not isinstance(provenance, dict):
        raise ValueError("candidate provenance must be a JSON object")
    source = provenance.get("source_commit", "")
    target = provenance.get("target", "")
    label = provenance.get("candidate", "")
    if (provenance.get("format") != 1 or provenance.get("source_tree_clean") is not True
            or not isinstance(source, str) or not re.fullmatch(r"[a-f0-9]{40}", source)
            or not isinstance(target, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", target)
            or provenance.get("package_version") != version
            or not isinstance(label, str) or not re.fullmatch(rf"ledgence-{re.escape(version)}-rc\.[1-9][0-9]*\+g{source[:12]}-{re.escape(target)}", label)
            or directory.name != label):
        raise ValueError("candidate provenance, version or archive root is inconsistent")
    epoch = provenance.get("source_date_epoch")
    if type(epoch) is not int or epoch < 0 or epoch != int(git(repository, "show", "-s", "--format=%ct", source)):
        raise ValueError("candidate source timestamp does not match its Git commit")
    rust = tomllib.loads(git(repository, "show", source + ":Cargo.toml"))
    client = tomllib.loads(git(repository, "show", source + ":sdk/python-client/pyproject.toml"))
    if rust["workspace"]["package"]["version"] != version or client["project"]["version"] != version:
        raise ValueError("stable version must match both original package versions")
    lock = subprocess.check_output(["git", "show", source + ":Cargo.lock"], cwd=repository)
    if (hashlib.sha256(lock).hexdigest() != provenance.get("cargo_lock_sha256")
            or (directory / "Cargo.lock").read_bytes() != lock):
        raise ValueError("candidate Cargo lock differs from its build source")
    return provenance, original


def promote(archive, expected_sha256, output, repository, release_ref, version, python=sys.executable):
    if not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise ValueError("expected candidate SHA256 must be 64 lowercase hexadecimal characters")
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version):
        raise ValueError("stable version must have exactly three numeric components")
    repository, archive, output = repository.resolve(), archive.resolve(), output.resolve()
    repository = Path(git(repository, "rev-parse", "--show-toplevel"))
    if output.exists() or output.is_relative_to(repository) or output.is_relative_to(ROOT.resolve()):
        raise ValueError("output must be a NEW directory outside the source and tooling checkouts")
    if archive.stat().st_size > 1024 ** 3:
        raise ValueError("candidate archive exceeds supported bounds")
    with tempfile.TemporaryDirectory(prefix="ledgence-promotion-") as temporary:
        temporary = Path(temporary)
        # Work from a private copy so changes to the supplied file cannot race
        # its checksum, extraction, or the recorded candidate identity.
        candidate_archive = temporary / "candidate.tar.gz"
        shutil.copyfile(archive, candidate_archive)
        if checksum(candidate_archive) != expected_sha256:
            raise ValueError("candidate archive SHA256 does not match the selected artifact")
        # Require package.py's original header modes before the extraction
        # filter can normalize them; payload mode equality starts at the tar.
        candidate = extract_archive(candidate_archive, temporary / "candidate", canonical_modes=True)
        verify_files(candidate)
        original_inventory = inventory(candidate)
        if ADDED_METADATA.intersection(original_inventory):
            raise ValueError("candidate already contains reserved promotion metadata")
        required = {"README.md", "LICENSE", "Cargo.lock", "python-client-validation.json", "runtime/ledgence/worker/bootstrap.py",
                    "bin/ledgence", "bin/ledgence-worker", "bin/ledgence-orchestrator"}
        if not required.issubset(original_inventory) or not any(name.endswith(".whl") and name.startswith("python-client/") for name in original_inventory):
            raise ValueError("candidate lacks required native/runtime/client payload")
        provenance, original_provenance = candidate_identity(candidate, repository, version)
        source_commit = provenance["source_commit"]
        release_commit, source_tree = clean_release(repository, release_ref, source_commit)
        label = f"ledgence-{version}-{provenance['target']}"
        stable = temporary / label
        candidate.rename(stable)
        (stable / "candidate-provenance.json").write_bytes(original_provenance)
        payload = {name: value for name, value in original_inventory.items() if name not in CHANGED_METADATA}
        comparison = {"candidate_archive_sha256": expected_sha256, "unchanged_files": payload,
                      "changed_metadata": sorted(CHANGED_METADATA), "added_metadata": sorted(ADDED_METADATA)}
        (stable / "promotion-payload-sha256.json").write_text(json.dumps(comparison, indent=2) + "\n")
        promoted = {"format": 2, "release": label, "release_version": version, "release_tag": "v" + version,
                    "release_commit": release_commit, "release_tree": source_tree,
                    "source_commit": source_commit, "source_tree_clean": True, "package_version": version,
                    "target": provenance["target"], "source_date_epoch": provenance["source_date_epoch"],
                    "promoted_from": {"candidate": provenance["candidate"], "archive": archive.name,
                                      "archive_sha256": expected_sha256, "provenance": "candidate-provenance.json",
                                      "provenance_sha256": hashlib.sha256(original_provenance).hexdigest()},
                    "promotion_tool_sha256": checksum(Path(__file__)),
                    "payload_manifest": "promotion-payload-sha256.json",
                    "payload_manifest_sha256": checksum(stable / "promotion-payload-sha256.json"),
                    "operation": "Offline metadata-only promotion; no compilation, package rewriting, network, Git mutation, tag or publication.",
                    "qualification": "Candidate selection and acceptance are operator decisions backed by the separate qualification reports. Promotion checks identity and byte preservation; it does not certify those gates.",
                    "validation": ["selected candidate archive SHA256 and complete file inventory", "clean source-equivalent release commit", "unchanged payload bytes and modes", "stable archive inventory and relocated execution"]}
        (stable / "provenance.json").write_text(json.dumps(promoted, indent=2) + "\n")
        (stable / "README.md").write_text(
            f"# Ledgence {version}\n\nThis stable release bundle was promoted from {provenance['candidate']}. "
            f"Its native binaries, Python distributions, worker helper, documentation and legal material retain the exact candidate bytes. "
            f"The original build source is {source_commit}; the source-equivalent release commit is {release_commit} (intended tag v{version}). "
            "The tag and publication are separate operations. See provenance.json and candidate-provenance.json for the promotion and original build records.\n\n"
            f"Use bin/ledgence-orchestrator, bin/ledgence-worker and bin/ledgence. Supply a compatible host CPython 3.11–3.14 "
            f"and pass --runner <bundle>/runtime/ledgence/worker/bootstrap.py. Native binaries target {provenance['target']}; "
            "CPython, PostgreSQL, brokers and host system libraries are not bundled. The unchanged client wheel and sdist are in python-client/. "
            "See docs/local-deployment.md and docs/releasing.md. The installed-SDK Compose companion is examples/local-compose-client.py; "
            "start and publish its programs from the matching source checkout first.\n\n"
            "Keep LICENSE and legal/ with redistributed binaries. Python distributions retain their own legal files. "
            "Third-party software retains its original licenses. The release version does not expand the documented API compatibility or platform support commitments.\n")
        current = inventory(stable)
        if set(current) != set(original_inventory) | ADDED_METADATA or any(current[name] != value for name, value in payload.items()):
            raise ValueError("promotion changed candidate payload bytes or modes")
        (stable / "SHA256SUMS").write_text("".join(f"{checksum(path)}  {path.relative_to(stable)}\n"
            for path in sorted(stable.rglob("*")) if path.is_file() and path != stable / "SHA256SUMS"))
        result_archive = temporary / (label + ".tar.gz")
        archive_tree(stable, result_archive, provenance["source_date_epoch"])
        verify_archive(result_archive, python)
        # Verify preservation after tar normalization/extraction as well.
        extracted = extract_archive(result_archive, temporary / "stable-check")
        extracted_inventory = inventory(extracted)
        if any(extracted_inventory[name] != value for name, value in payload.items()):
            raise ValueError("stable archive changed candidate payload bytes or modes")
        if clean_release(repository, release_ref, source_commit) != (release_commit, source_tree):
            raise ValueError("release ref changed during promotion")
        output.mkdir(parents=True, exist_ok=False)
        destination = output / result_archive.name
        shutil.copyfile(result_archive, destination)
        result = {"archive": str(destination), "sha256": checksum(destination), "source_commit": source_commit,
                  "release_commit": release_commit, "release_tree": source_tree, "payload_files_preserved": len(payload)}
        (output / "SHA256SUMS").write_text(f"{result['sha256']}  {destination.name}\n")
        shutil.copyfile(stable / "provenance.json", output / "provenance.json")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path, help="selected, qualified candidate archive")
    parser.add_argument("--sha256", required=True, help="expected SHA256 from the selected candidate's outer checksums")
    parser.add_argument("--output", required=True, type=Path, help="NEW output directory outside the checkouts")
    parser.add_argument("--repository", type=Path, default=ROOT, help="clean local checkout at the selected release commit")
    parser.add_argument("--release-ref", required=True, help="existing local ref or commit identifying that checkout's HEAD")
    parser.add_argument("--version", required=True, help="stable version already embedded in both original packages")
    parser.add_argument("--python", default=sys.executable, help="compatible host CPython for relocated execution")
    args = parser.parse_args()
    try:
        result = promote(args.archive, args.sha256, args.output, args.repository, args.release_ref, args.version, args.python)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Promotion failed: {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
