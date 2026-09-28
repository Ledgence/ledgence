#!/usr/bin/env python3
"""Verify the reviewed support-agent wheel graph without importing its packages."""

from __future__ import annotations

import argparse
import email
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import zipfile

HERE = Path(__file__).resolve().parent


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def regular_path(name: str) -> bool:
    path = PurePosixPath(name)
    return (bool(name) and name.isascii() and not path.is_absolute()
            and "\\" not in name and all(part not in (".", "..", "") for part in name.split("/")))


def verify(target: str, wheelhouse: Path, installed: Path | None = None,
           review: Path = HERE) -> dict:
    inventory = json.loads((review / "inventory.json").read_text())
    require(inventory["schema_version"] == 1, "unsupported inventory schema")
    require(target in inventory["targets"], "target has not been reviewed")
    packages = {package["name"]: package for package in inventory["packages"]}
    require(len(packages) == len(inventory["packages"]), "duplicate package inventory entry")
    selected = {}
    for name, package in packages.items():
        artifacts = [artifact for artifact in package["artifacts"] if target in artifact["targets"]]
        require(len(artifacts) == 1, f"expected one reviewed artifact for {name}")
        selected[name] = artifacts[0]
    # The reviewed marker/extras evaluation is stored explicitly per target. Artifact
    # metadata and hashes below prevent that graph drifting through a new wheel.
    reached = set()
    queue = list(inventory["roots"])
    while queue:
        name = queue.pop()
        require(name in packages, f"missing dependency: {name}")
        if name not in reached:
            reached.add(name)
            queue.extend(packages[name]["active_dependencies"][target])
    require(reached == set(packages), "inventory is not the exact reviewed dependency closure")
    expected_lock = {f"{name}=={package['version']} --hash=sha256:{selected[name]['sha256']}"
                     for name, package in packages.items()}
    lock_path = review.parent / f"requirements-{target}.txt"
    lock_lines = [line.strip() for line in lock_path.read_text().splitlines()
                  if line.strip() and not line.lstrip().startswith("#")]
    require(len(lock_lines) == len(expected_lock) and set(lock_lines) == expected_lock,
            "requirements lock differs from reviewed artifacts")
    legal_records = []
    for package in packages.values():
        legal_records.extend(package["source"]["license_files"].values())
        for artifact in package["artifacts"]:
            legal_records.extend(artifact["license_files"].values())
    for component in inventory["embedded_rust"] + inventory["supplemental_sources"]:
        legal_records.extend(component["license_files"].values())
    for record in legal_records:
        require(regular_path(record["path"]), "unsafe retained legal path")
        path = review / record["path"]
        require(path.is_file() and not path.is_symlink(), f"missing original legal file: {path}")
        require(digest(path.read_bytes()) == record["sha256"], f"changed original legal file: {path}")
    bundle = inventory["supplemental_bundle"]
    require(digest((review / bundle["path"]).read_bytes()) == bundle["sha256"],
            "supplemental notice bundle changed")
    actual_wheels = {path.name for path in wheelhouse.iterdir() if path.is_file()}
    require(actual_wheels == {artifact["filename"] for artifact in selected.values()},
            "wheelhouse must contain exactly the reviewed wheel artifacts")
    installed_files = {}
    native_count = 0
    wheel_bytes = 0
    for name, artifact in selected.items():
        path = wheelhouse / artifact["filename"]
        require(not path.is_symlink(), f"wheel artifact is a symlink: {path.name}")
        data = path.read_bytes()
        require(len(data) == artifact["size"] and digest(data) == artifact["sha256"],
                f"wheel size/hash mismatch: {path.name}")
        wheel_bytes += len(data)
        with zipfile.ZipFile(path) as wheel:
            entries = wheel.infolist()
            require(len({entry.filename for entry in entries}) == len(entries), "duplicate wheel paths")
            files = [entry for entry in entries if not entry.is_dir()]
            for entry in files:
                mode = entry.external_attr >> 16
                require(regular_path(entry.filename) and not stat.S_ISLNK(mode),
                        f"unsupported wheel path: {entry.filename}")
                require(".data/" not in entry.filename and not entry.filename.endswith((".pth", ".pyc")),
                        f"unreviewed install behavior: {entry.filename}")
                require(entry.file_size <= 64 * 1024 * 1024, "wheel file exceeds package file limit")
                content = wheel.read(entry)
                # pip rewrites RECORD while adding installer metadata. All importable
                # source, native code, certificates, licenses and other files stay exact.
                if not entry.filename.endswith(".dist-info/RECORD"):
                    require(entry.filename not in installed_files, f"overlapping wheel file: {entry.filename}")
                    installed_files[entry.filename] = digest(content)
                elif installed is not None:
                    require((installed / entry.filename).is_file(), "installed RECORD is missing")
            metadata_paths = [entry.filename for entry in files if entry.filename.endswith(".dist-info/METADATA")]
            require(len(metadata_paths) == 1, "expected one distribution METADATA")
            metadata = email.message_from_bytes(wheel.read(metadata_paths[0]))
            require(normalized(metadata["Name"]) == name and metadata["Version"] == packages[name]["version"],
                    f"distribution identity changed: {path.name}")
            for field, key in (("Requires-Python", "requires_python"), ("License-Expression", "license_expression"),
                               ("License", "license")):
                require(metadata[field] == artifact[key], f"{field} changed: {path.name}")
            require(metadata.get_all("Requires-Dist", []) == artifact["requires_dist"],
                    f"dependency metadata changed: {path.name}")
            native = [entry.filename for entry in files if entry.filename.endswith((".so", ".pyd", ".dll", ".dylib"))]
            require(native == artifact["native_files"], f"native component list changed: {path.name}")
            native_count += len(native)
            for original, record in artifact["license_files"].items():
                require(digest(wheel.read(original)) == record["sha256"], f"wheel notice changed: {original}")
    for name, expected in inventory["certifi_source_equivalence"].items():
        require(installed_files.get(name) == expected, f"certifi source equivalence changed: {name}")
    if installed is not None:
        expected_records = {str(PurePosixPath(name).parent / "RECORD")
                            for name in installed_files if name.endswith(".dist-info/METADATA")}
        for path in installed.rglob("*"):
            require(not path.is_symlink(), f"installed symlink: {path}")
            if path.is_file():
                relative = path.relative_to(installed).as_posix()
                parts = PurePosixPath(relative).parts
                installer_metadata = (len(parts) == 2 and parts[0].endswith(".dist-info")
                                      and parts[1] in ("INSTALLER", "REQUESTED", "direct_url.json"))
                require(relative in installed_files or relative in expected_records
                        or installer_metadata or parts[0] == "bin",
                        f"unexpected installed file: {relative}")
        metadata_paths = list(installed.glob("*.dist-info/METADATA"))
        identities = []
        for path in metadata_paths:
            metadata = email.message_from_bytes(path.read_bytes())
            identities.append((normalized(metadata["Name"]), metadata["Version"]))
        require(sorted(identities) == sorted((name, package["version"]) for name, package in packages.items()),
                "installed distributions differ from reviewed closure")
        for name, expected in installed_files.items():
            path = installed / name
            require(path.is_file() and digest(path.read_bytes()) == expected,
                    f"installed upstream file missing or changed: {name}")
    return {"target": target, "distributions": len(packages), "native_files": native_count,
            "wheel_bytes": wheel_bytes, "verified_upstream_files": len(installed_files),
            "installed_verified": installed is not None}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--wheelhouse", type=Path, required=True)
    parser.add_argument("--installed", type=Path)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(verify(args.target, args.wheelhouse, args.installed), sort_keys=True))
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        print(f"Dependency verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
