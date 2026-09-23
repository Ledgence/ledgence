#!/usr/bin/env python3
"""Retain exact license material for the selected Rust binary build graph."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parents[2]
BINS = ["ledgence-worker", "ledgence-orchestrator", "ledgence-cli"]


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True)


def legal_file(path, root):
    relative = path.relative_to(root)
    names = [part.lower() for part in relative.parts]
    return (path.is_file() and not path.is_symlink() and
            (names[-1].startswith(("license", "licence", "notice", "copyright", "copying", "authors"))
             or any(part in ("licenses", "licences") for part in names[:-1])))


def collect(output, target):
    output.mkdir(parents=True, exist_ok=False)
    metadata = json.loads(run("cargo", "metadata", "--locked", "--offline", "--all-features", "--format-version", "1", "--filter-platform", target))
    args = ["cargo", "tree", "--locked", "--offline", "--all-features", "--target", target,
            "--edges", "normal,build", "--prefix", "none", "--format", "{p}"]
    for package in BINS:
        args.extend(["-p", package])
    selected = {tuple(line.split()[:2]) for line in run(*args).splitlines()}
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    checksums = {(p["name"], p["version"]): p.get("checksum") for p in lock["package"]}
    packages = []
    for package in sorted(metadata["packages"], key=lambda p: (p["name"], p["version"])):
        if (package["name"], "v" + package["version"]) not in selected or package["source"] is None:
            continue
        name = package["name"] + "-" + package["version"]
        root = Path(package["manifest_path"]).parent
        paths = [(path, path.relative_to(root)) for path in root.rglob("*") if legal_file(path, root)]
        # A few published crates omit upstream legal files. Only version-specific,
        # reviewed retained copies can fill those gaps; never synthesize a license.
        for path in sorted((ROOT / "legal/third-party").glob(name + "-*")):
            if path.is_file() and path.suffix != ".json":
                paths.append((path, Path("reviewed") / path.name))
        if not paths:
            raise SystemExit(f"no retained license material for {name}")
        files = []
        for source, relative in sorted(paths):
            destination = output / "crates" / name / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            files.append({"path": str(destination.relative_to(output)), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()})
        packages.append({"name": package["name"], "version": package["version"],
                         "declared_license": package["license"], "source": package["source"],
                         "repository": package["repository"], "crate_sha256": checksums[(package["name"], package["version"])], "legal_files": files})
    sysroot = Path(run("rustc", "--print", "sysroot").strip())
    rustdocs = sysroot / "share/doc/rust"
    required = [rustdocs / "COPYRIGHT-library.html", rustdocs / "COPYRIGHT.html", rustdocs / "licenses"]
    if not all(path.exists() for path in required):
        raise SystemExit("the selected Rust distribution must include its copyright inventories and licenses")
    for source in required:
        destination = output / "rust-toolchain" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, destination)
        else:
            shutil.copyfile(source, destination)
    shutil.copyfile(ROOT / "LICENSE", output / "LEDGENCE-LICENSE")
    (output / "inventory.json").write_text(json.dumps({"format": 1, "target": target,
        "scope": "Selected normal and build dependencies for all three binaries with all features; build dependencies may not be linked. Rust toolchain inventories cover standard-library and incorporated native code. Host system libraries and CPython are not bundled.",
        "rustc": run("rustc", "-vV"), "packages": packages}, indent=2) + "\n")
    (output / "NOTICE.md").write_text("# Third-party components\n\nLedgence-owned code is MIT. Each dependency retains its own terms; the included exact license and notice material is not relicensed. inventory.json identifies the selected Cargo graph, including build dependencies conservatively. Rust's copyright inventories and licenses are retained separately. Binary release bundles require a host CPython and system libraries; those are not included. Uploaded programs have their own dependency obligations.\n")
    return packages


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    collect(args.output, args.target)
