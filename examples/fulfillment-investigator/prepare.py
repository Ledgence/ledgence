#!/usr/bin/env python3
"""Prepare a native Python package and separate synthetic data store (MIT)."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys

from fulfillment.data import prepare_data

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PROGRAM = "fulfillment-investigator"
VERSION = "1.0.0"


def prepare(directory, binaries):
    """Publish one immutable program; keep generated artifacts outside it."""
    if platform.python_implementation() != "CPython" or sys.version_info < (3, 11):
        raise ValueError("prepare and run this example with CPython 3.11 or newer")
    operating_system = {"Darwin": "macos", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(platform.machine())
    if operating_system is None or architecture is None:
        raise ValueError("unsupported host platform")
    directory, binaries = Path(directory).resolve(), Path(binaries).resolve()
    if directory.is_relative_to(ROOT):
        raise ValueError("choose a new output directory outside the repository")
    publisher = binaries / "ledgence"
    if not publisher.is_file():
        raise ValueError("build the current-source Ledgence CLI first")
    if not (HERE / "program.py").is_file():
        raise ValueError("program.py is missing from the source checkout")
    directory.mkdir(parents=True, exist_ok=False)
    package = directory / "packages" / "workflow"
    package.mkdir(parents=True)
    shutil.copyfile(HERE / "program.py", package / "program.py")
    # Only Python sources are application code. Fixture artifacts, credentials,
    # caches, and the mutable execution environment never enter the package.
    for source in sorted((HERE / "fulfillment").rglob("*.py")):
        if "__pycache__" in source.parts or source.is_symlink():
            continue
        destination = package / source.relative_to(HERE)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    shutil.copyfile(ROOT / "LICENSE", package / "LICENSE")
    manifest = {
        "schema_version": 1,
        "program": {"id": PROGRAM, "version": VERSION},
        "runtime": {"kind": "python", "python": f"{sys.version_info.major}.{sys.version_info.minor}", "protocol": 3},
        "handler": "program:handle",
        "platform": {"os": operating_system, "arch": architecture},
    }
    (package / "ledgence-program.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    result = subprocess.run(
        [str(publisher), "program", "publish", "--source", str(package), "--store", str(directory / "store")],
        capture_output=True, text=True, check=True, timeout=60,
    )
    descriptor = json.loads(result.stdout)
    fixtures = prepare_data(str(directory / "data"))
    instance = {
        "instance_id": "fulfillment-investigator",
        "name": "Fulfillment investigator",
        "scope": {"tenant_id": "acme", "namespace": "demo"},
        "suggested_queues": ["fulfillment"],
    }
    (directory / "instance.json").write_text(json.dumps(instance, indent=2) + "\n", encoding="utf-8")
    evidence = {
        "schema_version": 1,
        "synthetic": True,
        "program": {"id": PROGRAM, "version": VERSION},
        "packages": {"workflow": descriptor},
        "python": platform.python_version(),
        "platform": {"os": operating_system, "arch": architecture},
        "application_dependencies": [],
        "data_directory": str(directory / "data"),
        "fixtures": fixtures,
        "package_file_sha256": {
            path.relative_to(package).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(package.rglob("*")) if path.is_file()
        },
    }
    (directory / "prepared.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    return evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True, help="new directory outside the repository")
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    args = parser.parse_args(argv)
    try:
        result = prepare(args.directory, args.binaries)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Preparation failed ({type(error).__name__}); check paths and use a fresh directory.\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
