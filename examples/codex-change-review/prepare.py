#!/usr/bin/env python3
"""Package the three handlers from one application source tree (MIT)."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys

from change_review.config import AGENT_QUEUE, CONTROL_QUEUE, FINALIZE, IMPLEMENT, VERSION, WORKFLOW

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PROGRAMS = {
    "workflow": (WORKFLOW, "program:handle"),
    "implement": (IMPLEMENT, "change_review.steps:implement_task"),
    "finalize": (FINALIZE, "change_review.steps:finalize_task"),
}


def prepare(directory, binaries):
    if platform.python_implementation() != "CPython" or sys.version_info[:2] != (3, 13):
        raise ValueError("prepare and run this example with CPython 3.13")
    operating_system = {"Darwin": "macos", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(platform.machine())
    if operating_system is None or architecture is None:
        raise ValueError("unsupported host platform")
    directory, binaries = Path(directory).resolve(), Path(binaries).resolve()
    if directory.is_relative_to(ROOT):
        raise ValueError("choose a new output directory outside the repository")
    publisher = binaries / "ledgence"
    if not publisher.is_file():
        raise ValueError("build the Ledgence CLI first")
    directory.mkdir(parents=True, exist_ok=False)
    descriptors = {}
    for kind, (name, handler) in PROGRAMS.items():
        package = directory / "packages" / kind
        package.mkdir(parents=True)
        shutil.copyfile(HERE / "program.py", package / "program.py")
        shutil.copytree(HERE / "change_review", package / "change_review",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".env", ".env.*"))
        shutil.copyfile(ROOT / "LICENSE", package / "LEDGENCE-LICENSE")
        manifest = {"schema_version": 1, "program": {"id": name, "version": VERSION},
                    "runtime": {"kind": "python", "python": "3.13", "protocol": 3}, "handler": handler,
                    "platform": {"os": operating_system, "arch": architecture}}
        (package / "ledgence-program.json").write_text(json.dumps(manifest, indent=2) + "\n")
        result = subprocess.run([str(publisher), "program", "publish", "--source", str(package),
                                 "--store", str(directory / "store")],
                                capture_output=True, text=True, check=True, timeout=60)
        descriptors[kind] = json.loads(result.stdout)
    instance = {"instance_id": "codex-change-review", "name": "Codex change review",
                "scope": {"tenant_id": "acme", "namespace": "demo"},
                "suggested_queues": [CONTROL_QUEUE, AGENT_QUEUE]}
    (directory / "instance.json").write_text(json.dumps(instance, indent=2) + "\n")
    sources = [HERE / "program.py", *sorted((HERE / "change_review").rglob("*"))]
    evidence = {"packages": descriptors, "python": platform.python_version(),
                "platform": {"os": operating_system, "arch": architecture},
                "application_dependencies": [], "source_sha256": {
                    path.relative_to(HERE).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in sources if path.is_file() and "__pycache__" not in path.parts
                    and path.suffix != ".pyc"}}
    (directory / "prepared.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    args = parser.parse_args()
    try:
        result = prepare(args.directory, args.binaries)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Preparation failed ({type(error).__name__}); check paths and choose a fresh directory.\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
