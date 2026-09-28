#!/usr/bin/env python3
"""Publish the standard-library Codex support programs into a local store."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
VERSION = "1.0.0"


def target():
    if platform.python_implementation() != "CPython" or sys.version_info[:2] != (3, 13):
        raise ValueError("use CPython 3.13 to prepare and run this demo")
    targets = {("darwin", "arm64"): ("macos", "aarch64"),
               ("linux", "x86_64"): ("linux", "x86_64")}
    selected = targets.get((sys.platform, platform.machine().lower()))
    if selected is None:
        raise ValueError("supported demo hosts are macOS arm64 and Linux x86_64")
    return selected


def validate_package(directory):
    entries = list(directory.rglob("*"))
    if len(entries) > 4096:
        raise ValueError("package exceeds the 4096-entry limit")
    total = 0
    for path in entries:
        if (not path.relative_to(directory).as_posix().isascii() or path.is_symlink()
                or not (path.is_file() or path.is_dir())):
            raise ValueError("unsupported package entry")
        if path.is_file():
            size = path.stat().st_size
            if size > 64 * 1024 * 1024:
                raise ValueError("package file exceeds 64 MiB")
            total += size
    if total > 256 * 1024 * 1024:
        raise ValueError("package exceeds 256 MiB expanded")
    return {"entries": len(entries), "expanded_bytes": total}


def prepare(directory, binaries):
    operating_system, architecture = target()
    directory, binaries = directory.resolve(), binaries.resolve()
    if directory.is_relative_to(ROOT):
        raise ValueError("choose a new output directory outside the repository")
    if not (binaries / "ledgence-worker").is_file():
        raise ValueError("build the Ledgence binaries first")
    directory.mkdir(parents=True, exist_ok=False)
    descriptors, sizes = {}, {}
    for kind, name, protocol in (("agent", "codex-support-agent", 2),
                                 ("workflow", "codex-support-workflow", 3)):
        package = directory / "packages" / kind
        shutil.copytree(HERE / kind, package,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".env", ".env.*"))
        shutil.copy2(ROOT / "LICENSE", package / "LEDGENCE-LICENSE")
        manifest = {"schema_version": 1, "program": {"id": name, "version": VERSION},
                    "runtime": {"kind": "python", "python": "3.13", "protocol": protocol},
                    "handler": "program:handle",
                    "platform": {"os": operating_system, "arch": architecture}}
        (package / "ledgence-program.json").write_text(json.dumps(manifest, indent=2) + "\n")
        sizes[kind] = validate_package(package)
        result = subprocess.run([str(binaries / "ledgence-worker"), "publish", "--source", str(package),
                                 "--store", str(directory / "store")],
                                capture_output=True, text=True, check=True, timeout=60)
        descriptors[kind] = json.loads(result.stdout)
    instance = {"instance_id": "codex-support-demo", "name": "Codex support agent demo",
                "scope": {"tenant_id": "acme", "namespace": "demo"},
                "suggested_queues": ["codex-support-demo"]}
    (directory / "instance.json").write_text(json.dumps(instance, indent=2) + "\n")
    evidence = {"target": f"{operating_system}-{architecture}-cp313", "python": platform.python_version(),
                "packages": descriptors, "sizes": sizes, "application_dependencies": [],
                "host_requirements": ["CPython 3.13", "Codex CLI with ChatGPT login"],
                "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_worktree_clean": not subprocess.check_output(
                    ["git", "status", "--porcelain"], cwd=ROOT, text=True).strip(),
                "demo_source_sha256": {
                    path.relative_to(HERE).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in sorted(HERE.rglob("*")) if path.is_file()
                    and "__pycache__" not in path.parts and path.suffix != ".pyc"
                    and not path.name.startswith(".env")}}
    (directory / "prepared.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    args = parser.parse_args(argv)
    try:
        evidence = prepare(args.directory, args.binaries)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Preparation failed ({type(error).__name__}); check paths and use a fresh output directory.", file=sys.stderr)
        return 1
    print(json.dumps({key: value for key, value in evidence.items() if key != "demo_source_sha256"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
