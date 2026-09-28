#!/usr/bin/env python3
"""Build and publish two immutable demo packages for this host's CPython 3.13."""

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
VERSIONS = {"agent": "1.0.1", "workflow": "1.0.2"}


def target() -> tuple[str, str, str]:
    if platform.python_implementation() != "CPython" or sys.version_info[:2] != (3, 13):
        raise ValueError("use CPython 3.13 to build and run this demo")
    pair = (sys.platform, platform.machine().lower())
    targets = {("darwin", "arm64"): ("macos-arm64-cp313", "macos", "aarch64"),
               ("linux", "x86_64"): ("linux-x86_64-cp313", "linux", "x86_64")}
    if pair not in targets:
        raise ValueError("reviewed demo targets are macOS arm64 and Linux x86_64, CPython 3.13")
    return targets[pair]


def run(*argv, **kwargs):
    subprocess.run([str(arg) for arg in argv], check=True, **kwargs)


def validate_package(directory: Path) -> dict:
    paths = list(directory.rglob("*"))
    if len(paths) > 4096:
        raise ValueError("prepared package exceeds Ledgence's 4096-entry limit")
    total = 0
    for path in paths:
        relative = path.relative_to(directory).as_posix()
        if not relative.isascii() or path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError("prepared package contains an unsupported path or entry")
        if path.is_file():
            size = path.stat().st_size
            if size > 64 * 1024 * 1024:
                raise ValueError("prepared file exceeds 64 MiB")
            total += size
    if total > 256 * 1024 * 1024:
        raise ValueError("prepared package exceeds 256 MiB expanded")
    return {"entries": len(paths), "expanded_bytes": total}


def prepare(directory: Path, binaries: Path, wheelhouse: Path | None = None) -> dict:
    selected, operating_system, architecture = target()
    binaries = binaries.resolve()
    if not (binaries / "ledgence-worker").is_file():
        raise ValueError("build the Ledgence binaries first, or supply --binaries")
    directory = directory.resolve()
    if directory.is_relative_to(ROOT):
        raise ValueError("choose a build directory outside the Git repository")
    directory.mkdir(parents=True, exist_ok=False)
    packages = directory / "packages"
    agent = packages / "agent"
    workflow = packages / "workflow"
    requirements = HERE / f"requirements-{selected}.txt"
    wheels = wheelhouse.resolve() if wheelhouse else directory / "wheelhouse"
    if wheelhouse is None:
        run(sys.executable, "-m", "pip", "download", "--disable-pip-version-check",
            "--only-binary=:all:", "--require-hashes", "--dest", wheels, "-r", requirements)
    verify = HERE / "third_party/verify.py"
    run(sys.executable, verify, "--target", selected, "--wheelhouse", wheels)
    agent.mkdir(parents=True)
    run(sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--no-input",
        "--no-compile", "--only-binary=:all:", "--require-hashes", "--no-index",
        "--find-links", wheels, "--target", agent, "-r", requirements)
    run(sys.executable, verify, "--target", selected, "--wheelhouse", wheels, "--installed", agent)
    # Drop installer bookkeeping and command launchers, not original wheel files.
    # Programs import the vendored modules directly; no installed CLI is invoked.
    for metadata in agent.glob("*.dist-info"):
        for name in ("INSTALLER", "REQUESTED", "direct_url.json"):
            (metadata / name).unlink(missing_ok=True)
    shutil.rmtree(agent / "bin", ignore_errors=True)
    for kind, package, name, protocol in (("agent", agent, "support-agent", 2),
                                           ("workflow", workflow, "support-workflow", 3)):
        shutil.copytree(HERE / kind, package, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".env", ".env.*"))
        shutil.copy2(ROOT / "LICENSE", package / "LEDGENCE-LICENSE")
        manifest = {"schema_version": 1, "program": {"id": name, "version": VERSIONS[kind]},
                    "runtime": {"kind": "python", "python": "3.13", "protocol": protocol},
                    "handler": "program:handle", "platform": {"os": operating_system, "arch": architecture}}
        (package / "ledgence-program.json").write_text(json.dumps(manifest, indent=2) + "\n")
    # Retain the review and any supplemental notices missing from an upstream wheel.
    (agent / "third_party").mkdir()
    for name in ("NOTICE.md", "inventory.json", "SUPPLEMENTAL-LICENSES.txt"):
        shutil.copy2(HERE / "third_party" / name, agent / "third_party" / name)
    sizes = {kind: validate_package(package) for kind, package in (("agent", agent), ("workflow", workflow))}
    descriptors = {}
    for kind, package in (("agent", agent), ("workflow", workflow)):
        result = subprocess.run([str(binaries / "ledgence-worker"), "publish", "--source", str(package),
                                 "--store", str(directory / "store")], capture_output=True, text=True, check=True)
        descriptors[kind] = json.loads(result.stdout)
    instance = {"instance_id": "support-demo", "name": "Support agent demo",
                "scope": {"tenant_id": "acme", "namespace": "demo"}, "suggested_queues": ["support-demo"]}
    (directory / "instance.json").write_text(json.dumps(instance, indent=2) + "\n")
    evidence = {"target": selected, "python": platform.python_version(), "packages": descriptors,
                "sizes": sizes, "requirements_sha256": hashlib.sha256(requirements.read_bytes()).hexdigest(),
                "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()}
    evidence["demo_source_sha256"] = {
        path.relative_to(HERE).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(HERE.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
        and path.suffix != ".pyc" and not path.name.startswith(".env")
    }
    (directory / "prepared.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True, help="new output directory outside the repository")
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--wheelhouse", type=Path, help="reuse an exact, already downloaded reviewed wheel set")
    args = parser.parse_args(argv)
    try:
        result = prepare(args.directory, args.binaries, args.wheelhouse)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Preparation failed ({type(error).__name__}); inspect the build output and use a fresh directory after fixing it.", file=sys.stderr)
        return 1
    print(json.dumps({key: value for key, value in result.items() if key != "demo_source_sha256"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
