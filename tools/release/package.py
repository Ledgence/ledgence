#!/usr/bin/env python3
"""Build a candidate bundle from a clean commit; never tag, push or publish."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib

from notices import BINS, ROOT, collect


def command(args, **kwargs):
    print("+", " ".join(map(str, args)), flush=True)
    return subprocess.run(list(map(str, args)), cwd=ROOT, check=True, **kwargs)


def read(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def clean_source(commit=None):
    current = read("git", "rev-parse", "HEAD")
    if read("git", "status", "--porcelain", "--untracked-files=normal"):
        raise SystemExit("candidate builds require a clean committed source tree")
    if commit is not None and current != commit:
        raise SystemExit("source commit changed while building candidate")
    return current


def archive_tree(source, destination, epoch):
    with destination.open("wb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                for path in [source, *sorted(source.rglob("*"))]:
                    if path.is_symlink() or not (path.is_file() or path.is_dir()):
                        raise ValueError(f"unsupported bundle entry: {path}")
                    info = archive.gettarinfo(path, arcname=str(Path(source.name) / path.relative_to(source)))
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = epoch
                    info.mode = 0o755 if path.is_dir() or path.parent.name == "bin" else 0o644
                    if path.is_file():
                        with path.open("rb") as stream:
                            archive.addfile(info, stream)
                    else:
                        archive.addfile(info)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="NEW output directory outside checkout")
    parser.add_argument("--candidate", default="rc.1", help="candidate label; package versions remain those in source")
    parser.add_argument("--wheelhouse", type=Path)
    parser.add_argument("--offline", action="store_true", help="use only pre-fetched Cargo and reviewed Python artifacts")
    args = parser.parse_args()
    if not re.fullmatch(r"rc\.[1-9][0-9]*", args.candidate):
        parser.error("candidate must be rc.N with a positive integer")
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error("output must be a NEW directory outside checkout")
    commit = clean_source()
    rustc = read("rustc", "-vV")
    target = next(line.removeprefix("host: ") for line in rustc.splitlines() if line.startswith("host: "))
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    expected_rust = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    if not rustc.startswith("rustc " + expected_rust + " "):
        raise SystemExit("rustc does not match the committed toolchain")
    label = f"ledgence-{version}-{args.candidate}+g{commit[:12]}-{target}"
    epoch = int(read("git", "show", "-s", "--format=%ct", commit))
    env = dict(os.environ, SQLX_OFFLINE="true", SOURCE_DATE_EPOCH=str(epoch), PYTHONDONTWRITEBYTECODE="1")
    # No extra build/dependency versions are introduced by the release tool.
    build = ["cargo", "build", "--release", "--locked", "--all-features", "--bins", "--target", target]
    for package in BINS:
        build.extend(["-p", package])
    if args.offline:
        build.append("--offline")
    command(build, env=env)
    metadata = json.loads(read("cargo", "metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"))
    binaries = Path(metadata["target_directory"]) / target / "release"
    with tempfile.TemporaryDirectory(prefix="ledgence-candidate-") as temporary:
        stage = Path(temporary) / label
        (stage / "bin").mkdir(parents=True)
        for name in ("ledgence", "ledgence-worker", "ledgence-orchestrator"):
            shutil.copyfile(binaries / name, stage / "bin" / name)
            (stage / "bin" / name).chmod(0o755)
        shutil.copytree(ROOT / "sdk/python/ledgence", stage / "runtime/ledgence",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copytree(ROOT / "docs", stage / "docs")
        (stage / "examples").mkdir()
        shutil.copyfile(ROOT / "examples/local-compose-client.py", stage / "examples/local-compose-client.py")
        shutil.copyfile(ROOT / "LICENSE", stage / "LICENSE")
        shutil.copyfile(ROOT / "Cargo.lock", stage / "Cargo.lock")
        collect(stage / "legal", target)
        sdk = [sys.executable, ROOT / "tools/check-python-client.py", "--dist-dir", stage / "python-client",
               "--evidence", stage / "python-client-validation.json"]
        if args.wheelhouse:
            sdk.extend(["--wheelhouse", args.wheelhouse.resolve()])
        if args.offline:
            sdk.append("--offline")
        command(sdk, env=env)
        (stage / "README.md").write_text(f"# {label}\n\nThis is a release candidate assembled from commit {commit}, not a stable release. Embedded Rust and Python package versions remain {version}.\n\nUse bin/ledgence-orchestrator, bin/ledgence-worker and bin/ledgence. Supply a compatible host CPython 3.11–3.14 and pass --runner <bundle>/runtime/ledgence/worker/bootstrap.py. Native binaries target {target}; they require host system libraries and do not include CPython, PostgreSQL or a broker. The client wheel and sdist are in python-client/; installing the wheel resolves the reviewed pinned dependencies. See docs/local-deployment.md and docs/releasing.md. The installed-SDK Compose companion is examples/local-compose-client.py; start and publish its programs from the matching source checkout first.\n\nKeep LICENSE and legal/ with redistributed binaries; Python distributions carry their own retained legal files. Third-party software retains its original licenses.\n")
        # Captures dependency/toolchain identity, not a claim of byte-identical compilation.
        linker = ["otool", "-L"] if sys.platform == "darwin" else ["ldd"]
        linked = {name: read(*linker, str(stage / "bin" / name)) for name in ("ledgence", "ledgence-worker", "ledgence-orchestrator")}
        provenance = {"format": 1, "candidate": label, "source_commit": commit,
                      "source_tree_clean": True, "source_date_epoch": epoch,
                      "package_version": version, "target": target, "rustc": rustc,
                      "cargo": read("cargo", "-V"), "python_builder": sys.version,
                      "host": platform.platform(), "features": "all features of the three executable packages",
                      "cargo_lock_sha256": digest(ROOT / "Cargo.lock"),
                      "build_command": list(map(str, build)), "dynamic_libraries": linked,
                      "rustflags": os.environ.get("RUSTFLAGS"),
                      "encoded_rustflags": os.environ.get("CARGO_ENCODED_RUSTFLAGS"),
                      "validation": ["locked optimized native build", "selected Rust legal inventory", "installed client base/OTel and namespace tests", "relocated native bundle smoke"],
                      "qualification": "Read the separate integration, fault, retention and load reports for this exact commit. Packaging does not certify those gates or untested targets.",
                      "byte_reproducibility": "Archive ordering/metadata are normalized; identical compiled binary bytes are not claimed."}
        (stage / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
        command([sys.executable, ROOT / "tools/release/smoke.py", "--directory", stage], env=env)
        (stage / "SHA256SUMS").write_text("".join(f"{digest(path)}  {path.relative_to(stage)}\n"
            for path in sorted(stage.rglob("*")) if path.is_file()))
        clean_source(commit)
        output.mkdir(parents=True)
        archive = output / (label + ".tar.gz")
        archive_tree(stage, archive, epoch)
        command([sys.executable, ROOT / "tools/release/verify.py", "--archive", archive], env=env)
        (output / "SHA256SUMS").write_text(f"{digest(archive)}  {archive.name}\n")
        shutil.copyfile(stage / "provenance.json", output / "provenance.json")
        print(json.dumps({"archive": str(archive), "sha256": digest(archive), "source_commit": commit}, indent=2))


if __name__ == "__main__":
    main()
