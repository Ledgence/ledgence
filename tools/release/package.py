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

from notices import ROOT, collect
from console_bundle import validate as validate_console


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


def source_version():
    """Require all published package versions to agree before building a bundle."""
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    for name, path in (("ledgence-client", "sdk/python-client/pyproject.toml"),
                       ("ledgence-worker", "sdk/python/pyproject.toml")):
        try:
            declared = tomllib.loads((ROOT / path).read_text())["project"]["version"]
        except FileNotFoundError as error:
            raise ValueError(f"candidate source lacks the {name} package manifest") from error
        if declared != version:
            raise ValueError(f"Rust workspace and {name} package versions must match")
    return version


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
    parser.add_argument("--local-distribution", type=Path, default=os.environ.get('LEDGENCE_LOCAL_DISTRIBUTION'),
                        help="verified extracted local deployment kit for this exact version")
    parser.add_argument("--offline", action="store_true", help="use only pre-fetched Cargo and reviewed Python artifacts")
    web = parser.add_mutually_exclusive_group(required=True)
    web.add_argument('--console-dist', type=Path, help='prepared, validated Console dist from this exact clean commit')
    web.add_argument('--headless', action='store_true', help='explicitly omit Console static files')
    args = parser.parse_args()
    if not re.fullmatch(r"rc\.[1-9][0-9]*", args.candidate):
        parser.error("candidate must be rc.N with a positive integer")
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error("output must be a NEW directory outside checkout")
    commit = clean_source()
    try:
        version = source_version()
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    rustc = read("rustc", "-vV")
    target = next(line.removeprefix("host: ") for line in rustc.splitlines() if line.startswith("host: "))
    local_distribution = args.local_distribution.resolve() if args.local_distribution else None
    local_record = None
    if local_distribution:
        from local_distribution import verify_directory
        local_record = verify_directory(local_distribution, version=version)
    console_record = {'mode': 'headless'}
    console_dist = args.console_dist.resolve() if args.console_dist else None
    if console_dist:
        validate_console(console_dist, source_commit=commit, version=version, project=ROOT / 'console')
        console_record = {'mode': 'static', 'manifest_sha256': digest(console_dist / 'console-manifest.json')}
        if not os.environ.get('LEDGENCE_POSTGRES_URL'):
            parser.error('Console bundle requires LEDGENCE_POSTGRES_URL for relocated verification')
    expected_rust = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    if not rustc.startswith("rustc " + expected_rust + " "):
        raise SystemExit("rustc does not match the committed toolchain")
    label = f"ledgence-{version}-{args.candidate}+g{commit[:12]}-{target}"
    epoch = int(read("git", "show", "-s", "--format=%ct", commit))
    env = dict(os.environ, SQLX_OFFLINE="true", SOURCE_DATE_EPOCH=str(epoch), PYTHONDONTWRITEBYTECODE="1")
    # No extra build/dependency versions are introduced by the release tool.
    build = ["cargo", "build", "--release", "--locked", "--all-features",
             "-p", "ledgence-cli", "--bin", "ledgence", "--target", target]
    if args.offline:
        build.append("--offline")
    command(build, env=env)
    metadata = json.loads(read("cargo", "metadata", "--locked", "--offline", "--no-deps", "--format-version", "1"))
    binaries = Path(metadata["target_directory"]) / target / "release"
    with tempfile.TemporaryDirectory(prefix="ledgence-candidate-") as temporary:
        stage = Path(temporary) / label
        (stage / "bin").mkdir(parents=True)
        for name in ("ledgence",):
            shutil.copyfile(binaries / name, stage / "bin" / name)
            (stage / "bin" / name).chmod(0o755)
        shutil.copytree(ROOT / "sdk/python/ledgence", stage / "runtime/ledgence",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copytree(ROOT / "docs", stage / "docs")
        if local_distribution:
            shutil.copytree(local_distribution, stage / "local")
            from local_distribution import verify_directory
            if verify_directory(stage / "local", version=version) != local_record:
                raise ValueError("local distribution changed during candidate assembly")
        if console_dist:
            shutil.copytree(console_dist, stage / 'console')
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
        (stage / "README.md").write_text(f"# {label}\n\nThis is a release candidate assembled from commit {commit}, not a stable release. Embedded Rust and Python package versions remain {version}.\n\nUse bin/ledgence with the program, worker, orchestrator, task, approval and mcp command groups. Supply a compatible host CPython 3.11–3.14 and pass --runner <bundle>/runtime/ledgence/worker/bootstrap.py. The native binary targets {target}; it requires host system libraries and does not include CPython, PostgreSQL or a broker. The client wheel and sdist are in python-client/; installing the wheel resolves the reviewed pinned dependencies. See docs/local-deployment.md and docs/releasing.md. The installed-SDK Compose companion is examples/local-compose-client.py; start and publish its programs from the matching source checkout first.\n\nThe console/ directory, when included, is served with bin/ledgence orchestrator serve --instance-config PATH --console-dir <bundle>/console; see docs/console.md. A headless build explicitly omits it.\n\nKeep LICENSE and legal/ with redistributed binaries; Python distributions carry their own retained legal files. Third-party software retains its original licenses.\n")
        if local_record:
            with (stage / 'README.md').open('a') as readme:
                readme.write('\nThis bundle includes a verified local/ Compose kit pinned to the qualified container image. '
                             'With Docker and Compose installed, run bin/ledgence local up. '
                             'Use local status, local logs and local down to manage it; down retains data. '
                             'See local/README.md for direct Compose use and examples.\n')
        # Captures dependency/toolchain identity, not a claim of byte-identical compilation.
        linker = ["otool", "-L"] if sys.platform == "darwin" else ["ldd"]
        linked = {name: read(*linker, str(stage / "bin" / name)) for name in ("ledgence",)}
        provenance = {"format": 1, "candidate": label, "source_commit": commit,
                      "source_tree_clean": True, "source_date_epoch": epoch,
                      "package_version": version, "target": target, "rustc": rustc, "console": console_record,
                      "local_distribution": local_record,
                      "cargo": read("cargo", "-V"), "python_builder": sys.version,
                      "host": platform.platform(), "features": "all features of ledgence-cli", "executables": ["ledgence"],
                      "cargo_lock_sha256": digest(ROOT / "Cargo.lock"),
                      "build_command": list(map(str, build)), "dynamic_libraries": linked,
                      "rustflags": os.environ.get("RUSTFLAGS"),
                      "encoded_rustflags": os.environ.get("CARGO_ENCODED_RUSTFLAGS"),
                      "validation": ["locked optimized native build", "selected Rust legal inventory", "installed client base/OTel and namespace tests", "relocated native bundle smoke"] + (["Console source/lock/toolchain identity", "relocated Console with real PostgreSQL, assets and API"] if console_dist else []),
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
