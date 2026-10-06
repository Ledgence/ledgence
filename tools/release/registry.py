#!/usr/bin/env python3
"""Validate registry release sources and verify the bytes users download.

This tool never uploads, tags, or promotes branches. Publication is performed by
the registry's official action/Cargo after the workflow's required gates pass.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
import venv

ROOT = Path(__file__).resolve().parents[2]
CRATES = ("ledgence-worker-api", "ledgence-orchestration-api")
PYPI_PACKAGES = ("ledgence-client", "ledgence-worker")
VERSION = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(*command, cwd=ROOT, env=None):
    subprocess.run(command, cwd=cwd, env=env, check=True)


def git(*args, root=ROOT):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def release_version(root=ROOT):
    cargo = tomllib.loads((root / "Cargo.toml").read_text())
    python = tomllib.loads((root / "sdk/python-client/pyproject.toml").read_text())
    version = cargo["workspace"]["package"]["version"]
    require(VERSION.fullmatch(version), "registry releases require a final x.y.z version")
    require(python["project"]["version"] == version, "Rust/Python release versions differ")
    require(python["project"]["name"] == "ledgence-client", "unexpected Python distribution")
    worker = tomllib.loads((root / "sdk/python/pyproject.toml").read_text())["project"]
    require(worker["name"] == "ledgence-worker", "unexpected Python worker distribution")
    require(worker["version"] == version, "Rust/Python worker release versions differ")
    return version


def check_source(publish=False, root=ROOT, environ=None):
    environ = os.environ if environ is None else environ
    version = release_version(root)
    require(not git("status", "--porcelain", "--untracked-files=all", root=root),
            "registry qualification requires a clean committed checkout")
    if publish:
        require(environ.get("GITHUB_REPOSITORY") == "Ledgence/ledgence",
                "publication must run in Ledgence/ledgence")
        ref = environ.get("GITHUB_REF", "")
        require(ref == f"refs/tags/v{version}", "publication must run on its matching version tag")
        require(git("cat-file", "-t", ref, root=root) == "tag", "release tag must be annotated")
        require(git("rev-parse", ref + "^{commit}", root=root) == git("rev-parse", "HEAD", root=root),
                "checkout does not match release tag")
        # A fetched main ref is required; a missing ref fails closed.
        subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", "refs/remotes/origin/main"],
                       cwd=root, check=True)
    return version


def pypi_filenames(package, version):
    require(package in PYPI_PACKAGES, "unexpected PyPI project")
    stem = package.replace("-", "_")
    return [f"{stem}-{version}-py3-none-any.whl", f"{stem}-{version}.tar.gz"]


def inventory(directory, version, kind, package=None):
    require(package is None or kind == "pypi", "project selection requires PyPI")
    names = ([name for project in ([package] if package else PYPI_PACKAGES)
              for name in pypi_filenames(project, version)] if kind == "pypi" else
             [f"{name}-{version}.crate" for name in CRATES])
    require(directory.is_dir(), f"distribution directory missing: {directory}")
    require(sorted(p.name for p in directory.iterdir()) == sorted(names),
            f"unexpected {kind} distribution file set")
    result = {}
    for name in names:
        path = directory / name
        require(path.is_file() and not path.is_symlink(), f"invalid distribution: {name}")
        result[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def compare(first, second, version, kind):
    require(inventory(first, version, kind) == inventory(second, version, kind),
            f"rebuilt {kind} artifacts differ from qualified bytes")


def collect_pypi(client_directory, worker_directory, output, version):
    """Combine only the exact distributions retained by both qualification gates."""
    sources = {"ledgence-client": client_directory, "ledgence-worker": worker_directory}
    expected = {}
    for package, directory in sources.items():
        expected.update(inventory(directory, version, "pypi", package))
    require(not output.exists(), "qualified Python output directory must be new")
    output.mkdir(parents=True)
    for package, directory in sources.items():
        for name in pypi_filenames(package, version):
            shutil.copyfile(directory / name, output / name)
    require(inventory(output, version, "pypi") == expected,
            "collected Python distributions changed from qualified bytes")
    return expected


def get(url, missing_ok=False):
    request = urllib.request.Request(url, headers={
        "User-Agent": "Ledgence-release-verification (https://github.com/Ledgence/ledgence)"
    })
    # Registry index propagation may lag the successful upload. Retry only
    # transient transport/status failures, never an observed checksum mismatch.
    deadline = time.monotonic() + 180
    while True:
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if missing_ok and error.code == 404:
                return None
            if error.code not in (404, 429, 500, 502, 503, 504):
                raise
            if time.monotonic() >= deadline:
                raise
        except (urllib.error.URLError, TimeoutError):
            if time.monotonic() >= deadline:
                raise
        time.sleep(3)


def registry_inventory(directory, version, kind, allow_missing=False):
    """Read and hash actual downloads; an existing filename is never enough."""
    expected = inventory(directory, version, kind)
    existing = {}
    if kind == "pypi":
        for package in PYPI_PACKAGES:
            raw = get(f"https://pypi.org/pypi/{package}/{version}/json", missing_ok=allow_missing)
            if raw is None:
                continue
            metadata = json.loads(raw)
            require(re.sub(r"[-_.]+", "-", metadata["info"]["name"]).lower() == package,
                    f"PyPI project identity differs: {package}")
            require(metadata["info"]["version"] == version, "PyPI version differs")
            files = {file["filename"]: file for file in metadata["urls"]}
            require(len(files) == len(metadata["urls"]), "duplicate PyPI filenames")
            require(set(files) <= set(pypi_filenames(package, version)),
                    f"PyPI file set differs from qualified artifacts: {package}")
            for name, file in files.items():
                digest = expected[name]
                require(not file.get("yanked"), f"PyPI artifact is yanked: {name}")
                require(file["digests"]["sha256"] == digest, f"PyPI metadata checksum differs: {name}")
                require(file["url"].startswith("https://files.pythonhosted.org/"),
                        "unexpected PyPI download host")
                require(hashlib.sha256(get(file["url"])).hexdigest() == digest,
                        f"PyPI downloaded bytes differ: {name}")
                existing[name] = digest
    else:
        for name in CRATES:
            raw = get(f"https://crates.io/api/v1/crates/{name}/{version}", missing_ok=allow_missing)
            if raw is None:
                continue
            data = json.loads(raw)["version"]
            filename = f"{name}-{version}.crate"
            digest = expected[filename]
            require(data["num"] == version and not data["yanked"], f"unexpected crate version: {name}")
            require(data["checksum"] == digest, f"crates.io checksum differs: {name}")
            content = get(f"https://crates.io/api/v1/crates/{name}/{version}/download")
            require(hashlib.sha256(content).hexdigest() == digest,
                    f"crates.io downloaded bytes differ: {name}")
            existing[filename] = digest
    require(allow_missing or existing == expected, f"{kind} file set differs from qualified artifacts")
    return existing


def verify_registry(directory, version, kind):
    verified = registry_inventory(directory, version, kind)
    print(json.dumps({"registry": kind, "version": version, "verified_sha256": verified}, indent=2))


def prepare_publication(directory, output, version, kind):
    """Resume interrupted publication only after existing bytes match exactly."""
    expected = inventory(directory, version, kind)
    existing = registry_inventory(directory, version, kind, allow_missing=True)
    require(not output.exists(), "pending upload directory must be new")
    output.mkdir(parents=True)
    missing = [name for name in expected if name not in existing]
    for name in missing:
        shutil.copyfile(directory / name, output / name)
        require(hashlib.sha256((output / name).read_bytes()).hexdigest() == expected[name],
                f"pending distribution changed: {name}")
    return {"registry": kind, "version": version, "existing_sha256": existing,
            "missing": missing,
            "packages": ([name for name in CRATES if f"{name}-{version}.crate" in missing] if kind == "crates" else
                         [name for name in PYPI_PACKAGES if set(pypi_filenames(name, version)).intersection(missing)])}


def publication_outputs(result):
    # Package names are the fixed allowlist, never arbitrary registry metadata.
    return (f"upload={'true' if result['missing'] else 'false'}\n"
            f"packages={' '.join(result['packages'])}\n")


def check_pypi_install_report(report, version, expected=None, required=None):
    """Bind a fresh index installation to qualified wheel bytes when supplied."""
    found = set()
    for item in json.loads(report.read_text())["install"]:
        package = re.sub(r"[-_.]+", "-", item["metadata"]["name"]).lower()
        if package not in PYPI_PACKAGES:
            continue
        require(package not in found, "duplicate installed PyPI project")
        found.add(package)
        require(item["metadata"]["version"] == version and not item.get("is_yanked"),
                f"installed PyPI version differs: {package}")
        download = item["download_info"]
        filename = pypi_filenames(package, version)[0]
        require(download["url"].startswith("https://files.pythonhosted.org/")
                and download["url"].rsplit("/", 1)[-1] == filename,
                f"installed PyPI artifact did not come from the public wheel: {package}")
        if expected is not None:
            require(download["archive_info"]["hashes"]["sha256"] == expected[filename],
                    f"installed PyPI checksum differs from qualified bytes: {package}")
    require(required is None or required in found, f"fresh PyPI install omitted required project: {required}")


def check_python_installation(python, directory, env, version, packages, order="worker-first", otel=False):
    run(python, "-I", "-m", "pip", "check", cwd=directory, env=env)
    run(python, "-I", "-c", '''import importlib, importlib.metadata as m, importlib.util, json, pathlib, sys
packages = json.loads(sys.argv[2])
prefix = pathlib.Path(sys.prefix).resolve()
for name in (["worker", "client"] if sys.argv[3] == "worker-first" else ["client", "worker"]):
    package = "ledgence-" + name
    if package not in packages:continue
    module = importlib.import_module("ledgence." + name)
    assert m.version(package) == sys.argv[1]
    assert pathlib.Path(module.__file__).resolve().is_relative_to(prefix)
    if name == "client":
        from ledgence.client import AsyncClient
    else:
        from ledgence.worker import current_invocation, get_logger
        from ledgence.worker.workflow import Workflow, WorkflowError, workflow_context
        assert not m.distribution(package).requires, "worker acquired runtime dependencies"
        assert pathlib.Path(module.__file__).with_name("py.typed").is_file()
        try:current_invocation()
        except RuntimeError:pass
        else:raise AssertionError("unexpected invocation outside runtime")
        try:workflow_context()
        except WorkflowError:pass
        else:raise AssertionError("unexpected workflow outside runtime")
import ledgence
assert ledgence.__spec__.origin is None
for name in ("worker", "client"):
    if "ledgence-" + name not in packages:assert importlib.util.find_spec("ledgence." + name) is None
if packages == ["ledgence-worker"]:
    actual = {d.metadata["Name"].lower().replace("_", "-") for d in m.distributions()}
    assert actual - {"pip", "setuptools"} == {"ledgence-worker"}, actual
if sys.argv[4] == "otel":
    from ledgence.worker.otel import enable_context
    enable_context()
else:
    assert importlib.util.find_spec("opentelemetry") is None
print("Verified fresh PyPI installation:", packages, sys.argv[3], sys.argv[4])
''', version, json.dumps(packages), order, "otel" if otel else "base", cwd=directory, env=env)


def install_from_registry(version, kind, dist=None):
    expected = inventory(dist, version, kind) if dist is not None else None
    with tempfile.TemporaryDirectory(prefix="ledgence-registry-install-") as temp:
        directory = Path(temp)
        env = dict(os.environ)
        for name in ("PYTHONPATH", "PYTHONHOME", "CARGO_REGISTRY_TOKEN", "CARGO_TARGET_DIR"):
            env.pop(name, None)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        if kind == "pypi":
            for first in PYPI_PACKAGES:
                environment = directory / first
                venv.create(environment, with_pip=True, symlinks=True)
                python = str(environment / "bin/python")
                packages = [first, *[name for name in PYPI_PACKAGES if name != first]]
                for index, package in enumerate([*packages, "ledgence-client[otel]"]):
                    report = directory / f"{first}-{index}.json"
                    run(python, "-I", "-m", "pip", "--isolated", "install", "--no-cache-dir",
                        "--only-binary=:all:", "--index-url", "https://pypi.org/simple",
                        "--report", str(report), f"{package}=={version}", cwd=directory, env=env)
                    check_pypi_install_report(report, version, expected, package if index < 2 else None)
                    present = packages[:index + 1]
                    for order in ("worker-first", "client-first") if index else ("worker-first",):
                        check_python_installation(python, directory, env, version, present, order, index == 2)
        else:
            # No checkout paths or patches: Cargo must resolve both published
            # Ledgence packages through the public sparse registry.
            (directory / "Cargo.toml").write_text(
                '[package]\nname="ledgence-registry-check"\nversion="0.0.0"\nedition="2024"\n'
                '[dependencies]\n' +
                "".join(f'{name} = "={version}"\n' for name in CRATES))
            (directory / "src").mkdir()
            (directory / "src/main.rs").write_text(
                'fn main() {\n'
                'ledgence_worker_api::ProgramRef { id: "example".into(), version: "1.0.0".into() }'
                '.validate().unwrap();\n'
                'ledgence_orchestration_api::RetryPolicy::default().validate().unwrap();\n}\n')
            # Use a new Cargo home so cached local packages/config cannot satisfy
            # this check. rustup still uses the separately installed toolchain.
            env["CARGO_HOME"] = str(directory / "cargo-home")
            env.pop("CARGO_NET_OFFLINE", None)
            env["CARGO_REGISTRIES_CRATES_IO_PROTOCOL"] = "sparse"
            env["CARGO_REGISTRIES_CRATES_IO_INDEX"] = "sparse+https://index.crates.io/"
            toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
            run("cargo", f"+{toolchain}", "run", "--quiet", cwd=directory, env=env)
            lock = tomllib.loads((directory / "Cargo.lock").read_text())
            for name in CRATES:
                package = next(p for p in lock["package"] if p["name"] == name)
                require(package.get("source") == "registry+https://github.com/rust-lang/crates.io-index"
                        and package["version"] == version, f"{name} did not resolve the required version from crates.io")
                if expected is not None:
                    require(package.get("checksum") == expected[f"{name}-{version}.crate"],
                            f"installed crate checksum differs from qualified bytes: {name}")
    print(f"Fresh {kind} registry installation passed for {version}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("source")
    source.add_argument("--publish", action="store_true")
    collect = commands.add_parser("collect-pypi")
    collect.add_argument("--version", required=True)
    collect.add_argument("--client-dist", type=Path, required=True)
    collect.add_argument("--worker-dist", type=Path, required=True)
    collect.add_argument("--output", type=Path, required=True)
    for name in ("compare", "verify", "install", "prepare"):
        command = commands.add_parser(name)
        command.add_argument("--kind", choices=("pypi", "crates"), required=True)
        command.add_argument("--version", required=True)
        if name == "compare":
            command.add_argument("--first", type=Path, required=True)
            command.add_argument("--second", type=Path, required=True)
        elif name in ("verify", "prepare"):
            command.add_argument("--dist", type=Path, required=True)
            if name == "prepare":
                command.add_argument("--output", type=Path, required=True)
                command.add_argument("--github-output", type=Path)
        elif name == "install":
            command.add_argument("--dist", type=Path, help="qualified artifacts to match installed wheel or Cargo lock checksums")
    args = parser.parse_args()
    if args.command == "source":
        print(check_source(args.publish))
        return
    require(VERSION.fullmatch(args.version), "expected a final x.y.z version")
    if args.command == "collect-pypi":
        print(json.dumps(collect_pypi(args.client_dist, args.worker_dist, args.output, args.version), indent=2))
    elif args.command == "compare":
        compare(args.first, args.second, args.version, args.kind)
    elif args.command == "verify":
        verify_registry(args.dist, args.version, args.kind)
    elif args.command == "prepare":
        result = prepare_publication(args.dist, args.output, args.version, args.kind)
        if args.github_output:
            with args.github_output.open("a") as stream:
                stream.write(publication_outputs(result))
        print(json.dumps(result, indent=2))
    else:
        install_from_registry(args.version, args.kind, args.dist)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        sys.exit(f"Registry verification failed: {error}")
