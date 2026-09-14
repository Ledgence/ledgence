#!/usr/bin/env python3
"""Build an sdist and wheel, then exercise the installed client outside checkout.

The Python interpreter running this tool selects the matrix target. Dependencies
come only from the reviewed, hash-checked wheelhouse. --venv-dir retains the wheel
installation for real PostgreSQL/HTTP acceptance; otherwise everything is temporary.
"""
from __future__ import annotations

import argparse
import email
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "sdk/python-client"
SPEC = importlib.util.spec_from_file_location("client_dependencies", ROOT / "tools/check-python-client-dependencies.py")
deps = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deps)


def run(command, cwd, env):
    print("+", " ".join(map(str, command)), flush=True)
    subprocess.run(list(map(str, command)), cwd=cwd, env=env, check=True)


def interpreter(directory):
    return directory / "bin/python"


def install(python, wheelhouse, group, cwd, env):
    run([python, "-I", "-m", "pip", "install", "--disable-pip-version-check", "--no-input", "--no-compile",
         "--require-hashes", "--only-binary=:all:", "--no-index", "--find-links", wheelhouse,
         "-r", deps.LEGAL / f"{group}-requirements.txt"], cwd, env)


def verify_distribution(path, inventory):
    expected = {"third_party/" + r["path"]: deps.legal_path(r).read_bytes()
                for p in inventory["packages"] for a in [p["source"], *p["artifacts"]]
                for r in a["license_files"].values()}
    expected["LICENSE"] = (CLIENT / "LICENSE").read_bytes()
    expected["third_party/NOTICE.md"] = (deps.LEGAL / "NOTICE.md").read_bytes()
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            metadata_name = next(n for n in names if n.endswith(".dist-info/METADATA"))
            metadata = email.message_from_bytes(archive.read(metadata_name))
            deps.require(metadata["Name"] == "ledgence-client" and metadata["License-Expression"] == "MIT", "wheel identity/license mismatch")
            deps.require(metadata["Requires-Python"] == ">=3.11", "wheel Python requirement drift")
            requirements = metadata.get_all("Requires-Dist", [])
            expected_requirements = deps.project_requirements("runtime") + [r + '; extra == "otel"' for r in deps.project_requirements("otel")]
            normalize = lambda r: r.replace(" ", "").replace("'", '"')
            deps.require(sorted(map(normalize, requirements)) == sorted(map(normalize, expected_requirements)), "wheel graph differs from pyproject review")
            prefix = metadata_name.rsplit("/", 1)[0] + "/licenses/"
            for name, content in expected.items():
                deps.require(prefix + name in names and archive.read(prefix + name) == content, f"wheel omits legal material: {name}")
            deps.require("ledgence/client/__init__.py" in names, "public ledgence.client module missing")
            deps.require(not any(n.startswith(("ledgence_worker/", "aiohttp/", "opentelemetry/")) for n in names), "client wheel bundles unrelated package code")
    else:
        expected["third_party/inventory.json"] = (deps.LEGAL / "inventory.json").read_bytes()
        for group in deps.GROUPS:
            expected[f"third_party/{group}-requirements.txt"] = (deps.LEGAL / f"{group}-requirements.txt").read_bytes()
        with tarfile.open(path) as archive:
            files = {m.name.split("/", 1)[1]: archive.extractfile(m).read() for m in archive if m.isfile()}
            for name, content in expected.items():
                deps.require(files.get(name) == content, f"sdist omits legal/lock material: {name}")


def check_environment(python, cwd, env, inventory, target, otel):
    names = deps.closure(inventory, "runtime", target)
    if otel:
        names |= deps.closure(inventory, "otel", target)
    expected = {p["name"]: p["version"] for p in inventory["packages"] if p["name"] in names}
    code = '''import importlib.metadata as m, json, pathlib, re, sys
from ledgence.client import AsyncClient
import ledgence.client
expected=json.loads(sys.argv[1])
actual={re.sub(r"[-_.]+", "-", d.metadata["Name"]).lower():d.version for d in m.distributions()}
client=m.version("ledgence-client")
actual.pop("ledgence-client")
# ensurepip is part of the selected host interpreter, not a shipped SDK dependency.
for tool in ("pip", "setuptools"):actual.pop(tool,None)
if actual != expected:raise SystemExit(f"unexpected installed graph: {actual} != {expected}")
location=pathlib.Path(ledgence.client.__file__).resolve()
if not location.is_relative_to(pathlib.Path(sys.prefix).resolve()):raise SystemExit(f"import escaped installed wheel: {location}")
if not sys.argv[2] == "otel":
 try:m.distribution("opentelemetry-api")
 except m.PackageNotFoundError:pass
 else:raise SystemExit("optional tracing installed in base environment")
print(f"Verified ledgence-client {client} imported from {location}")
'''
    run([python, "-I", "-c", code, json.dumps(expected), "otel" if otel else "base"], cwd, env)
    run([python, "-I", "-m", "pip", "check", "--disable-pip-version-check"], cwd, env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheelhouse", type=Path, help="reuse downloaded reviewed artifacts")
    parser.add_argument("--offline", action="store_true", help="require wheelhouse artifacts already present")
    parser.add_argument("--venv-dir", type=Path, help="retain a NEW installed environment for end-to-end acceptance")
    parser.add_argument("--evidence", type=Path, help="write a machine-readable local execution record")
    args = parser.parse_args()
    inventory = deps.check_inventory(json.loads((deps.LEGAL / "inventory.json").read_text()))
    deps.check_pyproject()
    target = deps.current_target()
    if args.venv_dir:
        args.venv_dir = args.venv_dir.resolve()
        deps.require(not args.venv_dir.exists(), "--venv-dir must not already exist")
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env.update(PYTHONDONTWRITEBYTECODE="1", PIP_DISABLE_PIP_VERSION_CHECK="1", PIP_CONFIG_FILE=os.devnull,
               SOURCE_DATE_EPOCH="1789257600")
    with tempfile.TemporaryDirectory(prefix="ledgence-python-client-") as directory:
        temporary = Path(directory).resolve()
        wheelhouse = args.wheelhouse.resolve() if args.wheelhouse else temporary / "wheelhouse"
        deps.download(inventory, wheelhouse, [target], list(deps.GROUPS), offline=args.offline)
        shared_fixture = ROOT / "tests/fixtures/json-values.json"
        fixture = temporary / "json-values.json"
        shutil.copy2(shared_fixture, fixture)
        env["LEDGENCE_JSON_FIXTURES"] = str(fixture)
        build_dir = temporary / "build-env"
        runtime_dir = args.venv_dir or temporary / "runtime-env"
        # The reviewed targets are POSIX. Keep standalone interpreter loader
        # paths intact instead of copying a relocatable executable into the venv.
        venv.create(build_dir, with_pip=True, symlinks=True)
        venv.create(runtime_dir, with_pip=True, symlinks=True)
        build_python, runtime_python = interpreter(build_dir), interpreter(runtime_dir)
        install(build_python, wheelhouse, "build", temporary, env)
        source = temporary / "source"
        shutil.copytree(CLIENT, source, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "dist", ".venv"))
        dist = temporary / "dist"
        dist.mkdir()
        run([build_python, "-I", "-c", "import flit_core.buildapi as b,sys; b.build_sdist(sys.argv[1])", dist], source, env)
        sdist = next(dist.glob("*.tar.gz"))
        verify_distribution(sdist, inventory)
        unpacked = temporary / "unpacked"
        unpacked.mkdir()
        with tarfile.open(sdist) as archive:
            # Only our freshly built sdist is extracted, and paths/links are still checked.
            for member in archive:
                deps.require(not member.issym() and not member.islnk(), "sdist contains a link")
                deps.require((unpacked / member.name).resolve().is_relative_to(unpacked), "sdist path escapes extraction directory")
            archive.extractall(unpacked, filter="data")
        rebuilt = next(unpacked.iterdir())
        run([build_python, "-I", "-c", "import flit_core.buildapi as b,sys; b.build_wheel(sys.argv[1])", dist], rebuilt, env)
        wheel = next(dist.glob("*.whl"))
        verify_distribution(wheel, inventory)
        install(runtime_python, wheelhouse, "runtime", temporary, env)
        # An empty separately reviewed test lock intentionally installs no test framework.
        install(runtime_python, wheelhouse, "test", temporary, env)
        run([runtime_python, "-I", "-m", "pip", "install", "--no-index", "--no-deps", "--no-compile", wheel], temporary, env)
        check_environment(runtime_python, temporary, env, inventory, target, False)
        tests = temporary / "installed-tests"
        shutil.copytree(rebuilt / "tests", tests)
        run([runtime_python, "-I", "-m", "unittest", "discover", "-s", tests, "-v"], temporary, env)
        install(runtime_python, wheelhouse, "otel", temporary, env)
        check_environment(runtime_python, temporary, env, inventory, target, True)
        run([runtime_python, "-I", "-m", "unittest", "discover", "-s", tests, "-v"], temporary, env)
        evidence = {"target": target, "python": sys.version, "platform": sys.platform,
                    "sdist": {"filename": sdist.name, "sha256": deps.digest(sdist.read_bytes())},
                    "wheel": {"filename": wheel.name, "sha256": deps.digest(wheel.read_bytes())},
                    "checks": ["reviewed artifact hashes and legal bytes", "sdist legal contents", "wheel rebuilt from sdist",
                               "wheel legal contents", "installed base graph and tests outside checkout", "installed optional OTel graph and tests outside checkout"],
                    "retained_python": str(runtime_python) if args.venv_dir else None,
                    "hosted_ci": "not claimed by this local run"}
        if args.evidence:
            args.evidence.parent.mkdir(parents=True, exist_ok=True)
            args.evidence.write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        sys.exit(f"Python client build/install check failed: {error}")
