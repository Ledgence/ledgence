#!/usr/bin/env python3
"""Build and qualify ledgence-worker from its sdist outside the checkout.

Uses the client's already reviewed Flit backend and hash-checked wheelhouse.
The worker has no runtime dependencies. Client dependencies are installed only
for the separate namespace coexistence checks, never into the worker-only env.
This tests local artifacts; it does not publish or claim hosted CI results.
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
import tomllib
import venv
import zipfile

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "sdk/python"
SPEC = importlib.util.spec_from_file_location("client_package_check", ROOT / "tools/check-python-client.py")
client = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(client)
deps = client.deps
run, install, interpreter = client.run, client.install, client.interpreter
MODULES = {"__init__.py", "_logging.py", "_observations.py", "_protocol.py",
           "bootstrap.py", "otel.py", "workflow.py", "py.typed"}
SUITE = "tests/test_installed_package.py"


def check_project():
    project = tomllib.loads((WORKER / "pyproject.toml").read_text())
    metadata = project["project"]
    deps.require(metadata["name"] == "ledgence-worker", "unexpected worker distribution identity")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    deps.require(metadata["version"] == version, "worker version differs from workspace version")
    deps.require(metadata["requires-python"] == ">=3.11", "worker Python support drift")
    deps.require(metadata["license"] == "MIT" and metadata["license-files"] == ["LICENSE"], "worker license policy drift")
    deps.require(metadata["dependencies"] == [] and not metadata.get("optional-dependencies"), "worker acquired dependencies")
    deps.require(not any(metadata.get(key) for key in ("scripts", "gui-scripts", "entry-points")), "worker gained an operational entry point")
    deps.require(project["build-system"] == {"requires": ["flit_core==3.12.0"], "build-backend": "flit_core.buildapi"},
                 "worker build backend differs from reviewed build graph")
    deps.require(project["tool"]["flit"]["module"] == {"name": "ledgence.worker"}, "worker module identity drift")
    deps.require(not (WORKER / "ledgence/__init__.py").exists(), "worker owns the shared namespace root")
    deps.require((WORKER / "LICENSE").read_bytes() == (ROOT / "LICENSE").read_bytes(), "worker MIT license differs from root")
    source = WORKER / "ledgence/worker"
    files = {p.relative_to(source).as_posix() for p in source.rglob("*") if p.is_file()
             and "__pycache__" not in p.parts and p.suffix != ".pyc"}
    deps.require(files == MODULES, f"worker helper file inventory changed: {files ^ MODULES}")
    return metadata


def check_metadata(raw, project):
    metadata = email.message_from_bytes(raw)
    for key, expected in {"Name": "ledgence-worker", "Version": project["version"],
                          "Requires-Python": ">=3.11", "License-Expression": "MIT",
                          "Description-Content-Type": "text/markdown"}.items():
        deps.require(metadata[key] == expected, f"worker metadata {key} differs: {metadata[key]}")
    deps.require(not metadata.get_all("Requires-Dist") and not metadata.get_all("Provides-Extra"), "worker metadata declares dependencies")
    deps.require(metadata.get_all("License-File") == ["LICENSE"], "worker metadata omits MIT license")
    deps.require(metadata.get_payload(decode=True).decode("utf-8").rstrip("\n") ==
                 (WORKER / "README.md").read_text().rstrip("\n"), "worker README metadata differs")
    deps.require(set(metadata.get_all("Project-URL", [])) == {f"{label}, {url}" for label, url in project["urls"].items()},
                 "worker metadata project URLs differ")


def verify_distribution(path):
    project = check_project()
    helper = {"ledgence/worker/" + name: (WORKER / "ledgence/worker" / name).read_bytes() for name in MODULES}
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            deps.require(len(names) == len(set(names)), "worker wheel has duplicate members")
            files = {name: archive.read(name) for name in names}
        prefix = f"ledgence_worker-{project['version']}.dist-info/"
        expected = dict(helper, **{prefix + "licenses/LICENSE": (WORKER / "LICENSE").read_bytes()})
        deps.require(set(files) == set(expected) | {prefix + name for name in ("METADATA", "RECORD", "WHEEL")},
                     "worker wheel owns unexpected files or omits helper/legal files")
        check_metadata(files[prefix + "METADATA"], project)
        wheel = email.message_from_bytes(files[prefix + "WHEEL"])
        deps.require(wheel["Root-Is-Purelib"] == "true" and wheel.get_all("Tag") == ["py3-none-any"], "worker wheel is not pure Python")
    else:
        with tarfile.open(path) as archive:
            members = archive.getmembers()
            deps.require(all(not m.issym() and not m.islnk() for m in members), "worker sdist contains links")
            roots = {m.name.split("/", 1)[0] for m in members}
            deps.require(roots == {f"ledgence_worker-{project['version']}"}, "worker sdist root differs")
            names = [m.name for m in members]
            deps.require(len(names) == len(set(names)), "worker sdist has duplicate members")
            files = {m.name.split("/", 1)[1]: archive.extractfile(m).read() for m in members if m.isfile()}
        expected = dict(helper, **{name: (WORKER / name).read_bytes() for name in ("LICENSE", "README.md", "pyproject.toml", SUITE)})
        deps.require(set(files) == set(expected) | {"PKG-INFO"}, "worker sdist owns unexpected files or omits self-contained inputs")
        check_metadata(files["PKG-INFO"], project)
    for name, content in expected.items():
        deps.require(files.get(name) == content, f"worker artifact omits or changes source/legal bytes: {name}")


def build_from_sdist(build_python, source, temporary, env):
    dist = temporary / "dist"
    dist.mkdir()
    run([build_python, "-I", "-c", "import flit_core.buildapi as b,sys; b.build_sdist(sys.argv[1])", dist], source, env)
    sdist = next(dist.glob("*.tar.gz"))
    verify_distribution(sdist)
    unpacked = temporary / "unpacked"
    unpacked.mkdir()
    with tarfile.open(sdist) as archive:
        for member in archive:
            deps.require((unpacked / member.name).resolve().is_relative_to(unpacked), "sdist path escapes extraction directory")
        archive.extractall(unpacked, filter="data")
    rebuilt = next(unpacked.iterdir())
    run([build_python, "-I", "-c", "import flit_core.buildapi as b,sys; b.build_wheel(sys.argv[1])", dist], rebuilt, env)
    wheel = next(dist.glob("*.whl"))
    verify_distribution(wheel)
    return wheel, sdist, rebuilt


def check_environment(python, cwd, env, inventory, target, portions, order="worker-first", client_runtime=False):
    names = deps.closure(inventory, "runtime", target) if client_runtime else set()
    expected = {p["name"]: p["version"] for p in inventory["packages"] if p["name"] in names}
    project = check_project()
    expected.update({"ledgence-" + portion: project["version"] for portion in portions})
    code = '''import importlib, importlib.metadata as m, importlib.util, json, pathlib, re, sys
expected, portions = json.loads(sys.argv[1]), json.loads(sys.argv[2])
actual={re.sub(r"[-_.]+", "-", d.metadata["Name"]).lower():d.version for d in m.distributions()}
for name in ("pip", "setuptools"):actual.pop(name, None)
assert actual == expected, (actual, expected)
for portion in (["worker", "client"] if sys.argv[3] == "worker-first" else ["client", "worker"]):
    if portion not in portions:continue
    module=importlib.import_module("ledgence."+portion)
    assert pathlib.Path(module.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve()), module.__file__
    if portion == "client":assert module.__version__ == m.version("ledgence-client")
    if portion == "worker":
        from ledgence.worker import current_invocation, get_logger
        from ledgence.worker.workflow import Workflow, WorkflowContext, workflow_context
        for name in ("_logging", "_observations", "_protocol", "bootstrap", "otel", "workflow"):
            loaded=importlib.import_module("ledgence.worker."+name)
            assert pathlib.Path(loaded.__file__).resolve().parent == pathlib.Path(module.__file__).resolve().parent
    else:
        from ledgence.client import AsyncClient
try:import ledgence
except ModuleNotFoundError:
    assert not portions
else:
    assert ledgence.__spec__.origin is None, "shared ledgence root must be a native namespace"
    for portion in {"worker", "client"}-set(portions):
        assert importlib.util.find_spec("ledgence."+portion) is None, "uninstalled namespace portion remains importable"
assert "opentelemetry" not in sys.modules, "helper imports enabled optional tracing"
print("Verified installed graph and namespace origins:", portions, sys.argv[3])
'''
    run([python, "-I", "-B", "-c", code, json.dumps(expected), json.dumps(portions), order], cwd, env)
    run([python, "-I", "-m", "pip", "check", "--disable-pip-version-check"], cwd, env)


def install_wheel(python, wheel, cwd, env):
    run([python, "-I", "-m", "pip", "install", "--no-index", "--no-deps", "--no-compile", wheel], cwd, env)


def check_coexistence(build_python, worker_wheel, wheelhouse, temporary, env, inventory, target):
    source = temporary / "client-source"
    shutil.copytree(client.CLIENT, source, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "dist", ".venv"))
    dist = temporary / "client-dist"
    dist.mkdir()
    run([build_python, "-I", "-c", "import flit_core.buildapi as b,sys; b.build_wheel(sys.argv[1])", dist], source, env)
    client_wheel = next(dist.glob("*.whl"))
    client.verify_distribution(client_wheel, inventory)
    with zipfile.ZipFile(client_wheel) as first, zipfile.ZipFile(worker_wheel) as second:
        deps.require(not set(first.namelist()).intersection(second.namelist()), "client and worker distributions own overlapping files")
    for first in ("worker", "client"):
        runtime = temporary / (first + "-first-env")
        venv.create(runtime, with_pip=True, symlinks=True)
        python = interpreter(runtime)
        install(python, wheelhouse, "runtime", temporary, env)
        wheels = {"worker": worker_wheel, "client": client_wheel}
        second = "client" if first == "worker" else "worker"
        install_wheel(python, wheels[first], temporary, env)
        check_environment(python, temporary, env, inventory, target, [first], client_runtime=True)
        install_wheel(python, wheels[second], temporary, env)
        for order in ("worker-first", "client-first"):
            check_environment(python, temporary, env, inventory, target, [first, second], order, True)
        # Each package must survive uninstalling the other portion, regardless
        # of installation order. Each run also removes the final portion.
        run([python, "-I", "-m", "pip", "uninstall", "--yes", "ledgence-" + first], temporary, env)
        check_environment(python, temporary, env, inventory, target, [second], client_runtime=True)
        run([python, "-I", "-m", "pip", "uninstall", "--yes", "ledgence-" + second], temporary, env)
        check_environment(python, temporary, env, inventory, target, [], client_runtime=True)
    return client_wheel


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheelhouse", type=Path, help="reuse reviewed downloaded artifacts")
    parser.add_argument("--offline", action="store_true", help="require all wheelhouse artifacts already present")
    parser.add_argument("--dist-dir", type=Path, help="retain qualified worker wheel and sdist in a NEW directory")
    parser.add_argument("--evidence", type=Path, help="write a machine-readable local execution record")
    args = parser.parse_args()
    check_project()
    inventory = deps.check_inventory(json.loads((deps.LEGAL / "inventory.json").read_text()))
    deps.check_pyproject()
    target = deps.current_target()
    if args.dist_dir:
        args.dist_dir = args.dist_dir.resolve()
        deps.require(not args.dist_dir.exists(), "--dist-dir must not already exist")
    env = dict(os.environ)
    for name in ("PYTHONPATH", "PYTHONHOME", "LEDGENCE_JSON_FIXTURES"):
        env.pop(name, None)
    env.update(PYTHONDONTWRITEBYTECODE="1", PIP_DISABLE_PIP_VERSION_CHECK="1", PIP_CONFIG_FILE=os.devnull,
               SOURCE_DATE_EPOCH="1789257600", LEDGENCE_WORKER_INSTALLED_TESTS="1")
    with tempfile.TemporaryDirectory(prefix="ledgence-python-worker-") as directory:
        temporary = Path(directory).resolve()
        deps.require(not temporary.is_relative_to(ROOT), "installed qualification must run outside checkout")
        wheelhouse = args.wheelhouse.resolve() if args.wheelhouse else temporary / "wheelhouse"
        deps.download(inventory, wheelhouse, [target], ["build", "runtime"], offline=args.offline)
        build_dir, runtime_dir = temporary / "build-env", temporary / "worker-env"
        for path in (build_dir, runtime_dir):
            venv.create(path, with_pip=True, symlinks=True)
        build_python, runtime_python = interpreter(build_dir), interpreter(runtime_dir)
        install(build_python, wheelhouse, "build", temporary, env)
        source = temporary / "source"
        shutil.copytree(WORKER, source, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "dist", ".venv"))
        worker_wheel, sdist, rebuilt = build_from_sdist(build_python, source, temporary, env)
        install_wheel(runtime_python, worker_wheel, temporary, env)
        check_environment(runtime_python, temporary, env, inventory, target, ["worker"])
        tests = temporary / "installed-tests"
        shutil.copytree(rebuilt / "tests", tests)
        run([runtime_python, "-I", "-B", "-m", "unittest", "discover", "-s", tests, "-v"], temporary, env)
        client_wheel = check_coexistence(build_python, worker_wheel, wheelhouse, temporary, env, inventory, target)
        evidence = {"target": target, "python": sys.version, "platform": sys.platform,
                    "sdist": {"filename": sdist.name, "sha256": deps.digest(sdist.read_bytes())},
                    "wheel": {"filename": worker_wheel.name, "sha256": deps.digest(worker_wheel.read_bytes())},
                    "client_wheel": {"filename": client_wheel.name, "sha256": deps.digest(client_wheel.read_bytes())},
                    "checks": ["reviewed build and client dependency artifact hashes and legal bytes",
                               "worker has zero runtime dependencies and no CLI entry point",
                               "full source parity, namespace ownership, MIT license and typing marker in sdist/wheel",
                               "worker wheel rebuilt from sdist", "workspace version and README metadata",
                               "clean installed graph and origin assertions outside checkout without PYTHONPATH",
                               "installed invocation, checkpoint, local replay, logging and optional tracing tests",
                               "dedicated helper relocated from installed bytes under isolated protocols 1/2/3",
                               "actual client/worker wheels in both install, import and uninstall orders"],
                    "hosted_ci": "not claimed by this local run", "publication": "not performed"}
        if args.dist_dir:
            args.dist_dir.mkdir(parents=True)
            for path in (worker_wheel, sdist):
                shutil.copyfile(path, args.dist_dir / path.name)
        if args.evidence:
            args.evidence.parent.mkdir(parents=True, exist_ok=True)
            args.evidence.write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        sys.exit(f"Python worker build/install check failed: {error}")
