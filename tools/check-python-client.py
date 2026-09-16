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


def install(python, wheelhouse, group, cwd, env, target=None):
    run([python, "-I", "-m", "pip", "install", "--disable-pip-version-check", "--no-input", "--no-compile",
         "--require-hashes", "--only-binary=:all:", "--no-index", "--find-links", wheelhouse,
         "-r", deps.LEGAL / f"{group}-requirements.txt",
         *(["--target", target] if target else [])], cwd, env)


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
            deps.require("ledgence/client/py.typed" in names, "client typing marker missing")
            deps.require(not any(n.startswith("ledgence/") and not n.startswith("ledgence/client/")
                                 for n in names), "client wheel owns files outside its namespace portion")
            deps.require(not any(n.startswith(("ledgence_worker/", "aiohttp/", "opentelemetry/")) for n in names), "client wheel bundles unrelated package code")
    else:
        expected["third_party/inventory.json"] = (deps.LEGAL / "inventory.json").read_bytes()
        for group in deps.GROUPS:
            expected[f"third_party/{group}-requirements.txt"] = (deps.LEGAL / f"{group}-requirements.txt").read_bytes()
        with tarfile.open(path) as archive:
            files = {m.name.split("/", 1)[1]: archive.extractfile(m).read() for m in archive if m.isfile()}
            for name, content in expected.items():
                deps.require(files.get(name) == content, f"sdist omits legal/lock material: {name}")
            deps.require("src/ledgence/client/__init__.py" in files, "sdist client package missing")
            deps.require("src/ledgence/client/py.typed" in files, "sdist client typing marker missing")
            deps.require(not any(n.startswith("src/ledgence/") and not n.startswith("src/ledgence/client/")
                                 for n in files), "client sdist owns files outside its namespace portion")


def check_environment(python, cwd, env, inventory, target, otel):
    names = deps.closure(inventory, "runtime", target)
    if otel:
        names |= deps.closure(inventory, "otel", target)
    expected = {p["name"]: p["version"] for p in inventory["packages"] if p["name"] in names}
    code = '''import importlib.metadata as m, json, pathlib, re, sys
from ledgence.client import AsyncClient
import ledgence.client
import importlib.util
if ledgence.__spec__.origin is not None:raise SystemExit("ledgence must be a native namespace")
if importlib.util.find_spec("ledgence.worker") is not None:raise SystemExit("client-only environment includes worker helper")
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


def check_namespace_coexistence(python, wheelhouse, wheel, temporary, env):
    """Exercise independently delivered namespace portions outside the checkout."""
    helper_root = temporary / "relocated-helper"
    helper = helper_root / "ledgence/worker"
    shutil.copytree(ROOT / "sdk/python/ledgence/worker", helper,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    code = """import importlib, pathlib, sys
helper = pathlib.Path(sys.argv[1]).resolve()
if sys.argv[2] == 'client-first':
    from ledgence.client import AsyncClient
sys.path.insert(0, str(helper))
from ledgence.worker import current_invocation, get_logger
from ledgence.worker.workflow import workflow_context
from ledgence.client import AsyncClient
import ledgence, ledgence.client, ledgence.worker
if ledgence.__spec__.origin is not None:raise SystemExit('ledgence root is not a namespace')
if not pathlib.Path(ledgence.client.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve()):
    raise SystemExit('client did not come from installed wheel')
if not pathlib.Path(ledgence.worker.__file__).resolve().is_relative_to(helper):
    raise SystemExit('worker did not come from relocated helper')
try:current_invocation()
except RuntimeError:pass
else:raise SystemExit('unexpected invocation outside runtime')
print('Verified separate client/worker namespace portions:', sys.argv[2])
"""
    for order in ("client-first", "worker-first"):
        run([python, "-I", "-B", "-c", code, helper_root, order], temporary, env)

    # Prepare a real program artifact containing the built client and its pinned
    # runtime dependencies. -S prevents accidental use of the installed venv SDK.
    artifact = temporary / "prepared-program"
    install(python, wheelhouse, "runtime", temporary, env, target=artifact)
    run([python, "-I", "-m", "pip", "install", "--no-index", "--no-deps", "--no-compile",
         "--target", artifact, wheel], temporary, env)
    (artifact / "program.py").write_text("""from pathlib import Path
import os
from ledgence.client import AsyncClient
import ledgence, ledgence.client, ledgence.worker
from ledgence.worker import current_invocation, get_logger
from ledgence.worker.workflow import workflow_context, WorkflowError

root = Path(__file__).resolve().parent
assert ledgence.__spec__.origin is None
assert Path(ledgence.client.__file__).resolve().is_relative_to(root)
assert not Path(ledgence.worker.__file__).resolve().is_relative_to(root)
assert AsyncClient.__module__ == 'ledgence.client.client'
try:
    current_invocation()
except RuntimeError:
    pass
else:
    raise AssertionError('unexpected invocation during program import')
logger = get_logger('namespace.acceptance')

def handle(event):
    invocation = current_invocation()
    assert invocation.event_id == event['id']
    assert invocation.attempt_id == event['ldgattemptid']
    logger.info('namespace invocation')
    output = {'event_id': invocation.event_id, 'attempt_id': invocation.attempt_id,
              'pid': os.getpid(), 'client': AsyncClient.__name__}
    if 'ldgworkflowid' in event:
        context = workflow_context()
        assert invocation.workflow_id == context.workflow_id
        assert invocation.activation_id == context.activation_id
        return context.complete(output)
    try:
        workflow_context()
    except WorkflowError:
        return output
    raise AssertionError('workflow context leaked into ordinary invocation')
""")
    for version in (1, 2, 3):
        messages = []
        for number in (1, 2):
            event_id, attempt_id = f"evt-{number}", f"attempt-{number}"
            event = {"specversion": "1.0", "source": "/namespace-acceptance", "id": event_id,
                     "type": "example.run", "ldgattemptid": attempt_id, "data": {}}
            message = {"v": version, "type": "invoke", "event_id": event_id,
                       "attempt_id": attempt_id, "event": event}
            if version >= 2:
                message["processing_context"] = None
            if version == 3 and number == 1:
                event.update(ldgworkflowid="workflow-1", ldgactivationid="activation-1",
                             ldgtaskid="activation-1")
                message["extension"] = {"schema": "ledgence.workflow.activation.v1", "payload": {
                    "v": 1, "workflow_id": "workflow-1", "activation_id": "activation-1",
                    "revision": 0, "continuation": "start", "state": None,
                    "inputs": {}, "local_steps": []}}
            messages.append(message)
        messages.append({"v": version, "type": "shutdown"})
        command = [python, "-I", "-S", "-B", helper / "bootstrap.py", "--package-root", artifact,
                   "--handler", "program:handle", "--python-version", "%d.%d" % sys.version_info[:2],
                   "--protocol-version", str(version)]
        print("+", " ".join(map(str, command)), flush=True)
        result = subprocess.run(list(map(str, command)), cwd=temporary, env=env,
                                input=b"".join(json.dumps(m).encode() + b"\n" for m in messages),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        deps.require(result.returncode == 0, f"namespace bootstrap v{version} failed: {result.stderr.decode(errors='replace')}")
        frames = [json.loads(line) for line in result.stdout.splitlines()]
        controls = [frame for frame in frames if frame["type"] != "log"]
        deps.require([frame["type"] for frame in controls] == ["ready", "result", "result", "closing"],
                     f"namespace bootstrap v{version} control frames differ: {controls}")
        outputs = []
        for number, frame in enumerate(controls[1:3], 1):
            deps.require(frame["status"] == "success", f"namespace invocation v{version} failed: {frame}")
            output = frame["output"]
            if version == 3 and number == 1:
                deps.require(output["kind"] == "complete", "workflow completion was not preserved")
                output = output["output"]
            deps.require(output["event_id"] == f"evt-{number}" and output["attempt_id"] == f"attempt-{number}"
                         and output["client"] == "AsyncClient", "namespace invocation context mismatch")
            outputs.append(output)
        deps.require(outputs[0]["pid"] == outputs[1]["pid"] == controls[0]["pid"], "runtime session was not reused")
        if version >= 2:
            logs = [frame for frame in frames if frame["type"] == "log"]
            # Log delivery is best effort: shutdown can discard pending logs.
            # Check any delivered snapshots without depending on writer scheduling.
            identities = [frame["invocation"]["attempt_id"] for frame in logs]
            deps.require(identities in ([], ["attempt-1"], ["attempt-2"], ["attempt-1", "attempt-2"]),
                         f"namespace structured logging identity/order differs: {logs}")
            for frame in logs:
                expected_event = "evt-" + frame["invocation"]["attempt_id"].removeprefix("attempt-")
                deps.require(frame["invocation"]["event_id"] == expected_event,
                             f"namespace structured logging context differs: {frame}")
        deps.require(not list(artifact.rglob("__pycache__")), "isolated namespace imports changed artifact")
        print(f"Verified vendored client and relocated helper under isolated protocol v{version}", flush=True)


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
        check_namespace_coexistence(runtime_python, wheelhouse, wheel, temporary, env)
        install(runtime_python, wheelhouse, "otel", temporary, env)
        check_environment(runtime_python, temporary, env, inventory, target, True)
        run([runtime_python, "-I", "-m", "unittest", "discover", "-s", tests, "-v"], temporary, env)
        evidence = {"target": target, "python": sys.version, "platform": sys.platform,
                    "sdist": {"filename": sdist.name, "sha256": deps.digest(sdist.read_bytes())},
                    "wheel": {"filename": wheel.name, "sha256": deps.digest(wheel.read_bytes())},
                    "checks": ["reviewed artifact hashes and legal bytes", "sdist legal contents", "wheel rebuilt from sdist",
                               "wheel legal contents", "native namespace and subpackage typing marker in wheel/sdist",
                               "separate installed client and relocated helper in both import orders",
                               "vendored client and relocated helper under isolated protocols 1/2/3",
                               "installed base graph and tests outside checkout", "installed optional OTel graph and tests outside checkout"],
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
