#!/usr/bin/env python3
"""Install hash-pinned optional SDK fixtures and exercise the selected helper.

SDKs exist only in disposable qualification environments. They are not added to
Ledgence distributions. Tests deny provider/network/process calls; setup fetches
reviewed packages from PyPI. Claude is built from its pinned Python-only source.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
import urllib.request

from framework_integrations.source_wheel import validate as validate_source_wheel

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tools/framework_integrations"


def canonical(name): return re.sub(r"[-_.]+", "-", name).lower()
def python_at(directory): return directory / "bin/python"


def run(command, cwd, environment, label, *, timeout=600):
    with (cwd / (label + ".stdout")).open("wb") as out, (cwd / (label + ".stderr")).open("wb") as err:
        process = subprocess.run(list(map(str, command)), cwd=cwd, env=environment,
                                 stdout=out, stderr=err, timeout=timeout)
    if process.returncode:
        raise RuntimeError(f"{label} failed; inspect retained setup/test logs")


def install(python, requirements, cwd, environment, label):
    run([python, "-I", "-m", "pip", "install", "--disable-pip-version-check", "--no-input",
         "--require-hashes", "--only-binary=:all:", "--no-deps", "--no-compile", "-r", requirements],
        cwd, environment, label)


def source_only_claude(python, cwd, environment):
    source = json.loads((FIXTURES / "claude-source.json").read_text())
    archive_path = cwd / source["filename"]
    with urllib.request.urlopen(source["url"], timeout=60) as response:
        data = response.read(source["size"] + 1)
    if len(data) != source["size"] or hashlib.sha256(data).hexdigest() != source["sha256"]:
        raise ValueError("Claude source archive differs from reviewed bytes")
    archive_path.write_bytes(data)
    extracted = cwd / "source"; extracted.mkdir()
    original = {}
    expanded = 0
    seen = set()
    with tarfile.open(archive_path) as archive:
        for member in archive:
            if member.name in seen or len(seen) >= 4096:
                raise ValueError("duplicate or excessive source archive entries")
            seen.add(member.name)
            expanded += member.size
            if member.size > 8 * 1024 * 1024 or expanded > 32 * 1024 * 1024:
                raise ValueError("oversized source archive")
            path = extracted / member.name
            if not path.resolve().is_relative_to(extracted.resolve()) or not (member.isdir() or member.isfile()):
                raise ValueError("unsupported source archive entry")
            if member.isfile(): original[member.name] = archive.extractfile(member).read()
        archive.extractall(extracted, filter="data")
    source_root = extracted / ("claude_agent_sdk-" + source["version"])
    config = tomllib.loads((source_root / "pyproject.toml").read_text())
    if config["build-system"] != {"requires": ["hatchling"], "build-backend": "hatchling.build"} or "hooks" in config["tool"]["hatch"]["build"]:
        raise ValueError("source build contract differs")
    build_env = cwd / "build-env"
    run([python, "-I", "-m", "venv", build_env], cwd, environment, "build-venv")
    install(python_at(build_env), FIXTURES / "requirements/claude_build.txt", cwd, environment, "build-dependencies")
    output = cwd / "source-wheel"; output.mkdir()
    script = '''import sys,os
attempts=[]
def audit(event,args):
    if event.startswith(('socket.','subprocess.')) or event in ('os.system','os.fork','os.forkpty','os.posix_spawn','os.exec','os.spawn'):
        attempts.append(event)
        raise RuntimeError('source-only build forbids network/process access')
sys.addaudithook(audit)
os.chdir(sys.argv[1])
import hatchling.build
print(hatchling.build.build_wheel(sys.argv[2]))
assert not attempts
'''
    run([python_at(build_env), "-I", "-B", "-c", script, source_root, output], cwd, environment, "source-wheel")
    actual = {str(path.relative_to(extracted)): path.read_bytes() for path in source_root.rglob("*") if path.is_file()}
    if actual != original: raise ValueError("build modified source inputs")
    wheels = list(output.glob("*.whl"))
    if len(wheels) != 1: raise ValueError("unexpected source build outputs")
    validate_source_wheel(wheels[0], original, source["version"])
    return wheels[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--framework", required=True, choices=("pydantic_ai", "google_adk", "claude_agent_sdk"))
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--helper-root", type=Path, default=ROOT / "sdk/python",
                        help="directory containing the selected ledgence/worker helper (candidate runtime for release qualification)")
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    if not args.python.is_file():
        selected = shutil.which(str(args.python))
        if selected is None: parser.error("--python must name an available interpreter")
        args.python = Path(selected)
    args.evidence.mkdir(parents=True, exist_ok=False)
    directory = args.evidence.resolve()
    environment = {key: value for key, value in os.environ.items()
                   if key in ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT")}
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1", PIP_CONFIG_FILE=os.devnull,
                       PIP_INDEX_URL="https://pypi.org/simple")
    def helper_hashes():
        return {str(path.relative_to(args.helper_root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((args.helper_root / "ledgence/worker").rglob("*.py"))}
    helper_before = helper_hashes()
    status = {"framework": args.framework, "passed": False, "provider_calls": "not_exercised", "native_cli": "not_exercised"}
    try:
        runtime = directory / "runtime-env"
        run([args.python.resolve(), "-I", "-m", "venv", runtime], directory, environment, "runtime-venv")
        python = python_at(runtime)
        install(python, FIXTURES / "requirements" / (args.framework + ".txt"), directory, environment, "runtime-dependencies")
        if args.framework == "claude_agent_sdk":
            wheel = source_only_claude(args.python.resolve(), directory, environment)
            run([python, "-I", "-m", "pip", "install", "--no-index", "--no-deps", "--no-compile", wheel],
                directory, environment, "source-install")
        run([python, "-I", "-m", "pip", "check", "--disable-pip-version-check"], directory, environment, "dependency-check")
        reviewed = json.loads((FIXTURES / "reviewed-dependencies.json").read_text())["profiles"][args.framework]
        expected = {canonical(item["name"]): item["version"] for item in reviewed["packages"]}
        if args.framework == "claude_agent_sdk": expected["claude-agent-sdk"] = "0.2.180"
        run([python, "-I", "-B", FIXTURES / "inventory.py"], directory, environment, "installed-inventory")
        inventory_bytes = (directory / "installed-inventory.stdout").read_bytes()
        inventory = json.loads(inventory_bytes)
        if inventory["packages"] != expected:
            raise ValueError("installed SDK inventory differs from reviewed versions")
        status["installed_inventory_sha256"] = hashlib.sha256(inventory_bytes).hexdigest()
        run([python, "-I", "-B", FIXTURES / "run_tests.py", args.helper_root.resolve(), ROOT / "sdk/python/tests",
             args.framework, directory / "sdk-tests.json"], directory, environment, "sdk-tests")
        status.update(json.loads((directory / "sdk-tests.json").read_text()))
        run([python, "-I", "-B", FIXTURES / "inventory.py"], directory, environment, "installed-inventory-after")
        inventory_after = (directory / "installed-inventory-after.stdout").read_bytes()
        status["installed_inventory_sha256_after"] = hashlib.sha256(inventory_after).hexdigest()
        if inventory_after != inventory_bytes or helper_hashes() != helper_before:
            raise ValueError("SDK or helper bytes changed during qualification")
        status["installed_distributions"] = len(expected)
    except Exception as error:
        status["passed"] = False
        status["failure_type"] = type(error).__name__
        status["failed_steps"] = [path.stem for path in directory.glob("*.stderr") if path.stat().st_size]
    status["helper_sha256"] = helper_before
    status["helper_sha256_after"] = helper_hashes()
    status["source_sha"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    status["source_dirty"] = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT))
    status["python"] = subprocess.check_output([args.python, "-I", "-c", "import platform;print(platform.python_version())"], text=True).strip()
    status["requirements_sha256"] = hashlib.sha256((FIXTURES / "requirements" / (args.framework + ".txt")).read_bytes()).hexdigest()
    (directory / "summary.json").write_text(json.dumps(status, indent=2) + "\n")
    print(json.dumps(status))
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
