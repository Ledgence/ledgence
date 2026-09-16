#!/usr/bin/env python3
"""Check the explicitly reviewed Python graphs and actual distribution bytes.

No resolver, package imports, or network are needed for the default policy check.
--download verifies pinned published artifacts, and never builds dependency sources.
The small requirement evaluator deliberately rejects syntax outside this inventory.
"""
from __future__ import annotations

import argparse
import ast
import copy
import email
import hashlib
import json
import platform
import re
import sys
import tarfile
import tomllib
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "sdk/python-client"
LEGAL = CLIENT / "third_party"
APPROVED = {
    "aiohappyeyeballs": ("2.7.1", "PSF-2.0"),
    "aiohttp": ("3.14.3", "Apache-2.0 AND MIT"),
    "aiosignal": ("1.4.0", "Apache-2.0"),
    "attrs": ("26.1.0", "MIT"),
    "flit-core": ("3.12.0", "BSD-3-Clause AND MIT"),
    "frozenlist": ("1.8.0", "Apache-2.0"),
    "idna": ("3.19", "BSD-3-Clause"),
    "multidict": ("6.8.0", "Apache-2.0"),
    "opentelemetry-api": ("1.44.0", "Apache-2.0"),
    "propcache": ("0.5.2", "Apache-2.0"),
    "typing-extensions": ("4.16.0", "PSF-2.0"),
    "yarl": ("1.24.5", "Apache-2.0"),
}
GROUPS = {"runtime": ["aiohttp"], "build": ["flit-core"], "test": [], "otel": ["opentelemetry-api"]}
TARGETS = [f"{os}-cp{v}" for os in ("linux-x86_64", "macos-arm64") for v in ("311", "312", "313", "314")]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def version(value):
    require(bool(re.fullmatch(r"\d+(?:\.\d+)*", value)), f"unreviewed version syntax: {value}")
    values = [int(x) for x in value.split(".")]
    return tuple(values + [0] * (4 - len(values)))


def satisfies(selected, specification):
    for item in specification.strip().strip("()").split(","):
        if not item.strip():
            continue
        match = re.fullmatch(r"\s*(>=|<=|==|!=|>|<)\s*(\d+(?:\.\d+)*)\s*", item)
        require(match is not None, f"unreviewed requirement syntax: {item}")
        op, other = match.groups()
        left, right = version(selected), version(other)
        if not {">=": left >= right, "<=": left <= right, "==": left == right,
                "!=": left != right, ">": left > right, "<": left < right}[op]:
            return False
    return True


def marker_applies(marker, target):
    environment = {"python_version": "3." + target.rsplit("cp", 1)[1][1:],
                   "sys_platform": "darwin" if target.startswith("macos") else "linux",
                   "platform_python_implementation": "CPython", "extra": ""}

    def evaluate(node):
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and node.id in environment:
            return environment[node.id]
        if isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
            values = [evaluate(n) for n in node.values]
            return all(values) if isinstance(node.op, ast.And) else any(values)
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left, right = evaluate(node.left), evaluate(node.comparators[0])
            if isinstance(node.left, ast.Name) and node.left.id == "python_version":
                left, right = version(left), version(right)
            op = node.ops[0]
            if isinstance(op, ast.Eq): return left == right
            if isinstance(op, ast.NotEq): return left != right
            if isinstance(op, ast.Lt): return left < right
            if isinstance(op, ast.LtE): return left <= right
            if isinstance(op, ast.Gt): return left > right
            if isinstance(op, ast.GtE): return left >= right
        raise ValueError(f"unreviewed marker syntax: {marker}")

    return bool(evaluate(ast.parse(marker.strip(), mode="eval")))


def artifact_for(package, target):
    matches = [a for a in package["artifacts"] if target in a["targets"]]
    require(len(matches) == 1, f"expected exactly one wheel for {package['name']} on {target}")
    return matches[0]


def closure(inventory, group, target):
    packages = {p["name"]: p for p in inventory["packages"]}
    pending, seen = list(inventory["groups"][group]), set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        require(name in packages, f"unreviewed dependency: {name}")
        seen.add(name)
        wheel = artifact_for(packages[name], target)
        py = "3." + target.rsplit("cp", 1)[1][1:]
        require(satisfies(py, wheel["requires_python"]), f"unsupported Python: {name} {target}")
        for requirement in wheel["requires_dist"]:
            spec, _, marker = requirement.partition(";")
            if marker and not marker_applies(marker, target):
                continue
            match = re.fullmatch(r"([a-zA-Z0-9_.-]+)\s*(.*)", spec.strip())
            require(match is not None, f"unreviewed requirement: {requirement}")
            dependency, bounds = canonical(match[1]), match[2]
            require(dependency in packages, f"unreviewed active dependency: {requirement}")
            require(satisfies(packages[dependency]["version"], bounds), f"pin violates {requirement}")
            pending.append(dependency)
    return seen


def lock_text(inventory, group):
    selections = {t: closure(inventory, group, t) for t in inventory["targets"]}
    names = set().union(*selections.values())
    packages = {p["name"]: p for p in inventory["packages"]}
    lines = ["# Generated from the reviewed inventory; changes require dependency review.",
             "# Use --require-hashes --only-binary=:all: with the reviewed target wheelhouse."]
    if not names:
        lines.append("# The client test suite uses only stdlib unittest plus the separately locked runtime.")
    for name in sorted(names):
        p = packages[name]
        targets = {t for t, selected in selections.items() if name in selected}
        marker = ""
        if targets != set(TARGETS):
            require(targets == {t for t in TARGETS if t.endswith(("cp311", "cp312"))}, "unreviewed lock marker")
            marker = '; python_version < "3.13"'
        hashes = sorted({a["sha256"] for a in p["artifacts"] if targets.intersection(a["targets"])})
        lines.append(f"{name}=={p['version']}{marker} " + " ".join(f"--hash=sha256:{h}" for h in hashes))
    return "\n".join(lines) + "\n"


def legal_path(record):
    path = (LEGAL / record["path"]).resolve()
    require(path.is_relative_to(LEGAL.resolve()), "legal file escapes third_party")
    return path


def check_inventory(inventory, check_locks=True):
    require(inventory["schema_version"] == 1, "unknown inventory schema")
    require(inventory["targets"] == TARGETS, "target matrix changed without review")
    require(inventory["groups"] == GROUPS, "dependency roots changed without review")
    packages = inventory["packages"]
    require(len(packages) == len(APPROVED), "unreviewed package count")
    require({p["name"]: (p["version"], p["license_expression"]) for p in packages} == APPROVED,
            "package/version/license selection changed without review")
    for p in packages:
        require(p["artifacts"] and p["source"], f"missing published evidence: {p['name']}")
        for artifact in [*p["artifacts"], p["source"]]:
            require(re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]), "invalid artifact hash")
            require(artifact["url"].startswith("https://files.pythonhosted.org/packages/"), "unapproved artifact source")
            require(artifact["url"].rsplit("/", 1)[-1] == artifact["filename"], "artifact filename/URL mismatch")
            require(artifact["license_files"], f"missing legal evidence: {artifact['filename']}")
            for record in artifact["license_files"].values():
                require(digest(legal_path(record).read_bytes()) == record["sha256"], f"legal material changed: {record['path']}")
        for artifact in p["artifacts"]:
            require(artifact["targets"] and set(artifact["targets"]).issubset(TARGETS), "wheel has unreviewed targets")
        for target in TARGETS:
            artifact_for(p, target)
    aiohttp = next(p for p in packages if p["name"] == "aiohttp")
    require(aiohttp["embedded_components"] == [{"name": "llhttp", "license_expression": "MIT", "source_path": "vendor/llhttp/LICENSE"}], "llhttp review missing")
    require(all(any("vendor/llhttp/LICENSE" in path for path in a["license_files"]) for a in aiohttp["artifacts"]), "llhttp wheel license missing")
    for group in GROUPS:
        expected = lock_text(inventory, group)
        if check_locks:
            require((LEGAL / f"{group}-requirements.txt").read_text() == expected, f"{group} lock drift")
    return inventory


def project_requirements(group):
    inventory = json.loads((LEGAL / "inventory.json").read_text())
    return [line.split(" --hash=", 1)[0] for line in lock_text(inventory, group).splitlines() if line and not line.startswith("#")]


def check_pyproject():
    project = tomllib.loads((CLIENT / "pyproject.toml").read_text())
    require(project["project"]["name"] == "ledgence-client", "unexpected distribution identity")
    require(project["project"]["requires-python"] == ">=3.11", "Python support changed without review")
    require(project["project"]["dependencies"] == project_requirements("runtime"), "runtime requirements drift")
    require(project["project"].get("optional-dependencies") == {"otel": project_requirements("otel")}, "optional requirements drift")
    require(project["build-system"] == {"requires": ["flit_core==3.12.0"], "build-backend": "flit_core.buildapi"}, "build graph drift")
    require(project["tool"]["flit"]["module"]["name"] == "ledgence.client", "public module identity drift")


def is_legal(name):
    return any(word in name.lower().rsplit("/", 1)[-1] for word in ("license", "notice", "copying"))


def verify_artifact(path, artifact, package):
    require(digest(path.read_bytes()) == artifact["sha256"], f"artifact hash mismatch: {path.name}")
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            metadata_names = [n for n in names if n.endswith(".dist-info/METADATA") and n.count("/") == 1]
            require(len(metadata_names) == 1, f"ambiguous wheel metadata: {path.name}")
            metadata = email.message_from_bytes(archive.read(metadata_names[0]))
            require(canonical(metadata["Name"]) == package["name"] and metadata["Version"] == package["version"], "wheel identity mismatch")
            require(metadata.get_all("Requires-Dist", []) == artifact["requires_dist"], "wheel graph differs from review")
            require(metadata.get("Requires-Python") == artifact["requires_python"], "wheel interpreter support drift")
            require(metadata.get("License-Expression") == artifact["license_expression"] and metadata.get("License") == artifact["license"], "wheel license metadata drift")
            legal = {n: archive.read(n) for n in names if not n.endswith("/") and is_legal(n)}
            require([n for n in names if n.endswith((".so", ".dll", ".dylib", ".pyd"))] == artifact["native_files"], "native artifact inventory drift")
    else:
        with tarfile.open(path) as archive:
            legal = {m.name: archive.extractfile(m).read() for m in archive if m.isfile() and is_legal(m.name)}
    require(set(legal) == set(artifact["license_files"]), f"incomplete legal inventory: {path.name}")
    for name, data in legal.items():
        record = artifact["license_files"][name]
        require(digest(data) == record["sha256"] and data == legal_path(record).read_bytes(), f"legal bytes differ: {path.name}/{name}")


def current_target():
    machine = platform.machine().lower()
    prefix = {("linux", "x86_64"): "linux-x86_64", ("darwin", "arm64"): "macos-arm64"}.get((sys.platform, machine))
    target = f"{prefix}-cp{sys.version_info.major}{sys.version_info.minor}"
    require(target in TARGETS, f"no reviewed wheel target for {sys.platform}/{machine}/Python {sys.version_info[:2]}")
    return target


def download(inventory, directory, targets, groups, sources=False, offline=False):
    directory.mkdir(parents=True, exist_ok=True)
    selected = set().union(*(closure(inventory, g, t) for g in groups for t in targets))
    count = 0
    for package in inventory["packages"]:
        if package["name"] not in selected:
            continue
        artifacts = [a for a in package["artifacts"] if set(targets).intersection(a["targets"])]
        if sources:
            artifacts.append(package["source"])
        for artifact in artifacts:
            path = directory / artifact["filename"]
            if not path.exists():
                require(not offline, f"missing offline artifact: {path}")
                with urllib.request.urlopen(artifact["url"], timeout=60) as response:
                    data = response.read(32 * 1024 * 1024 + 1)
                require(digest(data) == artifact["sha256"], f"download hash mismatch: {path.name}")
                path.write_bytes(data)
            verify_artifact(path, artifact, package)
            count += 1
    return count


def self_test(inventory):
    def reject(label, change):
        modified = copy.deepcopy(inventory)
        change(modified)
        try:
            check_inventory(modified)
        except (ValueError, OSError):
            return
        raise ValueError(f"policy mutation was accepted: {label}")

    reject("unreviewed version", lambda i: i["packages"][0].update(version="999.0"))
    reject("source-obligation license", lambda i: i["packages"][0].update(license_expression="MPL-2.0"))
    reject("new runtime dependency", lambda i: i["packages"][0]["artifacts"][0]["requires_dist"].append("certifi>=1"))
    reject("changed dependency constraint", lambda i: i["packages"][0]["artifacts"][0]["requires_dist"].append("attrs>=999"))
    reject("missing platform wheel", lambda i: i["packages"][0]["artifacts"].clear())
    reject("missing full legal file", lambda i: next(iter(i["packages"][0]["artifacts"][0]["license_files"].values())).update(sha256="0" * 64))
    reject("unreviewed optional root", lambda i: i["groups"]["otel"].append("opentelemetry-sdk"))
    require("typing-extensions" in closure(inventory, "runtime", "linux-x86_64-cp312"), "Python 3.12 marker regression")
    require("typing-extensions" not in closure(inventory, "runtime", "linux-x86_64-cp313"), "Python 3.13 marker regression")
    print("Python dependency policy mutation checks passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download", type=Path, help="download and verify reviewed artifacts into this wheelhouse")
    parser.add_argument("--target", default="current", choices=["current", "all", *TARGETS])
    parser.add_argument("--group", action="append", choices=list(GROUPS))
    parser.add_argument("--sources", action="store_true", help="also check source archives; they are never built")
    parser.add_argument("--offline", action="store_true", help="verify only already downloaded artifacts")
    args = parser.parse_args()
    inventory = check_inventory(json.loads((LEGAL / "inventory.json").read_text()))
    check_pyproject()
    self_test(inventory)
    if args.download:
        targets = TARGETS if args.target == "all" else [current_target() if args.target == "current" else args.target]
        count = download(inventory, args.download.resolve(), targets, args.group or list(GROUPS), args.sources, args.offline)
        print(f"Verified {count} pinned artifacts for {', '.join(targets)}")
    print("Python client dependency policy passed: runtime/build/test/optional OTel graphs and retained legal files")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(f"Python dependency policy failed: {error}")
