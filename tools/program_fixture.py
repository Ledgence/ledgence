"""Prepare host-only packages for repository acceptance and Compose fixtures.

This is test/deployment tooling, not a cross-platform application builder. The
caller supplies already-prepared source and explicitly selects its interpreter.
No dependency resolution, source discovery or application code execution is performed.
"""
import json
from pathlib import Path
import subprocess


_PROBE = """import json, platform, sys
print(json.dumps({"implementation": sys.implementation.name,
                  "version": list(sys.version_info[:2]),
                  "os": platform.system(), "arch": platform.machine()}))
"""


def host_manifest(name, version, *, python, protocol=2, handler="program:handle"):
    """Describe the selected host interpreter; reject unsupported fixture hosts."""
    if protocol not in (2, 3):
        raise ValueError("fixture protocol must be 2 or 3")
    output = subprocess.check_output(
        [str(python), "-I", "-S", "-c", _PROBE], text=True, timeout=5,
    )
    info = json.loads(output)
    if info["implementation"] != "cpython" or tuple(info["version"]) < (3, 11):
        raise ValueError("fixture programs require CPython 3.11 or newer")
    os_name = {"Darwin": "macos", "Linux": "linux"}.get(info["os"])
    architecture = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(info["arch"])
    if os_name is None or architecture is None:
        raise ValueError("unsupported fixture host platform")
    return {
        "schema_version": 1,
        "program": {"id": name, "version": version},
        "runtime": {"kind": "python", "python": ".".join(map(str, info["version"])), "protocol": protocol},
        "handler": handler,
        "platform": {"os": os_name, "arch": architecture},
    }


def prepare_package(directory, name, version, source, *, python, protocol=2, handler="program:handle"):
    """Write a fresh prepared fixture, never merging with an existing directory."""
    manifest = host_manifest(name, version, python=python, protocol=protocol, handler=handler)
    directory = Path(directory)
    directory.mkdir(parents=True)
    # Keep fixture manifest encoding stable for immutable published coordinates.
    (directory / "ledgence-program.json").write_text(json.dumps(manifest), encoding="utf-8")
    (directory / "program.py").write_text(source, encoding="utf-8")
    return directory
