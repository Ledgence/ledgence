"""Prepare the agent recovery example for the current Python host (MIT)."""
import argparse
import json
from pathlib import Path
import platform
import shutil
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path, help="new package directory; must not already exist")
    args = parser.parse_args()
    if sys.version_info < (3, 11):
        parser.error("Python 3.11 or newer is required")
    os_name = {"Darwin": "macos", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(platform.machine())
    if os_name is None or architecture is None:
        parser.error("unsupported host platform")
    try:
        args.output.mkdir(parents=True)
    except FileExistsError:
        parser.error("output already exists; choose a new directory")
    manifest = {"schema_version": 1, "program": {"id": "agent-recovery", "version": "1.0.0"},
                "runtime": {"kind": "python", "python": f"{sys.version_info.major}.{sys.version_info.minor}",
                            "protocol": 3}, "handler": "program:handle",
                "platform": {"os": os_name, "arch": architecture}}
    (args.output / "ledgence-program.json").write_text(json.dumps(manifest, indent=2) + "\n")
    shutil.copyfile(Path(__file__).with_name("program.py"), args.output / "program.py")


if __name__ == "__main__": main()
