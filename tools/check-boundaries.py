"""Check production crate dependencies preserve the worker's hexagonal boundaries."""

import json
import pathlib
import subprocess
import sys


def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    graph = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        cwd=root,
        text=True,
    ))
    allowed = {
        "ledgence-worker-api": set(),
        "ledgence-worker-core": {"ledgence-worker-api"},
        "ledgence-adapter-artifact": {"ledgence-worker-api"},
        "ledgence-adapter-subprocess": {"ledgence-worker-api"},
        "ledgence-worker": {
            "ledgence-worker-api", "ledgence-worker-core",
            "ledgence-adapter-artifact", "ledgence-adapter-subprocess",
        },
    }
    members = set(graph["workspace_members"])
    packages = {p["name"]: p for p in graph["packages"] if p["id"] in members}
    errors = []
    for name, package in packages.items():
        if name not in allowed:
            errors.append(f"{name}: declare its architectural boundary in this check")
            continue
        for dependency in package["dependencies"]:
            if dependency["kind"] == "dev":
                continue
            target = dependency["name"]
            if target in packages and target not in allowed[name]:
                errors.append(f"{name} must not depend on {target} ({dependency['kind'] or 'normal'})")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Architecture boundaries passed for {len(packages)} workspace crates.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
