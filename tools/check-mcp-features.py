"""Verify MCP remains optional and independent from telemetry and queue SDKs."""
import os
from pathlib import Path
import re
import subprocess


def main():
    root = Path(__file__).resolve().parents[1]
    cargo = os.environ.get("CARGO", "cargo")
    selections = [
        ("ledgence-cli", ["--no-default-features"], False),
        ("ledgence-cli", [], True),
        ("ledgence-cli", ["--no-default-features", "--features", "mcp"], True),
        ("ledgence-worker", [], False),
        ("ledgence-orchestrator", [], False),
        ("ledgence-worker-api", [], False),
        ("ledgence-orchestration-api", [], False),
    ]
    for package, features, enabled in selections:
        tree = subprocess.check_output([
            cargo, "tree", "--locked", "-p", package, *features,
            "--target", "all", "--edges", "normal,build", "--prefix", "none", "--format", "{p}",
        ], cwd=root, text=True)
        packages = set(re.findall(r"^([A-Za-z0-9_-]+) v", tree, re.MULTILINE))
        assert ("rmcp" in packages) == enabled, (package, features, "MCP feature isolation")
        if features == ["--no-default-features", "--features", "mcp"]:
            assert not any(name.startswith(("opentelemetry", "aws-sdk-", "aws-smithy-"))
                           or name == "tracing-opentelemetry" for name in packages), "MCP requires an unrelated adapter"
    for features in (["--no-default-features"], ["--no-default-features", "--features", "mcp"]):
        subprocess.run([cargo, "clippy", "--locked", "-p", "ledgence-cli", *features,
                        "--all-targets", "--", "-D", "warnings"], cwd=root, check=True)
    print("MCP feature isolation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
