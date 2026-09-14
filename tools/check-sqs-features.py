"""Verify that AWS SDKs stay optional and SQS works without OpenTelemetry."""

import os
from pathlib import Path
import re
import subprocess


def main():
    root = Path(__file__).resolve().parents[1]
    cargo = os.environ.get("CARGO", "cargo")
    for package in ("ledgence-worker", "ledgence-orchestrator"):
        for defaults in ([], ["--no-default-features"]):
            selection = ["-p", package, *defaults, "--locked"]
            tree = subprocess.check_output(
                [cargo, "tree", *selection, "--target", "all", "--edges", "normal,build",
                 "--prefix", "none", "--format", "{p}"], cwd=root, text=True)
            packages = set(re.findall(r"^([A-Za-z0-9_-]+) v", tree, re.MULTILINE))
            unexpected = {name for name in packages
                          if name.startswith(("aws-sdk-", "aws-smithy-"))
                          or name in {"aws-config", "aws-runtime", "aws-types",
                                      "aws-credential-types", "aws-sigv4", "ledgence-adapter-sqs"}}
            if unexpected:
                raise RuntimeError(f"{package}: SQS leaked into a disabled build: {sorted(unexpected)}")
        selection = ["-p", package, "--no-default-features", "--features", "sqs", "--locked"]
        tree = subprocess.check_output(
            [cargo, "tree", *selection, "--target", "all", "--edges", "normal,build",
             "--prefix", "none", "--format", "{p}"], cwd=root, text=True)
        packages = set(re.findall(r"^([A-Za-z0-9_-]+) v", tree, re.MULTILINE))
        if "aws-sdk-sqs" not in packages or "ledgence-adapter-sqs" not in packages:
            raise RuntimeError(f"{package}: enabled SQS adapter missing")
        if any(name.startswith("opentelemetry") or name == "tracing-opentelemetry"
               for name in packages):
            raise RuntimeError(f"{package}: SQS requires OpenTelemetry")
        subprocess.run([cargo, "clippy", *selection, "--all-targets", "--", "-D", "warnings"],
                       cwd=root, check=True)
        print(f"Optional SQS feature isolation passed: {package}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
