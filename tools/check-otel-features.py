"""Prove executable builds can exclude every OpenTelemetry dependency."""
import os
from pathlib import Path
import re
import subprocess


def main():
    root = Path(__file__).resolve().parents[1]
    cargo = os.environ.get("CARGO", "cargo")
    for package in ("ledgence-worker", "ledgence-orchestrator", "ledgence-cli"):
        selection = ["-p", package, "--no-default-features", "--locked"]
        tree = subprocess.check_output([cargo, "tree", *selection, "--target", "all",
                                        "--edges", "normal,build", "--prefix", "none", "--format", "{p}"],
                                       cwd=root, text=True)
        packages = set(re.findall(r"^([A-Za-z0-9_-]+) v", tree, re.MULTILINE))
        unexpected = {name for name in packages if name.startswith("opentelemetry")
                      or name in {"tracing-opentelemetry", "ledgence-adapter-otel"}}
        if unexpected:
            raise RuntimeError(f"{package}: optional adapter leaked into disabled build: {sorted(unexpected)}")
        subprocess.run([cargo, "clippy", *selection, "--all-targets", "--", "-D", "warnings"],
                       cwd=root, check=True)
        print(f"OpenTelemetry-free build passed: {package}", flush=True)


if __name__ == "__main__":
    main()
