"""Keep S3 execution artifacts optional and independent of queue/telemetry adapters."""
import os
from pathlib import Path
import re
import subprocess


def main():
    root = Path(__file__).resolve().parents[1]
    cargo = os.environ.get("CARGO", "cargo")
    selections = [
        ("ledgence-adapter-blob", [], False),
        ("ledgence-orchestrator", [], False),
        ("ledgence-cli", [], False),
        ("ledgence-adapter-blob", ["--features", "s3"], True),
        ("ledgence-orchestrator", ["--features", "s3-artifacts"], True),
        ("ledgence-cli", ["--features", "s3-artifacts"], True),
    ]
    for package, features, enabled in selections:
        command = [cargo, "tree", "--locked", "-p", package, "--no-default-features", *features,
                   "--target", "all", "--edges", "normal,build", "--prefix", "none", "--format", "{p}"]
        tree = subprocess.check_output(command, cwd=root, text=True)
        packages = set(re.findall(r"^([A-Za-z0-9_-]+) v", tree, re.MULTILINE))
        assert ("aws-sdk-s3" in packages) == enabled, (package, "S3 feature isolation")
        assert not ({"aws-sdk-sqs", "ledgence-adapter-sqs"} & packages), (package, "S3 coupled to queue adapter")
        assert not any(name.startswith("opentelemetry") or name == "tracing-opentelemetry" for name in packages), (package, "S3 coupled to telemetry")
        if not enabled:
            assert not any(name.startswith(("aws-sdk-", "aws-smithy-")) or name == "aws-config" for name in packages), (package, "AWS SDK leaked into filesystem-only build")
    subprocess.run([cargo, "clippy", "--locked", "-p", "ledgence-cli", "--no-default-features",
                    "--features", "s3-artifacts", "--all-targets", "--", "-D", "warnings"], cwd=root, check=True)
    print("Optional artifact S3 feature isolation passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
