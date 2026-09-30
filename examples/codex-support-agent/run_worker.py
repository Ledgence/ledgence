#!/usr/bin/env python3
"""Start the demo worker with an operator-installed, authenticated Codex CLI."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]


def worker_environment(codex_bin: Path | None, inherited=None) -> dict[str, str]:
    environment = dict(os.environ if inherited is None else inherited)
    configured = codex_bin or environment.get("LEDGENCE_CODEX_BIN")
    if not configured:
        raise ValueError("set --codex-bin or LEDGENCE_CODEX_BIN to an absolute executable path")
    executable = Path(configured)
    if not executable.is_absolute() or not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError("Codex CLI must be an existing executable at an absolute path")
    # The CLI reads its own existing ChatGPT login. This launcher never reads
    # credential files or forwards API keys as an alternate authentication route.
    for name in tuple(environment):
        if name.startswith(("OPENAI_", "GOOGLE_", "GEMINI_")):
            environment.pop(name)
    environment["LEDGENCE_CODEX_BIN"] = str(executable.resolve())
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def command(args) -> list[str]:
    return [
        str(args.binaries.resolve() / "ledgence"), "worker", "connect",
        "--server", args.server, "--tenant", args.tenant, "--namespace", args.namespace,
        "--queue", args.queue, "--store", str(args.directory.resolve() / "store"),
        "--cache", str(args.directory.resolve() / "cache"), "--python", sys.executable,
        "--runner", str(ROOT / "sdk/python/ledgence/worker/bootstrap.py"), "--concurrency", "1",
    ]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True, help="output from prepare.py")
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--codex-bin", type=Path, help="absolute path to a Codex CLI authenticated with ChatGPT")
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--queue", default="codex-support-demo")
    args = parser.parse_args(argv)
    try:
        environment = worker_environment(args.codex_bin)
        if sys.version_info[:2] != (3, 13):
            raise ValueError("run this demo with CPython 3.13, matching the prepared packages")
        if not (args.directory / "prepared.json").is_file():
            raise ValueError("run prepare.py first")
        worker = command(args)
        os.execve(worker[0], worker, environment)
    except (OSError, ValueError) as error:
        # Do not print arbitrary file contents, environment values or OS exception text.
        print(f"Worker startup failed ({type(error).__name__}); check the documented interpreter, paths and Codex login.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
