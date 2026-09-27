#!/usr/bin/env python3
"""Start the demo worker with credentials supplied only through its environment."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]


def worker_environment(env_file: Path | None, inherited=None) -> dict[str, str]:
    environment = dict(os.environ if inherited is None else inherited)
    if env_file is not None:
        if env_file.stat().st_size > 16 * 1024:
            raise ValueError("credential file exceeds 16 KiB")
        values = {}
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name, separator, value = line.partition("=")
            name, value = name.strip(), value.strip()
            if not separator or name not in {"GOOGLE_API_KEY", "GOOGLE_GENAI_USE_VERTEXAI"} or name in values:
                raise ValueError("credential file must contain unique supported variable assignments")
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            values[name] = value
        if not values.get("GOOGLE_API_KEY"):
            raise ValueError("credential file has no GOOGLE_API_KEY")
        environment.update(values)
    key = environment.get("GOOGLE_API_KEY", "")
    if not key or not all(33 <= ord(character) <= 126 for character in key):
        raise ValueError("configure a nonempty GOOGLE_API_KEY without whitespace")
    if environment.get("GOOGLE_GENAI_USE_VERTEXAI", "false").lower() not in {"false", "0"}:
        raise ValueError("this demo uses the Gemini Developer API, not Vertex AI")
    environment["GOOGLE_GENAI_USE_VERTEXAI"] = "FALSE"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def command(args) -> list[str]:
    return [
        str(args.binaries.resolve() / "ledgence-worker"), "connect",
        "--server", args.server, "--tenant", args.tenant, "--namespace", args.namespace,
        "--queue", args.queue, "--store", str(args.directory.resolve() / "store"),
        "--cache", str(args.directory.resolve() / "cache"), "--python", sys.executable,
        "--runner", str(ROOT / "sdk/python/ledgence/worker/bootstrap.py"), "--concurrency", "1",
    ]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True, help="output from prepare.py")
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--env-file", type=Path, help="private credential file outside the repository")
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--queue", default="support-demo")
    args = parser.parse_args(argv)
    try:
        environment = worker_environment(args.env_file)
        if sys.version_info[:2] != (3, 13):
            raise ValueError("run this demo with CPython 3.13, matching the prepared packages")
        if not (args.directory / "prepared.json").is_file():
            raise ValueError("run prepare.py first")
        worker = command(args)
        os.execve(worker[0], worker, environment)
    except (OSError, ValueError) as error:
        # Do not print arbitrary file contents, environment values or OS exception text.
        print(f"Worker startup failed ({type(error).__name__}); check the documented interpreter, paths and credential configuration.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
