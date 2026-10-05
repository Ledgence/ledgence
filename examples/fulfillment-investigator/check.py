#!/usr/bin/env python3
"""Offline checks by default; --native opts into an isolated real deployment (MIT)."""
import argparse
import os
from pathlib import Path
import sys
import unittest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT / "sdk/python")]
sys.dont_write_bytecode = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", action="store_true", help="run real PostgreSQL, workers, and client recovery checks")
    parser.add_argument("--directory", type=Path, help="fresh native evidence directory")
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--psql", default="psql")
    args = parser.parse_args()
    if args.native:
        if args.directory is None or not os.environ.get("LEDGENCE_POSTGRES_URL"):
            parser.error("--native requires --directory and LEDGENCE_POSTGRES_URL (server allowing CREATE DATABASE)")
        from native_check import run
        run(args)
        return 0
    suite = unittest.defaultTestLoader.discover(str(HERE / "tests"))
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
