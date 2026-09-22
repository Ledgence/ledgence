#!/usr/bin/env python3
"""Verify and smoke-test an extracted copy of an actual candidate archive."""
import argparse
import hashlib
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile


def checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_files(directory):
    manifest = directory / "SHA256SUMS"
    expected = {}
    for line in manifest.read_text().splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  (.+)", line)
        if not match:
            raise ValueError("invalid checksum record")
        digest, name = match.groups()
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or name in expected:
            raise ValueError("invalid or duplicate checksum path")
        expected[name] = digest
    actual = {str(path.relative_to(directory)) for path in directory.rglob("*")
              if path.is_file() and path != manifest}
    if actual != set(expected):
        raise ValueError("candidate file inventory does not match checksums")
    for name, digest in expected.items():
        if checksum(directory / name) != digest:
            raise ValueError(f"candidate checksum mismatch: {name}")


def verify(archive, python):
    with tempfile.TemporaryDirectory(prefix="ledgence-archive-check-") as temporary:
        destination = Path(temporary)
        with tarfile.open(archive, "r:gz") as source:
            members = source.getmembers()
            if len(members) > 10000 or sum(item.size for item in members) > 1024 ** 3:
                raise ValueError("candidate archive exceeds supported bounds")
            names = set()
            roots = set()
            for item in members:
                path = PurePosixPath(item.name)
                if (not item.isfile() and not item.isdir()) or path.is_absolute() or ".." in path.parts or item.name in names:
                    raise ValueError("candidate archive contains an invalid entry")
                names.add(item.name)
                roots.add(path.parts[0])
            if len(roots) != 1:
                raise ValueError("candidate archive must have exactly one root")
            source.extractall(destination, filter="data")
        directory = destination / roots.pop()
        verify_files(directory)
        subprocess.run([python, str(Path(__file__).with_name("smoke.py")), "--directory", str(directory)], check=True, timeout=180)
    print("Actual candidate archive checksum inventory and relocated execution passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    verify(args.archive, args.python)
