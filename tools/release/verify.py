#!/usr/bin/env python3
"""Verify and smoke-test an extracted copy of an actual release bundle archive."""
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


def extract_archive(archive, destination, *, canonical_modes=False):
    """Extract only bounded, regular-file bundles into a private directory."""
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        if len(members) > 10000 or sum(item.size for item in members) > 1024 ** 3:
            raise ValueError("bundle archive exceeds supported bounds")
        names = set()
        roots = set()
        for item in members:
            path = PurePosixPath(item.name)
            if ((not item.isfile() and not item.isdir()) or path.is_absolute() or ".." in path.parts
                    or not path.parts or str(path) in names):
                raise ValueError("bundle archive contains an invalid entry")
            if canonical_modes:
                expected_mode = 0o755 if item.isdir() or path.parent.name == "bin" else 0o644
                if item.mode != expected_mode:
                    raise ValueError("candidate archive contains a noncanonical mode")
            names.add(str(path))
            roots.add(path.parts[0])
        if len(roots) != 1:
            raise ValueError("bundle archive must have exactly one root")
        source.extractall(destination, filter="data")
    directory = destination / roots.pop()
    if not directory.is_dir():
        raise ValueError("bundle archive root must be a directory")
    return directory


def verify(archive, python):
    with tempfile.TemporaryDirectory(prefix="ledgence-archive-check-") as temporary:
        directory = extract_archive(archive, Path(temporary) / "extracted")
        verify_files(directory)
        subprocess.run([python, str(Path(__file__).with_name("smoke.py")), "--directory", str(directory)], check=True, timeout=180)
    print("Actual bundle archive checksum inventory and relocated execution passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    verify(args.archive, args.python)
