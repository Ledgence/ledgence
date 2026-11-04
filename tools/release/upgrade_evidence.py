"""Require successful, binary-bound predecessor rehearsals before candidate upload (MIT)."""

import hashlib
import json
import sys
from pathlib import Path

import tomllib


def digest(binary):
    with binary.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify(directory):
    root = Path(__file__).resolve().parents[2]
    with (root / "Cargo.toml").open("rb") as source:
        version = tomllib.load(source)["workspace"]["package"]["version"]
    expected = f"ledgence {version}"
    candidate_hash = digest(directory / "candidate/bin/ledgence")
    for previous in ("0.6.1", "0.7.0"):
        evidence = directory / "evidence" / previous / "results.json"
        report = json.loads(evidence.read_text())
        old_hash = digest(directory / f"previous-{previous}/bin/ledgence")
        if (
            report.get("previous_version") != f"ledgence {previous}"
            or report.get("candidate_version") != expected
            or report.get("executable_sha256")
            != {"previous": old_hash, "candidate": candidate_hash}
            or old_hash == candidate_hash
        ):
            raise ValueError(f"{evidence} does not qualify the selected executables")
    evidence = directory / "evidence/0.8.0/summary.json"
    report = json.loads(evidence.read_text())
    completed = (
        "passed",
        "database_cleanup",
        "process_cleanup",
        "credential_scan_clean",
    )
    if any(report.get(key) is not True for key in completed):
        raise ValueError(f"{evidence} is incomplete")
    provenance = report["provenance"]
    if (
        provenance.get("versions") != ["ledgence 0.8.0", expected]
        or provenance.get("new_binary_sha256") != candidate_hash
        or provenance.get("old_verified_files", {}).get("bin/ledgence")
        != digest(directory / "previous-0.8.0/bin/ledgence")
    ):
        raise ValueError(f"{evidence} does not qualify the selected executables")
    print(f"Verified 0.6.1, 0.7.0 and 0.8.0 upgrade evidence for {expected}")


if __name__ == "__main__":
    verify(Path(sys.argv[1]))
