"""Immutable, bounded source and canonical diff for the bundled exercise (MIT)."""
import difflib
import hashlib
from pathlib import Path

from .config import MAX_CANDIDATE_BYTES
from .validation import bounded_json, execution, fields, text

BASE_SOURCE = (Path(__file__).with_name("fixtures") / "pagination.py").read_text(encoding="utf-8")
BASE_SHA256 = hashlib.sha256(BASE_SOURCE.encode("utf-8")).hexdigest()


def source_digest(source):
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def canonical_patch(source):
    return "".join(difflib.unified_diff(BASE_SOURCE.splitlines(keepends=True), source.splitlines(keepends=True),
                                        fromfile="a/pagination.py", tofile="b/pagination.py"))


def validate_candidate(value):
    value = bounded_json(value, MAX_CANDIDATE_BYTES)
    fields(value, {"change_id", "base_sha256", "sha256", "source", "patch", "summary", "execution"})
    text(value["change_id"], 128, identifier=True)
    source = text(value["source"], 6 * 1024)
    text(value["summary"], 1024)
    if not source.endswith("\n"):
        raise ValueError("candidate source must end with a newline")
    if value["base_sha256"] != BASE_SHA256 or value["sha256"] != source_digest(source):
        raise ValueError("candidate digest does not match the bundled base and source")
    if value["patch"] != canonical_patch(source):
        raise ValueError("candidate patch is not the canonical source diff")
    execution(value["execution"])
    return value


def make_candidate(change_id, source, summary, observed_execution):
    text(source, 6 * 1024)
    source = source.rstrip("\n") + "\n"
    return validate_candidate({"change_id": change_id, "base_sha256": BASE_SHA256,
                               "sha256": source_digest(source), "source": source,
                               "patch": canonical_patch(source), "summary": summary,
                               "execution": observed_execution})
