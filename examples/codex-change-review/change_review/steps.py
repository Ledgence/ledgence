"""Implementation, local tests, independent review, and final assembly (MIT)."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

from .candidate import BASE_SHA256, BASE_SOURCE, make_candidate, validate_candidate
from .codex import CodexError, decode_json, run_codex
from .config import DEFAULT_MODEL, MAX_REPORT_BYTES, MAX_STATE_BYTES, REQUIREMENTS, TEST_COUNT
from .processes import ProcessError, collect
from .validation import bounded_json, execution, fields, measurement, text

IMPLEMENT_SCHEMA = {"type": "object", "properties": {"source": {"type": "string"}, "summary": {"type": "string"}},
                    "required": ["source", "summary"], "additionalProperties": False}
REVIEW_SCHEMA = {"type": "object", "properties": {
    "verdict": {"type": "string", "enum": ["approve", "request_changes"]}, "summary": {"type": "string"},
    "findings": {"type": "array", "items": {"type": "object", "properties": {
        "severity": {"type": "string", "enum": ["low", "medium", "high"]},
        "line": {"type": "integer"}, "message": {"type": "string"}},
        "required": ["severity", "line", "message"], "additionalProperties": False}}},
    "required": ["verdict", "summary", "findings"], "additionalProperties": False}


def now_ms():
    return time.time_ns() // 1_000_000


async def codex_turn(prompt, schema, model):
    try:
        return await run_codex(prompt, schema, model=model)
    except CodexError:
        # The worker owns this process group. Retiring this helper lets it also
        # drain any CLI descendants rather than reusing an uncertain session.
        raise SystemExit("Codex invocation failed; retire the worker session") from None


async def implement_task(event):
    data = event["data"]
    change_id = text(data["change_id"], 128, identifier=True)
    model = text(data.get("model", DEFAULT_MODEL), 128, identifier=True)
    prompt = ("PHASE: IMPLEMENT\nReturn only the requested JSON object; do not use any tools.\n"
              "Repair this single small module according to the requirements. Return its full source and a short summary.\n"
              + REQUIREMENTS + "\nBASE_SHA256: " + BASE_SHA256 + "\nBASE_SOURCE:\n" + BASE_SOURCE)
    result = await codex_turn(prompt, IMPLEMENT_SCHEMA, model)
    fields(result["output"], {"source", "summary"})
    return make_candidate(change_id, result["output"]["source"], result["output"]["summary"], result["execution"])


def validate_review(value, candidate):
    value = bounded_json(value, MAX_REPORT_BYTES)
    fields(value, {"candidate_sha256", "verdict", "summary", "findings", "execution",
                   "started_at_ms", "finished_at_ms", "pid"})
    if value["candidate_sha256"] != candidate["sha256"] or value["verdict"] not in ("approve", "request_changes"):
        raise ValueError("review identity or verdict is invalid")
    text(value["summary"], 2048)
    findings = value["findings"]
    if type(findings) is not list or len(findings) > 8:
        raise ValueError("review findings must be a bounded list")
    for finding in findings:
        fields(finding, {"severity", "line", "message"})
        if (finding["severity"] not in ("low", "medium", "high") or type(finding["line"]) is not int
                or not 1 <= finding["line"] <= len(candidate["source"].splitlines())):
            raise ValueError("review finding location or severity is invalid")
        text(finding["message"], 1024)
    if (value["verdict"] == "approve") != (not findings):
        raise ValueError("review verdict must agree with its findings")
    execution(value["execution"])
    measurement(value)
    return value


async def review_candidate(*, candidate, model=DEFAULT_MODEL):
    candidate = validate_candidate(candidate)
    text(model, 128, identifier=True)
    started = now_ms()
    prompt = ("PHASE: REVIEW\nYou are an independent reviewer in a fresh session. Do not use tools or propose changes to files.\n"
              "Return only the requested JSON. Check the full candidate against the requirements, especially the exact threshold.\n"
              "Approve with no findings, or request_changes with 1-8 concrete findings using 1-based candidate line numbers.\n"
              + REQUIREMENTS + "\nCANDIDATE_SHA256: " + candidate["sha256"]
              + "\nCANDIDATE_SOURCE:\n" + candidate["source"] + "\nCANONICAL_PATCH:\n" + candidate["patch"])
    result = await codex_turn(prompt, REVIEW_SCHEMA, model)
    fields(result["output"], {"verdict", "summary", "findings"})
    return validate_review({"candidate_sha256": candidate["sha256"], **result["output"],
                            "execution": result["execution"], "started_at_ms": started,
                            "finished_at_ms": now_ms(), "pid": os.getpid()}, candidate)


def validate_tests(value, candidate):
    value = bounded_json(value, MAX_REPORT_BYTES)
    fields(value, {"candidate_sha256", "passed", "total", "failures", "errors", "output",
                   "started_at_ms", "finished_at_ms", "pid"})
    if value["candidate_sha256"] != candidate["sha256"] or type(value["passed"]) is not bool:
        raise ValueError("test identity or result is invalid")
    if (type(value["total"]) is not int or value["total"] != TEST_COUNT
            or any(type(value[name]) is not int or not 0 <= value[name] <= TEST_COUNT for name in ("failures", "errors"))
            or value["failures"] + value["errors"] > TEST_COUNT
            or value["passed"] != (value["failures"] == value["errors"] == 0)):
        raise ValueError("test counters are inconsistent")
    text(value["output"], 8192)
    measurement(value)
    return value


async def run_tests(*, candidate):
    candidate = validate_candidate(candidate)
    started = now_ms()
    with tempfile.TemporaryDirectory(prefix="ledgence-change-tests-") as directory:
        workdir = Path(directory)
        (workdir / "shipping.py").write_text(candidate["source"], encoding="utf-8")
        shutil.copyfile(Path(__file__).with_name("fixtures") / "acceptance.py", workdir / "acceptance.py")
        try:
            code, stdout, stderr = await collect(
                [sys.executable, "-I", "-S", "-B", str(workdir / "acceptance.py")], cwd=directory,
                environment={"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"}, timeout=10,
                stdout_limit=16 * 1024, stderr_limit=16 * 1024)
        except ProcessError:
            raise SystemExit("Acceptance subprocess failed; retire the worker session") from None
        try:
            lines = stdout.decode("utf-8").splitlines()
            counters = decode_json(lines[-1])
            fields(counters, {"passed", "total", "failures", "errors"})
            if code not in (0, 1) or (code == 0) != (counters["passed"] is True):
                raise ValueError("exit status")
        except (ValueError, IndexError, UnicodeError):
            raise SystemExit("Invalid acceptance subprocess report; retire the worker session") from None
        output = stderr.decode("utf-8", errors="replace")
        if len(output.encode("utf-8")) > 8192:
            output = output.encode("utf-8")[:8170].decode("utf-8", errors="ignore") + "\n[output truncated]"
        report = validate_tests({"candidate_sha256": candidate["sha256"], **counters, "output": output,
                                 "started_at_ms": started, "finished_at_ms": now_ms(), "pid": os.getpid()}, candidate)
        bounded_json({"candidate": candidate, "tests": report}, MAX_STATE_BYTES)
        return report


async def finalize_task(event):
    data = event["data"]
    candidate = validate_candidate(data["candidate"])
    tests = validate_tests(data["tests"], candidate)
    review = validate_review(data["review"], candidate)
    ready = tests["passed"] and review["verdict"] == "approve"
    status = "ready_for_review" if ready else "needs_changes"
    title = "Fix free-shipping threshold at 10000 cents"
    body = ("## Change\n\n" + candidate["summary"] + "\n\n"
            "## Validation\n\n"
            f"- Acceptance tests: {tests['total']} run, {tests['failures']} failures, {tests['errors']} errors.\n"
            f"- Independent Codex review: {review['verdict']}. {review['summary']}\n"
            f"- Candidate SHA-256: `{candidate['sha256']}`.\n"
            f"- Bundled base SHA-256: `{candidate['base_sha256']}`.\n"
            "\nGenerated as a draft for human review; no merge is performed.\n")
    bundle = {"change_id": candidate["change_id"], "status": status, "candidate": candidate,
              "tests": tests, "review": review, "pull_request": {"title": title, "body": body, "url": None}}
    bundle = bounded_json(bundle, 64 * 1024)
    if data.get("publication") is not None and ready:
        from .publishing import publish
        bundle = await publish(bundle, data["publication"])
    return bundle
