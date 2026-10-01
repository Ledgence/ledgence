"""Immutable change evidence, independent agent work, and human approval (MIT)."""
import hashlib
import os
from pathlib import Path
import re
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

NOTE_SCHEMA = {"type": "object", "properties": {
    "title": {"type": "string"}, "body": {"type": "string"}},
    "required": ["title", "body"], "additionalProperties": False}
COMPARISON_CASES = ((9999, 500), (10000, 0), (10001, 0))
BUNDLE_INPUTS = {"workflow_id", "candidate", "comparison", "tests", "review", "note", "decision"}


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


def validate_note(value, candidate):
    value = bounded_json(value, MAX_REPORT_BYTES)
    fields(value, {"candidate_sha256", "title", "body", "execution",
                   "started_at_ms", "finished_at_ms", "pid"})
    if value["candidate_sha256"] != candidate["sha256"]:
        raise ValueError("note and candidate identities differ")
    text(value["title"], 160)
    text(value["body"], 2048)
    if any(character in value["title"] for character in "\r\n\x00"):
        raise ValueError("note title must be one line")
    if "\x00" in value["body"]:
        raise ValueError("note body cannot contain null characters")
    execution(value["execution"])
    measurement(value)
    return value


async def draft_note(*, candidate, model=DEFAULT_MODEL):
    candidate = validate_candidate(candidate)
    text(model, 128, identifier=True)
    started = now_ms()
    prompt = ("PHASE: NOTE\nWrite a short customer-facing release note in a fresh session. Do not use tools.\n"
              "Return only the requested JSON: a title up to 160 UTF-8 bytes and body up to 2048 UTF-8 bytes.\n"
              "Describe the candidate's change accurately in plain language. This is a DRAFT pending tests, "
              "independent review and human approval. Do not claim it is approved, published or deployed.\n"
              + REQUIREMENTS + "\nCANDIDATE_SHA256: " + candidate["sha256"]
              + "\nCANDIDATE_SOURCE:\n" + candidate["source"] + "\nCANONICAL_PATCH:\n" + candidate["patch"])
    result = await codex_turn(prompt, NOTE_SCHEMA, model)
    fields(result["output"], {"title", "body"})
    return validate_note({"candidate_sha256": candidate["sha256"], **result["output"],
                          "execution": result["execution"], "started_at_ms": started,
                          "finished_at_ms": now_ms(), "pid": os.getpid()}, candidate)


def validate_comparison(value, candidate):
    value = bounded_json(value, MAX_REPORT_BYTES)
    fields(value, {"candidate_sha256", "cases", "started_at_ms", "finished_at_ms", "pid"})
    if value["candidate_sha256"] != candidate["sha256"]:
        raise ValueError("comparison and candidate identities differ")
    cases = value["cases"]
    if type(cases) is not list or len(cases) != len(COMPARISON_CASES):
        raise ValueError("comparison requires the three fixed boundary cases")
    for case, (total, expected) in zip(cases, COMPARISON_CASES, strict=True):
        fields(case, {"total_cents", "expected_cents", "before", "after"})
        if (type(case["total_cents"]) is not int or case["total_cents"] != total
                or type(case["expected_cents"]) is not int or case["expected_cents"] != expected):
            raise ValueError("comparison case differs from the fixed requirements")
        for side in ("before", "after"):
            observation = case[side]
            fields(observation, {"value", "error"})
            if observation["error"] is None:
                if type(observation["value"]) is not int or not -(2**63) <= observation["value"] < 2**63:
                    raise ValueError("comparison result must be a bounded integer")
            else:
                if observation["value"] is not None:
                    raise ValueError("failed comparison cannot have a result")
                text(observation["error"], 512)
    measurement(value)
    return value


async def compare_candidate(*, candidate):
    """Measure both sources outside the worker helper; never infer the after values."""
    candidate = validate_candidate(candidate)
    started = now_ms()
    with tempfile.TemporaryDirectory(prefix="ledgence-change-comparison-") as directory:
        workdir = Path(directory)
        (workdir / "before.py").write_text(BASE_SOURCE, encoding="utf-8")
        (workdir / "after.py").write_text(candidate["source"], encoding="utf-8")
        shutil.copyfile(Path(__file__).with_name("fixtures") / "comparison.py", workdir / "comparison.py")
        try:
            code, stdout, _stderr = await collect(
                [sys.executable, "-I", "-S", "-B", str(workdir / "comparison.py")], cwd=directory,
                environment={"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"}, timeout=10,
                stdout_limit=16 * 1024, stderr_limit=16 * 1024)
        except ProcessError:
            raise SystemExit("Comparison subprocess failed; retire the worker session") from None
        try:
            output = decode_json(stdout.decode("utf-8").splitlines()[-1])
            fields(output, {"cases"})
            if code != 0:
                raise ValueError("exit status")
            return validate_comparison({"candidate_sha256": candidate["sha256"], **output,
                                        "started_at_ms": started, "finished_at_ms": now_ms(),
                                        "pid": os.getpid()}, candidate)
        except (ValueError, IndexError, UnicodeError):
            raise SystemExit("Invalid comparison subprocess report; retire the worker session") from None


def assemble_bundle(data):
    """Revalidate all evidence before waiting or finalizing; never run the agent here."""
    data = bounded_json(data, MAX_STATE_BYTES)
    if (type(data) is not dict or not BUNDLE_INPUTS - {"decision"} <= set(data)
            or set(data) - BUNDLE_INPUTS - {"publication"}):
        raise ValueError("bundle input fields do not match the example contract")
    workflow_id = text(data["workflow_id"], 128, identifier=True)
    candidate = validate_candidate(data["candidate"])
    comparison = validate_comparison(data["comparison"], candidate)
    tests = validate_tests(data["tests"], candidate)
    review = validate_review(data["review"], candidate)
    note = validate_note(data["note"], candidate)
    compared = all(case["after"]["error"] is None and case["after"]["value"] == case["expected_cents"]
                   for case in comparison["cases"])
    ready = tests["passed"] and review["verdict"] == "approve" and compared
    decision = data.get("decision")
    status = "waiting_for_approval" if ready else "needs_changes"
    if decision is not None:
        fields(decision, {"workflow_id", "candidate_sha256", "outcome", "event_id"})
        if (decision["workflow_id"] != workflow_id or decision["candidate_sha256"] != candidate["sha256"]
                or decision["outcome"] not in ("approved", "rejected", "expired")):
            raise ValueError("decision does not bind this workflow and candidate")
        if decision["outcome"] == "expired":
            if decision["event_id"] is not None:
                raise ValueError("an expired decision cannot claim an event")
        else:
            text(decision["event_id"], 128)
        if decision["outcome"] == "approved" and not ready:
            raise ValueError("human approval cannot override failed evidence")
        status = decision["outcome"]
    title = "Fix free-shipping threshold at 10000 cents"
    human = "Pending" if decision is None else decision["outcome"]
    body = ("## Change\n\n" + candidate["summary"] + "\n\n"
            "## Validation\n\n"
            f"- Acceptance tests: {tests['total']} run, {tests['failures']} failures, {tests['errors']} errors.\n"
            f"- Independent Codex review: {review['verdict']}. {review['summary']}\n"
            f"- Boundary comparison: {'passed' if compared else 'needs changes'}.\n"
            f"- Human decision: {human}.\n"
            f"- Workflow: `{workflow_id}`.\n"
            f"- Candidate SHA-256: `{candidate['sha256']}`.\n"
            f"- Bundled base SHA-256: `{candidate['base_sha256']}`.\n"
            "\n## Release note draft\n\n" + note["title"] + "\n\n" + note["body"]
            + "\n\nNo merge or deployment is performed.\n")
    if "\x00" in body:
        raise ValueError("generated pull request text contains null characters")
    bundle = {"workflow_id": workflow_id, "change_id": candidate["change_id"], "status": status,
              "candidate": candidate, "comparison": comparison, "tests": tests, "review": review,
              "note": note, "decision": decision, "pull_request": {"title": title, "body": body, "url": None}}
    return bounded_json(bundle, MAX_STATE_BYTES)


def validate_bundle(value):
    """Validate a saved or exported packet, including any optional publication receipt."""
    value = bounded_json(value, MAX_STATE_BYTES)
    required = BUNDLE_INPUTS | {"change_id", "status", "pull_request"}
    if type(value) is not dict or not required <= set(value) or set(value) - required - {"publication"}:
        raise ValueError("bundle fields do not match the example contract")
    canonical = assemble_bundle({key: value[key] for key in BUNDLE_INPUTS})
    if any(value[key] != canonical[key] for key in required - {"pull_request"}):
        raise ValueError("bundle identity or status disagrees with its evidence")
    pr = value["pull_request"]
    fields(pr, {"title", "body", "url"})
    text(pr["title"], 256)
    text(pr["body"], 16 * 1024)
    if any(char in pr["title"] for char in "\r\n\x00") or "\x00" in pr["body"]:
        raise ValueError("invalid pull request text")
    if "publication" not in value:
        if pr["url"] is not None:
            raise ValueError("a pull request URL requires a publication receipt")
    else:
        from .publishing import validate_publication
        receipt = value["publication"]
        fields(receipt, {"repository", "branch", "base_branch", "base_commit", "head_commit",
                         "number", "url", "state", "reconciled"})
        target = validate_publication({key: receipt[key] for key in ("repository", "base_branch", "base_commit")})
        branch = ("ledgence/change-" + hashlib.sha256(value["change_id"].encode("utf-8")).hexdigest()[:16]
                  + "-" + value["candidate"]["sha256"][:16])
        if (value["status"] != "approved" or target["repository"] != receipt["repository"]
                or receipt["branch"] != branch or type(receipt["head_commit"]) is not str
                or re.fullmatch(r"[0-9a-f]{40}", receipt["head_commit"]) is None
                or type(receipt["number"]) is not int or receipt["number"] <= 0
                or type(receipt["reconciled"]) is not bool or receipt["state"] not in ("open", "closed", "merged")
                or receipt["url"] != f"https://github.com/{receipt['repository']}/pull/{receipt['number']}"
                or pr["url"] != receipt["url"]):
            raise ValueError("publication receipt does not match the approved bundle")
    return value


async def finalize_task(event):
    data = event["data"]
    bundle = assemble_bundle(data)
    if data.get("publication") is not None and bundle["status"] == "approved":
        from .publishing import publish
        bundle = validate_bundle(await publish(bundle, data["publication"]))
    return bundle
