"""Draft a support reply, then checkpoint for human approval (MIT)."""

from enum import StrEnum
import json
import unicodedata

from ledgence.worker.workflow import Workflow

DEFAULT_MODEL = "gpt-6-luna"
DEFAULT_QUEUE = "codex-support-demo"
DEFAULT_APPROVAL_TIMEOUT_MS = 3_600_000
MAX_APPROVAL_TIMEOUT_MS = 86_400_000
DRAFT_KEY = "draft"
APPROVAL_KEY = "approval:1"
SOURCE_LOCATIONS = {name: f"docs/{name}.md" for name in
                    ("task-results", "workflows", "workflow-events", "program-packages", "workflow-entrypoints")}
SOURCE_LOCATIONS["python-client"] = "sdk/python-client/README.md"
SOURCE_IDS = set(SOURCE_LOCATIONS)


def _text(value, name, limit, *, identifier=False, multiline=False):
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise ValueError(f"{name} must contain Unicode scalar values") from exc
    if size > limit:
        raise ValueError(f"{name} exceeds its {limit}-byte limit")
    for char in value:
        if identifier and not 33 <= ord(char) <= 126:
            raise ValueError(f"{name} must contain printable ASCII without whitespace")
        if ((unicodedata.category(char) == "Cc" and not (multiline and char in "\n\t"))
                or 0xFDD0 <= ord(char) <= 0xFDEF or ord(char) & 0xFFFE == 0xFFFE):
            raise ValueError(f"{name} contains unsupported characters")
    return value


def _ticket(event):
    data = event.get("data") if type(event) is dict else None
    if type(data) is not dict:
        raise ValueError("ticket data must be an object")
    ticket = {
        "ticket_id": _text(data.get("ticket_id"), "ticket_id", 128, identifier=True),
        "question": _text(data.get("question"), "question", 8192, multiline=True),
        "model": _text(data.get("model", DEFAULT_MODEL), "model", 128, identifier=True),
    }
    queue = _text(data.get("queue", DEFAULT_QUEUE), "queue", 128)
    timeout = data.get("approval_timeout_ms", DEFAULT_APPROVAL_TIMEOUT_MS)
    if type(timeout) is not int or not 0 <= timeout <= MAX_APPROVAL_TIMEOUT_MS:
        raise ValueError("approval_timeout_ms must be an integer from 0 through 86400000")
    return ticket, queue, timeout


def _draft(value, ticket):
    fields = {"ticket_id", "classification", "reply", "sources", "model", "execution"}
    if type(value) is not dict or value.keys() != fields:
        raise ValueError("draft must contain the structured support response")
    if value["ticket_id"] != ticket["ticket_id"] or value["model"] != ticket["model"]:
        raise ValueError("draft does not match the requested ticket and model")
    if value["classification"] not in ("how_to", "troubleshooting", "feature_question"):
        raise ValueError("draft classification is invalid")
    _text(value["reply"], "draft reply", 8192, multiline=True)
    sources = value["sources"]
    if type(sources) is not list or not 1 <= len(sources) <= 4:
        raise ValueError("draft must cite one through four bundled documents")
    seen = set()
    for source in sources:
        if type(source) is not dict or source.keys() != {"id", "title", "location"}:
            raise ValueError("draft source fields are invalid")
        source_id = _text(source["id"], "source id", 64, identifier=True)
        _text(source["title"], "source title", 128)
        _text(source["location"], "source location", 256)
        if (source_id not in SOURCE_IDS or source_id in seen
                or source["location"] != SOURCE_LOCATIONS[source_id]):
            raise ValueError("draft must cite distinct bundled document locations")
        seen.add(source_id)
    execution = value["execution"]
    if (type(execution) is not dict or execution.keys() != {
            "provider", "cli_invocations", "cli_version", "thread_id", "tool_calls", "usage"}
            or execution["provider"] != "codex"
            or type(execution["cli_invocations"]) is not int or execution["cli_invocations"] != 1):
        raise ValueError("draft must identify one Codex CLI invocation")
    _text(execution["cli_version"], "CLI version", 128)
    _text(execution["thread_id"], "Codex thread id", 128, identifier=True)
    if type(execution["tool_calls"]) is not int or not 2 <= execution["tool_calls"] <= 8:
        raise ValueError("draft tool_calls is outside the demo call budget")
    usage = execution["usage"]
    if type(usage) is not dict or usage.keys() != {
            "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"}:
        raise ValueError("draft token usage fields are invalid")
    for name, count in usage.items():
        if name == "reasoning_output_tokens" and count is None:
            continue
        if type(count) is not int or not 0 <= count <= 2**63 - 1:
            raise ValueError("draft token usage must contain nonnegative integers")
    if usage["cached_input_tokens"] > usage["input_tokens"]:
        raise ValueError("draft token usage totals are inconsistent")
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > 24 * 1024:
        raise ValueError("draft exceeds the 24 KiB response limit")
    return json.loads(encoded)


class Entry(StrEnum):
    START = "start"
    REVIEW = "review"
    FINISH = "finish"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    """Stage one draft task without calling the model or sending a reply."""
    try:
        ticket, queue, _timeout = _ticket(event)
    except ValueError as error:
        return ctx.fail("invalid_ticket", str(error))

    # The fixed key and immutable binding make activation replay idempotent.
    # One child attempt avoids repeating the whole agent and its tool loop.
    child = ctx.task(
        DRAFT_KEY, program="codex-support-agent", version="1.0.1", queue=queue,
        data=ticket, retry_policy={"max_attempts": 1, "retry_delay_ms": 0},
        attempt_timeout_ms=180_000,
    )
    return ctx.suspend(continuation=Entry.REVIEW, state={}, until=[child])


@workflow.entrypoint(Entry.REVIEW)
def review(event, ctx):
    try:
        ticket, _queue, timeout = _ticket(event)
    except ValueError as error:
        return ctx.fail("invalid_ticket", str(error))

    child = ctx.inputs.get(DRAFT_KEY)
    if type(child) is dict and child.get("state") in ("failed", "cancelled"):
        return ctx.fail("draft_failed", "The support draft task did not succeed")
    try:
        if (type(child) is not dict or "task_id" not in child
                or child.get("kind") == "workflow" or child.get("state") != "succeeded"
                or type(child.get("outcome")) is not dict
                or child["outcome"].get("kind") != "succeeded"):
            raise ValueError("the accepted support draft task result is missing")
        task_id = _text(child["task_id"], "draft_task_id", 128)
        draft = _draft(child["outcome"].get("output"), ticket)
    except ValueError as error:
        return ctx.fail("invalid_draft", str(error))
    # The orchestrator persists this wait and releases the activation slot.
    return ctx.wait_event(
        APPROVAL_KEY, continuation=Entry.FINISH,
        state={"draft": draft, "draft_task_id": task_id}, timeout_ms=timeout,
    )


@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    try:
        ticket, _queue, _timeout = _ticket(event)
    except ValueError as error:
        return ctx.fail("invalid_ticket", str(error))

    try:
        state = ctx.state
        if type(state) is not dict or state.keys() != {"draft", "draft_task_id"}:
            raise ValueError("the accepted draft checkpoint is missing")
        task_id = _text(state["draft_task_id"], "draft_task_id", 128)
        draft = _draft(state["draft"], ticket)
    except ValueError as error:
        return ctx.fail("invalid_draft", str(error))
    wake = ctx.wake
    if type(wake) is not dict or wake.get("key") != APPROVAL_KEY:
        return ctx.fail("invalid_approval", "Expected the approval:1 event or timeout")
    if wake.get("kind") == "timeout":
        status = "expired"
    elif wake.get("kind") == "event":
        envelope = wake.get("event")
        approval = envelope.get("data") if type(envelope) is dict else None
        if (type(approval) is not dict
                or approval.get("ticket_id") != ticket["ticket_id"]
                or approval.get("draft_task_id") != task_id
                or type(approval.get("approved")) is not bool):
            return ctx.fail("invalid_approval", "Approval must identify this ticket and draft task, "
                            "and include a boolean approved")
        status = "approved" if approval["approved"] else "rejected"
    else:
        return ctx.fail("invalid_approval", "Expected an approval event or timeout")
    return ctx.complete({"ticket_id": ticket["ticket_id"], "status": status,
                         "draft_task_id": task_id, "draft": draft})


handle = workflow.build()
