"""Small JSON boundary checks shared by the application helpers (MIT)."""
import json
import re


def bounded_json(value, maximum):
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > maximum:
            raise ValueError("size")
        return json.loads(encoded)
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ValueError("value must be JSON within the example's byte limit") from None


def fields(value, names):
    if type(value) is not dict or set(value) != set(names):
        raise ValueError("object fields do not match the example contract")


def text(value, maximum, *, identifier=False):
    if type(value) is not str or not value.strip():
        raise ValueError("expected nonempty text")
    try:
        if len(value.encode("utf-8")) > maximum:
            raise ValueError("text exceeds its byte limit")
    except UnicodeError:
        raise ValueError("text must contain valid Unicode") from None
    if identifier and not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value):
        raise ValueError("expected a simple identifier")
    return value


def execution(value):
    fields(value, {"provider", "model", "cli_version", "thread_id", "cli_invocations", "usage"})
    if value["provider"] != "codex" or type(value["cli_invocations"]) is not int or value["cli_invocations"] != 1:
        raise ValueError("expected one Codex CLI invocation")
    for name in ("model", "cli_version", "thread_id"):
        text(value[name], 128, identifier=True)
    usage = value["usage"]
    fields(usage, {"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"})
    for name, count in usage.items():
        if name == "reasoning_output_tokens" and count is None:
            continue
        if type(count) is not int or not 0 <= count < 2**63:
            raise ValueError("invalid token counter")
    if usage["cached_input_tokens"] > usage["input_tokens"]:
        raise ValueError("cached token count exceeds input tokens")
    return bounded_json(value, 2048)


def measurement(value):
    for name in ("started_at_ms", "finished_at_ms", "pid"):
        if type(value[name]) is not int or not 0 < value[name] < 2**63:
            raise ValueError("invalid process measurement")
    # Wall-clock adjustments can move time backwards. Measurements are evidence,
    # not a cross-host ordering or overlap guarantee.
