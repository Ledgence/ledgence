"""Optional bounded Codex CLI adapter (MIT).

Adapted from the Ledgence change-review example. Uses a separately installed
Codex CLI; no provider SDK is part of the public core or required offline.
Official interface: https://learn.chatgpt.com/docs/non-interactive-mode
"""
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


import asyncio
from contextlib import suppress


class ProcessError(RuntimeError):
    pass


async def collect(argv, *, cwd, environment, data=b"", timeout=120, stdout_limit=128 * 1024,
                  stderr_limit=32 * 1024):
    process = None
    tasks = []
    cleanup_failed = False
    async def read(stream, maximum):
        output = bytearray()
        while chunk := await stream.read(8192):
            output.extend(chunk)
            if len(output) > maximum:
                raise ProcessError("subprocess output limit exceeded")
        return bytes(output)

    async def write():
        try:
            process.stdin.write(data)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            process.stdin.close()

    try:
        async with asyncio.timeout(timeout):
            process = await asyncio.create_subprocess_exec(
                *argv, cwd=cwd, env=environment, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=False)
            tasks = [asyncio.create_task(read(process.stdout, stdout_limit)),
                     asyncio.create_task(read(process.stderr, stderr_limit)),
                     asyncio.create_task(write()), asyncio.create_task(process.wait())]
            stdout, stderr, _, code = await asyncio.gather(*tasks)
            return code, stdout, stderr
    except TimeoutError:
        raise ProcessError("subprocess execution deadline exceeded") from None
    except OSError:
        raise ProcessError("could not start or communicate with subprocess") from None
    finally:
        if process is not None:
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 2)
                except TimeoutError:
                    with suppress(ProcessLookupError):
                        process.kill()
                    try:
                        await asyncio.wait_for(process.wait(), 2)
                    except TimeoutError:
                        cleanup_failed = True
            process.stdin.close()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if cleanup_failed:
            raise ProcessError("subprocess cleanup deadline exceeded") from None

"""One fresh, bounded, tool-free Codex CLI turn using saved ChatGPT login (MIT)."""
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time

DEFAULT_MODEL = "gpt-6-luna"

MAX_FINAL_BYTES = 12 * 1024
MAX_EVENTS = 512


class CodexError(RuntimeError):
    """Fixed diagnostics only; never include raw CLI output or credentials."""


def decode_json(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value
    def finite(_):
        raise ValueError("non-finite JSON number")
    return json.loads(raw, object_pairs_hook=unique, parse_constant=finite)


def parse_events(raw):
    try:
        lines = raw.decode("utf-8").splitlines()
        if not 1 <= len(lines) <= MAX_EVENTS:
            raise ValueError("event count")
        thread = final = usage = None
        started = completed = False
        for line in lines:
            event = decode_json(line)
            if type(event) is not dict:
                raise ValueError("event object")
            kind = event.get("type")
            if kind == "thread.started":
                if thread is not None or started or completed:
                    raise ValueError("thread order")
                thread = text(event.get("thread_id"), 128, identifier=True)
            elif kind == "turn.started":
                if thread is None or started or completed:
                    raise ValueError("turn order")
                started = True
            elif kind == "turn.completed":
                if not started or completed:
                    raise ValueError("completion order")
                usage = event.get("usage")
                if type(usage) is not dict:
                    raise ValueError("usage object")
                usage = {name: usage.get(name) for name in
                         ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")}
                completed = True
            elif kind in ("item.started", "item.updated", "item.completed"):
                item = event.get("item")
                if type(item) is not dict:
                    raise ValueError("item object")
                if item.get("type") == "error":
                    warning = item.get("message")
                    if kind != "item.completed" or type(warning) is not str or warning.startswith("model rerouted:"):
                        raise ValueError("CLI warning")
                    continue
                if not started or completed:
                    raise ValueError("item order")
                if item.get("type") == "agent_message":
                    if kind == "item.completed":
                        final = text(item.get("text"), MAX_FINAL_BYTES)
                elif item.get("type") != "reasoning":
                    raise ValueError("tools are not allowed")
            else:
                raise ValueError("unexpected event")
        if not completed or final is None or usage is None:
            raise ValueError("incomplete turn")
        output = decode_json(final)
        if type(output) is not dict:
            raise ValueError("output object")
        # Validate usage here, before it is allowed into a candidate or report.
        execution({"provider": "codex", "model": DEFAULT_MODEL, "cli_version": "0.0.0",
                   "thread_id": thread, "cli_invocations": 1, "usage": usage})
        return output, thread, usage
    except (ValueError, UnicodeError, TypeError, RecursionError):
        raise CodexError("Codex returned an invalid, tool-using, failed, or incomplete turn") from None


def environment():
    names = {"HOME", "USER", "LOGNAME", "PATH", "TMPDIR", "TEMP", "TMP", "CODEX_HOME",
             "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTPS_PROXY", "HTTP_PROXY",
             "ALL_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "all_proxy", "no_proxy"}
    return {**{name: value for name, value in os.environ.items() if name in names},
            "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"}


def settings():
    values = {"forced_login_method": "chatgpt", "model_provider": "openai", "approval_policy": "never",
              "model_reasoning_effort": "low", "web_search": "disabled", "project_doc_max_bytes": 0,
              "skills.include_instructions": False, "skills.bundled.enabled": False,
              "history.persistence": "none", "analytics.enabled": False,
              "otel.exporter": "none", "otel.trace_exporter": "none", "otel.metrics_exporter": "none",
              "tools.update_plan.enabled": False, "features.skip_host_skill_discovery": True}
    for feature in ("skill_search", "shell_tool", "unified_exec", "shell_snapshot", "apps", "plugins",
                    "remote_plugin", "browser_use", "computer_use", "image_generation", "view_image",
                    "multi_agent", "multi_agent_v2", "memories", "code_mode", "code_mode_only",
                    "code_mode_host", "hooks", "goals", "sleep_tool", "tool_suggest",
                    "workspace_dependencies", "unbounded_connection_retries"):
        values["features." + feature] = False
    return [item for name, value in values.items() for item in ("-c", name + "=" + json.dumps(value))]


async def run_codex(prompt, schema, *, model=DEFAULT_MODEL, timeout=120):
    """Return structured output and observed CLI metrics; no inferred HTTP counts."""
    try:
        text(prompt, 48 * 1024)
        text(model, 128, identifier=True)
        schema = bounded_json(schema, 8 * 1024)
        if type(schema) is not dict or type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError("invalid invocation")
    except ValueError:
        raise CodexError("Codex invocation arguments are invalid") from None
    binary = Path(os.environ.get("LEDGENCE_CODEX_BIN", ""))
    if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise CodexError("LEDGENCE_CODEX_BIN must name an absolute executable Codex CLI path")
    deadline = time.monotonic() + timeout
    try:
        with tempfile.TemporaryDirectory(prefix="ledgence-fulfillment-codex-") as directory:
            env = environment()
            code, stdout, _ = await collect([str(binary), "--version"], cwd=directory, environment=env,
                                             timeout=min(10, timeout))
            version = re.fullmatch(rb"codex-cli ([0-9]+\.[0-9]+\.[0-9]+(?:[-.][A-Za-z0-9.-]+)?)\s*", stdout)
            if code or version is None:
                raise CodexError("Configured executable did not report a supported Codex CLI version")
            schema_path = Path(directory) / "output-schema.json"
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexError("Codex execution deadline exceeded")
            command = [str(binary), "exec", "--strict-config", "--ignore-user-config", "--ignore-rules",
                       "--ephemeral", "--skip-git-repo-check", "--sandbox", "read-only", "--json",
                       "--color", "never", "--model", model, "--cd", directory,
                       "--output-schema", str(schema_path), *settings(), "-"]
            code, stdout, _ = await collect(command, cwd=directory, environment=env,
                                             data=prompt.encode("utf-8"), timeout=remaining)
            if code:
                raise CodexError("Codex CLI exited without an accepted result")
            output, thread, usage = parse_events(stdout)
            return {"output": output, "execution": execution({"provider": "codex", "model": model,
                    "cli_version": version.group(1).decode("ascii"), "thread_id": thread,
                    "cli_invocations": 1, "usage": usage})}
    except (ProcessError, OSError):
        raise CodexError("Codex process failed its deadline, output, startup, or cleanup contract") from None
