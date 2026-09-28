"""Bounded, non-interactive Codex CLI invocation using the host's ChatGPT login.

The CLI remains an operator-installed executable; no credentials are copied into
the program package. Its HTTP retries and model requests are not observable here.
"""
import json
import math
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import time

MAX_STDOUT_BYTES = 1024 * 1024
MAX_STDERR_BYTES = 128 * 1024
MAX_PROMPT_BYTES = 128 * 1024
MAX_FINAL_BYTES = 24 * 1024
MAX_EVENTS = 2048
MCP_SERVER = "ledgence_docs"
TOOL_NAMES = {"search_docs", "read_doc"}
ERROR_CATEGORIES = frozenset({
    "runtime", "configuration", "authentication", "model_unavailable", "model_rerouted", "quota",
    "mcp", "network", "provider", "cli_exit", "timeout", "output_limit",
    "startup", "event_encoding", "event_json", "event_count", "event_shape",
    "event_order", "event_usage", "event_message", "event_incomplete",
    "event_type", "item_type", "tool_scope", "tool_failure",
    "event_before_turn", "event_after_turn", "event_repeated_start",
    "event_repeated_completion", "event_missing_thread", "event_repeated_thread",
})


class CodexError(ValueError):
    """Fixed, safe diagnostics; never contains CLI output or credential values."""

    def __init__(self, message, *, category="runtime"):
        super().__init__(message)
        # Callers may transport only this closed-set value across the worker
        # boundary, even when an integration accidentally supplies a raw message.
        self.category = category if isinstance(category, str) and category in ERROR_CATEGORIES else "runtime"


def _diagnostic_category(messages, *, default):
    """Classify recognized failure indicators without returning provider text."""
    text = "\n".join(messages).lower()
    if any(marker in text for marker in (
        "error loading config", "unknown configuration field", "invalid configuration",
        "unexpected argument", "invalid value for", "invalid output schema",
    )):
        return "configuration"
    if "model" in text and any(marker in text for marker in (
        "not supported", "not available", "not found", "does not exist",
        "unavailable", "unsupported", "not have access",
    )):
        return "model_unavailable"
    if any(marker in text for marker in (
        "not logged in", "authentication failed", "authentication error",
        "unauthorized", "invalid auth",
        "login required", "please log in", "token has expired", "refresh token",
    )) or re.search(r"\b(?:http(?: status)?|status(?: code)?)[:= ]+401\b", text):
        return "authentication"
    if any(marker in text for marker in (
        "rate limit", "rate_limit", "usage limit", "quota", "insufficient credits",
        "too many requests",
    )) or re.search(r"\b(?:http(?: status)?|status(?: code)?)[:= ]+429\b", text):
        return "quota"
    if any(
        any(marker in line for marker in ("mcp", "tools/list"))
        and any(marker in line for marker in (
            "failed", "error", "timed out", "timeout", "unexpected", "invalid",
            "unsupported", "could not", "couldn't",
        ))
        for line in text.splitlines()
    ):
        return "mcp"
    if any(marker in text for marker in (
        "connection refused", "connection reset", "connection timed out",
        "error sending request", "failed to connect", "dns error",
        "network error", "stream disconnected", "stream closed",
    )):
        return "network"
    if re.search(r"\b(?:http(?: status)?|status(?: code)?)[:= ]+5[0-9]{2}\b", text):
        return "provider"
    return default


def _failure_category(stdout, stderr):
    # Only fatal error fields are useful here. Agent messages and MCP documents
    # can discuss configuration/authentication and must not affect diagnostics.
    messages = []
    for line in stdout.splitlines()[:MAX_EVENTS]:
        try:
            event = _json_value(line)
        except (ValueError, UnicodeError, RecursionError):
            continue
        if not isinstance(event, dict):
            continue
        if event.get("type") == "error":
            message = event.get("message")
        elif event.get("type") == "turn.failed" and isinstance(event.get("error"), dict):
            message = event["error"].get("message")
        else:
            continue
        if isinstance(message, str):
            messages.append(message)
    category = _diagnostic_category(messages, default="cli_exit")
    if category != "cli_exit":
        return category
    return _diagnostic_category([stderr.decode("utf-8", errors="replace")], default="cli_exit")


def _environment():
    # Preserve the operator's existing HOME/CODEX_HOME for supported login.
    # Neither API credentials nor inherited app-server connection settings belong
    # in this standalone child. Use a small allowlist instead of copying secrets.
    names = {
        "HOME", "USER", "LOGNAME", "PATH", "TMPDIR", "TEMP", "TMP",
        "SYSTEMROOT", "WINDIR", "CODEX_HOME", "LANG", "LC_ALL",
        "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTPS_PROXY", "HTTP_PROXY",
        "ALL_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "all_proxy",
        "no_proxy",
    }
    environment = {key: value for key, value in os.environ.items() if key in names}
    environment.update({"PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"})
    return environment


def _terminate(process):
    """Reap the direct child, preserving the worker's process group ownership."""
    if process.poll() is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            process.wait(timeout=2)
    for pipe in (process.stdin, process.stdout, process.stderr):
        if pipe is not None:
            pipe.close()


def _collect(command, *, environment, directory, deadline, data=b""):
    """Bound both streams while also enforcing the deadline during stdin writes."""
    process = None
    output = {"stdout": bytearray(), "stderr": bytearray()}
    try:
        process = subprocess.Popen(
            command, cwd=directory, env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            # The Rust worker owns the process group, including Codex and MCP.
            start_new_session=False,
        )
        with selectors.DefaultSelector() as selector:
            for name in ("stdout", "stderr"):
                pipe = getattr(process, name)
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, name)
            if data:
                os.set_blocking(process.stdin.fileno(), False)
                selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
            else:
                process.stdin.close()
            written = 0
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise CodexError("Codex exceeded the agent execution budget", category="timeout")
                for key, _ in selector.select(min(remaining, 0.1)):
                    if key.data == "stdin":
                        try:
                            written += os.write(key.fd, data[written:written + 16384])
                        except BrokenPipeError:
                            written = len(data)
                        if written == len(data):
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                        continue
                    chunk = os.read(key.fd, 16384)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    target = output[key.data]
                    target.extend(chunk)
                    maximum = MAX_STDOUT_BYTES if key.data == "stdout" else MAX_STDERR_BYTES
                    if len(target) > maximum:
                        raise CodexError("Codex output exceeded the demo's size limit", category="output_limit")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexError("Codex exceeded the agent execution budget", category="timeout")
            try:
                code = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                raise CodexError("Codex exceeded the agent execution budget", category="timeout") from None
            return code, bytes(output["stdout"]), bytes(output["stderr"])
    except OSError:
        raise CodexError("Could not start or communicate with the configured Codex CLI", category="startup") from None
    finally:
        if process is not None:
            _terminate(process)


def _json_value(raw):
    def no_duplicates(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    def no_constants(_):
        raise ValueError("non-finite constant")

    return json.loads(raw, object_pairs_hook=no_duplicates, parse_constant=no_constants)


def _parse_events(raw):
    """Accept only a completed turn and auditable tools from our MCP server."""
    try:
        lines = raw.decode("utf-8").splitlines()
        if not 1 <= len(lines) <= MAX_EVENTS:
            raise ValueError("event count")
        thread_id = None
        started = completed = False
        final = None
        usage = None
        for line in lines:
            event = _json_value(line)
            if not isinstance(event, dict):
                raise ValueError("event object")
            kind = event.get("type")
            if not isinstance(kind, str):
                raise ValueError("event object")
            if kind == "thread.started":
                candidate = event.get("thread_id")
                if thread_id is not None:
                    raise ValueError("repeated thread")
                if not isinstance(candidate, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", candidate):
                    raise ValueError("thread id")
                thread_id = candidate
            elif kind == "turn.started":
                if thread_id is None:
                    raise ValueError("missing thread")
                if started:
                    raise ValueError("repeated start")
                started = True
            elif kind == "turn.completed":
                if thread_id is None:
                    raise ValueError("missing thread")
                if not started:
                    raise ValueError("before turn")
                if completed:
                    raise ValueError("repeated completion")
                completed = True
                raw_usage = event.get("usage")
                if not isinstance(raw_usage, dict):
                    raise ValueError("usage")
                usage = {}
                for name in ("input_tokens", "cached_input_tokens", "output_tokens"):
                    value = raw_usage.get(name)
                    if type(value) is not int or not 0 <= value <= 2**63 - 1:
                        raise ValueError("usage counter")
                    usage[name] = value
                reasoning = raw_usage.get("reasoning_output_tokens")
                if reasoning is not None and (type(reasoning) is not int or not 0 <= reasoning <= 2**63 - 1):
                    raise ValueError("reasoning counter")
                usage["reasoning_output_tokens"] = reasoning
                if usage["cached_input_tokens"] > usage["input_tokens"]:
                    raise ValueError("cached usage")
            elif kind in {"item.started", "item.updated", "item.completed"}:
                item = event.get("item")
                if not isinstance(item, dict):
                    raise ValueError("item object")
                item_type = item.get("type")
                if item_type == "error":
                    # Codex emits configuration/runtime/deprecation warnings as
                    # completed error *items*, even outside a turn. Its JSONL
                    # processor distinguishes these from fatal top-level errors.
                    message = item.get("message")
                    if kind != "item.completed" or not isinstance(message, str) or len(message.encode("utf-8")) > MAX_FINAL_BYTES:
                        raise ValueError("warning item")
                    # The CLI maps ModelRerouted to this exact warning prefix.
                    # Do not report the requested model for another model's work.
                    if message.startswith("model rerouted:"):
                        raise CodexError("Codex rerouted generation to another model", category="model_rerouted")
                    continue
                if thread_id is None:
                    raise ValueError("missing thread")
                if not started:
                    raise ValueError("before turn")
                if completed:
                    raise ValueError("after turn")
                if item_type == "mcp_tool_call":
                    if item.get("server") != MCP_SERVER or item.get("tool") not in TOOL_NAMES:
                        raise CodexError("Codex used a tool outside the demo's documentation tools", category="tool_scope")
                    if kind == "item.completed" and item.get("status") != "completed":
                        raise CodexError("Codex could not complete a documentation tool call", category="tool_failure")
                elif item_type == "agent_message":
                    if kind == "item.completed":
                        value = item.get("text")
                        if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_FINAL_BYTES:
                            raise ValueError("message")
                        final = value
                elif item_type != "reasoning":
                    raise CodexError("Codex used a tool outside the demo's documentation tools", category="item_type")
            elif kind in {"error", "turn.failed"}:
                message = event.get("message") if kind == "error" else event.get("error", {})
                if isinstance(message, dict):
                    message = message.get("message")
                category = _diagnostic_category([message] if isinstance(message, str) else [], default="provider")
                raise CodexError("Codex reported a failed generation", category=category)
            else:
                raise ValueError("event type")
        if not completed or not final or usage is None:
            raise ValueError("incomplete turn")
        return {"text": final, "thread_id": thread_id, "usage": usage}
    except (ValueError, UnicodeError, RecursionError) as error:
        if isinstance(error, CodexError):
            raise
        if isinstance(error, UnicodeError):
            category = "event_encoding"
        elif isinstance(error, (json.JSONDecodeError, RecursionError)):
            category = "event_json"
        else:
            category = {
                "duplicate key": "event_json", "non-finite constant": "event_json",
                "event count": "event_count", "event object": "event_shape",
                "thread id": "event_shape", "item object": "event_shape",
                "warning item": "event_shape",
                "turn order": "event_order", "item order": "event_order",
                "before turn": "event_before_turn", "after turn": "event_after_turn",
                "repeated start": "event_repeated_start",
                "repeated completion": "event_repeated_completion",
                "missing thread": "event_missing_thread", "repeated thread": "event_repeated_thread",
                "usage": "event_usage", "usage counter": "event_usage",
                "reasoning counter": "event_usage", "cached usage": "event_usage",
                "message": "event_message", "event type": "event_type",
                "incomplete turn": "event_incomplete",
            }.get(str(error), "event_json")
        raise CodexError("Codex returned an invalid or incomplete event stream", category=category) from None


def _settings(tool_server, audit_path, workdir):
    settings = {
        "forced_login_method": "chatgpt",
        "model_provider": "openai",
        "model_reasoning_effort": "low",
        "approval_policy": "never",
        "web_search": "disabled",
        "project_doc_max_bytes": 0,
        "skills.include_instructions": False,
        "skills.bundled.enabled": False,
        "features.skip_host_skill_discovery": True,
        "features.skill_search": False,
        "features.shell_tool": False,
        "features.unified_exec": False,
        "features.shell_snapshot": False,
        "features.apps": False,
        "features.plugins": False,
        "features.remote_plugin": False,
        "features.browser_use": False,
        "features.computer_use": False,
        "features.image_generation": False,
        "features.view_image": False,
        "features.multi_agent": False,
        "features.multi_agent_v2": False,
        "features.memories": False,
        "features.code_mode": False,
        "features.code_mode_only": False,
        "features.code_mode_host": False,
        "features.hooks": False,
        "features.goals": False,
        "features.sleep_tool": False,
        "features.tool_suggest": False,
        "features.workspace_dependencies": False,
        "features.unbounded_connection_retries": False,
        "tools.update_plan.enabled": False,
        "history.persistence": "none",
        "analytics.enabled": False,
        "otel.exporter": "none",
        "otel.trace_exporter": "none",
        "otel.metrics_exporter": "none",
        "mcp_servers.ledgence_docs.command": sys.executable,
        "mcp_servers.ledgence_docs.args": ["-I", str(tool_server), "--audit-path", str(audit_path)],
        "mcp_servers.ledgence_docs.cwd": str(workdir),
        "mcp_servers.ledgence_docs.enabled_tools": sorted(TOOL_NAMES),
        "mcp_servers.ledgence_docs.required": True,
        "mcp_servers.ledgence_docs.startup_timeout_sec": 10,
        "mcp_servers.ledgence_docs.tool_timeout_sec": 10,
    }
    arguments = []
    for name, value in settings.items():
        # JSON strings, arrays and booleans are also valid TOML values. No shell
        # interprets these argv entries, even when a path contains metacharacters.
        arguments.extend(["-c", f"{name}={json.dumps(value, ensure_ascii=False)}"])
    return arguments


def run_codex(prompt, schema, tool_server, audit_path, workdir, timeout=120.0, *, model="gpt-6-luna"):
    """Run one CLI generation; return only final text and observed CLI metrics."""
    executable = os.environ.get("LEDGENCE_CODEX_BIN", "")
    binary = Path(executable)
    if not executable or not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise CodexError("Set LEDGENCE_CODEX_BIN to an absolute executable Codex CLI path")
    if not isinstance(prompt, str) or not isinstance(schema, dict):
        raise CodexError("Codex prompt and output schema are invalid")
    try:
        encoded = prompt.encode("utf-8")
    except UnicodeError:
        raise CodexError("Codex prompt must be valid Unicode") from None
    try:
        schema_json = json.dumps(schema, ensure_ascii=False, allow_nan=False)
        if len(schema_json.encode("utf-8")) > MAX_FINAL_BYTES:
            raise ValueError("schema size")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise CodexError("Codex output schema must be a bounded JSON object") from None
    if not encoded or len(encoded) > MAX_PROMPT_BYTES:
        raise CodexError("Codex prompt exceeds the demo's size limit")
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise CodexError("Codex execution timeout must be between zero and 120 seconds")
    if not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", model):
        raise CodexError("Codex model must be a simple model identifier")
    directory = Path(workdir).resolve()
    server = Path(tool_server).resolve()
    audit = Path(audit_path).resolve()
    if not directory.is_dir() or not server.is_file() or audit.parent != directory:
        raise CodexError("Codex invocation paths are invalid")
    deadline = time.monotonic() + timeout
    environment = _environment()
    code, raw_version, _ = _collect(
        [str(binary), "--version"], environment=environment,
        directory=directory, deadline=min(deadline, time.monotonic() + 10),
    )
    try:
        match = re.fullmatch(r"codex-cli ([0-9]+\.[0-9]+\.[0-9]+(?:[-+.][A-Za-z0-9.+-]+)?)\s*", raw_version.decode("ascii"))
    except UnicodeError:
        match = None
    if code or match is None:
        raise CodexError("The configured executable is not a supported Codex CLI")
    version = match.group(1)
    schema_path = directory / "codex-output-schema.json"
    schema_created = False
    try:
        with schema_path.open("x", encoding="utf-8") as output:
            schema_created = True
            output.write(schema_json)
        command = [
            str(binary), "exec", "--strict-config", "--ignore-user-config",
            "--ignore-rules", "--ephemeral", "--skip-git-repo-check",
            "--sandbox", "read-only", "--json", "--color", "never",
            "--model", model, "--cd", str(directory),
            "--output-schema", str(schema_path),
            *_settings(server, audit, directory), "-",
        ]
        code, stdout, stderr = _collect(
            command, environment=environment, directory=directory,
            deadline=deadline, data=encoded,
        )
        if code:
            raise CodexError("Codex CLI exited before completing generation", category=_failure_category(stdout, stderr))
        result = _parse_events(stdout)
        result["cli_version"] = version
        return result
    except OSError:
        raise CodexError("Could not prepare the Codex output schema") from None
    finally:
        # The containing invocation directory is owned by the caller. Avoid
        # deleting a pre-existing file if exclusive creation failed.
        if schema_created:
            schema_path.unlink(missing_ok=True)
