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


class CodexError(ValueError):
    """Fixed, safe diagnostics; never contains CLI output or credential values."""


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
                    raise CodexError("Codex exceeded the agent execution budget")
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
                        raise CodexError("Codex output exceeded the demo's size limit")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexError("Codex exceeded the agent execution budget")
            try:
                code = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                raise CodexError("Codex exceeded the agent execution budget") from None
            return code, bytes(output["stdout"]), bytes(output["stderr"])
    except OSError:
        raise CodexError("Could not start or communicate with the configured Codex CLI") from None
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
            if kind == "thread.started":
                candidate = event.get("thread_id")
                if thread_id is not None or not isinstance(candidate, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", candidate):
                    raise ValueError("thread id")
                thread_id = candidate
            elif kind == "turn.started":
                if thread_id is None or started:
                    raise ValueError("turn order")
                started = True
            elif kind == "turn.completed":
                if not started or completed:
                    raise ValueError("turn order")
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
                if not started or completed:
                    raise ValueError("item order")
                item = event.get("item")
                if not isinstance(item, dict):
                    raise ValueError("item object")
                item_type = item.get("type")
                if item_type == "mcp_tool_call":
                    if item.get("server") != MCP_SERVER or item.get("tool") not in TOOL_NAMES:
                        raise CodexError("Codex used a tool outside the demo's documentation tools")
                    if kind == "item.completed" and item.get("status") != "completed":
                        raise CodexError("Codex could not complete a documentation tool call")
                elif item_type == "agent_message":
                    if kind == "item.completed":
                        value = item.get("text")
                        if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_FINAL_BYTES:
                            raise ValueError("message")
                        final = value
                elif item_type != "reasoning":
                    raise CodexError("Codex used a tool outside the demo's documentation tools")
            elif kind in {"error", "turn.failed"}:
                raise CodexError("Codex generation failed; check CLI login, subscription limits and model availability")
            else:
                raise ValueError("event type")
        if not completed or not final or usage is None:
            raise ValueError("incomplete turn")
        return {"text": final, "thread_id": thread_id, "usage": usage}
    except (ValueError, UnicodeError, RecursionError) as error:
        if isinstance(error, CodexError):
            raise
        raise CodexError("Codex returned an invalid or incomplete event stream") from None


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
        code, stdout, _ = _collect(
            command, environment=environment, directory=directory,
            deadline=deadline, data=encoded,
        )
        if code:
            raise CodexError("Codex generation failed; check CLI login, subscription limits and model availability")
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
