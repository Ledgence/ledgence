"""Bounded whole-run replay for claude-agent-sdk 0.2.180 (MIT).

``spec.configuration['options']`` declares native option semantics. Factories
return a fresh client from ``bindings.client(options=...)``; no borrowed client,
implicit CLI selection, resume, host callback, or SDK MCP server is supported.

The source-only CustomTransport profile qualifies the Python SDK boundary. Live
CLI/provider execution and external-effect termination require separate evidence.
The complete terminal/usage/lifecycle envelope shares an 8 KiB durable bound, so
an output below 8 KiB can still exceed the envelope after framing. SDK float costs
remain in native evidence; they are not coerced into exact neutral decimal costs.
"""
import asyncio
import copy
from dataclasses import asdict, dataclass, fields, is_dataclass, replace
from decimal import Decimal, InvalidOperation
import hashlib
import importlib
import inspect
import math
import os
from urllib.parse import urlsplit

from ..workflow import OperationKind, _await_owned, _encode
from ._core import PreparedCall, freeze, start_run, text
from ._schema import validate_schema, validate_arguments
from ._types import (
    AdapterProfile, AgentCapability, AgentRunSpec, AgentStatus,
    AgentBindingError, AgentExecutionError, AgentIntegrationError, AgentLimitError, AgentProfileError,
    LifecycleEvidence, UsageCoverage, UsageObservation, UsageOrigin, UsageSource,
)

__all__ = ["PROFILE", "ExternalCli", "CustomTransport", "run"]

PROFILE = AdapterProfile("claude_agent_sdk.whole_run.v1", "1", frozenset({
    AgentCapability.RESULT_REPLAY, AgentCapability.REPORTED_USAGE,
    AgentCapability.STOP_REQUEST,
}))
_DEPENDENCIES = (("claude-agent-sdk", "0.2.180", "claude_agent_sdk"),)
_CODEC = "claude-agent-sdk/whole-run-v1"
_MAX_MESSAGES = 256
_MAX_OBSERVED_BYTES = 256 * 1024
_CREDENTIAL_HEADERS = frozenset({"authorization", "x-api-key", "api-key"})
_DEFAULTS = {
    "model": None, "system_prompt": None, "tools": None, "allowed_tools": [],
    "disallowed_tools": [], "max_turns": None, "max_budget_usd": None,
    "output_format": None, "mcp_servers": {}, "permission_mode": "dontAsk",
    "setting_sources": [], "strict_mcp_config": True,
}


@dataclass(frozen=True, slots=True)
class ExternalCli:
    """Explicit executable path and application-owned runtime identity."""
    path: str
    identity: str


@dataclass(frozen=True, slots=True)
class CustomTransport:
    """A synchronous, fresh ``Transport`` factory; it owns/drains its resources."""
    factory: object
    identity: str


def _runtime(runtime):
    if type(runtime) is ExternalCli:
        if (type(runtime.path) is not str or not os.path.isabs(runtime.path)
                or "\0" in runtime.path):
            raise AgentProfileError("explicit_cli_path_required")
        text(runtime.path, 4096)
        text(runtime.identity)
        return runtime, {"kind": "external_cli", "identity": runtime.identity, "path": runtime.path}
    if type(runtime) is CustomTransport:
        if not callable(runtime.factory) or inspect.iscoroutinefunction(runtime.factory):
            raise AgentProfileError("transport_factory_must_be_synchronous")
        text(runtime.identity)
        return runtime, {"kind": "custom_transport", "identity": runtime.identity}
    raise AgentProfileError("explicit_runtime_required")


def _strings(value, limit=32):
    if type(value) is not list or len(value) > limit:
        raise AgentProfileError("native_string_list_invalid")
    for item in value:
        text(item, 256)
    if len(set(value)) != len(value):
        raise AgentProfileError("native_string_list_invalid")
    return list(value)


def _number(value, *, integer=False):
    if value is None:
        return
    if (type(value) not in ((int,) if integer else (int, float))
            or (type(value) is float and not math.isfinite(value)) or value <= 0):
        raise AgentProfileError("native_limit_invalid")


def _mcp(servers, *, declared):
    if type(servers) is not dict or len(servers) > 16:
        raise AgentProfileError("external_mcp_configuration_required")
    projection = {}
    for name, config in servers.items():
        text(name)
        if type(config) is not dict:
            raise AgentProfileError("external_mcp_configuration_required")
        kind = config.get("type")
        if kind == "stdio":
            if set(config) - {"type", "command", "args"}:
                raise AgentProfileError("mcp_option_unsupported")
            text(config.get("command"), 1024)
            args = _strings(config.get("args", []))
            projection[name] = {"type": kind, "command": config["command"], "args": args}
        elif type(kind) is str and kind in {"http", "sse"}:
            allowed = {"type", "url", "credential_headers" if declared else "headers"}
            if set(config) - allowed:
                raise AgentProfileError("mcp_option_unsupported")
            url = config.get("url")
            text(url, 1024)
            try:
                parsed = urlsplit(url)
                if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                        or parsed.username is not None or parsed.password is not None
                        or parsed.query or parsed.fragment):
                    raise ValueError
            except ValueError as exc:
                raise AgentProfileError("mcp_endpoint_invalid") from exc
            if declared:
                keys = _strings(config.get("credential_headers", []))
                normalized = [key.lower() for key in keys]
            else:
                headers = config.get("headers", {})
                if type(headers) is not dict:
                    raise AgentProfileError("mcp_headers_unsupported")
                normalized = []
                for key, value in headers.items():
                    text(key)
                    text(value, 4096)
                    if any(character in value for character in "\r\n\0"):
                        raise AgentProfileError("mcp_credential_invalid")
                    normalized.append(key.lower())
            if len(set(normalized)) != len(normalized) or set(normalized) - _CREDENTIAL_HEADERS:
                raise AgentProfileError("mcp_headers_unsupported")
            projection[name] = {"type": kind, "url": url, "credential_headers": sorted(normalized)}
        else:
            raise AgentProfileError("external_mcp_type_unsupported")
    return projection


def _options_projection(options, *, declared, sdk=None):
    if declared:
        if type(options) is not dict or set(options) - set(_DEFAULTS):
            raise AgentProfileError("native_options_unsupported")
        values = copy.deepcopy(_DEFAULTS)
        values.update(freeze(options, 4096))
    else:
        if type(options) is not sdk.ClaudeAgentOptions:
            raise AgentProfileError("native_options_type_unsupported")
        defaults = sdk.ClaudeAgentOptions()
        for field in fields(defaults):
            name = field.name
            if name in _DEFAULTS or name == "cli_path":
                continue
            value, default = getattr(options, name), getattr(defaults, name)
            if name == "hooks" and (value is None or (type(value) is dict and not value)):
                continue
            if name == "debug_stderr":
                matches = value is default
            else:
                matches = type(value) is type(default) and value == default
            if not matches:
                raise AgentProfileError("native_option_unsupported")
        values = {name: getattr(options, name) for name in _DEFAULTS}
    text(values["model"], 256)
    if values["system_prompt"] is not None:
        if type(values["system_prompt"]) is not str or len(values["system_prompt"].encode("utf-8")) > 4096:
            raise AgentProfileError("text_system_prompt_required")
    for name in ("tools", "allowed_tools", "disallowed_tools"):
        values[name] = _strings(values[name])
    if type(values["permission_mode"]) is not str or values["permission_mode"] not in {"dontAsk", "bypassPermissions"}:
        raise AgentProfileError("noninteractive_permission_mode_required")
    if values["setting_sources"] != [] or values["strict_mcp_config"] is not True:
        raise AgentProfileError("explicit_native_configuration_required")
    _number(values["max_turns"], integer=True)
    _number(values["max_budget_usd"])
    output = values["output_format"]
    if output is not None:
        if type(output) is not dict or set(output) != {"type", "schema"} or output["type"] != "json_schema":
            raise AgentProfileError("output_format_unsupported")
        values["output_format"] = {"type": "json_schema", "schema": validate_schema(output["schema"])}
    values["mcp_servers"] = _mcp(values["mcp_servers"], declared=declared)
    return freeze(values, 4096)


def _declared(configuration):
    if type(configuration) is not dict or "options" not in configuration:
        raise AgentProfileError("declared_native_options_required")
    return _options_projection(configuration["options"], declared=True)


class _OwnedTransport:
    """One retained close attempt, including pre-query connect failures."""
    def __init__(self, transport):
        self.transport = transport
        self.close_started = self.closed = False
        self.close_error = None

    async def connect(self): await self.transport.connect()
    async def write(self, data): await self.transport.write(data)
    def read_messages(self): return self.transport.read_messages()
    async def end_input(self): await self.transport.end_input()
    def is_ready(self): return self.transport.is_ready()

    async def close(self):
        if self.close_started:
            return
        self.close_started = True
        try:
            await self.transport.close()
            self.closed = True
        except BaseException as exc:
            # Let the SDK finish closing its receive stream even when the
            # transport failed. The executor raises this retained error below;
            # it never accepts a result or reports transport closure on failure.
            self.close_error = exc


class _Bindings:
    __slots__ = ("_session", "_configuration", "_anchor")

    def __init__(self, session):
        self._session = session
        self._configuration = session.state.configuration
        self._anchor = freeze(session.state.anchor, 4096)

    @property
    def configuration(self): return freeze(self._configuration, 4096)
    @property
    def anchor(self): return freeze(self._anchor, 4096)

    def client(self, *, options=None):
        session, sdk = self._session, self._session.sdk
        if session.client is not None:
            raise AgentProfileError("one_owned_client_required")
        declared = _declared(self.configuration)
        if options is None:
            values = copy.deepcopy(declared)
            for config in values["mcp_servers"].values():
                if config.get("credential_headers"):
                    raise AgentBindingError("mcp_credentials_required")
                config.pop("credential_headers", None)
            options = sdk.ClaudeAgentOptions(**values)
        projection = _options_projection(options, declared=False, sdk=sdk)
        if projection != declared:
            raise AgentBindingError("native_options_differ_from_manifest")
        if options.cli_path is not None:
            raise AgentProfileError("runtime_owns_cli_selection")
        # Reconstruct only reviewed fields. Mutable caller options are not live
        # configuration; credential header values stay outside the journal.
        values = {name: copy.deepcopy(getattr(options, name)) for name in _DEFAULTS}
        runtime = session.runtime
        if type(runtime) is ExternalCli:
            values["cli_path"] = runtime.path
            actual = sdk.ClaudeAgentOptions(**values)
            # The public SDK owns its standard transport. A completed disconnect
            # is not evidence that an unqualified partial CLI open was contained.
            session.client = sdk.ClaudeSDKClient(options=actual)
        else:
            values["cli_path"] = "/nonexistent/ledgence-explicit-custom-transport"
            actual = sdk.ClaudeAgentOptions(**values)
            transport = runtime.factory()
            if inspect.isawaitable(transport):
                if inspect.iscoroutine(transport): transport.close()
                raise AgentProfileError("transport_factory_must_be_synchronous")
            if not isinstance(transport, sdk.Transport):
                raise AgentProfileError("sdk_transport_required")
            if getattr(transport, "_ledgence_agent_owned", False) or transport.is_ready() is not False:
                raise AgentProfileError("fresh_transport_required")
            # Mark ownership on the application-provided object without retaining
            # it in process-global state. A closed prior transport is still used.
            transport._ledgence_agent_owned = True
            session.transport = _OwnedTransport(transport)
            session.client = sdk.ClaudeSDKClient(options=actual, transport=session.transport)
        session.options = declared
        session.cli_path = actual.cli_path
        return session.client


def _counter(value):
    if value is not None and (type(value) is not int or not 0 <= value < 1 << 64):
        raise AgentProfileError("reported_usage_invalid")
    return value


def _cost(value):
    if value is None:
        return None, False
    if type(value) is float:
        if not math.isfinite(value) or value < 0:
            raise AgentProfileError("reported_cost_invalid")
        return None, True  # No invented decimal precision from an SDK float.
    if type(value) is int:
        value = str(value)
    if type(value) is not str or not value.isascii() or not 1 <= len(value) <= 64:
        raise AgentProfileError("reported_cost_invalid")
    try:
        decimal = Decimal(value)
        if not decimal.is_finite() or decimal < 0:
            raise ValueError
    except (ValueError, InvalidOperation) as exc:
        raise AgentProfileError("reported_cost_invalid") from exc
    return value, False


def _usage(state, slot, source, identifier, usage, cost=None):
    if usage is not None and type(usage) is not dict:
        raise AgentProfileError("reported_usage_invalid")
    values = usage or {}
    for name in ("input_tokens", "output_tokens", "total_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
        _counter(values.get(name))
    reported_cost, imprecise = _cost(cost)
    input_tokens, output_tokens = values.get("input_tokens"), values.get("output_tokens")
    coverage = UsageCoverage.REPORTED if input_tokens is not None and output_tokens is not None else UsageCoverage.PARTIAL
    if not values and cost is None:
        coverage = UsageCoverage.UNKNOWN
    if imprecise:
        coverage = UsageCoverage.PARTIAL
    state.observe_usage(UsageObservation(slot, source, text(identifier, 256), UsageOrigin.CURRENT_DISPATCH,
                                        input_tokens, output_tokens, values.get("total_tokens"),
                                        reported_cost, "USD" if reported_cost is not None else None, coverage))


def _status(terminal):
    subtype, reason, failed = terminal.subtype, terminal.terminal_reason, terminal.is_error
    if type(failed) is not bool:
        raise AgentProfileError("terminal_error_flag_invalid")
    errors = {"error_max_turns", "error_max_budget_usd", "error_max_structured_output_retries", "error_during_execution"}
    if type(subtype) is not str or subtype not in errors | {"success"}:
        raise AgentProfileError("terminal_subtype_unsupported")
    if (reason is not None and type(reason) is not str) or reason not in {None, "completed", "max_turns", "max_budget_usd", "max_structured_output_retries", "api_error", "aborted_streaming", "aborted_tools"}:
        raise AgentProfileError("terminal_reason_unsupported")
    if reason in {"aborted_streaming", "aborted_tools"}:
        return AgentStatus.ABORTED
    if subtype == "success" and not failed and reason in {None, "completed"}:
        return AgentStatus.SUCCEEDED
    if failed:
        return AgentStatus.FAILED
    raise AgentProfileError("terminal_fields_inconsistent")


def _terminal(terminal):
    for name in ("duration_ms", "duration_api_ms", "num_turns"):
        _counter(getattr(terminal, name))
    text(terminal.session_id, 256)
    if terminal.uuid is not None: text(terminal.uuid, 256)
    if terminal.deferred_tool_use is not None:
        raise AgentProfileError("deferred_terminal_unsupported")
    if terminal.result is not None and type(terminal.result) is not str:
        raise AgentProfileError("terminal_output_invalid")
    _cost(terminal.total_cost_usd)
    if terminal.model_usage is not None:
        if type(terminal.model_usage) is not dict:
            raise AgentProfileError("reported_usage_invalid")
        for model, usage in terminal.model_usage.items():
            text(model, 256)
            if type(usage) is not dict: raise AgentProfileError("reported_usage_invalid")
            for name in ("inputTokens", "outputTokens", "cacheReadInputTokens", "cacheCreationInputTokens", "webSearchRequests"):
                _counter(usage.get(name))
            _cost(usage.get("costUSD"))
    return _status(terminal)


def _usage_wire(observation):
    value = asdict(observation)
    for name in ("source_kind", "origin", "coverage"):
        value[name] = value[name].value
    return value


def _lifecycle_wire(evidence):
    value = asdict(evidence)
    value["origin"] = evidence.origin.value
    value["terminal_status"] = evidence.terminal_status.value if evidence.terminal_status else None
    return value


class _Codec:
    @staticmethod
    def encode(value): return value
    @staticmethod
    def decode(value): return value


class _Session:
    def __init__(self, state, factory, runtime):
        self.state, self.factory, self.runtime = state, factory, runtime
        self.sdk = None
        self.client = self.transport = self.driver = self.control = None
        self.options = self.cli_path = None
        self.connected = self.closing = self.cancelled_driver = False

    async def open(self):
        # Construction and acquisition belong to the retained whole-run executor.
        pass

    def _check_client(self):
        if (type(self.client) is not self.sdk.ClaudeSDKClient
                or self.client.options.cli_path != self.cli_path
                or _options_projection(self.client.options, declared=False, sdk=self.sdk) != self.options):
            raise AgentBindingError("native_client_changed")

    async def _interrupt(self):
        await self.client.interrupt()
        self.state.lifecycle = replace(self.state.lifecycle, stop_acknowledged=True)

    async def request_stop(self):
        if self.closing:
            return
        if self.driver is not None and not self.driver.done() and not self.cancelled_driver:
            self.cancelled_driver = True
            self.driver.cancel()
        if self.connected and self.control is None:
            self.control = asyncio.create_task(self._interrupt())
        if self.control is not None:
            await _await_owned(self.control)

    async def _drive(self, slot):
        self.state.check()
        self._check_client()
        await self.client.query(self.state.prompt, session_id=self.state.anchor["conversation_id"])
        count, observed = 0, 0
        async for message in self.client.receive_response():
            if not is_dataclass(message) or type(message).__module__ != "claude_agent_sdk.types":
                raise AgentProfileError("native_message_unsupported")
            value = freeze(asdict(message), _MAX_OBSERVED_BYTES)
            observed += len(_encode(value, _MAX_OBSERVED_BYTES))
            count += 1
            if count > _MAX_MESSAGES or observed > _MAX_OBSERVED_BYTES:
                raise AgentLimitError("observed_message_bound_exceeded")
            if isinstance(message, self.sdk.MirrorErrorMessage):
                raise self.state.poison(AgentExecutionError("session_mirror_failed"))
            if type(message) is self.sdk.AssistantMessage and message.usage is not None:
                identifier = message.message_id or message.uuid or hashlib.sha256(_encode(value, _MAX_OBSERVED_BYTES)).hexdigest()
                _usage(self.state, slot, UsageSource.ASSISTANT_MESSAGE, identifier, message.usage)
            if type(message) is self.sdk.ResultMessage:
                status = _terminal(message)
                _usage(self.state, slot, UsageSource.RUN_TOTAL, message.uuid or message.session_id,
                       message.usage, message.total_cost_usd)
                output = message.structured_output if message.structured_output is not None else message.result
                if status is AgentStatus.SUCCEEDED and self.options["output_format"] is not None:
                    validate_arguments(self.options["output_format"]["schema"], output)
                self.state.lifecycle = replace(self.state.lifecycle, terminal_status=status, session_id=message.session_id)
                return message, status
        raise AgentExecutionError("terminal_result_missing")

    async def _execute(self, *, request):
        # The copied effective envelope is checked before invoking user code.
        if request["arguments"] != {"prompt": self.state.prompt, "options": _declared(self.state.configuration)}:
            raise AgentBindingError("whole_run_request_changed")
        self.sdk = importlib.import_module("claude_agent_sdk")
        slot = self.state.slot("WHOLE_RUN")
        terminal = status = failure = None
        try:
            self.state.check()
            bindings = _Bindings(self)
            client = self.factory(bindings)
            if inspect.isawaitable(client):
                if inspect.iscoroutine(client): client.close()
                raise AgentProfileError("factory_must_be_synchronous")
            if client is not self.client or self.client is None:
                raise AgentProfileError("fresh_bound_client_required")
            self._check_client()
            await self.client.connect()
            self.connected = True
            if self.state.stopped:
                await self.request_stop()
            self.state.check()
            self.driver = asyncio.create_task(self._drive(slot))
            terminal, status = await self.driver
        except BaseException as exc:
            failure = exc
        finally:
            self.closing = True
            cleanup_errors = []
            if self.driver is not None:
                if not self.driver.done() and not self.cancelled_driver:
                    self.cancelled_driver = True
                    self.driver.cancel()
                await asyncio.gather(self.driver, return_exceptions=True)
            if self.control is not None:
                results = await asyncio.gather(self.control, return_exceptions=True)
                cleanup_errors.extend(error for error in results if isinstance(error, BaseException))
            if self.client is not None:
                try:
                    await self.client.disconnect()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if self.transport is not None:
                try:
                    await self.transport.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
                if self.transport.close_error is not None:
                    cleanup_errors.append(self.transport.close_error)
                self.state.lifecycle = replace(self.state.lifecycle, transport_closed=self.transport.closed)
            if cleanup_errors:
                error = AgentExecutionError("resource_close_failed", lifecycle=self.state.lifecycle,
                                            external_work_may_have_occurred=True)
                error.__cause__ = cleanup_errors[0]
                self.state.poison(error)
                raise error
        if failure is not None:
            if isinstance(failure, AgentIntegrationError) and failure.lifecycle is None:
                failure.lifecycle = self.state.lifecycle
            raise failure
        output = terminal.structured_output if terminal.structured_output is not None else terminal.result
        result = self.state.result(status, output if status is AgentStatus.SUCCEEDED else None)
        return freeze({"terminal": asdict(terminal), "status": status.value,
                       "usage": [_usage_wire(item) for item in result.usage],
                       "lifecycle": _lifecycle_wire(self.state.lifecycle)}, 8 * 1024)

    async def run(self):
        slot = self.state.slot("WHOLE_RUN")
        wire = await self.state.execute(PreparedCall(
            slot, OperationKind.TOOL, {"kind": "external_agent_run", "profile": PROFILE.name},
            {"prompt": self.state.prompt, "options": _declared(self.state.configuration)}, _CODEC,
        ), self._execute, _Codec(), category="whole_run")
        if type(wire) is not dict or set(wire) != {"terminal", "status", "usage", "lifecycle"}:
            raise AgentProfileError("whole_run_record_invalid")
        sdk = importlib.import_module("claude_agent_sdk")
        if type(wire["terminal"]) is not dict or set(wire["terminal"]) != {field.name for field in fields(sdk.ResultMessage)}:
            raise AgentProfileError("terminal_record_invalid")
        native = sdk.ResultMessage(**wire["terminal"])
        status = _terminal(native)
        if wire["status"] != status.value:
            raise AgentProfileError("terminal_record_inconsistent")
        replayed = self.state.replayed(slot)
        origin = UsageOrigin.ACCEPTED_REPLAY if replayed else UsageOrigin.CURRENT_DISPATCH
        if type(wire["usage"]) is not list or len(wire["usage"]) > _MAX_MESSAGES:
            raise AgentProfileError("usage_record_invalid")
        for item in wire["usage"]:
            if type(item) is not dict or item.get("operation_key") != slot or item.get("origin") != UsageOrigin.CURRENT_DISPATCH.value:
                raise AgentProfileError("usage_record_invalid")
            values = dict(item)
            values.update(source_kind=UsageSource(values["source_kind"]), origin=origin, coverage=UsageCoverage(values["coverage"]))
            self.state.observe_usage(UsageObservation(**values))
        evidence = dict(wire["lifecycle"])
        if evidence.get("origin") != UsageOrigin.CURRENT_DISPATCH.value or evidence.get("terminal_status") != status.value:
            raise AgentProfileError("lifecycle_record_invalid")
        evidence.update(origin=origin, terminal_status=status)
        lifecycle = LifecycleEvidence(**evidence)
        output = native.structured_output if native.structured_output is not None else native.result
        return self.state.result(status, output if status is AgentStatus.SUCCEEDED else None, native, lifecycle=lifecycle)

    async def close(self):
        # The retained operation closes in its opening task before journal commit.
        pass


def run(ctx, *, spec, prompt, factory, runtime, require=frozenset()):
    """Synchronously register one owned run; await its whole-run accepted result."""
    runtime, identity = _runtime(runtime)
    if type(spec) is not AgentRunSpec:
        raise AgentProfileError("invalid_run_spec")
    configuration = freeze(spec.configuration, 4096)
    configuration["options"] = _declared(configuration)
    spec = replace(spec, configuration=freeze(configuration, 4096))
    return start_run(ctx, spec=spec, prompt=prompt, factory=factory, profile=PROFILE,
                     dependencies=_DEPENDENCIES, normalization="claude-whole-run-v1",
                     session_factory=lambda state, factory: _Session(state, factory, runtime),
                     require=require, runtime_identity=identity, whole_run=True)
