"""Optional bounded Google ADK operations (MIT).

Qualified with google-adk 2.14.0 / google-genai 2.34.0. Factories construct fresh
LlmAgent instances using these bindings. Tools are declared read operations;
callbacks are explicitly versioned, pure before/after-tool transforms. Accepted
tool records contain final arguments and the original read result. After-tool
transforms run again on replay, and their result binds the next model request.
No SDK is imported until run() checks the installed, owned distributions.
"""
import asyncio
import inspect
import re
from contextlib import aclosing

from ..workflow import OperationKind
from ._catalog import CatalogSpec, ReadSpec, callable_identity
from ._core import PreparedCall, freeze, start_run, text
from ._schema import effective_arguments, schema_for_callable, validate_arguments
from ._types import (
    AdapterProfile, AgentCapability, AgentStatus, AgentProfileError,
    AgentBindingError, AgentExecutionError, AgentIntegrationError, UsageObservation, UsageSource,
    UsageOrigin, UsageCoverage,
)

PROFILE = AdapterProfile("google_adk.bounded_operations.v1", "1", frozenset({
    AgentCapability.MODEL_REPLAY, AgentCapability.TOOL_REPLAY,
    AgentCapability.DISCOVERY_REPLAY, AgentCapability.REPORTED_USAGE,
}))
_DEPENDENCIES = (("google-adk", "2.14.0", "google.adk"),
                 ("google-genai", "2.34.0", "google.genai"))


def run(ctx, *, spec, prompt, factory, require=frozenset()):
    """Claim one invocation occurrence and return its retained awaitable result."""
    return start_run(ctx, spec=spec, prompt=prompt, factory=factory, require=require,
                     profile=PROFILE, dependencies=_DEPENDENCIES,
                     normalization="google-adk-json-causal-v1", session_factory=_Session)


def _dump(value):
    return value.model_dump(mode="json", exclude_none=True)


def _has_actions(actions):
    return any(value not in (None, {}, []) for value in _dump(actions).values())


def _name(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", value) is None:
        raise AgentProfileError("adk_tool_name_unsupported")
    return value


class _Bindings:
    def __init__(self, session):
        self.session = session

    def check(self):
        if not self.session.accepting_bindings:
            raise self.session.fail(AgentProfileError("adk_bindings_closed"))

    @property
    def configuration(self):
        return self.session.state.configuration

    def model(self, native, *, identity, configuration, close):
        self.check()
        s = self.session
        if (not isinstance(native, s.BaseLlm) or any(native is item[1] for item in s.models)
                or type(configuration) is not dict):
            raise AgentProfileError("adk_model_binding_invalid")
        if close is not None and not (inspect.iscoroutinefunction(close)
                or inspect.iscoroutinefunction(getattr(close, "__call__", None))):
            raise AgentProfileError("adk_model_closer_invalid")
        binding = {"identity": text(identity), "configuration": freeze(configuration, 4096),
                   "model": text(native.model)}
        # Ownership transfers at successful binding, even if later factory work fails.
        model = s.BoundModel(model=native.model)
        s.models.append((model, native, binding))
        if close is not None:
            s.closers.append(close)
        return model

    def read_tool(self, identity, function, *, version):
        self.check()
        s = self.session
        read = ReadSpec.create(identity, function, version)
        schema = schema_for_callable(function)
        return s.make_tool(_name(function.__name__), schema,
                           {"kind": "read", **read.binding()}, function=function)

    def catalog(self, identity, *, version, discover, registry):
        self.check()
        s = self.session
        catalog = CatalogSpec(s.state, identity, version=version, discover=discover, registry=registry)
        toolset = s.BoundToolset(catalog)
        s.toolsets.append(toolset)
        return toolset

    def transform(self, identity, callback, *, version):
        self.check()
        s = self.session
        binding = {"identity": text(identity), "version": text(version),
                   "callable": callable_identity(callback)}
        if id(callback) in s.transforms:
            raise AgentProfileError("adk_transform_duplicate")
        s.transforms[id(callback)] = (callback, binding)
        return callback


class _Session:
    """Resource-free until open; all acquisition and closure run on the owner."""
    def __init__(self, state, factory):
        self.state, self.factory = state, factory
        self.runner = self.sessions = self.agent = None
        self.delegate = self.model = None
        self.model_binding = None
        self.models = []
        self.closers, self.tools, self.toolsets, self.transforms = [], {}, [], {}
        self.hooks = {"before": [], "after": []}
        self.previous = "START"
        self.model_contents = []
        self.pending = {}
        self.model_active = False
        self.last_request = None
        self.accepting_bindings = False
        self.tool_specs = {}

    async def open(self):
        from google.adk.agents import LlmAgent
        from google.adk.models.base_llm import BaseLlm
        from google.adk.models.llm_request import LlmRequest
        from google.adk.models.llm_response import LlmResponse
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.adk.tools.base_tool import BaseTool
        from google.adk.tools.base_toolset import BaseToolset
        from google.genai import types

        s = self
        self.BaseLlm, self.LlmRequest, self.LlmResponse, self.types = BaseLlm, LlmRequest, LlmResponse, types

        class BoundModel(BaseLlm):
            async def generate_content_async(self, llm_request, stream=False):
                if self is not s.model:
                    raise s.fail(AgentProfileError("adk_inactive_model"))
                yield await s.generate(llm_request, stream=stream)

        class BoundTool(BaseTool):
            def __init__(self, name, schema, binding, function, catalog):
                super().__init__(name=name, description="Declared read operation")
                self.schema, self.binding = schema, binding
                self.function, self.catalog = function, catalog

            async def process_llm_request(self, *, tool_context, llm_request):
                s.check_context(tool_context)
                s.check_tool(self)
                declaration = types.FunctionDeclaration(name=self.name, description=self.description,
                                                        parameters_json_schema=freeze(self.schema))
                llm_request.config.tools = list(llm_request.config.tools or []) + [
                    types.Tool(function_declarations=[declaration])]
                llm_request.tools_dict[self.name] = self

            async def run_async(self, *, args, tool_context):
                return await s.execute_tool(self, args, tool_context)

        class BoundToolset(BaseToolset):
            def __init__(self, catalog):
                super().__init__()
                self.catalog, self.snapshot_tools = catalog, None

            async def get_tools(self, readonly_context=None):
                s.state.check()
                try:
                    if self.snapshot_tools is None:
                        descriptors = await self.catalog.snapshot()
                        self.snapshot_tools = [s.make_tool(_name(item["name"]), item["input_schema"],
                            {"kind": "catalog_read", "catalog": self.catalog.identity,
                             "catalog_version": self.catalog.version, "descriptor": item,
                             "executor": self.catalog.registry_identities[item["executor_id"]]},
                            catalog=self.catalog) for item in descriptors]
                    return list(self.snapshot_tools)
                except BaseException as exc:
                    raise s.fail(exc)

            async def close(self):
                pass

        self.BoundModel, self.BoundTool, self.BoundToolset = BoundModel, BoundTool, BoundToolset
        self.accepting_bindings = True
        try:
            self.agent = self.factory(_Bindings(self))
        finally:
            self.accepting_bindings = False
        if inspect.isawaitable(self.agent):
            if inspect.iscoroutine(self.agent):
                self.agent.close()
            raise AgentProfileError("adk_factory_must_be_synchronous")
        if type(self.agent) is not LlmAgent:
            raise AgentProfileError("adk_agent_binding_invalid")
        selected = next((item for item in self.models if item[0] is self.agent.model), None)
        if selected is None:
            raise AgentProfileError("adk_agent_binding_invalid")
        self.model, self.delegate, self.model_binding = selected
        self.validate_agent()
        self.sessions = InMemorySessionService()
        self.session_id = self.state.anchor["conversation_id"]
        await self.sessions.create_session(app_name="ledgence", user_id="ledgence", session_id=self.session_id)
        self.runner = Runner(agent=self.agent, app_name="ledgence", session_service=self.sessions)

    def make_tool(self, name, schema, binding, *, function=None, catalog=None):
        if name in self.tools:
            raise AgentProfileError("adk_duplicate_tool_name")
        tool = self.BoundTool(name, freeze(schema), freeze(binding), function, catalog)
        self.tools[name] = tool
        self.tool_specs[name] = (freeze(schema), freeze(binding), function, catalog)
        return tool

    def validate_agent(self):
        agent = self.agent
        allowed = {"name", "description", "model", "instruction", "global_instruction", "tools",
                   "generate_content_config", "before_tool_callback", "after_tool_callback"}
        for name, field in type(agent).model_fields.items():
            if name not in allowed:
                default = field.default_factory() if field.default_factory else field.default
                if getattr(agent, name) != default:
                    raise AgentProfileError("adk_agent_option_unsupported")
        if any(type(getattr(agent, field)) is not str for field in ("instruction", "global_instruction")):
            raise AgentProfileError("adk_instruction_provider_unsupported")
        if any(not (any(tool is own for own in self.toolsets) or any(tool is own for own in self.tools.values()))
               for tool in agent.tools):
            raise AgentProfileError("adk_unbound_tool")
        for toolset in self.toolsets:
            if toolset.tool_filter is not None or toolset.tool_name_prefix is not None:
                raise AgentProfileError("adk_catalog_option_unsupported")
        if agent.generate_content_config is not None:
            config = _dump(agent.generate_content_config)
            if (any(config.get(key) for key in ("tools", "cached_content", "http_options", "routing_config",
                    "speech_config", "audio_timestamp", "image_config", "media_resolution"))
                    or config.get("candidate_count", 1) != 1
                    or config.get("response_modalities", ["TEXT"]) != ["TEXT"]):
                raise AgentProfileError("adk_model_config_unsupported")
            freeze(config)
        self.agent_binding = freeze({"name": agent.name, "description": agent.description,
            "instruction": agent.instruction, "global_instruction": agent.global_instruction})
        for placement in ("before", "after"):
            original = getattr(agent, placement + "_tool_callback")
            callbacks = original if type(original) is list else ([] if original is None else [original])
            wrappers = []
            for callback in callbacks:
                declared = self.transforms.get(id(callback))
                if declared is None or declared[0] is not callback:
                    raise AgentProfileError("adk_undeclared_transform")
                self.hooks[placement].append(declared[1])
                wrappers.append(self.wrap_transform(callback, placement))
            setattr(agent, placement + "_tool_callback", wrappers or None)

    def fail(self, error):
        if not isinstance(error, (AgentIntegrationError, asyncio.CancelledError)):
            cause = error
            error = AgentExecutionError("adk_operation_failed", external_work_may_have_occurred=True)
            error.__cause__ = cause
        return self.state.poison(error)

    def check_tool(self, tool):
        owned = self.tools.get(tool.name)
        expected = self.tool_specs.get(tool.name)
        if (owned is not tool or expected is None or tool.schema != expected[0]
                or tool.binding != expected[1] or tool.function is not expected[2]
                or tool.catalog is not expected[3] or tool.description != "Declared read operation"
                or tool.is_long_running or tool.title is not None
                or tool.custom_metadata is not None or tool.behavior is not None
                or tool.response_scheduling is not None):
            raise self.fail(AgentProfileError("adk_tool_definition_changed"))

    def check_context(self, context):
        # This profile always starts with empty state. Compare public authoritative
        # state as well as deltas, including nested writes and action-only changes.
        if (context.state.to_dict() != {} or context.state.has_delta()
                or _has_actions(context.actions)):
            raise self.state.poison(AgentProfileError("adk_state_or_actions_unsupported"))

    def wrap_transform(self, callback, placement):
        async def transform(*, tool, args, tool_context, tool_response=None):
            self.state.check()
            self.check_context(tool_context)
            self.check_tool(tool)
            kwargs = {"tool": tool, "args": args, "tool_context": tool_context}
            if placement == "after":
                kwargs["tool_response"] = tool_response
            try:
                value = callback(**kwargs)
                if inspect.isawaitable(value):
                    value = await value
                if placement == "before":
                    if value is not None:
                        raise AgentProfileError("adk_tool_short_circuit_unsupported")
                    freeze(args)
                else:
                    effective = tool_response if value is None else value
                    freeze(effective, 8192)
                    if type(effective) is not dict:
                        raise AgentProfileError("adk_transform_result_invalid")
                    self.pending[tool_context.function_call_id]["response"] = freeze(effective, 8192)
                return value
            except BaseException as exc:
                self.check_context(tool_context)
                raise self.fail(exc)
            finally:
                self.check_context(tool_context)
                self.check_tool(tool)
        return transform

    async def execute_tool(self, tool, args, context):
        self.state.check()
        self.check_context(context)
        self.check_tool(tool)
        try:
            pending = self.pending.get(context.function_call_id)
            if pending is None or pending["name"] != tool.name or pending["done"]:
                raise AgentBindingError("adk_tool_lineage_invalid")
            arguments = (effective_arguments(tool.function, args) if tool.function is not None
                         else validate_arguments(tool.schema, args))
            target = {**tool.binding, "schema": tool.schema, "transforms": self.hooks}
            async def execute(*, request):
                final = validate_arguments(tool.schema, request["arguments"])
                if tool.function is not None:
                    value = tool.function(**final)
                else:
                    descriptor = request["target"]["descriptor"]
                    value = tool.catalog.registry[descriptor["executor_id"]](
                        target=descriptor["target"], arguments=final)
                if inspect.isawaitable(value):
                    value = await value
                return value
            value = await self.state.execute(PreparedCall(pending["slot"], OperationKind.TOOL,
                target, arguments, "google-adk-read-json-v1"), execute, category="tool")
            pending["done"] = True
            pending["response"] = freeze(value if type(value) is dict else {"result": value}, 8192)
            return value
        except BaseException as exc:
            self.check_context(context)
            raise self.fail(exc)
        finally:
            self.check_context(context)
            self.check_tool(tool)

    def validate_request(self, wire, tools):
        if any(not pending["done"] for pending in self.pending.values()):
            raise AgentBindingError("adk_unfinished_tool_lineage")
        if (set(tools) - set(self.tools) or any(self.tools[name] is not tool for name, tool in tools.items())):
            raise AgentProfileError("adk_unbound_request_tool")
        if any(wire.get(key) for key in ("cache_config", "cache_metadata", "previous_interaction_id",
                                         "cacheable_contents_token_count")):
            raise AgentProfileError("adk_request_mode_unsupported")
        # ADK populates these empty defaults even on its non-live path.
        if wire.get("live_connect_config", {}) not in ({}, {
                "input_audio_transcription": {}, "output_audio_transcription": {}}):
            raise AgentProfileError("adk_request_mode_unsupported")
        contents = wire.get("contents", [])
        if [content for content in contents if content.get("role") == "model"] != self.model_contents:
            raise AgentBindingError("adk_model_history_changed")
        if self.last_request is not None and contents[:len(self.last_request)] != self.last_request:
            raise AgentBindingError("adk_request_history_changed")
        if self.last_request is None:
            expected = [{"role": "user", "parts": [{"text": self.state.prompt}]}]
            if contents != expected:
                raise AgentProfileError("adk_initial_history_unsupported")
        elif self.pending:
            responses = [part["function_response"] for content in contents[len(self.last_request):]
                         for part in content.get("parts", []) if "function_response" in part]
            if len(responses) != len(self.pending):
                raise AgentBindingError("adk_tool_response_lineage_invalid")
            seen = set()
            for response in responses:
                call_id = response.get("id")
                pending = self.pending.get(call_id)
                if (pending is None or call_id in seen or response.get("name") != pending["name"]
                        or response.get("response") != pending["response"]):
                    raise AgentBindingError("adk_tool_response_lineage_invalid")
                seen.add(call_id)
        self.last_request = freeze(contents)
        for tool in tools.values():
            self.check_tool(tool)

    def normalize_response(self, response, slot, tools):
        if type(response) is not self.LlmResponse:
            raise AgentProfileError("adk_model_response_invalid")
        wire = freeze(_dump(response), 8192)
        allowed = {"content", "model_version", "usage_metadata", "finish_reason", "partial",
                   "custom_metadata", "avg_logprobs", "logprobs_result", "grounding_metadata"}
        if set(wire) - allowed or wire.get("partial") or not wire.get("content"):
            raise AgentProfileError("adk_model_response_unsupported")
        content = wire["content"]
        if content.get("role") != "model" or not content.get("parts"):
            raise AgentProfileError("adk_model_content_unsupported")
        ids = set()
        for index, part in enumerate(content["parts"]):
            if set(part) - {"text", "thought", "thought_signature", "function_call"}:
                raise AgentProfileError("adk_model_part_unsupported")
            call = part.get("function_call")
            if call is not None:
                if set(call) - {"id", "name", "args"} or call.get("name") not in tools:
                    raise AgentProfileError("adk_model_tool_call_unsupported")
                if not call.get("id"):
                    call["id"] = self.state.slot("TOOL", slot, index)
                text(call["id"])
                if call["id"] in ids:
                    raise AgentProfileError("adk_duplicate_function_call_id")
                ids.add(call["id"])
                if type(call.get("args")) is not dict:
                    raise AgentProfileError("adk_tool_arguments_invalid")
            elif type(part.get("text")) is not str:
                raise AgentProfileError("adk_model_part_unsupported")
        return wire

    async def generate(self, request, *, stream):
        self.state.check()
        if stream or self.model_active:
            raise self.state.poison(AgentProfileError("adk_model_concurrency_unsupported"))
        self.model_active = True
        try:
            wire = freeze(_dump(request))
            tools = dict(request.tools_dict)
            self.validate_request(wire, tools)
            slot = self.state.slot("MODEL", self.previous)
            target = {"model": self.model_binding, "agent": self.agent_binding,
                      "tools": {name: {**tool.binding, "schema": tool.schema} for name, tool in tools.items()},
                      "transforms": self.hooks}
            async def execute(*, request):
                native = self.LlmRequest.model_validate(request["arguments"]["request"])
                native.tools_dict = dict(tools)
                accepted = None
                # One non-streaming response, and generator cleanup completes before acceptance.
                async with aclosing(self.delegate.generate_content_async(native, stream=False)) as responses:
                    async for response in responses:
                        if accepted is not None:
                            raise AgentProfileError("adk_multiple_model_responses_unsupported")
                        accepted = self.normalize_response(response, slot, tools)
                if accepted is None:
                    raise AgentProfileError("adk_missing_model_response")
                for tool in tools.values():
                    self.check_tool(tool)
                return accepted
            accepted = await self.state.execute(PreparedCall(slot, OperationKind.MODEL, target,
                {"request": wire}, "google-adk-llm-response-v1"), execute, category="model")
            # Validate replay too and never expose the authoritative wire to SDK mutation.
            accepted = self.normalize_response(self.LlmResponse.model_validate(accepted), slot, tools)
            self.model_contents.append(freeze(accepted["content"], 8192))
            self.pending = {}
            for index, part in enumerate(accepted["content"]["parts"]):
                if "function_call" in part:
                    call = part["function_call"]
                    self.pending[call["id"]] = {"slot": self.state.slot("TOOL", slot, index),
                                               "name": call["name"], "done": False}
            usage = accepted.get("usage_metadata")
            if usage is not None:
                self.state.observe_usage(UsageObservation(slot, UsageSource.MODEL_RESPONSE, slot,
                    UsageOrigin.ACCEPTED_REPLAY if self.state.replayed(slot) else UsageOrigin.CURRENT_DISPATCH,
                    usage.get("prompt_token_count"), usage.get("candidates_token_count"),
                    usage.get("total_token_count"), coverage=UsageCoverage.REPORTED))
            self.previous = slot
            return self.LlmResponse.model_validate(accepted)
        except BaseException as exc:
            raise self.fail(exc)
        finally:
            self.model_active = False

    async def run(self):
        final = None
        async with aclosing(self.runner.run_async(user_id="ledgence", session_id=self.session_id,
                new_message=self.types.Content(role="user", parts=[self.types.Part(text=self.state.prompt)]))) as events:
            async for event in events:
                self.state.check()
                if _has_actions(event.actions):
                    raise self.state.poison(AgentProfileError("adk_event_actions_unsupported"))
                if event.is_final_response():
                    final = event
        session = await self.sessions.get_session(app_name="ledgence", user_id="ledgence", session_id=self.session_id)
        if session.state != {}:
            raise self.state.poison(AgentProfileError("adk_session_state_unsupported"))
        if final is None or final.content is None or self.previous == "START" or self.pending:
            raise AgentExecutionError("adk_final_response_missing")
        output = "".join(part.text or "" for part in final.content.parts or [] if not part.thought)
        return self.state.result(AgentStatus.SUCCEEDED, output, native=final)

    async def request_stop(self):
        # Cancelling the driver is not evidence of a provider stop acknowledgement.
        pass

    async def close(self):
        errors = []
        # Also inspect authoritative state after failures/cancellation, before the
        # session service is closed. Cleanup continues if this inspection fails.
        if self.sessions is not None:
            try:
                session = await self.sessions.get_session(app_name="ledgence", user_id="ledgence",
                                                          session_id=self.session_id)
                if session is not None and session.state != {}:
                    raise self.fail(AgentProfileError("adk_session_state_unsupported"))
            except BaseException as exc:
                errors.append(exc)
        if self.runner is not None:
            try:
                await self.runner.close()
            except BaseException as exc:
                errors.append(exc)
        for close in reversed(self.closers):
            try:
                await close()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise BaseExceptionGroup("ADK resource cleanup failed", errors)
