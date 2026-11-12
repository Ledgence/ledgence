"""Optional PydanticAI 2.66.0 operation replay adapter (MIT).

Factories return a fresh named Agent with a static Model, ``bindings.durability``
and registered read tools. Imports of the optional SDK happen only on invocation.

Factories are trusted construction code. Legacy dynamic ``system_prompt``,
``instructions`` and ``output_validator`` decorators are outside this profile;
2.66.0 has no public accessor proving their absence. Use static instructions and
explicitly versioned ``bindings.transform`` hooks. This is not a runtime audit of
arbitrary Agent callbacks. Custom models must clean their own partially failed
context entry, as required by the native async-context contract.
"""
from dataclasses import is_dataclass, replace
from functools import wraps
import inspect

from ..workflow import OperationKind
from ._catalog import CatalogSpec, ReadSpec
from ._core import PreparedCall, freeze, start_run, text
from ._schema import effective_arguments, schema_for_callable, validate_arguments
from ._types import (
    AdapterProfile, AgentCapability, AgentProfileError, AgentIntegrationError,
    AgentStatus, UsageObservation, UsageSource, UsageOrigin, UsageCoverage,
)

PROFILE = AdapterProfile("pydantic_ai.bounded_operations.v1", "1", frozenset({
    AgentCapability.MODEL_REPLAY, AgentCapability.TOOL_REPLAY,
    AgentCapability.DISCOVERY_REPLAY, AgentCapability.REPORTED_USAGE,
}))
DEPENDENCIES = (("pydantic-ai-slim", "2.66.0", "pydantic_ai"),)
NORMALIZATION = "pydantic-generated-request-metadata-v1"


def run(ctx, *, spec, prompt, factory, require=frozenset()):
    """Register and own a bounded native Agent run before its first await."""
    return start_run(ctx, spec=spec, prompt=prompt, factory=factory, profile=PROFILE,
                     dependencies=DEPENDENCIES, normalization=NORMALIZATION,
                     session_factory=_Session, require=require)


class _Bindings:
    def __init__(self, state):
        # These public SDK imports are intentionally behind the exact-version gate.
        from pydantic_ai import Agent, Tool, FunctionToolset
        from pydantic_ai.capabilities import AbstractCapability
        from pydantic_ai.durable_exec import (
            BaseDurabilityCapability, DurabilityEngineSpec, DurableOperationBackend,
            JSON_CODEC, durable_unit_scope, ModelRequestId, ModelRequestParams,
            ToolsetCallToolId, ToolsetCallToolParams, DynamicToolsetCallToolParams,
            ToolsetGetToolsId, ToolsetValidateToolArgumentsId,
        )
        from pydantic_ai.exceptions import ModelRetry, CallDeferred, ApprovalRequired, ToolFailed, RunCancelled
        from pydantic_ai.messages import ModelMessage, ModelResponse
        from pydantic_ai.models import ModelRequestParameters
        from pydantic_ai.toolsets import DynamicToolset

        self._state = state
        self.Agent, self.Tool, self.FunctionToolset = Agent, Tool, FunctionToolset
        self.AbstractCapability, self.DynamicToolset = AbstractCapability, DynamicToolset
        self.json, self.ModelMessage, self.ModelResponse = JSON_CODEC, ModelMessage, ModelResponse
        self.ModelRequestParameters = ModelRequestParameters
        self.ModelRetry = ModelRetry
        self.unsupported_controls = (CallDeferred, ApprovalRequired, ToolFailed, RunCancelled)
        self.reads, self.catalogs, self.catalog_tools, self.transforms = {}, {}, {}, []
        self.accepted = []  # Immutable accepted wire snapshots, never live SDK objects.
        self.model_busy = False
        self.agent_identity = None
        self.baseline = []
        baseline = Agent(name="ledgence-profile-defaults")
        baseline.root_capability.apply(self.baseline.append)
        self.function_container_types = {FunctionToolset, *(type(item) for item in baseline.toolsets)}
        bindings = self

        class Backend(DurableOperationBackend):
            def bind(self, operation):
                class Bound:
                    @property
                    def operation(self): return operation
                    async def __call__(self, params, *, config=None):
                        try:
                            return await self.dispatch(params, config=config)
                        except AgentIntegrationError as exc:
                            raise bindings._state.poison(exc)
                    async def dispatch(self, params, *, config=None):
                        bindings._state.check()
                        operation_id = operation.operation_id
                        if config not in (None, {}):
                            raise bindings.fail("native_operation_config_unsupported")
                        if isinstance(operation_id, ModelRequestId) and isinstance(params, ModelRequestParams):
                            if operation_id.streaming:
                                raise bindings.fail("streaming_unsupported")
                            return await bindings.model(operation, params, durable_unit_scope)
                        if isinstance(operation_id, ToolsetCallToolId) and isinstance(
                                params, (ToolsetCallToolParams, DynamicToolsetCallToolParams)):
                            return await bindings.tool(operation, params, durable_unit_scope)
                        if isinstance(operation_id, (ToolsetGetToolsId, ToolsetValidateToolArgumentsId)):
                            if (operation_id.toolset_kind != "dynamic"
                                    or operation_id.toolset_id not in bindings.catalogs):
                                raise bindings.fail("native_discovery_unsupported")
                            # Discovery itself is CatalogSpec.snapshot's single journal boundary.
                            # The SDK reconstruction/validator only uses that accepted local registry.
                            with durable_unit_scope():
                                return await operation.handler(params)
                        raise bindings.fail("native_operation_unsupported")
                return Bound()
            def config_for_tool(self, operation, *, tool, tool_name): return {}
            def registrations(self): return ()

        self.backend = Backend()

        class Durability(BaseDurabilityCapability):
            engine_spec = DurabilityEngineSpec(
                engine_name="Ledgence", durable_unit_noun="operation", durable_container_noun="run",
                codec=JSON_CODEC, wrapped_toolset_kinds=frozenset({"function", "dynamic"}),
                toolset_lifecycles={"function": "enter-always", "dynamic": "enter-never"})
            @property
            def in_durable_context(self): return True
            def get_durable_operation_backend(self): return bindings.backend
            def get_default_conversation_id(self, run_id): return state.anchor["conversation_id"]

        self.Durability = Durability
        self.durability = Durability()

    @property
    def configuration(self): return self._state.configuration

    def fail(self, code):
        return self._state.poison(AgentProfileError(code))

    def read_tool(self, identity, function, *, version):
        spec = ReadSpec.create(identity, function, version)
        schema = schema_for_callable(function)
        if spec.identity in self.reads:
            raise self.fail("duplicate_read_identity")
        @wraps(function)
        async def execute(*args, **kwargs):
            # Native function validation occurs before this call. Bind defaults as well.
            bound = inspect.signature(function).bind(*args, **kwargs)
            arguments = effective_arguments(function, dict(bound.arguments))
            return await self.invoke_read(function, arguments)
        tool = self.Tool(execute, takes_ctx=False)
        if any(entry["tool"].name == tool.name for entry in self.reads.values()):
            raise self.fail("duplicate_read_name")
        self.reads[spec.identity] = {"spec": spec, "schema": schema, "tool": tool,
                                    "function": execute}
        return tool

    def transform(self, identity, capability, *, version):
        """Declare a deterministic native before/after-tool transform."""
        identity, version = text(identity), text(version)
        base = self.AbstractCapability
        if not isinstance(capability, base) or capability.defer_loading:
            raise self.fail("invalid_transform")
        allowed = {"before_tool_execute", "after_tool_execute"}
        for name in dir(base):
            if (not name.startswith("_") and callable(getattr(base, name, None))
                    and name not in allowed
                    and inspect.getattr_static(type(capability), name, None) != inspect.getattr_static(base, name, None)):
                raise self.fail("transform_hook_unsupported")
        if any(item[0]["identity"] == identity for item in self.transforms):
            raise self.fail("duplicate_transform_identity")
        cls = type(capability)
        self.transforms.append(({"identity": identity, "version": version,
                                 "class": cls.__module__ + ":" + cls.__qualname__}, capability))
        return capability

    async def invoke_read(self, function, arguments):
        try:
            result = function(**freeze(arguments))
            if inspect.isawaitable(result):
                result = await result
            return freeze(result, 8192)
        except self.unsupported_controls as exc:
            raise self.fail("native_tool_control_unsupported") from exc

    def catalog(self, identity, *, version, discover, registry):
        catalog = CatalogSpec(self._state, identity, version=version, discover=discover, registry=registry)
        if catalog.identity in self.catalogs:
            raise self.fail("duplicate_catalog_identity")
        entry = {"spec": catalog, "snapshots": {}, "toolset": None}
        self.catalogs[catalog.identity] = entry

        async def reconstruct(ctx):
            parent = self.discovery_parent(ctx)
            if parent not in entry["snapshots"]:
                entry["snapshots"][parent] = await catalog.snapshot(parent)
            tools = []
            for descriptor in entry["snapshots"][parent]:
                descriptor = freeze(descriptor)
                name = descriptor["name"]
                key = (catalog.identity, parent, name)
                self.catalog_tools[key] = descriptor
                def make_tool(item):
                    async def execute(**arguments):
                        args = validate_arguments(item["input_schema"], arguments)
                        return await self.invoke_read(catalog.registry[item["executor_id"]],
                                                      {"target": item["target"], "arguments": args})
                    def validate(ctx, **arguments):
                        validate_arguments(item["input_schema"], arguments)
                    return self.Tool.from_schema(execute, name=item["name"], description=None,
                                                 json_schema=item["input_schema"], args_validator=validate)
                tools.append(make_tool(descriptor))
            return self.FunctionToolset(tools, id="catalog-" + catalog.identity)
        native = self.DynamicToolset(reconstruct, id=catalog.identity)
        entry["toolset"] = native
        return native

    def discovery_parent(self, ctx):
        # Tool reconstruction refers to the catalog shown to its accepted model turn.
        # Model discovery refers to the accepted predecessor, not a callback counter.
        if ctx.tool_call_id is not None:
            if not self.accepted:
                raise self.fail("tool_without_accepted_parent")
            return self.accepted[-1][2]
        return self.accepted[-1][0] if self.accepted else "START"

    def declarations(self):
        return {"agent": self.agent_identity,
                "transforms": [item[0] for item in self.transforms]}

    def validate_agent(self, agent):
        from pydantic_ai.models import Model
        from pydantic_ai.models.wrapper import WrapperModel
        from pydantic_ai.models.fallback import FallbackModel
        if (type(agent) is not self.Agent or not isinstance(agent.model, Model)
                or isinstance(agent.model, (WrapperModel, FallbackModel)) or not agent.name
                or callable(agent.model_settings) or agent.event_stream_handler is not None
                or agent.validation_context is not None):
            raise self.fail("agent_profile_unsupported")
        bound = self.Durability.from_agent(agent)
        if bound is None or bound.get_durable_operation_backend() is not self.backend:
            raise self.fail("registered_durability_required")
        leaves = []
        agent.root_capability.apply(leaves.append)
        for capability in leaves:
            if isinstance(capability, self.Durability):
                continue
            if any(capability is item[1] for item in self.transforms):
                continue
            # SDK-injected defaults are obtained through its public Agent API. They
            # cannot perform discovery: every registered tool forbids defer_loading.
            if any(type(capability) is type(default) and
                   (not is_dataclass(default) or capability == default) for default in self.baseline):
                continue
            raise self.fail("native_capability_unsupported")
        for toolset in agent.toolsets:
            if any(toolset is entry["toolset"] for entry in self.catalogs.values()):
                continue
            # Only the SDK's own function containers and our exact catalog instances.
            if (type(toolset) not in self.function_container_types
                    or toolset.timeout is not None or toolset.requires_approval):
                raise self.fail("native_toolset_unsupported")
            for tool in toolset.tools.values():
                registered = next((entry for entry in self.reads.values()
                                   if tool.function is entry["function"]), None)
                if (registered is None or tool is not registered["tool"] or tool.takes_ctx
                        or tool.prepare is not None or tool.args_validator is not None
                        or tool.requires_approval or tool.defer_loading or tool.timeout is not None):
                    raise self.fail("unregistered_native_tool")
        self.agent_identity = {"name": text(agent.name), "model": text(agent.model.model_name, 256),
                               "system": text(agent.model.system, 128)}

    def normalized_messages(self, messages):
        wire = self.json.dump(list[self.ModelMessage], messages)
        for message in wire:
            if message["kind"] == "request":
                message["timestamp"] = self._state.anchor["created_at"]
                for part in message["parts"]:
                    if part["part_kind"] in ("user-prompt", "tool-return", "retry-prompt"):
                        part["timestamp"] = self._state.anchor["created_at"]
        return freeze(wire)

    def check_prefix(self, messages):
        responses = [m for m in messages if m["kind"] == "response"]
        if len(responses) != len(self.accepted):
            raise self.fail("model_lineage_unproven")
        for response, (_, accepted, _) in zip(responses, self.accepted):
            if response["parts"] != accepted["parts"]:
                raise self.fail("accepted_response_changed")

    def codec(self, operation, *, model=False):
        bindings = self
        class Codec:
            control_exceptions = (bindings.ModelRetry,)
            @staticmethod
            def encode(value):
                if model:
                    if not isinstance(value, bindings.ModelResponse):
                        raise bindings.fail("model_response_unsupported")
                    bindings.validate_response(bindings.json.dump(bindings.ModelResponse, value))
                return operation.result_codec.dump(value)
            @staticmethod
            def decode(wire): return operation.result_codec.load(wire)
        return Codec

    def validate_response(self, response):
        if (response.get("state") != "complete" or response.get("workspace_ref") is not None
                or response.get("history_digest") is not None or response.get("failed_attempts")):
            raise self.fail("model_continuation_unsupported")
        calls = []
        for part in response["parts"]:
            if part["part_kind"] not in ("text", "tool-call"):
                raise self.fail("model_part_unsupported")
            if part["part_kind"] == "tool-call":
                identity = part.get("tool_call_id")
                if not identity or identity in calls:
                    raise self.fail("ambiguous_tool_call_id")
                calls.append(identity)

    async def model(self, operation, params, scope):
        if (params.model_id is not None
                or operation.operation_id.model_name != self.agent_identity["model"]):
            raise self.fail("dynamic_model_unsupported")
        if self.model_busy:
            raise self.fail("competing_model_continuation")
        self.model_busy = True
        try:
            messages = self.normalized_messages(params.messages)
            self.check_prefix(messages)
            if params.run_context.run_step != len(self.accepted) + 1:
                raise self.fail("model_step_inconsistent")
            parameters = self.json.dump(self.ModelRequestParameters, params.model_request_parameters)
            if params.model_request_parameters.native_tools:
                raise self.fail("native_model_tools_unsupported")
            if any(tool.defer_loading or tool.kind != "function"
                   for tool in params.model_request_parameters.function_tools):
                raise self.fail("deferred_model_tools_unsupported")
            parent = self.accepted[-1][0] if self.accepted else "START"
            slot = self._state.slot("MODEL", parent)
            arguments = {"messages": messages, "model_id": params.model_id,
                         "settings": params.model_settings, "parameters": parameters}
            target = {**self.declarations(), "kind": "model"}
            async def execute(*, request):
                args = request["arguments"]
                effective = replace(params, messages=self.json.load(list[self.ModelMessage], args["messages"]),
                                    model_id=args["model_id"], model_settings=args["settings"],
                                    model_request_parameters=self.json.load(self.ModelRequestParameters, args["parameters"]))
                with scope():
                    return await operation.handler(effective)
            result = await self._state.execute(PreparedCall(slot, OperationKind.MODEL, target, arguments,
                                                          "pydantic-model-response-2.66.0"),
                                              execute, self.codec(operation, model=True))
            # Re-decode the accepted wire before taking the causal snapshot. SDK mutation
            # of the returned native instance can never mutate this authority.
            accepted_native = operation.result_codec.load(self._state.wire(slot))
            accepted = freeze(self.json.dump(self.ModelResponse, accepted_native), 8192)
            self.validate_response(accepted)
            self.accepted.append((slot, accepted, parent))
            usage = accepted_native.usage
            # Native usage.cost mixes provider reports with genai-prices estimates;
            # it cannot truthfully populate the neutral reported_cost field.
            measured = not usage.unmeasured_requests
            self._state.observe_usage(UsageObservation(
                slot, UsageSource.MODEL_RESPONSE, slot,
                UsageOrigin.ACCEPTED_REPLAY if self._state.replayed(slot) else UsageOrigin.CURRENT_DISPATCH,
                usage.input_tokens if measured else None, usage.output_tokens if measured else None,
                usage.total_tokens if measured else None,
                coverage=UsageCoverage.REPORTED if measured else UsageCoverage.UNKNOWN))
            return result
        finally:
            self.model_busy = False

    async def tool(self, operation, params, scope):
        if not self.accepted:
            raise self.fail("tool_without_accepted_parent")
        parent, response, catalog_parent = self.accepted[-1]
        matches = [(i, part) for i, part in enumerate(response["parts"])
                   if part["part_kind"] == "tool-call" and part["tool_call_id"] == params.ctx.tool_call_id]
        if len(matches) != 1 or matches[0][1]["tool_name"] != params.name:
            raise self.fail("tool_lineage_unproven")
        position, _ = matches[0]
        if params.ctx.run_step != len(self.accepted):
            raise self.fail("tool_step_inconsistent")
        kind = operation.operation_id.toolset_kind
        target = {**self.declarations(), "kind": "tool", "name": params.name}
        if kind == "dynamic":
            catalog_id = operation.operation_id.toolset_id
            descriptor = self.catalog_tools.get((catalog_id, catalog_parent, params.name))
            if descriptor is None:
                raise self.fail("catalog_tool_not_reconstructed")
            arguments = validate_arguments(descriptor["input_schema"], params.tool_args)
            catalog = self.catalogs[catalog_id]["spec"]
            target.update({"catalog": catalog_id, "descriptor": descriptor,
                           "executor": catalog.registry_identities[descriptor["executor_id"]]})
        elif kind == "function":
            registered = next((entry for entry in self.reads.values()
                               if entry["tool"].name == params.name), None)
            if registered is None:
                raise self.fail("unregistered_native_tool")
            arguments = effective_arguments(registered["spec"].function, params.tool_args)
            target.update({"read": registered["spec"].binding(), "schema": registered["schema"]})
        else:
            raise self.fail("native_toolset_unsupported")
        slot = self._state.slot("TOOL", parent, position)
        async def execute(*, request):
            effective = replace(params, tool_args=request["arguments"])
            with scope():
                return await operation.handler(effective)
        return await self._state.execute(PreparedCall(slot, OperationKind.TOOL, target, arguments,
                                                    "pydantic-tool-result-2.66.0"),
                                        execute, self.codec(operation))


class _Session:
    def __init__(self, state, factory):
        self._state, self.factory = state, factory
        self.agent = None
        self.entered = False
    async def open(self):
        self.bindings = _Bindings(self._state)
        self.agent = self.factory(self.bindings)
        self.bindings.validate_agent(self.agent)
        await self.agent.__aenter__()
        self.entered = True
    async def run(self):
        native = await self.agent.run(self._state.prompt, run_id=self._state.anchor["run_id"])
        output = native.output
        from pydantic import BaseModel
        if isinstance(output, BaseModel):
            output = output.model_dump(mode="json")
        return self._state.result(AgentStatus.SUCCEEDED, freeze(output, 8192), native=native)
    async def request_stop(self):
        # Core owns cancellation of the separate run driver. There is no advertised
        # native STOP_REQUEST capability or claim of confirmed external cancellation.
        pass
    async def close(self):
        if self.entered:
            self.entered = False
            await self.agent.__aexit__(None, None, None)
            self._state.lifecycle = replace(self._state.lifecycle, transport_closed=True)
