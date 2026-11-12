# Optional agent framework adapters

**Development source beyond published Ledgence 0.9.0.** Use matching development
workers and the worker helper from the same checkout. Installing the published
0.9.0 helper or updating only a program does not enable these adapters.

Ledgence offers separate optional adapters for PydanticAI, Google ADK and Claude
Agent SDK. Choose the framework in your application. The Rust platform and
`ledgence.worker` have no required framework or provider dependency. Importing an
integration module does not load its SDK; invocation checks the exact installed
distribution and its module origin. The worker never installs SDKs, downloads a
CLI, chooses a provider, or discovers application credentials for these helpers.

A framework can run normally inside an ordinary **task**, which remains a leaf
execution unit. The adapters below require an active protocol-3
`WorkflowContext`. Use them in a workflow controller when accepted operation
replay is needed. Call the helper directly from that controller: do not wrap it
in `ctx.local` or `ctx.operation`. Workflow composition, approvals and protected
writes belong in controller code outside model/tool callbacks.

## Choose a profile

| Module under `ledgence.worker.integrations` | Exact application SDKs | Accepted replay boundary | Capabilities |
| --- | --- | --- | --- |
| `pydantic_ai` | `pydantic-ai-slim==2.66.0` | Individual non-streaming model calls, declared read tools and catalog discovery | `MODEL_REPLAY`, `TOOL_REPLAY`, `DISCOVERY_REPLAY`, `REPORTED_USAGE` |
| `google_adk` | `google-adk==2.14.0`, `google-genai==2.34.0` | Individual non-streaming model calls, declared read tools and catalog discovery | `MODEL_REPLAY`, `TOOL_REPLAY`, `DISCOVERY_REPLAY`, `REPORTED_USAGE` |
| `claude_agent_sdk` | `claude-agent-sdk==0.2.180` | One whole external-agent run, including its terminal result and bounded evidence | `RESULT_REPLAY`, `REPORTED_USAGE`, `STOP_REQUEST` |

The exported profiles are `pydantic_ai.bounded_operations.v1`,
`google_adk.bounded_operations.v1` and `claude_agent_sdk.whole_run.v1`, all with
profile version `1`. None offers `SESSION_RESUME`, native approval bridging,
partial-stream checkpointing, or arbitrary SDK session/history restoration.
The capability differences describe different recovery boundaries; they do not
select a preferred framework.

Install and prepare the SDKs in the **application's** dependencies, preserving
their licenses and notices. Provider-specific extras and clients are also
application-owned. See the [framework examples](../examples/framework-integrations/README.md)
and [dependency policy](dependencies.md). Source-only Claude SDK tests use an
explicit offline transport; they do not qualify live CLI/provider execution or
prove that an external effect has stopped. The proprietary external CLI is not
bundled or required by Ledgence.

The development [program package profile](program-publication.md) permits up to
8,192 ZIP entries, counting directories and the manifest. Existing 0.9.0 workers
use 4,096. Upgrade every eligible worker and use the matching CLI/server before
publishing larger dependency packages. Byte limits still apply; preparing an
arbitrary framework environment does not guarantee that its archive will fit.

## Register a run

The common types have no SDK imports:

```python
from ledgence.worker.integrations import AgentCapability, AgentLimits, AgentRunSpec
from ledgence.worker.integrations import pydantic_ai

spec = AgentRunSpec(
    key="read-account", version="application-1",
    configuration={"model_snapshot": "application-selected-model", "policy": "read-only"},
    branch="main",
    limits=AgentLimits(max_model_turns=3, max_tool_calls=4, timeout_ms=60_000),
)
# ctx is the active WorkflowContext; make_agent is the application's factory.
result = await pydantic_ai.run(
    ctx, spec=spec, prompt="Summarize the account.", factory=make_agent,
    require=frozenset({AgentCapability.MODEL_REPLAY, AgentCapability.TOOL_REPLAY}),
)
```

ADK uses the same `run(ctx, *, spec, prompt, factory, require=...)` signature.
Claude additionally requires `runtime=...` and declares native options in
`spec.configuration["options"]`; see its factory contract below. Select the
module explicitly. There is no universal replacement for native Agent, Model,
Message or Tool objects.

`run` synchronously claims the `(context, key, branch)` occurrence and returns an
owned awaitable. Keep that handle if multiple observers need its result; a second
helper call with the same occurrence raises `AgentRunConflict`, even after the
first finishes. Explicit different branches create independent occurrences.
The invocation retains registered work even if its handle is never awaited.

A manifest is acknowledged before the factory runs. It binds the prompt,
configuration, application version, limits, profile, SDK version, normalization
policy and any runtime identity. Its accepted initialization time and native run
IDs survive replacement attempts. Factory code reads copied
`bindings.configuration`; do not substitute mutable outer configuration. Each
observed model/tool request is additionally frozen and bound after supported
transforms, and the executor receives a decoded copy of that effective request.
Changing a binding fails before dispatching that operation again.

Factories are synchronous, construction-only code that returns fresh,
invocation-owned native objects. They must not perform discovery, model/tool
work or unrelated IO. Model/provider transport retry settings belong in the
semantic configuration and should be explicitly set where supported. A boundary
dispatch is not a count of individual HTTP retries inside a provider SDK.

## PydanticAI factory

Return a fresh named native `Agent` with a static `Model` instance,
`capabilities=[bindings.durability]`, and tools created with
`bindings.read_tool(identity, function, version=...)`. The application configures
the native model and its credentials. Model strings, wrapper/fallback models,
other durability engines, native MCP, arbitrary dynamic toolsets, workspace
capabilities, streams, deferred controls and suspended model continuations are
outside this profile.

Declare a pure native capability with
`bindings.transform(identity, capability, version=...)` and include the returned
capability in the Agent's capability list. Only `before_tool_execute` and
`after_tool_execute` overrides are supported. Their declarations bind each
operation; effective transformed arguments bind the read call, and transformed
results bind the next model request. These deterministic hooks can execute again
on replay.

Use static instructions. Legacy dynamic `system_prompt`, `instructions` and
`output_validator` decorators are outside the supported profile. PydanticAI
2.66.0 has no public inventory that proves their absence; trusted factories must
not register them. This adapter does not claim to inspect every arbitrary Agent
callback. Static instruction changes are checked in effective model bindings.

The public durability backend uses typed operation parameters and the SDK's
provided result codecs. Public `ModelRetry` and native validation outcomes can
reconstruct a new causal model turn; they are not fabricated successful JSON
exceptions. Approval, deferred, cancelled and other unsupported tool controls
poison the run. Model/tool identities follow immutable accepted response parts,
including separate positions for identical parallel calls. Duplicate tool IDs
within a response fail; an ID may recur in a later accepted turn.

The owner constructs, enters and exits Agent in the same task. The native run
uses a separate driver. Retained operations finish before Agent exits; a custom
Model must still clean its own partially failed async-context entry.

## Google ADK factory

Return a fresh native `LlmAgent` using a bound static model and registered tools:

```python
# native_model and close_model are constructed by the application in this factory.
bound_model = bindings.model(
    native_model, identity="account-model",
    configuration=bindings.configuration,
    close=close_model,  # zero-argument async callable, or explicit None if resource-free
)
read_tool = bindings.read_tool("account-read", lookup_account, version="1")
# Return LlmAgent(name="account_reader", model=bound_model, tools=[read_tool]).
```

Model ownership transfers when binding succeeds, including cleanup if later
factory work fails. Register an explicit async closer; Runner alone does not
close model/provider clients. An async-enter-only model needs a future lifecycle
contract. Already-entered borrowed resources are outside this factory contract.

Declare each pure before/after-tool callback with
`bindings.transform(identity, callback, version=...)`, then assign it to the
corresponding native `before_tool_callback` or `after_tool_callback`. Before
callbacks may mutate arguments and must return `None`; a replacement result
would bypass the read boundary and is rejected. After callbacks may return a
JSON object or mutate the existing response. They reexecute on replay; the
original accepted read result is retained and the effective transformed response
binds the next model request.

Static instructions and effective native request settings are bound. Native
state/action changes, transfers, authentication, long-running tools, subagents,
history processors, native session resume and undeclared hooks are rejected.
Missing function-call IDs are filled deterministically before acceptance using
the model slot and part position; supplied duplicate IDs fail. Read arguments
are checked after transforms, with defaults materialized before journaling.

After stopping admission, the owner drains retained operations, closes Runner,
then attempts all registered model closers in reverse construction order. Model
generator closure completes before accepting its result. Cleanup failure
prevents successful helper completion.

## Claude Agent SDK factory

The factory returns `bindings.client(options=...)`; `bindings.client()` builds
from the declared options when no credential headers need supplying. It must
return that fresh client, not a borrowed `ClaudeSDKClient`. Native options must
match `spec.configuration["options"]` after the reviewed semantic projection.
Declare a model, plain text system prompt if needed, explicit `tools`,
`allowed_tools` and `disallowed_tools` lists, and optional native `max_turns`,
`max_budget_usd` and bounded JSON-schema `output_format`. These native limits do
not create per-model or per-tool Ledgence replay.

Choose an explicit application-owned runtime:

```python
from ledgence.worker.integrations.claude_agent_sdk import ExternalCli, CustomTransport

runtime = ExternalCli(path="/absolute/path/to/claude", identity="reviewed-runtime-1")
# For offline tests or an explicitly implemented transport:
# runtime = CustomTransport(factory=make_transport, identity="offline-transport-1")
```

The runtime owns CLI selection; do not also set `ClaudeAgentOptions.cli_path`.
The helper does not download a CLI or fall back to an ambient executable. Custom
transport factories are synchronous and must return a fresh SDK `Transport`
that owns and drains its tasks/resources.

Use `permission_mode="dontAsk"` (or the explicitly chosen native
`"bypassPermissions"`), `setting_sources=[]` and `strict_mcp_config=True`.
Host callbacks, hooks, permission callbacks, SDK-hosted MCP servers, SessionStore,
resume/fork/checkpoint settings, plugins, subagents and arbitrary extra options
are rejected. Plain external MCP `stdio`, `http` or `sse` dictionaries are
supported only by the bounded projection: stdio command/args, or HTTP(S) endpoint
plus declared credential-header names. HTTP URLs cannot contain user info,
query or fragment. Only authorization/API-key header values are separated from
the journal; unsupported environment/header semantics must not be smuggled
through opaque configuration strings.

The whole-run executor constructs/connects the client, owns a separate receive
driver, drains it and disconnects in its owning task before accepting the result.
A completed accepted replay opens no client. Before acceptance, a retry can repeat
all inner runtime work, including tools and external effects. Host-side nested
Ledgence operations remain forbidden. Native terminal failure or abort is
returned as the corresponding `AgentStatus`; inspect status before using output.

## Read tools and catalogs

Both operation adapters expose `read_tool(identity, function, version=...)`.
The callable has a stable module/qualified name and JSON inputs/outputs; its
implementation changes require an application/registration version change.
Static signatures support `str`, `int`, `float`, `bool` and nested `list[T]`
annotations with valid JSON defaults. Use a catalog for richer argument schemas.
Calling something a read operation is an application contract, not an IO sandbox.

`bindings.catalog(identity, version=..., discover=..., registry=...)` accepts a
named `discover(configuration=...)` read and a fixed executor-ID-to-callable map.
Discovery returns at most 16 descriptors with exactly these fields:
`id`, `name`, `version`, `input_schema`, `executor_id`, and JSON-object `target`.
IDs/names must be unique; the executor must exist in the declared registry
(maximum 64 entries). It is invoked as
`executor(target=copied_target, arguments=copied_effective_arguments)`.

Discovery is an ordinary journaled TOOL operation. Accepted descriptors reconstruct
native tools through local registry lookup, without repeating discovery IO.
PydanticAI snapshots per causal model turn/catalog; ADK snapshots once at START
per invocation/catalog, following its native toolset cache. Neither accepts
arbitrary native dynamic factories as durable discovery.

The enforced schema subset is a closed object (`additionalProperties: false`)
with named required/optional properties: string, integer, number, boolean,
object and homogeneous array types, optional descriptions and primitive enums.
Depth is at most eight and total properties at most 32. References, combinators,
formats and unspecified additional properties are rejected. Both native
construction and final transformed arguments are checked; constraints are not
silently discarded.

## Limits, results and recovery

| Bound | Current value |
| --- | --- |
| Prompt / semantic configuration | 4 KiB each |
| Effective operation input envelope | 16 KiB, including target and codec metadata |
| Accepted operation output | 8 KiB, including the native codec envelope |
| Full adapter operation record / manifest | 32 KiB / 16 KiB |
| Shared adapter reservation budget per WorkflowContext | 64 records and 192 KiB |
| PydanticAI/ADK model turns / read calls / discoveries | Defaults and ceilings: 4 / 8 / 4; may be lowered to positive integers |
| Concurrent calls | At most 4 per run and across adapter calls in the context; may be lowered per run |
| Helper deadline | Default 60,000 ms; configurable from 1 to 300,000 ms |
| Claude observed messages | At most 256 messages / 256 KiB before the final bounded envelope |

Claude rejects explicit `max_model_turns`, `max_tool_calls` or `max_discoveries`;
leave these `AgentLimits` fields as `None`. Its terminal result, usage and
lifecycle evidence together must fit the 8 KiB accepted output. Bounds include
framing, so an application output smaller than 8 KiB can still be rejected.
The shared ordinary workflow journal still has its own limits; other local steps
can exhaust it even when an adapter's budget would fit.

`AgentResult` contains `status`, copied JSON `output`, SDK-native `native`,
`profile`, `usage` and `lifecycle`. Usage observations identify their operation,
source (`MODEL_RESPONSE`, `ASSISTANT_MESSAGE` or `RUN_TOTAL`) and origin
(`CURRENT_DISPATCH` or `ACCEPTED_REPLAY`). Replayed usage is prior evidence, not a
new charge. Do not add assistant-message observations to a run total that already
includes them. Missing counts/cost remain unknown; coverage can be reported,
partial or unknown. PydanticAI's pricing estimates are not normalized as reported
cost. Imprecise SDK float costs remain native evidence rather than being promoted
to exact decimal cost. No prices are guessed from tokens or elapsed time.

`AgentDependencyError`, `AgentCapabilityError`, `AgentProfileError`,
`AgentRunConflict`, `AgentBindingError`, `AgentLimitError` and
`AgentExecutionError` distinguish the failure classes. Errors have bounded codes;
`external_work_may_have_occurred` records uncertainty, not success or exactly-once
execution. Ordinary failed callbacks, binding/codec/budget errors and lost
acknowledgements poison the run. A caught forbidden nested control attempt also
prevents that adapter from succeeding. Workflow controls also require the active
invocation controller scope; SDK callbacks must not detach or reset their async
context to seek controller authority. These guards cover inherited and empty/reset
callback contexts, not deliberately adversarial Python injecting a retained
controller context. Programs and factories remain operator-trusted.
The helper does not retry a failed slot or invent a new key. A replacement worker
may redispatch unaccepted work; only server-accepted operations replay.

Cancellation/deadlines stop new admission and cancel a separate driver, while
retaining operation executors and the resource owner. During manifest
acknowledgement or resource acquisition, the owner is retained; once acquisition
returns, no new query starts after stop. Cleanup may outlive the helper deadline.
The worker attempt/process timeout remains final containment. This is not a hard
wall-clock stop guarantee.

Claude may send a native interrupt request. `stop_requested`,
`stop_acknowledged`, `terminal_status`, `transport_closed` and
`local_process_exit_observed` are separate evidence fields. A transport close or
interrupt acknowledgement does not establish that a remote effect stopped.
An observed session ID does not establish a restorable checkpoint;
`checkpoint_verified` remains false and `SESSION_RESUME` is absent.

Use [approvals](workflow-approvals.md), [provider operations](provider-operations.md)
and explicit controller checkpoints for external writes, human decisions or
longer workflows. Programs remain operator-trusted; no framework adapter adds a
hostile-code sandbox or mandatory vendor service to Ledgence.
