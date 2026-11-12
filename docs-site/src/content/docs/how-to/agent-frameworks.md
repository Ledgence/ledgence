---
title: Run an agent framework with accepted-result replay
description: Choose the optional PydanticAI, Google ADK or Claude Agent SDK profile and understand its recovery boundary.
---

**Development source for the next release; not available in 0.9.0.** Use matching
workers and the worker helper from the same development checkout. The published
local kit and Python helper do not contain these adapters.

Your application chooses its agent framework, models and credentials. Ledgence's
Rust components and standard-library worker helper do not require a framework or
provider account. The optional adapters import their SDK only when invoked and
verify its exact installed version and module origin. Application dependencies
must already be prepared in the program package; workers install nothing.

## Choose the recovery boundary

| Framework | Application dependencies | What an accepted retry reuses |
| --- | --- | --- |
| PydanticAI | `pydantic-ai-slim==2.66.0` | Individual model responses, declared read-tool results and discovered catalogs |
| Google ADK | `google-adk==2.14.0`, `google-genai==2.34.0` | Individual model responses, declared read-tool results and discovered catalogs |
| Claude Agent SDK | `claude-agent-sdk==0.2.180` | One complete external-agent run, including its terminal result and bounded evidence |

PydanticAI and ADK offer `MODEL_REPLAY`, `TOOL_REPLAY`, `DISCOVERY_REPLAY` and
`REPORTED_USAGE`. Claude offers `RESULT_REPLAY`, `REPORTED_USAGE` and
`STOP_REQUEST`. No profile offers `SESSION_RESUME`, partial-token checkpointing or
automatic native approval bridging. Claude's whole-run boundary can repeat all
inner work if it was not accepted; it does not checkpoint each native tool call.

Start from the same-scope
[framework examples](https://github.com/Ledgence/ledgence/tree/develop/examples/framework-integrations):

| Example | Entry point | Dependency selection |
| --- | --- | --- |
| PydanticAI | [pydantic_program.py](https://github.com/Ledgence/ledgence/blob/develop/examples/framework-integrations/pydantic_program.py) | `requirements-pydantic.txt` |
| Google ADK | [google_adk_program.py](https://github.com/Ledgence/ledgence/blob/develop/examples/framework-integrations/google_adk_program.py) | `requirements-adk.txt` |
| Claude Agent SDK | [claude_program.py](https://github.com/Ledgence/ledgence/blob/develop/examples/framework-integrations/claude_program.py) | `requirements-claude.txt` |

The examples use deterministic offline models/transports with actual SDKs.
Follow their README for installation, preparation and checks. They do not make
provider calls or download a CLI. A source-only Claude SDK/transport pass proves
the Python SDK boundary; it does not qualify a live CLI/provider or external
process termination. Any external CLI is supplied separately by the application.

Development builds support an explicitly negotiated 8,192-entry program package
profile. Published 0.9.0 workers allow 4,096 entries. Upgrade all eligible workers
before publishing a larger package; directory entries and the manifest count,
and existing byte limits still apply.
[Build and publish programs](/how-to/build-and-publish-programs).

## Put composition in a workflow

An ordinary task may run its native framework directly. Tasks remain leaves;
running a framework does not grant them workflow control APIs or accepted-call
replay. Use these helpers inside an active protocol-3 workflow controller when
replay matters. See [your first workflow](/tutorials/first-workflow) for the
controller and checkpoint model.

Import the shared types and select a framework module explicitly:

```python
from ledgence.worker.integrations import AgentCapability, AgentLimits, AgentRunSpec
from ledgence.worker.integrations import pydantic_ai, google_adk, claude_agent_sdk
```

For PydanticAI or ADK, create a semantic run specification and use the corresponding
call with your application factory:

```python
spec = AgentRunSpec(
    key="account-summary", version="application-1", branch="main",
    configuration={"policy": "read-only", "model_snapshot": "your-selected-model"},
    limits=AgentLimits(max_model_turns=3, max_tool_calls=4, timeout_ms=60_000),
)

# Choose one operation adapter; ctx is the active WorkflowContext.
result = await pydantic_ai.run(ctx, spec=spec, prompt=question, factory=make_pydantic_agent)
# result = await google_adk.run(ctx, spec=spec, prompt=question, factory=make_adk_agent)
```

For Claude, declare native options and supply an explicit runtime instead of
per-model/tool limits:

```python
spec = AgentRunSpec(
    key="account-summary", version="application-1",
    configuration={"options": {
        "model": "your-selected-model", "tools": [],
        "allowed_tools": [], "disallowed_tools": [],
        "permission_mode": "dontAsk", "setting_sources": [], "strict_mcp_config": True,
        "max_turns": 3,
    }},
)
result = await claude_agent_sdk.run(
    ctx, spec=spec, prompt=question, factory=make_claude_client,
    runtime=claude_agent_sdk.ExternalCli(
        path="/absolute/path/to/claude", identity="reviewed-runtime-1",
    ),
)
```

The model names, factories, questions and runtime path above are application
inputs, not bundled defaults. The offline Claude example uses an explicit
`CustomTransport(factory=..., identity=...)` instead. The helper never downloads
or implicitly selects an executable.

The returned `AgentResult` exposes `status`, JSON `output`, SDK-native `native`,
`profile`, `usage` and `lifecycle`. Inspect status before consuming output. After
awaiting it, the workflow controller may complete, checkpoint, request approval
or compose other work. Keep those decisions outside SDK model/tool callbacks;
do not put the helper itself inside `ctx.local` or `ctx.operation`. Workflow
controls require the active invocation controller scope. A callback that catches
a forbidden nested-control error still poisons its adapter; using an empty/reset
async context does not grant controller authority. This is an operating contract
for trusted Python, not a security sandbox against deliberately injected
controller context.

Use `require=frozenset({AgentCapability.MODEL_REPLAY})`, for example, to reject an
incompatible profile before factory work. Keep the original awaitable if another
observer needs it: calling a helper twice with the same context/key/branch raises
`AgentRunConflict`. A separate explicit branch is a distinct occurrence.

## Construct only supported native objects

Each synchronous factory receives copied `bindings.configuration`. It must return
fresh owned SDK objects and perform construction only. Discovery, model/tool IO
and arbitrary side effects do not belong in the factory. Include the selected
model/snapshot, provider retry policy and other behavior-affecting settings in the
semantic configuration and application version.

**PydanticAI:** return a named native Agent with a static Model instance,
`bindings.durability`, and `bindings.read_tool(identity, function, version=...)`.
Use `bindings.transform(identity, capability, version=...)` only for pure native
`before_tool_execute`/`after_tool_execute` overrides. Native ModelRetry and
validation outcomes are codec-preserved; approvals/deferred controls, MCP,
workspace state, streams, fallbacks and hidden continuations are unsupported.
Use static instructions. Legacy dynamic prompt/instruction/output-validator
decorators are outside this profile, and their absence cannot be inspected through
2.66.0's public API; this is an explicit trusted-factory constraint.

**Google ADK:** return a native LlmAgent with
`bindings.model(native_model, identity=..., configuration=..., close=...)` and
registered read tools. Supply a zero-argument async model closer, or explicit
`None` for a resource-free model. Ownership transfers at binding; Runner alone
does not close model clients. Declare pure native before/after-tool callbacks via
`bindings.transform(identity, callback, version=...)`. Before callbacks may mutate
arguments and return `None`; after callbacks transform JSON responses. State,
actions, transfers, auth, subagents, history processors and native resume are
unsupported.

**Claude Agent SDK:** return `bindings.client(options=...)` matching the declared
option projection, or `bindings.client()` when no credential headers are needed.
Host callbacks, SDK-hosted MCP, hooks, SessionStore, resume/checkpoints and opaque
native configuration are unsupported. Bounded external stdio/HTTP/SSE MCP
configurations require explicit semantic declarations; credential-header values
are kept out of the journal. Consult the full contract before using them.

## Declare read tools and discovery

PydanticAI and ADK read tools accept JSON, with static `str`, `int`, `float`,
`bool` and `list[T]` annotations and valid JSON defaults. Arguments are bound after
supported transforms and defaults. A read declaration is an application contract,
not a sandbox that prevents a function from writing elsewhere.

For dynamic schemas, use
`bindings.catalog(identity, version=..., discover=..., registry=...)`. Discovery
returns bounded descriptors naming a registered read executor, schema, version
and JSON target. Accepted snapshots rebuild native tools locally without repeating
discovery IO. PydanticAI snapshots per causal turn/catalog; ADK snapshots once at
START per invocation/catalog. Arbitrary SDK dynamic factories do not qualify.

The schema subset supports closed objects with required/optional properties,
strings, integers/numbers, booleans, nested objects, homogeneous arrays and
primitive enums. References, unions/combinators, formats and unspecified extra
properties fail. Final transformed arguments are checked against that schema.

Model identities follow accepted predecessors; tool identities follow immutable
response part positions. Equal parallel inputs are separate calls. Supplied IDs
may recur in later turns, but duplicate IDs within one response fail. An
unaccepted callback can repeat after worker loss; none of these profiles promises
exactly-once external execution.

## Keep work bounded and read evidence accurately

The operation profiles default to at most 4 model turns, 8 read calls, 4 discoveries
and 4 concurrent calls; positive lower limits are supported. The context also
shares a four-call concurrency bound and a 64-record/192-KiB adapter reservation
budget. Prompts and configuration are each bounded to 4 KiB, effective operation
inputs to 16 KiB and accepted outputs to 8 KiB including codec framing. The
ordinary workflow journal has additional shared limits.

Claude leaves per-model/tool/discovery limits unset; native `max_turns` and
`max_budget_usd` have different meanings. Its complete terminal, usage and
lifecycle envelope must fit 8 KiB. It observes at most 256 messages/256 KiB.

The helper deadline defaults to 60 seconds and can be lowered or raised up to
five minutes. Stop closes admission and cancels a separate driver. Resource
acquisition, accepted-operation work and cleanup stay owned until they finish;
cleanup can outlive the deadline. The worker attempt/process timeout is final
containment. Interrupted observers do not authorize early model-resource closure.

Usage has an operation/source identity, coverage and an origin distinguishing
accepted replay from current dispatch. Replayed observations are not new charges;
assistant messages and run totals may overlap. Missing counts/cost stay unknown.
PydanticAI pricing estimates and imprecise SDK float costs are not presented as
exact reported decimal cost. No prices are inferred from tokens or duration.

For Claude, stop request, interrupt acknowledgement, native terminal status,
transport closure and observed process exit are separate evidence. An
acknowledgement or close does not prove a remote effect stopped. A session ID is
not a restorable checkpoint; `SESSION_RESUME` is absent.

Use [approvals](/how-to/require-approval) and
[provider recovery](/how-to/recover-provider-operations) in the controller for
reviewed external writes. The examples share that composition across frameworks.
See the [complete adapter contract](https://github.com/Ledgence/ledgence/blob/develop/docs/agent-integrations.md)
for exact API fields, error classes, limits, schema rules and credential separation.
