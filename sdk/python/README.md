![Ledgence](https://ledgence.com/brand/ledgence-logo-v1.png)

# Ledgence Python worker helper

The Rust worker supplies `ledgence.worker` with its Python runner. It supports
ordinary program handlers, invocation context and logs, and protocol 3 workflows
with registered entrypoints, durable local results, tasks, subworkflows, forks,
events, timers, and action approvals. The separately installed
[`ledgence.client`](../python-client/README.md) submits and observes work.

Use a runtime helper matching your worker. The helper is standard-library-only;
application integrations and their prepared dependencies belong in the program
package. No agent framework or model provider is required.

This README describes this package source. Check the
[release reference](https://docs.ledgence.com/reference/releases) for verified
registry availability. A source checkout may contain capabilities not yet
published. The APIs below require matching server and worker support.

## Install for local development

`ledgence-worker` requires Python 3.11+. In an existing uv project, install it
as a development dependency.

```sh
uv add --dev ledgence-worker
```

This selects an available registry version. For an existing deployment, select
the version matching its worker and pin that exact version in your dependency
configuration before synchronizing. Installing the helper does not check the
remote worker's compatibility. For pip, use `python -m pip install ledgence-worker`
in the application's activated virtual environment with the same version policy.
Use the source installation below for an unreleased checkout. The historical
`v0.4.0` source tag does not contain this package's `pyproject.toml`.

### Install from source

From your application's existing uv project, add the local package as a normal
development dependency, substituting your Ledgence checkout's absolute path:

```sh
uv add --dev /absolute/path/to/ledgence/sdk/python
uv run python -c "from ledgence.worker.workflow import Workflow; from ledgence.worker import current_invocation; print('worker imports ready')"
```

This installs the versioned helper into the project's environment and records
the local source in its dependency configuration. Select that environment in
your editor. No `PYTHONPATH` change is needed. Keep the checkout available for
future dependency synchronization, and use a helper version matching the deployed
worker release; installation does not check the deployed worker's compatibility.
A local source entry or lockfile does not freeze the directory's contents;
record and retain the exact checkout commit when sharing or reproducing a
development setup.

If you contribute to the helper itself, use
`uv add --dev --editable /absolute/path/to/ledgence/sdk/python` so edits to its
Python source are reflected directly. Ordinary program authors should use the
normal installation above. In an existing activated Python virtual environment,
the pip alternative is:

```sh
python -m pip install /absolute/path/to/ledgence/sdk/python
python -c "from ledgence.worker.workflow import Workflow; print('worker imports ready')"
```

The installed package includes inline type information and a
`ledgence/worker/py.typed` marker for compatible editor/type-checking tools. It
shares the native `ledgence` namespace with the independently installed
`ledgence-client`; neither distribution provides a root `ledgence/__init__.py`.

Use this environment to import your programs, register workflow entrypoints,
and test ordinary business functions. `current_invocation()` and
`workflow_context()` still require an active invocation; installing the package
does not create a local workflow execution harness. The package supplies no CLI,
server, Python client, or replacement for the worker's `--runner` configuration.
At execution, the Rust worker keeps supplying its matching bundled helper,
which the bootstrap loads before a copy vendored in a program artifact.

Keep `ledgence-worker` out of application runtime dependencies: the worker
supplies it. Prepare all actual application dependencies separately for the
worker's platform and exact Python major/minor before publishing a program.
See [Develop Python programs](../../docs-site/src/content/docs/how-to/develop-python-programs.md)
for a complete authoring example and [program packages](../../docs/program-packages.md)
for the runtime artifact contract.

## Durable model and tool calls

Protocol 3 workflows can give an application-owned model or tool call an explicit
durable identity without adopting an agent framework:

```python
from ledgence.worker.workflow import OperationKind

response = await ctx.operation(
    f"turn:{round}:model", call_model,
    kind=OperationKind.MODEL, version="adapter-1",
    arguments={
        "model": "demo-model-2026-01",
        "messages": messages,
        "temperature": 0,
        "max_tokens": 256,
        "tools": tool_schemas,
    },
)
result = await ctx.operation(
    f"turn:{round}:tool:0", lookup,
    kind=OperationKind.TOOL, version="lookup-1",
    arguments={"query": response["query"]},
)
```

`kind` requires an `OperationKind` member; plain strings and other enums are
rejected. `version` identifies the application adapter/tool behavior. Include the
effective provider/model identifier or snapshot, prompts, settings, tool schemas,
and other behavior-affecting configuration in `arguments`. A provider's mutable
model alias does not pin its underlying behavior. A model-generated tool request
is application input: validate and normalize it before selecting an allowed tool
and calling `operation`.

Before starting the callable, the helper binds its Python signature, materializes
keyword defaults, and freezes the effective JSON arguments. The callable receives
an independent copy as `fn(**effective_arguments)`. Each record binds the explicit
key, operation kind, original callable module/qualified name, version, and effective
arguments. Repeating that key in the same activation with the identical binding
reuses the owned execution or acknowledged result; a changed binding fails before
another call starts. Object order is ignored, but numbers such as `1`, `1.0`,
`True`, and positive/negative zero retain distinct bindings. Caller or callable
mutation cannot change the saved request, and each awaited result is a fresh copy.

Keep clients and credentials outside arguments; JSON requests and results are
stored as supplied. Sockets, SDK response objects, closures, and process memory
are not checkpointed. Application adapters convert completed responses to bounded
JSON. Partial streams, generators, and
provider exceptions are not successful records. The helper imports no model SDK
and performs no provider calls itself. Synchronous and asynchronous callables use
the same execution, cancellation, and acknowledgment rules as `ctx.local`.

The result is returned only after the existing Rust local-result commit is
acknowledged. An activation retry replays accepted results without deliberately
executing their callables. If an effect succeeds before its record is accepted,
it can repeat; pass a stable business idempotency key in the effective arguments
or reconcile with the external system. A lost or invalid acknowledgment prevents
successful completion of the original activation, even if the controller catches
the error. Durable recovery does not guarantee exactly-once provider execution.

Operation keys share the activation-local journal with `ctx.local`; their semantic
binding keeps them distinct from ordinary local calls. The approval-reserved key
prefix remains unavailable. Operations have the same limits: 128 records and
256 KiB combined per activation, 128 KiB per full binding/result record. The
versioned input envelope counts toward the ordinary JSON depth and byte limits.
Control flow remains ordinary Python: bound rounds and tool counts, then return
`ctx.continue_(continuation=..., state=...)` with the round, messages, and results
needed next. A new activation receives that explicit checkpoint and a new journal;
Python frames and ordinary locals are not restored. A local operation cannot
start another durable operation, stage children, or call workflow control APIs.

## Durable approvals

Protocol 3 workflows can return `ctx.request_approval(key, action=...,
continuation=..., state=..., timeout_ms=...)` to checkpoint an action and release
their worker slot. `resume=` is an alternative to `continuation=` and
`ctx.wait_approval(...)` is an alias. Normalize effective arguments before
requesting approval; optional `proposed_arguments` preserves the original input
as audit context. Every request has a finite deadline of at most 365 days.

Import `ApprovalAction` and `ApprovalStatus` from `ledgence.worker.workflow`.
`ApprovalAction.for_callable(fn, version="1", arguments={...})` binds the callable's
module and qualified name and freezes its keyword arguments, including defaults.
It performs Python signature binding; application validation and domain-specific
normalization remain the caller's responsibility. `ApprovalAction(name,
version="1", arguments={...})` supports actions defined by other adapters.

On the resumed entrypoint, `ctx.approval` is an immutable typed observation.
Check `ctx.approval.status is ApprovalStatus.APPROVED`, then
`await ctx.approved_local(fn, version="1")` to execute only the saved effective
arguments through a durable local step. Business argument names such as `key`
are passed to the callable exactly as approved. This helper accepts no replacement
arguments and verifies the callable identity/version. Copies returned by
`ctx.wake`, `ctx.approval.action.arguments`, or `ctx.approval.to_dict()` cannot
change its private execution binding. Rejected and expired requests resume with
their corresponding status; workflow cancellation closes the request without
resuming its controller. See the runnable
[durable approval example](../../examples/durable-approval/README.md).

Execution also checks that signature binding introduces no new effective defaults.
When constructing an `ApprovalAction` directly, include all effective arguments
before review; `approved_local` will not add an omitted, unreviewed default.

Approvals are for operator-trusted code. A Python callable remains responsible
for its behavior and external authorization. Approval does not make an external
effect exactly once: an effect can succeed before its durable local result is
acknowledged, so use external idempotency keys or reconciliation as needed.

Request a protected external write with
`ctx.request_effect_approval`, using the same parameters as `request_approval`.
In its approved continuation, execute
`await ctx.approved_effect(dispatch, version="1", lookup=read_receipt)`.
The saved `protection: "external_effect"` contract rejects `approved_local`
before execution or cached replay and rejects generic journal commits for that
occurrence. `ctx.approval.protection` exposes
`ApprovalProtection.EXTERNAL_EFFECT`. Untagged existing requests keep their
original behavior. Use matching components to review the protected request.
Only the first committed orchestrator grant permits the dispatch callback. If
an attempt dies before the grant commits, a retry can still receive that first
grant. After it commits, a retry without a saved result can only query the
destination. Both callbacks receive the frozen approved arguments. Ledgence
retains unresolved evidence even if the workflow ends. It saves the returned
receipt and closes that retention hold together. Return a validated JSON receipt, or
raise `EffectOutcomeUnknown` (from `ledgence.worker.workflow`) if the outcome
is inconclusive. Disable write retries inside your own adapter and never fall back
to `approved_local` for the same external write. See
[the refund contract and example](../../docs/workflow-approvals.md#protect-an-approved-external-write)
for migration, replay, and failure handling. The published 0.7.0 helper predates
protected-effect operations.

Matching authenticated workers negotiate the v2 evidence capability
before accepting work. The existing `approved_effect` call then records a unique
owned claim and reports the callback mode with its confirmation. Repeated awaits
share that invocation. A remotely accepted result can replay its original journal
binding without another callback or report. The helper receives only the runtime
capability flag, while Rust validates the authenticated receipt identities.
Historical and credentialless v1 operation keeps its existing wire contract.

The same managed-process lifecycle applies. Checkpointing drains owned calls and
cancellation may terminate them before a provider reply can be recorded. A separate
authenticated evidence operation is available to surviving Rust/HTTP callers with
a retained claim. See [effect evidence](../../docs/effect-evidence.md) for its
provenance, migration, inspection and retention contract.

<a id="native-operator-rendezvous-development"></a>

## Native operator rendezvous

Matching authenticated components support a native wait for an operator
assessment of an approved effect. Use your
named write and lookup callbacks in place of `issue_refund` and `find_refund`, and
add `Entry.REVIEW` to the existing workflow enum. Catch `EffectOutcomeUnknown`
and return the wait decision from the workflow entrypoint:

```python
from ledgence.worker.workflow import ApprovalStatus, EffectOutcomeUnknown, EffectResolutionOutcomeKind

@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    if ctx.approval.status is not ApprovalStatus.APPROVED:
        return ctx.fail("refund_not_approved", "A refund requires approval")
    try:
        receipt = await ctx.approved_effect(issue_refund, lookup=find_refund, version="1")
    except EffectOutcomeUnknown:
        return ctx.wait_effect_resolution("refund-review:0", resume=Entry.REVIEW, state={})
    return ctx.complete(receipt)

@workflow.entrypoint(Entry.REVIEW)
def review(event, ctx):
    outcome = ctx.effect_resolution.outcome
    if outcome.kind is EffectResolutionOutcomeKind.RECORDED_SUCCESS:
        return ctx.complete(outcome.result)
    if outcome.kind is EffectResolutionOutcomeKind.RECORDED_FAILURE:
        failure = outcome.failure
        return ctx.fail(failure["kind"], failure["message"])
    return ctx.fail("refund_unresolved", "The operator result remains unknown or disputed")
```

Register `Entry.REVIEW` in the workflow's entrypoint enum. The outcome describes
an operator assessment, not independent provider verification. Application code
chooses how that assessment affects its business workflow.

`wait_effect_resolution(key, *, resume, state, occurrence=None,
after_resolution_id=...)` checkpoints explicit JSON state and releases the
worker slot after owned work drains. It waits without a dedicated process or
open request. It has no timeout argument. Use the existing cancellation API when
an application needs to stop a pending review.

`ctx.effect_occurrence` returns the typed `EffectOccurrence` of the approved protected-effect
or resolution activation, or `None` when neither exists. An explicitly supplied
occurrence lets a later entrypoint refer to the original approved effect; the
server checks workflow and scope ownership. Reuse the same key across retries
of the same logical wait; an intentional later wait requires a new key.

When omitted, `after_resolution_id` selects the current resolution ID if this
activation resumed an assessment for the same occurrence, otherwise `None`.
A later wait therefore asks for a newer assessment. Explicit `None` permits
selection of the current assessment again. This is deliberate replay selection,
not a new dispatch. Keys for native effect waits and generic event waits occupy
separate namespaces; a generic event never satisfies a native wait.

On resume, `ctx.effect_resolution` is a typed `EffectResolutionWake` (otherwise
`None`). `.occurrence`, `.resolution_id`, `.accepted_revision`, `.key` and
`.outcome` are available directly; `.to_dict()` returns an independent copy of
the complete selected identity/revision/timestamp record. `EffectResolutionOutcomeKind`
has `RECORDED_SUCCESS`, `RECORDED_FAILURE`, `CLOSED_UNKNOWN` and `DISPUTED`.
The corresponding `.result`, `.failure` or `.first_conflicting_entry_sequence`
accessors reject the wrong variant. Results and failure values are detached JSON.

Selection happens durably in the coordinator, whether an operator resolves
before or after park. Attempts of the resumed activation share that frozen wake.
Later reports and assessments cannot rewrite it; request another assessment with
a new wait key when needed. A success selection can be disputed by a later unequal
report present at selection. Opaque report contents do not reinterpret failure
or unknown assessments. Cancellation and terminal execution prevent resumption.

The worker negotiates `native_effect_resolution.v1` before accepting work and
supplies the additive outer runtime feature manifest to this helper. Unsupported
servers/runtimes reject native waiting explicitly; there is no generic-event
fallback. Older helpers retain their existing behavior. An accepted assessment
releases the unresolved execution retention hold without confirming the journal;
all evidence and assessment receipts remain retained after ordinary collection.
See [operator resolution and public clients](../../docs/effect-evidence.md#record-an-operator-assessment).

<a id="provider-operations-development"></a>

## Provider operations

`ledgence.worker.providers` defines application-owned provider adapters. Register
`ProviderAdapter(adapter_id, version=..., lookup=ProviderLookup.BY_BINDING,
completion_criterion=...)`, decorate its dispatch and read-only probe callbacks,
and include it in `Workflow(Entry, providers=[adapter])`. `workflow.build()` freezes
the registry. Callbacks must be named functions inside the prepared application
artifact. The immutable owning program descriptor is part of the reviewed binding.

`adapter.binding(business=..., target=..., credential_reference=..., queue=...,
deduplication=None, reconciliation=None)` returns a `ProviderBindingRequest`.
`business` contains `namespace` and `key`. `target` contains `provider`,
`environment`, `account`, `api_variant`, `operation`, `endpoint_identity` and
`dimensions`. `deduplication`, when available, contains the provider's actual
`domain`, `key` and `horizon`. The optional `reconciliation` dictionary overrides
finite timing and check limits. The queue is supplied separately and must differ
from the controller queue.

| Helper | Contract |
| --- | --- |
| `ctx.request_provider_approval(key, provider=adapter, arguments=..., binding=..., resume=Entry.DISPATCH, state=..., timeout_ms=...)` | Checkpoint the complete intent for review. Effective request JSON lives only in the action arguments. |
| `await ctx.approved_provider_operation()` | Invoke the registered dispatch on the single original grant and return a durable `ProviderOperationTicket`. |
| `ctx.wait_provider_operation(key, ticket=ticket, resume=Entry.COMPLETE, state=...)` | Park until provider knowledge or an operator assessment is selected, releasing the worker. |
| `ctx.provider_operation_result` | Typed immutable selection on a provider resume, otherwise `None`. |

`ProviderOperationTicket.to_dict()` and `.from_dict(...)` support explicit saved
state. `ticket.occurrence` is an `EffectOccurrence`. An existing equal operation
can return the original ticket, including one owned by another workflow. Only
the owning workflow can wait for that occurrence. Changed action, program or
binding data conflicts with the existing business identity.

Every callback receives one `ProviderRequest`. Its `.arguments`, `.binding` and
`.target` return independent JSON copies. `.provider_reference` is the retained
provider reference available to this check. The helper passes a credential
reference through the binding. Application code resolves its credential value.

Return a `ProviderReport` using one of these constructors. Each takes the required
`target=...` and optional `provider_reference=...` and `provider_request_id=...`.

- `ProviderReport.unknown(reason, ...)`
- `ProviderReport.pending(...)`
- `ProviderReport.requires_action(action_kind, ...)`
- `ProviderReport.succeeded(result, ...)`
- `ProviderReport.failed(kind, message, ...)`
- `ProviderReport.cancelled(reason, ...)`

A success result may be `None`. Missing or malformed responses and transport
errors do not establish terminal provider failure. The report's target must
match the reviewed target. These reports retain adapter-classified provenance.
Disable automatic mutation retries and redirects in the adapter's HTTP client
when one network attempt is required. Ledgence grants dispatch once and never
retries that callback after an uncertain grant or result.

`ProviderLookup.UNAVAILABLE` permits dispatch-only registration. Recovery then
reaches needs-review handling without allocating status probes. Reference-only
lookup requires a retained provider reference. A probe invokes its registered
callback directly and receives no `WorkflowContext`.

`ProviderSelectionKind` distinguishes `PROVIDER_SUCCEEDED`, `PROVIDER_FAILED`,
`PROVIDER_CANCELLED`, `OPERATOR_ASSESSMENT`, `DISPUTED` and `NEEDS_REVIEW`.
Read `.result` only on provider success. `.selection` returns the complete selected
JSON, `.occurrence` identifies its original operation, and
`.selected_effect_revision` identifies the evidence boundary. A later report
cannot rewrite this selection. A follow-up `wait_provider_operation` can omit the
ticket after a provider wake, using its original occurrence and selected revision
as the default `after_effect_revision`. Supply a fresh stable key for each
intentional wait.

Callback results use the provider ledger. They never write an ordinary local-step
result. `approved_local` and `approved_effect` reject provider-protected approvals.
The runtime negotiates `provider_operation.v1`, with native effect resolution and
evidence v2 prerequisites. Ordinary tasks and historical A/B helpers preserve
their existing protocol.

See [provider operations](../../docs/provider-operations.md) for policy,
configuration, limits, restore quarantine and evidence retention. The
[asynchronous export example](../../examples/provider-recovery/README.md) includes
a real local provider and an independent mutation/effect census.

## Runtime helper

MIT-licensed, standard-library-only program support. The Rust worker starts the
configured CPython executable with `-I -S -B` and `ledgence/worker/bootstrap.py`. Python
is a separately installed runtime; Ledgence does not embed or download CPython.
Programs declare an exact supported Python major/minor (at least 3.11), with code
and vendored dependencies in their immutable artifact directory.

The worker supplies the `ledgence.worker` context helper alongside the runner;
applications do not need to vendor that helper. Application dependencies belong
in the program artifact. The bootstrap loads its own helper first, then adds the
absolute artifact directory to Python's import path. Bytecode generation is
explicitly disabled, so ordinary imports do not add `__pycache__` directories to
an artifact even when filesystem permissions would allow it.

`ledgence` is a native Python namespace package; it has no `__init__.py`.
Keep the complete `ledgence/worker` directory and its `ledgence` parent together
when distributing the runner. The separately installed `ledgence-client` SDK
provides `ledgence.client` in the same namespace without adding client dependencies
to the worker helper. A program using the client must include that SDK and its
prepared dependencies in its artifact, just like any other application dependency.
Do not add a root `ledgence/__init__.py` to either component or the artifact.

The public helper import is `ledgence.worker`, replacing the legacy
`ledgence_worker` path. Update existing program imports and publish a new immutable
program version/digest. The old import and runner path are not aliases; protocol
versions 1, 2, and 3 retain their wire behavior.

The worker sets a separate temporary working directory for each session. Relative
writes go there and persist across that session's invocations. Confirmed session
cleanup removes this directory. Read packaged resources relative to the program
module's `__file__`, not the working directory. Directory separation is file
lifecycle management and does not change OS access permissions.

Expose a callable as `module:function`; protocols 1/2 require a synchronous
handler, while protocol 3 also supports `async def`. Its module and package
parents must originate inside the artifact. Names already loaded by the bootstrap
(such as `json`, `os`, `ledgence`, and `ledgence.worker`) cannot be handler modules; conflicts
are rejected before readiness. Use an application-specific module name. Regular
and namespace packages are supported, and application code may still import the
standard library normally.

The callable receives the complete
CloudEvent dictionary and returns a JSON-compatible value. `current_invocation()`
provides transport execution identifiers when needed. The helper starts a fresh
`contextvars.Context` for every invocation; process/module globals intentionally
persist. Programs must clean up other invocation state and background work.

One process handles one invocation at a time. Results support null, booleans,
Unicode scalar strings, integers from `-2**63` through `2**64 - 1`, finite binary64
floating-point values, arrays, and objects with string keys. Python tuples are
encoded as JSON arrays. At most 64 nested containers are permitted; a scalar has
depth zero. Cycles, non-string keys, lone surrogate characters, out-of-range
integers, non-finite numbers, unawaited values, and oversized results produce typed
`invalid_output` failures and leave the process reusable. Rust checks the shared
wire profile when accepting the response.

The default protocol frame limit is 2 MiB, including result/input envelopes and
the newline. The worker supplies the configured limits explicitly when starting
the helper. The delivery submission limit remains 1 MiB of application data;
the additional frame capacity carries CloudEvent metadata and protocol fields.

Business exceptions produce typed failure results. Error text is shortened by the
complete frame's encoded UTF-8 byte budget, including event and attempt IDs, JSON
escaping, and the newline. The handler is not called if those IDs cannot fit even
an empty failure response. A protocol
fault terminates the process. Python and native stdout writes are redirected to
stderr; an isolated descriptor carries bounded JSON-lines responses. Parent-side
log capture must continue draining stderr even after its retention limit.

This process boundary executes trusted code with the worker's OS permissions.
The helper is not a sandbox and cannot undo external effects on retries.

## Protocol versions and contextual logs

Existing `runtime.protocol: 1` packages keep the original invoke/result protocol.
Protocol 2 adds contextual logs; protocol 3 also enables asynchronous handlers and
workflow control exchanges. The ready version must match the manifest; a mismatch
fails before calling the handler. Protocols 2/3 pass an invocation-local
`processing_context` outside the complete, unchanged CloudEvent. It is null when
worker tracing is disabled. It never replaces the event's origin `traceparent`.

`current_invocation()` provides `event_id`, `attempt_id`, `source`, `tenant_id`,
`namespace`, `run_id`, `task_id`, `attempt_no`, and the optional frozen W3C
`processing_context`. Optional `workflow_id` and `activation_id` come from envelope
extensions. Nested workflow invocations also expose `parent_workflow_id` and
`root_workflow_id`; root workflow invocations expose a null parent and their own
workflow ID as the root. Ordinary tasks outside workflows have neither. Protocol 3
logs include the paired ancestor IDs only for nested workflows, preserving older
root log shapes. These identifiers never enter user data. Context is reset on
success, business exception, and invalid
output. No invocation context is written to process-global environment variables.

```python
from ledgence.worker import current_invocation, get_logger

log = get_logger(__name__)

def handle(event):
    log.info("Invoice issued", extra={"attributes": {"invoice.id": event["data"]["invoice_id"]}})
    return {"task_id": current_invocation().task_id}
```

`get_logger` returns a normal `logging.Logger`, adding one Ledgence handler while
preserving existing handlers and root configuration. An unset logger level becomes
INFO. Each `LogRecord` is handled once by Ledgence, including when it propagates
through parent loggers configured with `get_logger`. Re-dispatching that same
record does not emit another Ledgence frame; separate logging calls create
independent records, even when their messages match. Application and root handlers
continue to receive records according to normal Python logging rules.
User attributes occupy a separate map. A record snapshots correlation and
attributes synchronously at emission; intentionally copied old contexts retain
their original IDs even if a background thread logs during a later invocation.
Outside an invocation, records carry process identity only unless the application
explicitly activates its own OTel span. Raw stdout/stderr remains process-level.

One dedicated writer in protocols 2/3 owns ready, result, log and closing frames. Its optional
queue is bounded to 64 records and 1 MiB; each log frame is at most 16 KiB or the
configured output limit, including the newline. Encoding bounds the complete
attribute tree (64 nodes, four nested levels, shared text budget). Oversized
optional content is shortened; a record whose correlation cannot fit is dropped.
Telemetry encoding errors and queue overflow drop logs and increment a saturating
local counter; they do not replace a handler result. This Python-side count stays
in the process: it is not carried in result or closing frames, durable task
history, or a collector export. Worker-side optional-record validation and output
budget drops have separate bounded diagnostics. Results/control have a
reserved priority slot. At most one already-writing bounded log precedes a newly
queued result; IPC itself still has backpressure. Closing discards remaining
optional records. Telemetry is best effort and may be lost on shutdown or crash.

## Optional OpenTelemetry API bridge

Programs may vendor `opentelemetry-api` and call
`from ledgence.worker.otel import enable_context; enable_context()` once at module
initialization. The bridge activates only the invocation's processing carrier
using the fixed W3C propagator, and attaches an empty context when it is null.
It resets the OTel context in `finally`. It neither installs a provider nor creates
a duplicate span for the worker's execution span. Logging inside application
child spans captures those children's active trace/span IDs.

The default helper imports no OpenTelemetry dependency. To record custom spans,
the application supplies and owns its SDK/provider and dependencies, with their
legal notices. Use bounded asynchronous processors for any application exporter.
Ledgence does not bundle a Python network exporter or serialize Python spans over
the result channel. The worker exports its own execution spans independently.

`register_shutdown(provider.shutdown)` optionally registers an application-owned
callback. Each registered callback runs once before the graceful closing ACK;
exceptions are reported on stderr and other callbacks continue. The parent's
existing process shutdown deadline bounds callbacks, including a hanging exporter.
Forced retirement/crashes do not promise a callback or complete flush. There is no
per-invocation flush or reliance on `atexit`.

The default tests require only the standard library. Optional API/SDK parentage
tests accept an already prepared dependency directory and make no network calls:

```sh
LEDGENCE_PYTHON_OTEL_TEST_PACKAGES=/path/to/reviewed-api-sdk-packages \
  python3 -m unittest discover -s sdk/python/tests -v
```

The reviewed test set is `opentelemetry-api==1.44.0`,
`opentelemetry-sdk==1.44.0`, `opentelemetry-semantic-conventions==0.65b0`, and
`typing_extensions==4.16.0`. The test exporter is in memory. Tests copy prepared
packages into the temporary artifact to exercise the same isolated import path as
real programs; nothing is installed at execution time.

## Registered workflow entrypoints (protocol 3)

Register ordinary Python handlers and export the built workflow as the package's
handler, for example `program:handle`:

```python
from enum import StrEnum
from ledgence.worker.workflow import Workflow

class Entry(StrEnum):
    START = "start"
    RESUME = "resume"

workflow = Workflow(Entry)

@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    return ctx.sleep("delay:0", 1000, continuation=Entry.RESUME,
                     state={"saved": event["data"]})

@workflow.entrypoint(Entry.RESUME)
def resume(event, ctx):
    return ctx.complete(ctx.state["saved"])

handle = workflow.build()
```

`Workflow` accepts a nonempty `StrEnum` family without aliases. Register every
member once and choose exactly one default. `build()` validates and freezes that
registry and returns an async handler; individual entrypoints may be sync or
async and receive `(event, ctx)`. Public submissions invoke the default. If the
enum declares the wire value `"start"`, it must be the default.

In a registered workflow, use members of that exact enum for branch and resume
targets, including approval continuations. `ctx.entrypoint` exposes the selected
member. No graph declaration or string-routing phase is required; Python code
chooses the next decision and explicitly saves any state it needs later.

For independent branches of this exact pinned package, `ctx.branch(key,
entrypoint=..., queue=..., data=...)` builds an immutable specification without
scheduling. `await ctx.fork(key, branches=[...])` durably registers those owned
workflows and returns a `ForkRef` while the parent stays in its current activation.
The parent can continue local work before returning
`ctx.join(group, resume=..., state=...)` to wait for every branch's terminal
outcome. Branch execution overlaps only when matching worker capacity is free.
Fork keys are workflow-wide, branch keys share the child namespace, and exact
retries reuse the original registration. See
[entrypoints and forks](../../docs/workflow-entrypoints.md) for a complete example,
reconciliation, and bounds.

## Explicit checkpoint workflows (protocol 3)

Existing controllers can also use
`from ledgence.worker.workflow import workflow_context` with string continuations and
return `ctx.suspend(...)`, `ctx.continue_(...)`, `ctx.wait_event(...)`,
`ctx.sleep(...)`, `ctx.request_approval(...)`, `ctx.complete(output)`, or
`ctx.fail(kind, message)`. `ctx.continuation` starts as `"start"`; `ctx.state` is
explicit JSON state and `ctx.inputs` is the frozen batch of child outcomes.
The complete CloudEvent, including user-owned `data`, remains the handler argument.

`await ctx.local(key, fn, **json_kwargs)` executes in the current process and
returns only after the Rust owner acknowledges durable storage of the result.
Calls sharing an activation-local key and the same callable/input reuse the result.
The callable does application work and may make ordinary nested Python calls.
It cannot call workflow control APIs, start another journaled local step, or stage
distributed tasks or subworkflows; the controller does those after awaiting its result. Otherwise
replaying the cached result would skip those workflow operations. This boundary
also applies through asynchronous child tasks and `asyncio.to_thread`.
Pass all changing inputs explicitly: closure variables and process memory are not
part of the persisted binding. Ordinary calls have no durable result record. An
external effect can still repeat after a crash before its commit acknowledgement;
use an external idempotency key for effects that require deduplication.

`ctx.local(...)` starts an owned operation immediately and returns an awaitable.
Use `await ctx.gather(ctx.local(...), ctx.local(...))` to overlap asynchronous I/O.
A synchronous callable retains its normal blocking semantics. The activation
retains ownership of started local operations and drains them before returning a
checkpoint, even when an observer stops awaiting one. An unobserved local failure
fails the activation; explicitly caught local errors remain under controller control.
A failed commit cannot be
caught and converted into successful workflow completion.

`ctx.task(key, program=..., version=..., queue=..., data=...)` stages a distributed
child command. The following checkpoint atomically registers those commands;
calling `task()` alone makes no network call. Child keys belong to the whole
workflow, and an identical binding reuses the original child. Use iteration
suffixes such as `"invoice:3"` to request a new child in a loop. A changed binding
for an existing key conflicts. `ctx.suspend(continuation=..., state=..., until=[ref])`
waits until all listed children are terminal; the next activation can read
`ctx.get_result(ref_or_key)` for successful output or inspect `ctx.inputs` for
failures. String keys can also refer to children from earlier activations.

`ctx.workflow(key, program=..., version=..., queue=..., data=...)` stages an owned
subworkflow with the same retry and attempt-timeout options as `ctx.task`. Both
methods return references that are not awaitable. Return a checkpoint to dispatch
work, mixing both reference kinds in `until` when needed:

```python
child = ctx.workflow("invoice-flow", program="invoice-flow", version="1.0.0",
                     queue="billing", data=event["data"])
summary = ctx.task("summary", program="summary", version="1.0.0",
                   queue="billing", data=event["data"])
return ctx.suspend(continuation="collect", state=None, until=[child, summary])
```

Tasks and workflows share the parent-wide key namespace. Reusing a key with a
different kind conflicts, as does changing its program, input, or scheduling
options. A child workflow wakes the parent only when the whole child workflow is
terminal. A completed controller activation by itself does not resolve the wait.
On resume, use the original string keys with `ctx.get_result(...)`, or inspect
`ctx.inputs[key]`: task results retain `task_id`, while workflow results have
`kind="workflow"` and `workflow_id`. Failed or cancelled children are inspectable
outcomes, and `get_result` raises for them.

Parent cancellation or failure drains owned descendants before becoming terminal.
Completion with unfinished children is rejected. A decision can stage at most 64
combined child commands and wait on at most 64 children. Each parent may have at
most 64 live subworkflows; nested depth is capped at 16 (root depth is zero).
See [`docs/subworkflows.md`](../../docs/subworkflows.md) for the lifecycle,
lineage, limits, and upgrade requirements.

External events and durable timers also use explicit checkpoint decisions:

```python
from ledgence.worker.workflow import workflow_context

def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        return ctx.wait_event(
            "callback:1", continuation="received", state={}, timeout_ms=60_000,
        )
    wake = ctx.wake
    if wake["kind"] == "event":
        return ctx.complete(wake["event"]["data"])
    return ctx.fail("callback_timeout", "No callback arrived before the deadline")
```

`ctx.wake` is `None` initially and when no external wait resumed the activation.
An event wake is `{"kind": "event", "key": ..., "event": <full CloudEvent>,
"accepted_at": <milliseconds>}`. Event timeouts carry `{"kind": "timeout",
"key": ..., "deadline": <milliseconds>}`; timers use `kind="timer"` with the same
key/deadline fields. Returned wake/state/input values are independent JSON copies.
`ctx.inputs` continues to contain only child outcomes. Event `data` and its original
context envelope are preserved separately from the controller invocation event.
Generic events provide application input; only `request_approval` and a durable
review decision grant the action-bound authority used by `approved_local`.

Return `ctx.sleep("retry:1", 5_000, continuation="retry", state={...})` to register
a durable timer. Both helpers stage the existing child commands in the same
checkpoint and end this activation; the Rust orchestrator owns the persisted wait
and later activation. They do not call `asyncio.sleep` or hold a worker slot until
the deadline. Timeout/delay values are integer milliseconds from zero through
31,536,000,000 (365 days). `timeout_ms=None` waits for an event without a deadline.
Timer deadlines are persisted by the orchestrator when the checkpoint is applied.

Wait keys are one-shot across the workflow run, including later activations.
Use a fresh key such as `"retry:2"` for the next loop iteration. Reusing a closed
key does not start another wait. Events can be accepted before their wait is
registered. An event wake carries at most 64 KiB of complete encoded CloudEvent;
child inputs plus wake share a 256 KiB encoded budget, and the entire activation
context retains its 640 KiB budget. Application event data retains depth64 while
workflow envelope metadata has separate room.

External event IDs are limited to 128 UTF-8 bytes and sources to 2,048 UTF-8
bytes, in addition to the complete event budget. Sources must be valid URI
references; encode non-ASCII URI characters with percent escapes.

Rust and Python may spell the same finite float differently. Reading a context
already accepted by Rust therefore allows a bounded encoding expansion: for each
float, at most `max(0, len(repr(value)) - 3)` additional bytes. Rust's canonical
float tokens have at least three bytes; CPython JSON uses that float representation
(and the helper bounds it at 32 bytes). Existing traversal, string, node and depth
checks still apply. The allowance covers received state, child inputs, event
wakes, journal records, copied getters and exact committed-step replay.

New decisions, child commands, local inputs/results and added journal entries
retain the strict Python encoding limits. Copying a near-boundary received value
into a new write, or adding to a ledger whose Python encoding expanded, can be
rejected conservatively even when Rust's representation would fit. Pure reading
and exact committed replay do not spend a new-write budget.

Each protocol 3 invocation uses a fresh event loop inside the reused Python
process. Create and close loop-bound clients inside the handler, rather than
saving them in module globals. Started asynchronous work is drained or cancelled
before the process is reused. While local operations run, the activation holds
one consumer/process slot; a committed suspension releases it for other tasks.

Unexpected controller exceptions are retryable activation runtime failures.
`ctx.fail(...)` explicitly requests workflow failure. Ordinary task output is
never interpreted as a workflow decision, even when it contains a `kind` field.

See [`docs/workflows.md`](../../docs/workflows.md) for the workflow contract,
checkpoint limits, cancellation, and recovery semantics.

## Durable resource admission (development source)

The development source adds provider-neutral admission for registered workflow
callbacks. It requires a credentialed worker/server connection advertising
`resource_admission.v1`, plus an operator-created resource policy. Admission is
independent of protected-effect evidence. It is not available merely because a
helper package is installed; see the release reference before choosing a
published version.

Register a module-level callback on the workflow, freeze its effective JSON
arguments before the checkpoint, and consume the typed outcome after resumption:

```python
from enum import StrEnum
from ledgence.worker.workflow import Workflow, OperationKind
from ledgence.worker.admission import AdmissionOutcomeKind

class Entry(StrEnum):
    START = "start"
    CALL = "call"

workflow = Workflow(Entry)

@workflow.operation(kind=OperationKind.TOOL, version="1")
async def format_reply(message, suffix="!"):
    # A synthetic callback; replace its body with your own awaited adapter.
    # Initialize credentials/clients inside the callback, outside JSON arguments.
    return {"reply": message.strip() + suffix}

@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    return ctx.request_admission(
        "reply:1", resource="reply-service",
        operation=format_reply.bind(message=event["data"]["message"]),
        resume=Entry.CALL, state=None, timeout_ms=60_000,
    )

@workflow.entrypoint(Entry.CALL)
async def call(event, ctx):
    outcome = await ctx.admitted_operation(format_reply)
    if outcome.kind == AdmissionOutcomeKind.SUCCEEDED:
        return ctx.complete(outcome.result)
    if outcome.kind == AdmissionOutcomeKind.WAIT_REQUIRED:
        return ctx.wait_admission(resume=Entry.CALL, state=None, timeout_ms=60_000)
    if outcome.kind == AdmissionOutcomeKind.NEEDS_REVIEW:
        return ctx.fail("admission_unknown", "Review the original operation's evidence")
    return ctx.fail("admission_closed", "Admission closed before this callback could run")

handle = workflow.build()
```

The first wait parks the workflow and frees the runtime process. An offer reserves
capacity; only the first accepted Start reply authorizes the callback. Its exact
callable, version, default-expanded arguments, resource and program artifact stay
bound to the original occurrence. `wait_admission` preserves that occurrence and
cannot extend its original deadline for admission or authorize a later Start.
After Start, a new bounded wait episode may wait for Finish or operator release
beyond that deadline. Its timeout yields `NEEDS_REVIEW` without releasing unknown
occupancy or granting another callback. A retry is not a new logical operation.

A completed callback reports its bounded JSON result through Finish. Accepted
results replay without Start or callback execution. This result is separate from
`ctx.local` records. Finish retries retain identical source and output; an uncertain
Start is never retried to obtain execution permission. Malformed or uncertain
runtime acknowledgements prevent the controller from committing a decision.

Exceptions, cancellation, worker loss and invalid outputs do not prove external
completion and do not release occupancy. Even if your controller explicitly
catches a callback exception, its source remains unresolved; completing the
workflow does not silently release it. Review evidence before an operator release.
A release is an audited capacity override, not proof of business success.

Registration freezes at `workflow.build()`. Lambdas, closures, bound instance
methods, replacement callbacks and callbacks outside the prepared artifact are
rejected. Pass only effective JSON parameters to `.bind`; do not serialize client
objects, credentials or live sessions. One authorization covers one bounded
callback, not every hidden HTTP request or SDK retry inside it. Configure those
retries explicitly or use an adapter with matching accounting. Async callbacks
should await their work; wrap blocking I/O appropriately. Callback code cannot
stage nested workflow operations through the Ledgence context.

Admission supports one unit of one named resource per request. Protected provider
dispatch uses the separate atomic composition below. Fairness, inherited budgets
and standalone task admission remain deferred. Ordinary callbacks are governed
only when explicitly registered and admitted; existing ungoverned execution paths
do not add admission RPCs.

### Approved provider operations with shared admission (development checkout)

The current development checkout adds atomic provider admission. It requires the
explicit `provider_admission.v1` capability; separate provider-operation and
resource-admission capabilities do not imply support. This capability is not part
of the published 0.8.0 helper.

Freeze a resource requirement into the provider intent before requesting human
approval:

```python
from ledgence.worker.providers import (
    ProviderAdmissionRequirement,
    ProviderDispatchOutcome,
    ProviderReport,
)

binding = exports.binding(
    business={"namespace": "exports", "key": export_key},
    target=target,
    credential_reference="exports-account",
    queue="provider-checks",
    admission=ProviderAdmissionRequirement(resource="export-dispatch"),
)
```

The resource is reviewed together with the pinned adapter, target and arguments.
After approval, return `ctx.request_provider_admission("export-capacity",
resume=Entry.DISPATCH, state=None, timeout_ms=60_000)`. This durable wait releases
the worker slot. It retains the original approved occurrence across subsequent
activations; neither the resumed helper nor an old dispatch helper can replace
its resource, adapter or arguments.

```python
from ledgence.worker.provider_admission import ProviderAdmissionOutcomeKind

@workflow.entrypoint(Entry.DISPATCH)
async def dispatch(event, ctx):
    admitted = await ctx.admitted_provider_operation()
    if admitted.kind in (
        ProviderAdmissionOutcomeKind.WAIT_REQUIRED,
        ProviderAdmissionOutcomeKind.NEEDS_REVIEW,
    ):
        return ctx.wait_admission(resume=Entry.DISPATCH, state=None, timeout_ms=60_000)
    if admitted.kind is ProviderAdmissionOutcomeKind.CLOSED:
        return ctx.fail("admission_closed", "Dispatch closed before a provider ticket was available")
    ticket = admitted.ticket
    if ticket.occurrence.workflow_id != ctx.workflow_id:
        return ctx.complete({"existing_operation": ticket.occurrence.to_dict()})
    return ctx.wait_provider_operation("export-result", ticket=ticket,
                                       resume=Entry.RESULT, state=None)
```

The ticket is a recovery handle, never permission to dispatch again. A retained
ticket replays without a Start exchange or callback. A deduplicated operation can
belong to another workflow; that ticket cannot be adopted as this workflow's
native provider wait. `ctx.wait_admission()` reuses the original admission
occurrence, so a new waiting episode cannot extend its pre-Start deadline.
An offer that expires before the atomic Start commits returns `WAIT_REQUIRED`
when the task still has authority, including expiry during database contention.
Tentative grants and rate charges are rolled back before that response. The SDK
does not retry Start to recover executable permission.

Governed dispatch callbacks return `ProviderDispatchOutcome.completed(report)`
or `ProviderDispatchOutcome.unknown(report)`. These describe the original call's
completion separately from the provider's business state:

- A complete HTTP 202 response can justify `completed(ProviderReport.pending(...))`:
  call occupancy is released while normal provider recovery continues.
- `unknown(ProviderReport.succeeded(...))` retains uncertain call occupancy even
  though the adapter reports a successful business outcome.
- Exceptions, cancellation and plain `ProviderReport` values from a governed
  dispatch do not invent completion or release capacity. Ordinary ungoverned
  callbacks and read-only probes continue returning plain `ProviderReport`.

The combined Start is sent once. Lost acknowledgement can consume the grant
without a physical provider call; it never grants another mutation on retry.
The combined Report freezes its linked sources and outcome and retries identical
bytes while execution control remains live. Large provider reports use the
provider ledger's existing limits, including results up to 128 KiB; they are not
converted into a generic admission's 64 KiB output.

Read-only reconciliation probes and evidence-only business observations do not
release the original call's occupancy. Unknown completion requires audited
administrative release or acceptance of the original frozen completed report;
an operator release does not stop remote work or replenish dispatch/recovery
budgets. An accepted `unknown` report cannot later be changed to `completed`
under the same report identity. This resource controls registered initial dispatch authorizations, not
all physical requests or hidden retries inside an adapter.

See the runnable [provider admission example](../../examples/provider-admission/README.md)
for an approval, resource wait, dispatch and business-result workflow using a
local synthetic provider.
