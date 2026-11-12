"""Private journal coordinator and invocation-owned resource lifecycle (MIT).

SDK adapters supply session open/run/request_stop/close hooks. Only the separate
run driver is cancellable. Owners and journal executors remain retained until
cleanup; this deliberately does not promise a hard external-stop deadline.
"""
import asyncio
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import inspect

from ..workflow import WorkflowContext, WorkflowError, OperationKind, _encode, _freeze, _text, _await_owned
from ._dependencies import load_dependencies
from ._types import (
    AdapterProfile, AgentCapability, AgentLimits, AgentRunSpec, AgentStatus,
    AgentResult, UsageSource, UsageOrigin, UsageCoverage, UsageObservation,
    LifecycleEvidence, AgentIntegrationError, AgentCapabilityError, AgentRunConflict,
    AgentProfileError, AgentBindingError, AgentLimitError, AgentExecutionError,
)

IDENTITY_RECIPE = "ledgence-agent-causal-v1"
INPUT_BYTES = 16 * 1024
OUTPUT_BYTES = 8 * 1024
RECORD_BYTES = 32 * 1024
MANIFEST_BYTES = 16 * 1024
LEDGER_BYTES = 192 * 1024
LEDGER_RECORDS = 64


def freeze(value, limit=INPUT_BYTES):
    try:
        return _freeze(value, limit)
    except WorkflowError as exc:
        raise AgentLimitError("json_bound_exceeded") from exc


def text(value, limit=128):
    try:
        return _text(value, "integration identity", limit)
    except WorkflowError as exc:
        raise AgentProfileError("invalid_identity") from exc


def causal_key(key, branch, relation, parent="START", position=""):
    digest = hashlib.sha256()
    for part in (IDENTITY_RECIPE, key, branch, relation, parent, str(position)):
        raw = part.encode("utf-8")
        digest.update(len(raw).to_bytes(4, "big"))
        digest.update(raw)
    return "agent:" + digest.hexdigest()


@dataclass(frozen=True, slots=True)
class PreparedCall:
    slot: str
    kind: OperationKind
    target: dict
    arguments: dict
    codec: str
    max_output_bytes: int = OUTPUT_BYTES


class JsonCodec:
    @staticmethod
    def encode(value): return value
    @staticmethod
    def decode(value): return value


class _Budget:
    def __init__(self):
        self.entries = {}
        self.parallel = asyncio.Semaphore(4)

    def reserve(self, key, size):
        if key in self.entries:
            return
        used = sum(self.entries.values())
        if len(self.entries) >= LEDGER_RECORDS or used + size > LEDGER_BYTES:
            raise AgentLimitError("adapter_journal_budget_exceeded", operation_key=key,
                                  reserved_bytes=used)
        self.entries[key] = size

    def settled(self, ctx, key):
        if key in ctx._records:
            self.entries[key] = len(_encode(ctx._records[key], RECORD_BYTES, 96,
                                            authoritative=True))
        elif ctx._fatal is None:
            self.entries.pop(key, None)


def _limits(value, whole_run):
    if type(value) is not AgentLimits:
        raise AgentProfileError("invalid_limits")
    resolved = asdict(value)
    for name, ceiling in (("max_model_turns", 4), ("max_tool_calls", 8), ("max_discoveries", 4)):
        number = resolved[name]
        if whole_run:
            if number is not None:
                raise AgentCapabilityError("operation_limit_unsupported")
        elif number is None:
            resolved[name] = ceiling
        elif type(number) is not int or not 1 <= number <= ceiling:
            raise AgentLimitError("operation_limit_invalid")
    for name, ceiling in (("max_parallel_calls", 4), ("timeout_ms", 300_000)):
        if type(resolved[name]) is not int or not 1 <= resolved[name] <= ceiling:
            raise AgentLimitError("run_limit_invalid")
    return AgentLimits(**resolved)


async def _manifest_anchor(*, request):
    # Generation happens inside the accepted local operation, never in its input.
    return {"created_at": datetime.now(timezone.utc).isoformat(),
            "conversation_id": request["occurrence"], "run_id": request["occurrence"]}


class _RunResult:
    def __init__(self, state):
        self._state = state

    async def _result(self):
        state = self._state
        try:
            result = await _await_owned(state.task)
            return replace(result, output=freeze(result.output, OUTPUT_BYTES))
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                state.request_stop()
            if (state.task.done() and not state.task.cancelled()
                    and state.task.exception() is exc):
                state.ctx._observed_failures.add(state.task)
            raise

    def __await__(self): return self._result().__await__()


class RunState:
    """Internal port for adapters; not a second persistence authority."""
    def __init__(self, ctx, spec, prompt, profile, dependencies, normalization,
                 runtime_identity, whole_run):
        self.ctx, self.spec, self.prompt, self.profile = ctx, spec, prompt, profile
        self._slots = set()
        self.whole_run = whole_run
        self._configuration = freeze(spec.configuration, 4096)
        self.anchor = None
        self.manifest = {
            "identity": IDENTITY_RECIPE, "occurrence": self.slot("MANIFEST"),
            "key": spec.key, "branch": spec.branch, "version": spec.version,
            "configuration": self.configuration, "prompt": prompt,
            "limits": asdict(spec.limits),
            "profile": {"name": profile.name, "version": profile.version,
                        "capabilities": sorted(c.value for c in profile.capabilities)},
            "dependencies": {name: version for name, version, _ in dependencies},
            "normalization": normalization, "runtime": runtime_identity,
        }
        freeze(self.manifest, MANIFEST_BYTES - 2048)
        self.task = self.driver = self.session = None
        self.children = set()
        self.controls = set()
        self.calls = {}
        self.categories = {name: set() for name in ("model", "tool", "discovery", "whole_run")}
        self._usage = {}
        self._wire = {}
        self._replays = set()
        self._dispatched = set()
        self.poisoned = None
        self.stopped = False
        self.stopping = asyncio.Event()
        self._closing = False
        self._driver_cancelled = False
        self._stop_control_started = False
        self.lifecycle = LifecycleEvidence()
        self.parallel = asyncio.Semaphore(spec.limits.max_parallel_calls)
        if ctx._integration_budget is None:
            ctx._integration_budget = _Budget()
        self.budget = ctx._integration_budget
        self._timer = None

    @property
    def configuration(self): return freeze(self._configuration, 4096)

    def slot(self, relation, parent="START", position=""):
        key = causal_key(self.spec.key, self.spec.branch, relation, parent, position)
        if key not in self._slots and len(self._slots) >= 128:
            raise self.poison(AgentLimitError("causal_slot_bound_exceeded"))
        self._slots.add(key)
        return key

    def wire(self, slot): return freeze(self._wire[slot], OUTPUT_BYTES)
    def replayed(self, slot): return slot in self._replays

    def poison(self, error):
        if self.poisoned is None:
            self.poisoned = error
        return self.poisoned

    def check(self):
        if self.poisoned is not None:
            raise self.poisoned
        if self.stopped or self._closing:
            raise AgentExecutionError("run_stopping", lifecycle=self.lifecycle,
                                      external_work_may_have_occurred=bool(self._dispatched))

    def request_stop(self, *, deadline=False):
        if self.task is not None and self.task.done():
            return
        if deadline:
            self.poison(AgentExecutionError("deadline_exceeded", lifecycle=self.lifecycle,
                                            external_work_may_have_occurred=bool(self._dispatched)))
        self.stopped = True
        self.stopping.set()
        self.lifecycle = replace(self.lifecycle, stop_requested=True)
        # R14: retain manifest/open/cleanup. Only a separate SDK driver may be cancelled.
        if (self.driver is not None and not self.driver.done() and not self.whole_run
                and not self._driver_cancelled):
            self._driver_cancelled = True
            self.driver.cancel()
        if self.session is not None and not self._closing and not self._stop_control_started:
            self._stop_control_started = True
            self.controls.add(asyncio.create_task(self._request_native_stop()))

    async def _request_native_stop(self):
        try:
            await self.session.request_stop()
        except Exception as exc:
            error = AgentExecutionError("stop_request_failed", lifecycle=self.lifecycle,
                                        external_work_may_have_occurred=bool(self._dispatched))
            error.__cause__ = exc
            self.poison(error)
            raise error

    def _adopt(self, task):
        if task in self.ctx._integration_children and task not in self.children:
            raise AgentBindingError("operation_has_another_owner")
        self.children.add(task)
        self.ctx._integration_children.add(task)

    def _settled(self, key, task):
        try:
            self.budget.settled(self.ctx, key)
            if task.cancelled():
                self.poison(AgentExecutionError("owned_operation_cancelled", operation_key=key,
                                                external_work_may_have_occurred=key in self._dispatched))
            elif task.exception() is not None:
                exc = task.exception()
                error = exc if isinstance(exc, AgentIntegrationError) else AgentExecutionError(
                    "operation_not_accepted", operation_key=key,
                    external_work_may_have_occurred=key in self._dispatched)
                if error is not exc:
                    error.__cause__ = exc
                self.poison(error)
        except Exception as exc:
            self.poison(AgentExecutionError("journal_accounting_failed", operation_key=key,
                                            external_work_may_have_occurred=key in self._dispatched))
            self.poisoned.__cause__ = exc

    @staticmethod
    def _reservation(key, name, persisted_input, output_limit, record_limit):
        binding = {"key": key, "callable": name, "input": persisted_input, "output": None}
        size = len(_encode(binding, record_limit, 96)) - 4 + output_limit
        if size > record_limit:
            raise AgentLimitError("operation_record_bound_exceeded", operation_key=key)
        return size

    def _check_binding(self, key, name, persisted_input):
        previous = self.ctx._records.get(key)
        if previous is not None:
            saved = {field: previous[field] for field in ("key", "callable", "input")}
            expected = {"key": key, "callable": name, "input": persisted_input}
            if _encode(saved, RECORD_BYTES, 96, authoritative=True) != _encode(expected, RECORD_BYTES, 96):
                raise AgentBindingError("operation_binding_changed", operation_key=key)

    async def _open_manifest(self):
        key = self.slot("MANIFEST")
        # Gate the callback so even an eager task factory cannot dispatch before adoption.
        gate = self.ctx._loop.create_future()
        async def initialize(*, request):
            await gate
            return await _manifest_anchor(request=request)
        name = initialize.__module__ + ":" + initialize.__qualname__
        arguments = {"request": freeze(self.manifest, MANIFEST_BYTES - 2048)}
        self._check_binding(key, name, arguments)
        reserve = self._reservation(key, name, arguments, 1024, MANIFEST_BYTES)
        self.budget.reserve(key, reserve)
        try:
            result = self.ctx.local(key, initialize, **arguments)
        except BaseException:
            self.budget.settled(self.ctx, key)
            raise
        self._adopt(result._task)
        result._task.add_done_callback(lambda task: self._settled(key, task))
        gate.set_result(None)
        # Raw task: owner drainage must not mark a child failure observed on behalf of a user.
        self.anchor = freeze(await _await_owned(result._task), 1024)

    async def execute(self, prepared, executor, codec=JsonCodec, *, category=None):
        """Dispatch the copied effective envelope once; replay decodes fresh objects."""
        self.check()
        try:
            if type(prepared) is not PreparedCall or type(prepared.kind) is not OperationKind:
                raise AgentProfileError("invalid_prepared_call")
            key = text(prepared.slot)
            if key not in self._slots:
                raise AgentBindingError("causal_slot_belongs_to_another_run", operation_key=key)
            text(prepared.codec)
            if (type(prepared.max_output_bytes) is not int
                    or not 1 <= prepared.max_output_bytes <= OUTPUT_BYTES):
                raise AgentLimitError("output_bound_invalid")
            category = category or ("model" if prepared.kind is OperationKind.MODEL else "tool")
            if category not in self.categories or (self.whole_run != (category == "whole_run")):
                raise AgentProfileError("operation_category_unsupported")
            if type(prepared.target) is not dict or type(prepared.arguments) is not dict:
                raise AgentProfileError("invalid_prepared_call")
            envelope = freeze({"profile": self.profile.name, "version": self.profile.version,
                               "codec": prepared.codec, "target": prepared.target,
                               "arguments": prepared.arguments}, INPUT_BYTES)
            fingerprint = _encode({"kind": prepared.kind.value, "envelope": envelope,
                                   "category": category, "output_limit": prepared.max_output_bytes},
                                  INPUT_BYTES + 512)
            if key in self.calls:
                known, task = self.calls[key]
                if known != fingerprint:
                    raise AgentBindingError("operation_binding_changed", operation_key=key)
            else:
                if key in self.ctx._pending:
                    raise AgentBindingError("operation_has_another_owner", operation_key=key)
                ceilings = {"model": self.spec.limits.max_model_turns,
                            "tool": self.spec.limits.max_tool_calls,
                            "discovery": self.spec.limits.max_discoveries, "whole_run": 1}
                if len(self.categories[category]) >= ceilings[category]:
                    raise AgentLimitError("operation_count_exceeded", operation_key=key)
                gate = self.ctx._loop.create_future()
                async def invoke(*, request):
                    await gate
                    async with self.parallel, self.budget.parallel:
                        self.check()
                        self._dispatched.add(key)
                        try:
                            native = executor(request=freeze(request, INPUT_BYTES))
                            if inspect.isawaitable(native):
                                native = await native
                            if self.ctx._fatal is not None:
                                raise AgentExecutionError("workflow_runtime_failed", operation_key=key,
                                                          external_work_may_have_occurred=True) from self.ctx._fatal
                            value = codec.encode(native)
                            return freeze(value, prepared.max_output_bytes)
                        except AgentIntegrationError as exc:
                            exc.external_work_may_have_occurred = True
                            exc.operation_key = key
                            self.poison(exc)
                            raise
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            error = AgentExecutionError("callback_failed", operation_key=key,
                                                        external_work_may_have_occurred=True)
                            self.poison(error)
                            raise error from exc
                name = prepared.kind.value + ":" + invoke.__module__ + ":" + invoke.__qualname__
                persisted = {"v": 1, "kind": prepared.kind.value, "version": self.profile.version,
                             "arguments": {"request": envelope}}
                self._check_binding(key, name, persisted)
                self.budget.reserve(key, self._reservation(key, name, persisted,
                                                          prepared.max_output_bytes, RECORD_BYTES))
                if key in self.ctx._records:
                    self._replays.add(key)
                try:
                    result = self.ctx.operation(key, invoke, kind=prepared.kind,
                                                version=self.profile.version,
                                                arguments={"request": envelope})
                except BaseException:
                    self.budget.settled(self.ctx, key)
                    raise
                task = result._task
                self.calls[key] = (fingerprint, task)
                self.categories[category].add(key)
                self._adopt(task)
                task.add_done_callback(lambda done: self._settled(key, done))
                gate.set_result(None)
            value = await _await_owned(task)
            self._wire[key] = freeze(value, prepared.max_output_bytes)
            try:
                return codec.decode(freeze(value, prepared.max_output_bytes))
            except getattr(codec, "control_exceptions", ()):
                raise
            except Exception as exc:
                raise AgentProfileError("result_codec_failed", operation_key=key,
                                        external_work_may_have_occurred=key in self._dispatched) from exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, getattr(codec, "control_exceptions", ())):
                raise
            error = exc if isinstance(exc, AgentIntegrationError) else AgentExecutionError(
                "operation_not_accepted", operation_key=getattr(prepared, "slot", None),
                external_work_may_have_occurred=getattr(prepared, "slot", None) in self._dispatched)
            self.poison(error)
            if error is exc:
                raise
            raise error from exc

    def observe_usage(self, observation):
        try:
            if type(observation) is not UsageObservation:
                raise ValueError
            text(observation.operation_key)
            text(observation.source_id, 256)
            if (type(observation.source_kind) is not UsageSource or type(observation.origin) is not UsageOrigin
                    or type(observation.coverage) is not UsageCoverage):
                raise ValueError
            for value in (observation.input_tokens, observation.output_tokens, observation.total_tokens):
                if value is not None and (type(value) is not int or not 0 <= value < 1 << 64):
                    raise ValueError
            if observation.reported_cost is not None:
                value = observation.reported_cost
                if type(value) is not str or not value.isascii() or not 1 <= len(value) <= 64:
                    raise ValueError
                cost = Decimal(value)
                if not cost.is_finite() or cost < 0:
                    raise ValueError
            if observation.currency is not None:
                text(observation.currency, 16)
                if not observation.currency.isascii() or observation.reported_cost is None:
                    raise ValueError
            key = (observation.operation_key, observation.source_kind, observation.source_id)
            old = self._usage.get(key)
            if old is not None and old != observation:
                # Do not fabricate totals from disagreeing observations.
                observation = replace(old, input_tokens=None, output_tokens=None, total_tokens=None,
                                      reported_cost=None, currency=None, coverage=UsageCoverage.PARTIAL)
            if old is None and len(self._usage) >= 256:
                raise ValueError
            self._usage[key] = observation
        except (ValueError, InvalidOperation, AgentIntegrationError) as exc:
            raise self.poison(AgentProfileError("usage_observation_invalid")) from exc

    def result(self, status, output, native=None, *, lifecycle=None):
        if type(status) is not AgentStatus:
            raise self.poison(AgentProfileError("invalid_terminal_status"))
        return AgentResult(status, freeze(output, OUTPUT_BYTES), native, self.profile,
                           tuple(self._usage.values()), lifecycle or replace(self.lifecycle, terminal_status=status))

    async def _drain_children(self):
        while True:
            tasks = self.children | self.controls
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if tasks == self.children | self.controls:
                return

    async def _run(self, ready, session_factory, factory):
        await ready
        result = None
        failure = None
        try:
            await self._open_manifest()
            self.check()
            # Construct a resource-free session holder before any SDK acquisition.
            self.session = session_factory(self, factory)
            await self.session.open()
            self.check()
            self.driver = asyncio.create_task(self.session.run())
            result = await self.driver
        except BaseException as exc:
            failure = exc
        finally:
            self._closing = True
            await self._drain_children()
            if self.session is not None:
                try:
                    await self.session.close()
                except BaseException as exc:
                    error = AgentExecutionError("resource_close_failed", lifecycle=self.lifecycle,
                                                external_work_may_have_occurred=bool(self._dispatched))
                    error.__cause__ = exc
                    self.poison(error)
            self._timer.cancel()
        if self.poisoned is not None:
            if isinstance(self.poisoned, AgentIntegrationError):
                # Errors may be recorded before stop acknowledgement or close.
                # Deliver the final evidence only after retained cleanup settles.
                self.poisoned.lifecycle = self.lifecycle
            raise self.poisoned
        if failure is not None:
            if isinstance(failure, AgentIntegrationError):
                failure.lifecycle = self.lifecycle
                raise failure
            if isinstance(failure, asyncio.CancelledError):
                raise failure
            raise AgentExecutionError("agent_run_failed", lifecycle=self.lifecycle,
                                      external_work_may_have_occurred=bool(self._dispatched)) from failure
        if self.stopped:
            raise asyncio.CancelledError
        if type(result) is not AgentResult or result.profile != self.profile:
            raise AgentProfileError("invalid_agent_result")
        evidence = result.lifecycle
        if type(evidence) is not LifecycleEvidence:
            raise AgentProfileError("invalid_lifecycle_evidence")
        if evidence.origin is UsageOrigin.CURRENT_DISPATCH:
            evidence = replace(self.lifecycle, terminal_status=result.status)
        if (type(evidence.origin) is not UsageOrigin or type(evidence.stop_requested) is not bool
                or evidence.checkpoint_verified is not False
                or (evidence.terminal_status is not None and type(evidence.terminal_status) is not AgentStatus)):
            raise AgentProfileError("invalid_lifecycle_evidence")
        for value in (evidence.stop_acknowledged, evidence.transport_closed, evidence.local_process_exit_observed):
            if value is not None and type(value) is not bool:
                raise AgentProfileError("invalid_lifecycle_evidence")
        if evidence.session_id is not None:
            text(evidence.session_id, 256)
        return replace(result, output=freeze(result.output, OUTPUT_BYTES),
                       usage=tuple(self._usage.values()), lifecycle=evidence)


def start_run(ctx, *, spec, prompt, factory, profile, dependencies, normalization,
              session_factory, require=frozenset(), runtime_identity=None, whole_run=False):
    """Synchronous claim/registration; adapters must not wrap this in async def."""
    if not isinstance(ctx, WorkflowContext):
        raise AgentProfileError("workflow_controller_required")
    ctx._active()
    if ctx._finish_task is not None or ctx._finish_cancelled:
        raise AgentProfileError("workflow_closing")
    if type(spec) is not AgentRunSpec or type(spec.configuration) is not dict:
        raise AgentProfileError("invalid_run_spec")
    text(spec.key); text(spec.branch); text(spec.version)
    try:
        if type(prompt) is not str or len(prompt.encode("utf-8")) > 4096:
            raise AgentLimitError("prompt_bound_exceeded")
    except UnicodeError as exc:
        raise AgentProfileError("invalid_prompt_unicode") from exc
    if (type(profile) is not AdapterProfile or type(profile.capabilities) is not frozenset
            or any(type(cap) is not AgentCapability for cap in profile.capabilities)):
        raise AgentProfileError("invalid_profile")
    text(profile.name); text(profile.version)
    if type(require) is not frozenset or any(type(cap) is not AgentCapability for cap in require):
        raise AgentCapabilityError("invalid_required_capabilities")
    if not require <= profile.capabilities:
        raise AgentCapabilityError("required_capability_unsupported")
    if not callable(factory) or inspect.iscoroutinefunction(factory):
        raise AgentProfileError("factory_must_be_synchronous")
    spec = replace(spec, configuration=freeze(spec.configuration, 4096), limits=_limits(spec.limits, whole_run))
    key = (spec.key, spec.branch)
    if key in ctx._integration_owners:
        raise AgentRunConflict("run_occurrence_already_claimed")
    if len(ctx._integration_owners) >= LEDGER_RECORDS:
        raise AgentLimitError("run_count_exceeded")
    load_dependencies(dependencies)
    state = RunState(ctx, spec, prompt, profile, dependencies, text(normalization),
                     freeze(runtime_identity, 4096), whole_run)
    ready = ctx._loop.create_future()
    ctx._integration_owners[key] = state
    state.task = asyncio.create_task(state._run(ready, session_factory, factory))
    state._timer = ctx._loop.call_later(spec.limits.timeout_ms / 1000,
                                      lambda: state.request_stop(deadline=True))
    ready.set_result(None)
    return _RunResult(state)
