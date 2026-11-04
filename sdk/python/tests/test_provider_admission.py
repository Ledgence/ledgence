"""Composed admission never turns recovery evidence into another provider grant (MIT)."""

import asyncio
import unittest
from copy import deepcopy
from unittest.mock import patch

import test_admission as generic
import test_provider_operations as ordinary
import test_workflow
from ledgence.worker.provider_admission import (
    ProviderAdmissionOutcomeKind,
    matches_record,
)
from ledgence.worker.providers import (
    ProviderAdapter,
    ProviderAdmissionRequirement,
    ProviderBindingRequest,
    ProviderDispatchOutcome,
    ProviderLookup,
    ProviderReport,
    _call,
    _validate_binding,
)
from ledgence.worker.workflow import WorkflowContext, WorkflowError

CALLS = []
MODE = "pending"
GATE = None
CTX = None


async def dispatch(request):
    CALLS.append(request.arguments)
    if GATE is not None:
        await GATE.wait()
    if MODE == "raise":
        raise RuntimeError("call completion unknown")
    if MODE == "nested":
        return CTX.complete(None)
    if MODE == "plain":
        return ProviderReport.pending(target=request.target)
    if MODE == "unknown":
        return ProviderDispatchOutcome.unknown(
            ProviderReport.succeeded({"ok": True}, target=request.target)
        )
    if MODE == "large":
        return ProviderDispatchOutcome.completed(
            ProviderReport.succeeded({"data": "a" * 100_000}, target=request.target)
        )
    return ProviderDispatchOutcome.completed(
        ProviderReport.pending(target=request.target, provider_reference="export-1")
    )


def sync_dispatch(request):
    CALLS.append(request.arguments)
    return ProviderDispatchOutcome.completed(
        ProviderReport.pending(target=request.target)
    )


def adapter():
    value = ProviderAdapter(
        "example.export",
        version="1",
        lookup=ProviderLookup.BY_BINDING,
        completion_criterion="Export is available",
    )
    value.dispatch(dispatch)
    value.probe(ordinary.probe)
    return value


def binding(provider, *, resolved=True):
    value = provider.binding(
        business={"namespace": "exports", "key": "export-1"},
        target=ordinary.target(),
        credential_reference="local",
        queue="probes",
        admission=ProviderAdmissionRequirement("model"),
    ).to_dict()
    if resolved:
        value["program"] = {
            "program": {"id": "workflow", "version": "1"},
            "digest": generic.DIGEST,
            "size": 100,
        }
    return value


def wake(provider):
    value = binding(provider)
    occurrence = {
        "scope": generic.SCOPE,
        "workflow_id": "workflow-1",
        "origin_activation_id": "origin",
        "key": "call",
    }
    ticket = {
        "occurrence": {
            "scope": generic.SCOPE,
            "workflow_id": "workflow-1",
            "activation_id": "origin",
            "approval_key": "refund",
        },
        "binding_digest": generic.DIGEST,
    }
    return {
        "kind": "resource_admission",
        "key": "capacity",
        "occurrence": occurrence,
        "waiting_activation_id": "origin",
        "resumed_activation_id": "activation-1",
        "resumed_revision": 1,
        "binding": {
            "resource": "model",
            "program": value.pop("program"),
            "provider": {
                "ticket": ticket,
                "binding": value,
                "action": {
                    "name": value["contract"]["dispatch_callable"],
                    "version": "1",
                    "arguments": {"amount": 50},
                },
            },
        },
        "selected_at": 1000,
        "selection": {
            "kind": "offered",
            "offer": {
                "occurrence": occurrence,
                "generation": 1,
                "policy_revision": 1,
                "activation_id": "activation-1",
                "expires_at": 10000,
                "binding_digest": generic.DIGEST,
            },
        },
    }


def source(selected, start_id="start", finish_id="finish"):
    admission = generic.source(selected, start_id, finish_id)
    return {
        "admission": admission,
        "provider": {
            "source_id": "provider-source",
            "claim_id": start_id,
            "report_id": finish_id,
            "ticket": selected["binding"]["provider"]["ticket"],
            "worker": admission["worker"],
            "accepted_at": 1000,
            "first_possible_dispatch_at": 1000,
            "reconciliation_deadline": 100000,
        },
    }


def report(source, completion="completed"):
    provider = source["provider"]
    return {
        "source": source,
        "call_completion": completion,
        "receipt": {
            "ticket": provider["ticket"],
            "source_id": provider["source_id"],
            "source": {"kind": "dispatch", "claim_id": provider["claim_id"]},
            "report_id": provider["report_id"],
            "report_digest": generic.DIGEST,
            "worker": provider["worker"],
            "accepted_at": 2000,
            "recorded_revision": 2,
            "accepted_via": "evidence_only",
            "already_accepted": False,
        },
    }


def record(selected, *, completed=False):
    value = source(selected)
    return {
        "ticket": value["provider"]["ticket"],
        "source": value,
        "report": report(value) if completed else None,
    }


def snapshot(selected, *, retained=None, state="awaiting_rewait"):
    return {
        "occurrence": selected["occurrence"],
        "binding": selected["binding"],
        "state": state,
        "revision": 2,
        "created_at": 900,
        "updated_at": 2000,
        "deadline_at": 100000,
        "offer": None,
        "source": None
        if retained is None or retained["source"] is None
        else retained["source"]["admission"],
        "finish": None,
        **({"provider": retained} if retained is not None else {}),
    }


class ProviderAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        global MODE, GATE, CTX
        MODE, GATE, CTX = "pending", None, None
        CALLS.clear()
        self.provider = adapter()
        self.handler = ordinary.registry(self.provider)
        self.wake = wake(self.provider)
        self.contexts, self.calls = [], []
        self.mutate = None
        self.existing = None

    async def asyncTearDown(self):
        for ctx in self.contexts:
            await ctx._finish(cancel=True)

    async def rpc(self, op, payload):
        self.calls.append((op, deepcopy(payload)))
        if op == "provider_admission.start.v1":
            result = (
                {
                    "kind": "existing",
                    "audience": source(self.wake)["admission"]["worker"]["audience"],
                    "snapshot": self.existing,
                }
                if self.existing is not None
                else {"kind": "granted", "source": source(self.wake, **payload)}
            )
        elif op == "provider_admission.report.v1":
            result = report(payload["source"], payload["outcome"]["call_completion"])
        else:
            raise AssertionError(
                "governed callback used a non-composed runtime operation"
            )
        if self.mutate:
            self.mutate(op, result)
        return result

    def context(self, *, wake=None, retained=None, raw=None, enabled=True, rpc=None):
        value = raw or test_workflow.payload(
            revision=1, wake=deepcopy(self.wake if wake is None else wake)
        )
        if retained is not None:
            value["provider_admission"] = retained
        ctx = WorkflowContext(
            value,
            rpc or self.rpc,
            effect_evidence_v2=True,
            native_effect_resolution=True,
            provider_operation=True,
            resource_admission=True,
            provider_admission=enabled,
        )
        ctx._providers = self.handler._ledgence_providers
        self.contexts.append(ctx)
        return ctx

    async def test_requirement_is_frozen_versioned_and_v1_bytes_do_not_gain_a_field(
        self,
    ):
        old = ordinary.binding(self.provider, resolved=False)
        self.assertEqual(old["v"], 1)
        self.assertNotIn("admission", old)
        self.assertEqual(ProviderBindingRequest.from_dict(old).to_dict(), old)
        value = binding(self.provider, resolved=False)
        self.assertEqual(value["admission"], {"resource": "model"})
        for changed in [
            dict(value, v=1),
            {k: v for k, v in value.items() if k != "admission"},
            dict(value, admission=None),
            dict(old, admission=None),
        ]:
            with self.assertRaises(WorkflowError):
                _validate_binding(changed, resolved=False)
        with self.assertRaises(WorkflowError):
            ProviderAdmissionRequirement("invalid/resource")

    async def test_native_request_uses_approved_key_and_rejects_old_dispatch_helper(
        self,
    ):
        raw = ordinary.approval(self.provider)
        raw["provider_binding"] = binding(self.provider)
        ctx = self.context(
            raw=test_workflow.payload(
                revision=1, wake={"kind": "approval", "approval": raw}
            )
        )
        decision = ctx.request_provider_admission(
            "capacity", resume="done", state=None, timeout_ms=5000
        )
        self.assertEqual(
            decision["wait"]["request"], {"kind": "provider", "approval_key": "refund"}
        )
        self.assertEqual(self.calls, [])
        with self.assertRaises(WorkflowError):
            ctx.approved_provider_operation()
        with self.assertRaises(WorkflowError):
            self.context(
                raw=test_workflow.payload(
                    revision=1, wake={"kind": "approval", "approval": raw}
                ),
                enabled=False,
            )

    async def test_explicit_composed_capability_and_generic_callback_bypass_fail_before_rpc(
        self,
    ):
        with self.assertRaises(WorkflowError):
            self.context(enabled=False)
        ctx = self.context()
        with self.assertRaises(WorkflowError):
            ctx.admitted_operation(None)
        self.assertEqual(self.calls, [])
        self.assertEqual(CALLS, [])

    async def test_registered_callback_cannot_be_replaced_after_binding(self):
        ctx = self.context()
        self.provider._dispatch = ordinary.dispatch
        with self.assertRaises(WorkflowError):
            ctx.admitted_provider_operation()
        self.assertEqual(self.calls, [])
        self.assertEqual(CALLS, [])

    async def test_checked_callback_survives_scheduling_and_start_await_mutations(self):
        replacements = []

        async def replacement(request):
            replacements.append(request.arguments)
            return ProviderDispatchOutcome.completed(
                ProviderReport.pending(target=request.target)
            )

        for timing in ("scheduled", "start_reply"):
            with self.subTest(timing=timing):
                self.provider._dispatch = dispatch
                self.calls.clear()
                CALLS.clear()
                self.mutate = None
                ctx = self.context()
                pending = ctx.admitted_provider_operation()
                if timing == "scheduled":
                    self.provider._dispatch = replacement
                else:

                    def mutate(op, _reply):
                        if op == "provider_admission.start.v1":
                            self.provider._dispatch = replacement

                    self.mutate = mutate
                outcome = await pending
                self.assertIs(outcome.kind, ProviderAdmissionOutcomeKind.TICKET)
                self.assertEqual(CALLS, [{"amount": 50}])
                self.assertEqual(replacements, [])
                self.assertEqual(
                    [op for op, _ in self.calls],
                    [
                        "provider_admission.start.v1",
                        "provider_admission.report.v1",
                    ],
                )
                self.assertEqual(
                    self.calls[1][1]["outcome"]["call_completion"], "completed"
                )
        self.provider._dispatch = dispatch

    async def test_completed_pending_returns_ticket_not_business_success_and_calls_once(
        self,
    ):
        ctx = self.context()
        first, second = (
            ctx.admitted_provider_operation(),
            ctx.admitted_provider_operation(),
        )
        result, replay = await asyncio.gather(first, second)
        self.assertIs(result.kind, ProviderAdmissionOutcomeKind.TICKET)
        self.assertEqual(result.ticket.to_dict(), replay.ticket.to_dict())
        self.assertEqual(len(CALLS), 1)
        self.assertEqual(
            [op for op, _ in self.calls],
            ["provider_admission.start.v1", "provider_admission.report.v1"],
        )
        self.assertEqual(self.calls[1][1]["outcome"]["call_completion"], "completed")
        self.assertEqual(
            self.calls[1][1]["outcome"]["report"]["knowledge"], {"kind": "pending"}
        )
        decision = ctx.wait_provider_operation(
            "result", ticket=result.ticket, resume="done", state=None
        )
        self.assertEqual(decision["wait"]["occurrence"]["activation_id"], "origin")

    async def test_unknown_succeeded_keeps_explicit_unknown_call_and_large_reports_are_untruncated(
        self,
    ):
        global MODE
        for mode, completion in (("unknown", "unknown"), ("large", "completed")):
            MODE = mode
            ctx = self.context()
            await ctx.admitted_provider_operation()
            payload = self.calls[-1][1]["outcome"]
            self.assertEqual(payload["call_completion"], completion)
            self.assertEqual(payload["report"]["knowledge"]["kind"], "succeeded")
            if mode == "large":
                self.assertEqual(
                    len(payload["report"]["knowledge"]["result"]["data"]), 100000
                )

    async def test_callback_exception_plain_report_or_nested_orchestration_does_not_report_completion(
        self,
    ):
        global MODE, CTX
        for MODE in ("raise", "plain", "nested"):
            ctx = self.context()
            CTX = ctx
            count = len(self.calls)
            result = await ctx.admitted_provider_operation()
            self.assertIs(result.kind, ProviderAdmissionOutcomeKind.TICKET)
            self.assertEqual(len(self.calls), count + 1)
            self.assertIsNone(ctx._provider_admission_record["report"])
            await ctx.admitted_provider_operation()
            self.assertEqual(len(self.calls), count + 1)

    async def test_preloaded_or_native_provider_record_skips_start_and_callback(self):
        for completed in (False, True):
            retained = record(self.wake, completed=completed)
            result = await self.context(retained=retained).admitted_provider_operation()
            self.assertEqual(result.ticket.to_dict(), retained["ticket"])
            selected = deepcopy(self.wake)
            selected["selection"] = {"kind": "provider", "record": retained}
            await self.context(wake=selected).admitted_provider_operation()
        self.assertEqual((self.calls, CALLS), ([], []))

    async def test_existing_foreign_dedup_ticket_is_not_adopted_by_native_wait(self):
        retained = {
            "ticket": deepcopy(self.wake["binding"]["provider"]["ticket"]),
            "source": None,
            "report": None,
        }
        retained["ticket"]["occurrence"]["workflow_id"] = "other-workflow"
        self.existing = snapshot(
            self.wake, retained=retained, state="closed_before_start"
        )
        ctx = self.context()
        result = await ctx.admitted_provider_operation()
        with self.assertRaises(WorkflowError):
            ctx.wait_provider_operation(
                "result", ticket=result.ticket, resume="done", state=None
            )
        self.assertEqual(CALLS, [])
        self.assertEqual(len(self.calls), 1)

    async def test_awaiting_rewait_reuses_original_occurrence(self):
        self.existing = snapshot(self.wake)
        ctx = self.context()
        self.assertIs(
            (await ctx.admitted_provider_operation()).kind,
            ProviderAdmissionOutcomeKind.WAIT_REQUIRED,
        )
        decision = ctx.wait_admission(resume="done", state=None, timeout_ms=1000)
        self.assertEqual(
            decision["wait"]["request"]["occurrence"], self.wake["occurrence"]
        )
        self.assertEqual(CALLS, [])

    async def test_lost_start_or_mutated_source_fails_without_callback_or_retry(self):
        async def lost(op, payload):
            self.calls.append((op, payload))
            raise OSError("lost acknowledgement")

        ctx = self.context(rpc=lost)
        with self.assertRaises(OSError):
            await ctx.admitted_provider_operation()
        self.assertEqual(len(self.calls), 1)
        with self.assertRaises(WorkflowError):
            ctx.complete(None)

        def mutate(op, reply):
            reply["source"]["provider"]["ticket"]["occurrence"]["activation_id"] = (
                "replacement"
            )

        self.mutate = mutate
        with self.assertRaises(WorkflowError):
            await self.context().admitted_provider_operation()
        self.assertEqual(CALLS, [])

    async def test_report_mismatch_poisoned_and_start_window_is_monotonic(self):
        def mutate(op, reply):
            if op.endswith("report.v1"):
                reply["call_completion"] = "unknown"

        self.mutate = mutate
        ctx = self.context()
        with self.assertRaises(WorkflowError):
            await ctx.admitted_provider_operation()
        with self.assertRaises(WorkflowError):
            ctx.complete(None)
        self.mutate = None
        before = len(CALLS)
        with (
            patch("ledgence.worker.provider_admission.monotonic", side_effect=[0, 60]),
            self.assertRaises(WorkflowError),
        ):
            await self.context().admitted_provider_operation()
        self.assertEqual(len(CALLS), before)
        with self.assertRaises(WorkflowError):
            await _call(
                sync_dispatch,
                {"arguments": {}},
                binding(self.provider),
                None,
                governed=True,
                dispatch_deadline=-1,
            )
        self.assertEqual(len(CALLS), before)

    async def test_cancelled_awaiter_does_not_repeat_or_abandon_owned_callback(self):
        global GATE
        GATE = asyncio.Event()
        ctx = self.context()

        async def await_result():
            return await ctx.admitted_provider_operation()

        waiter = asyncio.create_task(await_result())
        while not CALLS:
            await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        GATE.set()
        await ctx._drain()
        self.assertEqual(len(CALLS), 1)
        self.assertEqual(len(self.calls), 2)

    async def test_review_cannot_accept_dedup_record_or_changed_frozen_report(self):
        selected = deepcopy(self.wake)
        selected["selection"] = {"kind": "needs_review", "source_id": "source"}
        retained = record(self.wake)
        retained["source"] = None
        with self.assertRaises(WorkflowError):
            matches_record(selected, retained)
        retained = record(self.wake, completed=True)
        selected["selection"] = {"kind": "provider", "record": deepcopy(retained)}
        retained["report"]["call_completion"] = "unknown"
        with self.assertRaises(WorkflowError):
            matches_record(selected, retained)


class ProviderAdmissionProtocolTests(unittest.TestCase):
    launch = ordinary.ProviderProtocolTests.launch
    invocation = ordinary.ProviderProtocolTests.invocation

    def message(self):
        import hashlib
        import json

        provider = ordinary.adapter()
        ordinary.registry(provider)
        selected = wake(provider)
        contract = selected["binding"]["provider"]["binding"]["contract"]
        contract.update(
            dispatch_callable="program:dispatch", reconcile_callable="program:probe"
        )
        contract.pop("digest")
        contract["digest"] = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    contract, sort_keys=True, ensure_ascii=False, separators=(",", ":")
                ).encode()
            ).hexdigest()
        )
        selected["binding"]["provider"]["action"]["name"] = "program:dispatch"
        message = self.invocation()
        message["runtime_capabilities"] = {"approved_effect_evidence_v2": True}
        message["runtime_features"] = {
            "v": 1,
            "supported": [
                "native_effect_resolution.v1",
                "provider_operation.v1",
                "resource_admission.v1",
                "provider_admission.v1",
            ],
        }
        message["extension"]["payload"] = test_workflow.payload(
            revision=1, wake=selected, provider_admission=record(selected)
        )
        return message

    def test_real_bootstrap_replays_ticket_without_callback_and_gates_composed_capability(
        self,
    ):
        program = ordinary.PROTOCOL_SOURCE.replace(
            "def start(event, ctx):\n    raise AssertionError('WORKFLOW_ENTERED')",
            "async def start(event, ctx):\n    result = await ctx.admitted_provider_operation()\n    return ctx.complete(result.ticket.to_dict())",
        )
        first = self.message()
        second = deepcopy(first)
        second["runtime_features"]["supported"].remove("provider_admission.v1")
        result, frames = self.launch(
            program,
            [first, second],
            protocol=3,
            input_limit=256 * 1024,
            output_limit=256 * 1024,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[1]["status"], "success", result.stderr)
        self.assertEqual(
            frames[1]["output"]["output"],
            first["extension"]["payload"]["provider_admission"]["ticket"],
        )
        self.assertEqual(frames[2]["status"], "runtime_error")
        self.assertFalse(any(frame["type"] == "runtime_request" for frame in frames))
        self.assertNotIn(b"DISPATCH_ENTERED", result.stderr)
