"""Synthetic admission: no provider request, credential or external service."""

import asyncio
import unittest
from copy import deepcopy
from enum import StrEnum
from pathlib import Path
from unittest.mock import patch

import test_workflow
from ledgence.worker.admission import AdmissionOutcomeKind, validate_artifact_registry
from ledgence.worker.workflow import (
    OperationKind,
    Workflow,
    WorkflowContext,
    WorkflowError,
)

CALLS = []
MODE = None
CTX = None
GATE = None


async def model(prompt, temperature=0):
    CALLS.append((prompt, temperature))
    if MODE == "raise":
        raise RuntimeError("unknown external completion")
    if MODE == "invalid":
        return object()
    if MODE == "nested":
        return CTX.complete("illegal")
    if GATE is not None:
        await GATE.wait()
    return {"text": prompt, "temperature": temperature}


class Entry(StrEnum):
    START = "start"
    RUN = "run"


def registered(fn=model):
    workflow = Workflow(Entry)
    operation = workflow.operation(kind=OperationKind.MODEL, version="1")(fn)

    @workflow.entrypoint(Entry.START, default=True)
    def start(event, ctx):
        return ctx.complete(None)

    @workflow.entrypoint(Entry.RUN)
    def run(event, ctx):
        return ctx.complete(None)

    handler = workflow.build()
    return workflow, operation, handler


SCOPE = {"tenant_id": "acme", "namespace": "demo"}
DIGEST = "sha256:" + "a" * 64


def wake(operation):
    occurrence = {
        "scope": SCOPE,
        "workflow_id": "workflow-1",
        "origin_activation_id": "origin",
        "key": "model:1",
    }
    return {
        "kind": "resource_admission",
        "key": "wait:1",
        "occurrence": occurrence,
        "waiting_activation_id": "origin",
        "resumed_activation_id": "activation-1",
        "resumed_revision": 1,
        "binding": {
            "resource": "model",
            "program": {
                "program": {"id": "workflow", "version": "1"},
                "digest": DIGEST,
                "size": 100,
            },
            "operation": operation.bind(prompt="hello").to_dict(),
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
                "binding_digest": DIGEST,
            },
        },
    }


def source(selected, start_id="start", finish_id="finish"):
    return {
        "occurrence": selected["occurrence"],
        "source_id": "source",
        "start_id": start_id,
        "finish_id": finish_id,
        "generation": 1,
        "binding_digest": DIGEST,
        "owner": {
            "scope": SCOPE,
            "task_id": "activation-1",
            "attempt_id": "attempt-1",
            "lease_id": "lease-1",
            "generation": 1,
            "worker_session_id": "worker",
            "consumer_id": 0,
        },
        "worker": {
            "audience": {
                "scope": SCOPE,
                "authority_id": "authority",
                "instance_id": "instance",
            },
            "issuer": "issuer",
            "subject": "worker",
            "credential_id": "credential",
        },
        "accepted_at": 1000,
        "start_deadline_at": 31000,
    }


def finish(selected):
    return {
        "source": source(selected),
        "output": {"text": "saved"},
        "accepted_at": 2000,
        "already_accepted": True,
    }


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        global MODE, CTX, GATE
        MODE = CTX = GATE = None
        CALLS.clear()
        self.workflow, self.operation, self.handler = registered()
        self.wake = wake(self.operation)
        self.calls = []
        self.contexts = []

    async def asyncTearDown(self):
        for ctx in self.contexts:
            await ctx._finish(cancel=True)

    def context(self, *, selected=True, receipt=None, rpc=None, enabled=True):
        async def exchange(op, payload):
            self.calls.append((op, deepcopy(payload)))
            if op.endswith("start.v1"):
                return {"kind": "granted", "source": source(self.wake, **payload)}
            return {
                "source": payload["source"],
                "output": payload["output"],
                "accepted_at": 2000,
                "already_accepted": False,
            }

        payload = test_workflow.payload(revision=1, continuation="run")
        if selected:
            payload["wake"] = deepcopy(self.wake)
        if receipt is not None:
            payload["admission_result"] = receipt
        ctx = WorkflowContext(payload, rpc or exchange, resource_admission=enabled)
        ctx._operations = self.handler._ledgence_operations
        self.contexts.append(ctx)
        return ctx

    async def test_checkpoint_freezes_defaults_and_rewait_preserves_occurrence(self):
        ctx = self.context(selected=False)
        binding = self.operation.bind(prompt="hello")
        decision = ctx.request_admission(
            "model:1",
            resource="model",
            operation=binding,
            resume="run",
            state=None,
            timeout_ms=10000,
        )
        self.assertEqual(
            decision["wait"]["request"]["operation"]["arguments"],
            {"prompt": "hello", "temperature": 0},
        )
        self.assertEqual(CALLS, [])
        self.assertEqual(self.calls, [])
        resumed = self.context()
        decision = resumed.wait_admission(resume="run", state={}, timeout_ms=10)
        self.assertEqual(
            decision["wait"]["request"],
            {"kind": "existing", "occurrence": self.wake["occurrence"]},
        )

    async def test_success_uses_saved_arguments_and_one_deduplicated_callback(self):
        ctx = self.context()
        a = ctx.admitted_operation(self.operation)
        b = ctx.admitted_operation(self.operation)
        first, second = await asyncio.gather(a, b)
        first.result["text"] = "changed"
        self.assertEqual(second.result, {"text": "hello", "temperature": 0})
        self.assertEqual(CALLS, [("hello", 0)])
        self.assertEqual(
            [op for op, _ in self.calls],
            ["resource_admission.start.v1", "resource_admission.finish.v1"],
        )

    async def test_preloaded_finish_replays_without_start_or_callback(self):
        ctx = self.context(receipt=finish(self.wake))
        result = await ctx.admitted_operation(self.operation)
        self.assertEqual(result.result, {"text": "saved"})
        self.assertEqual(self.calls, [])
        self.assertEqual(CALLS, [])

    async def test_finished_selection_replays(self):
        self.wake["selection"] = {"kind": "finished", "receipt": finish(self.wake)}
        result = await self.context().admitted_operation(self.operation)
        self.assertEqual(result.kind, AdmissionOutcomeKind.SUCCEEDED)
        self.assertEqual(self.calls, [])
        self.assertEqual(CALLS, [])

    async def test_uncertain_start_is_never_retried_or_used_as_permission(self):
        async def rpc(op, payload):
            self.calls.append(op)
            raise TimeoutError()

        ctx = self.context(rpc=rpc)
        with self.assertRaises(TimeoutError):
            await ctx.admitted_operation(self.operation)
        with self.assertRaises(WorkflowError):
            ctx.complete(None)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(CALLS, [])

    async def test_uncertain_finish_poisoned_context_does_not_repeat_callback(self):
        async def rpc(op, payload):
            self.calls.append(op)
            if op.endswith("start.v1"):
                return {"kind": "granted", "source": source(self.wake, **payload)}
            raise TimeoutError()

        ctx = self.context(rpc=rpc)
        with self.assertRaises(TimeoutError):
            await ctx.admitted_operation(self.operation)
        with self.assertRaises(WorkflowError):
            ctx.admitted_operation(self.operation)
        self.assertEqual(CALLS, [("hello", 0)])
        self.assertEqual(len(self.calls), 2)

    async def test_callback_failure_invalid_result_and_nested_orchestration_never_finish(
        self,
    ):
        global MODE, CTX
        for mode, error in [
            ("raise", RuntimeError),
            ("invalid", WorkflowError),
            ("nested", WorkflowError),
        ]:
            with self.subTest(mode=mode):
                MODE = mode
                CTX = self.context()
                self.calls.clear()
                with self.assertRaises(error):
                    await CTX.admitted_operation(self.operation)
                self.assertEqual(len(self.calls), 1)

    async def test_cancelled_observer_does_not_cancel_owned_callback(self):
        global GATE
        GATE = asyncio.Event()
        ctx = self.context()

        async def observe():
            return await ctx.admitted_operation(self.operation)

        observer = asyncio.create_task(observe())
        await asyncio.sleep(0.01)
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await observer
        self.assertFalse(ctx._admission_pending.cancelled())
        GATE.set()
        await ctx._drain()
        self.assertEqual(len(self.calls), 2)

    async def test_controller_shutdown_cancels_callback_without_false_finish(self):
        global GATE
        GATE = asyncio.Event()
        ctx = self.context()
        ctx.admitted_operation(self.operation)
        await asyncio.sleep(0.01)
        await ctx._finish(cancel=True)
        self.assertEqual(len(self.calls), 1)

    async def test_closed_and_review_never_start(self):
        for selection, kind in [
            ({"kind": "closed", "reason": "deadline"}, AdmissionOutcomeKind.CLOSED),
            (
                {"kind": "needs_review", "source_id": "source"},
                AdmissionOutcomeKind.NEEDS_REVIEW,
            ),
        ]:
            self.wake["selection"] = selection
            result = await self.context().admitted_operation(self.operation)
            self.assertEqual(result.kind, kind)
        self.assertEqual(self.calls, [])
        self.assertEqual(CALLS, [])

    async def test_missing_feature_foreign_wake_or_preloaded_closed_result_reject(self):
        with self.assertRaises(WorkflowError):
            self.context(enabled=False)
        for key, value in [("resumed_revision", 2), ("resumed_activation_id", "other")]:
            original = self.wake[key]
            self.wake[key] = value
            with self.assertRaises(WorkflowError):
                self.context()
            self.wake[key] = original
        self.wake["selection"] = {"kind": "closed", "reason": "deadline"}
        with self.assertRaises(WorkflowError):
            self.context(receipt=finish(self.wake))
        self.assertEqual(CALLS, [])

    async def test_registry_replacement_or_changed_defaults_reject_before_start(self):
        _, operation, _ = registered()
        ctx = self.context()
        with self.assertRaises(WorkflowError):
            ctx.admitted_operation(operation)
        self.wake["binding"]["operation"]["version"] = "other"
        with self.assertRaises(WorkflowError):
            self.context().admitted_operation(self.operation)
        self.assertEqual(self.calls, [])

    async def test_grant_mutations_do_not_execute(self):
        for field, value in [
            ("binding_digest", "sha256:" + "b" * 64),
            ("generation", 2),
            ("start_id", "foreign"),
        ]:

            async def rpc(op, payload, field=field, value=value):
                value_source = source(self.wake, **payload)
                value_source[field] = value
                return {"kind": "granted", "source": value_source}

            with self.assertRaises(WorkflowError):
                await self.context(rpc=rpc).admitted_operation(self.operation)
        self.assertEqual(CALLS, [])

    async def test_expired_local_dispatch_budget_never_calls_callback(self):
        ctx = self.context()
        with (
            patch("ledgence.worker.admission.monotonic", side_effect=[0, 31]),
            self.assertRaises(WorkflowError),
        ):
            await ctx.admitted_operation(self.operation)
        self.assertEqual(CALLS, [])

    async def test_registration_is_frozen_and_rejects_ambiguous_callables(self):
        with self.assertRaises(WorkflowError):
            self.workflow.operation(kind=OperationKind.MODEL, version="2")
        for fn in [
            lambda: None,
            self.test_registration_is_frozen_and_rejects_ambiguous_callables,
        ]:
            with self.assertRaises(WorkflowError):
                registered(fn)
        with self.assertRaises(WorkflowError):
            self.operation.bind(prompt=object())
        validate_artifact_registry(self.handler, Path(__file__).parent)
        with self.assertRaises(WorkflowError):
            validate_artifact_registry(self.handler, Path("/nonexistent"))

    async def test_existing_snapshots_never_authorize_and_preserve_original_occurrence(
        self,
    ):
        for state, expected in [
            ("awaiting_rewait", AdmissionOutcomeKind.WAIT_REQUIRED),
            ("unknown_occupied", AdmissionOutcomeKind.NEEDS_REVIEW),
            ("closed_before_start", AdmissionOutcomeKind.CLOSED),
            ("administratively_released", AdmissionOutcomeKind.CLOSED),
        ]:
            stored = {
                "occurrence": self.wake["occurrence"],
                "binding": self.wake["binding"],
                "state": state,
                "revision": 3,
                "created_at": 0,
                "updated_at": 2000,
                "deadline_at": 10000,
                "offer": None,
                "finish": None,
                "source": source(self.wake)
                if state in ("unknown_occupied", "administratively_released")
                else None,
            }

            async def rpc(op, payload, stored=stored):
                self.calls.append(op)
                return {"kind": "existing", "snapshot": stored}

            ctx = self.context(rpc=rpc)
            result = await ctx.admitted_operation(self.operation)
            self.assertEqual(result.kind, expected)
            if state == "closed_before_start":
                self.assertIsNone(result.reason)
            if state == "awaiting_rewait":
                wait = ctx.wait_admission(resume="run", state=None, timeout_ms=30000)
                self.assertEqual(
                    wait["wait"]["request"]["occurrence"], self.wake["occurrence"]
                )
        self.assertEqual(CALLS, [])
        self.assertEqual(self.calls, ["resource_admission.start.v1"] * 4)

    async def test_finish_racing_context_fetch_replays_existing_without_callback(self):
        receipt = finish(self.wake)

        async def rpc(op, payload):
            self.calls.append(op)
            return {
                "kind": "existing",
                "snapshot": {
                    "occurrence": self.wake["occurrence"],
                    "binding": self.wake["binding"],
                    "state": "finished",
                    "revision": 4,
                    "created_at": 0,
                    "updated_at": 2000,
                    "deadline_at": 10000,
                    "offer": None,
                    "source": receipt["source"],
                    "finish": receipt,
                },
            }

        result = await self.context(rpc=rpc).admitted_operation(self.operation)
        self.assertEqual(result.result, {"text": "saved"})
        self.assertEqual(self.calls, ["resource_admission.start.v1"])
        self.assertEqual(CALLS, [])

    async def test_preloaded_finish_cannot_change_review_source_or_frozen_result(self):
        self.wake["selection"] = {"kind": "needs_review", "source_id": "another-source"}
        with self.assertRaises(WorkflowError):
            self.context(receipt=finish(self.wake))
        original = finish(self.wake)
        self.wake["selection"] = {"kind": "finished", "receipt": original}
        altered = deepcopy(original)
        altered["output"] = "changed"
        with self.assertRaises(WorkflowError):
            self.context(receipt=altered)
        self.assertEqual(self.calls, [])

    async def test_caught_callback_failure_can_checkpoint_but_never_finish_or_repeat(
        self,
    ):
        global MODE
        MODE = "raise"
        ctx = self.context()
        with self.assertRaises(RuntimeError):
            await ctx.admitted_operation(self.operation)
        with self.assertRaises(RuntimeError):
            await ctx.admitted_operation(self.operation)
        self.assertEqual(
            ctx.complete("application handled failure")["kind"], "complete"
        )
        await ctx._drain()
        self.assertEqual(CALLS, [("hello", 0)])
        self.assertEqual([op for op, _ in self.calls], ["resource_admission.start.v1"])

    async def test_callback_arguments_are_prepared_before_start_authority(self):
        from ledgence.worker import admission

        events = []
        original = admission._freeze

        async def rpc(op, payload):
            if op.endswith("start.v1"):
                events.append("start")
                return {"kind": "granted", "source": source(self.wake, **payload)}
            return {
                "source": payload["source"],
                "output": payload["output"],
                "accepted_at": 2000,
                "already_accepted": False,
            }

        ctx = self.context(rpc=rpc)

        def freeze(value, *args, **kwargs):
            if value == self.wake["binding"]["operation"]["arguments"]:
                events.append("arguments")
            return original(value, *args, **kwargs)

        with patch.object(admission, "_freeze", side_effect=freeze):
            await ctx.admitted_operation(self.operation)
        self.assertEqual(events, ["arguments", "start"])
