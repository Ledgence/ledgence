"""Native effect wait identity, typed outcomes and real protocol compatibility (MIT)."""
import asyncio
from dataclasses import FrozenInstanceError
import unittest

import test_bootstrap
import test_workflow
from test_workflow_approvals import snapshot
from ledgence.worker.workflow import (
    EffectOccurrence, EffectResolutionOutcome, EffectResolutionOutcomeKind,
    EffectResolutionWake, WorkflowContext, WorkflowError, MAX_RECORD_BYTES,
)

FEATURES = {"v": 1, "supported": ["native_effect_resolution.v1"]}


def occurrence(**changes):
    return {"scope": {"tenant_id": "tenant", "namespace": "tests"},
            "workflow_id": "workflow-1", "activation_id": "activation-1", "approval_key": "refund", **changes}


def wake(**changes):
    return {"kind": "effect_resolution", "key": "review:0", "occurrence": occurrence(),
            "parking_activation_id": "activation-park", "parking_revision": 3,
            "resumed_activation_id": "activation-resumed", "resumed_revision": 4,
            "resolution_id": "resolution-1", "accepted_at": 20, "accepted_revision": 9,
            "basis_revision": 8, "selected_at": 25, "selected_effect_revision": 11,
            "outcome": {"kind": "recorded_success", "result": {"receipt": "r1"}}, **changes}


class ResolutionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflow.WorkflowTests.asyncSetUp
    asyncTearDown = test_workflow.WorkflowTests.asyncTearDown

    def context(self, raw=None, enabled=True, rpc=None):
        async def unexpected(*args):
            raise AssertionError("preparing a wait cannot issue an RPC")
        context = WorkflowContext(raw or test_workflow.payload(), rpc or unexpected,
                                  native_effect_resolution=enabled)
        self.contexts.append(context)
        return context

    def resumed(self, **changes):
        return self.context(test_workflow.payload(activation_id="activation-resumed", revision=4,
                                                  wake=wake(**changes)))

    async def test_first_wait_uses_executing_activation_and_freezes_state(self):
        approval = snapshot(protection="external_effect")
        context = self.context(test_workflow.payload(revision=1, wake={"kind": "approval", "approval": approval}))
        state = {"a": [1]}
        decision = context.wait_effect_resolution("review:0", resume="after", state=state)
        state["a"].append(2)
        self.assertEqual(decision["state"], {"a": [1]})
        self.assertEqual(decision["wait"], {"kind": "effect_resolution", "key": "review:0",
                                          "occurrence": occurrence(), "after_resolution_id": None})
        self.assertNotEqual(decision["wait"]["occurrence"]["activation_id"], approval["activation_id"])
        self.assertEqual(context._validate_decision(decision), decision)
        self.assertIsNone(context.effect_resolution)

    async def test_resumed_default_preserves_origin_and_waits_after_previous_assessment(self):
        context = self.resumed(outcome={"kind": "disputed", "first_conflicting_entry_sequence": 10})
        resolution = context.effect_resolution
        self.assertEqual(resolution.outcome.kind, EffectResolutionOutcomeKind.DISPUTED)
        self.assertEqual(resolution.outcome.first_conflicting_entry_sequence, 10)
        decision = context.wait_effect_resolution("review:9", resume="after", state=None)
        self.assertEqual(decision["wait"]["occurrence"], occurrence())
        self.assertEqual(decision["wait"]["after_resolution_id"], "resolution-1")
        latest = context.wait_effect_resolution("review:latest", resume="after", state=None, after_resolution_id=None)
        self.assertIsNone(latest["wait"]["after_resolution_id"])

    async def test_explicit_saved_occurrence_works_in_later_normal_continuation(self):
        saved = EffectOccurrence.from_dict(occurrence())
        context = self.context(test_workflow.payload(activation_id="later", revision=6))
        self.assertIsNone(context.effect_occurrence)
        decision = context.wait_effect_resolution("review:later", resume="after", state=None,
                                                 occurrence=saved, after_resolution_id="prior")
        self.assertEqual(decision["wait"]["occurrence"], occurrence())
        self.assertEqual(decision["wait"]["after_resolution_id"], "prior")
        different = EffectOccurrence.from_dict(occurrence(workflow_id="foreign"))
        with self.assertRaisesRegex(WorkflowError, "another workflow"):
            context.wait_effect_resolution("foreign", resume="after", state=None, occurrence=different)

    async def test_no_implicit_occurrence_without_protected_approved_wake(self):
        for protection, status in ((None, "approved"), ("external_effect", "rejected")):
            approval = snapshot(status=status)
            if protection is not None:
                approval["protection"] = protection
            context = self.context(test_workflow.payload(revision=1, wake={"kind": "approval", "approval": approval}))
            self.assertIsNone(context.effect_occurrence)
            with self.assertRaisesRegex(WorkflowError, "EffectOccurrence"):
                context.wait_effect_resolution("review", resume="after", state=None)

    async def test_missing_capability_rejects_helper_raw_decision_and_wake(self):
        context = self.context(enabled=False)
        address = EffectOccurrence.from_dict(occurrence())
        with self.assertRaisesRegex(WorkflowError, "unavailable"):
            context.wait_effect_resolution("review", resume="after", state=None, occurrence=address)
        enabled = self.context()
        decision = enabled.wait_effect_resolution("review", resume="after", state=None, occurrence=address)
        with self.assertRaisesRegex(WorkflowError, "unavailable"):
            context._validate_decision(decision)
        with self.assertRaisesRegex(WorkflowError, "unavailable"):
            self.context(test_workflow.payload(activation_id="activation-resumed", revision=4, wake=wake()), enabled=False)

    async def test_wake_cannot_be_transplanted_to_another_activation_or_revision(self):
        for changes in ({"activation_id": "foreign"}, {"revision": 5}, {"workflow_id": "foreign"}):
            raw = test_workflow.payload(activation_id="activation-resumed", revision=4, wake=wake())
            raw.update(changes)
            with self.subTest(changes=changes), self.assertRaisesRegex(WorkflowError, "another workflow activation"):
                self.context(raw)

    async def test_wait_drains_owned_work_and_forbids_local_staging(self):
        started, finish = asyncio.Event(), asyncio.Event()
        commits = []
        async def commit(operation, record):
            commits.append(record)
            return {"committed": True}
        context = self.context(rpc=commit)
        async def local():
            started.set()
            await finish.wait()
            with self.assertRaisesRegex(WorkflowError, "cannot stage"):
                context.wait_effect_resolution("bad", resume="after", state=None,
                                               occurrence=EffectOccurrence.from_dict(occurrence()))
            return 42
        context.local("work", local)
        await started.wait()
        decision = context.wait_effect_resolution("review", resume="after", state=None,
                                                 occurrence=EffectOccurrence.from_dict(occurrence()))
        drain = asyncio.create_task(context._drain())
        await asyncio.sleep(0)
        self.assertFalse(drain.done())
        finish.set()
        await drain
        self.assertEqual(len(commits), 1)
        self.assertEqual(context._validate_decision(decision), decision)

    async def test_origin_and_outcome_are_immutable_independent_copies(self):
        value = wake()
        context = self.resumed()
        resolution = EffectResolutionWake.from_dict(value)
        value["outcome"]["result"]["receipt"] = "changed"
        resolution.outcome.result["receipt"] = "changed again"
        address = resolution.occurrence
        address.scope["tenant_id"] = "other"
        self.assertEqual(address.to_dict(), occurrence())
        self.assertEqual(resolution.outcome.result, {"receipt": "r1"})
        self.assertEqual(context.effect_resolution.to_dict(), wake())
        with self.assertRaises(FrozenInstanceError):
            resolution._encoded = b"{}"
        with self.assertRaisesRegex(WorkflowError, "does not contain"):
            _ = resolution.outcome.failure

    async def test_all_outcomes_and_fullwidth_revisions(self):
        outcomes = [{"kind": "recorded_success", "result": None},
                    {"kind": "recorded_failure", "failure": {"kind": "failed", "message": "observed"}},
                    {"kind": "closed_unknown"}, {"kind": "disputed", "first_conflicting_entry_sequence": 10}]
        for outcome in outcomes:
            with self.subTest(outcome=outcome):
                self.assertEqual(self.resumed(outcome=outcome).effect_resolution.outcome.to_dict(), outcome)
        limit = (1 << 64) - 1
        result = EffectResolutionWake.from_dict(wake(basis_revision=limit-1, accepted_revision=limit,
                                                     selected_effect_revision=limit))
        self.assertEqual(result.accepted_revision, limit)
        self.assertIsNone(EffectResolutionOutcome.from_dict(outcomes[0]).result)

    async def test_strict_shapes_numbers_and_bounds(self):
        for field in ("parking_revision", "resumed_revision", "basis_revision", "accepted_revision", "selected_effect_revision"):
            for bad in (True, 1.0, -1, 1 << 64, "9"):
                with self.subTest(field=field, bad=bad), self.assertRaises(WorkflowError):
                    EffectResolutionWake.from_dict(wake(**{field: bad}))
        for changes in ({"resumed_revision": 5}, {"accepted_revision": 11},
                        {"parking_activation_id": "activation-resumed"},
                        {"basis_revision": 0}, {"accepted_at": 253402300800000}, {"selected_at": 19}):
            with self.subTest(changes=changes), self.assertRaises(WorkflowError):
                EffectResolutionWake.from_dict(wake(**changes))
        for outcome in ({"kind": "recorded_success"}, {"kind": "recorded_success", "result": "x"*MAX_RECORD_BYTES},
                        {"kind": "closed_unknown", "result": None},
                        {"kind": "disputed", "first_conflicting_entry_sequence": 11}):
            with self.subTest(outcome=outcome), self.assertRaises(WorkflowError):
                EffectResolutionWake.from_dict(wake(outcome=outcome))
        context = self.context()
        decision = context.wait_effect_resolution("review", resume="after", state=None,
                                                 occurrence=EffectOccurrence.from_dict(occurrence()))
        del decision["wait"]["after_resolution_id"]
        with self.assertRaises(WorkflowError):
            context._validate_decision(decision)


class ResolutionProtocolTests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation
    reply = test_workflow.WorkflowProtocolTests.reply

    def test_manifest_validation_precedes_application(self):
        invalid = [None, [], True, {"v": True, "supported": []}, {"v": 2, "supported": []},
                   {"v": 1, "supported": None}, {"v": 1, "supported": [True]},
                   {"v": 1, "supported": ["bad name"]}, {"v": 1, "supported": ["a", "a"]},
                   {"v": 1, "supported": [str(i) for i in range(17)]},
                   {"v": 1, "supported": ["x"*129]}, {"v": 1, "supported": [], "extra": 1}]
        for manifest in invalid:
            with self.subTest(manifest=manifest):
                message = dict(self.invocation(), runtime_features=manifest)
                result, frames = self.launch("def handle(event): raise AssertionError('HANDLER_ENTERED')", [message], protocol=3)
                self.assertEqual(result.returncode, 70, result.stderr)
                self.assertEqual([f["type"] for f in frames], ["ready"])
                self.assertNotIn(b"HANDLER_ENTERED", result.stderr)
        result, frames = self.launch("def handle(event): raise AssertionError('HANDLER_ENTERED')",
                                     [dict(self.invocation(), runtime_features=FEATURES)], protocol=3)
        self.assertEqual(result.returncode, 70)
        self.assertNotIn(b"HANDLER_ENTERED", result.stderr)

    def test_unknown_feature_is_ignored_and_capabilities_do_not_leak_across_invocations(self):
        source = """from ledgence.worker.workflow import workflow_context, EffectOccurrence, WorkflowError
async def handle(event):
    ctx = workflow_context()
    occurrence = EffectOccurrence.from_dict(event['data']['occurrence'])
    try:
        return ctx.wait_effect_resolution('review', resume='after', state=None, occurrence=occurrence)
    except WorkflowError:
        return ctx.complete('unavailable')
"""
        messages = []
        for i, (manifest, evidence) in enumerate((
            (FEATURES, True),
            ({"v": 1, "supported": ["future.feature"]}, False),
            (None, True),
        )):
            message = self.invocation(str(i))
            message["event"]["data"] = {"occurrence": occurrence()}
            if evidence:
                message["runtime_capabilities"] = {"approved_effect_evidence_v2": True}
            if manifest is not None:
                message["runtime_features"] = manifest
            messages.append(message)
        result, frames = self.launch(source, messages, protocol=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[1]["output"]["kind"], "wait")
        self.assertEqual([frame["output"]["output"] for frame in frames[2:]], ["unavailable", "unavailable"])

    def test_real_bootstrap_routes_enum_entrypoint_and_preserves_typed_resume(self):
        source = """from enum import StrEnum
from ledgence.worker.workflow import Workflow, EffectResolutionOutcomeKind
class Entry(StrEnum):
    START = 'start'
    AFTER = 'after'
workflow = Workflow(Entry)
@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    return ctx.wait_effect_resolution('review:0', resume=Entry.AFTER, state={'saved': 1})
@workflow.entrypoint(Entry.AFTER)
def after(event, ctx):
    result = ctx.effect_resolution
    assert result.outcome.kind is EffectResolutionOutcomeKind.RECORDED_SUCCESS
    return ctx.complete({'result':result.outcome.result,'origin':ctx.effect_occurrence.activation_id})
handle = workflow.build()
"""
        first = self.invocation(activation=test_workflow.payload(revision=1, continuation="start",
                     wake={"kind": "approval", "approval": snapshot(protection="external_effect")}))
        second = self.invocation("2", activation=test_workflow.payload(
            activation_id="activation-resumed", revision=4, continuation="after", wake=wake()))
        for message in (first, second):
            message["runtime_features"] = FEATURES
            message["runtime_capabilities"] = {"approved_effect_evidence_v2": True}
        result, frames = self.launch(source, [first, second], protocol=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([f["type"] for f in frames], ["ready", "result", "result"])
        self.assertEqual(frames[1]["output"]["wait"]["occurrence"], occurrence())
        self.assertEqual(frames[2]["output"]["output"], {"result": {"receipt": "r1"}, "origin": "activation-1"})


if __name__ == "__main__":
    unittest.main()
