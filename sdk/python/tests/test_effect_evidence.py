"""Negotiated effect evidence preserves owned callbacks and durable replay (MIT)."""
import asyncio
from copy import deepcopy
import json
import unittest

import test_approved_effects
import test_bootstrap
import test_workflow
from ledgence.worker.workflow import EffectOutcomeUnknown, WorkflowContext, WorkflowError


class EffectEvidenceTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = test_workflow.WorkflowTests.asyncTearDown
    effect = test_approved_effects.EffectTests.effect

    async def asyncSetUp(self):
        await test_workflow.WorkflowTests.asyncSetUp(self)
        test_approved_effects.EffectTests.setup_authority(self)
        self.operations = []
        self.confirmations = []
        self.cached = None

    def activation(self, rpc=None, *, records=()):
        context = WorkflowContext(test_workflow.payload(
            revision=1, wake={"kind": "approval", "approval": self.approval},
            local_steps=list(records)), rpc or self.authority, effect_evidence_v2=True)
        self.contexts.append(context)
        return context

    def identity(self):
        return {"workflow_id": "workflow-1", "activation_id": "activation-1", "approval_key": "refund"}

    async def authority(self, operation, request):
        self.operations.append(operation)
        if operation == "approved_effect.claim.v2":
            self.assertEqual(set(request), {"approval_key", "action", "lookup", "claim_id"})
            self.assertEqual(request["action"], self.approval["action"])
            self.assertTrue(request["claim_id"].startswith("claim_"))
            self.claims.append(deepcopy(request))
            if self.cached is not None:
                return dict(self.identity(), kind="replay", record=deepcopy(self.cached))
            kind = "lookup_only" if self.claimed else "dispatch"
            self.claimed = True
            claim = dict(deepcopy(request), **self.identity())
            claim.update(claimed_at=100, initial_permission=kind,
                         recorded_revision=len(self.claims), already_accepted=False)
            return {"kind": kind, "claim": claim}
        self.assertEqual(operation, "approved_effect.confirm.v2")
        self.assertEqual(set(request), {"approval_key", "lookup", "record", "claim_id",
                                       "observation_id", "reported_callback"})
        self.assertEqual(request["claim_id"], self.claims[-1]["claim_id"])
        self.assertEqual(request["lookup"], self.claims[-1]["lookup"])
        self.assertTrue(request["observation_id"].startswith("observation_"))
        self.confirmations.append(deepcopy(request))
        self.commits.append(deepcopy(request["record"]))
        self.cached = deepcopy(request["record"])
        return dict(self.identity(), committed=True, key=request["record"]["key"],
                    claim_id=request["claim_id"], observation_id=request["observation_id"],
                    recorded_at=200, recorded_revision=2, already_accepted=False)

    async def test_duplicate_awaits_share_claim_callback_and_confirmation(self):
        ctx = self.activation()
        results = await ctx.gather(self.effect(ctx), self.effect(ctx))
        self.assertEqual(results, [{"receipt": "r1"}] * 2)
        results[0]["receipt"] = "changed"
        self.assertEqual(await self.effect(ctx), {"receipt": "r1"})
        self.assertEqual((len(self.claims), len(self.commits), len(self.provider.posts)), (1, 1, 1))
        self.assertEqual(self.confirmations[0]["reported_callback"], "dispatch")
        self.assertNotIn("claim_id", self.commits[0]["input"])
        self.assertNotIn("observation_id", self.commits[0]["input"])
        self.assertEqual(set(self.commits[0]["input"]), {"v", "action_json", "lookup"})

    async def test_preloaded_replay_never_calls_runtime(self):
        await self.effect(self.activation())
        async def unexpected(*args):
            self.fail("preloaded replay invoked runtime")
        self.assertEqual(await self.effect(self.activation(unexpected, records=self.commits)), {"receipt": "r1"})
        self.assertEqual(len(self.claims), 1)

    async def test_lost_confirmation_remote_replay_preserves_original_binding(self):
        async def lose(operation, request):
            reply = await self.authority(operation, request)
            if operation == "approved_effect.confirm.v2":
                raise ConnectionError("lost accepted confirmation")
            return reply
        with self.assertRaises(ConnectionError):
            await self.effect(self.activation(lose))
        # Another encoder's accepted nested JSON text must survive replay.
        self.cached["input"]["action_json"] = json.dumps(self.approval["action"], indent=2)
        original = deepcopy(self.cached)
        resumed = self.activation()
        self.assertEqual(await self.effect(resumed), {"receipt": "r1"})
        self.assertEqual(await self.effect(resumed), {"receipt": "r1"})
        self.assertEqual(resumed._records[original["key"]], original)
        self.assertEqual((len(self.claims), len(self.commits), len(self.provider.posts)), (2, 1, 1))
        self.assertEqual(self.provider.lookups, [])
        self.assertEqual(self.operations, ["approved_effect.claim.v2", "approved_effect.confirm.v2",
                                          "approved_effect.claim.v2"])
        self.assertNotEqual(self.claims[0]["claim_id"], self.claims[1]["claim_id"])

    async def test_lost_grant_never_falls_back_or_issues_mutation(self):
        async def lose(operation, request):
            await self.authority(operation, request)
            raise ConnectionError("lost claim")
        with self.assertRaises(ConnectionError):
            await self.effect(self.activation(lose))
        self.provider.unknown = True
        with self.assertRaises(EffectOutcomeUnknown):
            await self.effect(self.activation())
        self.assertEqual(self.provider.posts, [])
        self.assertEqual(len(self.provider.lookups), 1)
        self.assertEqual(self.operations, ["approved_effect.claim.v2"] * 2)
        self.assertEqual(self.commits, [])

    async def test_lookup_reports_its_actual_callback(self):
        self.claimed = True
        self.assertEqual(await self.effect(self.activation()), {"receipt": "r1"})
        self.assertEqual(self.confirmations[0]["reported_callback"], "lookup")
        self.assertEqual(self.provider.posts, [])
        self.assertEqual(len(self.provider.lookups), 1)

    async def test_invalid_claim_cannot_run_either_callback(self):
        cases = [("workflow_id", "other"), ("activation_id", "other"), ("approval_key", "other"),
                 ("claim_id", "other"), ("lookup", "other"), ("initial_permission", "lookup_only"),
                 ("already_accepted", True), ("already_accepted", 0), ("claimed_at", True),
                 ("recorded_revision", -1), ("recorded_revision", 0), ("extra", 1)]
        for field, value in cases:
            self.claimed = False
            async def invalid(operation, request):
                reply = await self.authority(operation, request)
                reply["claim"][field] = value
                return reply
            with self.subTest(field=field, value=value):
                ctx = self.activation(invalid)
                with self.assertRaises(WorkflowError):
                    await self.effect(ctx)
                with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
                    ctx.complete(None)
        self.assertEqual((self.provider.posts, self.provider.lookups, self.commits), ([], [], []))

    async def test_lookup_receipt_preserves_the_original_dispatch_permission(self):
        async def repeated(operation, request):
            reply = await self.authority(operation, request)
            if operation.endswith("claim.v2"):
                reply["kind"] = "lookup_only"
                reply["claim"].update(initial_permission="dispatch", already_accepted=False)
            return reply
        with self.assertRaises(WorkflowError):
            await self.effect(self.activation(repeated))
        self.assertEqual((self.provider.posts, self.provider.lookups, self.commits), ([], [], []))

        async def accepted(operation, request):
            reply = await repeated(operation, request)
            if operation.endswith("claim.v2"):
                reply["claim"]["already_accepted"] = True
            return reply
        self.assertEqual(await self.effect(self.activation(accepted)), {"receipt": "r1"})
        self.assertEqual(self.provider.posts, [])
        self.assertEqual(len(self.provider.lookups), 1)
        self.assertEqual(self.confirmations[0]["reported_callback"], "lookup")

    async def test_invalid_remote_replay_never_creates_confirmation(self):
        await self.effect(self.activation())
        original = deepcopy(self.cached)
        mutations = [
            lambda record: record.update(key="_approval:" + "0" * 64),
            lambda record: record.update(callable="other"),
            lambda record: record["input"].update(v=True),
            lambda record: record["input"].update(lookup="other"),
            lambda record: record["input"].update(extra=1),
            lambda record: record["input"].update(action_json='{"name":"x","name":"x"}'),
            lambda record: record["input"].update(action_json=record["input"]["action_json"].replace('"amount":50', '"amount":50.0')),
        ]
        for mutate in mutations:
            self.cached = deepcopy(original)
            mutate(self.cached)
            ctx = self.activation()
            with self.assertRaises(WorkflowError):
                await self.effect(ctx)
            self.assertEqual(ctx._records, {})
        self.assertEqual((len(self.commits), len(self.provider.posts), len(self.provider.lookups)), (1, 1, 0))

    async def test_invalid_confirmation_is_uncertain_and_not_cached(self):
        for field, value in [("committed", False), ("committed", 1), ("claim_id", "other"),
                             ("observation_id", "other"), ("key", "other"), ("workflow_id", "other"),
                             ("already_accepted", None), ("recorded_at", -1), ("recorded_revision", True), ("recorded_revision", 0)]:
            self.cached = None
            async def invalid(operation, request):
                reply = await self.authority(operation, request)
                if operation.endswith("confirm.v2"):
                    reply[field] = value
                return reply
            with self.subTest(field=field):
                ctx = self.activation(invalid)
                with self.assertRaises(WorkflowError):
                    await self.effect(ctx)
                self.assertEqual(ctx._records, {})
                with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
                    ctx.complete(None)

    async def test_user_return_with_protocol_fields_is_ordinary_output(self):
        value = {"kind": "replay", "record": {"output": None}, "claim_id": "user"}
        async def dispatch(amount, currency="USD"):
            return value
        from test_workflow_approvals import snapshot
        self.approval = snapshot(dispatch)
        ctx = self.activation()
        result = await ctx.approved_effect(dispatch, lookup=self.provider.lookup, version="1")
        self.assertEqual(result, value)
        self.assertEqual(len(self.confirmations), 1)
        self.assertEqual(self.commits[0]["output"], value)

    async def test_null_callback_and_replay_remain_valid(self):
        def dispatch(amount, currency="USD"):
            return None
        from test_workflow_approvals import snapshot
        self.approval = snapshot(dispatch)
        ctx = self.activation()
        self.assertIsNone(await ctx.approved_effect(dispatch, lookup=self.provider.lookup, version="1"))
        resumed = self.activation()
        self.assertIsNone(await resumed.approved_effect(dispatch, lookup=self.provider.lookup, version="1"))
        self.assertEqual(len(self.confirmations), 1)
        self.assertIsNone(self.commits[0]["output"])

    async def test_cancelled_observer_cannot_abandon_claim_or_confirmation(self):
        for phase in ("approved_effect.claim.v2", "approved_effect.confirm.v2"):
            self.cached = None
            self.claimed = False
            entered, release = asyncio.Event(), asyncio.Event()
            async def delayed(operation, request):
                if operation == phase:
                    entered.set()
                    await release.wait()
                return await self.authority(operation, request)
            ctx = self.activation(delayed)
            pending = asyncio.ensure_future(self.effect(ctx))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                self.assertTrue(any(not task.done() for _, task in ctx._pending.values()))
                release.set()
                await asyncio.wait_for(ctx._drain(), 2)
                self.assertEqual(await self.effect(ctx), {"receipt": "r1"})
            finally:
                release.set()
                await asyncio.wait_for(ctx._finish(cancel=True), 2)
                await asyncio.gather(pending, return_exceptions=True)
        self.assertEqual((len(self.claims), len(self.confirmations), len(self.provider.posts)), (2, 2, 2))

    async def test_runtime_cancellation_retains_no_unacknowledged_record(self):
        for phase in ("approved_effect.claim.v2", "approved_effect.confirm.v2"):
            self.cached = None
            self.claimed = False
            entered, release = asyncio.Event(), asyncio.Event()
            async def delayed(operation, request):
                if operation == phase:
                    entered.set()
                    await release.wait()
                return await self.authority(operation, request)
            ctx = self.activation(delayed)
            pending = asyncio.ensure_future(self.effect(ctx))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                await asyncio.wait_for(ctx._finish(cancel=True), 2)
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                self.assertEqual(ctx._records, {})
                self.assertTrue(all(task.done() for _, task in ctx._pending.values()))
            finally:
                release.set()
                await asyncio.wait_for(ctx._finish(cancel=True), 2)
                await asyncio.gather(pending, return_exceptions=True)
        self.assertEqual(self.confirmations, [])
        self.assertEqual(len(self.provider.posts), 1)
        self.assertFalse(any(operation.endswith("observe") for operation in self.operations))

    async def test_remote_rust_size_boundary_result_replays_without_new_report(self):
        import test_workflow_inbound
        from ledgence.worker.workflow import MAX_RECORD_BYTES, _encode
        await self.effect(self.activation())
        binding = {key: self.cached[key] for key in ("key", "callable", "input")}
        self.cached = test_workflow_inbound.full_array(
            lambda value: dict(binding, output=value), MAX_RECORD_BYTES)
        with self.assertRaises(WorkflowError):
            _encode(self.cached, MAX_RECORD_BYTES, 96)
        ctx = self.activation()
        output = await self.effect(ctx)
        self.assertEqual(output, self.cached["output"])
        output[0] = "changed by caller"
        self.assertEqual(await self.effect(ctx), self.cached["output"])
        self.assertEqual((len(self.confirmations), len(self.provider.posts)), (1, 1))
        self.assertEqual(self.provider.lookups, [])

    async def test_ordinary_local_has_no_effect_operations(self):
        operations = []
        async def commit(operation, record):
            operations.append(operation)
            return {"committed": True}
        ctx = self.activation(commit)
        self.assertEqual(await ctx.local("plain", lambda: 42), 42)
        self.assertEqual(operations, ["local_step.commit"])


class CapabilityProtocolTests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch
    invocation = test_bootstrap.BootstrapTests.invocation

    def test_malformed_capability_is_rejected_before_application(self):
        for capability in (None, [], True, {"approved_effect_evidence_v2": 1},
                           {"approved_effect_evidence_v2": None}, {"unknown": True}):
            with self.subTest(capability=capability):
                message = dict(self.invocation(), runtime_capabilities=capability,
                               v=3, processing_context=None)
                result, frames = self.launch(
                    "def handle(event):\n    print('HANDLER_ENTERED')\n    return 42\n", [message], protocol=3)
                self.assertEqual(result.returncode, 70)
                self.assertEqual([frame["type"] for frame in frames], ["ready"])
                self.assertNotIn(b"HANDLER_ENTERED", result.stderr)

    def test_additive_capability_preserves_ordinary_task_and_absent_default(self):
        messages = [dict(self.invocation(str(i), str(i)), v=3, processing_context=None,
                         **({"runtime_capabilities": value} if value is not None else {}))
                    for i, value in enumerate(({"approved_effect_evidence_v2": True}, {}, None,
                                                {"approved_effect_evidence_v2": False}))]
        result, frames = self.launch("def handle(event):\n    return 42\n", messages, protocol=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([frame["output"] for frame in frames[1:]], [42] * 4)


if __name__ == "__main__":
    unittest.main()
