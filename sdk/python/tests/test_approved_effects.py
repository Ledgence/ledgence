"""Approved effects use a single durable dispatch grant and replay confirmed results (MIT)."""
import asyncio
from copy import deepcopy
import json
import unittest

import test_workflow
from test_workflow_approvals import snapshot
from ledgence.worker.workflow import ApprovalAction, EffectOutcomeUnknown, WorkflowError


class Provider:
    def __init__(self):
        self.posts = []
        self.lookups = []
        self.unknown = False

    async def issue(self, amount, currency="USD"):
        self.posts.append((amount, currency))
        await asyncio.sleep(0)
        return {"receipt": "r1"}

    def lookup(self, **approved):
        self.lookups.append(approved)
        if self.unknown:
            raise EffectOutcomeUnknown()
        return {"receipt": "r1"}


class EffectTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = test_workflow.WorkflowTests.asyncTearDown
    context = test_workflow.WorkflowTests.context

    async def asyncSetUp(self):
        await test_workflow.WorkflowTests.asyncSetUp(self)
        self.setup_authority()

    def setup_authority(self):
        self.provider = Provider()
        self.approval = snapshot(self.provider.issue)
        self.claims = []
        self.commits = []
        self.claimed = False

    async def authority(self, operation, request):
        if operation == "approved_effect.confirm":
            self.assertEqual(set(request), {"approval_key", "lookup", "record"})
            self.assertEqual(request["approval_key"], "refund")
            self.assertEqual(request["lookup"], self.claims[-1]["lookup"])
            self.assertEqual(request["record"]["input"]["lookup"], request["lookup"])
            self.commits.append(deepcopy(request["record"]))
            return {"committed": True}
        self.assertEqual(operation, "approved_effect.claim")
        self.assertEqual(set(request), {"approval_key", "action", "lookup"})
        self.assertEqual(request["approval_key"], "refund")
        self.assertEqual(request["action"], self.approval["action"])
        self.assertNotEqual(request["lookup"], request["action"]["name"])
        self.claims.append(deepcopy(request))
        disposition = "lookup_only" if self.claimed else "dispatch"
        self.claimed = True
        return dict(deepcopy(request), disposition=disposition, workflow_id="workflow-1", activation_id="activation-1")

    def activation(self, rpc=None, *, replay=False, approval=None):
        return self.context(test_workflow.payload(
            revision=1, wake={"kind": "approval", "approval": approval or self.approval},
            local_steps=self.commits if replay else [],
        ), rpc or self.authority)

    def effect(self, ctx):
        return ctx.approved_effect(self.provider.issue, lookup=self.provider.lookup, version="1")

    async def test_concurrent_bound_methods_get_one_grant_and_replay_makes_no_rpc(self):
        ctx = self.activation()
        first, second = await ctx.gather(self.effect(ctx), self.effect(ctx))
        first["receipt"] = "changed"
        self.assertEqual(second, {"receipt": "r1"})
        self.assertEqual(self.provider.posts, [(50, "USD")])
        self.assertEqual(self.provider.lookups, [])
        self.assertEqual(len(self.claims), 1)
        self.assertEqual(len(self.commits), 1)
        self.assertEqual(json.loads(self.commits[0]["input"]["action_json"]), self.approval["action"])

        async def no_rpc(*args):
            self.fail(f"replay issued RPC {args[0]}")
        resumed = self.activation(no_rpc, replay=True)
        self.assertEqual(await self.effect(resumed), second)
        with self.assertRaisesRegex(WorkflowError, "different binding"):
            resumed.approved_local(self.provider.issue, version="1")
        self.assertEqual(self.provider.posts, [(50, "USD")])
        self.assertEqual(self.provider.lookups, [])

    async def test_explicit_protection_uses_claim_and_confirmation_after_rejecting_local(self):
        self.approval["protection"] = "external_effect"
        ctx = self.activation()
        with self.assertRaisesRegex(WorkflowError, "requires approved_effect"):
            await ctx.approved_local(self.provider.issue, version="1")
        self.assertEqual(self.provider.posts, [])
        self.assertEqual(await self.effect(ctx), {"receipt": "r1"})
        async def unexpected_rpc(*args):
            self.fail("durable protected replay must not contact the orchestrator")
        self.assertEqual(await self.effect(self.activation(unexpected_rpc, replay=True)),
                         {"receipt": "r1"})
        self.assertEqual(len(self.claims), 1)
        self.assertEqual(len(self.commits), 1)
        self.assertEqual(self.provider.posts, [(50, "USD")])
        self.assertEqual(self.provider.lookups, [])

    async def test_replay_accepts_other_json_encoders_and_rejects_changed_actions(self):
        await self.effect(self.activation())
        saved = self.commits[0]["input"]
        action = self.approval["action"]
        reordered = json.dumps({key: action[key] for key in ("name", "version", "arguments")}, indent=2)
        self.assertNotEqual(saved["action_json"], reordered)
        saved["action_json"] = reordered
        self.assertEqual(await self.effect(self.activation(replay=True)), {"receipt": "r1"})
        for text in (reordered.replace('"amount": 50', '"amount": 50.0'),
                     reordered.replace('"amount": 50', '"amount": 70, "amount": 50')):
            saved["action_json"] = text
            with self.assertRaises(WorkflowError):
                await self.effect(self.activation(replay=True))
        self.assertEqual((len(self.provider.posts), len(self.provider.lookups), len(self.claims)), (1, 0, 1))

    async def test_replayed_negative_zero_preserves_its_sign(self):
        action = ApprovalAction.for_callable(self.provider.issue, version="1", arguments={"amount": -0.0})
        self.approval = snapshot(action=action.to_dict())
        await self.effect(self.activation())
        saved = self.commits[0]["input"]
        canonical = saved["action_json"]
        self.assertIn('"amount":-0.0', canonical)
        saved["action_json"] = canonical.replace('"amount":-0.0', '"amount":-0')
        self.assertEqual(await self.effect(self.activation(replay=True)), {"receipt": "r1"})
        saved["action_json"] = canonical.replace('"amount":-0.0', '"amount":0.0')
        with self.assertRaises(WorkflowError):
            await self.effect(self.activation(replay=True))

    async def test_lost_grant_or_result_ack_never_dispatches_twice(self):
        for lost in ("approved_effect.claim", "approved_effect.confirm"):
            with self.subTest(lost=lost):
                self.setup_authority()
                async def lose_reply(operation, request):
                    reply = await self.authority(operation, request)
                    if operation == lost:
                        raise ConnectionError("lost reply after acceptance")
                    return reply
                ctx = self.activation(lose_reply)
                with self.assertRaises(ConnectionError):
                    await self.effect(ctx)
                with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
                    self.effect(ctx)
                if lost == "approved_effect.claim":
                    # No request reached the provider before the grant reply
                    # was lost. Even a missing receipt cannot authorize a send.
                    self.provider.unknown = True
                    with self.assertRaises(EffectOutcomeUnknown):
                        await self.effect(self.activation(replay=True))
                    self.assertEqual(self.provider.posts, [])
                    self.assertEqual(self.provider.lookups, [{"amount": 50, "currency": "USD"}])
                    self.assertEqual(len(self.claims), 2)
                    self.assertEqual(self.commits, [])
                else:
                    self.assertEqual(await self.effect(self.activation(replay=True)), {"receipt": "r1"})
                    self.assertEqual(self.provider.posts, [(50, "USD")])
                    self.assertEqual(self.provider.lookups, [])
                    self.assertEqual(len(self.claims), 1)
                    self.assertEqual(len(self.commits), 1)

    async def test_unknown_lookup_is_not_committed_and_retry_remains_read_only(self):
        self.claimed = True
        self.provider.unknown = True
        ctx = self.activation()
        for _ in range(2):
            with self.assertRaises(EffectOutcomeUnknown):
                await self.effect(ctx)
        self.assertEqual(len(self.provider.lookups), 1)
        self.assertEqual(self.commits, [])
        # A handler can catch this provider outcome and checkpoint a durable wait.
        await ctx._drain()
        self.assertEqual(ctx.wait_event("verify", continuation="verify", state={})["kind"], "wait")
        self.provider.unknown = False
        self.assertEqual(await self.effect(self.activation()), {"receipt": "r1"})
        self.assertEqual(self.provider.posts, [])
        self.assertEqual(len(self.provider.lookups), 2)

    async def test_invalid_or_uncorrelated_grant_fails_closed(self):
        altered = deepcopy(self.approval["action"])
        altered["arguments"]["amount"] = 50.0
        for field, value in (("workflow_id", "other"), ("activation_id", "other"), ("approval_key", "other"),
                             ("disposition", True), ("disposition", "grant"), ("action", altered),
                             ("lookup", "other"), ("extra", False), ("missing", None), ("old_backend", None)):
            with self.subTest(field=field):
                async def invalid(operation, request):
                    response = await self.authority(operation, request)
                    if field == "missing":
                        del response["action"]
                    elif field == "old_backend":
                        del response["lookup"]
                    else:
                        response[field] = value
                    return response
                ctx = self.activation(invalid)
                with self.assertRaises(WorkflowError):
                    await self.effect(ctx)
                with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
                    ctx.complete(None)
        self.assertEqual((self.provider.posts, self.provider.lookups, self.commits), ([], [], []))

    async def test_reject_invalid_dispatch_or_lookup_before_claim(self):
        ctx = self.activation()
        with self.assertRaises(WorkflowError):
            ctx.approved_effect(self.provider.lookup, lookup=self.provider.issue, version="1")
        with self.assertRaises(WorkflowError):
            ctx.approved_effect(self.provider.issue, lookup=self.provider.lookup, version="2")
        with self.assertRaises(WorkflowError):
            ctx.approved_effect(self.provider.issue, lookup=self.provider.issue, version="1")
        with self.assertRaises(WorkflowError):
            ctx.approved_effect(self.provider.issue, lookup=Provider().issue, version="1")
        with self.assertRaises(WorkflowError):
            ctx.approved_effect(self.provider.issue, lookup=lambda amount, currency, account="new": None, version="1")
        with self.assertRaises(WorkflowError):
            self.effect(self.context())
        with self.assertRaises(WorkflowError):
            self.effect(self.activation(approval=snapshot(self.provider.issue, status="rejected")))
        self.assertEqual((self.claims, self.provider.posts, self.provider.lookups, self.commits), ([], [], [], []))

    async def test_deepest_approved_arguments_can_dispatch_and_journal_a_receipt(self):
        value = None
        for _ in range(63):
            value = [value]
        def dispatch(data):
            self.assertEqual(data, value)
            return "confirmed"
        def lookup(data):
            self.fail("unexpected lookup")
        action = ApprovalAction.for_callable(dispatch, arguments={"data": value}, version="1")
        self.approval = snapshot(action=action.to_dict())
        for replay in (False, True):
            ctx = self.activation(replay=replay)
            self.assertEqual(await ctx.approved_effect(dispatch, lookup=lookup, version="1"), "confirmed")
        self.assertEqual(len(self.claims), 1)
        self.assertEqual(len(self.commits), 1)

    async def test_user_argument_names_do_not_collide_with_sdk_parameters(self):
        def dispatch(**kwargs):
            return kwargs
        def lookup(**kwargs):
            self.fail("unexpected lookup")
        arguments = {"self": 1, "fn": "a", "lookup": "b", "version": "user", "key": "k"}
        action = ApprovalAction.for_callable(dispatch, arguments=arguments, version="1")
        self.approval = snapshot(action=action.to_dict())
        ctx = self.activation()
        self.assertEqual(await ctx.approved_effect(dispatch, lookup=lookup, version="1"), arguments)


if __name__ == "__main__":
    unittest.main()
