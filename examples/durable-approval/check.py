"""Run the example locally with simulated activation transport; no providers (MIT)."""
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk/python"))
from ledgence.worker.workflow import WorkflowContext, _workflow
import program


class ExampleTests(unittest.IsolatedAsyncioTestCase):
    async def activate(self, *, continuation="start", wake=None, records=None, data=None):
        async def commit(operation, record):
            self.assertEqual(operation, "local_step.commit")
            self.commits.append(copy.deepcopy(record))
            return {"committed": True}
        context = WorkflowContext({"v": 1, "workflow_id": "workflow", "activation_id": "resumed" if wake else "request",
                                   "revision": 1 if wake else 0, "continuation": continuation,
                                   "state": None, "inputs": {}, "local_steps": records or [], "wake": wake}, commit)
        token = _workflow.set(context)
        try:
            decision = context._validate_decision(await program.handle({"data": data or {"amount": 100}}))
            await context._finish()
            return decision
        finally:
            _workflow.reset(token)

    async def test_approved_effective_amount_and_replay(self):
        self.commits = []
        request = await self.activate()
        self.assertEqual(self.commits, [])
        action = request["wait"]["action"]
        self.assertEqual(action["arguments"], {"amount": 50, "currency": "USD"})
        self.assertEqual(request["wait"]["proposed_arguments"], {"amount": 100})
        approval = {"scope": {"tenant_id": "demo", "namespace": "approvals"}, "workflow_id": "workflow",
                    "key": "refund", "activation_id": "request", "revision": 0, "action": action,
                    "proposed_arguments": {"amount": 100}, "created_at": 1, "deadline": 100,
                    "status": "approved", "decision": {"decision_id": "review-1", "decision": "approve",
                        "reviewer": "local-demo", "reason": None, "decided_at": 2},
                    "resumed_activation_id": "resumed"}
        wake = {"kind": "approval", "approval": approval}
        first = await self.activate(continuation="resolve", wake=wake)
        replay = await self.activate(continuation="resolve", wake=wake, records=self.commits)
        self.assertEqual(first, replay)
        self.assertEqual(first["output"]["result"]["amount"], 50)
        self.assertEqual(len(self.commits), 1)
        for status in ("rejected", "expired"):
            refusal = copy.deepcopy(approval)
            refusal["status"] = status
            refusal["decision"] = ({**approval["decision"], "decision": "reject"}
                                     if status == "rejected" else None)
            result = await self.activate(continuation="resolve", wake={"kind": "approval", "approval": refusal})
            self.assertEqual(result["output"], {"status": status, "executed": False})
        self.assertEqual(len(self.commits), 1)
        print(json.dumps(first["output"], sort_keys=True))


if __name__ == "__main__": unittest.main()
