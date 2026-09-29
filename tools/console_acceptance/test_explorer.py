"""Validate the native fixture through the real SDK before starting services."""

import json
from pathlib import Path
import sys
import types
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk/python"))
from ledgence.worker.workflow import WorkflowContext, WorkflowError, _workflow
from console_acceptance.explorer import STEP, WORKFLOW


def task_result(output):
    return {"task_id": "child-task", "state": "succeeded", "outcome": {
        "kind": "succeeded", "attempt_id": "child-attempt", "quiescence": "confirmed",
        "execution_may_have_started": True, "output": output,
    }}


class NativeExplorerFixtureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.program = types.ModuleType("console_explorer_fixture")
        exec(compile(WORKFLOW, "console-explorer-workflow.py", "exec"), self.program.__dict__)
        self.step = types.ModuleType("console_explorer_step")
        exec(compile(STEP, "console-explorer-step.py", "exec"), self.step.__dict__)
        self.operations = []

    async def invoke(self, entrypoint, *, state=None, inputs=None, data=None, local_steps=None):
        async def rpc(operation, payload):
            self.operations.append((operation, json.loads(json.dumps(payload))))
            if operation == "workflow.fork":
                return {"committed": True, "key": payload["key"],
                        "branch_keys": [branch["key"] for branch in payload["branches"]]}
            self.assertEqual(operation, "local_step.commit")
            return {"committed": True}

        context = WorkflowContext({"v": 1, "workflow_id": "workflow-1", "activation_id": "activation-" + entrypoint,
            "revision": 0, "continuation": entrypoint, "state": state,
            "inputs": inputs or {}, "local_steps": local_steps or []}, rpc)
        token = _workflow.set(context)
        try:
            decision = await self.program.handle({"data": data if data is not None else {"queue": "native-test"}})
            context._validate_decision(decision)
            return decision
        finally:
            await context._finish(cancel=True)
            _workflow.reset(token)

    async def test_first_task_local_and_distributed_review_join_then_final_task(self):
        start = await self.invoke("start")
        self.assertEqual(start["until"], ["implement:0"])
        self.assertEqual(start["continuation"], "validate")
        self.assertEqual(len(start["commands"]), 1)
        candidate = self.step.handle({"data": start["commands"][0]["data"]})
        validate = await self.invoke("validate", inputs={"implement:0": task_result(candidate)})
        self.assertEqual([operation for operation, _ in self.operations], ["workflow.fork", "local_step.commit"])
        fork = self.operations[0][1]
        self.assertEqual([branch["key"] for branch in fork["branches"]], ["review:0"])
        self.assertEqual(self.operations[1][1]["key"], "tests:0")
        self.assertEqual(validate["commands"], [])
        self.assertEqual(validate["until"], ["review:0"])
        self.assertEqual(validate["continuation"], "collect")
        review = await self.invoke("review", data=fork["branches"][0]["data"])
        self.assertEqual(review["kind"], "complete")
        collect = await self.invoke("collect", state=validate["state"], inputs={"review:0": {
            "kind": "workflow", "workflow_id": "review-workflow", "state": "succeeded",
            "outcome": {"kind": "succeeded", "output": review["output"]},
        }})
        self.assertEqual(collect["until"], ["finalize:0"])
        self.assertEqual(collect["continuation"], "finish")
        final = self.step.handle({"data": collect["commands"][0]["data"]})
        finish = await self.invoke("finish", inputs={"finalize:0": task_result(final)})
        self.assertEqual(finish["kind"], "complete")
        self.assertEqual(finish["output"], {"tests": {"passed": True, "candidate": {"value": 7}},
                                          "review": {"approved": True, "candidate": {"value": 7}}})

    async def test_unsuccessful_first_task_never_registers_review_or_local_work(self):
        failed = {"task_id": "cancelled-child", "state": "cancelled", "outcome": {"kind": "cancelled"}}
        with self.assertRaisesRegex(WorkflowError, "child did not succeed"):
            await self.invoke("validate", inputs={"implement:0": failed})
        self.assertEqual(self.operations, [])


if __name__ == "__main__":
    unittest.main()
