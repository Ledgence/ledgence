"""Offline example checks with simulated journal transport; no provider (MIT)."""
import copy
from functools import wraps
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk/python"))
from ledgence.worker.workflow import WorkflowContext, _workflow
import program


_OMITTED = object()


class ExampleTests(unittest.IsolatedAsyncioTestCase):
    async def activate(self, *, state=None, records=(), lose_ack=None, data=_OMITTED):
        self.commits = []
        async def commit(operation, record):
            self.assertEqual(operation, "local_step.commit")
            self.commits.append(copy.deepcopy(record))
            if record["key"] == lose_ack:
                raise RuntimeError("simulated lost commit acknowledgment")
            return {"committed": True}
        context = WorkflowContext({"v": 1, "workflow_id": "workflow", "activation_id": "turn-1" if state else "turn-0",
            "revision": 1 if state else 0, "continuation": "turn" if state else "start", "state": state,
            "inputs": {}, "local_steps": list(records)}, commit)
        token = _workflow.set(context)
        try:
            result = context._validate_decision(await program.handle({"data": {"order_id": "ORD-1042"} if data is _OMITTED else data}))
            await context._finish()
            return result
        finally:
            await context._finish(cancel=True)
            _workflow.reset(token)

    async def test_two_turns_and_model_or_tool_ack_recovery(self):
        for lost in ("model:turn:0", "tool:turn:0:lookup:0"):
            calls = {"model": 0, "tool": 0}
            model, tool = program.scripted_model, program.lookup_order
            @wraps(model)
            def counted_model(**arguments):
                calls["model"] += 1
                return model(**arguments)
            @wraps(tool)
            def counted_tool(**arguments):
                calls["tool"] += 1
                return tool(**arguments)
            with patch.object(program, "scripted_model", counted_model), patch.object(program, "lookup_order", counted_tool):
                with self.assertRaisesRegex(RuntimeError, "lost commit"):
                    await self.activate(lose_ack=lost)
                records = copy.deepcopy(self.commits)
                checkpoint = await self.activate(records=records)
                self.assertEqual(calls, {"model": 1, "tool": 1})
                self.assertEqual(checkpoint["kind"], "continue")
                state = checkpoint["state"]
                self.assertEqual(state["round"], 1)
                self.assertEqual(state["messages"][-1]["result"]["region"], "us-east")
                result = await self.activate(state=state)
                self.assertEqual(result["output"], {"answer": "Order ORD-1042 is shipped; estimated delivery in 2 days.",
                    "turns": 2, "model": program.MODEL, "simulated": True})
                self.assertEqual(calls, {"model": 2, "tool": 1})
                print(json.dumps(result["output"], sort_keys=True))

    async def test_invalid_input_and_turn_budget_do_not_call_model(self):
        for data in (None, {}, [], "", 0, False, {"order_id": ""}, {"order_id": 42}, {"order_id": "x" * 65}):
            result = await self.activate(data=data)
            self.assertEqual(result["kind"], "fail")
            self.assertEqual(self.commits, [])
        result = await self.activate(state={"round": program.MAX_TURNS, "messages": []})
        self.assertEqual(result["error"]["kind"], "turn_limit")
        self.assertEqual(self.commits, [])


if __name__ == "__main__":
    unittest.main()
