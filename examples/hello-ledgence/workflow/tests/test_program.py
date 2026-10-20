"""Test registration and outcome handling; orchestration needs a running stack."""
import inspect
import unittest
from unittest.mock import Mock

from program import finish, handle, workflow


class ProgramTests(unittest.IsolatedAsyncioTestCase):
    def test_workflow_registration(self):
        # build() validates the enum entries and exactly one default entrypoint.
        self.assertIs(workflow.build(), handle)
        self.assertTrue(inspect.iscoroutinefunction(handle))

    async def test_unsuccessful_child_does_not_read_a_result(self):
        for state in ("failed", "cancelled"):
            with self.subTest(state=state):
                ctx = Mock()
                ctx.inputs = {"greeting": {"state": state}}
                await finish({}, ctx)
                ctx.fail.assert_called_once_with(
                    "greeting_failed", "The greeting task did not succeed."
                )
                ctx.get_result.assert_not_called()
                ctx.complete.assert_not_called()

    async def test_success_returns_the_child_output(self):
        ctx = Mock()
        ctx.inputs = {"greeting": {"state": "succeeded"}}
        ctx.get_result.return_value = {"message": "Hello, Ledgence!"}
        await finish({}, ctx)
        ctx.complete.assert_called_once_with({"message": "Hello, Ledgence!"})
        ctx.fail.assert_not_called()


if __name__ == "__main__":
    unittest.main()
