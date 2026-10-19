"""Unit tests do not simulate orchestration, durability, or workflow resumption."""
import inspect
import unittest

from program import greet, handle, workflow


class ProgramTests(unittest.TestCase):
    def test_greeting(self):
        self.assertEqual(greet("Ada"), {"message": "Hello, Ada!"})

    def test_invalid_name(self):
        for name in (None, 1, "", "   ", "x" * 201):
            with self.subTest(name=name), self.assertRaises(ValueError):
                greet(name)

    def test_workflow_registration(self):
        # build() validates enum coverage and the single default entrypoint.
        self.assertIs(workflow.build(), handle)
        self.assertTrue(inspect.iscoroutinefunction(handle))


if __name__ == "__main__":
    unittest.main()
