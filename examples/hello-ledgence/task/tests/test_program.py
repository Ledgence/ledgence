"""Unit tests for application behavior; no running Ledgence stack is needed."""
import unittest

from program import greet, handle


class ProgramTests(unittest.TestCase):
    def test_greeting(self):
        self.assertEqual(greet("Ada"), {"message": "Hello, Ada!"})

    def test_invalid_name(self):
        for name in (None, 1, "", "   ", "x" * 201):
            with self.subTest(name=name), self.assertRaises(ValueError):
                greet(name)

    def test_task_reads_application_data(self):
        event = {
            "specversion": "1.0",
            "id": "test-event",
            "source": "urn:example:test",
            "type": "example.greet",
            "data": {"name": "Ada"},
        }
        self.assertEqual(handle(event), {"message": "Hello, Ada!"})


if __name__ == "__main__":
    unittest.main()
