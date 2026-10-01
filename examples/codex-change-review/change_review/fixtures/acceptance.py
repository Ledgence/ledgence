"""Fixed acceptance tests; never supplied or modified by the agent (MIT)."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest


class PaginationAcceptance(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("pagination_candidate", Path(__file__).with_name("pagination.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.page_count = module.page_count

    def test_empty_and_partial_page(self):
        for value, expected in ((0, 0), (1, 1), (99, 1)):
            result = self.page_count(value)
            self.assertIs(type(result), int)
            self.assertEqual(result, expected)

    def test_exact_page_boundaries(self):
        for value in (100, 200, 10**30):
            result = self.page_count(value)
            self.assertIs(type(result), int)
            self.assertEqual(result, value // 100)

    def test_extra_page(self):
        for value, expected in ((101, 2), (199, 2), (201, 3)):
            result = self.page_count(value)
            self.assertIs(type(result), int)
            self.assertEqual(result, expected)

    def test_negative_integer(self):
        with self.assertRaises(ValueError):
            self.page_count(-1)

    def test_boolean(self):
        for value in (True, False):
            with self.assertRaises(TypeError):
                self.page_count(value)

    def test_non_integer(self):
        for value in (1.0, "100", None, [], {}):
            with self.assertRaises(TypeError):
                self.page_count(value)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(PaginationAcceptance)
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=2).run(suite)
    print(json.dumps({"passed": result.wasSuccessful(), "total": result.testsRun,
                      "failures": len(result.failures), "errors": len(result.errors)}))
    raise SystemExit(0 if result.wasSuccessful() else 1)
