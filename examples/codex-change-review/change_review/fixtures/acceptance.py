"""Fixed acceptance tests; never supplied or modified by the agent (MIT)."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest


class ShippingAcceptance(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("shipping_candidate", Path(__file__).with_name("shipping.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.shipping_cost = module.shipping_cost

    def test_below_threshold(self):
        for value in (0, 1, 9999):
            result = self.shipping_cost(value)
            self.assertIs(type(result), int)
            self.assertEqual(result, 500)

    def test_exact_threshold(self):
        result = self.shipping_cost(10000)
        self.assertIs(type(result), int)
        self.assertEqual(result, 0)

    def test_above_threshold(self):
        for value in (10001, 20000):
            result = self.shipping_cost(value)
            self.assertIs(type(result), int)
            self.assertEqual(result, 0)

    def test_negative_integer(self):
        with self.assertRaises(ValueError):
            self.shipping_cost(-1)

    def test_boolean(self):
        for value in (True, False):
            with self.assertRaises(TypeError):
                self.shipping_cost(value)

    def test_non_integer(self):
        for value in (1.0, "10000", None, [], {}):
            with self.assertRaises(TypeError):
                self.shipping_cost(value)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ShippingAcceptance)
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=2).run(suite)
    print(json.dumps({"passed": result.wasSuccessful(), "total": result.testsRun,
                      "failures": len(result.failures), "errors": len(result.errors)}))
    raise SystemExit(0 if result.wasSuccessful() else 1)
