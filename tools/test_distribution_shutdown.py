"""Shutdown receipts must prove a graceful actual Compose service exit (MIT)."""
import copy
import unittest
from distribution_shutdown import event_proof


class ShutdownEventTests(unittest.TestCase):
    def test_sigterm_and_clean_exit_are_required_and_sigkill_is_rejected(self):
        events = [{'Action': 'kill', 'Actor': {'Attributes': {'signal': '15'}}},
                  {'Action': 'die', 'Actor': {'Attributes': {'exitCode': '0'}}}]
        self.assertEqual(event_proof(events), {'sigterm_observed': True, 'sigkill_absent': True,
                                              'orchestrator_exit_code': 0})
        forced = copy.deepcopy(events)
        forced.insert(1, {'Action': 'kill', 'Actor': {'Attributes': {'signal': '9'}}})
        nonzero = copy.deepcopy(events)
        nonzero[1]['Actor']['Attributes']['exitCode'] = '137'
        for invalid in ([], events[:1], events[1:], forced, nonzero, events + events[1:]):
            with self.subTest(events=invalid), self.assertRaises(AssertionError):
                event_proof(invalid)


if __name__ == '__main__':
    unittest.main()
