"""Keep the documented agent recovery check in the SDK gate (MIT)."""
from pathlib import Path
import subprocess
import sys
import unittest


class AgentRecoveryExampleTests(unittest.TestCase):
    def test_public_example_recovery_and_checkpoint_checks(self):
        root = Path(__file__).resolve().parents[3]
        result = subprocess.run(
            [sys.executable, "-B", str(root / "examples/agent-recovery/check.py")],
            cwd=root, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"turns": 2', result.stdout)
