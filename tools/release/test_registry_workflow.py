"""Guard publication prerequisites when push qualification is deduplicated.

These assertions cover the checked-in Actions routing, complementing the source,
version and artifact tests in test_registry.py. actionlint validates YAML syntax.
"""
from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/publish.yml"
SOURCE_GATES = {"quality": "ci", "documentation": "docs", "console": "console", "examples": "examples"}


class RegistryWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text()
        jobs = cls.workflow.split("\njobs:\n", 1)[1]
        cls.jobs = dict(re.findall(r"^  ([a-z][a-z-]*):\n(.*?)(?=^  [a-z][a-z-]*:|\Z)", jobs, re.M | re.S))

    def scalar(self, job, key):
        match = re.search(rf"^    {re.escape(key)}: (.+)$", self.jobs[job], re.M)
        self.assertIsNotNone(match, f"{job} must declare {key}")
        return match[1].strip()

    def test_manual_qualification_keeps_every_source_gate(self):
        for gate, workflow in SOURCE_GATES.items():
            with self.subTest(gate=gate):
                # Do not require publish=true: a manual dry run needs the same gates.
                self.assertEqual(self.scalar(gate, "if"), "github.event_name == 'workflow_dispatch'")
                self.assertEqual(self.scalar(gate, "uses"), f"./.github/workflows/{workflow}.yml")
                self.assertEqual(self.scalar(gate, "needs"), "source")
        for gate in ("source", "python-package", "rust-packages"):
            self.assertNotRegex(self.jobs[gate], r"(?m)^    if:", "push artifact qualification must run")

    def test_publishers_cannot_bypass_a_missing_or_failed_gate(self):
        required = {"source", "python-package", "rust-packages", *SOURCE_GATES}
        for publisher in ("pypi", "crates"):
            with self.subTest(publisher=publisher):
                needs = self.scalar(publisher, "needs")
                self.assertTrue(needs.startswith("[") and needs.endswith("]"))
                self.assertEqual({name.strip() for name in needs[1:-1].split(",")}, required)
                condition = self.scalar(publisher, "if")
                self.assertTrue(condition.startswith("inputs.publish && "))
                self.assertEqual(condition, f"inputs.publish && (inputs.registry == 'both' || inputs.registry == '{publisher}')")
                self.assertIn("python tools/release/registry.py source --publish", self.jobs[publisher])

    def test_push_cancellation_cannot_interrupt_manual_publication(self):
        concurrency = self.workflow.split("\nconcurrency:\n", 1)[1].split("\njobs:\n", 1)[0]
        self.assertIn("group: registry-packages-${{ github.event_name }}-${{ github.ref }}", concurrency)
        self.assertIn("cancel-in-progress: ${{ github.event_name == 'push' }}", concurrency)


if __name__ == "__main__":
    unittest.main()
