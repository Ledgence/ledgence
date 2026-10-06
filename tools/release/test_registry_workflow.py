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

    def test_pypi_collects_both_exact_qualified_projects_before_upload(self):
        qualification, publisher = self.jobs["python-package"], self.jobs["pypi"]
        for artifact in ("python-registry-package", "python-worker-package"):
            with self.subTest(artifact=artifact):
                self.assertIn(f"name: {artifact}\n", qualification)
                self.assertIn(f"name: {artifact}\n", publisher)
        self.assertIn("python tools/check-python-client.py", qualification)
        self.assertIn("python tools/check-python-worker.py", qualification)
        collect = publisher.index("registry.py collect-pypi")
        prepare = publisher.index("registry.py prepare --kind pypi")
        upload = publisher.index("uses: pypa/gh-action-pypi-publish@")
        verify = publisher.index("registry.py verify --kind pypi")
        install = publisher.index("registry.py install --kind pypi")
        self.assertLess(publisher.index("name: python-registry-package\n"), collect)
        self.assertLess(publisher.index("name: python-worker-package\n"), collect)
        self.assertLess(collect, prepare)
        self.assertLess(prepare, upload)
        self.assertLess(upload, verify)
        self.assertLess(verify, install)
        self.assertIn('--client-dist "$RUNNER_TEMP/python-package/dist"', publisher[collect:prepare])
        self.assertIn('--worker-dist "$RUNNER_TEMP/python-worker-package/dist"', publisher[collect:prepare])
        self.assertEqual(publisher.count('--dist "$RUNNER_TEMP/qualified-python"'), 3)
        self.assertIn("packages-dir: registry-upload/", publisher)
        self.assertIn("skip-existing: false", publisher)
        self.assertIn("attestations: true", publisher)
        self.assertIn("if: steps.pending.outputs.upload == 'true'", publisher)

    def test_oidc_permissions_remain_confined_to_controlled_publishers(self):
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                if name in ("pypi", "crates"):
                    self.assertIn("id-token: write", job)
                else:
                    self.assertNotIn("id-token: write", job)
        self.assertIn("name: pypi\n", self.jobs["pypi"])
        self.assertNotIn("password:", self.jobs["pypi"])
        self.assertNotIn("secrets.", self.jobs["pypi"])


if __name__ == "__main__":
    unittest.main()
