"""Release input integrity and missing-proof regressions, without claiming native acceptance (MIT)."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import subprocess
from unittest.mock import patch
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release.upgrade_bundles import bundle, inputs, digest, source_clean
from release.upgrade_evidence import verify, verify_s3
from release.upgrade_test_fixtures import bundle_fixture, reseal, artifact_exercise_fixture


class UpgradeBundleTests(unittest.TestCase):
    def setUp(self):
        # Inert bundle tests run during uncommitted development. Their fixture
        # source state is explicit; production inputs has no bypass switch.
        clean = patch("release.upgrade_bundles.source_clean", return_value=True)
        clean.start()
        self.addCleanup(clean.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.candidate = self.root / "candidate"
        bundle_fixture(self.candidate, "current")
        for version in ("0.6.1", "0.7.0", "0.8.0", "0.9.0"):
            previous = self.root / ("previous-" + version)
            bundle_fixture(previous, version)
            evidence = self.root / "evidence" / version
            evidence.mkdir(parents=True)
            candidate_version = json.loads((self.candidate / "provenance.json").read_text())["package_version"]
            if version in ("0.6.1", "0.7.0"):
                report = {"previous_version": "ledgence " + version, "candidate_version": "ledgence " + candidate_version,
                          "executable_sha256": {"previous": digest(previous / "bin/ledgence"), "candidate": digest(self.candidate / "bin/ledgence")}}
                name = "results.json"
            else:
                proof = self.proof(version)
                report = {"passed": True, "database_cleanup": True, "process_cleanup": True, "credential_scan_clean": True,
                          "provenance": proof, "provenance_after": proof, "exercise": artifact_exercise_fixture()}
                name = "summary.json"
            (evidence / name).write_text(json.dumps(report))
        self.report_path = self.root / "evidence/0.9.0/summary.json"

    def proof(self, version="0.9.0"):
        return inputs(self.root / ("previous-" + version), self.candidate / "bin", self.candidate / "console",
                      self.candidate / "runtime/ledgence/worker/bootstrap.py", predecessor=version, execute=False)

    def test_all_bundle_bound_proofs_pass(self):
        verify(self.root)
        self.assertEqual(bundle(self.root / "previous-0.9.0", predecessor="0.9.0", execute=False)["console_contract"], 14)

    def test_dirty_harness_checkout_rejected_despite_clean_bundle(self):
        with patch("release.upgrade_bundles.source_clean", return_value=False):
            with self.assertRaisesRegex(ValueError, "clean source checkout"):
                self.proof()
            with self.assertRaisesRegex(ValueError, "clean source checkout"):
                verify(self.root)

    def test_source_clean_checks_untracked_and_staged_harness_changes(self):
        repository = self.root / "source-git"
        repository.mkdir()
        subprocess.run(["git", "init", "--quiet", str(repository)], check=True)
        self.assertTrue(source_clean(repository))
        harness = repository / "gate.py"
        harness.write_text("pass\n")
        self.assertFalse(source_clean(repository))
        subprocess.run(["git", "add", "gate.py"], cwd=repository, check=True)
        self.assertFalse(source_clean(repository))

    def test_source_tree_helper_is_rejected_even_if_present(self):
        helper = self.root / "source/worker/bootstrap.py"
        helper.parent.mkdir(parents=True); helper.write_text("pass")
        with self.assertRaisesRegex(ValueError, "bundled helpers"):
            inputs(self.root / "previous-0.9.0", self.candidate / "bin", self.candidate / "console", helper,
                   predecessor="0.9.0", execute=False)

    def test_every_helper_and_console_asset_is_bound_before_and_after(self):
        for name in ("runtime/ledgence/worker/workflow.py", "console/index.html", "provenance.json"):
            with self.subTest(name=name):
                file = self.candidate / name; original = file.read_bytes()
                file.write_bytes(original + b"modified")
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    verify(self.root)
                file.write_bytes(original)
        file = self.candidate / "runtime/ledgence/worker/workflow.py"
        file.write_bytes(b"resealed mutation"); reseal(self.candidate)
        with self.assertRaisesRegex(ValueError, "immutable bundles"):
            verify(self.root)

    def test_partial_downgrade_interruption_restore_or_cleanup_proof_fails(self):
        original = json.loads(self.report_path.read_text())
        changes = [("passed",), ("database_cleanup",), ("process_cleanup",), ("credential_scan_clean",),
                   ("exercise", "fresh_restore"), ("exercise", "double_migration"),
                   ("exercise", "interrupted_migration", "observed_transactional_ddl"),
                   ("exercise", "old_binary_rejection", "schema_error_verified"),
                   ("exercise", "resumed", "local_step_not_reexecuted")]
        for path in changes:
            with self.subTest(path=path):
                report = copy.deepcopy(original); target = report
                for key in path[:-1]: target = target[key]
                target[path[-1]] = False
                self.report_path.write_text(json.dumps(report))
                with self.assertRaises(ValueError): verify(self.root)
        self.report_path.write_text(json.dumps(original))
        for key in ("provenance_after", "exercise"):
            report = copy.deepcopy(original); del report[key]
            self.report_path.write_text(json.dumps(report))
            with self.assertRaises(ValueError): verify(self.root)

    def test_wrong_published_identity_and_dirty_candidate_fail_even_resealed(self):
        for directory, key, value in ((self.root / "previous-0.9.0", "release_commit", "d" * 40),
                                      (self.candidate, "source_tree_clean", False)):
            file = directory / "provenance.json"; original = file.read_bytes()
            record = json.loads(original); record[key] = value; file.write_text(json.dumps(record)); reseal(directory)
            with self.assertRaises(ValueError): self.proof()
            file.write_bytes(original); reseal(directory)

    def test_duplicate_and_traversal_manifest_paths_fail(self):
        path = self.candidate / "SHA256SUMS"; original = path.read_text()
        for extra in (original.splitlines()[0], "a" * 64 + "  ../outside", "a" * 64 + "  runtime//extra"):
            path.write_text(original + extra + "\n")
            with self.assertRaises(ValueError): self.proof()
        path.write_text(original)

    def test_s3_requires_restart_clean_source_pin_and_owned_cleanup(self):
        evidence = self.root / "s3"; evidence.mkdir()
        root = Path(__file__).resolve().parents[2]
        record = {"passed": True, "restart_validated": True, "cleanup_complete": True, "source_dirty": False,
                  "source_sha": json.loads((self.candidate / "provenance.json").read_text())["source_commit"],
                  "harness_sha256": digest(root / "tools/check-artifact-s3.py"), "phases": ["write", "read"],
                  "fixture_image": "docker.io/rustfs/rustfs:1.0.4@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2"}
        path = evidence / "summary.json"; path.write_text(json.dumps(record)); verify_s3(evidence, self.candidate)
        for key, value in (("restart_validated", False), ("source_dirty", True), ("source_sha", "a" * 40),
                           ("cleanup_complete", False), ("phases", ["write"]), ("fixture_image", None)):
            changed = dict(record, **{key: value}); path.write_text(json.dumps(changed))
            with self.assertRaises(ValueError): verify_s3(evidence, self.candidate)


if __name__ == "__main__": unittest.main()
