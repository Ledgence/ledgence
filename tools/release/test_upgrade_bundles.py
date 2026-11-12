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

    def test_exact_published_identities_for_both_targets(self):
        # Independent expected identities prevent fixtures from making an
        # incorrect global source pin appear to qualify both published targets.
        expected = {
            ("0.8.0", "x86_64-unknown-linux-gnu"): ("e95f0baac01d443e0e72c3ffa5010ef5a28c6e6b", "e95f0baac01d443e0e72c3ffa5010ef5a28c6e6b", 7),
            ("0.8.0", "aarch64-apple-darwin"): ("e95f0baac01d443e0e72c3ffa5010ef5a28c6e6b", "cf1804ea98d7a18c4608b9b3082646fa41c505c4", 7),
            ("0.9.0", "x86_64-unknown-linux-gnu"): ("c5589aaae95d1dd944372beaf27847702a96e13c", "c5589aaae95d1dd944372beaf27847702a96e13c", 14),
            ("0.9.0", "aarch64-apple-darwin"): ("c5589aaae95d1dd944372beaf27847702a96e13c", "c5589aaae95d1dd944372beaf27847702a96e13c", 14),
        }
        for (version, target), (release, source, contract) in expected.items():
            with self.subTest(version=version, target=target):
                directory = self.root / (version + target)
                bundle_fixture(directory, version, target=target)
                actual = bundle(directory, predecessor=version, execute=False)
                self.assertEqual((actual["release_commit"], actual["source_commit"], actual["console_contract"], actual["target"]),
                                 (release, source, contract, target))

    def test_missing_unknown_or_inconsistent_target_is_rejected_even_resealed(self):
        directory = self.root / "previous-0.9.0"
        for name in ("provenance.json", "candidate-provenance.json"):
            path = directory / name
            original = path.read_text()
            for target in (None, "unknown-target", "aarch64-apple-darwin"):
                with self.subTest(provenance=name, target=target):
                    record = json.loads(original)
                    if target is None: record.pop("target")
                    else: record["target"] = target
                    path.write_text(json.dumps(record)); reseal(directory)
                    with self.assertRaisesRegex(ValueError, "target"):
                        bundle(directory, predecessor="0.9.0", execute=False)
            path.write_text(original); reseal(directory)

    def test_known_source_cannot_cross_platform_or_release_pins(self):
        for target, crossed_source in (("x86_64-unknown-linux-gnu", "cf1804ea98d7a18c4608b9b3082646fa41c505c4"),
                                       ("aarch64-apple-darwin", "e95f0baac01d443e0e72c3ffa5010ef5a28c6e6b")):
            directory = self.root / ("crossed-" + target)
            bundle_fixture(directory, "0.8.0", target=target)
            for name in ("provenance.json", "candidate-provenance.json"):
                path = directory / name; record = json.loads(path.read_text())
                record["source_commit"] = crossed_source
                path.write_text(json.dumps(record))
            reseal(directory)
            with self.assertRaisesRegex(ValueError, "pinned published predecessor"):
                bundle(directory, predecessor="0.8.0", execute=False)
        for version in ("0.8.0", "0.9.0"):
            for target in ("x86_64-unknown-linux-gnu", "aarch64-apple-darwin"):
                directory = self.root / ("wrong-release-" + version + target)
                bundle_fixture(directory, version, target=target)
                path = directory / "provenance.json"; record = json.loads(path.read_text())
                record["release_commit"] = "cf1804ea98d7a18c4608b9b3082646fa41c505c4"
                path.write_text(json.dumps(record)); reseal(directory)
                with self.assertRaisesRegex(ValueError, "pinned published predecessor"):
                    bundle(directory, predecessor=version, execute=False)

    def test_native_upgrade_rejects_cross_platform_candidate(self):
        directory = self.root / "macos-previous"
        bundle_fixture(directory, "0.9.0", target="aarch64-apple-darwin")
        with self.assertRaisesRegex(ValueError, "matching predecessor and candidate targets"):
            inputs(directory, self.candidate / "bin", self.candidate / "console",
                   self.candidate / "runtime/ledgence/worker/bootstrap.py", predecessor="0.9.0", execute=False)

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

    def test_old_restart_only_or_damaged_artifact_restore_receipts_fail(self):
        original = json.loads(self.report_path.read_text())
        damages = (
            lambda artifacts: artifacts.pop("backup_restore"),
            lambda artifacts: artifacts["backup_restore"].update(phases=["quiesced", "same_key_replayed"]),
            lambda artifacts: artifacts["backup_restore"].update(all_writers_stopped=False),
            lambda artifacts: artifacts["backup_restore"].update(fresh_database=False),
            lambda artifacts: artifacts["backup_restore"].update(new_store_path=False),
            lambda artifacts: artifacts["backup_restore"].pop("fresh_empty_spool"),
            lambda artifacts: artifacts["backup_restore"].update(fresh_empty_spool=False),
            lambda artifacts: artifacts["backup_restore"].update(no_new_generation=False),
            lambda artifacts: artifacts["backup_restore"].update(database_dump_sha256="missing"),
            lambda artifacts: artifacts["backup_restore"]["store_inventory"].pop(".ledgence-artifact-store"),
            lambda artifacts: artifacts["backup_restore"].update(restored_inventory_sha256="0" * 64),
            lambda artifacts: artifacts["backup_restore"].update(download_sha256="0" * 64),
            lambda artifacts: artifacts["backup_restore"]["rejected_copies"].pop("foreign_marker"),
            lambda artifacts: artifacts["backup_restore"]["rejected_copies"]["missing_marker"].update(catalog_unchanged=False),
            lambda artifacts: artifacts["backup_restore"]["rejected_copies"]["missing_marker"].pop("fresh_empty_spool"),
            lambda artifacts: artifacts["backup_restore"]["rejected_copies"]["foreign_marker"].update(fresh_empty_spool=False),
        )
        for index, damage in enumerate(damages):
            with self.subTest(damage=index):
                report = copy.deepcopy(original)
                damage(report["exercise"]["artifacts"])
                self.report_path.write_text(json.dumps(report))
                with self.assertRaises(ValueError):
                    verify(self.root)
        self.report_path.write_text(json.dumps(original))

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
