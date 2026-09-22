"""Exercise artifact identity, local Git release selection and payload preservation."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import promote
from package import archive_tree
from verify import checksum, extract_archive, verify_files


class PromotionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ledgence-promotion-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repository = self.root / "repository"
        self.repository.mkdir()
        self.git("init", "-q", "-b", "develop")
        self.git("config", "user.name", "ledgence-dev")
        self.git("config", "user.email", "dev@ledgence.com")
        (self.repository / "Cargo.toml").write_text('[workspace.package]\nversion = "0.1.0"\n')
        client = self.repository / "sdk/python-client/pyproject.toml"
        client.parent.mkdir(parents=True)
        client.write_text('[project]\nversion = "0.1.0"\n')
        (self.repository / "Cargo.lock").write_bytes(b"# reviewed lock\n")
        self.git("add", ".")
        self.git("commit", "-qm", "candidate source")
        self.source = self.git("rev-parse", "HEAD")
        self.epoch = int(self.git("show", "-s", "--format=%ct"))
        self.label = "ledgence-0.1.0-rc.7+g" + self.source[:12] + "-fixture-target"
        self.stage = self.root / self.label
        self.stage.mkdir()
        self.payload = {
            "bin/ledgence": b"cli\x00bytes\n", "bin/ledgence-worker": b"worker\x00bytes\n",
            "bin/ledgence-orchestrator": b"orchestrator\x00bytes\n",
            "runtime/ledgence/worker/bootstrap.py": b"# unchanged helper\n",
            "python-client/ledgence_client-0.1.0-py3-none-any.whl": b"unchanged wheel",
            "python-client/ledgence_client-0.1.0.tar.gz": b"unchanged sdist",
            "python-client-validation.json": b'{"tested":true}\n',
            "LICENSE": b"MIT\n", "legal/dependency.txt": b"original third-party notice\n",
            "docs/releasing.md": b"original docs\n", "docs/SHA256SUMS": b"a nested payload manifest\n",
            "Cargo.lock": b"# reviewed lock\n",
        }
        for name, content in self.payload.items():
            path = self.stage / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            path.chmod(0o755 if name.startswith("bin/") else 0o644)
        provenance = {"format": 1, "candidate": self.label, "source_commit": self.source, "source_tree_clean": True,
                      "package_version": "0.1.0", "target": "fixture-target", "source_date_epoch": self.epoch,
                      "cargo_lock_sha256": hashlib.sha256(self.payload["Cargo.lock"]).hexdigest()}
        self.provenance_bytes = (json.dumps(provenance, indent=2) + "\n").encode()
        (self.stage / "provenance.json").write_bytes(self.provenance_bytes)
        (self.stage / "README.md").write_text("# " + self.label + "\n\nThis is a release candidate, not a stable release.\n")
        self.manifest()
        self.archive = self.root / (self.label + ".tar.gz")
        self.rearchive()
        self.output = self.root / "stable-output"
        # Synthetic payload bytes cannot execute. Unit tests still check the
        # actual produced tar and all its checksums; a real bundle smoke is run
        # separately with the unchanged production verifier.
        self.verifier = patch("promote.verify_archive", side_effect=self.verify_fixture).start()
        self.addCleanup(patch.stopall)

    def git(self, *arguments):
        return subprocess.check_output(["git", *arguments], cwd=self.repository, text=True, stderr=subprocess.PIPE).strip()

    def manifest(self):
        (self.stage / "SHA256SUMS").write_text("".join(f"{checksum(path)}  {path.relative_to(self.stage)}\n"
            for path in sorted(self.stage.rglob("*")) if path.is_file() and path != self.stage / "SHA256SUMS"))

    def rearchive(self):
        archive_tree(self.stage, self.archive, self.epoch)
        self.expected = checksum(self.archive)

    def verify_fixture(self, archive, python):
        with tempfile.TemporaryDirectory(dir=self.root) as directory:
            verify_files(extract_archive(archive, Path(directory) / "checked"))

    def run_promotion(self, **changes):
        arguments = dict(archive=self.archive, expected_sha256=self.expected, output=self.output,
                         repository=self.repository, release_ref="HEAD", version="0.1.0")
        arguments.update(changes)
        return promote.promote(**arguments)

    def test_source_equivalent_merge_preserves_exact_payload_and_original_provenance(self):
        self.git("checkout", "-qb", "qualified")
        self.git("commit", "--allow-empty", "-qm", "qualification reference")
        self.git("checkout", "-qb", "release-preparation", self.source)
        self.git("merge", "--no-ff", "-qm", "prepare release", "qualified")
        release = self.git("rev-parse", "HEAD")
        self.assertNotEqual(release, self.source)
        result = self.run_promotion(release_ref="refs/heads/release-preparation")
        stable = extract_archive(Path(result["archive"]), self.root / "result")
        self.assertEqual(stable.name, "ledgence-0.1.0-fixture-target")
        verify_files(stable)
        self.assertEqual((stable / "candidate-provenance.json").read_bytes(), self.provenance_bytes)
        for name, content in self.payload.items():
            self.assertEqual((stable / name).read_bytes(), content, name)
            self.assertEqual((stable / name).stat().st_mode & 0o777, 0o755 if name.startswith("bin/") else 0o644)
        record = json.loads((stable / "provenance.json").read_text())
        self.assertEqual(record["release_commit"], release)
        self.assertEqual(record["source_commit"], self.source)
        self.assertEqual(record["promoted_from"]["archive_sha256"], self.expected)
        self.assertEqual(record["release_tree"], self.git("rev-parse", self.source + "^{tree}"))
        self.assertNotIn("not a stable release", (stable / "README.md").read_text())
        self.assertEqual(self.git("rev-parse", "HEAD"), release)
        self.assertEqual(self.git("tag"), "")
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.verifier.assert_called_once()

    def test_rejects_wrong_outer_checksum_without_output(self):
        with self.archive.open("ab") as stream:
            stream.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "archive SHA256"):
            self.run_promotion()
        self.assertFalse(self.output.exists())
        self.verifier.assert_not_called()

    def test_rejects_tampered_payload_even_with_matching_outer_checksum(self):
        (self.stage / "bin/ledgence-worker").write_bytes(b"replaced binary")
        self.rearchive()
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.run_promotion()
        self.assertFalse(self.output.exists())

    def test_rejects_release_commit_with_different_source_tree(self):
        (self.repository / "different-source").write_text("changed")
        self.git("add", ".")
        self.git("commit", "-qm", "different release tree")
        with self.assertRaisesRegex(ValueError, "tree differs"):
            self.run_promotion()
        self.assertFalse(self.output.exists())

    def test_rejects_release_ref_that_does_not_identify_checkout_head(self):
        self.git("commit", "--allow-empty", "-qm", "later commit")
        with self.assertRaisesRegex(ValueError, "checkout's HEAD"):
            self.run_promotion(release_ref=self.source)
        self.assertFalse(self.output.exists())

    def test_rejects_dirty_release_checkout(self):
        (self.repository / "untracked").write_text("not committed")
        with self.assertRaisesRegex(ValueError, "must be clean"):
            self.run_promotion()
        self.assertFalse(self.output.exists())

    def test_rejects_same_tree_without_build_source_ancestry(self):
        tree = self.git("rev-parse", "HEAD^{tree}")
        unrelated = self.git("commit-tree", tree, "-m", "unrelated identical tree")
        self.git("checkout", "--detach", "-q", unrelated)
        with self.assertRaisesRegex(ValueError, "must be an ancestor"):
            self.run_promotion()

    def test_does_not_overwrite_existing_output(self):
        self.output.mkdir()
        marker = self.output / "preserve"
        marker.write_bytes(b"untouched")
        with self.assertRaisesRegex(ValueError, "NEW directory"):
            self.run_promotion()
        self.assertEqual(marker.read_bytes(), b"untouched")

    def test_rejects_changed_archive_after_metadata_comparison(self):
        def corrupted_archive(stage, destination, epoch):
            (stage / "bin/ledgence-worker").write_bytes(b"unexpected modification")
            (stage / "SHA256SUMS").write_text("".join(f"{checksum(path)}  {path.relative_to(stage)}\n"
                for path in sorted(stage.rglob("*")) if path.is_file() and path != stage / "SHA256SUMS"))
            archive_tree(stage, destination, epoch)
        with patch("promote.archive_tree", side_effect=corrupted_archive):
            with self.assertRaisesRegex(ValueError, "stable archive changed"):
                self.run_promotion()
        self.assertFalse(self.output.exists())

    def test_rejects_release_ref_moving_during_verification(self):
        def verify_and_move(archive, python):
            self.verify_fixture(archive, python)
            self.git("commit", "--allow-empty", "-qm", "release moved")
        self.verifier.side_effect = verify_and_move
        with self.assertRaisesRegex(ValueError, "changed during promotion"):
            self.run_promotion()
        self.assertFalse(self.output.exists())

    def test_rejects_version_that_would_relabel_embedded_packages(self):
        with self.assertRaisesRegex(ValueError, "provenance, version"):
            self.run_promotion(version="0.2.0")
        self.assertFalse(self.output.exists())

    def test_rejects_header_modes_that_extraction_would_normalize(self):
        with tarfile.open(self.archive, "r:gz") as archive:
            entries = [(entry, archive.extractfile(entry).read() if entry.isfile() else None)
                       for entry in archive.getmembers()]
        with tarfile.open(self.archive, "w:gz") as archive:
            for entry, content in entries:
                if entry.name == self.label + "/bin/ledgence-worker":
                    entry.mode = 0o777
                archive.addfile(entry, io.BytesIO(content) if content is not None else None)
        with self.assertRaisesRegex(ValueError, "noncanonical mode"):
            self.run_promotion(expected_sha256=checksum(self.archive))
        self.assertFalse(self.output.exists())
        self.verifier.assert_not_called()

    def test_rejects_archive_links_before_extraction(self):
        with tarfile.open(self.archive, "w:gz") as archive:
            info = tarfile.TarInfo(self.label + "/escape")
            info.type = tarfile.SYMTYPE
            info.linkname = str(self.repository / "Cargo.lock")
            archive.addfile(info)
        with self.assertRaisesRegex(ValueError, "invalid entry"):
            self.run_promotion(expected_sha256=checksum(self.archive))
        self.assertEqual((self.repository / "Cargo.lock").read_bytes(), b"# reviewed lock\n")


if __name__ == "__main__":
    unittest.main()
