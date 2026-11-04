#!/usr/bin/env python3
"""Offline tests for corresponding-source collection and release verification."""
import copy
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("image_sources", Path(__file__).with_name("image_sources.py"))
sources = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sources)


class FixtureDownloader:
    def __init__(self):
        self.content = {}
        self.indices = {}
        self.calls = []

    def json(self, url):
        return copy.deepcopy(self.indices[url])

    def fetch(self, url, destination, *, sha256=None, sha1=None, size=None):
        self.calls.append(url)
        data = self.content[url]
        sources.require(sha256 is None or hashlib.sha256(data).hexdigest() == sha256, "SHA-256 mismatch")
        sources.require(sha1 is None or hashlib.sha1(data).hexdigest() == sha1, "snapshot mismatch")
        sources.require(size is None or len(data) == size, "size mismatch")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ledgence-image-sources-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.inventory_path = self.directory / "runtime.json"
        self.output = self.directory / "output"
        self.downloader = FixtureDownloader()
        self.image = {"id": "sha256:" + "a" * 64, "platform": "linux/arm64",
                      "requested_reference": "ledgence:test", "repository_digests": []}
        python = b"fixture python sources"
        python_hash = hashlib.sha256(python).hexdigest()
        recipe = f"ENV PYTHON_VERSION 3.14.7\nENV PYTHON_SHA256 {python_hash}\n".encode()
        self.inventory = {
            "format": 1, "base_image": "python:3.14-slim-bookworm@sha256:" + "b" * 64,
            "python": {"version": "3.14.7", "source_url":
                       "https://www.python.org/ftp/python/3.14.7/Python-3.14.7.tar.xz",
                       "sha256": python_hash},
            "base_recipe": {"url": "https://raw.githubusercontent.com/docker-library/python/"
                            + "c" * 40 + "/3.14/slim-bookworm/Dockerfile",
                            "sha256": hashlib.sha256(recipe).hexdigest()},
            "packages": [{"binary_name": "sample", "binary_version": "1:2.0-3+b1",
                          "architecture": "arm64", "source_name": "sample", "source_version": "1:2.0-3"},
                         {"binary_name": "sample-data", "binary_version": "1:2.0-3",
                          "architecture": "all", "source_name": "sample", "source_version": "1:2.0-3"}],
        }
        self.downloader.content[self.inventory["python"]["source_url"]] = python
        self.downloader.content[self.inventory["base_recipe"]["url"]] = recipe
        self.add_package()
        self.save_inventory()

    def add_package(self, name="sample", version="1:2.0-3"):
        payloads = {f"{name}_2.0.orig.tar.xz": b"upstream source",
                    f"{name}_2.0-3.debian.tar.xz": b"Debian build configuration"}
        dsc = (f"Source: {name}\nVersion: {version}\nChecksums-Sha256:\n" + "".join(
            f" {hashlib.sha256(content).hexdigest()} {len(content)} {filename}\n"
            for filename, content in sorted(payloads.items()))).encode()
        payloads[f"{name}_2.0-3.dsc"] = dsc
        index = {"package": name, "version": version, "result": [], "fileinfo": {}}
        for filename, content in payloads.items():
            checksum = hashlib.sha1(content).hexdigest()
            index["result"].append({"hash": checksum})
            index["fileinfo"][checksum] = [{"name": filename, "size": len(content),
                                           "archive_name": "debian"}]
            self.downloader.content[f"{sources.SNAPSHOT}/file/{checksum}"] = content
        endpoint = (f"{sources.SNAPSHOT}/mr/package/{name}/"
                    f"{sources.quote(version, safe='')}/srcfiles?fileinfo=1")
        self.downloader.indices[endpoint] = index
        return endpoint

    def save_inventory(self):
        self.inventory_path.write_text(json.dumps(self.inventory))

    def collect(self, **kwargs):
        return sources.collect(self.inventory_path, self.output, downloader=self.downloader,
                               image=self.image, **kwargs)

    def rewrite_artifact(self, edits):
        """Rehash changed payloads to exercise semantic checks beyond outer SHA."""
        with tarfile.open(self.output / "corresponding-source.tar.gz", "r:gz") as archive:
            files = {member.name: archive.extractfile(member).read() for member in archive}
        edits(files)
        with tarfile.open(self.output / "corresponding-source.tar.gz", "w:gz") as archive:
            for name, data in files.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                archive.addfile(entry, io.BytesIO(data))
        (self.output / "SHA256SUMS").write_text("".join(
            f"{sources.digest(self.output / name)}  {name}\n"
            for name in ("corresponding-source.tar.gz", "source-manifest.json")))

    def test_collect_deduplicates_sources_and_preserves_binary_inventory(self):
        manifest = self.collect()
        self.assertEqual(manifest["binary_package_count"], 2)
        self.assertEqual(manifest["source_package_count"], 1)
        self.assertEqual(len(manifest["debian"][0]["files"]), 3)
        self.assertEqual(len(self.downloader.calls), 5)
        self.assertEqual(sources.verify_output(self.output, self.image["id"], "linux/arm64"), manifest)
        self.assertEqual({path.name for path in self.output.iterdir()},
                         {"corresponding-source.tar.gz", "source-manifest.json", "SHA256SUMS"})

    def test_reproducible_archive(self):
        self.collect()
        other = self.directory / "second"
        sources.collect(self.inventory_path, other, downloader=self.downloader, image=self.image)
        self.assertEqual((self.output / "corresponding-source.tar.gz").read_bytes(),
                         (other / "corresponding-source.tar.gz").read_bytes())

    def test_existing_output_not_overwritten(self):
        self.output.mkdir()
        retained = self.output / "user.txt"
        retained.write_text("keep")
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.collect()
        self.assertEqual(retained.read_text(), "keep")

    def test_source_download_failure_leaves_no_incomplete_artifact(self):
        self.downloader.content[self.inventory["python"]["source_url"]] = b"wrong source"
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.collect()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.directory.glob(".ledgence-sources-*")), [])

    def test_package_index_identity_mismatch(self):
        next(iter(self.downloader.indices.values()))["version"] = "other"
        with self.assertRaisesRegex(ValueError, "identity"):
            self.collect()

    def test_missing_source_file_rejected(self):
        index = next(iter(self.downloader.indices.values()))
        index["fileinfo"] = {}
        with self.assertRaisesRegex(ValueError, "filename"):
            self.collect()

    def test_package_payload_checksum_mismatch(self):
        index = next(iter(self.downloader.indices.values()))
        checksum = index["result"][0]["hash"]
        self.downloader.content[f"{sources.SNAPSHOT}/file/{checksum}"] = b"changed"
        with self.assertRaises(ValueError):
            self.collect()

    def test_dsc_exact_version_and_signed_record(self):
        content = ("-----BEGIN PGP SIGNED MESSAGE-----\nHash: SHA256\n\n"
                   "Source: sample\nVersion: 1.0-1\nChecksums-Sha256:\n "
                   + "a" * 64 + " 12 source.tar.xz\n-----BEGIN PGP SIGNATURE-----\nsignature\n").encode()
        self.assertEqual(sources.parse_dsc(content, "sample", "1.0-1")["source.tar.xz"]["size"], 12)
        with self.assertRaisesRegex(ValueError, "differs"):
            sources.parse_dsc(content, "sample", "2.0-1")

    def test_dsc_unsafe_or_duplicate_paths_rejected(self):
        for name in ("../source.tar", "/source.tar", "source/child.tar"):
            content = f"Source: sample\nVersion: 1\nChecksums-Sha256:\n {'a' * 64} 1 {name}\n".encode()
            with self.subTest(name=name), self.assertRaises(ValueError):
                sources.parse_dsc(content, "sample", "1")

    def test_mutable_recipe_identity_rejected(self):
        self.inventory["base_recipe"]["url"] = self.inventory["base_recipe"]["url"].replace("c" * 40, "master")
        self.save_inventory()
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.collect()

    def test_python_recipe_must_match_runtime_version(self):
        recipe = b"ENV PYTHON_VERSION 3.14.1\n"
        self.downloader.content[self.inventory["base_recipe"]["url"]] = recipe
        self.inventory["base_recipe"]["sha256"] = hashlib.sha256(recipe).hexdigest()
        self.save_inventory()
        with self.assertRaisesRegex(ValueError, "recipe differs"):
            self.collect()

    def test_inventory_duplicate_binary_rejected(self):
        self.inventory["packages"].append(self.inventory["packages"][0])
        self.save_inventory()
        with self.assertRaisesRegex(ValueError, "duplicate binary"):
            self.collect()

    def test_dpkg_multiarch_binary_names_are_supported(self):
        self.inventory["packages"][0]["binary_name"] = "sample:arm64"
        self.save_inventory()
        self.collect()

    def test_mismatched_dpkg_architecture_is_rejected(self):
        self.inventory["packages"][0]["binary_name"] = "sample:amd64"
        self.save_inventory()
        with self.assertRaisesRegex(ValueError, "qualifier"):
            self.collect()

    def test_empty_inventory_rejected(self):
        self.inventory["packages"] = []
        self.save_inventory()
        with self.assertRaisesRegex(ValueError, "empty"):
            self.collect()

    def test_verifier_rejects_wrong_image_and_architecture(self):
        self.collect()
        with self.assertRaisesRegex(ValueError, "image identity"):
            sources.verify_output(self.output, image="sha256:" + "f" * 64)
        with self.assertRaisesRegex(ValueError, "platform"):
            sources.verify_output(self.output, platform="linux/amd64")

    def test_verifier_rejects_corrupted_archive(self):
        self.collect()
        with (self.output / "corresponding-source.tar.gz").open("ab") as stream:
            stream.write(b"corruption")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            sources.verify_output(self.output)

    def test_verifier_rejects_extra_file_even_with_rehashed_archive(self):
        self.collect()
        self.rewrite_artifact(lambda files: files.update({"corresponding-source/extra": b"extra"}))
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            sources.verify_output(self.output)

    def test_verifier_rejects_payload_change_even_with_rehashed_archive(self):
        self.collect()
        self.rewrite_artifact(lambda files: files.update({"corresponding-source/python/Dockerfile": b"changed"}))
        with self.assertRaisesRegex(ValueError, "payload checksum"):
            sources.verify_output(self.output)

    def test_verifier_rejects_missing_source_even_with_rehashed_archive(self):
        self.collect()
        self.rewrite_artifact(lambda files: files.pop("corresponding-source/python/Dockerfile"))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            sources.verify_output(self.output)

    def previous_source(self):
        """Build the previous archive through the same collector as an actual release."""
        self.collect()
        archive = self.output / "corresponding-source.tar.gz"
        return archive, sources.digest(archive)

    def current_image(self, **changes):
        return dict(self.image, id="sha256:" + "e" * 64, requested_reference="ledgence:current",
                    repository_digests=["ledgence@sha256:" + "d" * 64], **changes)

    def reuse(self, archive, checksum, *, output_name="rebound", image=None):
        return sources.collect(self.inventory_path, self.directory / output_name,
                               image=image or self.current_image(), downloader=self.downloader,
                               seed_archive=archive, seed_sha256=checksum)

    def test_offline_reuse_is_byte_identical_to_fresh_sources_for_current_image(self):
        archive, checksum = self.previous_source()
        new_image = self.current_image()
        cold = self.directory / "cold"
        cold_manifest = sources.collect(self.inventory_path, cold, downloader=self.downloader, image=new_image)
        with patch.object(self.downloader, "json", side_effect=AssertionError("unexpected index fetch")), \
             patch.object(self.downloader, "fetch", side_effect=AssertionError("unexpected source download")):
            manifest = self.reuse(archive, checksum, image=new_image)
        rebound = self.directory / "rebound"
        self.assertEqual(manifest, cold_manifest)
        self.assertEqual(manifest["image"], new_image)
        self.assertEqual(sources.verify_output(rebound, new_image["id"], new_image["platform"]), manifest)
        with self.assertRaisesRegex(ValueError, "image identity"):
            sources.verify_output(rebound, self.image["id"])
        for filename in ["corresponding-source.tar.gz", "source-manifest.json", "SHA256SUMS"]:
            self.assertEqual((cold / filename).read_bytes(), (rebound / filename).read_bytes())
        self.assertNotEqual(checksum, sources.digest(rebound / "corresponding-source.tar.gz"))
        self.assertEqual(checksum, sources.digest(archive), "rebind modified the original archive")

    def test_cli_reuses_seed_and_verifies_current_image(self):
        archive, checksum = self.previous_source()
        output = self.directory / "cli-artifact"
        current = self.current_image()
        def inspect_image(reference, platform, inventory):
            self.assertEqual((reference, platform), (current["requested_reference"], current["platform"]))
            inventory.write_bytes(self.inventory_path.read_bytes())
            return current
        collect = ["image_sources.py", "--image", current["requested_reference"], "--platform", current["platform"],
                   "--output", str(output), "--seed-archive", str(archive), "--seed-sha256", checksum]
        verify = ["image_sources.py", "--verify", str(output), "--platform", current["platform"],
                  "--expected-image", current["id"]]
        with patch.object(sources, "inventory_from_image", side_effect=inspect_image) as inspection, \
             patch.object(sources.Downloader, "json", side_effect=AssertionError("unexpected index fetch")), \
             patch.object(sources.Downloader, "fetch", side_effect=AssertionError("unexpected source download")), \
             patch.object(sys, "argv", collect), redirect_stdout(io.StringIO()):
            sources.main()
        inspection.assert_called_once()
        with patch.object(sys, "argv", verify), redirect_stdout(io.StringIO()) as console:
            sources.main()
        self.assertTrue(json.loads(console.getvalue())["verified"])

    def test_changed_runtime_inventory_uses_authoritative_source_downloads(self):
        archive, checksum = self.previous_source()
        self.inventory["packages"][0]["binary_version"] = "1:2.0-3+b2"
        self.save_inventory()
        self.downloader.calls.clear()
        manifest = self.reuse(archive, checksum)
        self.assertEqual(len(self.downloader.calls), 5)
        self.assertEqual(manifest["runtime_inventory_sha256"], sources.digest(self.inventory_path))
        self.assertEqual(sources.verify_output(self.directory / "rebound", self.current_image()["id"]), manifest)
        with tarfile.open(self.directory / "rebound" / "corresponding-source.tar.gz") as new:
            inventory_bytes = new.extractfile("corresponding-source/runtime-inventory.json").read()
        self.assertEqual(inventory_bytes, self.inventory_path.read_bytes())

    def test_inventory_match_includes_the_exact_bytes(self):
        archive, checksum = self.previous_source()
        self.inventory_path.write_text(json.dumps(self.inventory, indent=2) + "\n")
        self.downloader.calls.clear()
        manifest = self.reuse(archive, checksum)
        self.assertEqual(len(self.downloader.calls), 5)
        self.assertEqual(manifest["runtime_inventory_sha256"], sources.digest(self.inventory_path))

    def test_changed_inventory_with_failed_download_cannot_emit_prior_source(self):
        archive, checksum = self.previous_source()
        self.inventory["packages"][0]["binary_version"] = "1:2.0-3+b2"
        self.save_inventory()
        self.downloader.content[self.inventory["python"]["source_url"]] = b"changed"
        with self.assertRaises(ValueError):
            self.reuse(archive, checksum)
        self.assertFalse((self.directory / "rebound").exists())

    def test_wrong_seed_platform_cannot_use_identical_architecture_independent_inventory(self):
        for package in self.inventory["packages"]:
            package["architecture"] = "all"
        self.save_inventory()
        archive, checksum = self.previous_source()
        with patch.object(self.downloader, "json", side_effect=AssertionError("unexpected fallback")), \
             patch.object(self.downloader, "fetch", side_effect=AssertionError("unexpected fallback")), \
             self.assertRaises(ValueError):
            self.reuse(archive, checksum, image=self.current_image(platform="linux/amd64"))
        self.assertFalse((self.directory / "rebound").exists())

    def test_unbound_previous_manifest_is_rejected(self):
        sources.collect(self.inventory_path, self.output, downloader=self.downloader)
        archive = self.output / "corresponding-source.tar.gz"
        with self.assertRaises(ValueError):
            self.reuse(archive, sources.digest(archive))
        self.assertFalse((self.directory / "rebound").exists())

    def test_seed_checksum_mandatory_and_checked_before_fallback(self):
        archive, checksum = self.previous_source()
        self.inventory_path.write_text(json.dumps(self.inventory, indent=2))
        options = [
            (archive, None), (None, checksum), (archive, "z" * 64),
            (archive, "0" * 64), (self.directory / "missing.tar.gz", checksum),
        ]
        for candidate, expected in options:
            with self.subTest(candidate=candidate, expected=expected), \
                 patch.object(self.downloader, "json", side_effect=AssertionError("unexpected fallback")), \
                 patch.object(self.downloader, "fetch", side_effect=AssertionError("unexpected fallback")), \
                 self.assertRaises((ValueError, OSError)):
                self.reuse(candidate, expected)
            self.assertFalse((self.directory / "rebound").exists())

    def test_bad_seed_payload_rejected_even_when_current_inventory_has_changed(self):
        self.previous_source()
        self.inventory_path.write_text(json.dumps(self.inventory, indent=2))
        self.rewrite_artifact(lambda files: files.update({"corresponding-source/python/Dockerfile": b"invalid"}))
        archive = self.output / "corresponding-source.tar.gz"
        with patch.object(self.downloader, "json", side_effect=AssertionError("unexpected fallback")), \
             patch.object(self.downloader, "fetch", side_effect=AssertionError("unexpected fallback")), \
             self.assertRaises(ValueError):
            self.reuse(archive, sources.digest(archive))
        self.assertFalse((self.directory / "rebound").exists())

    def test_reuse_rejects_unsafe_or_duplicate_archive_members_without_extracting_them(self):
        archive, _ = self.previous_source()
        outside = self.directory / "must-not-be-written"
        additional = [
            ("corresponding-source/../../must-not-be-written", tarfile.REGTYPE),
            (str(outside), tarfile.REGTYPE),
            ("corresponding-source/source-manifest.json", tarfile.REGTYPE),
            ("corresponding-source/link", tarfile.SYMTYPE),
            ("corresponding-source/link", tarfile.LNKTYPE),
        ]
        for index, (name, kind) in enumerate(additional):
            damaged = self.directory / f"unsafe-{index}.tar.gz"
            with tarfile.open(archive, "r:gz") as old, tarfile.open(damaged, "w:gz") as new:
                for member in old:
                    new.addfile(member, old.extractfile(member))
                extra = tarfile.TarInfo(name)
                extra.type = kind
                if kind == tarfile.REGTYPE:
                    extra.size = len(b"overwrite")
                    new.addfile(extra, io.BytesIO(b"overwrite"))
                else:
                    extra.linkname = str(outside)
                    new.addfile(extra)
            with self.subTest(name=name, kind=kind), self.assertRaises(ValueError):
                self.reuse(damaged, sources.digest(damaged))
            self.assertFalse((self.directory / "rebound").exists())
            self.assertFalse(outside.exists())

    def test_non_tar_seed_with_valid_outer_digest_is_rejected(self):
        archive = self.directory / "damaged.tar.gz"
        archive.write_bytes(b"not a tar archive")
        with self.assertRaises((ValueError, tarfile.TarError, OSError)):
            self.reuse(archive, sources.digest(archive))
        self.assertFalse((self.directory / "rebound").exists())

    def test_image_inventory_uses_immutable_inspected_id(self):
        inspected = {"Id": self.image["id"], "Os": "linux", "Architecture": "arm64", "RepoDigests": []}
        responses = [subprocess.CompletedProcess([], 0, json.dumps([inspected])),
                     subprocess.CompletedProcess([], 0, self.inventory_path.read_bytes())]
        with patch.object(sources.subprocess, "run", side_effect=responses) as run:
            identity = sources.inventory_from_image("ledgence:test", "linux/arm64", self.directory / "from-image.json")
        self.assertEqual(identity["id"], self.image["id"])
        command = run.call_args_list[1].args[0]
        self.assertIn(self.image["id"], command)
        self.assertIn("--read-only", command)
        self.assertEqual(command[command.index("--network") + 1], "none")
        self.assertNotIn("ledgence:test", command)

    def test_image_inventory_rejects_wrong_architecture_before_running(self):
        inspected = {"Id": self.image["id"], "Os": "linux", "Architecture": "amd64"}
        with patch.object(sources.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, json.dumps([inspected]))) as run:
            with self.assertRaisesRegex(ValueError, "platform differs"):
                sources.inventory_from_image("ledgence:test", "linux/arm64", self.directory / "never.json")
        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
