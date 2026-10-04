"""Package provenance and source/artifact separation checks (MIT)."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prepare


class PrepareTests(unittest.TestCase):
    def test_package_contains_only_sources_and_records_the_published_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            temporary = Path(folder).resolve()
            root = temporary / "repo"
            source = root / "examples" / "fulfillment-investigator"
            modules = source / "fulfillment"
            modules.mkdir(parents=True)
            (root / "LICENSE").write_text("MIT test notice\n")
            (source / "program.py").write_text("def handle(event): return event\n")
            (modules / "__init__.py").write_text("")
            (modules / "data.py").write_text("VERSION = '1'\n")
            (modules / ".env").write_text("not packaged\n")
            (modules / "fixtures.json").write_text('{"not": "program source"}\n')
            binaries = temporary / "bin"
            binaries.mkdir()
            (binaries / "ledgence").write_text("unused test publisher")
            output = temporary / "prepared"
            descriptor = {"program_id": "fulfillment-investigator", "version": "1.0.0", "digest": "test"}
            fixtures = {"schema_version": 1, "synthetic": True, "scenarios": {}}

            def data(directory):
                target = Path(directory)
                target.mkdir()
                (target / "fixtures.json").write_text(json.dumps(fixtures))
                return fixtures

            with patch.object(prepare, "HERE", source), patch.object(prepare, "ROOT", root), \
                    patch.object(prepare, "prepare_data", side_effect=data) as generated, \
                    patch.object(prepare.subprocess, "run", return_value=SimpleNamespace(stdout=json.dumps(descriptor))) as published:
                evidence = prepare.prepare(output, binaries)
            package = output / "packages" / "workflow"
            names = {path.relative_to(package).as_posix() for path in package.rglob("*") if path.is_file()}
            self.assertEqual(names, {"program.py", "fulfillment/__init__.py", "fulfillment/data.py", "LICENSE", "ledgence-program.json"})
            self.assertEqual(evidence["packages"], {"workflow": descriptor})
            self.assertEqual(evidence["application_dependencies"], [])
            self.assertEqual(evidence["fixtures"], fixtures)
            self.assertEqual(evidence["data_directory"], str(output / "data"))
            self.assertEqual(evidence, json.loads((output / "prepared.json").read_text()))
            for name in names:
                self.assertEqual(evidence["package_file_sha256"][name], hashlib.sha256((package / name).read_bytes()).hexdigest())
            manifest = json.loads((package / "ledgence-program.json").read_text())
            self.assertEqual(manifest["runtime"]["protocol"], 3)
            self.assertEqual(manifest["runtime"]["python"], f"{sys.version_info.major}.{sys.version_info.minor}")
            self.assertEqual(manifest["handler"], "program:handle")
            self.assertEqual(published.call_args.args[0], [str(binaries / "ledgence"), "program", "publish", "--source", str(package), "--store", str(output / "store")])
            generated.assert_called_once_with(str(output / "data"))

    def test_existing_output_and_repository_destination_are_not_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            temporary = Path(folder).resolve()
            root = temporary / "repo"
            source = root / "examples" / "fulfillment-investigator"
            source.mkdir(parents=True)
            (source / "program.py").write_text("# example\n")
            binaries = temporary / "bin"
            binaries.mkdir()
            (binaries / "ledgence").write_text("unused test publisher")
            existing = temporary / "existing"
            existing.mkdir()
            sentinel = existing / "keep.txt"
            sentinel.write_text("preserve\n")
            with patch.object(prepare, "HERE", source), patch.object(prepare, "ROOT", root), \
                    patch.object(prepare.subprocess, "run") as published, patch.object(prepare, "prepare_data") as generated:
                with self.assertRaises(FileExistsError):
                    prepare.prepare(existing, binaries)
                with self.assertRaisesRegex(ValueError, "outside the repository"):
                    prepare.prepare(root / "generated", binaries)
                published.assert_not_called()
                generated.assert_not_called()
            self.assertEqual(sentinel.read_text(), "preserve\n")
            self.assertFalse((root / "generated").exists())


if __name__ == "__main__":
    unittest.main()
