"""Host fixture preparation and immutable local catalog publication checks."""
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import program_fixture
from http_acceptance.harness import Deployment


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("local_publisher", ROOT / "deploy/local/publish.py")
PUBLISHER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLISHER)


class PackageTests(unittest.TestCase):
    def test_selected_interpreter_and_protocol_are_explicit(self):
        for protocol in (2, 3):
            with self.subTest(protocol=protocol), tempfile.TemporaryDirectory() as temporary:
                package = program_fixture.prepare_package(Path(temporary) / "package", "fixture", "1.0.0",
                                                          "def handle(event): return event['data']\n",
                                                          python=sys.executable, protocol=protocol)
                manifest = json.loads((package / "ledgence-program.json").read_text())
                self.assertEqual(manifest["runtime"], {"kind": "python", "python": f"{sys.version_info.major}.{sys.version_info.minor}",
                                                      "protocol": protocol})
                self.assertEqual(manifest["program"], {"id": "fixture", "version": "1.0.0"})
                self.assertEqual(manifest["platform"]["os"], {"Darwin": "macos", "Linux": "linux"}[platform.system()])
                self.assertEqual(manifest["handler"], "program:handle")
                self.assertEqual({p.name for p in package.iterdir()}, {"program.py", "ledgence-program.json"})
                before = (package / "program.py").read_bytes()
                with self.assertRaises(FileExistsError):
                    program_fixture.prepare_package(package, "changed", "2.0.0", "changed", python=sys.executable)
                self.assertEqual((package / "program.py").read_bytes(), before)
                self.assertEqual(json.loads((package / "ledgence-program.json").read_text()), manifest)

    def test_bad_interpreter_or_platform_fails_before_creating_package(self):
        valid = {"implementation": "cpython", "version": [3, 14], "os": "Linux", "arch": "x86_64"}
        for change in ({"implementation": "pypy"}, {"version": [3, 10]}, {"os": "Windows"}, {"arch": "sparc"}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                package = Path(temporary) / "package"
                with patch.object(program_fixture.subprocess, "check_output", return_value=json.dumps(valid | change)) as probe:
                    with self.assertRaises(ValueError):
                        program_fixture.prepare_package(package, "fixture", "1.0.0", "source", python="selected-python")
                    self.assertEqual(probe.call_args.args[0][:4], ["selected-python", "-I", "-S", "-c"])
                    self.assertEqual(probe.call_args.kwargs["timeout"], 5)
                self.assertFalse(package.exists())

    def test_probe_failure_leaves_no_package(self):
        for failure in (subprocess.CalledProcessError(1, "python"), subprocess.TimeoutExpired("python", 5)):
            with self.subTest(failure=type(failure)), tempfile.TemporaryDirectory() as temporary:
                package = Path(temporary) / "package"
                with patch.object(program_fixture.subprocess, "check_output", side_effect=failure), self.assertRaises(type(failure)):
                    program_fixture.prepare_package(package, "fixture", "1.0.0", "source", python="selected-python")
                self.assertFalse(package.exists())


class AcceptancePackageTests(unittest.TestCase):
    def test_task_protocol_stays_two_unless_workflow_protocol_is_requested(self):
        for requested, expected in ((None, 2), (3, 3)):
            with self.subTest(protocol=requested), tempfile.TemporaryDirectory() as temporary:
                deployment = Deployment.__new__(Deployment)
                deployment.directory = Path(temporary)
                deployment.store = deployment.directory / "store"
                deployment.python = sys.executable
                calls = []

                def publish(binary, arguments):
                    self.assertEqual(binary, "ledgence")
                    self.assertEqual(arguments[:2], ["program", "publish"])
                    package = Path(arguments[arguments.index("--source") + 1])
                    manifest = json.loads((package / "ledgence-program.json").read_text())
                    self.assertEqual(manifest["runtime"]["protocol"], expected)
                    self.assertEqual(manifest["program"], {"id": "invoice", "version": "1.0.0"})
                    self.assertEqual((package / "program.py").read_text(), "fixture source")
                    self.assertEqual((package / "prepared_dependency.py").read_text(), "VALUE = 'packaged'\n")
                    calls.append(arguments)
                    return {"published": True}

                deployment.command = publish
                result = deployment.publish("invoice", "1.0.0", program_source="fixture source", runtime_protocol=requested)
                self.assertEqual(result, {"published": True})
                self.assertEqual(len(calls), 1)


class LocalPublicationTests(unittest.TestCase):
    def test_existing_coordinates_keep_artifact_contents_and_register_after_publication(self):
        # These programs are immutable catalog entries used across Compose runs.
        expected = [
            ("invoice-issuer", "1.0.0", 2, PUBLISHER.TASK, "task"),
            ("workflow-example", "1.0.1", 3, (ROOT / "examples/checkpoint-workflow/controller/program.py").read_text(), "workflow"),
            ("workflow-summary", "1.0.0", 2, (ROOT / "examples/checkpoint-workflow/child/program.py").read_text(), "task"),
        ]
        calls = []

        def run(command, *, check):
            self.assertIs(check, True)
            index, stage = divmod(len(calls), 2)
            name, version, protocol, code, kind = expected[index]
            if stage == 0:
                self.assertEqual(command[:3], ["ledgence", "program", "publish"])
                package = Path(command[command.index("--source") + 1])
                self.assertEqual({p.name for p in package.iterdir()}, {"program.py", "ledgence-program.json"})
                self.assertEqual((package / "program.py").read_text(), code)
                # Preserve the old fixture manifest bytes, including key order and whitespace.
                manifest = {"schema_version": 1, "program": {"id": name, "version": version},
                            "runtime": {"kind": "python", "python": "3.14", "protocol": protocol},
                            "handler": "program:handle", "platform": {"os": "linux", "arch": "aarch64"}}
                self.assertEqual((package / "ledgence-program.json").read_bytes(), json.dumps(manifest).encode())
            else:
                self.assertEqual(command, ["ledgence", "program", "register", "--server", "http://orchestrator:8080",
                                          "--program", name, "--version", version, "--kind", kind])
            calls.append(command)

        info = {"implementation": "cpython", "version": [3, 14], "os": "Linux", "arch": "aarch64"}
        with patch.object(PUBLISHER, "EXAMPLES", ROOT / "examples/checkpoint-workflow"), \
                patch.object(PUBLISHER.subprocess, "run", side_effect=run), \
                patch.object(program_fixture.subprocess, "check_output", return_value=json.dumps(info)):
            PUBLISHER.main()
        self.assertEqual(len(calls), 6)

    def test_failed_publish_never_registers_or_advances_catalog(self):
        with patch.object(PUBLISHER, "prepare_package", return_value=Path("/fixture")), \
                patch.object(PUBLISHER.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "publish")) as command:
            with self.assertRaises(subprocess.CalledProcessError):
                PUBLISHER.main()
        command.assert_called_once()
        self.assertEqual(command.call_args.args[0][:3], ["ledgence", "program", "publish"])


if __name__ == "__main__":
    unittest.main()
