"""Keep provider qualification bound to current and historical source (MIT)."""

import ast
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/ci.yml"
PREDECESSOR = "3a5cc0d37cb70baeb300d0f77be3659f8c2ef48f"
FAMILIES = {
    "pending-capacity-auth-public-surfaces",
    "lost-provider-and-report-replies-expired-key",
    "lost-claim-and-no-lookup",
    "typed-provider-terminal-and-human-action",
    "bounded-probe-timeout-and-capacity",
    "concurrent-business-and-provider-domains",
    "typed-dispute-before-selection",
    "cancelled-original-report-no-revival",
    "real-worker-kill-and-replacement",
    "prior-worker-before-provider-callback",
    "cancelled-retained-after-collection",
    "actual-database-restore-quarantine",
}


def step(name):
    body = WORKFLOW.read_text().split(f"      - name: {name}\n", 1)[1]
    return body.split("\n      - name:", 1)[0]


def script(name):
    body = step(name).split("        run: |\n", 1)[1]
    lines = []
    for line in body.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line[10:] if line else "")
    return "\n".join(lines) + "\n"


class ProviderWorkflowTests(unittest.TestCase):
    def test_full_gate_is_separate_and_has_all_required_families(self):
        workflow = WORKFLOW.read_text()
        self.assertIn(f"PROVIDER_PREDECESSOR_SHA: '{PREDECESSOR}'", workflow)
        matrix = re.search(r"suite:\s*\[([^]]+)\]", workflow)
        self.assertIsNotNone(matrix, "required acceptance matrix is missing")
        suites = {name.strip().strip("\"'") for name in matrix.group(1).split(",")}
        self.assertTrue({"http", "sqs", "api", "artifacts", "provider", "provider-sqs",
                         "admission", "admission-sqs", "workflows-integrated", "workflows-sqs",
                         "workflows-tracing"} <= suites)
        gate = step("Check complete provider recovery with the pinned prior worker")
        self.assertIn("if: matrix.suite == 'provider'", gate)
        self.assertIn("--scenario all", gate)
        for option in ("--old-binary", "--old-runner", "--python-client", "--psql"):
            self.assertIn(option, gate)
        self.assertIn("startsWith(matrix.suite, 'provider')", step(
            "Build and install the Python client wheel for end-to-end acceptance"
        ))
        self.assertIn("test_provider_operations_fixtures.py", workflow)
        self.assertIn("unittest discover -s examples/provider-recovery/tests -v", workflow)
        tree = ast.parse((ROOT / "tools/check-provider-operations.py").read_text())
        families = {
            node.args[0].value for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "run" and node.args
            and isinstance(node.args[0], ast.Constant)
        }
        self.assertEqual(families, FAMILIES)

    def test_shell_blocks_are_valid_and_preserve_source_checks(self):
        for name in (
            "Build each qualified executable configuration once",
            "Verify executables and prepare isolated database access",
            "Check complete provider recovery with the pinned prior worker",
            "Check provider recovery and frozen report retries through ElasticMQ",
        ):
            subprocess.run(["bash", "-n"], input=script(name), text=True, check=True)
        build = script("Build each qualified executable configuration once")
        self.assertIn('git fetch --no-tags --depth=1 origin "$PROVIDER_PREDECESSOR_SHA"', build)
        self.assertIn('CARGO_TARGET_DIR="$GITHUB_WORKSPACE/target" cargo build', build)
        self.assertIn("--all-features --locked", build)
        verify = script("Verify executables and prepare isolated database access")
        self.assertIn('test "$(cat SOURCE_SHA)" = "$GITHUB_SHA"', verify)
        self.assertIn('test "$(cat predecessor/SOURCE_SHA)" = "$PROVIDER_PREDECESSOR_SHA"', verify)
        self.assertIn("set(expected) == actual", verify)

    def test_provider_sqs_has_its_own_service_and_current_binary_command(self):
        workflow = WORKFLOW.read_text()
        self.assertIn("contains(matrix.suite, 'sqs')", workflow)
        gate = step("Check provider recovery and frozen report retries through ElasticMQ")
        self.assertIn("if: matrix.suite == 'provider-sqs'", gate)
        for forbidden in ("--scenario all", "--old-binary", "--old-runner", "pg_dump", "pg_restore"):
            self.assertNotIn(forbidden, gate)
        with tempfile.TemporaryDirectory(prefix="ledgence-provider-sqs-ci-") as temporary:
            base = Path(temporary)
            commands = base / "commands"
            commands.mkdir()
            capture = base / "argv.json"
            python = commands / "python"
            python.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\nfrom pathlib import Path\n"
                "Path(os.environ['CAPTURE_ARGV']).write_text(json.dumps(sys.argv[1:]))\n"
            )
            python.chmod(0o755)
            run = base / "runner with spaces"
            run.mkdir()
            env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"],
                       RUNNER_TEMP=str(run), CAPTURE_ARGV=str(capture))
            subprocess.run(
                ["bash", "-eu", "-o", "pipefail", "-c", script(
                    "Check provider recovery and frozen report retries through ElasticMQ"
                )], cwd=ROOT, env=env, check=True, capture_output=True, text=True,
            )
            self.assertEqual(json.loads(capture.read_text()), [
                "tools/check-provider-operations.py", "--scenario", "sqs",
                "--endpoint", "http://127.0.0.1:9324",
                "--psql", str(run / "ledgence-psql"),
                "--binaries", str(run / "ledgence-acceptance-binaries/all-features"),
                "--python-client", str(run / "ledgence-client-e2e-env/bin/python"),
                "--evidence", str(run / "ledgence-provider-sqs-evidence"),
            ])

    def test_provider_sqs_parser_accepts_current_only_flags_before_database_setup(self):
        env = dict(os.environ)
        env.pop("LEDGENCE_POSTGRES_URL", None)
        result = subprocess.run([
            sys.executable, "-B", str(ROOT / "tools/check-provider-operations.py"),
            "--scenario", "sqs", "--endpoint", "http://127.0.0.1:9324",
            "--binaries", "/unused/current", "--python-client", sys.executable,
        ], cwd=ROOT, env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertIn("LEDGENCE_POSTGRES_URL must name", result.stderr)
        self.assertNotIn("full qualification requires", result.stderr)

    def test_provider_sqs_selects_two_families_without_repeating_the_integrated_suite(self):
        source = ast.parse((ROOT / "tools/provider_operations/sqs.py").read_text())
        labels = next(
            ast.literal_eval(node.value) for node in source.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "SCENARIOS" for target in node.targets)
        )
        self.assertEqual(labels, (
            "sqs-pending-probe-terminal-wake",
            "sqs-lost-probe-report-acknowledgement",
        ))
        tree = ast.parse((ROOT / "tools/check-provider-operations.py").read_text())
        selection = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.If)
            and ast.unparse(node.test) == "args.scenario == 'sqs'"
            and any(isinstance(child, ast.For) for child in node.body)
        )
        loop = next(node for node in selection.body if isinstance(node, ast.For))
        self.assertEqual(ast.unparse(loop.iter), "zip(SCENARIOS, (False, True), strict=True)")
        self.assertEqual(len(selection.orelse), 1)
        self.assertEqual(
            selection.orelse[0].value.args[0].value,
            "pending-capacity-auth-public-surfaces",
        )
        remaining = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.If) and ast.unparse(node.test) == "args.scenario == 'all'"
        )
        names = {
            node.args[0].value for node in ast.walk(remaining)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "run" and isinstance(node.args[0], ast.Constant)
        }
        self.assertEqual(names, FAMILIES - {"pending-capacity-auth-public-surfaces"})

    def test_build_handoff_preserves_current_bytes_and_exact_old_helper(self):
        with tempfile.TemporaryDirectory(prefix="ledgence-provider-ci-") as temporary:
            base = Path(temporary)
            origin, checkout, run = base / "origin", base / "checkout", base / "run"
            origin.mkdir()
            run.mkdir()

            def git(*args, cwd=origin):
                return subprocess.check_output(
                    ["git", *args], cwd=cwd, stderr=subprocess.PIPE, text=True
                ).strip()

            git("init", "-q")
            helper = origin / "sdk/python/ledgence/worker"
            helper.mkdir(parents=True)
            (helper / "bootstrap.py").write_text("# actual prior bootstrap\n")
            (helper / "workflow.py").write_text("# actual prior workflow helper\n")
            git("add", ".")
            git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                "commit", "-qm", "prior")
            prior = git("rev-parse", "HEAD")
            (helper / "bootstrap.py").write_text("# changed current bootstrap\n")
            (helper / "providers.py").write_text("# current-only provider helper\n")
            git("add", ".")
            git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                "commit", "-qm", "current")
            current = git("rev-parse", "HEAD")
            git("clone", "-q", str(origin), str(checkout), cwd=base)
            commands = base / "commands"
            commands.mkdir()
            cargo = commands / "cargo"
            cargo.write_text("""#!/usr/bin/env python3
import os, sys
from pathlib import Path
assert '--locked' in sys.argv
target = Path(os.environ.get('CARGO_TARGET_DIR', str(Path.cwd() / 'target'))) / 'debug'
is_old = Path.cwd().name == 'ledgence-predecessor-source'
label = 'prior' if is_old else ('current-all' if '--all-features' in sys.argv else 'current-default')
binary = target / ('examples/otlp-capture' if '--example' in sys.argv else 'ledgence')
binary.parent.mkdir(parents=True, exist_ok=True)
binary.write_text(label)
binary.chmod(0o755)
""")
            cargo.chmod(0o755)
            # The workflow runs on Ubuntu, but its regression also runs on macOS,
            # where the GNU checksum executable need not be installed.
            checksum = commands / "sha256sum"
            checksum.write_text("""#!/usr/bin/env python3
import hashlib, sys
from pathlib import Path
assert sys.argv[1:] == ['--check', 'SHA256SUMS']
for line in Path('SHA256SUMS').read_text().splitlines():
    digest, name = line.split('  ', 1)
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest
""")
            checksum.chmod(0o755)
            env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"],
                       RUNNER_TEMP=str(run), GITHUB_WORKSPACE=str(checkout),
                       GITHUB_SHA=current, GITHUB_RUN_ATTEMPT="1",
                       GITHUB_OUTPUT=str(base / "outputs"), PROVIDER_PREDECESSOR_SHA=prior,
                       LEDGENCE_POSTGRES_CONTAINER="unused", LEDGENCE_PYTHON="unused",
                       GITHUB_ENV=str(base / "environment"))
            subprocess.run(["bash", "-eu", "-o", "pipefail", "-c", script(
                "Build each qualified executable configuration once"
            )], cwd=checkout, env=env, check=True, capture_output=True, text=True)
            bundle = run / "ledgence-acceptance-binaries"
            self.assertEqual((bundle / "default/ledgence").read_text(), "current-default")
            self.assertEqual((bundle / "all-features/ledgence").read_text(), "current-all")
            self.assertEqual((bundle / "predecessor/ledgence").read_text(), "prior")
            old = bundle / "predecessor/runtime/ledgence/worker"
            self.assertEqual((old / "bootstrap.py").read_text(), "# actual prior bootstrap\n")
            self.assertFalse((old / "providers.py").exists())
            self.assertEqual((bundle / "SOURCE_SHA").read_text().strip(), current)
            self.assertEqual((bundle / "predecessor/SOURCE_SHA").read_text().strip(), prior)
            for line in (bundle / "SHA256SUMS").read_text().splitlines():
                digest, name = line.split("  ", 1)
                self.assertEqual(hashlib.sha256((bundle / name).read_bytes()).hexdigest(), digest)
            download = run / "ledgence-acceptance-download"
            download.mkdir()
            archive = download / "ledgence-acceptance-binaries.tar.gz"
            shutil.copyfile(run / archive.name, archive)
            consumer = script("Verify executables and prepare isolated database access")
            subprocess.run(["bash", "-eu", "-o", "pipefail", "-c", consumer],
                           cwd=checkout, env=env, check=True, capture_output=True, text=True)
            # An unlisted helper must fail even when every listed checksum matches.
            (old / "unlisted.py").write_text("# not covered by source inventory\n")
            with tarfile.open(archive, "w:gz") as package:
                for path in bundle.iterdir():
                    package.add(path, arcname=path.name)
            result = subprocess.run(["bash", "-eu", "-o", "pipefail", "-c", consumer],
                                    cwd=checkout, env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
