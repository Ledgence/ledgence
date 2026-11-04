"""Exercise the candidate's checksum-bound upgrade routing without services (MIT)."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/candidate.yml"
STEP = "Rehearse migrations and restore with published predecessor bundles"
PUBLISHED = {
    "0.6.1": "7fc70c2e520d4bfb5836136e8811d3cec61399545d3c26eebeca2a29e3ad38be",
    "0.7.0": "1795cc0ed9acc42613ab8e8c63d845a9ce5235a5d06cff4a5289e287d06143c3",
    "0.8.0": "343942fcb3e142abc3636e1458203e697e0c27b2a4a5300bd6603412270bce5e",
}


def upgrade_script():
    body = WORKFLOW.read_text().split(f"      - name: {STEP}\n", 1)[1]
    body = body.split("\n      - name:", 1)[0].split("        run: |\n", 1)[1]
    return "\n".join(line[10:] for line in body.splitlines()) + "\n"


def archive(path, label):
    """Real tar extraction exercises the packaged candidate/helper destinations."""
    with tarfile.open(path, "w:gz") as package:
        for name in ("bin/ledgence", "console/index.html", "runtime/ledgence/worker/bootstrap.py"):
            content = f"{label}:{name}".encode()
            entry = tarfile.TarInfo(f"bundle/{name}")
            entry.size = len(content)
            package.addfile(entry, io.BytesIO(content))
    return hashlib.sha256(path.read_bytes()).hexdigest()


class CandidateWorkflowTests(unittest.TestCase):
    def test_all_published_linux_baselines_are_pinned(self):
        script = upgrade_script()
        subprocess.run(["bash", "-n"], input=script, text=True, check=True)
        for version, checksum in PUBLISHED.items():
            gate = "check-resource-upgrade.py" if version == "0.8.0" else "check-approval-upgrade.py"
            self.assertIn(f"{version} {checksum} {gate}\n", script)
        self.assertIn('baseline="ledgence-$version-x86_64-unknown-linux-gnu"', script)
        self.assertLess(script.index("sha256sum --check"), script.index('tar -xzf "$rehearsal/$baseline.tar.gz"'))
        workflow = WORKFLOW.read_text()
        self.assertIn("path: ${{ runner.temp }}/approval-upgrade/evidence/", workflow)

    def exercise(self, *, corrupt=None, failing=None, omitted=None, incomplete=False, wrong_hash=None):
        with tempfile.TemporaryDirectory(prefix="ledgence-candidate-upgrade-") as temporary:
            base = Path(temporary)
            run, fixtures, commands = base / "run", base / "fixtures", base / "commands"
            for directory in (run / "ledgence-candidate", fixtures, commands):
                directory.mkdir(parents=True)
            # Match actions/setup-python instead of invoking the host OS interpreter.
            (commands / "python3").symlink_to(sys.executable)
            archive(run / "ledgence-candidate/candidate.tar.gz", "current")
            script = upgrade_script()
            for version, published in PUBLISHED.items():
                name = f"ledgence-{version}-x86_64-unknown-linux-gnu.tar.gz"
                digest = archive(fixtures / name, version)
                script = script.replace(published, digest)
                if corrupt == version:
                    with (fixtures / name).open("ab") as stream:
                        stream.write(b"altered after digest")

            def executable(name, source):
                path = commands / name
                path.write_text(f"#!{sys.executable}\n" + source)
                path.chmod(0o755)

            executable("curl", """import os, shutil, sys
from pathlib import Path
args = sys.argv[1:]
url, = [arg for arg in args if arg.startswith('https://')]
name = url.rsplit('/', 1)[1]
version = name.split('-')[1]
assert url == f'https://github.com/Ledgence/ledgence/releases/download/v{version}/{name}'
assert '--fail' in args and '--proto' in args and '=https' in args
shutil.copyfile(Path(os.environ['FIXTURES']) / name, args[args.index('--output') + 1])
""")
            # Ubuntu has sha256sum; this equivalent checker keeps the fixture
            # runnable on macOS while still comparing actual extracted archives.
            executable("sha256sum", """import hashlib, sys
from pathlib import Path
assert sys.argv[1:] == ['--check']
digest, name = sys.stdin.read().strip().split('  ', 1)
sys.exit(0 if hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest else 1)
""")
            executable("python", """import hashlib, json, os, sys, tomllib
from pathlib import Path
options = dict(zip(sys.argv[2::2], sys.argv[3::2]))
previous = Path(options['--previous-bundle'])
version = previous.name.removeprefix('previous-')
# Docker exec -i based clients drain inherited standard input in the real Linux job.
sys.stdin.read()
expected = 'check-resource-upgrade.py' if version == '0.8.0' else 'check-approval-upgrade.py'
assert sys.argv[1] == f'tools/{expected}'
assert (previous / 'bin/ledgence').read_text() == f'{version}:bin/ledgence'
assert (previous / 'runtime/ledgence/worker/bootstrap.py').read_text() == f'{version}:runtime/ledgence/worker/bootstrap.py'
assert (Path(options['--binaries']) / 'ledgence').read_text() == 'current:bin/ledgence'
assert (Path(options['--console-dist']) / 'index.html').read_text() == 'current:console/index.html'
assert Path(options['--candidate-runner']).read_text() == 'current:runtime/ledgence/worker/bootstrap.py'
assert Path(options['--evidence']).name == version
psql = Path(options['--psql'])
assert psql.name == 'psql'
for tool in ('psql','pg_dump','pg_restore'):
    wrapper = psql.with_name(tool)
    assert os.access(wrapper,os.X_OK) and 'docker exec -i "$LEDGENCE_POSTGRES_CONTAINER"' in wrapper.read_text()

with Path(os.environ['CALLS']).open('a') as log:
    log.write(json.dumps(options) + '\\n')
if version == os.environ.get('FAIL_VERSION'):
    sys.exit(9)
if version != os.environ.get('OMIT_VERSION'):
    evidence = Path(options['--evidence'])
    evidence.mkdir(parents=True)
    old = hashlib.sha256((previous / 'bin/ledgence').read_bytes()).hexdigest()
    new = hashlib.sha256((Path(options['--binaries']) / 'ledgence').read_bytes()).hexdigest()
    if version == os.environ.get('WRONG_HASH'):
        new = '0' * 64
    release = 'ledgence ' + tomllib.loads(Path('Cargo.toml').read_text())['workspace']['package']['version']
    if version == '0.8.0':
        data = dict(passed=True, database_cleanup=True, process_cleanup=True, credential_scan_clean=not os.environ.get('INCOMPLETE'),
                    provenance=dict(versions=['ledgence 0.8.0', release], new_binary_sha256=new, old_verified_files={'bin/ledgence':old}))
        (evidence / 'summary.json').write_text(json.dumps(data))
    else:
        data = dict(previous_version='ledgence ' + version, candidate_version=release, executable_sha256=dict(previous=old,candidate=new))
        (evidence / 'results.json').write_text(json.dumps(data))
""")
            calls = base / "calls.jsonl"
            env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"],
                       RUNNER_TEMP=str(run), FIXTURES=str(fixtures), CALLS=str(calls),
                       FAIL_VERSION=failing or "", OMIT_VERSION=omitted or "", INCOMPLETE="1" if incomplete else "",
                       WRONG_HASH=wrong_hash or "")
            result = subprocess.run(["bash", "-eu", "-o", "pipefail", "-c", script],
                                    cwd=ROOT, env=env, capture_output=True, text=True)
            recorded = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
            return result, recorded

    def test_all_upgrades_use_same_candidate_and_distinct_evidence(self):
        result, calls = self.exercise()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([Path(call["--evidence"]).name for call in calls], list(PUBLISHED))
        for key in ("--binaries", "--console-dist", "--candidate-runner"):
            self.assertEqual(len({call[key] for call in calls}), 1)
        self.assertEqual(len({call["--previous-bundle"] for call in calls}), len(PUBLISHED))

    def test_missing_successful_or_incomplete_rehearsals_block_qualification(self):
        for version in PUBLISHED:
            with self.subTest(version=version):
                result, calls = self.exercise(omitted=version)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(calls), len(PUBLISHED))
        result, _ = self.exercise(incomplete=True)
        self.assertNotEqual(result.returncode, 0)

    def test_receipt_from_another_candidate_blocks_qualification(self):
        for version in PUBLISHED:
            with self.subTest(version=version):
                result, calls = self.exercise(wrong_hash=version)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(calls), len(PUBLISHED))

    def test_corrupt_last_stable_archive_never_executes_upgrade(self):
        result, calls = self.exercise(corrupt="0.8.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([Path(call["--evidence"]).name for call in calls], ["0.6.1", "0.7.0"])

    def test_failed_historical_upgrade_stops_candidate_qualification(self):
        result, calls = self.exercise(failing="0.6.1")
        self.assertEqual(result.returncode, 9)
        self.assertEqual([Path(call["--evidence"]).name for call in calls], ["0.6.1"])


if __name__ == "__main__":
    unittest.main()
