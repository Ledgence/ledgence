#!/usr/bin/env python3
"""Black-box checks for install.sh using real archives and an offline release endpoint."""
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "install.sh"
BASE_URL = "https://releases.example.test/releases"
LINUX = "x86_64-unknown-linux-gnu"
MACOS = "aarch64-apple-darwin"


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ledgence-installer-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        self.home = self.directory / "home"
        self.home.mkdir()
        self.prefix = self.home / ".local"
        self.fixtures = self.directory / "releases"
        self.fixtures.mkdir()
        self.commands = self.directory / "commands"
        self.commands.mkdir()
        # A restricted PATH proves the installer has no Python/Rust/Docker
        # requirement. Only the curl fixture itself uses an absolute Python path.
        for name in ("tar", "awk", "sed", "sort", "find", "cmp", "mktemp",
                     "mkdir", "mv", "rm", "rmdir", "ln", "readlink", "chmod"):
            source = shutil.which(name)
            self.assertIsNotNone(source, name)
            (self.commands / name).symlink_to(source)
        for name in ("sha256sum", "shasum"):
            source = shutil.which(name)
            if source:
                (self.commands / name).symlink_to(source)
        self.write_command("uname", '#!/bin/sh\ncase "$1" in -s) printf "%s\\n" "$TEST_OS";; -m) printf "%s\\n" "$TEST_ARCH";; esac\n')
        self.write_command("getconf", '#!/bin/sh\n[ "$TEST_GLIBC" = yes ] || exit 1\nprintf "glibc 2.39\\n"\n')
        self.write_command("curl", f"""#!{sys.executable}
import os
from pathlib import Path
import shutil
import sys
args = sys.argv[1:]
assert "--proto" in args and args[args.index("--proto") + 1] == "=https"
assert "--proto-redir" in args and args[args.index("--proto-redir") + 1] == "=https"
url = args[-1]
base = os.environ["TEST_BASE_URL"]
if "--head" in args:
    print(os.environ.get("TEST_LATEST_URL", base + "/tag/v0.3.1"), end="")
    raise SystemExit(0)
prefix = base + "/download/v"
if not url.startswith(prefix):
    raise SystemExit(22)
if os.environ.get("TEST_DOWNLOAD_FAIL") in (url.rsplit("/", 1)[-1], "all"):
    raise SystemExit(22)
replacement = os.environ.get("TEST_REPLACE_LAUNCHER")
if replacement and url.endswith(".tar.gz"):
    launcher = Path(replacement)
    launcher.parent.mkdir(parents=True, exist_ok=True)
    if launcher.is_symlink():
        launcher.unlink()
    launcher.write_text("concurrent user executable")
source = Path(os.environ["TEST_RELEASES"]) / url[len(prefix):]
destination = Path(args[args.index("--output") + 1])
if not source.is_file():
    raise SystemExit(22)
shutil.copyfile(source, destination)
""")
        self.env = {
            **os.environ,
            "HOME": str(self.home),
            "PATH": str(self.commands),
            "TEST_RELEASES": str(self.fixtures),
            "TEST_BASE_URL": BASE_URL,
            "TEST_OS": "Linux",
            "TEST_ARCH": "x86_64",
            "TEST_GLIBC": "yes",
        }
        for key in ("TEST_DOWNLOAD_FAIL", "TEST_LATEST_URL", "TEST_REPLACE_LAUNCHER"):
            self.env.pop(key, None)
        self.make_release()

    def write_command(self, name, content):
        path = self.commands / name
        path.write_text(content)
        path.chmod(0o755)

    def make_release(self, version="0.3.1", target=LINUX, *, mutate=None,
                     executable_version=None, executable_failure=False, member=None):
        label = f"ledgence-{version}-{target}"
        executable = ("#!/bin/sh\nexit 127\n" if executable_failure else
                      f"#!/bin/sh\nprintf 'ledgence {executable_version or version}\\n'\n").encode()
        files = {
            "bin/ledgence": executable,
            "LICENSE": b"MIT fixture\n",
            "legal/THIRD-PARTY.txt": b"fixture notices\n",
            "runtime/ledgence/worker/bootstrap.py": b"# fixture helper\n",
            "console/index.html": b"<html>fixture</html>\n",
            "provenance.json": b'{"format": 1}\n',
        }
        files["SHA256SUMS"] = "".join(
            f"{hashlib.sha256(content).hexdigest()}  {name}\n"
            for name, content in sorted(files.items())
        ).encode()
        if mutate:
            mutate(files)
        directory = self.fixtures / version
        directory.mkdir(exist_ok=True)
        archive = directory / f"{label}.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            for name, content in files.items():
                entry = tarfile.TarInfo(f"{label}/{name}")
                entry.size = len(content)
                entry.mode = 0o755 if name == "bin/ledgence" else 0o644
                output.addfile(entry, io.BytesIO(content))
            if member:
                output.addfile(member(label))
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
        (directory / "SHA256SUMS").write_text(f"{checksum}  {archive.name}\n")
        return archive

    def run_installer(self, *args, expected=0):
        result = subprocess.run(
            ["/bin/sh", str(INSTALLER), "--base-url", BASE_URL, *args],
            env=self.env, capture_output=True, text=True, timeout=20,
        )
        if expected == 0:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def launcher(self):
        return self.prefix / "bin/ledgence"

    def assert_installed(self, version="0.3.1", target=LINUX):
        launcher = self.launcher()
        self.assertTrue(launcher.is_symlink())
        bundle = self.prefix / "share/ledgence/versions" / f"{version}-{target}"
        self.assertEqual(launcher.resolve(), bundle / "bin/ledgence")
        for path in ("LICENSE", "legal/THIRD-PARTY.txt",
                     "runtime/ledgence/worker/bootstrap.py", "console/index.html"):
            self.assertTrue((bundle / path).is_file(), path)
        self.assertEqual(subprocess.check_output([str(launcher), "--version"], text=True),
                         f"ledgence {version}\n")
        self.assertFalse((self.prefix / "share/ledgence/.install-lock").exists())
        return bundle

    def test_latest_stable_complete_bundle(self):
        result = self.run_installer()
        self.assert_installed()
        self.assertIn("export PATH=", result.stdout)
        self.assertFalse((self.home / ".profile").exists())

    def test_explicit_version_and_repeat_install(self):
        self.run_installer("--version", "v0.3.1", "--no-modify-path")
        bundle = self.assert_installed()
        inode = bundle.stat().st_ino
        self.run_installer("--version", "0.3.1")
        self.assertEqual(self.assert_installed().stat().st_ino, inode)

    def test_upgrade_preserves_old_bundle(self):
        self.run_installer()
        old = self.assert_installed()
        self.make_release("0.3.2")
        self.run_installer("--version", "0.3.2")
        self.assert_installed("0.3.2")
        self.assertTrue(old.is_dir())

    def test_failed_upgrade_preserves_active_installation(self):
        self.run_installer()
        old = self.assert_installed()
        self.make_release("0.3.2")
        self.env["TEST_DOWNLOAD_FAIL"] = f"ledgence-0.3.2-{LINUX}.tar.gz"
        self.run_installer("--version", "0.3.2", expected=1)
        self.assertEqual(self.launcher().resolve(), old / "bin/ledgence")
        self.assertFalse((self.prefix / "share/ledgence/.install-lock").exists())

    def test_prefix_spaces_and_quotes_produces_valid_path_command(self):
        self.prefix = self.directory / "user's custom prefix"
        result = self.run_installer("--prefix", str(self.prefix))
        self.assert_installed()
        export = next(line.strip() for line in result.stdout.splitlines() if "export PATH=" in line)
        actual = subprocess.check_output(
            ["/bin/sh", "-c", export + '; printf "%s" "$PATH"'], env={"PATH": "/usr/bin"}, text=True)
        self.assertEqual(actual, f"{self.prefix}/bin:/usr/bin")

    def test_path_already_present(self):
        self.env["PATH"] = f"{self.prefix}/bin:{self.commands}"
        result = self.run_installer()
        self.assertNotIn("export PATH=", result.stdout)
        self.assertIn("Run: ledgence --help", result.stdout)

    def test_earlier_cli_on_path_requires_prepend_even_if_install_bin_present(self):
        self.write_command("ledgence", "#!/bin/sh\nprintf 'other cli\\n'\n")
        self.env["PATH"] = f"{self.commands}:{self.prefix}/bin"
        result = self.run_installer()
        self.assert_installed()
        self.assertIn("takes precedence on PATH", result.stdout)
        self.assertIn("export PATH=", result.stdout)

    def test_missing_runtime_resources_rejected_with_consistent_checksums(self):
        for missing in ("console/index.html", "runtime/ledgence/worker/bootstrap.py"):
            def mutate(files):
                del files[missing]
                files[missing.rsplit("/", 1)[0] + "/placeholder.txt"] = b"nonfunctional sibling"
                files["SHA256SUMS"] = "".join(
                    f"{hashlib.sha256(content).hexdigest()}  {name}\n"
                    for name, content in sorted(files.items()) if name != "SHA256SUMS"
                ).encode()
            with self.subTest(missing=missing):
                self.make_release(mutate=mutate)
                self.run_installer(expected=1)
                self.assertFalse(self.launcher().exists())

    def test_macos_arm64_and_shasum_fallback(self):
        self.env.update(TEST_OS="Darwin", TEST_ARCH="arm64")
        checksum = self.commands / "sha256sum"
        if checksum.is_symlink():
            checksum.unlink()
        if not (self.commands / "shasum").exists():
            self.skipTest("host lacks shasum")
        self.make_release(target=MACOS)
        self.run_installer()
        self.assert_installed(target=MACOS)

    def test_unsupported_host_is_actionable(self):
        self.env["TEST_ARCH"] = "aarch64"
        result = self.run_installer(expected=1)
        self.assertIn("unsupported host", result.stderr)
        self.assertFalse(self.prefix.exists())

    def test_musl_is_rejected(self):
        self.env["TEST_GLIBC"] = "no"
        result = self.run_installer(expected=1)
        self.assertIn("glibc", result.stderr)
        self.assertFalse(self.prefix.exists())

    def test_incompatible_binary_preserves_installation(self):
        self.run_installer()
        old = self.assert_installed()
        self.make_release("0.3.2", executable_failure=True)
        result = self.run_installer("--version", "0.3.2", expected=1)
        self.assertIn("cannot run on this host", result.stderr)
        self.assertEqual(self.launcher().resolve(), old / "bin/ledgence")

    def test_wrong_binary_version_rejected(self):
        self.make_release(executable_version="0.0.0")
        self.run_installer(expected=1)
        self.assertFalse(self.launcher().exists())

    def test_corrupt_archive_rejected(self):
        archive = self.make_release()
        with archive.open("ab") as stream:
            stream.write(b"corrupted")
        result = self.run_installer(expected=1)
        self.assertIn("archive checksum mismatch", result.stderr)

    def test_duplicate_external_checksum_rejected(self):
        path = self.fixtures / "0.3.1/SHA256SUMS"
        path.write_text(path.read_text() * 2)
        self.run_installer(expected=1)

    def test_missing_external_checksum_rejected(self):
        path = self.fixtures / "0.3.1/SHA256SUMS"
        path.write_text("0" * 64 + "  unrelated.tar.gz\n")
        self.run_installer(expected=1)

    def test_internal_checksum_mismatch_rejected(self):
        self.make_release(mutate=lambda files: files.update({"console/index.html": b"changed"}))
        self.run_installer(expected=1)

    def test_unlisted_file_rejected(self):
        self.make_release(mutate=lambda files: files.update({"console/extra.html": b"extra"}))
        self.run_installer(expected=1)

    def test_missing_resource_rejected(self):
        self.make_release(mutate=lambda files: files.pop("console/index.html"))
        self.run_installer(expected=1)

    def test_archive_link_rejected(self):
        def symlink(label):
            entry = tarfile.TarInfo(label + "/link")
            entry.type = tarfile.SYMTYPE
            entry.linkname = "/tmp"
            return entry
        self.make_release(member=symlink)
        self.run_installer(expected=1)

    def test_archive_path_traversal_rejected(self):
        self.make_release(member=lambda label: tarfile.TarInfo(label + "/../escape"))
        self.run_installer(expected=1)
        self.assertFalse((self.prefix / "share/ledgence/escape").exists())

    def test_modified_existing_version_is_preserved(self):
        self.run_installer()
        bundle = self.assert_installed()
        resource = bundle / "console/index.html"
        resource.write_text("local changes")
        result = self.run_installer(expected=1)
        self.assertIn("preserved without replacement", result.stderr)
        self.assertEqual(resource.read_text(), "local changes")

    def test_republished_version_is_not_overwritten(self):
        self.run_installer()
        bundle = self.assert_installed()
        original = (bundle / "bin/ledgence").read_bytes()
        self.make_release(executable_version="0.0.0")
        self.run_installer(expected=1)
        self.assertEqual((bundle / "bin/ledgence").read_bytes(), original)

    def test_concurrent_user_launcher_is_preserved_during_first_install(self):
        self.env["TEST_REPLACE_LAUNCHER"] = str(self.launcher())
        self.run_installer(expected=1)
        self.assertEqual(self.launcher().read_text(), "concurrent user executable")

    def test_concurrent_user_launcher_is_preserved_during_upgrade(self):
        self.run_installer()
        self.make_release("0.3.2")
        self.env["TEST_REPLACE_LAUNCHER"] = str(self.launcher())
        self.run_installer("--version", "0.3.2", expected=1)
        self.assertEqual(self.launcher().read_text(), "concurrent user executable")

    def test_changed_valid_bundle_cannot_replace_same_version(self):
        self.run_installer()
        bundle = self.assert_installed()
        original = (bundle / "console/index.html").read_bytes()
        def mutate(files):
            files["console/index.html"] = b"changed release bytes"
            files["SHA256SUMS"] = "".join(
                f"{hashlib.sha256(content).hexdigest()}  {name}\n"
                for name, content in sorted(files.items()) if name != "SHA256SUMS"
            ).encode()
        self.make_release(mutate=mutate)
        result = self.run_installer(expected=1)
        self.assertIn("existing version differs", result.stderr)
        self.assertEqual((bundle / "console/index.html").read_bytes(), original)

    def test_foreign_executable_preserved(self):
        self.launcher().parent.mkdir(parents=True)
        self.launcher().write_text("my executable")
        self.run_installer(expected=1)
        self.assertEqual(self.launcher().read_text(), "my executable")

    def test_foreign_symlink_preserved(self):
        self.launcher().parent.mkdir(parents=True)
        self.launcher().symlink_to("/some/other/ledgence")
        self.run_installer(expected=1)
        self.assertEqual(os.readlink(self.launcher()), "/some/other/ledgence")

    def test_concurrent_install_is_rejected(self):
        lock = self.prefix / "share/ledgence/.install-lock"
        lock.mkdir(parents=True)
        result = self.run_installer(expected=1)
        self.assertIn("another install", result.stderr)
        self.assertTrue(lock.exists())

    def test_invalid_options(self):
        for args in (("--unknown",), ("--version",), ("--version", "../bad"),
                     ("--version", "1.0.0-rc.1"), ("--version", "01.2.3"),
                     ("--prefix", "relative"), ("--prefix", "/tmp/colon:directory"),
                     ("--prefix", "/tmp/line\nbreak"), ("--prefix", "/tmp/line\rbreak"),
                     ("--base-url", "http://insecure.example/releases")):
            with self.subTest(args=args):
                self.run_installer(*args, expected=1)

    def test_reinstall_with_unlisted_or_symlinked_local_file_is_preserved(self):
        self.run_installer()
        bundle = self.assert_installed()
        extra = bundle / "extra"
        extra.symlink_to("/not-present")
        self.run_installer(expected=1)
        self.assertTrue(extra.is_symlink())

    def test_malformed_internal_manifest_path_rejected(self):
        self.make_release(mutate=lambda files: files.update(
            {"SHA256SUMS": b"0" * 64 + b"  ../outside\n"}))
        self.run_installer(expected=1)

    def test_custom_prefix_symlink_already_on_path(self):
        self.prefix.mkdir()
        alias = self.directory / "prefix-alias"
        alias.symlink_to(self.prefix, target_is_directory=True)
        self.env["PATH"] = f"{alias}/bin:{self.commands}"
        result = self.run_installer("--prefix", str(alias))
        self.assert_installed()
        self.assertNotIn("export PATH=", result.stdout)

    def test_latest_redirect_must_match_release_endpoint(self):
        self.env["TEST_LATEST_URL"] = "https://other.example/tag/v0.3.1"
        self.run_installer(expected=1)

    def test_help_does_not_require_platform_dependencies(self):
        self.env["PATH"] = "/usr/bin:/bin"
        result = subprocess.run(["/bin/sh", str(INSTALLER), "--help"], env=self.env,
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--version", result.stdout)


if __name__ == "__main__":
    unittest.main()
