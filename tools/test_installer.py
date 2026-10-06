#!/usr/bin/env python3
"""Black-box checks for install.sh using real archives and an offline release endpoint."""
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
import shlex
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
INSTALLER_URL = "https://releases.example.test/install.sh"


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
        # GNU tar invokes gzip externally for .tar.gz archives; keep that real
        # utility on PATH as well (macOS bsdtar handles gzip internally).
        for name in ("sh", "tar", "gzip", "awk", "sed", "sort", "find", "cmp", "mktemp",
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
url = args[-1]
if url == os.environ.get("TEST_INSTALLER_URL"):
    sys.stdout.buffer.write(Path(os.environ["TEST_INSTALLER_CONTENT"]).read_bytes())
    raise SystemExit(int(os.environ.get("TEST_INSTALLER_CURL_EXIT", "0")))
assert "--proto" in args and args[args.index("--proto") + 1] == "=https"
assert "--proto-redir" in args and args[args.index("--proto-redir") + 1] == "=https"
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
            "SHELL": "/bin/sh",
            "PATH": str(self.commands),
            "TEST_RELEASES": str(self.fixtures),
            "TEST_BASE_URL": BASE_URL,
            "TEST_OS": "Linux",
            "TEST_ARCH": "x86_64",
            "TEST_GLIBC": "yes",
        }
        for key in ("TEST_DOWNLOAD_FAIL", "TEST_LATEST_URL", "TEST_REPLACE_LAUNCHER",
                    "TEST_INSTALLER_URL", "TEST_INSTALLER_CONTENT", "TEST_INSTALLER_CURL_EXIT",
                    "ZDOTDIR", "ENV", "BASH_ENV", "CDPATH"):
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

    def environment_file(self):
        return self.prefix / "share/ledgence/env"

    def shell_output(self, code, *, shell="/bin/sh", startup=False, env=None):
        options = ["-c"]
        if startup:
            options = ["--noprofile", "-ic"] if Path(shell).name == "bash" else ["-d", "-ic"]
        result = subprocess.run([shell, *options, code], env=env or self.env,
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def piped_install(self, shell, *, content=None, curl_exit=0):
        # Serve the real installer through the same curl | sh shape as the docs,
        # changing only its release endpoint to the offline fixtures.
        script = self.directory / "served-install.sh"
        script.write_text((content if content is not None else INSTALLER.read_text()).replace(
            "https://github.com/Ledgence/ledgence/releases", BASE_URL))
        env = {**self.env, "SHELL": shell, "TEST_INSTALLER_URL": INSTALLER_URL,
               "TEST_INSTALLER_CONTENT": str(script), "TEST_INSTALLER_CURL_EXIT": str(curl_exit)}
        command = ('(set -o pipefail; curl -fsSL "$TEST_INSTALLER_URL" | sh) '
                   '&& . "$HOME/.local/share/ledgence/env" && ledgence --version')
        return subprocess.run([shell, "-c", command], env=env,
                              capture_output=True, text=True, timeout=20)

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
        self.assertTrue(self.environment_file().is_file())
        self.assertIn(str(self.environment_file()), result.stdout)
        self.assertTrue((self.home / ".profile").is_file())
        self.assertEqual(self.shell_output('. "$HOME/.profile"; ledgence --version'),
                         "ledgence 0.3.1\n")

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
        environment = self.environment_file().read_bytes()
        profile = (self.home / ".profile").read_bytes()
        self.make_release("0.3.2")
        self.env["TEST_DOWNLOAD_FAIL"] = f"ledgence-0.3.2-{LINUX}.tar.gz"
        self.run_installer("--version", "0.3.2", expected=1)
        self.assertEqual(self.launcher().resolve(), old / "bin/ledgence")
        self.assertFalse((self.prefix / "share/ledgence/.install-lock").exists())
        self.assertEqual(self.environment_file().read_bytes(), environment)
        self.assertEqual((self.home / ".profile").read_bytes(), profile)

    def test_prefix_shell_characters_produce_valid_environment_and_profiles(self):
        self.prefix = self.directory / "user's custom $HOME `uname` \\ prefix"
        result = self.run_installer("--prefix", str(self.prefix))
        self.assert_installed()
        source = next(line.strip() for line in result.stdout.splitlines()
                      if line.strip().startswith(". "))
        actual = self.shell_output(source + '; printf "%s" "$PATH"')
        self.assertEqual(actual, f"{self.prefix}/bin:{self.commands}")
        self.assertEqual(self.shell_output('. "$HOME/.profile"; ledgence --version'),
                         "ledgence 0.3.1\n")

    def test_path_already_present(self):
        self.env["PATH"] = f"{self.prefix}/bin:{self.commands}"
        result = self.run_installer()
        self.assertIn("Run: ledgence --help", result.stdout)
        self.assertEqual(self.shell_output('. "$HOME/.local/share/ledgence/env"; printf "%s" "$PATH"'),
                         self.env["PATH"])

    def test_environment_handles_empty_and_unset_path(self):
        self.run_installer()
        for setup in ("PATH=", "unset PATH"):
            with self.subTest(setup=setup):
                self.assertEqual(self.shell_output(setup + '; . "$HOME/.local/share/ledgence/env"; '
                                                   'printf "%s" "$PATH"'), str(self.prefix / "bin"))

    def test_environment_removes_only_exact_bin_components(self):
        self.run_installer()
        bin_dir = str(self.prefix / "bin")
        cases = (
            ([str(self.commands), bin_dir], [bin_dir, str(self.commands)]),
            ([str(self.commands), bin_dir, "/other/bin"], [bin_dir, str(self.commands), "/other/bin"]),
            (["", bin_dir, "", str(self.commands), ""], [bin_dir, "", "", str(self.commands), ""]),
            ([bin_dir, bin_dir], [bin_dir]),
            ([bin_dir + "ary", bin_dir + "-suffix", bin_dir], [bin_dir, bin_dir + "ary", bin_dir + "-suffix"]),
        )
        for initial, expected in cases:
            with self.subTest(initial=initial):
                actual = self.shell_output('. "$HOME/.local/share/ledgence/env"; printf "%s" "$PATH"',
                                           env={**self.env, "PATH": ":".join(initial)})
                self.assertEqual(actual.split(":"), expected)

    def test_repeated_environment_after_other_path_hooks_stays_unique(self):
        self.run_installer()
        other_prefix = self.directory / "second installation"
        self.run_installer("--prefix", str(other_prefix))
        actual = self.shell_output('. "$HOME/.local/share/ledgence/env"; '
                                   'PATH="/other/bin:$PATH"; '
                                   '. "$SECOND_ENV"; '
                                   '. "$HOME/.local/share/ledgence/env"; '
                                   'printf "%s" "$PATH"',
                                   env={**self.env, "SECOND_ENV": str(other_prefix / "share/ledgence/env")})
        self.assertEqual(actual.split(":"), [str(self.prefix / "bin"), str(other_prefix / "bin"),
                                               "/other/bin", str(self.commands)])

    def test_earlier_cli_on_path_requires_prepend_even_if_install_bin_present(self):
        self.write_command("ledgence", "#!/bin/sh\nprintf 'other cli\\n'\n")
        self.env["PATH"] = f"{self.commands}:{self.prefix}/bin"
        result = self.run_installer()
        self.assert_installed()
        self.assertIn("takes precedence on PATH", result.stdout)
        actual = self.shell_output('. "$HOME/.local/share/ledgence/env"; '
                                   'printf "%s\\n" "$PATH"; '
                                   '. "$HOME/.local/share/ledgence/env"; '
                                   'ledgence --version; printf "%s" "$PATH"')
        lines = actual.splitlines()
        self.assertEqual(lines[0], lines[2])
        self.assertEqual(lines[1], "ledgence 0.3.1")
        self.assertEqual(lines[0].split(":"), [str(self.prefix / "bin"), str(self.commands)])

    def test_profile_guard_preserves_content_and_is_added_once(self):
        profile = self.home / ".profile"
        for original in (b"# user's existing settings\n", b"# user's existing settings"):
            with self.subTest(final_newline=original.endswith(b"\n")):
                profile.write_bytes(original)
                self.run_installer()
                configured = profile.read_bytes()
                self.assertTrue(configured.startswith(original + (b"" if original.endswith(b"\n") else b"\n")))
                appended = configured[len(original):].decode()
                statements = [line for line in appended.splitlines()
                              if line.strip() and not line.lstrip().startswith("#")]
                self.assertEqual(len(statements), 1, appended)
                self.assertIn(str(self.environment_file()), statements[0])
                self.run_installer()
                self.assertEqual(profile.read_bytes(), configured)
        self.environment_file().unlink()
        self.assertEqual(self.shell_output('. "$HOME/.profile"; printf alive'), "alive")

    def test_profile_symlink_and_target_are_preserved(self):
        target = self.directory / "shared profile"
        target.write_text("# managed dotfiles\n")
        profile = self.home / ".profile"
        profile.symlink_to(target)
        link_inode = profile.lstat().st_ino
        target_inode = target.stat().st_ino
        self.run_installer()
        self.assertEqual(profile.lstat().st_ino, link_inode)
        self.assertEqual(profile.readlink(), target)
        self.assertEqual(target.stat().st_ino, target_inode)
        self.assertTrue(target.read_text().startswith("# managed dotfiles\n"))
        self.assertEqual(self.shell_output('. "$HOME/.profile"; ledgence --version'),
                         "ledgence 0.3.1\n")

    def test_bash_configures_interactive_and_first_existing_login_profile(self):
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("host lacks bash")
        self.env["SHELL"] = bash
        names = (".bash_profile", ".bash_login", ".profile")
        for first in range(len(names)):
            with self.subTest(login_profile=names[first]):
                for name in (*names, ".bashrc"):
                    (self.home / name).unlink(missing_ok=True)
                original = {}
                for name in names[first:]:
                    original[name] = f"# existing {name}\n".encode()
                    (self.home / name).write_bytes(original[name])
                bashrc = self.home / ".bashrc"
                bashrc.write_text("export SHELL_SETUP_VALUE=kept")
                self.run_installer()
                self.assertTrue(bashrc.read_text().startswith("export SHELL_SETUP_VALUE=kept\n"))
                self.assertNotEqual((self.home / names[first]).read_bytes(), original[names[first]])
                for name in names[first + 1:]:
                    self.assertEqual((self.home / name).read_bytes(), original[name])
                for name in names[:first]:
                    self.assertFalse((self.home / name).exists())
                self.assertEqual(self.shell_output('printf "%s\\n" "$SHELL_SETUP_VALUE"; ledgence --version',
                                                   shell=bash, startup=True), "kept\nledgence 0.3.1\n")
                self.assertEqual(self.shell_output(f'. "$HOME/{names[first]}"; ledgence --version',
                                                   shell=bash), "ledgence 0.3.1\n")

    def test_bash_creates_profile_when_no_login_profile_exists(self):
        self.env["SHELL"] = "/bin/bash"
        self.run_installer()
        self.assertTrue((self.home / ".bashrc").is_file())
        self.assertTrue((self.home / ".profile").is_file())
        self.assertFalse((self.home / ".bash_profile").exists())
        self.assertFalse((self.home / ".bash_login").exists())

    def test_zsh_startup_respects_zdotdir(self):
        zsh = shutil.which("zsh")
        if not zsh:
            self.skipTest("host lacks zsh")
        self.env["SHELL"] = zsh
        zdotdir = self.directory / "zsh user's $config `uname` \\ directory"
        zdotdir.mkdir()
        self.env["ZDOTDIR"] = str(zdotdir)
        zshrc = zdotdir / ".zshrc"
        zshrc.write_text("export SHELL_SETUP_VALUE=kept\n")
        self.run_installer()
        self.assertFalse((self.home / ".zshrc").exists())
        self.assertFalse((self.home / ".profile").exists())
        self.assertTrue(zshrc.read_text().startswith("export SHELL_SETUP_VALUE=kept\n"))
        self.assertEqual(self.shell_output('printf "%s\\n" "$SHELL_SETUP_VALUE"; ledgence --version',
                                           shell=zsh, startup=True), "kept\nledgence 0.3.1\n")

    def test_zsh_default_startup_uses_home(self):
        zsh = shutil.which("zsh")
        if not zsh:
            self.skipTest("host lacks zsh")
        self.env["SHELL"] = zsh
        self.run_installer()
        self.assertTrue((self.home / ".zshrc").is_file())
        self.assertEqual(self.shell_output('ledgence --version', shell=zsh, startup=True),
                         "ledgence 0.3.1\n")

    def test_dash_uses_posix_profile(self):
        self.env["SHELL"] = "/bin/dash"
        self.run_installer()
        self.assertTrue((self.home / ".profile").is_file())
        self.assertEqual(self.shell_output('. "$HOME/.profile"; ledgence --version'),
                         "ledgence 0.3.1\n")

    def test_unknown_shell_warns_and_supports_manual_posix_source(self):
        self.env["SHELL"] = "/usr/bin/fish"
        result = self.run_installer()
        self.assert_installed()
        self.assertIn("fish", result.stderr)
        self.assertFalse(any(self.home.glob(".*profile")))
        self.assertFalse(any(self.home.glob(".*rc")))
        self.assertEqual(self.shell_output('. "$HOME/.local/share/ledgence/env"; ledgence --version'),
                         "ledgence 0.3.1\n")

    def test_no_modify_path_preserves_profiles_on_install_and_reinstall(self):
        profiles = {name: f"# original {name}".encode()
                    for name in (".bashrc", ".bash_profile", ".bash_login", ".profile", ".zshrc")}
        for name, content in profiles.items():
            (self.home / name).write_bytes(content)
        for _ in range(2):
            self.run_installer("--no-modify-path")
            self.assert_installed()
            self.assertTrue(self.environment_file().is_file())
            for name, content in profiles.items():
                self.assertEqual((self.home / name).read_bytes(), content)
        self.assertEqual(self.shell_output('. "$HOME/.local/share/ledgence/env"; ledgence --version'),
                         "ledgence 0.3.1\n")
        # Opting out also respects a user removing previously generated setup.
        self.run_installer()
        (self.home / ".profile").write_bytes(profiles[".profile"])
        self.run_installer("--no-modify-path")
        self.assertEqual((self.home / ".profile").read_bytes(), profiles[".profile"])

    def test_no_modify_path_does_not_create_profiles(self):
        self.run_installer("--no-modify-path")
        self.assertTrue(self.environment_file().is_file())
        for name in (".bashrc", ".bash_profile", ".bash_login", ".profile", ".zshrc"):
            self.assertFalse((self.home / name).exists(), name)

    def test_unsafe_profile_objects_warn_without_preventing_install(self):
        profile = self.home / ".profile"
        for kind in ("directory", "dangling symlink", "fifo"):
            with self.subTest(kind=kind):
                if kind == "directory":
                    profile.mkdir()
                elif kind == "dangling symlink":
                    profile.symlink_to(self.directory / "missing-profile")
                else:
                    os.mkfifo(profile)
                original = profile.lstat()
                result = self.run_installer()
                self.assert_installed()
                self.assertIn(str(profile), result.stderr)
                self.assertEqual(profile.lstat().st_ino, original.st_ino)
                self.assertEqual(profile.lstat().st_mode, original.st_mode)
                if kind == "directory":
                    profile.rmdir()
                else:
                    profile.unlink()

    def test_unwritable_profile_warns_without_preventing_install(self):
        profile = self.home / ".profile"
        original = b"# read only profile\n"
        profile.write_bytes(original)
        profile.chmod(0o444)
        if os.access(profile, os.W_OK):
            self.skipTest("current user can write mode 0444 files")
        result = self.run_installer()
        self.assert_installed()
        self.assertIn(str(profile), result.stderr)
        self.assertEqual(profile.read_bytes(), original)

    def test_readonly_configured_profile_needs_no_write_or_warning(self):
        self.run_installer()
        profile = self.home / ".profile"
        configured = profile.read_bytes()
        profile.chmod(0o444)
        result = self.run_installer()
        self.assert_installed()
        self.assertNotIn(str(profile), result.stderr)
        self.assertEqual(profile.read_bytes(), configured)
        self.assertEqual(profile.stat().st_mode & 0o777, 0o444)

    def test_existing_environment_collision_is_preserved(self):
        environment = self.environment_file()
        environment.parent.mkdir(parents=True)
        profile = self.home / ".profile"
        profile.write_text("# original profile\n")
        original = b"# user-owned environment\n"
        target = self.directory / "user environment"
        target.write_bytes(original)
        for kind in ("file", "symlink", "directory", "fifo"):
            with self.subTest(kind=kind):
                if kind == "file":
                    environment.write_bytes(original)
                elif kind == "symlink":
                    environment.symlink_to(target)
                elif kind == "directory":
                    environment.mkdir()
                else:
                    os.mkfifo(environment)
                before = environment.lstat()
                self.run_installer(expected=1)
                self.assertEqual(environment.lstat().st_ino, before.st_ino)
                self.assertEqual(environment.lstat().st_mode, before.st_mode)
                if kind in ("file", "symlink"):
                    self.assertEqual(environment.read_bytes(), original)
                self.assertEqual(profile.read_text(), "# original profile\n")
                self.assertFalse(self.launcher().exists())
                if kind == "directory":
                    environment.rmdir()
                else:
                    environment.unlink()
        self.assertEqual(target.read_bytes(), original)

    def test_custom_prefix_without_home_can_opt_out_of_profiles(self):
        self.env.pop("HOME")
        self.prefix = self.directory / "custom prefix without HOME"
        self.run_installer("--prefix", str(self.prefix), "--no-modify-path")
        self.assert_installed()
        self.assertTrue(self.environment_file().is_file())
        self.assertFalse((self.home / ".profile").exists())

    def test_failed_first_download_preserves_profiles_and_creates_no_environment(self):
        profile = self.home / ".profile"
        original = b"# original profile without final newline"
        profile.write_bytes(original)
        self.env["TEST_DOWNLOAD_FAIL"] = "all"
        self.run_installer(expected=1)
        self.assertEqual(profile.read_bytes(), original)
        self.assertFalse(self.environment_file().exists())
        self.assertFalse(self.launcher().exists())

    def test_documented_piped_install_activates_current_bash_and_zsh(self):
        for name in ("bash", "zsh"):
            shell = shutil.which(name)
            if not shell:
                continue
            with self.subTest(shell=name):
                result = self.piped_install(shell)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertTrue(result.stdout.endswith("ledgence 0.3.1\n"), result.stdout)
                self.assert_installed()

    def test_truncated_piped_installer_does_not_start_partial_install(self):
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("host lacks bash")
        source = INSTALLER.read_text()
        # Cut after installer initialization but before download/verification.
        # A fully parsed main function prevents any setup from running here.
        truncated = source[:source.index("printf 'Downloading Ledgence")]
        result = self.piped_install(bash, content=truncated, curl_exit=22)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.prefix.exists())
        self.assertFalse((self.home / ".profile").exists())
        self.assertFalse((self.home / ".bashrc").exists())

    def test_pipefail_does_not_source_existing_env_after_curl_failure(self):
        bash = shutil.which("bash")
        if not bash:
            self.skipTest("host lacks bash")
        marker = self.directory / "unexpected-source"
        self.environment_file().parent.mkdir(parents=True)
        self.environment_file().write_text(f": > {shlex.quote(str(marker))}\n")
        result = self.piped_install(bash, content="# incomplete download\n", curl_exit=22)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(marker.exists())

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
        self.assertIn("Run: ledgence --help", result.stdout)
        actual = self.shell_output('. "$HOME/.local/share/ledgence/env"; printf "%s" "$PATH"')
        self.assertEqual(actual.split(":")[0], str(self.prefix / "bin"))

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
