---
title: Install the native tools
description: Install the released native CLI, understand the upcoming one-line installer, and choose the right Python and local-stack setup.
---

Ledgence 0.3.1 native bundles contain one `ledgence` executable, the Python worker helper, the client wheel, and Console assets. Choose the archive for your host. For a complete released local deployment built from source, follow [Run Ledgence locally](/tutorials/run-locally). The new [image-based local distribution](/how-to/run-local-distribution) is available on `develop` with a matching qualified kit.

## Next release: one-line installation

**The one-line installer is being prepared for the next release. Published
0.3.1 does not include an `install.sh` download or the new `ledgence local`
commands.** The manual installation below works with the published assets today.

After a release publishes the installer, install Ledgence with:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Then open a new terminal, or activate Ledgence in the current one:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The installer uses `$HOME/.local`, requires no sudo, and retains the complete
versioned bundle with its legal notices. It verifies release checksums, the
internal inventory, and executable version before selecting the installation.
A failed download or verification leaves the previously selected installation
in place. It does not install Docker, a host Python interpreter, or the Python
client into your application.

### Shell setup and installer options

The installer adds a setup block to your shell profiles by default, using
`$SHELL` to select Bash (`~/.bashrc` and the first existing readable login profile
from `~/.bash_profile`, `~/.bash_login`, `~/.profile`, falling back to
`~/.profile`), Zsh (`${ZDOTDIR:-$HOME}/.zshrc`), or sh/dash (`~/.profile`, for new
login shells). Export a custom `ZDOTDIR` before installing. Existing content is
preserved, and reinstalling does not append duplicate setup blocks. Unreadable,
unwritable, or nonregular profiles are preserved with a warning to configure the
shell manually. Other shells receive manual setup instructions.

The generated environment file moves the installation's `bin` directory to the
front of `PATH`, removing duplicate entries for that directory. Sourcing that
file makes Ledgence available immediately: a child installer process cannot
update the current terminal's environment. Pass `--no-modify-path` to skip profile changes.
For `--prefix /absolute/path`, source `/absolute/path/share/ledgence/env` instead;
the installer prints the exact command.

From a development source checkout containing `install.sh`, the installer can
already install the released 0.3.1 executable:

```sh
sh install.sh --version 0.3.1 && . "$HOME/.local/share/ledgence/env"
```

That command provides the released 0.3.1 capabilities. It does not add `local`
commands or the development build's resource defaults. The following manual
instructions remain the supported path without a source checkout.

`--version X.Y.Z` selects an exact stable version; without it the installer
resolves the latest stable release once. Pass `--prefix /absolute/path` for a
different installation location, or `--no-modify-path` to configure `PATH`
yourself. These options can be supplied to a downloaded copy of `install.sh`.

## Before you start

The native targets are **Linux x86_64/glibc**, built and qualified on Ubuntu 24.04, and **macOS arm64**. Other Linux distributions need compatible host libraries; inspect `candidate-provenance.json` for the archive's actual dynamic requirements. There are no published native Windows, Linux ARM64, macOS Intel, or musl/Alpine bundles in 0.3.1.

| What you will run | Additional requirements |
| --- | --- |
| CLI administration or MCP against an existing API | No Docker or host Python required. |
| Native Python worker | A separately supplied CPython 3.11–3.14 interpreter and prepared application dependencies matching its platform and exact major/minor. |
| Native orchestrator | A separately configured PostgreSQL database and program store. |
| Python HTTP client | Python 3.11+ and `ledgence-client` in your application's environment. |
| Complete local container stack | Docker with Compose; containers supply their own interpreter and services. |

Check your host and, if running a native Python worker, its interpreter:

```sh
uname -s
uname -m
python3 --version
```

Before replacing an existing deployment, follow [Upgrade to 0.3.1](/how-to/upgrade-to-0-3).

## Download and verify the archive

Set the target for your host:

```sh
  # Linux x86_64/glibc (Ubuntu 24.04 qualification):
export LEDGENCE_TARGET=x86_64-unknown-linux-gnu
  # On macOS Apple silicon, use this instead:
  # export LEDGENCE_TARGET=aarch64-apple-darwin

mkdir -p "$HOME/.local/share/ledgence/downloads/0.3.1"
cd "$HOME/.local/share/ledgence/downloads/0.3.1"
export LEDGENCE_ARCHIVE="ledgence-0.3.1-$LEDGENCE_TARGET.tar.gz"
curl --fail --location --output "$LEDGENCE_ARCHIVE" \
  "https://github.com/Ledgence/ledgence/releases/download/v0.3.1/$LEDGENCE_ARCHIVE"
curl --fail --location --output SHA256SUMS \
  https://github.com/Ledgence/ledgence/releases/download/v0.3.1/SHA256SUMS
```

The release checksum file can list both platforms. Verify the entry for the archive you downloaded. This checksum check also works with older system Python versions; running Ledgence programs still requires CPython 3.11–3.14:

```sh
python3 - <<'PYTHON'
import hashlib
import os
from pathlib import Path
archive = Path(os.environ["LEDGENCE_ARCHIVE"])
entries = [line.split() for line in Path("SHA256SUMS").read_text().splitlines() if line.strip()]
matches = [digest for digest, name in entries if name.lstrip("*") == archive.name]
if len(matches) != 1:
    raise SystemExit("Expected exactly one checksum for the selected archive")
digest = hashlib.sha256()
with archive.open("rb") as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
actual = digest.hexdigest()
if actual != matches[0]:
    raise SystemExit("Archive checksum mismatch")
print(archive.name, "OK")
PYTHON
```

Continue only after `OK`. Extract the archive and verify its complete internal file inventory:

```sh
tar -xzf "$LEDGENCE_ARCHIVE"
export LEDGENCE_BUNDLE="$PWD/ledgence-0.3.1-$LEDGENCE_TARGET"
cd "$LEDGENCE_BUNDLE"
  # Linux:
sha256sum -c SHA256SUMS
  # On macOS, use: shasum -a 256 -c SHA256SUMS
```

All listed files should report `OK`. Keep the entire bundle together, including `LICENSE`, `legal/`, `console/`, `runtime/`, and provenance. `provenance.json` identifies the release; `candidate-provenance.json` preserves the build source and host requirements.

## Make the CLI available

```sh
export PATH="$LEDGENCE_BUNDLE/bin:$PATH"
ledgence --version
ledgence --help
ledgence program --help
ledgence worker --help
ledgence orchestrator --help
ledgence task --help
```

Worker and orchestrator remain separate processes. The [CLI reference](/reference/cli) covers all command groups and maps the old executable names. The historical 0.1.0 archive still contains three executables; its bundled documentation remains authoritative for that archive.

This manual `PATH` export affects the current shell. Add the same bundle's
`bin` directory to your shell profile for future terminals, and keep the entire
bundle at that location. Released 0.3.1 requires the explicit resource paths
shown below; automatic worker-helper and Console discovery is a `develop`
feature.

## Verify execution with your Python interpreter

Create a disposable example in a fresh directory. `LEDGENCE_PYTHON` must select CPython 3.11–3.14. If `python3 --version` shows an older system interpreter, replace `$(command -v python3)` below with the absolute path to your supported interpreter:

```sh
export LEDGENCE_PYTHON="$(command -v python3)"
export LEDGENCE_EXAMPLE_DIR="$(mktemp -d)"

ledgence program example \
  --directory "$LEDGENCE_EXAMPLE_DIR/example" --python "$LEDGENCE_PYTHON"
ledgence program publish \
  --source "$LEDGENCE_EXAMPLE_DIR/example/program" --store "$LEDGENCE_EXAMPLE_DIR/store"
ledgence worker run \
  --tasks "$LEDGENCE_EXAMPLE_DIR/example/tasks.json" \
  --store "$LEDGENCE_EXAMPLE_DIR/store" --cache "$LEDGENCE_EXAMPLE_DIR/cache" \
  --python "$LEDGENCE_PYTHON" \
  --runner "$LEDGENCE_BUNDLE/runtime/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

Expect two successful JSON reports with the same `process_id`, with `reused_process: true` in the second. This checks local publication, retrieval, Python execution, and warm process reuse; it does not start durable orchestration.

The bundle's client wheel is separate from the worker helper. Install it in a virtual environment when your application needs the HTTP client:

```sh
"$LEDGENCE_PYTHON" -m venv "$LEDGENCE_EXAMPLE_DIR/client"
"$LEDGENCE_EXAMPLE_DIR/client/bin/python" -m pip install \
  "$LEDGENCE_BUNDLE/python-client/ledgence_client-0.3.1-py3-none-any.whl"
```

Installation obtains the pinned client dependencies unless you supply a reviewed offline wheelhouse.

For an application using the published package instead, add
`ledgence-client==0.3.1` with its existing environment manager, for example
`python -m pip install "ledgence-client==0.3.1"` in an activated virtual
environment, or `uv add "ledgence-client==0.3.1"` in an existing uv project.
The client environment is separate from the worker interpreter; installing
the native CLI does not make `from ledgence.client import AsyncClient` available
to an application. See the [Python client reference](/reference/python-client).

## Start a native service with Console

Configure PostgreSQL and the artifact store using the bundle's `docs/postgres.md` and `docs/http-orchestration.md`. Create the server-owned instance file described in the [Console reference](/reference/console#serving-console), with the existing tenant/namespace binding if adopting data. Apply migrations explicitly:

```sh
ledgence orchestrator migrate
ledgence orchestrator serve \
  --store /absolute/path/to/program-store \
  --bind 127.0.0.1:8080 --instance-config /absolute/path/to/instance.json \
  --console-dir "$LEDGENCE_BUNDLE/console"
```

These commands require the intended PostgreSQL connection and service configuration from the bundled guides. Keep the instance file on every subsequent start. Open [http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/); no Node process is required. To run the complete example with a database, worker, store, and callbacks, follow the [local stack tutorial](/tutorials/run-locally).

**Source:** [Release and checksums](https://github.com/Ledgence/ledgence/releases/tag/v0.3.1) · [Bundle verification](https://github.com/Ledgence/ledgence/blob/v0.3.1/tools/release/verify.py)
