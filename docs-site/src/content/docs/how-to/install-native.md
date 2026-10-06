---
title: Install the native tools
description: Install the CLI in one command, configure your shell, and start the local container stack.
---

Ledgence 0.4.0 installs one `ledgence` executable with the Python worker helper,
client wheel, Console assets, and a matching local container kit. Native bundles
support Linux x86_64/glibc and macOS Apple silicon. See [requirements](#before-you-start)
for other platforms and service prerequisites.

<span id="next-release-one-line-installation"></span>

## One-line installation

Install the latest stable release:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Then open a new Bash/Zsh terminal, or activate Ledgence in the current
Bash, Zsh, or POSIX shell:

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

### Open your local Console

Start Docker with Linux containers and Compose **2.23.1 or newer**, then run:

```sh
ledgence local up
ledgence local status
```

Open the Console URL printed by `up`, normally
[http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/). The command starts
PostgreSQL, runs migrations, and starts the orchestrator with Console and a worker.
It waits for readiness before returning. No Rust, Node, or host Python is needed
for this container stack.

The first Console starts without application programs or executions. Follow the
[local distribution guide](/how-to/run-local-distribution#programs-examples-and-direct-compose)
for prepared examples and program compatibility. To stop the stack and preserve
its data, run `ledgence local down`; the next `ledgence local up` reuses its saved
configuration. Installing a new CLI does not upgrade an existing stack.

For a build from source, use the [source tutorial](/tutorials/run-locally). To
connect an MCP client or administer an existing API, use the
[MCP setup](/how-to/connect-mcp) or [CLI reference](/reference/cli).

### Shell setup and installer options

The installer adds a setup block to your shell profiles by default, using
`$SHELL` to select Bash (`~/.bashrc` and the first existing readable login profile
from `~/.bash_profile`, `~/.bash_login`, `~/.profile`, falling back to
`~/.profile`), Zsh (`${ZDOTDIR:-$HOME}/.zshrc`), or sh/dash/ksh (`~/.profile`, for new
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

To pin this release explicitly:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.4.0/install.sh | sh -s -- --version 0.4.0
```

`--version X.Y.Z` selects an exact stable version; without it the installer
resolves the latest stable release once. Pass `--prefix /absolute/path` for a
different installation location, or `--no-modify-path` to configure `PATH`
yourself. These options can be supplied to a downloaded copy of `install.sh`.

### Troubleshoot installation

| Symptom | What to check |
| --- | --- |
| The installer URL returns `404`. | Check the [release assets](https://github.com/Ledgence/ledgence/releases/tag/v0.4.0) and your network access. The pinned command above selects the 0.4.0 installer. |
| `ledgence: command not found` after installation. | In Bash, Zsh, or a POSIX shell, source the generated environment file above. For a custom prefix use the path printed by the installer. Other shells need their own `PATH` setup. |
| A previous executable still runs. | Run `command -v ledgence` and `ledgence --version`. Source the new environment file so its `bin` directory takes precedence. The installer preserves unrelated executables. |
| `local` is an unknown command. | Check `command -v ledgence` and `ledgence --version`. This command requires 0.4.0 or newer; source the installed environment file if an older executable takes precedence. |
| `local up` reports no bundled local distribution. | Keep the entire native bundle, including `local/`, together. A source-built CLI needs an explicit `--distribution` pointing to a qualified kit; `deploy/distribution` is only a packaging template. |

For a failed Docker startup, follow the
[local status and logs procedure](/how-to/run-local-distribution#inspect-stop-and-restart).

## Before you start

For writing programs in your own Python project, follow
[Develop Python programs](/how-to/develop-python-programs). Its local
`ledgence-worker` package comes from updated `develop` source and has no PyPI
release. It supports imports and editor tooling in your application environment;
the native worker continues to supply its own helper when executing programs.

The native targets are **Linux x86_64/glibc**, built and qualified on Ubuntu 24.04, and **macOS arm64**. Other Linux distributions need compatible host libraries; inspect `candidate-provenance.json` for the archive's actual dynamic requirements. There are no published native Windows, Linux ARM64, macOS Intel, or musl/Alpine bundles in 0.4.0.

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

Before replacing an existing deployment, follow [Upgrade to 0.4.0](/how-to/upgrade-to-0-4).

## Download and verify the archive

Set the target for your host:

```sh
  # Linux x86_64/glibc (Ubuntu 24.04 qualification):
export LEDGENCE_TARGET=x86_64-unknown-linux-gnu
  # On macOS Apple silicon, use this instead:
  # export LEDGENCE_TARGET=aarch64-apple-darwin

mkdir -p "$HOME/.local/share/ledgence/downloads/0.4.0"
cd "$HOME/.local/share/ledgence/downloads/0.4.0"
export LEDGENCE_ARCHIVE="ledgence-0.4.0-$LEDGENCE_TARGET.tar.gz"
curl --fail --location --output "$LEDGENCE_ARCHIVE" \
  "https://github.com/Ledgence/ledgence/releases/download/v0.4.0/$LEDGENCE_ARCHIVE"
curl --fail --location --output SHA256SUMS \
  https://github.com/Ledgence/ledgence/releases/download/v0.4.0/SHA256SUMS
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
export LEDGENCE_BUNDLE="$PWD/ledgence-0.4.0-$LEDGENCE_TARGET"
cd "$LEDGENCE_BUNDLE"
  # Linux:
sha256sum -c SHA256SUMS
  # On macOS, use: shasum -a 256 -c SHA256SUMS
```

All listed files should report `OK`. Keep the entire bundle together, including `LICENSE`, `legal/`, `console/`, `runtime/`, `local/`, and provenance. `provenance.json` identifies the release; `candidate-provenance.json` preserves the build source and host requirements.

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
bundle at that location. The CLI discovers its bundled worker helper and
Console resources automatically; explicit paths can also be supplied and take
precedence. Installing with the one-line command handles shell profiles for you.

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
  --python "$LEDGENCE_PYTHON" --concurrency 1
```

Expect two successful JSON reports with the same `process_id`, with `reused_process: true` in the second. This checks local publication, retrieval, Python execution, and warm process reuse; it does not start durable orchestration.

The Python client is separate from the worker helper. Install it in a virtual
environment when your application needs the HTTP client:

```sh
"$LEDGENCE_PYTHON" -m venv "$LEDGENCE_EXAMPLE_DIR/client"
"$LEDGENCE_EXAMPLE_DIR/client/bin/python" -m pip install \
  "ledgence-client==0.4.0"
```

Installation obtains the pinned client dependencies unless you supply a reviewed
offline wheelhouse. A manually extracted native archive also supplies
`$LEDGENCE_BUNDLE/python-client/ledgence_client-0.4.0-py3-none-any.whl`; install
that file instead when using the bundled client artifact.

For an existing application, add `ledgence-client==0.4.0` with its environment
manager, for example
`python -m pip install "ledgence-client==0.4.0"` in an activated virtual
environment, or `uv add "ledgence-client==0.4.0"` in an existing uv project.
The client environment is separate from the worker interpreter; installing
the native CLI does not make `from ledgence.client import AsyncClient` available
to an application. See the [Python client reference](/reference/python-client).

## Start a native service with Console

Configure PostgreSQL and the artifact store using the [PostgreSQL](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/postgres.md) and [HTTP orchestration](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/http-orchestration.md) guides, also included in the bundle. Create the server-owned instance file described in the [Console reference](/reference/console#serving-console), with the existing tenant/namespace binding if adopting data. Apply migrations explicitly:

```sh
ledgence orchestrator migrate
ledgence orchestrator serve \
  --store /absolute/path/to/program-store \
  --bind 127.0.0.1:8080 --instance-config /absolute/path/to/instance.json
```

The complete installed bundle supplies its Console assets automatically. An
explicit `--console-dir /absolute/path/to/console` overrides that choice;
`--runner /absolute/path/to/bootstrap.py` similarly overrides the worker helper.
These commands require the intended PostgreSQL connection and service configuration from the bundled guides. Keep the instance file on every subsequent start. Open [http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/); no Node process is required. To run the complete example with a database, worker, store, and callbacks, follow the [local stack tutorial](/tutorials/run-locally).

**Source:** [Release and checksums](https://github.com/Ledgence/ledgence/releases/tag/v0.4.0) · [Bundle verification](https://github.com/Ledgence/ledgence/blob/v0.4.0/tools/release/verify.py)
