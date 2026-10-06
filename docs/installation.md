# Installation and local startup

Install the native `ledgence` CLI to administer a service or run workers. Install
`ledgence-client` in an application's Python environment to call the API. To run
the complete local stack, use Docker Compose; the CLI's new `local` commands
manage a versioned Compose distribution.

> **Available in development builds:** the installer in this source checkout,
> `ledgence local`, and automatic discovery of installed runtime/Console files
> are being prepared for the next release. Published **0.3.1** has neither the
> `local` commands nor a local distribution kit. Its [native installation guide](https://docs.ledgence.com/how-to/install-native)
> and [source Compose tutorial](https://docs.ledgence.com/tutorials/run-locally)
> remain the supported released setup paths. No new release number or public
> installer download is assumed by this guide.

## Choose what to install

| Need | Install on the host |
| --- | --- |
| Call an existing API from Python | Python 3.11+ and `ledgence-client` in the application environment. |
| Administer an existing API or connect an MCP client | The native CLI; Docker and a local Python interpreter are not required for those commands. |
| Run a Python program in a native worker | The native CLI and a host CPython interpreter matching the program manifest. |
| Start the image-based local stack | Docker Engine or Docker Desktop with Compose 2.23.1+, plus a development CLI and matching qualified distribution kit. |
| Build the existing local stack from source | Git and Docker with Compose; Rust, Node and CPython are supplied by the container build. |

The published native bundles target **Linux x86_64 with glibc** and **macOS Apple
Silicon**. Linux qualification uses Ubuntu 24.04; this is not a claim of support
for every glibc distribution. The installer rejects musl/Alpine and unsupported
native OS/architecture pairs, and checks that the downloaded executable starts
before selecting it. There are no published native Windows, Linux ARM64, or
macOS Intel binaries in 0.3.1.

A distribution kit declares its qualified Linux container platforms, which the
CLI checks against the Docker engine. Container platform support is separate
from native CLI availability. A program still needs dependencies built for the
worker's actual architecture and the exact Python major/minor in its manifest.
An image supporting several architectures does not make one program package
portable across them. The initial distribution uses CPython 3.14.

## Next release: one-line installation

**This download is not available in 0.3.1.** After a release publishes
`install.sh`, install Ledgence with:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Then open a new terminal, or activate the installation in the current one:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The installer configures supported shell profiles for future terminals. An
installer running as a child process cannot change the current shell's
environment. See [shell setup](#shell-setup) for sh/dash login shells, custom
prefixes and other shells. Until that release is available, use the working
0.3.1 instructions below.

## Install the currently released native CLI

The [manual native guide](https://docs.ledgence.com/how-to/install-native) works
with the published 0.3.1 assets today. Alternatively, from a development checkout
that contains `install.sh`, run:

```sh
sh install.sh --version 0.3.1 && . "$HOME/.local/share/ledgence/env"
```

This installs the **released 0.3.1 executable**, not the development executable
from that checkout. It therefore does not add `local` commands or the new
resource defaults. Use the 0.3.1 native instructions after installation. The
installer does not require Python, Rust, Node or Docker.

Without `--version`, the script resolves the latest stable release once. At
present that is 0.3.1. `--prefix /absolute/path` changes the default `$HOME/.local`
installation location. `--base-url HTTPS_URL` selects an alternative release
endpoint with the same release/download layout. To leave shell profiles
unchanged, pass `--no-modify-path`; you can still source the generated environment
file when needed.

### Shell setup

By default, the installer appends a setup block to the startup files for the
shell identified by `$SHELL`:

- **Bash:** `~/.bashrc` and the first existing readable `~/.bash_profile`,
  `~/.bash_login`, or `~/.profile`; it falls back to `~/.profile` if none is readable.
- **Zsh:** `${ZDOTDIR:-$HOME}/.zshrc`. Export a custom `ZDOTDIR` before installing
  so the installer uses it.
- **sh or dash:** `~/.profile`, which applies to new login shells.

Existing profile content is preserved, and reinstalling does not append a
duplicate setup block. Unreadable, unwritable, or nonregular profiles are
preserved with a warning to configure the shell manually. The generated
environment file moves the installation's `bin` directory to the front of `PATH`,
removing duplicate entries for that directory. For another shell, the installer
prints instructions for manual setup.

To activate an existing installation in the current shell, run:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

For a custom prefix, source `/absolute/path/share/ledgence/env` instead. The
installer prints the exact command for the selected prefix.

The script downloads the selected archive and release checksums, verifies the
complete internal inventory, and checks the executable version before activating
a launcher. It retains the complete bundle and its legal notices. Download or
verification failures leave the previous installation selected. Reinstalling
the same intact version reuses it; damaged or locally modified installations are
preserved and reported instead of overwritten.

With the default prefix, the layout is:

```text
~/.local/bin/ledgence
  -> ~/.local/share/ledgence/versions/<version>-<target>/bin/ledgence
~/.local/share/ledgence/env

~/.local/share/ledgence/versions/<version>-<target>/
  bin/ledgence
  runtime/ledgence/worker/bootstrap.py
  console/
  python-client/
  docs/
  legal/
  LICENSE
  provenance.json
  SHA256SUMS
  local/       # only when that release includes a qualified local kit
```

An unrelated existing `ledgence` executable or launcher is not replaced.

Keep each bundle intact. Its symlink launcher makes the CLI discoverable on
`PATH`; it does not separate the binary from resources needed by other commands.
See [installed resource behavior](cli.md#installed-resources).

## Start a development distribution

This path requires a development CLI containing `local` and a matching kit
prepared from a qualified image digest. The 0.3.1 release assets do not supply
such a kit. Maintainers prepare and qualify it through the
[release tooling](releasing.md); a source checkout alone is not a ready-to-pull
image distribution.

Docker must already be installed and running Linux containers. From the matching
development checkout, build the host CLI and select the prepared kit:

```sh
cargo build --locked --package ledgence-cli --all-features
./target/debug/ledgence local --help

# Replace this path with the extracted, qualified kit directory.
LEDGENCE_LOCAL_KIT=/absolute/path/to/ledgence-VERSION-local
./target/debug/ledgence local up \
  --distribution "$LEDGENCE_LOCAL_KIT" \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --port 8086
```

The kit's `distribution.json` identifies its version, immutable image digest,
Python version and supported container platforms. `SHA256SUMS` verifies the kit
files. A new installation requires the kit version to match the compiled CLI.
Use `local --help` to check command availability: while development retains the
current package version, `--version` alone does not distinguish it from a
released executable.

When a later release includes the installer and local kit as qualified assets,
its complete native installation will support the shorter sequence:

```sh
# Requires an installed release that includes `local` and its distribution kit.
ledgence local up
ledgence local status
ledgence local logs --follow --service worker
ledgence local down
```

Do not use that sequence with the published 0.3.1 binary. Obtain the installer
and matching bundle from the selected future release's actual asset list; this
guide marks the one-line installer as unavailable until that release.

`up` starts PostgreSQL, runs the kit's migrator, and starts the orchestrator and
one worker. It waits for Compose readiness and prints the API and Console URLs,
Docker context, scope and actual worker platform. With defaults, Console is
`http://127.0.0.1:8080/console/` and the instance uses tenant `acme`, namespace
`demo`, queue `demo`. The base stack starts without application programs or a
callback receiver; follow the copied kit's `README.md` for optional examples.

## Keep local data and configuration

Without `--directory`, local state lives at `$XDG_DATA_HOME/ledgence/local`, or
`$HOME/.local/share/ledgence/local` if `XDG_DATA_HOME` is unset. This is separate
from the versioned native bundles. Always pass the same custom directory when
operating a nondefault installation:

```sh
./target/debug/ledgence local status --directory "$HOME/.local/share/ledgence/local-preview"
./target/debug/ledgence local logs --directory "$HOME/.local/share/ledgence/local-preview" --tail 50
./target/debug/ledgence local down --directory "$HOME/.local/share/ledgence/local-preview"
```

The directory retains a verified copy of the kit and `.ledgence-state.json`,
including a unique project identity, version/image, Docker context/endpoint,
port and concurrency. Docker named volumes hold the database, program store and
worker cache. Keep the state directory at its original absolute path so the CLI
can operate that project again; moving it is rejected. It remains usable after
the original kit or native bundle has been replaced by another CLI installation
supporting the same state format.

Choose `--port`, `--concurrency` (1–1024), and an optional named local `--context`
on the first `up`. These choices are then retained. A port conflict before state
creation allows another `--port` in the same directory. A later startup failure
preserves state for inspection and retry. The CLI rejects remote Docker
endpoints and does not silently follow an unrelated `DOCKER_HOST` setting.

`down` preserves volumes and gives services 65 seconds to stop. Another `up`
reuses the saved kit and data. Installing a newer CLI does not select a new image
or migrate that installation. Changes to retained data require a planned backup
and upgrade procedure; there is no implicit reset, version upgrade or database
rollback. Use a separate directory and free port to create an independent stack.

Backing up the state directory alone does not back up the database or program
packages. Preserve the database and immutable program store together according
to application requirements, plus the saved kit and project configuration.
Stop writers and use PostgreSQL backup tools; copying a live database volume is
not a consistent backup. Validate restoration before an upgrade. There is no
`local restore` command or automatic relocation to a different Docker engine.
Restoring retained data is an operator procedure using compatible schema/image
versions and the matching project configuration.

## Continue using source Compose

The [existing source deployment](local-deployment.md) remains available, including
its optional ElasticMQ route. It builds the image from the checkout and requires
Git and Docker, with no host Rust, Node or Python installation. For a released
setup today, follow the [0.3.1 tutorial](https://docs.ledgence.com/tutorials/run-locally).

The image-based kit instead pulls the exact image digest stored in its Compose
files and requires no source tree to run. These are separate setup paths:

| Setting | Source Compose | Distribution kit / local CLI |
| --- | --- | --- |
| Runtime image | Built from the selected checkout | Qualified image pinned by digest |
| Host API port | `LEDGENCE_HTTP_PORT` | `--port` for CLI; `LEDGENCE_PORT` for direct kit Compose |
| Settings | Source Compose files and explicit overrides | Saved kit and CLI state; explicit overrides for independent direct Compose use |
| Examples | Included in the source deployment | Optional `compose.examples.yaml`; see the kit's README |

When using Compose directly, retain the same project name and file list for
startup, logs and shutdown. A kit README describes that route. The CLI manages
its own saved base project and does not adopt an existing source Compose project.

## Install the Python client in your project

The CLI installer does not select or alter an application's Python environment.
For the published client, use Python 3.11+ in your project:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install "ledgence-client==0.3.1"
```

In an existing uv project, the equivalent dependency declaration is:

```sh
uv add "ledgence-client==0.3.1"
```

Use your project's existing package manager rather than creating a second
environment for it. The package is imported as
`from ledgence.client import AsyncClient`; it does not install the native CLI or
start infrastructure. Installing it globally, with `pipx`, or inside a private
CLI environment is not a substitute for adding it to the application's
environment. See the [client guide](../sdk/python-client/README.md) for task and
workflow calls.

Host Python runs the client; worker Python executes the packaged program. They
need not be the same interpreter version. The worker helper under
`ledgence.worker` is a separate bundled component, and application dependencies
must be prepared for the worker environment before program publication.
