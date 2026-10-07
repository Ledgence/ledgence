# Installation and local startup

Install the native `ledgence` CLI to administer a service or run workers. Install
`ledgence-client` in an application's Python environment to call the API. To run
the complete local stack, use Docker Compose; the CLI's new `local` commands
manage a versioned Compose distribution.

Ledgence **0.4.0** includes the installer, `ledgence local`, a matching
container distribution kit, and discovery of installed runtime and Console files.
The [manual native guide](https://docs.ledgence.com/how-to/install-native) and
[source Compose deployment](local-deployment.md) remain available.

Preparing programs with `program build` needs local
Docker and a matching digest-pinned worker runtime image. Publishing an already
prepared program with `--server` needs only a CLI and an explicitly
enabled server supporting publication; the CLI checks server capabilities. Neither operation updates an existing saved local kit.
See [program preparation and publication](program-publication.md).

## Choose what to install

| Need | Install on the host |
| --- | --- |
| Call an existing API from Python | Python 3.11+ and `ledgence-client` in the application environment. |
| Author Python programs with local imports and editor support | Python 3.11+ and `ledgence-worker` matching the worker, installed from a verified registry version or matching source in the application's development environment; [setup below](#install-the-worker-helper-for-development). |
| Administer an existing API or connect an MCP client | The native CLI; Docker and a local Python interpreter are not required for those commands. |
| Run a Python program in a native worker | The native CLI and a host CPython interpreter matching the program manifest. |
| Start the image-based local stack | Docker Engine or Docker Desktop with Compose 2.23.1+, plus the complete 0.4.0 native CLI installation. |
| Build the existing local stack from source | Git and Docker with Compose; Rust, Node and CPython are supplied by the container build. |

The published native bundles target **Linux x86_64 with glibc** and **macOS Apple
Silicon**. Linux qualification uses Ubuntu 24.04; this is not a claim of support
for every glibc distribution. The installer rejects musl/Alpine and unsupported
native OS/architecture pairs, and checks that the downloaded executable starts
before selecting it. There are no published native Windows, Linux ARM64, or
macOS Intel binaries in 0.4.0.

The 0.4.0 distribution kit supports **Linux amd64 and arm64** containers, which
the CLI checks against the Docker engine. Container platform support is separate
from native CLI availability. A program still needs dependencies built for the
worker's actual architecture and the exact Python major/minor in its manifest.
An image supporting several architectures does not make one program package
portable across them. The initial distribution uses CPython 3.14.

## One-line installation

Install the latest stable Ledgence release:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Then open a new Bash/Zsh terminal, or activate the installation in the current
Bash, Zsh, or POSIX shell:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The installer configures supported shell profiles for future terminals. An
installer running as a child process cannot change the current shell's
environment. See [shell setup](#shell-setup) for sh/dash login shells, custom
prefixes and other shells.

Start Docker with Linux containers and Compose 2.23.1 or newer, then run:

```sh
ledgence local up
ledgence local status
```

Open the Console URL printed by `up`, normally
`http://127.0.0.1:8080/console/`. The initial stack has a database, orchestrator,
Console and worker, with no application programs or executions yet. See
[startup and examples](#start-the-local-stack) for the exact scope.
Use `ledgence local down` to stop it while retaining its configuration and data.
When only administering an existing API or connecting an MCP client, skip local
startup. The container stack supplies CPython; host Python is not required.

## Pin a version or use a custom installation

For a reproducible installation of this release:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.4.0/install.sh | sh -s -- --version 0.4.0
```

Without `--version`, the script resolves the latest stable release once.
`--prefix /absolute/path` changes the default `$HOME/.local` installation
location. `--base-url HTTPS_URL` selects an alternative release endpoint with
the same release/download layout. To leave shell profiles unchanged, pass
`--no-modify-path`; you can still source the generated environment file when
needed. For example:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.4.0/install.sh | sh -s -- --version 0.4.0 --prefix "$HOME/tools/ledgence"
. "$HOME/tools/ledgence/share/ledgence/env"
```

The installer does not require Python, Rust, Node or Docker. Manual archive
download, checksum verification and extraction remain supported in the
[manual native guide](https://docs.ledgence.com/how-to/install-native).

### Shell setup

By default, the installer appends a setup block to the startup files for the
shell identified by `$SHELL`:

- **Bash:** `~/.bashrc` and the first existing readable `~/.bash_profile`,
  `~/.bash_login`, or `~/.profile`; it falls back to `~/.profile` if none is readable.
- **Zsh:** `${ZDOTDIR:-$HOME}/.zshrc`. Export a custom `ZDOTDIR` before installing
  so the installer uses it.
- **sh, dash or ksh:** `~/.profile`, which applies to new login shells.

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
  local/       # matching digest-pinned container distribution kit
```

An unrelated existing `ledgence` executable or launcher is not replaced.

Keep each bundle intact. Its symlink launcher makes the CLI discoverable on
`PATH`; it does not separate the binary from resources needed by other commands.
See [installed resource behavior](cli.md#installed-resources).

## Start the local stack

The complete 0.4.0 native installation includes its matching local distribution
kit. Docker must already be installed and running Linux containers with Compose
2.23.1 or newer:

```sh
ledgence local up
ledgence local status
ledgence local logs --follow --service worker
```

The kit's `distribution.json` identifies its version, immutable image digest,
Python version and supported container platforms. `SHA256SUMS` verifies the kit
files. A new installation requires the kit version to match the compiled CLI.

To use a separately extracted kit with the installed CLI, select it explicitly:

```sh
ledgence local up \
  --distribution /absolute/path/to/qualified-local-kit \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --port 8086
```

Alternatively, from a matching Ledgence source checkout with its Rust toolchain,
build the CLI and select the same version's qualified kit:

```sh
cargo build --locked --package ledgence-cli --all-features
./target/debug/ledgence local up \
  --distribution /absolute/path/to/qualified-local-kit \
  --directory "$HOME/.local/share/ledgence/local-preview" \
  --port 8086
```

Maintainers prepare and qualify kits through the
[release tooling](releasing.md); a source checkout alone does not provide a
published runtime image.

`up` starts PostgreSQL, runs the kit's migrator, and starts the orchestrator and
one worker. It waits for Compose readiness and prints the API and Console URLs,
Docker context, scope and actual worker platform. With defaults, Console is
`http://127.0.0.1:8080/console/` and the instance uses tenant `acme`, namespace
`demo`, queue `demo`. The base stack starts without application programs or a
callback receiver. See [optional container examples](#run-the-optional-container-examples)
below before following the copied kit's `README.md`.

## Keep local data and configuration

Without `--directory`, local state lives at `$XDG_DATA_HOME/ledgence/local`, or
`$HOME/.local/share/ledgence/local` if `XDG_DATA_HOME` is unset. This is separate
from the versioned native bundles. Always pass the same custom directory when
operating a nondefault installation:

```sh
ledgence local status --directory "$HOME/.local/share/ledgence/local-preview"
ledgence local logs --directory "$HOME/.local/share/ledgence/local-preview" --tail 50
ledgence local down --directory "$HOME/.local/share/ledgence/local-preview"
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

## Run the optional container examples

`local up` creates a saved Compose project with a generated name. The kit's
`README.md` demonstrates direct Compose using the separate name `ledgence-local`.
Those projects have independent containers and named volumes; direct Compose
does not populate or operate the CLI-managed database. Both default to host port
8080, so stop the CLI-managed project first, or choose a different free port for
the direct Compose project.

The kit files are copied **directly into the installation directory**, alongside
`.ledgence-state.json`. After default `local up`, use:

```sh
ledgence local down
cd "${XDG_DATA_HOME:-$HOME/.local/share}/ledgence/local"
export LEDGENCE_CONCURRENCY=1
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml up --no-build --detach --wait --wait-timeout 120
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml run --rm --no-deps publish
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml run --rm --no-deps demo
```

For a custom installation, stop it with
`ledgence local down --directory /original/installation/directory`, then change
into that same directory. To keep it running instead, set `LEDGENCE_PORT` to a
free port, such as `export LEDGENCE_PORT=8087`, before every direct Compose
operation. If the CLI installation uses a custom Docker context, use that context
for each direct command as well (`docker --context CONTEXT compose ...`).
Choose an unused project name if `ledgence-local` already belongs to another
installation, including a source-Compose deployment.

Keep the example's concurrency at **1** for its process-reuse assertion. Its
publish step prepares worker-compatible programs inside the container, registers
them, and its demo submits real tasks, workflows and completion callbacks. Open
`http://127.0.0.1:8080/console/`, or the selected alternative port, to explore
them. Use the same project name, context, port setting and file list for logs and
shutdown:

```sh
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml logs --tail 50
docker compose --project-name ledgence-local --file compose.yaml --file compose.examples.yaml down --timeout 65
```

This preserves the example project's volumes. `ledgence local status`, `logs`,
`down` and `up` continue to address the original saved CLI project; they do not
adopt or stop this direct Compose project. Stop the direct project before
restarting the CLI project on the same port. The CLI project retains its own
original data. See the copied kit's `README.md` for the standalone Compose route.

## Continue using source Compose

The [existing source deployment](local-deployment.md) remains available, including
its optional ElasticMQ route. It builds the image from the checkout and requires
Git and Docker, with no host Rust, Node or Python installation. For a released
setup, use the `v0.4.0` source tag and the [source deployment guide](local-deployment.md).

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
python -m pip install "ledgence-client==0.4.0"
```

In an existing uv project, the equivalent dependency declaration is:

```sh
uv add "ledgence-client==0.4.0"
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

## Install the worker helper for development

`ledgence-worker` provides the `ledgence.worker` imports used by program code.
This source packages version **0.5.0**, requiring Python 3.11+. Check
[registry availability](https://pypi.org/project/ledgence-worker/0.5.0/) before
installing by name. Source installation below works with a checkout containing
`sdk/python/pyproject.toml`; the historical `v0.4.0` tag lacks that metadata.

From your application's existing uv project:

```sh
uv add --dev /absolute/path/to/ledgence/sdk/python
uv run python -c "from ledgence.worker.workflow import Workflow; print('worker imports ready')"
```

The path refers to the Ledgence checkout, not your application directory. This
records a local development dependency and installs it into the application's
environment. Select that environment in your editor; no `PYTHONPATH` is needed.
For an existing activated virtual environment managed with pip, use
`python -m pip install /absolute/path/to/ledgence/sdk/python` instead.
Only helper contributors normally need uv's `--editable` option.

Match the helper version to the deployed worker release. The package supports
imports, type-aware tools and tests of ordinary business functions; invocation
and workflow context APIs still require execution by Ledgence. The native CLI,
server, and `ledgence-client` are separate installations. At runtime the worker
supplies its bundled helper before any artifact copy, so this authoring package
does not replace `--runner` and need not be shipped with your program.

Follow [Develop Python programs](../docs-site/src/content/docs/how-to/develop-python-programs.md)
for the authoring workflow. Application runtime dependencies must still be
prepared for the worker platform and exact Python major/minor under the
[program package contract](program-packages.md).
