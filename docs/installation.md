# Installation and local startup

Start with [Choose a local installation](https://docs.ledgence.com/getting-started/installation).
Choose one route; each gives you an API, Console, PostgreSQL and a Python worker.
The guides target the published **Ledgence 0.6.1** release.

## Choose what to install

| Route | Choose it when | Guide |
| --- | --- | --- |
| Automatic local setup — recommended | You want the CLI to start and manage the published container stack. | [Installer and local startup](https://docs.ledgence.com/how-to/install-native) |
| Direct Docker Compose | You prefer to control the published kit with Compose commands. | [Docker Compose installation](https://docs.ledgence.com/how-to/install-compose) |
| Build the CLI from source | You want to compile the CLI while using the same published runtime as the tutorials. | [CLI source build](https://docs.ledgence.com/how-to/build-cli-from-source) |

These are complete local setup routes. `ledgence-client` and `ledgence-worker`
are Python packages for applications, not full platform installers. The
published Rust crates are APIs for adapters; they are not CLI binaries.

The downloaded CLI is available for Linux x86_64/glibc and macOS arm64. The
container runtime supports Linux amd64 and arm64. See the route's prerequisites
before choosing it; container support does not imply a native CLI download for
every host platform.

## One-line installation

With a supported native host, install the latest stable CLI:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Open a new Bash/Zsh terminal, or make the CLI available in the current terminal:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The dot command reads the environment file written by the installer. It updates
this terminal's command search path; the piped installer cannot change its
parent shell. Shell startup files are configured for future supported terminals.
This installs the CLI and its matching kit, not Docker or application dependencies.

## Pin a version or use a custom installation

Use the [installer options](https://docs.ledgence.com/how-to/install-native#shell-setup-and-installer-options)
for exact versions and custom prefixes. The tutorials use 0.6.1 components.
For manual checksums and archive extraction, follow
[manual native installation](https://docs.ledgence.com/how-to/manual-native-installation).

### Shell setup

The installer selects shell startup files using `SHELL`, falling back to `sh`
when it is unset. Unless `--no-modify-path` is supplied, it adds a line that loads
its generated environment file:

| Shell | Files configured |
| --- | --- |
| Bash | `~/.bashrc` and the first existing readable login file in this order: `~/.bash_profile`, `~/.bash_login`, `~/.profile`. If none qualifies, it uses `~/.profile`; it does not create a new `.bash_profile` that would hide an existing login file. |
| Zsh | `${ZDOTDIR:-$HOME}/.zshrc`. If you use a custom `ZDOTDIR`, export its absolute path before installing. |
| sh, dash, ksh | `~/.profile`, which is read by new login shells. |
| Other shells | No automatic profile edit; the installer prints the directory to add using that shell's own `PATH` syntax. |

Existing file content is preserved, and reinstalling does not append an
identical setup line. Valid profile symlinks are followed without replacing the
link. Unreadable, unwritable or nonregular profiles are preserved with a warning
to configure them manually.

The default installation prefix is `~/.local`. The CLI launcher and complete
versioned bundles have distinct locations:

```text
~/.local/bin/ledgence
  -> ~/.local/share/ledgence/versions/<version>-<target>/bin/ledgence
~/.local/share/ledgence/env
~/.local/share/ledgence/versions/<version>-<target>/
  bin/ledgence
  console/
  runtime/
  local/
  python-client/
  docs/
  legal/
  LICENSE
  provenance.json
  SHA256SUMS
```

The environment file puts the prefix's `bin` directory first in `PATH` and
removes duplicate entries for that directory while retaining other entries.
Sourcing it updates the current shell; starting the installer in a child
process cannot update that shell. Keep the complete versioned bundle together
so the CLI can locate its helper, Console assets and local kit.

`--prefix /absolute/path` changes the installation root. For example, with
`--prefix "$HOME/tools/ledgence"`, the launcher is
`~/tools/ledgence/bin/ledgence`, bundles live under
`~/tools/ledgence/share/ledgence/versions/`, and current-terminal activation is:

```sh
. "$HOME/tools/ledgence/share/ledgence/env"
```

The installer prints the exact activation command for the selected prefix.
`--no-modify-path` leaves shell startup files unchanged but still creates the
environment file, so you can source it yourself. The [installer source](../install.sh)
is the complete reference for profile selection and installation checks.

## Start the local stack

Start Docker with Linux containers and Compose 2.23.1 or newer, then run:

```sh
ledgence local up
ledgence local status
```

The command starts the database, migrates its schema, and starts the server and
worker. It returns when the services are ready. Open the printed Console URL,
normally <http://127.0.0.1:8080/console/>. The instance is initially empty.

Next, follow [Hello, Ledgence!](https://docs.ledgence.com/tutorials/first-task),
then [Your first workflow](https://docs.ledgence.com/tutorials/first-workflow).
Use this same running instance; no preloaded sample packages are needed.

## Keep local data and configuration

```sh
ledgence local down
ledgence local up
```

`down` stops the services while preserving data. `up` reuses the saved kit,
image and configuration. Installing a new CLI does not upgrade a saved stack.
See [local operations](https://docs.ledgence.com/how-to/run-local-distribution)
for log commands, separate installations, backups and explicit upgrades.

Use only the lifecycle commands for your chosen route. A direct Compose project
and a CLI-managed installation have different project identities and volumes.

## Run the optional container examples

After the first lessons, the [distribution kit guide](../deploy/distribution/README.md#optional-examples)
provides an optional multi-program demonstration and callback receiver. It uses
a separate direct Compose project and requires concurrency one for its process
reuse assertion. Follow [the handoff instructions](https://docs.ledgence.com/how-to/run-local-distribution#programs-examples-and-direct-compose)
if a CLI-managed stack already occupies port 8080. These examples are optional;
they are not an installation requirement.

## Continue using source Compose

To change and rebuild Ledgence services themselves, use the
[full source stack guide](https://docs.ledgence.com/tutorials/run-locally) and
[local deployment reference](local-deployment.md). It builds a local image and
has its own example programs. It is different from compiling only the CLI and
running the published runtime. Do not copy its mutable image tag into a program
builder configuration requiring a registry digest.

## Install the Python client in your project

For a new tutorial project, `ledgence program new` already declares both Python
packages. `uv sync` installs them in the project's `.venv`; no extra pip step is
needed. See [the first task](https://docs.ledgence.com/tutorials/first-task).

For an existing application that submits work, add the client to that application's
environment with its package manager. For a uv project:

```sh
uv add "ledgence-client==0.6.1"
```

The [client README](../sdk/python-client/README.md) and
[client reference](https://docs.ledgence.com/reference/python-client) cover pip,
optional tracing and the API. Installing a client does not start a server or worker.

## Install the worker helper for development

For an existing Python program project:

```sh
uv add --dev "ledgence-worker==0.6.1"
```

The helper provides `ledgence.worker` imports and types for your editor and
local tests. The running worker supplies its own matching helper at execution
time. Follow [program development](https://docs.ledgence.com/how-to/develop-python-programs)
for application runtime dependencies and [the helper contract](../sdk/python/README.md).
