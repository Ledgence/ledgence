---
title: Install with the CLI and Docker
description: Install the released command-line tool and start a local Ledgence stack in two steps.
prev:
  label: Choose an installation
  link: /getting-started/installation
next:
  label: Your first task
  link: /tutorials/first-task
---

This is the recommended local installation. The installer downloads the released
CLI, and `ledgence local up` starts the matching services in Docker. You do not
need a source checkout, Rust, Node, or host Python to start the stack.

Want to manage Compose yourself or compile the CLI? Return to
[Choose your local installation](/getting-started/installation).

## Before you start

- Start Docker Engine or Docker Desktop with **Linux containers** and
  **Compose 2.23.1 or newer**.
- Use **Linux x86_64/glibc** or **macOS Apple silicon** for the published CLI.
  Linux binaries are qualified on Ubuntu 24.04; other distributions need
  compatible host libraries.
- Keep local port **8080** available. If you already have a Ledgence installation
  with saved data, use its [upgrade guide](/how-to/upgrade-to-0-6-1) first.

Check that Docker is available from your terminal:

```sh
docker info
docker compose version
```

`docker info` should describe a running engine, and the Compose version should be
at least 2.23.1. If Docker cannot connect, start Docker before continuing.

<span id="next-release-one-line-installation"></span>

## One-line installation

Run this in your terminal to install the latest stable release:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

Then make the command available in **this terminal** and check its version:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The `.` command loads the installer's shell setup. `$HOME` is your home directory.
This is needed once in your current terminal because an installer process cannot
change its parent shell. New Bash/Zsh terminals load the setup automatically.
These guides describe **0.6.1**; the [version-pinned command below](#shell-setup-and-installer-options)
lets you select that exact release.

The installer uses your own `~/.local` directory without `sudo`. It verifies the
release files, keeps the complete bundle and legal notices, and configures your
shell. It does not install Docker or change a Python project's dependencies.

## Open your local Console

With Docker running:

```sh
ledgence local up
ledgence local status
```

`up` starts PostgreSQL, applies database migrations, and starts the orchestrator,
Console and one worker. It waits for readiness and prints the service addresses.
The first start can take longer while Docker downloads the images.

Open **[http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/)**. An empty
program registry and execution history are expected: you have installed the
platform but have not published a program yet.

The default API is `http://127.0.0.1:8080`, with tenant `acme`, namespace `demo`
and queue `demo`. Continue with **[Your first task: Hello Ledgence](/tutorials/first-task)**
to create and run a program.

## Stop and come back later

```sh
ledgence local down
```

This stops the services and preserves their database and programs. To use the
same installation again:

```sh
ledgence local up
```

Installing another CLI version does not upgrade this saved stack. See
[Manage your local installation](/how-to/run-local-distribution) for logs,
ports, retained configuration and data.

## Shell setup and installer options

To select **0.6.1** explicitly instead of the latest stable release:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.6.1/install.sh | sh -s -- --version 0.6.1
```

The installer configures Bash, Zsh and supported POSIX-shell profiles without
removing their existing content. For sh/dash/ksh the setup applies to new login
shells. Other shells receive manual instructions. An unreadable or unwritable
profile is preserved and produces a warning.

Use `--no-modify-path` if you want to manage your shell yourself. A custom
`--prefix /absolute/path` changes the installation directory; the installer
prints the corresponding command to activate it. Details of shell files,
custom prefixes and the retained bundle layout are in the
[installation reference](https://github.com/Ledgence/ledgence/blob/develop/docs/installation.md#shell-setup).

## Troubleshoot installation

| Symptom | Next step |
| --- | --- |
| `ledgence: command not found` | Load `"$HOME/.local/share/ledgence/env"` with the `.` command above, or open a new supported terminal. For a custom prefix, use the path printed by the installer. |
| An older version still runs | Run `command -v ledgence` and `ledgence --version`. Load the installer's environment file so the selected installation takes precedence. |
| Docker cannot connect | Start Docker and check `docker info` before retrying `ledgence local up`. |
| Port 8080 is already in use | Stop the other local service, or create a [separate installation on another port](/how-to/run-local-distribution#start-your-local-stack). |
| Startup failed after containers were created | Inspect `ledgence local status` and `ledgence local logs --tail 100`; resolve the reported problem, then retry `ledgence local up`. Keep the saved state and volumes. |
| No bundled local distribution was found | A complete native bundle includes `local/`. A [source-built CLI](/how-to/build-cli-from-source) needs the separately downloaded qualified kit. |

<span id="download-and-verify-the-archive"></span>
<span id="make-the-cli-available"></span>
<span id="verify-installation"></span>
<span id="install-the-python-client"></span>
<span id="start-a-native-service-with-console"></span>

## Other installation needs

For archive verification, manual `PATH` setup, or running services outside
Docker, use [Manual native installation](/how-to/manual-native-installation).
For Python application dependencies, use [Develop Python programs](/how-to/develop-python-programs)
or the [Python client reference](/reference/python-client). The first task
tutorial covers the dependencies needed for its own project.

**Source:** [0.6.1 release](https://github.com/Ledgence/ledgence/releases/tag/v0.6.1) ·
[Installer](https://github.com/Ledgence/ledgence/blob/v0.6.1/install.sh)
