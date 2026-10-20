---
title: Install with Docker Compose
description: Download the released local kit and manage Ledgence directly with Docker Compose.
prev:
  label: Choose an installation
  link: /getting-started/installation
next:
  label: Your first task
  link: /tutorials/first-task
---

Choose this method if you want to operate Compose yourself. You will download
the **0.6.0 local kit**, which pulls the released Ledgence image by its immutable
digest. You do not clone the repository or build the runtime image.

For a shorter setup managed by the CLI, choose
[Install with the CLI and Docker](/how-to/install-native).

## Before you start

Start Docker Engine or Docker Desktop using **Linux containers** and **Compose
2.23.1 or newer**. The kit supports `linux/amd64` and `linux/arm64`. Keep port
**8080** available. Do not run this project and another local stack on the same
port.

The commands below use a Bash, Zsh or POSIX terminal with `curl` and `tar`. They
assume a fresh installation. If you already have local Ledgence data, read
[the upgrade guide](/how-to/upgrade-to-0-6) before changing its kit or image.

## 1. Install the CLI for your programs

The Compose stack itself does not require a host CLI, but the next tutorials use
`ledgence` to create, build and publish your Python programs. On a supported
native host, install it now:

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.6.0/install.sh | sh -s -- --version 0.6.0
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The `.` command activates the installer in this terminal. This step installs a
command-line tool; it does not start another stack. **Do not run `ledgence local
up` for this Compose project.**

The published CLI supports Linux x86_64/glibc and macOS Apple silicon. Container
support on other hosts does not guarantee a native CLI there. See the
[CLI source build](/how-to/build-cli-from-source) if you need that alternative;
its host platform still needs to support the Rust implementation.

## 2. Download and verify the local kit

In the directory where you keep local projects, create a new folder:

```sh
mkdir ledgence-local
cd ledgence-local
curl --fail --location --output ledgence-0.6.0-local.tar.gz https://github.com/Ledgence/ledgence/releases/download/v0.6.0/ledgence-0.6.0-local.tar.gz
curl --fail --location --output ledgence-0.6.0-local.tar.gz.sha256 https://github.com/Ledgence/ledgence/releases/download/v0.6.0/ledgence-0.6.0-local.tar.gz.sha256
```

Verify that the download matches the release checksum. Use **one** command for
your operating system:

**Linux:**

```sh
sha256sum -c ledgence-0.6.0-local.tar.gz.sha256
```

**macOS:**

```sh
shasum -a 256 -c ledgence-0.6.0-local.tar.gz.sha256
```

Continue only if the archive reports `OK`. Then extract it:

```sh
tar -xzf ledgence-0.6.0-local.tar.gz
cd ledgence-0.6.0-local
```

Verify the extracted files, again using the command for your system:

**Linux:**

```sh
sha256sum -c SHA256SUMS
```

**macOS:**

```sh
shasum -a 256 -c SHA256SUMS
```

Every listed file should report `OK`. Keep this directory and its files together:
it contains the Compose configuration and `distribution.json`, which identifies
the exact runtime image and Python version.

## 3. Start Ledgence

Run these commands **inside `ledgence-0.6.0-local`**:

```sh
docker compose --project-name ledgence-local --file compose.yaml pull
docker compose --project-name ledgence-local --file compose.yaml up --no-build --detach --wait --wait-timeout 120
docker compose --project-name ledgence-local --file compose.yaml ps
```

Compose downloads the pinned images, starts PostgreSQL, applies the migrations,
and starts the orchestrator and worker. The migration container exits
successfully; the other services stay running. `up --wait` returns after startup
readiness checks pass.

Open **[http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/)**. The program
registry starts empty. The API uses tenant `acme`, namespace `demo` and queue
`demo`; the worker runs CPython 3.14. HTTP program publication is already enabled
for the tutorials.

Continue with **[Your first task: Hello Ledgence](/tutorials/first-task)**. Create
your application in its own project folder; keep this Compose directory for
operating the services.

## Stop, restart, or inspect the services

Return to the extracted kit directory when you need to operate this stack:

```sh
docker compose --project-name ledgence-local --file compose.yaml logs --tail 100 orchestrator worker
docker compose --project-name ledgence-local --file compose.yaml down --timeout 65
```

`down` stops the services and keeps their named data volumes. Start them again
with the same `up` command from step 3. Keep the same `--project-name` and
`--file` values: together they identify this installation. Do not add `--volumes`
to cleanup commands unless you intend to delete its data.

The `ledgence local status`, `logs` and `down` commands manage a different,
CLI-created project; they do not operate this direct Compose installation.
For optional prebuilt example programs, follow the kit's `README.md`. To learn
how to write your own code, follow the first task tutorial instead.

**Source:** [0.6.0 release assets](https://github.com/Ledgence/ledgence/releases/tag/v0.6.0) ·
[Local kit contract](https://github.com/Ledgence/ledgence/blob/v0.6.0/docs/container-distribution.md)
