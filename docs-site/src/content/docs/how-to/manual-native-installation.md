---
title: Manual native installation
description: Verify release archives, configure the CLI manually, and understand the requirements for running services outside Docker.
---

This is an advanced alternative to the [CLI installer](/how-to/install-native).
Use it when you need to inspect the release archive or manage its installation
location yourself. For a first local stack, choose one of the
[three installation methods](/getting-started/installation) instead.

## Before you start

Ledgence 0.6.1 publishes native archives for **Linux x86_64/glibc** and **macOS
Apple silicon**. Linux qualification uses Ubuntu 24.04. Other Linux distributions
need compatible host libraries; the bundle's `candidate-provenance.json` records
its dynamic requirements. There are no native Windows, Linux ARM64, macOS Intel,
or musl/Alpine archives in this release.

Each archive contains the CLI, Python worker helper, client wheel, Console
assets, a matching container kit and legal notices. It does not install Docker,
PostgreSQL, a host Python interpreter, or dependencies into your own Python project.

## Download and verify the archive

Create a **new** directory in a location where you intend to keep the bundle:

```sh
mkdir -p ~/tools
cd ~/tools
mkdir ledgence-0.6.1-download
cd ledgence-0.6.1-download
```

Choose the block for your host. Download checksums from the same release as the
archive; the check below selects only your archive's entry because `SHA256SUMS`
lists more than one release file.

**Linux x86_64/glibc:**

```sh
curl --fail --location --output ledgence-0.6.1-x86_64-unknown-linux-gnu.tar.gz https://github.com/Ledgence/ledgence/releases/download/v0.6.1/ledgence-0.6.1-x86_64-unknown-linux-gnu.tar.gz
curl --fail --location --output SHA256SUMS https://github.com/Ledgence/ledgence/releases/download/v0.6.1/SHA256SUMS
grep '  ledgence-0.6.1-x86_64-unknown-linux-gnu.tar.gz$' SHA256SUMS | sha256sum -c
```

**macOS Apple silicon:**

```sh
curl --fail --location --output ledgence-0.6.1-aarch64-apple-darwin.tar.gz https://github.com/Ledgence/ledgence/releases/download/v0.6.1/ledgence-0.6.1-aarch64-apple-darwin.tar.gz
curl --fail --location --output SHA256SUMS https://github.com/Ledgence/ledgence/releases/download/v0.6.1/SHA256SUMS
grep '  ledgence-0.6.1-aarch64-apple-darwin.tar.gz$' SHA256SUMS | shasum -a 256 -c
```

Continue only after your archive reports `OK`. Extract it and verify all files
inside, again choosing your host's block.

**Linux:**

```sh
tar -xzf ledgence-0.6.1-x86_64-unknown-linux-gnu.tar.gz
cd ledgence-0.6.1-x86_64-unknown-linux-gnu
sha256sum -c SHA256SUMS
```

**macOS:**

```sh
tar -xzf ledgence-0.6.1-aarch64-apple-darwin.tar.gz
cd ledgence-0.6.1-aarch64-apple-darwin
shasum -a 256 -c SHA256SUMS
```

Every listed file should report `OK`. Keep the entire extracted directory
intact, including `LICENSE`, `legal/`, `console/`, `runtime/`, `local/` and
provenance files. The CLI locates those resources relative to its bundle.

## Make the CLI available

While inside the extracted bundle:

```sh
export PATH="$PWD/bin:$PATH"
ledgence --version
```

`$PWD` is the current directory; `PATH` tells your shell where to find commands.
This adds the bundle's `bin` directory first for the current terminal. To retain
it for future terminals, put the same `export PATH=...` line in your shell startup
file, replacing `$PWD` with the **absolute bundle path**. Do not copy the `$PWD`
form into a startup file, where it would refer to a different directory.

The [one-line installer](/how-to/install-native#one-line-installation) handles
this shell setup for you if manual placement is not necessary.

## Verify installation

```sh
ledgence --version
ledgence program --help
```

These commands verify CLI startup. They do not start services or run a program.
To use the complete bundle with Docker, continue at
[Open your local Console](/how-to/install-native#open-your-local-console), then
follow [Your first task](/tutorials/first-task).

## Install the Python client

The native CLI and Python packages are separate. If your application calls the
HTTP API, add `ledgence-client==0.6.1` to that application's existing environment.
For example, from an existing uv project with Python 3.11+:

```sh
uv add "ledgence-client==0.6.1"
```

For a pip-managed project, use its activated virtual environment:

```sh
python -m pip install "ledgence-client==0.6.1"
```

Choose your existing package manager; these are alternatives, not two required
steps. The archive also contains the same-version client wheel under
`python-client/` for a reviewed local installation. Its dependencies must still
be available.

For writing programs, `ledgence program new` creates a project whose development
dependencies include both the client and worker helper. Follow
[Develop Python programs](/how-to/develop-python-programs) rather than assembling
another environment by hand.

## Start a native service with Console

Running native services is an operator setup, separate from the local Docker
installation. Prepare these resources first:

| Service | What you supply |
| --- | --- |
| Orchestrator | PostgreSQL, a writable program store, the intended database connection, and a persistent instance configuration. An external broker is needed only for SQS-compatible delivery. |
| Worker | A separately installed CPython 3.11–3.14 interpreter, prepared programs matching its platform and Python major/minor, a private cache, and access to the program store and orchestration API. |
| Console | The bundled static files, served by the orchestrator; no Node server is required. |

Use the [PostgreSQL configuration guide](https://github.com/Ledgence/ledgence/blob/v0.6.1/docs/postgres.md)
and [HTTP orchestration guide](https://github.com/Ledgence/ledgence/blob/develop/docs/http-orchestration.md)
for connection settings and worker startup. Those settings identify real
infrastructure, so they are intentionally not supplied as local tutorial defaults.
Create the server-owned instance file described in
[Serving Console](/reference/console#serving-console), preserving its tenant and
namespace binding when adopting existing data.

After configuring the intended database connection, apply migrations and start
the orchestrator. Replace the two absolute paths with the resources you prepared:

```sh
ledgence orchestrator migrate
ledgence orchestrator serve --store /absolute/path/to/program-store --bind 127.0.0.1:8080 --instance-config /absolute/path/to/instance.json
```

Keep the same instance file on subsequent starts. The complete bundle supplies
Console assets automatically; `--console-dir /absolute/path/to/console` overrides
that location. The worker's `--runner /absolute/path/to/bootstrap.py` similarly
overrides its bundled helper.

Program uploads need the server's explicit `--allow-program-publication` option
and a writable filesystem store. Review the
[publication requirements](/how-to/build-and-publish-programs#before-you-start)
before enabling it. The example above does not start a worker or enable that writer.

Before replacing existing services, follow [Upgrade to 0.6.1](/how-to/upgrade-to-0-6-1).

**Source:** [Release archives and checksums](https://github.com/Ledgence/ledgence/releases/tag/v0.6.1) ·
[Bundle verification](https://github.com/Ledgence/ledgence/blob/v0.6.1/tools/release/verify.py)
