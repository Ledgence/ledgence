---
title: Build the CLI from source
description: Compile the released Rust CLI and use it with the same published local runtime as the other installation methods.
prev:
  label: Choose an installation
  link: /getting-started/installation
next:
  label: Your first task
  link: /tutorials/first-task
---

This method compiles the **command-line tool** from the Ledgence 0.6.1 source
tag. The database, orchestrator, Console and worker still run from the published
Docker images, so you can follow the same task and workflow tutorials afterward.

If you want to change the platform and build its container image too, use the
[advanced full-source walkthrough](/tutorials/run-locally). For a prebuilt CLI,
use the [installer](/how-to/install-native).

## Before you start

You need:

- Git and [Rust installed with rustup](https://www.rust-lang.org/tools/install).
  The checkout's `rust-toolchain.toml` selects **Rust 1.98.1**.
- A native C/C++ compiler and linker, such as Xcode Command Line Tools on macOS
  or `build-essential` on Ubuntu.
- Docker Engine or Docker Desktop, running Linux containers with Compose
  **2.23.1 or newer**, and local port **8080** available.
- `curl` and `tar` for the published local kit.

The documented native release targets are Linux x86_64/glibc and macOS Apple
silicon. Building from source does not establish support for another host
platform. The container kit supports `linux/amd64` and `linux/arm64`.

## 1. Build and install the CLI

In the directory where you keep projects:

```sh
git clone --branch v0.6.1 --depth 1 https://github.com/Ledgence/ledgence.git ledgence-source
cd ledgence-source
cargo install --path crates/ledgence-cli --locked
```

The release tag gives you the source for the documented version; Git's
"detached HEAD" notice is expected. `--path` builds the CLI package in this
checkout, and `--locked` keeps the dependency versions from its lockfile.
The first compilation can take several minutes.

With a default Rust installation, Cargo puts the command in `~/.cargo/bin`.
Check it directly:

```sh
"$HOME/.cargo/bin/ledgence" --version
command -v ledgence
```

The version should be **0.6.1**. `command -v` should identify the same Cargo
installation. If another `ledgence` takes precedence, put Cargo's bin directory
first for this terminal:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
```

`PATH` is the list of directories your shell searches for commands; this adds
Cargo's directory at the front. Rustup normally configures it for new terminals.
If necessary, add that same line to your shell's startup file (`~/.zshrc` for
Zsh or `~/.bashrc` for Bash). If you configured a custom Cargo installation root,
use its `bin` directory instead.

The CLI is not published as a crates.io application: `cargo install ledgence`
is not the installation command. The local `--path` build above is supported.

## 2. Download the matching runtime kit

The compiled CLI does not bundle a ready-to-run local kit. From the source
checkout, move back to its parent and create a separate installation folder:

```sh
cd ..
mkdir ledgence-local
cd ledgence-local
curl --fail --location --output ledgence-0.6.1-local.tar.gz https://github.com/Ledgence/ledgence/releases/download/v0.6.1/ledgence-0.6.1-local.tar.gz
curl --fail --location --output ledgence-0.6.1-local.tar.gz.sha256 https://github.com/Ledgence/ledgence/releases/download/v0.6.1/ledgence-0.6.1-local.tar.gz.sha256
```

Check the downloaded archive with the command for your operating system:

**Linux:**

```sh
sha256sum -c ledgence-0.6.1-local.tar.gz.sha256
```

**macOS:**

```sh
shasum -a 256 -c ledgence-0.6.1-local.tar.gz.sha256
```

Continue only after `OK`, then extract it:

```sh
tar -xzf ledgence-0.6.1-local.tar.gz
```

Use the **published kit**, not `deploy/distribution` from the checkout: that
source directory is a packaging template without a qualified runtime image.

## 3. Start the local services

Stay in the `ledgence-local` directory containing the extracted kit:

```sh
ledgence local up --distribution "$PWD/ledgence-0.6.1-local"
ledgence local status
```

`$PWD` means the current directory. The argument therefore gives Ledgence the
absolute path to the kit you just extracted. The CLI verifies its file inventory,
saves a copy with this installation's settings, then waits for the Docker services
to become ready. Subsequent starts use that saved copy.

Open **[http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/)**. You now have
the same empty program registry, API and worker as the installer route, with
tenant `acme`, namespace `demo` and queue `demo`. The worker runs CPython 3.14 and
accepts programs through the configured HTTP publication endpoint.

Continue with **[Your first task: Hello Ledgence](/tutorials/first-task)**.

## Stop and restart

```sh
ledgence local down
ledgence local up
```

`down` preserves the data; `up` reuses the saved kit. You do not need to pass
`--distribution` again. An existing CLI-managed installation retains its old kit
and image: use a [separate state directory](/how-to/run-local-distribution#start-your-local-stack)
if you already have one, rather than trying to replace its files.

**Source:** [CLI package](https://github.com/Ledgence/ledgence/blob/v0.6.1/crates/ledgence-cli/Cargo.toml) ·
[Rust toolchain](https://github.com/Ledgence/ledgence/blob/v0.6.1/rust-toolchain.toml)
