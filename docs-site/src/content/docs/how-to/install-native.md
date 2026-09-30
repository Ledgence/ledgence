---
title: Install the native tools
description: Download and verify the macOS Apple silicon release, then run the bundled worker with your host Python.
---

Install Ledgence's prebuilt tools on a Mac with Apple silicon. This guide uses the published **0.1.0 native bundle**; source and registry packages are separately available at 0.1.1. See [Releases and packages](/reference/releases) for the exact versions.

For a complete stack on Linux or macOS, follow [Run Ledgence locally](/tutorials/run-locally). That tutorial builds the 0.1.1 source with Docker Compose. Neither published version includes Console; use [Explore Ledgence Console](/tutorials/use-console) for the current source implementation.

The current source tree uses one `ledgence` executable with grouped commands.
The [CLI reference](/reference/cli) explains that newer command layout; the
commands below intentionally match the published 0.1.0 archive.

## Before you start

You need macOS on `arm64`, `curl`, and a separately installed CPython 3.11–3.14. The archive supplies the native tools and Ledgence worker helper; it does not bundle Python, PostgreSQL, or a broker.

Check your machine and interpreter:

```sh
uname -m
python3 --version
```

The architecture should be `arm64`. The commands below use `python3`; replace it with the command for your supported interpreter if needed. A program's manifest must match the interpreter's exact Python major/minor and platform.

## Download and verify the archive

Keep the release in its own directory so its helper, licenses, and provenance stay with the executables:

```sh
mkdir -p "$HOME/.local/share/ledgence/downloads/0.1.0"
cd "$HOME/.local/share/ledgence/downloads/0.1.0"

curl --fail --location --output ledgence-0.1.0-aarch64-apple-darwin.tar.gz \
  https://github.com/Ledgence/ledgence/releases/download/v0.1.0/ledgence-0.1.0-aarch64-apple-darwin.tar.gz
curl --fail --location --output SHA256SUMS \
  https://github.com/Ledgence/ledgence/releases/download/v0.1.0/SHA256SUMS
shasum -a 256 -c SHA256SUMS
```

Continue only when the archive reports `OK`. Extract it, then verify its internal file inventory:

```sh
tar -xzf ledgence-0.1.0-aarch64-apple-darwin.tar.gz
export LEDGENCE_BUNDLE="$PWD/ledgence-0.1.0-aarch64-apple-darwin"
cd "$LEDGENCE_BUNDLE"
shasum -a 256 -c SHA256SUMS
```

All listed files should report `OK`. The included `provenance.json` identifies the release and source; `candidate-provenance.json` records the build host and library requirements. Keep `LICENSE` and `legal/` with the bundle when redistributing it.

## Make the tools available

Add this release's binaries to the current terminal session:

```sh
export PATH="$LEDGENCE_BUNDLE/bin:$PATH"
ledgence --help
ledgence-orchestrator --help
ledgence-worker --help
```

`ledgence` is the HTTP task client, `ledgence-orchestrator` runs the orchestration service, and `ledgence-worker` publishes and executes programs. These commands inspect the installation; they do not start a server.

## Verify execution with your Python interpreter

Create a disposable example in a fresh directory. The worker prepares its manifest for your selected interpreter:

```sh
export LEDGENCE_PYTHON="$(command -v python3)"
export LEDGENCE_EXAMPLE_DIR="$(mktemp -d)"

ledgence-worker example \
  --directory "$LEDGENCE_EXAMPLE_DIR/example" --python "$LEDGENCE_PYTHON"
ledgence-worker publish \
  --source "$LEDGENCE_EXAMPLE_DIR/example/program" --store "$LEDGENCE_EXAMPLE_DIR/store"
ledgence-worker run \
  --tasks "$LEDGENCE_EXAMPLE_DIR/example/tasks.json" \
  --store "$LEDGENCE_EXAMPLE_DIR/store" --cache "$LEDGENCE_EXAMPLE_DIR/cache" \
  --python "$LEDGENCE_PYTHON" \
  --runner "$LEDGENCE_BUNDLE/runtime/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

Expect two JSON reports with successful outcomes and the same `process_id`. The second report sets `reused_process` to `true`. This checks package publication, retrieval, Python execution, and warm process reuse locally. It does not start durable orchestration or require PostgreSQL.

The worker uses the helper from this bundle and the host interpreter you selected. Installing `ledgence-client` does not supply that helper. If you need a Python client matched to this native release, the bundle includes its 0.1.0 wheel in `python-client/`.

## Continue to a complete stack

Follow the [local stack tutorial](/tutorials/run-locally) for PostgreSQL-backed orchestration, workflow checkpoints, and completion callbacks. It uses its own matching 0.1.1 source and client.

For a native service setup with this bundle, use the included `docs/http-orchestration.md` and `docs/postgres.md` for 0.1.0 configuration and explicit migrations. Run only operator-trusted programs: subprocess management is not a sandbox for untrusted code.

**Source:** [Native release and checksums](https://github.com/Ledgence/ledgence/releases/tag/v0.1.0) · [Bundle verification](https://github.com/Ledgence/ledgence/blob/v0.1.0/tools/release/smoke.py)
