---
title: Install the native tools
description: Download and verify Ledgence 0.3.0 for Linux x86_64 or macOS Apple silicon, then run the unified CLI and bundled Console.
---

Ledgence 0.3.0 native bundles contain one `ledgence` executable, the Python worker helper, the client wheel, and Console assets. Choose the archive for your host. For a complete local deployment built from source, follow [Run Ledgence locally](/tutorials/run-locally).

## Before you start

The native targets are **Linux x86_64/glibc**, built and qualified on Ubuntu 24.04, and **macOS arm64**. Other Linux distributions need compatible host libraries; inspect `candidate-provenance.json` for the archive's actual dynamic requirements. CPython 3.11–3.14, PostgreSQL, brokers, and system libraries are supplied separately. A program's manifest must match its worker interpreter's exact Python major/minor and platform.

Check your host and interpreter:

```sh
uname -s
uname -m
python3 --version
```

Before replacing an existing deployment, follow [Upgrade to 0.3.0](/how-to/upgrade-to-0-3).

## Download and verify the archive

Set the target for your host:

```sh
  # Linux x86_64/glibc (Ubuntu 24.04 qualification):
export LEDGENCE_TARGET=x86_64-unknown-linux-gnu
  # On macOS Apple silicon, use this instead:
  # export LEDGENCE_TARGET=aarch64-apple-darwin

mkdir -p "$HOME/.local/share/ledgence/downloads/0.3.0"
cd "$HOME/.local/share/ledgence/downloads/0.3.0"
export LEDGENCE_ARCHIVE="ledgence-0.3.0-$LEDGENCE_TARGET.tar.gz"
curl --fail --location --output "$LEDGENCE_ARCHIVE" \
  "https://github.com/Ledgence/ledgence/releases/download/v0.3.0/$LEDGENCE_ARCHIVE"
curl --fail --location --output SHA256SUMS \
  https://github.com/Ledgence/ledgence/releases/download/v0.3.0/SHA256SUMS
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
export LEDGENCE_BUNDLE="$PWD/ledgence-0.3.0-$LEDGENCE_TARGET"
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

Worker and orchestrator remain separate processes. The [CLI reference](/reference/cli) maps the old executable names to the new command groups. The historical 0.1.0 archive still contains three executables; its bundled documentation remains authoritative for that archive.

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
  "$LEDGENCE_BUNDLE/python-client/ledgence_client-0.3.0-py3-none-any.whl"
```

Installation obtains the pinned client dependencies unless you supply a reviewed offline wheelhouse.

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

**Source:** [Release and checksums](https://github.com/Ledgence/ledgence/releases/tag/v0.3.0) · [Bundle verification](https://github.com/Ledgence/ledgence/blob/v0.3.0/tools/release/verify.py)
