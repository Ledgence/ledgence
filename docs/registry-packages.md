# Registry packages

The preserved `v0.3.0` source tag had no published registry packages or native
distribution. Use the 0.4.0 versions below.

Ledgence distributes a Python client and reusable Rust adapter contracts separately
from native worker/orchestrator release bundles.

The following packages are publicly available at **0.4.0**:

| Registry | Package | Purpose |
| --- | --- | --- |
| PyPI | [`ledgence-client`](https://pypi.org/project/ledgence-client/0.4.0/) | Async task/workflow client; import `ledgence.client` |
| crates.io | [`ledgence-worker-api`](https://crates.io/crates/ledgence-worker-api/0.4.0) | Worker execution, runtime, artifact and telemetry contracts |
| crates.io | [`ledgence-orchestration-api`](https://crates.io/crates/ledgence-orchestration-api/0.4.0) | Task/workflow orchestration and delivery contracts |

Native bundles for **0.4.0** include Console on Linux x86_64/glibc (Ubuntu 24.04 qualification) and macOS arm64. The historical first native bundle remains **v0.1.0 for macOS arm64**.
See the [release reference](https://docs.ledgence.com/reference/releases) for the
artifact matrix. Package publication does not imply a native bundle exists for
that version or platform. Public APIs may evolve before 1.0.

## Install a published package

In an active Python 3.11+ virtual environment:

```sh
python -m pip install "ledgence-client==0.4.0"
```

For Rust adapters, use Rust 1.98 or newer and add the contract you need:

```toml
[dependencies]
ledgence-worker-api = "0.4.0"
ledgence-orchestration-api = "0.4.0"
```

Rust implementation crates and binaries have `publish = false`. Installing
these contract libraries does not deploy Ledgence services. The Python client
does not include the separately supplied `ledgence.worker` helper or CPython.
Ledgence-owned source is MIT; packaged legal notices remain applicable.
See [dependency policy](dependencies.md).

## Local worker helper package

Updated `develop` source also packages `sdk/python` as **`ledgence-worker`
0.4.0**, providing `ledgence.worker` for authoring and testing programs in a
Python 3.11+ application environment. It has no runtime dependencies. This
distribution is **not published to PyPI** and is not part of the published
package table above. The existing `v0.4.0` tag lacks its packaging metadata.

Use a checkout containing `sdk/python/pyproject.toml`, then run
`uv add --dev /absolute/path/to/ledgence/sdk/python` from your application project,
or use the [pip alternative](installation.md#install-the-worker-helper-for-development).
There is no name-based registry install for this package yet. It does not install
the CLI, services or client. The Rust worker continues to supply its own helper
at execution; see the [helper guide](../sdk/python/README.md#install-for-local-development).

The remaining sections describe maintainer qualification and publication.
Installing an existing package does not require these steps. The worker helper
has a local qualification gate, but the publication procedures below publish
only the client and Rust API packages.

## Qualification

Use the repository Rust toolchain and Python 3.11–3.14. Start from a clean,
committed checkout. These tools require new output directories outside checkout
and upload nothing:

```sh
python3 tools/check-python-client.py \
  --dist-dir /tmp/ledgence-python/dist \
  --evidence /tmp/ledgence-python/evidence.json
python3 tools/check-python-worker.py \
  --dist-dir /tmp/ledgence-python-worker/dist \
  --evidence /tmp/ledgence-python-worker/evidence.json
python3 tools/check-rust-packages.py --output /tmp/ledgence-rust
```

The Python gate builds a wheel from its source distribution, checks metadata,
typing files and legal notices, and runs installed base/optional-tracing tests
outside checkout. The source distribution includes its test corpus. The gate
also checks coexistence with the separately delivered worker helper. CI repeats
installed-client tests across Python 3.11–3.14 on Linux x86_64/glibc and macOS arm64.

The worker gate builds its source distribution and wheel, checks metadata,
license and typing files, and tests the installed helper outside the checkout,
including coexistence with the installed client. It reuses the client's
hash-reviewed build/runtime wheelhouse for those checks; client dependencies do
not become worker dependencies. The same CI interpreter/platform matrix runs
this gate. The registry qualification workflow retains the worker package as a
separate artifact, and its source gate checks version alignment with the client
and Rust workspace. These checks do not publish `ledgence-worker` to PyPI.

The Rust gate selects only the two API crates and uses Cargo's multi-package
archive verification. Cargo supplies a temporary registry for unpublished
inter-crate dependencies. The gate checks source/notice bytes, normalized
manifests, the registry dependency graph and the worker archive checksum recorded
in the orchestration archive. It then runs tests, doctests and warning-denied
documentation builds from fresh archive extracts, using a temporary registry of
checksum-verified dependencies without workspace paths. Registry reads are
required. Exported archives and `evidence.json` identify the exact source commit
and SHA256 values.

Run package-gate regressions with:

```sh
python3 -m unittest discover -s tools -p 'test_rust_packages.py' -v
python3 -m unittest discover -s tools/release -p 'test_*.py' -v
```

## Initial registry setup

The packages listed above have already been published. Existing releases use the
configured package identities; a pending publisher or bootstrap credential is
only needed when establishing a new package. The setup below documents that
initial process.

A maintainer must control the registry accounts and complete their email and
authentication requirements. Do not commit passwords or API tokens.

For PyPI, configure a [pending trusted publisher](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/):

- Project: `ledgence-client`
- Repository owner: `Ledgence`
- Repository: `ledgence`
- Workflow filename: `publish.yml`
- Environment: `pypi`

A pending publisher does not reserve the name. After the first upload it becomes
the project's ordinary trusted publisher. The workflow uses PyPI's official
publishing action with attestations and no persistent PyPI token.

[crates.io currently requires an API token for a crate's first publication](https://crates.io/docs/trusted-publishing).
For the first run, configure a short-lived token with the required publish
permission for the two selected names as the GitHub environment secret
`CARGO_REGISTRY_BOOTSTRAP_TOKEN` in environment `crates-io`. Select `bootstrap`
for `crates_auth`. After publication, configure both crates with trusted
publishing for owner `Ledgence`, repository `ledgence`, workflow `publish.yml`,
environment `crates-io`. Delete the bootstrap secret and revoke its token, then
use `trusted` for subsequent releases.

These accounts serve release maintenance. Self-hosted users need no vendor account.

## Controlled publication

The manual **Registry packages** workflow, `.github/workflows/publish.yml`,
defaults to qualification only. Changes to package sources, manifests, helpers
or publishing gates also trigger artifact qualification on push. On a push, this
workflow validates the source versions and runs both package gates; the standalone
CI, documentation, Console and examples workflows provide their applicable source
checks without being repeated inside Registry packages. A green package workflow
alone does not qualify a commit for release.

A manual dispatch, including `publish=false`, runs all four source workflows and
both package gates against the selected source. Publisher jobs require all of
those gates to succeed in that dispatch. No result from another commit or an
unverified cache authorizes publication. Actions are pinned to commit revisions.
See [CI qualification](ci.md) for suite coverage and retained timing evidence.

For publication, select the matching annotated version tag, for example
`v0.4.0`, and set `publish=true`. Rust and Python versions must match that tag,
its commit must be contained in `main`, and checkout must be clean.
The tag is prepared through the normal tested feature/develop/release Git flow;
this workflow does not promote branches or create tags.

Select `both`, `pypi`, or `crates` with `registry`. Publishers have separate
registry environments and credentials. Qualification on `develop` needs no
registry credentials. Once this workflow is present on the repository's default
branch, dispatch it with GitHub CLI:

```sh
# Qualification only; no upload.
gh workflow run publish.yml --ref develop -f publish=false

# Publish an already qualified, annotated release tag.
gh workflow run publish.yml --ref v0.4.0 \
  -f publish=true -f registry=both -f crates_auth=trusted
```

PyPI receives the exact wheel/sdist retained by the package gate. Cargo repackages
and verifies selected crates in dependency order; the package gate must reproduce
the qualified bytes before publication. After upload, the workflow compares
registry checksums and downloaded bytes with the qualified distributions, then
installs into an isolated Python environment or builds a new Cargo consumer
using registry-only dependencies.

Uploads are separate irreversible operations, not a transaction. Before an
upload, the workflow downloads any existing version artifacts and requires their
bytes to match the qualified checksums. It uploads only missing files or crates.
A completed registry is verified again without re-uploading it. Never overwrite
a version or blindly ignore an existing file. A source change requires
a new version. Existing tags and release assets remain unchanged.
