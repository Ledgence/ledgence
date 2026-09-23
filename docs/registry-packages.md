# Registry packages

Ledgence distributes a Python client and reusable Rust adapter contracts separately
from native worker/orchestrator release bundles.

| Registry | Package | Purpose |
| --- | --- | --- |
| PyPI | `ledgence-client` | Async task/workflow client; import `ledgence.client` |
| crates.io | `ledgence-worker-api` | Worker execution, runtime, artifact and telemetry contracts |
| crates.io | `ledgence-orchestration-api` | Task/workflow orchestration and delivery contracts |

Rust implementation crates and binaries have `publish = false`. Installing
these contract libraries does not deploy Ledgence services. The Python client
does not include the separately supplied `ledgence.worker` helper or CPython.
Ledgence-owned source is MIT; packaged legal notices remain applicable.
See [dependency policy](dependencies.md).

## Qualification

Use the repository Rust toolchain and Python 3.11–3.14. Start from a clean,
committed checkout. Both tools require new output directories outside checkout
and upload nothing:

```sh
python3 tools/check-python-client.py \
  --dist-dir /tmp/ledgence-python/dist \
  --evidence /tmp/ledgence-python/evidence.json
python3 tools/check-rust-packages.py --output /tmp/ledgence-rust
```

The Python gate builds a wheel from its source distribution, checks metadata,
typing files and legal notices, and runs installed base/optional-tracing tests
outside checkout. The source distribution includes its test corpus. The gate
also checks coexistence with the separately delivered worker helper. CI repeats
installed-client tests across Python 3.11–3.14 on Linux x86_64/glibc and macOS arm64.

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

## One-time registry setup

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
or publishing gates also trigger qualification on push. It runs the complete CI and documentation
workflows plus both package gates. Publisher jobs wait for all those jobs to
succeed. Actions are pinned to commit revisions.

For publication, select the matching annotated version tag, for example
`v0.1.1`, and set `publish=true`. Rust and Python versions must match that tag,
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
gh workflow run publish.yml --ref v0.1.1 \
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
