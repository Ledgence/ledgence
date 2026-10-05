# Release bundles

A candidate is a reviewable artifact built from a clean committed source tree.
Creating it does not tag the repository, promote `develop` to `main`, push changes,
upload an artifact or declare a stable public API. Those are separate release
operations. Embedded Rust and SDK package versions remain the committed versions;
`rc.N` identifies the candidate bundle and its provenance.

## Build and inspect

Use the repository-pinned Rust toolchain, a supported host CPython 3.11–3.14,
and the reviewed Python wheel selection for that host. The installed-client gate
currently reviews Linux x86_64/glibc and macOS arm64. Other native targets need
additional Python artifact and execution qualification before this combined
bundle can be produced. A pure Python client wheel alone does not demonstrate
compatibility of its native transitive dependencies on an untested target.

After the quality gates pass and the source is committed, explicitly build
Console with its pinned toolchain. The prepared dist must name that exact clean
commit, version, toolchain and lock hash. Packaging never downloads Node or starts
an implicit frontend install. Supply `LEDGENCE_POSTGRES_URL` for the disposable
relocated Console database gate (and `LEDGENCE_PSQL` if psql is a wrapper):

```sh
pnpm --dir console build
python3 tools/release/package.py --candidate rc.1 --output /tmp/ledgence-rc --console-dist console/dist
```

For a prepopulated reviewed wheelhouse and Cargo cache:

```sh
python3 tools/release/package.py --candidate rc.1 --output /tmp/ledgence-rc-offline \
  --wheelhouse /path/to/reviewed/wheelhouse --offline --console-dist /path/to/prepared/console-dist
```

Use `--headless` instead of `--console-dist` to explicitly omit web assets. A
headless bundle makes no Console distribution claim. The manual Candidate
packaging workflow always builds and verifies headless candidates on Linux x86_64
and macOS arm64. By default it also builds a Linux x86_64 candidate containing
Console, using the pinned frontend toolchain and a disposable PostgreSQL 18.6
service for relocated verification. Set `include_console=false` to run only the
headless jobs; set `candidate=rc.N` to select the candidate label.

Each successful job retains its verified archive, `SHA256SUMS` and provenance as
an Actions artifact for 30 days. Artifact names distinguish the headless targets
from `candidate-console-linux-x86_64`. These are reviewable candidates; the
workflow does not create tags, promote branches or publish GitHub Releases.
For a macOS bundle containing Console, use the local static build and PostgreSQL
verification described above. Integration and capacity qualification remain
separate required gates.

Offline mode requires the prepared, reviewed static build as well as Cargo/Python
caches; it never silently fetches frontend dependencies.

The output directory must be new and outside checkout. The tool builds an optimized
`ledgence` executable with `--locked`, packages the worker helper, rebuilds the SDK wheel from
its sdist, runs the existing installed-client base/optional-OTel tests and legal
gates, inventories the selected normal/build Cargo graph and Rust toolchain
copyrights, and exercises the relocated executable/helper with a real host Python
process. It rejects a changed or dirty source tree before finalizing.

Bundles built from the current source contain one public executable. The already
published native `0.1.0` bundle retains its three original executables; see its
own documentation. The [CLI migration guide](cli.md) maps the command prefixes.

The archive contains:

- `bin/ledgence`, the unified CLI for program, worker, orchestrator, task, approval and MCP commands;
- `console/` containing verified static assets, manifest and retained notices (unless explicitly headless);
- `runtime/ledgence/worker/`, preserving the native Python namespace;
- `python-client/` with the tested wheel and source distribution;
- `examples/local-compose-client.py`, an installed-SDK companion for the published local Compose programs;
- documentation, Ledgence's MIT license and complete retained third-party legal material;
- the Cargo lock, target/toolchain/build provenance, installed-client evidence,
  and a checksum inventory for every included file.

The actual archive is extracted, its complete file inventory and checksums verified,
and its relocated executable/helper executed again before the tool reports success. A Console bundle additionally starts its extracted orchestrator against a uniquely created disposable PostgreSQL database and verifies all assets, notices, deep links and actual Console APIs.
You can repeat this check with `python3 tools/release/verify.py --archive ARCHIVE`.

The release directory has an outer `SHA256SUMS` for the archive. After extracting,
verify the internal `SHA256SUMS` with `sha256sum -c SHA256SUMS` on Linux or
`shasum -a 256 -c SHA256SUMS` on macOS. The provenance records actual dynamic
library requirements. CPython, PostgreSQL, brokers and host system libraries are
not bundled. Install the supplied SDK wheel normally to obtain its pinned
reviewed dependencies, or use a separately prepared reviewed wheelhouse offline.

Archive paths, modes, ownership, order and timestamps are normalized. Toolchain,
source commit, feature selection, Cargo lock hash, dynamic libraries and artifact
hashes are recorded. This is reproducible procedure/provenance, not a claim of
byte-for-byte compiler output reproducibility across different build hosts.
[Cargo locked builds](https://doc.rust-lang.org/cargo/commands/cargo-build.html)
prevent dependency resolution drift; they do not pin operating-system/toolchain
implementation details by themselves.

## Candidate acceptance

Run and preserve the repository quality gates for the exact candidate source:
formatting, crate boundaries, warnings, Rust unit/doc tests, feature isolation,
Python helper and installed-client gates, current dependency policy, real
PostgreSQL/HTTP/workflow/completion acceptance, both queue modes, deployment
restart qualification and representative load/soak scenarios. Packaging evidence
covers only the gates the packaging tool actually executes; it is not a
substitute for the integration and capacity reports.

Check that:

- retained results, callback obligations and replay identities survive the
  documented restart/retention windows;
- telemetry failures do not prevent normal work, and enabled/disabled metrics
  cost is measured on release builds;
- reported capacity states workload, resources, concurrency, broker, storage,
  duration, errors and latency percentiles; no extrapolated billion/day guarantee;
- notices cover the selected Rust graph, incorporated native code, Rust standard
  library, SDK artifacts and any separately redistributed runtime/image;
- the documented deployment uses the same source and exact supported program
  platform/runtime contract as the candidate.

Only an explicitly selected, validated candidate should later be promoted to a
stable version. The bundle tooling does not publish or promote `main`. Registry
packages use the separate gated [registry release workflow](registry-packages.md).
Required legal notices do not require users to open-source their applications. See [dependency policy](dependencies.md) and
[local image distribution boundaries](local-deployment.md#qualification-and-distribution-boundary).

## Prepare a stable bundle offline

After selecting a candidate whose required qualification gates have passed, use
`tools/release/promote.py` to prepare a stable archive from its exact bytes. This
operation does not rebuild binaries or Python distributions, change Git, create a
tag, access a registry, or publish anything. The version must already match the
Rust workspace and Python client versions in the candidate's original source.

Provide a clean local release checkout and an explicit existing ref identifying
its HEAD. The release commit must contain the candidate build commit and have the
identical Git tree. A source-equivalent release merge commit may differ from the
original build commit; both identities remain in the resulting provenance. The
tool does not create that merge or decide whether a candidate is qualified.

```sh
python3 tools/release/promote.py \
  --archive /path/to/selected-candidate.tar.gz \
  --sha256 EXPECTED_64_CHARACTER_CANDIDATE_SHA256 \
  --repository /path/to/clean-release-checkout \
  --release-ref refs/heads/release-preparation \
  --version 0.3.1 --output /tmp/ledgence-stable
```

Take the expected SHA256 from the selected candidate's retained outer checksum
file. The output directory must be new and outside both checkouts. The tool checks
that digest, the original normalized archive modes, and every internal checksum
before repackaging. It changes only the
bundle README, `provenance.json`, and internal `SHA256SUMS`; it adds the unchanged
original `candidate-provenance.json` and `promotion-payload-sha256.json`. All other
files, including Console assets/manifest/notices, executables, wheel, sdist, helper, documentation and legal files,
must keep identical bytes and modes. The stable archive uses a version/target root
without an `rc.N` suffix and receives its own outer checksum.

The actual stable archive is extracted and smoke-tested again. Original build
provenance stays intact, while promotion provenance records the release commit,
source tree, original candidate digest and preserved payload inventory. This is
an artifact preparation step; the matching version tag, `main` promotion and public
release remain separate decisions and operations. Preserve the candidate and its
qualification reports alongside the new archive.

Promotion executes the bundled binary, so promote Linux candidates on Linux.
The manual `promote-bundle.yml` workflow downloads
`candidate-console-linux-x86_64` from an explicit Actions run in this repository
and requires the selected archive's expected SHA256. Dispatch it on the matching
annotated release tag after that commit is included in `main`:

```sh
gh workflow run promote-bundle.yml --ref v0.3.1 \
  -f candidate_run=RUN_ID \
  -f candidate_sha256=EXPECTED_64_CHARACTER_CANDIDATE_SHA256 \
  -f version=0.3.1
```

It checks the clean tag identity and source-equivalent candidate, promotes the
existing bytes without rebuilding, and runs relocated execution with CPython
3.14 and disposable PostgreSQL 18.6. Only after promotion and verification pass
does it retain `stable-linux-x86_64` with the stable archive, checksums and
provenance for 30 days. It does not create tags or publish releases or packages.
The candidate's integration and capacity qualification must already be complete.

Run the promotion regression tests with
`python3 -m unittest discover -s tools/release -p 'test_*.py' -v`.
