# Release candidates

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

After the quality gates pass and the source is committed:

```sh
python3 tools/release/package.py --candidate rc.1 --output /tmp/ledgence-rc
```

For a prepopulated reviewed wheelhouse and Cargo cache:

```sh
python3 tools/release/package.py --candidate rc.1 --output /tmp/ledgence-rc-offline \
  --wheelhouse /path/to/reviewed/wheelhouse --offline
```

The output directory must be new and outside checkout. The tool builds optimized
binaries with `--locked`, packages the worker helper, rebuilds the SDK wheel from
its sdist, runs the existing installed-client base/optional-OTel tests and legal
gates, inventories the selected normal/build Cargo graph and Rust toolchain
copyrights, and exercises the relocated binaries/helper with a real host Python
process. It rejects a changed or dirty source tree before finalizing.

The archive contains:

- `bin/ledgence`, `bin/ledgence-orchestrator`, `bin/ledgence-worker`;
- `runtime/ledgence/worker/`, preserving the native Python namespace;
- `python-client/` with the tested wheel and source distribution;
- `examples/local-compose-client.py`, an installed-SDK companion for the published local Compose programs;
- documentation, Ledgence's MIT license and complete retained third-party legal material;
- the Cargo lock, target/toolchain/build provenance, installed-client evidence,
  and a checksum inventory for every included file.

The actual archive is extracted, its complete file inventory and checksums verified,
and its relocated binaries/helper executed again before the tool reports success.
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
stable version. The tooling deliberately provides no automated publishing or
`main` promotion. Required legal notices do not require users to open-source
their applications. See [dependency policy](dependencies.md) and
[local image distribution boundaries](local-deployment.md#qualification-and-distribution-boundary).
