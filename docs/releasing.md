# Release bundles

A candidate is a reviewable artifact built from a clean committed source tree.
Creating it does not tag the repository, promote `develop` to `main`, push changes,
upload an artifact or declare a stable public API. Those are separate release
operations. Embedded Rust and SDK package versions remain the committed versions;
`rc.N` identifies the candidate bundle and its provenance.

For the digest-pinned local stack and installer, follow
[container distribution](container-distribution.md). Its workflow qualifies both
container architectures, publishes corresponding sources before images, and
produces the kit that native packaging includes under `local/`. Supply
`--local-distribution DIRECTORY` locally, or `local_distribution_run` in Candidate
packaging. Leave a new public release marked as a prerelease until every native
bundle, package, checksum, installer and kit has passed its download checks.

See [CI qualification](ci.md) for the independent source and delivery suites,
artifact reuse, timing evidence and exact-commit requirements.

The current stable release is **0.9.0**. It adds resource admission, weighted offers, shared run budgets, worker poll plans and queue concurrency. See the [0.9.0 release notes](releases/0.9.0.md) and the [upgrade from 0.8.0](../docs-site/src/content/docs/how-to/upgrade-to-0-9.md).

The current release candidate is **0.10.0**. Review its
[candidate notes](releases/0.10.0.md) and
[0.9-to-0.10 upgrade procedure](../docs-site/src/content/docs/how-to/upgrade-to-0-10.md).
Keep the latest stable identity above until every distribution channel is verified.

For future releases, keep public metadata at the last verified version until all
channels are complete. A source version bump or local build does not establish
registry, native-bundle, image or website availability. Never reuse an older
commit's qualification as evidence for new source or replace an immutable
published artifact.

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

The Linux Console job upgrades published Linux x86_64 bundles from 0.6.1, 0.7.0,
0.8.0 and the latest stable 0.9.0, each pinned to its archive checksum. The older
approval rehearsals remain required; the resource and artifact rehearsals do not
replace them. Every rehearsal uses the extracted candidate executable, Console
and Python runner and owns a separate database/evidence directory under
`approval-upgrade/evidence/<version>`. Neither source-only tests nor an older
baseline substitute for the latest-stable rehearsal. The development candidate's
Console contract is 16; the published 0.9.0 bundle retains contract 14.
The resource and artifact gates pin each published target's source and release
commits and require matching predecessor/candidate targets. For 0.8.0, Linux was
built at the release merge while macOS retained its source-equivalent ancestor
build; their distinct published identities remain explicitly pinned.

`check-artifact-upgrade.py` verifies the actual published 0.9.0 bundle inventory
and candidate provenance before and after use. It preserves existing task, fork,
local-step, callback, queue and resource state; interrupts actual transactional
artifact DDL and proves rollback; migrates twice; rejects the old binary by schema;
and restores into a fresh database before resuming work. It then uploads to a
preserved root, checks old credentials cannot acquire the new write capability,
and verifies bytes after restart. Its separate artifact-bearing restore must
preserve a stopped candidate database and the entire filesystem store, including
the hidden marker, then restore both to fresh locations before opening the store.
Require matching store identity, immutable reference and downloaded bytes; spools
and worker caches do not satisfy this recovery check. Receipt validation in
`tools/release/upgrade_evidence.py` rejects missing phases or mismatched executables.

The Linux Console candidate also runs `tools/check-artifact-s3.py` in owned mode,
with the pinned external RustFS fixture replaced between write/read phases. Its
receipt must report clean candidate source, the exact harness/image, passed
phases, restart validation and cleanup. External-endpoint conformance mode cannot
satisfy this release gate. This is a source/adapter gate using Cargo tests, not
execution of the selected packaged binary. The fixture is not distributed with
Ledgence and does not establish AWS qualification. Candidate evidence is retained
for 30 days.


Each successful job retains its verified archive, `SHA256SUMS` and provenance as
an Actions artifact for 30 days. Artifact names distinguish the headless targets
from `candidate-console-linux-x86_64-<job-attempt>`. Bundle, profile, upgrade and
S3 evidence names include the producing job attempt so reruns preserve distinct
uploads. These are reviewable candidates; the
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

- `bin/ledgence`, the unified CLI for program, worker, orchestrator, task, workflow progress, artifacts, approval, MCP and local-stack commands;
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

Before publishing any version, review the full change from the previous stable
tag to the exact candidate commit. For each promised capability, trace its
public entry point through the implementation and exercise its failure paths.
Check the upgrade from the previous release, documentation, supported targets
and distribution artifacts. A test fixture that implements the policy itself
establishes fixture behavior only.

Record the stable tag, candidate SHA, findings and supporting test and GitHub
job results. Hold publication for incomplete scope, unresolved blocking defects
or missing, pending, cancelled or failed required checks. Explain any jobs that
are inapplicable. If the candidate changes, review the updated diff and qualify
the new commit. Once the authorized candidate passes, proceed with promotion
and verify each published artifact using the procedures below.

Run and preserve the repository quality gates for the exact candidate source:
formatting, crate boundaries, warnings, Rust unit/doc tests, feature isolation,
Python helper, installed-client and installed-worker-package gates, current dependency policy, real
PostgreSQL/HTTP/workflow/completion acceptance, both queue modes, deployment
restart qualification and representative load/soak scenarios. Packaging evidence
covers only the gates the packaging tool actually executes; it is not a
substitute for the integration and capacity reports.

For the development adapters, also run the installed-framework and real-worker
qualification with each selected candidate's bundled helper. The three optional
SDK profiles have separate outcomes; no SDK may be silently omitted. Preserve
full/partial replay, committed-but-lost acknowledgement, unaccepted crash,
handled-failure cleanup, warm reuse, nested-control rejection and dependency-free
base-package evidence. Live model providers and a separately installed Claude CLI
remain unexercised by the offline gates and must not be presented as qualified.
See the [framework qualification procedure](../tools/framework_integrations/README.md).

The framework jobs select the Console archive by the producing job's immutable
artifact ID when `include_console=true`; only an explicitly headless partial
candidate uses the headless archive. Each
`candidate-framework-<framework>-<source-sha>-<job-attempt>` upload contains
`qualification.json`, `sdk-summary.json` and `worker-summary.json`. The receipt
binds the actual archive digest, provenance and inventory digests, executable,
recursive helper inventory, source commit, package version and static-asset mode.
SDK results must include positive test counts with no skips, failures, errors or
forbidden attempts; real-worker evidence must contain every required case and
successful cleanup (9 Claude cases, 11 each for PydanticAI and ADK). Logs and a
successful shell exit do not replace these structured receipts.

The expanded package profile also requires actual mixed-version checks: a legacy
client publishes a small package to the candidate server; the candidate client
publishes a small package to the prior server; an oversized-for-legacy package is
rejected before PUT; the candidate publishes, registers and executes an expanded
package; the prior worker rejects it before execution. Exercise cold cache, warm
reuse and cache reopening. Use `tools/check-program-profiles.py` with the exact
candidate and verified prior binaries/helpers, and retain its cleanup and
executable-hash evidence. Small fixture archives alone do not qualify full SDK
application packages or another platform's wheels.

For 0.10, the Linux Console candidate gate verifies the published 0.9.0 archive
against its pinned SHA256, boots that version on a disposable database, and passes
the packaged candidate binary, Console and helper to the artifact upgrade
rehearsal. The 0.8.0 resource rehearsal and separate 0.6.1/0.7.0 approval rehearsals
remain required. Verify a successful receipt for every predecessor against its
executable and the selected candidate executable. A missing receipt blocks the
candidate even if its shell exited successfully. Preserve external provider
request counts across restore, retained source discovery, denied budget authority,
credential checks and artifact-store restoration evidence. Qualify the macOS
bundle against the same latest stable predecessor; Linux results do not establish
another target's native execution.

Compare the previous optimized release and the candidate on ordinary tasks and workflows. Separately measure enabled admission with a hot resource, independent resources, parked work, and late-arriving interactive traffic. Use the worker's default acquisition wait for the primary measurements. Repeat the matched timing trials with an explicit zero wait and report those results separately. For the independent-resource trials, record both the available worker slots and the combined reservation limits. Count successful business outcomes and independently observed calls. Record the hardware, arrival schedule, SQL and WAL collection boundaries, and measured percentile changes. Keep latency measurements separate from proxy diagnostics and do not extrapolate local measurements to fleet throughput. [Governed load procedure and measurement boundaries](resource-load-qualification.md).

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
For 0.10.0, that workflow must qualify and publish both `ledgence-client` and
`ledgence-worker` under the `pypi` environment, then check their downloaded bytes
and fresh registry installation together. Keep each Python project's own trusted
publisher configured; the exact identity and initial setup procedure are
documented in the registry guide. The standalone worker wheel remains separate from the native
bundle's dedicated `runtime/ledgence/worker/` helper; publishing an authoring
package does not replace that runtime layout.
Required legal notices do not require users to open-source their applications. See [dependency policy](dependencies.md) and
[local image distribution boundaries](local-deployment.md#qualification-and-distribution-boundary).

For 0.10.0, also retain program build/publication acceptance from the actual
selected container image and matching CLI. It must prepare the native-dependency
example for each qualified image architecture, verify immutable repetition and
receipt recovery, execute it in the worker, and check persistence and execution
after restart. Fake builders and standalone package tests do not establish this
runtime compatibility. A complete native bundle must include that release's
verified local kit; installing it must not rewrite an existing saved installation.

After all release distribution channels are verified, update the public installation
commands, package matrix, release notes and current-release descriptions together.
Refresh the verified release features, including `programScaffolding`, in
`docs-site/source-features.json` with the verified source tag and actual
verification date, and update `docs-site/release.json` only to
artifacts that really exist. Explain how existing operators enable the explicit
filesystem writer or create a separate local installation; do not imply that a
CLI update upgrades saved kits. Preserve historical release notes and
version-pinned example corpora. Run documentation tests and the full build before
deploying the site, and report GitHub publication and website deployment as
separate outcomes.

## Prepare a stable bundle offline

After selecting a candidate whose required qualification gates have passed, use
`tools/release/promote.py` to prepare a stable archive from its exact bytes. This
operation does not rebuild binaries or Python distributions, change Git, create a
tag, access a registry, or publish anything. The version must already match the
Rust workspace and both Python distribution versions in the candidate's original source.

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
  --version 0.10.0 --output /tmp/ledgence-stable
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
The manual `promote-bundle.yml` workflow downloads the selected
`candidate-console-linux-x86_64-<job-attempt>` from an explicit Actions run in this repository
and requires the selected archive's expected SHA256. Dispatch it on the matching
annotated release tag after that commit is included in `main`:

```sh
gh workflow run promote-bundle.yml --ref v0.10.0 \
  -f candidate_run=RUN_ID \
  -f candidate_sha256=EXPECTED_64_CHARACTER_CANDIDATE_SHA256 \
  -f version=0.10.0
```

It checks the clean tag identity and source-equivalent candidate, promotes the
existing bytes without rebuilding, and runs relocated execution with CPython
3.14 and disposable PostgreSQL 18.6. Only after promotion and verification pass
does it retain `stable-linux-x86_64` with the stable archive, checksums and
provenance for 30 days. It does not create tags or publish releases or packages.
The candidate's integration and capacity qualification must already be complete.

Before promotion, `tools/release/candidate_preflight.py` snapshots the successful
candidate run's paginated job and artifact metadata. It requires every applicable
job and the Console upgrade/profile/S3 steps, then freezes immutable artifact IDs
using each successful job's own attempt. It downloads the selected archive and
all three framework receipts separately and verifies their complete byte
bindings. A framework-only rerun can reuse earlier receipts only when they bind
the exact selected archive; a new binary or differently packaged archive rejects
stale receipts even at the same source SHA. Different upload IDs are allowed only
for a byte-identical complete archive, with both IDs retained in the evidence.

Immediately before `promote.py`, the workflow rechecks the frozen run, jobs and
artifacts. A new rerun or changed selection fails preflight and requires a fresh
snapshot; it cannot silently fall back to an artifact with a matching name.
These checks retain the source-equivalent ancestor/tree release-merge rule above.

Run the promotion regression tests with
`python3 -m unittest discover -s tools/release -p 'test_*.py' -v`.
