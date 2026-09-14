# Dependency policy

Ledgence-owned code is MIT-licensed. Applications and programs that use Ledgence may remain proprietary, including commercial and hosted uses. MIT does not require publishing their source or modifications. Copyright and permission notices must remain with copies or substantial portions of MIT-covered software; third-party components retain their own terms. See the [MIT license](https://opensource.org/license/mit).

Dependency choices must preserve this product model: no required commercial service, license key, product branding, advertising credit, or disclosure of users' application/program source. Required legal notices may accompany source and binary distributions in notice files. A license scan is a selection gate, not a replacement for fulfilling the selected licenses.

## Adding or updating a dependency

Review the dependency's purpose, maintained release, enabled features, target platforms, source, license files, and resulting transitive dependencies. Prefer a focused library over a framework that takes over execution or deployment. Keep vendor-specific clients behind adapters. The worker does not embed Python through PyO3 or depend on Ray.

The workspace lockfile records the selected crate versions. Commit it and review its changes with the manifests. CI uses `--locked`. Prefer crates.io releases; Git sources need an explicit reviewed source allowance and a commit revision. Do not silently broaden an allowance or suppress an advisory to make CI pass.

The required Rust policy tool is `cargo-deny 0.20.2`:

```sh
cargo install cargo-deny --version 0.20.2 --locked
cargo deny --locked check
```

The policy checks all workspace roots, enabled features, and normal, build, development, and transitive dependencies. Unpublished workspace crates and development dependencies are not excluded. All resolved target dependencies are examined; checking a target's metadata is not a claim that it has been built or tested. [Graph configuration](https://embarkstudios.github.io/cargo-deny/checks/cfg.html), [license configuration](https://embarkstudios.github.io/cargo-deny/checks/licenses/cfg.html).

## License review

The initial general allowlist is small:

| SPDX identifier | Why it is needed | Distribution handling |
| --- | --- | --- |
| MIT | Ledgence and libraries including Tokio and tracing | Retain applicable copyright and permission notices. |
| Apache-2.0 | An available license for several Rust libraries and required portions of TLS dependencies | Retain the license, applicable attribution/NOTICE material, and required change notices. |
| ISC | TLS dependencies | Retain applicable copyright and permission notices. |
| BSD-3-Clause | Portions of the TLS implementation | Preserve required notices; do not imply endorsement by the copyright holders. |
| Unicode-3.0 | Unicode identifier/data dependencies | Include the required copyright and permission notice with the material or its documentation. |

These are permissive terms compatible with the intended application model when their conditions are met. They are not identical licenses. For an `OR` expression, an allowed alternative may satisfy the choice; an `AND` expression requires all applicable terms. An SPDX exception such as `Apache-2.0 WITH LLVM-exception` is a distinct expression and must be reviewed before adding a narrow crate/version exception. Unknown or unapproved expressions fail the gate. License clarifications must be based on the actual files and pinned file hashes, not an inferred label.

A narrowly scoped exception allows `CDLA-Permissive-2.0` for `webpki-root-certs` version `1.0.9`. This package supplies root-certificate data through the TLS verifier's WebAssembly target path in the all-target graph; it is not required by the currently tested Linux/macOS builds. Section 2.1 requires making the agreement text available with shared data, including modified data. It does not require publishing application source or displaying product branding. Keep the package's full license text with distributions that include this data. This review does not add CDLA to the general allowlist or approve later versions automatically. See the [CDLA-Permissive-2.0 agreement](https://cdla.dev/permissive-2-0/).

Primary license texts: [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0), [ISC](https://opensource.org/license/isc), [BSD-3-Clause](https://opensource.org/license/bsd-3-clause), [Unicode-3.0](https://www.unicode.org/license.txt).

A separate, version-specific exception allows `Zlib` for `foldhash 0.2.0`, used by SQLx through hashbrown/hashlink. Its actual published license permits commercial use, modification, and redistribution. It requires preserving the notice in source distributions, marking altered source, and not misrepresenting the original authorship; product-documentation acknowledgment is optional. It requires neither publishing application source nor product branding. The unmodified notice is retained in [legal/third-party/foldhash-0.2.0-LICENSE](../legal/third-party/foldhash-0.2.0-LICENSE). This does not allow Zlib generally or approve other foldhash versions. [Published foldhash license](https://docs.rs/crate/foldhash/0.2.0/source/LICENSE).

Copyleft licenses remain open-source licenses, but they are outside this initial allowlist. A dependency with additional source-disclosure, replacement/relinking, or other obligations requires a specific compatibility review. A source-available commercial restriction does not satisfy the dependency policy. Do not describe an unapproved component as permitted just because the project itself uses MIT.

## Security and source checks

Vulnerability, unsoundness, notice, and unmaintained advisories fail the dependency check. Yanked releases are denied. The initial policy contains no advisory exceptions. CI fetches the current RustSec database; an offline local run must use a database no older than seven days. Failure to fetch fresh advisory data is not a passing security result. Duplicate crate versions are reported for review rather than automatically treated as vulnerabilities. Wildcard version requirements and unapproved source registries/Git repositories are denied. [Advisory controls](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html), [source controls](https://embarkstudios.github.io/cargo-deny/checks/sources/cfg.html).

A clean advisory result means no matching published advisory was found in the database checked. It does not establish that dependencies are vulnerability-free. Source review and tests remain necessary, especially for archive handling, process supervision, and network input.

## Python and release contents

The Python runner uses the standard library and executes against a host-provided CPython interpreter (3.11 or newer, with 3.11–3.14 configured in CI). Ledgence does not bundle or embed that interpreter in this foundation. Python's own license and incorporated components remain separate from Ledgence's MIT license. Bundling Python in a future distribution requires reviewing and carrying the notices for the particular interpreter build. See [Python 3.12 licensing](https://docs.python.org/3.12/license.html).

Uploaded application packages have their own dependency and licensing responsibilities. Executing a program does not relicense it under MIT, and a successful Rust dependency check does not audit the program's dependencies.

Before publishing a binary, SDK, source bundle, or container image, inventory the actual included components and ship their required license/notice files alongside Ledgence's license. Include bundled native code, copied/generated material, and runtime components that Cargo metadata does not cover. Preserve third-party notices in vendored source. Review the Rust standard-library inventory for the toolchain used, rather than assuming the final executable contains only crates from `Cargo.lock`. See [Rust's copyright inventory](https://github.com/rust-lang/rust/blob/main/COPYRIGHT).

## Continuous integration

CI runs formatting, Clippy with warnings denied, Rust tests, documentation checks, and the Python runner tests on Linux and macOS with CPython 3.11–3.14. A configured matrix is not evidence of a completed hosted run. Dependency policy runs separately against the committed graph. CI performs no deployment or publishing. GitHub Actions are pinned to reviewed full commit IDs and run with read-only repository permissions. These checks can also run locally; GitHub is a CI implementation, not a runtime requirement.

The matrix checks the HTTP adapter's no-feature, client-only, and server-only builds separately. The dedicated PostgreSQL job runs real database tests and then separate-process HTTP acceptance. This preserves transport feature isolation while keeping database-dependent checks explicit.

The hardening changes reuse the already reviewed MIT-licensed `nix 0.31.3` for Unix nonblocking/no-follow file opens and signal tests; no new package or license exception is added. The `serde_json` round-trip float parser is enabled within the existing dependency.

The delivery-contract feature reuses `time 0.3.55` with its `formatting` feature to generate RFC 3339 event timestamps from supplied authoritative times. This enables its standard-library/allocation path; it adds no external package version or license exception. Its existing MIT/Apache-2.0 license choice remains applicable. Local time accounting uses standard-library `Instant`. PostgreSQL, HTTP server, gRPC, and OpenTelemetry dependencies are not introduced by this contract feature. [Pinned time feature documentation](https://docs.rs/time/0.3.55/time/#feature-flags).

## PostgreSQL adapter and developer verification

The PostgreSQL adapter uses SQLx `0.9.0` (MIT OR Apache-2.0, declared Rust minimum `1.94.0`) with defaults disabled and only `postgres`, `runtime-tokio`, `tls-rustls-ring-native-roots`, `migrate`, and `macros`. SQLx remains confined to `ledgence-adapter-postgres`; the crate-boundary check rejects direct SQLx dependencies elsewhere, including test/build declarations and renamed dependencies. It also rejects enabling SQLx defaults or unreviewed features. The service and core use Ledgence contracts instead of database-client types. [SQLx release manifest](https://github.com/transact-rs/sqlx/blob/v0.9.0/Cargo.toml).

The native-root TLS feature uses Ring and the platform certificate store. This activates Ring alongside the existing HTTPS artifact adapter's AWS-LC path; select features and review both implementations when updating the graph. MySQL, SQLite, alternative async runtimes, and application JSON/type integrations are not selected. Lockfiles and Cargo metadata can include optional packages that are not in the activated build tree; use `cargo tree --workspace --all-features --target all --edges normal,build,dev` to inspect selection and the complete dependency-policy gate for acceptance. [SQLx TLS features](https://docs.rs/sqlx/0.9.0/sqlx/#tls-support).

SQLx CLI is not required or installed. Its `0.9.0` published lockfile with minimal `postgres,rustls` features failed the existing advisory rules: unmaintained `backoff` and `instant`, older `anyhow` and `event-listener` releases with unsoundness advisories, and a yanked `chacha20` release. Updating that tool's lockfile would not remove its unmaintained dependencies. No advisory exception was added. [Backoff advisory](https://rustsec.org/advisories/RUSTSEC-2025-0012), [instant advisory](https://rustsec.org/advisories/RUSTSEC-2024-0384), [anyhow advisory](https://rustsec.org/advisories/RUSTSEC-2026-0190), [event-listener advisory](https://rustsec.org/advisories/RUSTSEC-2026-0221).

Ordinary builds use committed `.sqlx` query metadata with `SQLX_OFFLINE=true`. The verification tool uses SQLx `0.9.0`'s documented `SQLX_OFFLINE_DIR` mechanism: an online macro expansion describes the queries against PostgreSQL and writes their descriptions to a temporary directory. It removes the adapter's cached build first, checks every adapter target, and compares both the filename set and JSON contents with `.sqlx`. Missing, stale, or changed descriptions fail. The tool never rewrites committed metadata. [SQLx offline generation documentation](https://github.com/transact-rs/sqlx/blob/v0.9.0/FAQ.md), [versioned macro implementation](https://github.com/transact-rs/sqlx/blob/v0.9.0/sqlx-macros-core/src/query/mod.rs).

For local verification, provide Cargo, Python 3, Docker, a `psql` executable, and a dedicated **empty disposable PostgreSQL 18 database** in a test container. Its role must also be able to create databases for the isolated integration tests. The crash-recovery test deliberately kills and restarts the container identified by `LEDGENCE_POSTGRES_CONTAINER`; all databases in that container must be disposable test databases. For example, after starting the test container and creating the disposable database:

```sh
export LEDGENCE_POSTGRES_URL='postgres://postgres:ledgence-test@127.0.0.1:5432/ledgence_check'
export LEDGENCE_POSTGRES_CONTAINER='ledgence-postgres-check'
python3 tools/check-postgres.py
# If psql is outside PATH:
python3 tools/check-postgres.py --psql /path/to/psql
```

The script applies the sorted migration SQL inside one transaction to create a scratch schema for query checking. It rejects a populated database and leaves the schema in place; recreate the disposable database for another run. This scratch setup does not exercise or replace SQLx's migration bookkeeping. Applications use the public `PostgresStore::migrate` operation. For a separate application database selected by `LEDGENCE_POSTGRES_URL`, `cargo run -p ledgence-adapter-postgres --example migrate --locked` invokes that operation explicitly. The selected database tests exercise the actual SQLx migrator on their own empty databases, including migration checksums. After checking query metadata, the script requires at least one ignored database test and runs the adapter tests with `--ignored --test-threads=1`. Ordinary `cargo test` does not silently claim to have run those database tests.

The dedicated Linux CI job uses Docker's PostgreSQL `18.6` image pinned to the multi-platform index digest `sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`, verified on 2026-09-13. It runs the matching `psql` inside that service container, avoiding a separate client install. The existing Linux/macOS Rust and Python matrix builds offline; the PostgreSQL service job runs separately on Linux. This is a configured CI matrix, not a claim that every hosted job has run. Local macOS verification can connect to the same image through Docker. [PostgreSQL 18.6 release](https://www.postgresql.org/docs/release/18.6/), [Docker image](https://hub.docker.com/_/postgres).

## HTTP orchestration adapter

The HTTP milestone adds five external package versions. Their actual published notice files are retained under `legal/third-party`; the existing policy allowlist needs no broadening for them.

| Package | Declared license | Retained notice files |
| --- | --- | --- |
| `axum 0.8.9` | MIT | [LICENSE](../legal/third-party/axum-0.8.9-LICENSE) |
| `axum-core 0.5.6` | MIT | [LICENSE](../legal/third-party/axum-core-0.5.6-LICENSE) |
| `httpdate 1.0.3` | MIT OR Apache-2.0 | [MIT license](../legal/third-party/httpdate-1.0.3-LICENSE-MIT) |
| `matchit 0.8.4` | MIT AND BSD-3-Clause | [MIT license](../legal/third-party/matchit-0.8.4-LICENSE), [httprouter BSD notice](../legal/third-party/matchit-0.8.4-LICENSE.httprouter) |
| `mime 0.3.17` | MIT OR Apache-2.0 | [MIT license](../legal/third-party/mime-0.3.17-LICENSE-MIT) |

For `httpdate` and `mime`, these files retain the MIT alternative. Both licenses apply to the incorporated portions of `matchit`; its BSD notice must not be omitted. These terms do not require application source disclosure or product branding. The table is the incremental review for this milestone, not a complete release notice inventory.

Axum `0.8.9` disables defaults and enables only `http1` and `tokio`. The `ledgence-adapter-http` server feature activates Axum and `http-body-util 0.1.5` (already present in the dependency graph). Its client feature activates the existing `reqwest 0.13.4`, with defaults disabled and `rustls`; it reuses the reviewed TLS stack. No new TLS implementation, hosted service, or gRPC dependency is introduced. The adapter's empty default features require neither HTTP side. The public `TaskService` interface remains independent of Axum, reqwest, and SQLx types.

`tools/check-http-features.py` inspects each selected production/build tree and runs Clippy with warnings denied for no features, client only, and server only. Client-only trees reject Axum; server-only trees reject reqwest and TLS implementations; every selection rejects SQLx. This complements the full all-feature dependency-policy scan and crate-boundary check rather than replacing either one. Development tests may intentionally compose both HTTP sides and PostgreSQL. Orchestrator nonblocking logging reuses the existing `nix` version and features; it introduces no new package version or license exception.

## Optional Rust telemetry adapter

`ledgence-adapter-otel` confines the OpenTelemetry API/SDK, tracing bridge, and OTLP HTTP/protobuf exporter. Platform executables select the adapter through the `otel` feature. It is enabled in ordinary builds; `--no-default-features` preserves the tracing-disabled build. Runtime export remains off unless an explicit traces endpoint is configured. No provider or exporter SDK type appears in portable execution or orchestration DTOs.

All direct telemetry dependencies disable their default features: `opentelemetry =0.32.0` (`trace`), `opentelemetry_sdk =0.32.1` (`trace`), `tracing-opentelemetry =0.33.0` (no features), and `opentelemetry-otlp =0.32.0` (`trace,http-proto,reqwest-blocking-client,internal-logs`). The adapter reuses `reqwest =0.13.4` with `blocking,rustls`. Its bounded HTTP client also uses the already selected `opentelemetry-http =0.32.0` and `async-trait =0.1.92`. The ordinary SDK batch processor owns the blocking export thread; no experimental async processor, retry feature, tonic transport, logs exporter, or metrics provider is configured. Upstream `http-proto` activates generated metric message types internally; this is not metric export. `prost =0.14.4` and `opentelemetry-proto =0.32.0` decode bounded collector acknowledgements and actual OTLP requests in the capture fixture. [OTLP manifest](https://docs.rs/crate/opentelemetry-otlp/0.32.0/source/Cargo.toml), [SDK batch processor](https://docs.rs/crate/opentelemetry_sdk/0.32.1/source/src/trace/span_processor.rs).

The resolved graph adds 23 package versions. All have retained legal material listed, with source and SHA256, in [the notice inventory](../legal/third-party/observability-notice-inventory.json). OpenTelemetry crates that omit a standalone root license in the published archive use the license from the exact upstream revision in their published `.cargo_vcs_info.json`. This incremental inventory does not replace release-specific inventory of native, toolchain, and image contents.

| Packages | Selected permissive license |
| --- | --- |
| anyhow 1.0.104; async-trait 0.1.92; futures-macro 0.3.34; getrandom 0.3.4; itertools 0.14.0 | MIT or Apache-2.0 |
| opentelemetry, opentelemetry-http, opentelemetry-otlp, opentelemetry-proto 0.32.0; opentelemetry_sdk 0.32.1 | Apache-2.0 |
| portable-atomic 1.15.0; ppv-lite86 0.2.21 | MIT or Apache-2.0 |
| prost, prost-derive 0.14.4 | Apache-2.0 |
| r-efi 5.3.0; rand 0.9.5; rand_chacha 0.9.0; rand_core 0.9.5 | MIT or Apache-2.0 |
| tracing-opentelemetry 0.33.0 | MIT |
| wasip2 1.0.4+wasi-0.2.12; wit-bindgen 0.57.1; zerocopy 0.8.57; zerocopy-derive 0.8.57 | MIT or Apache-2.0 |

The additional alternatives in some declared OR expressions do not require selecting LGPL, BSD-2-Clause, or Apache's LLVM exception. Retain the chosen license and applicable notices. These components remain third-party code under their respective terms; they are not relicensed to MIT. No general allowlist or license exception is broadened by this feature. Rust OpenTelemetry is Beta upstream; the versions are pinned behind the adapter and upgrades require renewed compatibility and dependency review. [Rust status](https://opentelemetry.io/docs/languages/rust/).


## Python task client

The separate `ledgence-client` distribution provides `ledgence.client`. Its required transport is pinned to `aiohttp==3.14.3` without extras. The standard-library-only `ledgence_worker` helper does not acquire this dependency. Optional tracing imports `opentelemetry-api==1.44.0`; providers and exporters remain application choices.

The reviewed runtime pins aiohappyeyeballs 2.7.1, aiosignal 1.4.0, attrs 26.1.0, frozenlist 1.8.0, idna 3.19, multidict 6.8.0, propcache 0.5.2, yarl 1.24.5, and typing_extensions 4.16.0 where interpreter markers require it. Runtime closure is ten packages on Python 3.11–3.12 and nine on 3.13–3.14. The optional OTel graph consists of the API plus typing_extensions on every supported interpreter. The build graph is flit_core 3.12.0; tests add no third-party framework beyond the separately locked runtime. The checker invokes the PEP 517 backend directly, so no extra build frontend is installed.

Python package approval is separate from Cargo's license allowlist. PSF-2.0 is approved specifically for aiohappyeyeballs 2.7.1 and typing_extensions 4.16.0, including their full supplied license/history texts. Other selected terms are MIT, Apache-2.0 and BSD-3-Clause. Aiohttp includes llhttp under MIT; flit_core includes tomli 1.2.3 under MIT alongside Flit's BSD-3-Clause terms. The inspected terms require applicable legal notices and, for distributed modifications, change notices or summaries; they do not require application source disclosure, product branding or promotional credit. These components keep their actual licenses. No general copyleft or source-obligation allowance is introduced.

[The client legal inventory](../sdk/python-client/third_party/inventory.json) records 47 actual wheel artifacts and 12 source archives, their published URLs and SHA256 hashes, wheel requirements and interpreter markers, native-extension filenames, and exact legal-file hashes. [Third-party notices](../sdk/python-client/third_party/NOTICE.md) identify the separate graphs and retained material, including llhttp, the full PSF histories, and the propcache/yarl NOTICE files. Client wheels and source distributions include the retained license/NOTICE texts; source distributions also include the inventory and graph locks. This is the client dependency inventory, not an interpreter, operating-system, or complete release-image SBOM.

The configured matrix is CPython 3.11–3.14 on Linux x86_64/glibc (`ubuntu-24.04`) and macOS arm64 (`macos-14`). Each target has an explicitly inspected wheel selection. Other architectures, musl, Windows, PyPy, free-threaded interpreters, dependency extras and building third-party source archives are outside this review. Source archives are inspected for provenance and embedded legal material, never built by these gates. Artifact inspection for another target is not evidence of execution on that target; local and hosted CI results must be recorded separately.

Run the deterministic policy and installed-wheel gates with a supported host interpreter:

```sh
python tools/check-python-client-dependencies.py
python tools/check-python-client-dependencies.py --download /tmp/ledgence-client-wheels
python tools/check-python-client.py --wheelhouse /tmp/ledgence-client-wheels --offline
```

The policy rejects changed package/version/license selections, missing active transitive dependencies, invalid interpreter markers, requirement/lock drift, and missing or changed retained legal files. Download mode additionally verifies actual artifact hashes, metadata, native-file inventory and complete license/NOTICE bytes. The four graph lockfiles require hashes and only reviewed binary wheels; the isolated build gate installs from a target wheelhouse with `--no-index`, checks the installed closure, rebuilds the wheel from the sdist, verifies legal contents, and runs tests outside checkout before and after adding the optional API. CPython's supplied `ensurepip` bootstrap tooling is not a client runtime/build dependency and is not bundled in the wheel. This deterministic selection/notice gate does not claim a vulnerability scan.

Dependency upgrades require renewed artifact, source, license, coupling, marker and target review, followed by deliberate updates to the version-specific policy, inventory, graph locks and relevant manifests. Updating a version range or a lockfile alone cannot pass the gate. The published client metadata pins the complete reviewed runtime and optional graphs, including interpreter markers, so ordinary installation also selects the reviewed versions. The checked-in locks and wheelhouse additionally identify the exact artifact bytes validated for this release.
