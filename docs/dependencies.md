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

The hardening changes reuse the already reviewed MIT-licensed `nix 0.31.3` for Unix nonblocking/no-follow file opens and signal tests; no new package or license exception is added. The `serde_json` round-trip float parser is enabled within the existing dependency.
