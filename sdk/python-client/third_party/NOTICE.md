# Python client third-party notices

Ledgence-owned client code is MIT licensed. Third-party packages keep the licenses listed below. This inventory does not relicense them to MIT. These packages are installed as separate distributions; their source code is not vendored into the client wheel. The included legal texts accompany the source distribution and wheel so distributions can retain the applicable notices.

The reviewed runtime is aiohttp 3.14.3 without extras. Optional OpenTelemetry imports only the API; applications select providers and exporters separately. The Python worker helper remains a separate standard-library-only package.

| Graph | Package | Version | Reviewed terms |
| --- | --- | --- | --- |
| Runtime | aiohttp, including embedded llhttp | 3.14.3 | Apache-2.0 AND MIT |
| Runtime | aiohappyeyeballs | 2.7.1 | PSF-2.0 |
| Runtime | aiosignal | 1.4.0 | Apache-2.0 |
| Runtime | attrs | 26.1.0 | MIT |
| Runtime | frozenlist | 1.8.0 | Apache-2.0 |
| Runtime | idna | 3.19 | BSD-3-Clause |
| Runtime | multidict | 6.8.0 | Apache-2.0 |
| Runtime | propcache | 0.5.2 | Apache-2.0 |
| Runtime on Python 3.11–3.12; optional OTel on all targets | typing_extensions | 4.16.0 | PSF-2.0 |
| Runtime | yarl | 1.24.5 | Apache-2.0 |
| Optional tracing | opentelemetry-api | 1.44.0 | Apache-2.0 |
| Build only | flit_core, including vendored tomli 1.2.3 | 3.12.0 | BSD-3-Clause AND MIT |
| Test only | Python unittest | Host interpreter | No additional third-party test package |

Full, unmodified license and NOTICE files are in `licenses/<package-version>/source/` and `licenses/<package-version>/wheel/`. This includes aiohttp's `vendor/llhttp/LICENSE`, Flit's vendored tomli license, the complete supplied PSF license/history material, and propcache/yarl NOTICE files. Apache attribution notices and BSD/MIT copyright notices remain in those files. The reviewed artifacts have not been modified by Ledgence. Any downstream modifications must retain applicable notices and mark changes or provide change summaries when the component's license requires it.

The source-distribution `inventory.json` records exact source URLs, SHA256 hashes, wheel metadata, retained legal-file hashes and native-extension filenames. It covers CPython 3.11–3.14 on Linux x86_64 (manylinux, glibc) and macOS arm64. Downloading and inspecting another target's wheel is not execution evidence for that target. Other architectures, musl, Windows, PyPy, free-threaded builds, aiohttp extras, and builds from third-party source archives are outside this reviewed matrix. No source archive is built by the project gates.

The pinned, separate graph locks are `runtime-requirements.txt`, `build-requirements.txt`, `test-requirements.txt`, and `otel-requirements.txt`. `tools/check-python-client-dependencies.py` verifies their complete active dependency closures, interpreter markers, exact selected versions and legal bytes. Use its downloaded target wheelhouse with `--require-hashes --only-binary=:all: --no-index --find-links WHEELHOUSE`; unreviewed wheels or source builds are not substitutes for a reviewed artifact.

The host CPython interpreter, its `ensurepip` bootstrap tooling, operating-system libraries and certificate store are supplied by the environment and are not bundled by ledgence-client. Their licenses remain applicable to distributors that include them. This inventory is not a whole-container or interpreter SBOM and is not a vulnerability scan.
