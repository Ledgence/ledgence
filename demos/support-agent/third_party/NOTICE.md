# Support-agent demo third-party notices

Ledgence-owned demo code is MIT licensed. These third-party distributions retain their own licenses; they are not relicensed to MIT. This optional Google ADK/Gemini example is separate from Ledgence's vendor-neutral core. No optional ADK extras are installed.

The two exact wheel locks cover CPython 3.13 (standard GIL build), macOS arm64 and Linux x86_64 with glibc 2.28 or newer. No source builds, musl, Windows, other Python versions or architectures are reviewed here. Linux wheel inspection is not Linux execution evidence. The host interpreter, pip, OS libraries, and system certificate store are separately supplied and are not bundled by this inventory.

| Distribution | Version | Terms |
| --- | --- | --- |
| aiosqlite | 0.22.1 | MIT |
| annotated-doc | 0.0.5 | MIT |
| annotated-types | 0.8.0 | MIT |
| anyio | 4.15.1 | MIT |
| attrs | 26.1.0 | MIT |
| authlib | 1.8.0 | BSD-3-Clause |
| certifi | 2026.7.22 | MPL-2.0 |
| cffi | 2.1.1 | MIT-0 |
| charset-normalizer | 3.5.1 | MIT |
| click | 8.5.0 | BSD-3-Clause |
| cryptography | 50.0.1 | Apache-2.0 OR BSD-3-Clause |
| distro | 1.9.0 | Apache License, Version 2.0 |
| fastapi | 0.141.1 | MIT |
| google-adk | 2.10.0 | Apache-2.0 |
| google-auth | 2.58.1 | Apache 2.0 |
| google-genai | 2.25.0 | Apache-2.0 |
| graphviz | 0.21 | MIT |
| h11 | 0.16.0 | MIT |
| httpcore | 1.0.9 | BSD-3-Clause |
| httpx | 0.28.1 | BSD-3-Clause |
| idna | 3.20 | BSD-3-Clause |
| joserfc | 1.7.5 | BSD-3-Clause |
| jsonschema | 4.26.0 | MIT |
| jsonschema-specifications | 2025.9.1 | MIT |
| opentelemetry-api | 1.42.1 | Apache-2.0 |
| opentelemetry-sdk | 1.42.1 | Apache-2.0 |
| opentelemetry-semantic-conventions | 0.63b1 | Apache-2.0 |
| packaging | 26.3 | Apache-2.0 OR BSD-2-Clause |
| pyasn1 | 0.6.4 | BSD-2-Clause |
| pyasn1-modules | 0.4.2 | BSD-2-Clause |
| pycparser | 3.0 | BSD-3-Clause |
| pydantic | 2.13.5 | MIT |
| pydantic-core | 2.46.5 | MIT |
| python-dotenv | 1.2.3 | BSD-3-Clause |
| python-multipart | 0.0.32 | Apache-2.0 |
| pyyaml | 6.0.3 | MIT |
| referencing | 0.37.0 | MIT |
| requests | 2.34.2 | Apache-2.0 |
| rpds-py | 2026.6.3 | MIT |
| sniffio | 1.3.1 | MIT OR Apache-2.0 |
| starlette | 1.7.0 | BSD-3-Clause |
| tenacity | 9.1.4 | Apache 2.0 |
| typing-extensions | 4.16.0 | PSF-2.0 |
| typing-inspection | 0.4.4 | MIT |
| urllib3 | 2.8.0 | MIT |
| uvicorn | 0.54.0 | BSD-3-Clause |
| watchdog | 6.0.0 | Apache-2.0 |
| websockets | 15.0.1 | BSD-3-Clause |

## Scoped certifi compatibility review

Unmodified certifi 2026.7.22 is the sole MPL-2.0 component in this optional demo. This is a component-specific compatibility review, not a permissive-license classification or an expansion of the core dependency allowlist. Its wheel contains Python source, the PEM certificate bundle, a type marker and metadata; there are no native or bytecode-only certifi components. All seven files under `certifi/` byte-match the exact PyPI source archive identified in `inventory.json`.

Recipients receive certifi's complete installed Python/data source under `certifi/`, its original notice under `certifi-2026.7.22.dist-info/licenses/LICENSE`, and the full MPL-2.0 text in `SUPPLEMENTAL-LICENSES.txt`. The certificate PEM itself is the distributed source form. The inventory also gives the corresponding source archive URL and SHA256. Thus source remains available in the same package, without requiring access to a vendor account or disclosing the surrounding application. MPL sections 3.1–3.4 apply to certifi's covered source and modifications to it; the larger work may use different terms. Users may keep their separate applications and Ledgence-owned modifications proprietary. Modifying certifi itself requires continuing to satisfy its MPL source and notice obligations. The build verifies unmodified upstream dependency files; modified dependencies require a new review.

## Native components and retained notices

The wheels' original dist-info license files and SBOMs remain installed. `SUPPLEMENTAL-LICENSES.txt` contains original additional source notices and native-component license material absent from those wheel notices. The repository retains individual legal files under `licenses/`; their hashes, original archive paths, exact source URLs and artifact hashes are recorded in `inventory.json`. The build packages the inventory, this notice and the single supplemental text to stay within the archive-entry limit.

Pydantic-core, rpds-py and cryptography include Rust components. Their reviewed source Cargo locks and wheel SBOMs identify the conservative union of 144 registry crate versions listed in `embedded_rust`. This superset includes build-only and other-target components. The selected terms use permissive alternatives where dual/triple licensing is offered (MIT, Apache-2.0, BSD, Zlib, Unicode data terms or Apache-2.0 with LLVM exception); GPL/LGPL alternatives are not selected. Original alternative-license texts may remain in upstream legal files without changing that selection. The wit-bindgen-rt crate omits its legal files, so its release repository's original legal files are retained.

Cryptography's wheel SBOM and embedded version identify statically linked OpenSSL 4.0.2 (Apache-2.0); its SBOM supplies the exact source SHA256. PyYAML's tagged build configuration identifies statically linked libyaml 0.2.5 (MIT). CFFI's tagged wheel build configuration identifies libffi 3.4.6 (MIT) on Linux; the macOS wheel links the system libffi instead. The supplemental inventory records those original source archives and legal files. Native files and wheel tags are recorded per artifact; required system libraries remain the host's responsibility. The graphviz Python package is included by ADK, but this demo does not render graphs and does not require the Graphviz executable.

`verify.py` validates exact wheel names/hashes, pinned metadata and dependency closure, retained legal bytes and installed upstream files. No source archive is built or executed by this gate. Locks accept only reviewed wheel artifacts. Keep all packaged notices and source intact when redistributing. This inventory is not a whole-container SBOM or a vulnerability scan.
