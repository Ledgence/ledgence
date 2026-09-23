# Documentation dependency review

Reviewed 2026-09-22 against `package-lock.json`: 374 locked packages, including optional platform variants. The initial macOS ARM64 installation contained 271 packages. Package totals can differ across platforms; the lockfile and generated manifest are authoritative.

## Purpose and boundaries

This is an independently built static documentation site. It adds no dependency or vendor SDK type to the Rust platform or Python SDK. It needs no vendor account, subscription, hosted search, or proprietary service. `dist/` is the deployable artifact; do not deploy `node_modules/`, build tools, or a Node server.

| Direct dependency | Pinned version | License | Use and source |
| --- | --- | --- | --- |
| Astro | 7.3.3 | MIT | Static page generation; [source](https://github.com/withastro/astro). Some browser behavior is included in generated pages. |
| Starlight | 0.42.2 | MIT | Documentation navigation, search integration, and accessible browser components; [source](https://github.com/withastro/starlight). |
| Inter via Fontsource | 5.3.0 | OFL-1.1 | Unmodified, locally hosted font files; [font source](https://github.com/rsms/inter), [packaging source](https://github.com/fontsource/fontsource). |

Ledgence-owned code remains MIT. Third-party files retain their own licenses. Inter's OFL applies to the font, including any font modifications, and does not relicense documents or Ledgence code. Do not sell the font alone or rename a modified font contrary to reserved-name requirements. Keep the original copyright and OFL text with deployed font files.

## Transitive review

The build requires Node.js 22.19 or newer (Node.js 24 is recommended). Undici, used by Astro’s build-time font tooling, is pinned to MIT-licensed 8.10.2 in both npm and pnpm overrides. Version 8.11.0 was less than 24 hours old when this lock was created: npm selected it, while pnpm 11’s default release-age check selected 8.10.2. The matched pin preserves that protection and makes imports reproducible. Keep both overrides synchronized when deliberately upgrading. npm deduplication also coalesces optional `@emnapi/core` and `@emnapi/wasi-threads` entries to their already reviewed compatible versions (1.11.1 and 1.2.2).

The locked JavaScript graph primarily uses MIT, ISC, Apache-2.0, BSD-2-Clause, BSD-3-Clause, BlueOak-1.0.0, and CC0-1.0. All installed packages provided public source repository metadata. The generated manifest preserves repository locations, registry archive URLs, integrity values, declared SPDX expressions, and original notice hashes. Bundled legal texts (including Astro, Vite, Rolldown, and Pagefind notices) are collected unchanged, rather than treating the package's top-level SPDX field as exhaustive.

Pagefind 1.5.2 supplies local search, including browser JavaScript and WebAssembly. Its default UI archive omits the repository license. `scripts/legal-overrides/` preserves the exact [v1.5.2 source license](https://github.com/Pagefind/pagefind/blob/bf17396721be637cc67c8ed7ead1dc7b8ac43d96/LICENSE) for both the UI and generated search assets, with a pinned source URL and SHA-256. The separate CLI wrapper notices remain intact. Compiler and native binding archives without their own license file use the actual matching parent package's notices, only when version, license, and dependency relationship agree.

Additional licenses are accepted only for the exact package/version/expression entries in `reviewedToolLicenses` in `scripts/licenses.mjs`:

- `argparse@2.0.1`: Python-2.0. Preserve the full bundled Python license history and terms. The parser is unmodified documentation tooling; the terms do not require proprietary application source disclosure.
- `tslib@2.8.1`: 0BSD. A permissive license used by optional build-tool WebAssembly support. This review does not add 0BSD to the general allowlist.
- `lightningcss@1.33.0` and its matching native variants: MPL-2.0. This CSS transformation tool runs during the build. Its implementation is not part of the generated site or Ledgence product. MPL remains applicable to the tool's covered files; copying or distributing those files requires a new review. [MPL terms](https://www.mozilla.org/en-US/MPL/2.0/)
- `@img/sharp-libvips-* @1.3.3`, plus the locked Sharp Windows/WebAssembly variants at 0.35.4: LGPL-3.0-or-later or the original compound expression. These are image-processing tools, not website runtime dependencies. Every term in an `AND` expression is retained; none is treated as an alternative. The original libvips `README.md` license table and `versions.json` identify its bundled libraries, including LGPL, MPL, font, image-codec, and patent terms. [Matching source/build recipes](https://github.com/lovell/sharp-libvips/tree/6e5971d333377743163edc3ad9e5d0b897abcbc9)

These limited tooling uses do not place Ledgence applications or generated HTML/CSS/images under MPL or LGPL. This is not a blanket approval for those licenses, nor a legal bundle for redistributing the build tools. Shipping tool binaries, embedding their implementation in client assets, modifying them, or linking them into Ledgence requires reviewing source-offer, relinking, modification, and notice obligations before making that change. Only static site files are distributed here.

### Upstream declarations without full license text

Four unmodified CLI/build helpers omit standalone complete terms in both their npm archives and reviewed source trees:

| Exact helper | Upstream declaration | Reviewed source |
| --- | --- | --- |
| am-i-vibing 0.4.0 | MIT in package metadata and README; README retains Matt Kane's copyright | [Release source](https://github.com/ascorbic/am-i-vibing/tree/7e343f755e33876bf17a614af821e90005349a98) |
| process-ancestry 0.1.0 | MIT in package metadata and README; README identifies Matt Kane | [Release source](https://github.com/ascorbic/process-ancestry/tree/45004fd6155437eef40a632877a567db1b9addc5) |
| piccolore 0.1.3 | ISC in package metadata; README identifies its picocolors ancestry | [Release source](https://github.com/delucis/piccolore/tree/5af9fa6834292e3cb3a2bc778e4057517c1505e0) |
| boolbase 1.0.0 | ISC in package metadata | [Source repository](https://github.com/fb55/boolbase) |

Their original README and package metadata are retained as provenance and explicitly marked `upstream-declarations-only; tooling-not-distributed`. Metadata is not presented as a complete license text. No invented copyright or generic replacement license is attached. They are accepted only as local, unmodified build tools absent from the static output; redistribution requires resolving the missing complete notices first. The libvips bundle's original declarations are recorded with the same limitation.

No reviewed term requires Ledgence product branding, logos, promotional credits, or product renaming. Required legal attribution stays in accompanying notices. The website must preserve notices for browser components and fonts even though their npm packages are installed for the build; an npm `dev` flag is not evidence that all of a package's code is absent from deployed assets.

## Update and release gates

1. Pin direct versions and review the full npm lockfile change, source availability, bundled notices, and intended coupling. Do not broadly allow a new SPDX expression merely to make a build pass.
2. Install from the lockfile with `npm ci --ignore-scripts`. Review any lifecycle script before enabling it.
3. Run `npm run licenses` (or `node scripts/licenses.mjs --check` for a read-only audit), then the normal site build and checks. The collector fails on unreviewed licenses, missing required packages, mismatched installed metadata, missing legal text outside the exact reviewed cases, or altered vendored upstream notices.
4. Preserve `public/notices/` in `dist/notices/`, including `LEDGENCE-LICENSE.txt`, `manifest.json`, original package notices, and the pinned Pagefind license. Keep the notices link in the site footer.
5. Inspect the static output and deploy only `dist/`. A change that introduces a server, native build binaries, or any of the narrowly excepted build helpers into shipped assets invalidates this review and must be assessed before release.

Notice generation is offline after dependency installation. It never substitutes fetched generic license templates or requires a hosted compliance service. Optional packages for other platforms are listed in the manifest; actual installed notices are regenerated on the build platform.
