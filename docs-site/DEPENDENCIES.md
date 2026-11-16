# Documentation dependency review

Reviewed 2026-11-16 against `package-lock.json`: 362 locked packages, including optional platform variants. The validated macOS ARM64 installation contained 271 packages. Package totals can differ across platforms; the lockfile and generated manifest are authoritative.

## Purpose and boundaries

This is an independently built static documentation site. It adds no dependency or vendor SDK type to the Rust platform or Python SDK. It needs no vendor account, subscription, hosted search, or proprietary service. `dist/` is the deployable artifact; do not deploy `node_modules/`, build tools, or a Node server.

| Direct dependency | Pinned version | License | Use and source |
| --- | --- | --- | --- |
| Astro | 7.4.7 | MIT | Static page generation; [source](https://github.com/withastro/astro). Some browser behavior is included in generated pages. |
| Starlight | 0.42.2 | MIT | Documentation navigation, search integration, and accessible browser components; [source](https://github.com/withastro/starlight). |
| Inter via Fontsource | 5.3.0 | OFL-1.1 | Unmodified, locally hosted font files; [font source](https://github.com/rsms/inter), [packaging source](https://github.com/fontsource/fontsource). |

Ledgence-owned code remains MIT. Third-party files retain their own licenses. Inter's OFL applies to the font, including any font modifications, and does not relicense documents or Ledgence code. Do not sell the font alone or rename a modified font contrary to reserved-name requirements. Keep the original copyright and OFL text with deployed font files.

## Transitive review

The build requires Node.js 22.23.4 or newer (Node.js 24 is recommended). Both package managers pin Undici 8.12.2 for Astro's build-time font tooling and Zod 4.6.7 for schema validation. These versions fit their callers' declared dependency ranges and retain the MIT license. Undici fixes eight advisories affecting the 8.x series, including TLS option handling, redirect caching and resource consumption. Zod fixes resource exhaustion when validating oversized collections. [Undici release](https://github.com/nodejs/undici/releases/tag/v8.12.2), [Zod advisory](https://github.com/advisories/GHSA-mrhx-c7xc-m3w5).

Keep npm and pnpm overrides synchronized when upgrading. npm deduplication also coalesces optional `@emnapi/core` and `@emnapi/wasi-threads` entries to their already reviewed compatible versions (1.11.3 and 1.2.3).

The locked JavaScript graph primarily uses MIT, ISC, Apache-2.0, BSD-2-Clause, BSD-3-Clause, BlueOak-1.0.0, and CC0-1.0. All installed packages provided public source repository metadata. The generated manifest preserves repository locations, registry archive URLs, integrity values, declared SPDX expressions, and original notice hashes. `packagePath` retains the npm lock location; `installedPath` records the matching installed package, or `null` for an absent optional package. Archive integrity is lockfile provenance verified during package-manager installation, not a claim that the notice collector rehashes all installed package files. Bundled legal texts (including Astro, Vite, Rolldown, and Pagefind notices) are collected unchanged, rather than treating the package's top-level SPDX field as exhaustive.

Pagefind 1.5.2 supplies local search, including browser JavaScript and WebAssembly. Its default UI archive omits the repository license. `scripts/legal-overrides/` preserves the exact [v1.5.2 source license](https://github.com/Pagefind/pagefind/blob/bf17396721be637cc67c8ed7ead1dc7b8ac43d96/LICENSE) for both the UI and generated search assets, with a pinned source URL and SHA-256. The separate CLI wrapper notices remain intact. Compiler and native binding archives without their own license file use the actual matching parent package's notices, only when the nearest actual installed parent matches the locked version, license, and exact dependency relationship. A nearer incompatible parent is rejected rather than falling back to an unrelated copy.

Additional licenses are accepted only for the exact package/version/expression entries in `reviewedToolLicenses` in `scripts/licenses.mjs`:

- `argparse@2.0.1`: Python-2.0. Preserve the full bundled Python license history and terms. The parser is unmodified documentation tooling; the terms do not require proprietary application source disclosure.
- `tslib@2.8.1`: 0BSD. A permissive license used by optional build-tool WebAssembly support. This review does not add 0BSD to the general allowlist.
- `lightningcss@1.33.0` and its matching native variants: MPL-2.0. This CSS transformation tool runs during the build. Its implementation is not part of the generated site or Ledgence product. MPL remains applicable to the tool's covered files; copying or distributing those files requires a new review. [MPL terms](https://www.mozilla.org/en-US/MPL/2.0/)
- `@img/sharp-libvips-* @1.3.4`, plus the locked Sharp Windows/WebAssembly variants at 0.35.5: LGPL-3.0-or-later or the original compound expression. These are image-processing tools, not website runtime dependencies. Every term in an `AND` expression is retained; none is treated as an alternative. The original libvips `README.md` license table and `versions.json` identify its bundled libraries, including LGPL, MPL, font, image-codec, and patent terms. [Matching source/build recipes](https://github.com/lovell/sharp-libvips/tree/ebb95f8add54eee8bed840e3fb587e4cbec857d7)

These limited tooling uses do not place Ledgence applications or generated HTML/CSS/images under MPL or LGPL. This is not a blanket approval for those licenses, nor a legal bundle for redistributing the build tools. Shipping tool binaries, embedding their implementation in client assets, modifying them, or linking them into Ledgence requires reviewing source-offer, relinking, modification, and notice obligations before making that change. Only static site files are distributed here.

### 0.6.1 build dependency maintenance

The direct Astro, Starlight and font versions remain unchanged. Matching npm
and pnpm overrides select the following reviewed transitive versions:

| Package | Version | Reason and primary source |
| --- | --- | --- |
| http-cache-semantics | 4.3.0 | Refresh the cache-policy implementation beyond the affected `<=4.2.0` range: [GHSA-ch52-4w7c-c8xp](https://github.com/advisories/GHSA-ch52-4w7c-c8xp), [selected source](https://github.com/kornelski/http-cache-semantics/tree/b1d4bd682fbab0252985de45219f4e7497c0067c). |
| postcss-selector-parser | 7.1.6 | Fix quadratic selector parsing: [GHSA-rj75-hqrm-r3gf](https://github.com/advisories/GHSA-rj75-hqrm-r3gf). |
| Sharp / bundled libvips | 0.35.5 / 1.3.4 | The published bundle includes librsvg 2.63.2: [Sharp advisory](https://github.com/lovell/sharp/security/advisories/GHSA-wq5f-xc86-pv6w), [release](https://github.com/lovell/sharp/releases/tag/v0.35.5). |
| smol-toml | 1.9.1 | Fix quadratic key scanning, with the follow-up patch: [GHSA-r4xh-jqrq-34v2](https://github.com/advisories/GHSA-r4xh-jqrq-34v2), [release](https://github.com/squirrelchat/smol-toml/releases/tag/v1.9.1). |
| Vite | 8.3.4 | Includes corrected Windows editor argument escaping: [GHSA-698v-4r3x-3vcv](https://github.com/advisories/GHSA-698v-4r3x-3vcv), [release](https://github.com/vitejs/vite/releases/tag/v8.3.4). |

Expressive Code 0.44.2 still requests `postcss-nested ^6.0.1`, whose 6.2.0
release requests selector-parser 6. The override is scoped to that exact
`postcss-nested@6.2.0` parent and selects parser 7.1.6. This deliberately crosses
its declared parser major range; it does not upgrade or downgrade Starlight,
Expressive Code or nested. The parser's 7.0 change makes insertions during
iteration safe, so nested's selector mutation needs compatibility verification.
All 69 upstream [nested 6.2.0 fixtures](https://github.com/postcss/postcss-nested/blob/6.2.0/index.test.js)
passed against this selection, using Node's built-in test runner in a private
review copy, along with the documentation tests and full static build. Revisit
this scoped override when Expressive Code adopts a corrected parser normally.

Vite's compatible PostCSS, Nano ID, Rolldown and OXC type dependencies also move
within their existing permitted ranges. The lock retains the same 374 package
paths, including optional platform variants, and introduces no new dependency
names or license expressions. Changed JavaScript packages retain MIT,
BSD-2-Clause or BSD-3-Clause terms; Sharp retains Apache-2.0. Its native variants
retain their original compound expressions. The libvips component table and
version inventory remain build-tool provenance; the selected archive includes
librsvg 2.63.2 and retains the reviewed LGPL/MPL and other component terms.
The exact identities in `scripts/licenses.mjs` are refreshed without broadening
the general license allowlist. No tool binary is added to the static output.

### November 2026 Astro audit maintenance

Astro 7.4.7 is the smallest release that fixes both
[GHSA-mh32-jxcr-mh9f](https://github.com/withastro/astro/security/advisories/GHSA-mh32-jxcr-mh9f)
(fixed in 7.4.5) and
[GHSA-r3h5-4585-7x3w](https://github.com/withastro/astro/security/advisories/GHSA-r3h5-4585-7x3w)
(fixed in 7.4.7). The former concerns untrusted dynamic tag names; the latter
concerns the on-demand Node adapter image endpoint. This site remains static,
has no `@astrojs/node` adapter, and does not select tags from untrusted requests.
The upgrade satisfies the advisory gate without adding a server or changing the
Starlight UI version. [Astro release](https://github.com/withastro/astro/releases/tag/astro@7.4.7).

The synchronized npm and pnpm selections also pin Vite 8.3.5 and Zod 4.6.7,
which satisfy Astro's updated minimum ranges. MDX 8.1.3 remains within
Starlight 0.42.2's `^8.0.0` range and shares Astro's Markdown 0.4.7 and internal
helpers 0.13.1. Rolldown 1.2.16 satisfies both Astro's `^1.2.16` and Vite's
`~1.2.14`, avoiding duplicate bundlers. `es-module-lexer` 3.0.4 satisfies both
Astro and MDX's `^3.0.4`; its exact override keeps both package managers on
the same selection without exempting a newer version from pnpm's minimum-age
policy. These pins add no out-of-range override.
[Published MDX metadata](https://registry.npmjs.org/@astrojs/mdx/8.1.3),
[Vite metadata](https://registry.npmjs.org/vite/8.3.5).

Starlight still requires Satteri `^0.10.3`, while the updated Markdown package
requires `^0.11.6`. Both versions are retained. npm and pnpm can hoist those
valid versions into different directories, so notice collection resolves exact
locked name/version/license identities and rejects ambiguous archive origins,
unlocked installed packages, missing required identities, and linked package
directories. Native notices follow the actual installed owner. Neither version
nor license checks are relaxed to accommodate a different layout.

The final lock has 362 paths versus 374 before this change. Eighty newly
selected package/version identities carry MIT (79) or ISC (1); there are no new
license expressions. New names are `fnv1a-64`, `std-env`, `tinyps`, `yaml`, and
the Android ARM64 and FreeBSD x64 Satteri platform variants. Full original legal
texts for the installed new helpers are preserved. `am-i-vibing` 0.5.0 retains
the exact declaration-only limitation described below; its archive and release
source still omit complete license text. The existing narrow build-tool
exceptions and static-only distribution boundary remain unchanged.

This update was validated by comparing the complete npm and pnpm
package/archive identities. CI verifies pnpm 11.19.0 import determinism and
runs frozen installs, tests, notice collection, and full builds with both
managers. The npm audit gate remains at severity `low`.
Regression tests cover both Satteri layouts and reject missing, mismatched,
ambiguous, linked, and incorrectly owned notice inputs.

### Upstream declarations without full license text

Four unmodified CLI/build helpers omit standalone complete terms in both their npm archives and reviewed source trees:

| Exact helper | Upstream declaration | Reviewed source |
| --- | --- | --- |
| am-i-vibing 0.5.0 | MIT in package metadata and README; README retains Matt Kane's copyright | [Release source](https://github.com/ascorbic/am-i-vibing/tree/a858d9865e3f5a278504aad0a775a8b7a8d70bbc) |
| process-ancestry 0.1.0 | MIT in package metadata and README; README identifies Matt Kane | [Release source](https://github.com/ascorbic/process-ancestry/tree/45004fd6155437eef40a632877a567db1b9addc5) |
| piccolore 0.1.3 | ISC in package metadata; README identifies its picocolors ancestry | [Release source](https://github.com/delucis/piccolore/tree/5af9fa6834292e3cb3a2bc778e4057517c1505e0) |
| boolbase 1.0.0 | ISC in package metadata | [Source repository](https://github.com/fb55/boolbase) |

Their original README and package metadata are retained as provenance and explicitly marked `upstream-declarations-only; tooling-not-distributed`. Metadata is not presented as a complete license text. No invented copyright or generic replacement license is attached. They are accepted only as local, unmodified build tools absent from the static output; redistribution requires resolving the missing complete notices first. The libvips bundle's original declarations are recorded with the same limitation.

No reviewed term requires Ledgence product branding, logos, promotional credits, or product renaming. Required legal attribution stays in accompanying notices. The website must preserve notices for browser components and fonts even though their npm packages are installed for the build; an npm `dev` flag is not evidence that all of a package's code is absent from deployed assets.

## Update and release gates

1. Pin direct versions and review the full npm lockfile change, source availability, bundled notices, and intended coupling. Do not broadly allow a new SPDX expression merely to make a build pass.
2. Install from the lockfile with `npm ci --ignore-scripts` and run `npm audit --audit-level=low`. CI checks the npm advisory registry once in the npm job and verifies that importing the npm lock produces the committed pnpm lock in the pnpm job. Review any lifecycle script before enabling it.
3. Run `npm run licenses` (or `node scripts/licenses.mjs --check` for a read-only audit), then the normal site build and checks. The collector fails on unreviewed licenses, missing required packages, mismatched installed metadata, missing legal text outside the exact reviewed cases, or altered vendored upstream notices.
4. Preserve `public/notices/` in `dist/notices/`, including `LEDGENCE-LICENSE.txt`, `manifest.json`, original package notices, and the pinned Pagefind license. Keep the notices link in the site footer.
5. Inspect the static output and deploy only `dist/`. A change that introduces a server, native build binaries, or any of the narrowly excepted build helpers into shipped assets invalidates this review and must be assessed before release.

Notice generation is offline after dependency installation. It never substitutes fetched generic license templates or requires a hosted compliance service. Optional packages for other platforms are listed in the manifest; actual installed notices are regenerated on the build platform.
