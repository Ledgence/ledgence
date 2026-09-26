# Console dependency review

The Console is a private package built with exact direct versions and a committed
pnpm lockfile. Every dependency upgrade requires a fresh graph review. The review
is independent of the documentation site's license exceptions.

## Direct runtime choices

| Package                | Version | Purpose                                                     | License                              |
| ---------------------- | ------- | ----------------------------------------------------------- | ------------------------------------ |
| react / react-dom      | 19.3.0  | Component renderer                                          | MIT                                  |
| react-router           | 8.4.0   | Declarative client-side routing                             | MIT                                  |
| @tanstack/react-query  | 5.103.2 | Bounded observation cache, cancellation, polling            | MIT                                  |
| lossless-json          | 4.3.1   | Preserve user JSON numeric values and reject duplicate keys | MIT                                  |
| lucide-react           | 1.48.0  | Local tree-shaken icons                                     | ISC and included Feather MIT notices |
| @radix-ui/react-dialog | 1.1.23  | Modal focus, keyboard, dismissal semantics                  | MIT                                  |

Vite 7.3.6 with @vitejs/plugin-react 5.2.0 avoids adopting Vite 8's MPL-licensed
Lightning CSS graph. TypeScript 6.0.3 is within typescript-eslint 8.70.1's supported
range; selecting TypeScript 7 would exceed that range. These are explicit reviewed
compatibility choices, not floating `latest` dependencies. The exact development
tool versions are in `package.json`.

Six source primitives are adapted from a pinned shadcn/ui commit: Button, Input,
Card, Dialog, Table and Skeleton. Their original paths, hashes, MIT license and
adaptation notes are in `third_party/shadcn/provenance.json`. No generator runs at
installation. Tailwind, class-merging libraries, external fonts and a graph-layout
runtime are unnecessary for these views and are not included.

## Full dependency graph

`third_party/inventory.json` records all 287 locked packages, including optional
platform packages: exact npm archive, lockfile integrity, archive SHA-256,
repository metadata, declared license, lifecycle scripts and hashes of retained
legal material. Archive integrity was verified against the lockfile before review.
The graph is public-source and does not require a hosted account or vendor SDK.

The ordinary allowed licenses are MIT, ISC, Apache-2.0, BSD-2-Clause and BSD-3-Clause.
The inventory additionally records these exact reviewed packages:

- `tslib@2.8.1`: 0BSD, including its copyright notice.
- `minimatch@10.2.6`: BlueOak-1.0.0; build/lint tooling, with the full supplied license.
- `caniuse-lite@1.0.30001812`: CC-BY-4.0, unmodified browser-compatibility data used by
  build tooling. Its attribution/license material is retained; it is not application
  code included in the browser bundle.

Required copyright, permission, attribution and notice files accompany the source
and generated distribution. None of these licenses requires product naming,
promotional credits or disclosure of user application source. This inventory does
not represent third-party code as MIT-owned Ledgence code.

Some npm archives omit a separate license file. Exact upstream or parent-version
legal material is retained where the archive explicitly identifies that source.
Examples include esbuild/Rollup platform binaries, @humanfs/types, imurmurhash,
esrecurse and natural-compare. Every supplement is identified in the inventory.
`stackback@0.0.2`, used solely by Vitest's diagnostic tooling, provides its original
MIT declaration and author metadata without a standalone MIT text; its original
README/package metadata and the full BSD notice for its bundled V8-derived source
are retained. It is never a browser/runtime dependency. This is a package-specific
review, not an allowance for dependencies with unknown licensing.

`rollup@4.62.2` and `react-remove-scroll-bar@2.3.7` are explicitly overridden within
their consumers' compatible version ranges. The former avoids a later optional
native archive without adequate upstream legal material; the latter retains a
verifiable upstream release and full MIT notice. Remove an override only after
reviewing the replacement graph.

## Installation and distribution gates

- Strict peer versions and exact Node/pnpm engines are enforced.
- New package versions must be at least 24 hours old at resolution time.
- Automatic peer installation is disabled.
- Install with `--frozen-lockfile --ignore-scripts`. The only declared dependency
  installation hook in the reviewed graph, `esbuild@0.28.2`, is explicitly denied
  by `allowBuilds`; the verified platform package supplies the binary. No package
  lifecycle script is required by the tested build.
- `licenses:check` rejects a changed lockfile/build policy/direct dependency set,
  unknown installed versions, unreviewed licenses, missing notices and changed
  notice hashes. After changing dependency versions, remove this package's own
  `node_modules` and install the new frozen graph so stale pnpm entries cannot be
  mistaken for the reviewed graph.
- The bundler records every actual browser package. The build fails if an included
  package lacks a reviewed inventory entry. It also requires distribution notices,
  local entry assets and an exact static-asset manifest.
- The latest review's `pnpm audit` found no known advisories across the full locked
  graph. Run the audit again when dependencies change; this is a point-in-time check.

The `/console/notices/` distribution page links the original legal files and
identifies both runtime and build-tool material. Keeping additional tool notices
does not imply those tools execute on the user's deployed instance.
