# Ledgence documentation site

The public documentation at `https://docs.ledgence.com` is a static Astro/Starlight site. Content lives beside the product code and follows Diátaxis: tutorials teach through a controlled experience, how-to guides solve a specific task, reference records exact behavior, and concepts explain design and tradeoffs.

## Develop and build

Use Node.js 24 or newer (minimum 22.19). From this directory:

```sh
npm ci --ignore-scripts
npm run dev
```

Or use pnpm 11.19.0 with `pnpm install --frozen-lockfile` and `pnpm dev`. Install scripts stay disabled; the reviewed platform binaries are supplied by optional packages.

```sh
pnpm check
pnpm test
pnpm build
pnpm preview
```

The build checks page metadata, collects dependency notices, renders every page to static HTML, generates Pagefind search, exports Markdown with `llms.txt`, and checks local links, anchors, feature-guide links and assets. `release.json` records the 0.6 release tag, native targets and independently published package versions. `public/source.json` keeps release identity separate from the documentation checkout revision. The product SHA is optional: the build resolves the local release tag when it exists and otherwise omits the unknown SHA instead of substituting HEAD or a historical hash. `source-features.json` records Console, the installer, local distribution, worker authoring package, program publication and project scaffolding as part of the same release; generated provenance uses the `release` channel. Native documentation targets Linux x86_64/glibc (Ubuntu 24.04 qualification) and macOS arm64, with dynamic requirements preserved in bundle provenance. Complete release qualification and verify the published artifacts before deploying updated release documentation. A public release does not imply a stable 1.0 API.

## Authoring

Edit `src/content/docs/`. Every page needs a `title` and `description`. Begin body headings at `##`. Keep one reader need per page; link across types for additional context. The installation selector leads into the four content types; do not add empty placeholders. Use `agent` in approachable prose while preserving exact `program`, task, workflow and worker API names.

The site provides Diátaxis learning paths and release installation guidance. The [capability map](src/content/docs/reference/capabilities.md) links the implemented execution, integration and operational features. Existing `../docs/` contracts remain the detailed source for limits and protocol semantics. Move and link material deliberately as coverage grows; never present code snippets as runnable unless prerequisites and package publication are described.

Keep `release.json` tied to the verified public release. Record each public source feature in `source-features.json` with its verified release ref/date and guide. Future unreleased features use `availability: development` and a guide that states the prerequisites. The generated `source.json` and `llms.txt` retain this distinction. Do not advance release versions or qualification dates because a documentation build succeeds. At release time, verify the installer, native bundles, matching kit and public images before changing their availability or promoting the one-line installation path.

The `v0.6.0` source tag freezes the implementation and its preparation-time
documentation. Post-publication guides in this checkout reflect verified release
availability. Implementation links remain pinned to `v0.6.0`; references to
updated repository guides use `develop`. Generated `public/source.json` records
the exact documentation build revision separately from the product release
revision. Keep historical release notes, versioned upgrade guides and pinned
example corpora intact when refreshing current references.

Beginner tutorials follow one continuous path: choose one installation method,
run a small task, compose that same task in a workflow, then inspect those
executions in Console. Keep alternative installation commands on their own
pages; do not restart infrastructure or switch to unrelated programs halfway
through a lesson. Put advanced operations and full-source builds outside this
path.

For runnable Python examples, name the file, show its complete contents with a
code-fence title such as `python title="submit.py"`, explain the relevant lines,
and then show the terminal command that runs it. Prefer an editor and ordinary
files over Python embedded in shell heredocs. State the current directory,
prerequisites, expected output and next step. Label partial reference snippets
as fragments. Use environment variables only when the interface needs them;
explain their purpose, scope and lifetime, or use a named configuration file.
A reader should understand what each step changes.

The first task and workflow lessons have runnable counterparts in
`../examples/hello-ledgence/`. The documentation tests compare all complete
Python snippets with those files so they cannot silently drift apart. When
changing the learning path, run its unit tests and execute the documented
publication/submission sequence against the stated released runtime; a static
build alone cannot prove the example works.

Code examples must follow the checked-in SDK and examples. Only advertise verified registry versions and native targets. The self-hosted Console serves one instance and has no tenant administration or scope selectors. Describe the retained CLI/SDK fields as a fixed compatibility binding. Do not imply a vendor-hosted dashboard, exactly-once effects, or public multi-tenant isolation. Review the local trusted-code boundary where relevant.

`astro.config.mjs` owns sidebar labels and clean URL routing. `src/styles/docs.css` adapts the neutral Ledgence palette. Search is entirely static; no vendor account, external search service or runtime server is required. System light/dark appearance, keyboard search, mobile navigation, heading links and copyable code are supplied by Starlight.

## Brand assets

The Curvatura master artwork lives in [`../assets/brand/`](../assets/brand/).
`public/brand/` contains the unchanged mark, Apple touch icon, and social image
used by this site; `public/favicon.svg` copies the theme-aware master favicon.
The header renders the mark as a CSS mask in the current text color, preserving
contrast in both themes. Social metadata in `astro.config.mjs` references the
local 1280 × 640 image. Update these copies from the masters together; do not
redraw the symbol in CSS or change its geometry independently.

## Publish

Infrastructure and upload tooling live in the sibling `ledgence-landing-frontend`
repository, alongside the existing AWS setup. Run these commands there, with
infrastructure setup needed only when creating or changing that infrastructure:

```sh
pnpm deploy:docs:infra --profile ledgence-platform --expected-account 919333997957
pnpm deploy:docs --profile ledgence-platform --expected-account 919333997957
```

Every deployment must use the explicit `ledgence-platform` profile and verify
account `919333997957`; abort on any mismatch. Never deploy with `ledgence-llc`
or organization account `816975651859`. Use `--source` to choose a different
checkout; consult that script's help. No credentials are stored in this project.

The build uses `build.format: directory` and no trailing slash. The deployment script uploads HTML plus extensionless aliases (for example, `tutorials/run-locally/index.html` is also served at `/tutorials/run-locally`) for clean URLs on a private S3 REST origin, without a CloudFront function. The root uses `index.html`; missing pages keep a 404 response.

## Dependencies

See [the dependency review](DEPENDENCIES.md). Both lockfiles are committed. Undici is pinned to 8.10.2 in npm and pnpm overrides so lockfile imports remain consistent without bypassing pnpm’s release-age protection. Keep these two overrides synchronized. After a dependency change, review npm's lock, run `pnpm import`, run the license collector and build with both package managers. Keep required original legal files in the published `notices/` directory. Ledgence-owned code is MIT; third-party assets retain their own licenses.
