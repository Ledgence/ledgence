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

The build checks page metadata, collects dependency notices, renders every page to static HTML, generates Pagefind search, exports Markdown with `llms.txt`, and checks local links, anchors and assets. `public/source.json` records the product checkout revision. Before public deployment, ensure that revision is pushed to the product repository and the tutorials match it. Development documentation is explicitly marked; this site does not imply a stable release.

## Authoring

Edit `src/content/docs/`. Every page needs a `title` and `description`. Begin body headings at `##`. Keep one reader need per page; link across types for additional context. The four sections already contain real content: do not add empty placeholders. Use `agent` in approachable prose while preserving exact `program`, task, workflow and worker API names.

The initial site is an incremental Diátaxis pilot. Existing `../docs/` contracts remain the detailed source for features outside that pilot. Move and link material deliberately as coverage grows; never present code snippets as runnable unless prerequisites and package publication are described.

Code examples must follow the checked-in SDK and examples. Do not invent a registry install command, a hosted dashboard, exactly-once effects, or public multi-tenant isolation. Review the local trusted-code boundary where relevant.

`astro.config.mjs` owns sidebar labels and clean URL routing. `src/styles/docs.css` adapts the neutral Ledgence palette. Search is entirely static; no vendor account, external search service or runtime server is required. System light/dark appearance, keyboard search, mobile navigation, heading links and copyable code are supplied by Starlight.

## Publish

Infrastructure and upload tooling live in the sibling `ledgence-landing-frontend` repository, alongside the existing AWS setup. Run `pnpm deploy:docs:infra` once and `pnpm deploy:docs` to build and upload this site, using the ordinary AWS credential chain. Use `--source` to choose a different checkout; consult that script's help. No credentials are stored in this project.

The build uses `build.format: directory` and no trailing slash. The deployment script uploads HTML plus extensionless aliases (for example, `tutorials/run-locally/index.html` is also served at `/tutorials/run-locally`) for clean URLs on a private S3 REST origin, without a CloudFront function. The root uses `index.html`; missing pages keep a 404 response.

## Dependencies

See [the dependency review](DEPENDENCIES.md). Both lockfiles are committed. Undici is pinned to 8.10.2 in npm and pnpm overrides so lockfile imports remain consistent without bypassing pnpm’s release-age protection. Keep these two overrides synchronized. After a dependency change, review npm's lock, run `pnpm import`, run the license collector and build with both package managers. Keep required original legal files in the published `notices/` directory. Ledgence-owned code is MIT; third-party assets retain their own licenses.
