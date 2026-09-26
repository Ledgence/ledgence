# Ledgence Console

The Console is the static operator interface for one self-hosted Ledgence instance.
It provides execution inspection and submission, durable workflow observations,
a registered program catalog, and worker process observations. The orchestrator
serves the production assets at `/console/`; a deployed Console needs neither a
Node.js process nor a hosted vendor service.

## Development

Use Node.js **24.21.0** and pnpm **11.27.1**. This directory is an independent pnpm
workspace; it does not change the documentation site's package manager.

From the repository root:

```sh
pnpm --dir console install --frozen-lockfile --ignore-scripts
pnpm --dir console exec playwright install chromium webkit
pnpm --dir console dev
```

On Linux browser runners, provision the operating-system browser prerequisites
with the pinned Playwright installer (`playwright install --with-deps chromium webkit`).
Vite binds to loopback and proxies `/v1` to `http://127.0.0.1:8080`. Start a configured
orchestrator separately. Include `http://127.0.0.1:5173` in that development
instance's `allowed_origins`: the proxy preserves the browser Origin. Retain
`http://127.0.0.1:8080` too if using the directly served Console. An unlisted Origin
causes write requests to be rejected. The development server contains no demo API
or mock data.

## Checks and build

```sh
pnpm --dir console format:check
pnpm --dir console lint
pnpm --dir console typecheck
pnpm --dir console contracts:check
pnpm --dir console test
pnpm --dir console test:e2e
pnpm --dir console licenses:check
pnpm --dir console build
```

`test` includes unit tests and real Chromium/WebKit component tests. `test:e2e`
checks browser interaction against Rust-generated contract fixtures. These
controlled transport tests complement the repository's real server, database and
worker acceptance gate; they do not establish end-to-end orchestration behavior.

Production output is `dist/`, including legal notices, the inventory of packages
actually included in browser chunks and `console-manifest.json`. The manifest
records the Console contract, source revision and dirty state, toolchain, lockfile
hash, and the exact length/SHA-256/content type of each asset. The Rust server
checks these assets before serving them. Do not serve source files or development
fixtures as the Console distribution.

When building outside a Git checkout, provide both `LEDGENCE_SOURCE_REVISION`
(a complete lowercase 40-character commit ID) and `LEDGENCE_SOURCE_DIRTY`
(`true` or `false`). Supplying only one or an invalid value fails the build.
A release must be built from its recorded clean source state; development builds
may record a dirty source tree.

For the real acceptance stack, the repository gate supplies `LEDGENCE_CONSOLE_URL`
and runs `pnpm --dir console test:live`. This separate Chromium/WebKit suite uses
unmodified HTTP responses from the prepared native server and submits additional
work only to its owned acceptance instance. It verifies served assets, real
execution/workflow observations, dialogs under the production CSP, numeric
round-trips and responsive layouts. A separate ten-minute WebKit session keeps
real polling active, exercises an offline/reconnect interval and checks continued
navigation without reloading. Controlled component tests separately verify hidden
polling, permanent-error stops and cancellation of replaced filters.
Interaction checks exercise real keyboard traversal, modal focus retention/return,
reduced-motion navigation and touch taps. macOS WebKit uses its native Option-Tab
shortcut for full-item navigation, with plain Tab also checked between fields.
Scaling coverage applies CSS `zoom: 2` to the root, verifies doubled rendered text,
and checks controls/reflow at 1280px and a constrained 640px viewport. This tests
layout scaling; it does not claim browser-chrome zoom or physical-device coverage.
Running it without an explicit Console URL fails; it never silently falls back to
mocks.

## Contracts and behavior

The only browser API is `/v1/console/*`. Instance configuration controls public
capabilities, limits and polling. The browser never supplies an internal tenant,
namespace or scope. Every response is checked against the configured instance
identity and Console contract version before entering query state.

The canonical Rust serialization fixture is
[`console-v1.json`](../crates/ledgence-orchestration-api/tests/fixtures/console-v1.json).
Strict runtime decoders consume that fixture in tests. Unknown fields or result
variants fail explicitly. Metadata u64 values remain decimal strings; JSON input,
output and CloudEvents use `lossless-json` to preserve integer/floating tokens and
negative zero across inspection, formatting and submission.

Lists use bounded keyset pages, exact filters and server observation timestamps.
Only the first live page polls; older pages refresh explicitly. Previous cursors
are held in bounded process memory rather than accumulated in URLs. Active
resource polling stops when a terminal outcome is observed and pauses while the
page is hidden. Query cancellation is passed through to `fetch`; query keys include
origin, contract, instance, resource and complete query parameters.

Mutation retries are manual. A frozen command retains its exact bytes and identity
when transport leaves the result uncertain. Separating an operation requires an
explicit confirmation. Run again creates a new submission with a new identity.
Inputs, outputs, events and command bodies are not written to URLs, local storage
or analytics; only the appearance preference is persistent.

The recorded-work view draws relationships from durable creation and activation
IDs. It is bounded and paginated, and does not infer relationships from correlation
keys or present recorded history as future execution. Worker telemetry is
observational: fresh, stale, expired and unsupported details are distinct, and
missing reports never become fabricated process slots.

## Interface and dependencies

The shell follows the approved neutral layout, with light/dark/system appearance,
responsive tables, local system fonts, visible focus, semantic controls and
reduced-motion support. Selected shadcn primitives are adapted locally; only the
Radix Dialog runtime is used. Its modal scroll lock inserts reviewed CSS at runtime,
so the server's CSP permits inline styles while keeping scripts restricted to local
assets without inline script or evaluation. All bundled resources are local.

See [DEPENDENCIES.md](DEPENDENCIES.md) for the exact dependency review policy and
[`third_party/inventory.json`](third_party/inventory.json) for provenance and legal
material. Ledgence-owned Console code is MIT; third-party licenses remain their own.
