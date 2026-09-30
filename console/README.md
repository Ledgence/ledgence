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
[`console-v4.json`](../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json).
Strict runtime decoders consume that fixture in tests. Unknown fields or result
variants fail explicitly. Metadata u64 values remain decimal strings; JSON input,
output and CloudEvents use `lossless-json` to preserve integer/floating tokens and
negative zero across inspection, formatting and submission.

Lists use bounded keyset pages, exact filters and server observation timestamps.
Execution history appends the next page automatically near the end of the list,
without Refresh, row-count selectors, or page-navigation controls. Failed loads
retain the visible rows and offer a retry. Filters start a new traversal; returning
from a detail restores the cached rows and scroll position. The initial page polls
until loading older executions starts, then the loaded traversal stays stable.
Each row retains its own page's observation timestamp. Cached inactive queries
expire after five minutes; a browser reload starts a fresh traversal.
Workflow detail keeps observation timestamps and navigation for additional pages,
without Refresh or row-count selectors. Other paginated views retain their manual
controls. Previous cursors are held in bounded process memory rather than
accumulated in URLs. Active
resource polling stops when a terminal outcome is observed and pauses while the
page is hidden. Query cancellation is passed through to `fetch`; query keys include
origin, contract, instance, resource and complete query parameters.

Mutation retries are manual. A frozen command retains its exact bytes and identity
when transport leaves the result uncertain. Dialogs and pending commands belong
to their resource: refreshing the same resource preserves them, while navigating
to another resource starts with a separate form. Separating an operation requires an
explicit confirmation. Run again creates a new submission with a new identity.
Inputs, outputs, events and command bodies are not written to URLs, local storage
or analytics. Only appearance, sidebar collapse, and the preferred Graph/Trace view persist in
local storage. Shareable filters, selected record and view belong in the URL;
scroll, expansion and focus restoration use bounded memory per history entry.

Executions combines root workflows and standalone tasks in one server-paginated
history, with task/workflow and exact program/version filters. Include child
executions expands discovery; program history and exact-ID searches include
children automatically. Controller invocations and local steps stay inside their
workflow. Programs replaces the former Agents navigation; `/agents` links remain
usable. Type filters match any registered version and preserve mixed and
unspecified registrations.

Workflow details offer Graph, Trace and General; task details offer Trace and
General. General keeps input/output, resources and durable records in explicit
sections loaded on demand. Existing detail URLs continue to resolve to their
corresponding section. The main Executions table uses inclusive UTC calendar days
for its Submitted from/through controls and server-side type/status filtering.
Exact timestamp links retain their precision until their calendar field changes.

Graph and Trace share durable identities, selection and an evidence inspector.
Each entrypoint invocation is a node; repeated handlers remain distinct. The canvas
shows one workflow and references to its direct children. Open workflow navigates
to the child's own canvas; neither Graph nor Trace expands a child's internals.
Back restores the previous view, selection, camera and node positions. Up and
breadcrumbs use recorded ownership, never correlation keys.

The graph uses React Flow for interaction and a replaceable Dagre layout adapter.
It is an execution viewer: moving a card only changes local presentation; creating,
reconnecting and deleting edges are disabled. Fit changes the camera; Reorganize
restores automatic card positions. Status updates preserve the view; newly loaded
nodes are positioned without moving existing cards. These adjustments are held in
bounded history memory, not persisted as workflow data or stored in the backend.
An initial readable view can require panning; Fit can show the whole loaded graph.
Entrypoints, work cards, and compact fork/join controls have distinct shapes and
dimensions. Layout and connector routing use those same bounds. Cards show
recorded status; child durations include queue and wait time, while local durations
describe the observed callable interval. Replayed locals display Replay instead
of suggesting that the callable ran again. Selecting a node highlights its
connections without moving the canvas.

Connections come exclusively from typed relations supplied by the server:
invocation, registration, branch membership, terminal-outcome wait and resume.
The client resolves their references to loaded nodes; it does not reconstruct
relations from node metadata. Dashed connections mark branch membership. Where
a complete entrypoint-to-fork-to-child path is visible, the graph omits the
redundant direct invocation line; that invocation remains in the inspector and
accessible evidence list. Local invocation identifies its entrypoint without
claiming completion or local-to-local execution order. No dependency is inferred
from timestamps, proximity, trace parentage or correlation. A join waits for
terminal outcomes, which may include failure or cancellation. A closed
external wait without a recorded wake remains unknown. These views work without
OpenTelemetry export. Worker failure diagnostics retain their separate wire field
`phase`, labeled **Failure stage** in the interface.

Explorer pages remain bounded and identify partial views. Relations with unloaded
endpoints retain their typed references and evidence-carrier IDs in the accessible
list; no placeholder execution is fabricated. Missing targets may be on another
page or unavailable; the Console does not infer deletion or expiry.
Trace rows are virtualized, with a complete accessible work list for the loaded
page. Routing is checked against card obstacles; extreme density or overlapping
manual positions can still need Reorganize or Trace. Unit geometry checks cover
the canonical four-branch workflow and bounded synthetic pages of 100 records.
The browser and server use Console contract **4**; deploy matching assets together.
Old Explorer cursors are invalidated by the coordinated backend upgrade.

Resources shows per-attempt runtime and Python-process CPU where recorded.
Child processes are excluded from that CPU scope. Process lifetime peak memory
includes earlier warm invocations; it is not an invocation peak and is never
summed across attempts. Missing measurements remain unavailable, not zero; terminal
attempts do not poll indefinitely for absent reports. Worker telemetry is
observational: fresh, stale, expired and unsupported details are distinct, and
missing reports never become fabricated process slots.

## Interface and dependencies

The shell has a persistent collapsible desktop sidebar, compact mobile navigation,
and light/dark/system appearance. Collapse from the sidebar header; its Ledgence
mark expands it again without navigating. Execution details share one header for
the instance, ancestry and title, with status and timing directly below;
primary date/type/status filters stay visible and advanced exact filters expand on demand. The explorer can expand to
full screen with Escape to exit, keeps selection and canvas state, and opens its
inspector only when work is selected. Secondary evidence remains available in an
expandable work list.

The interface uses responsive tables, local system fonts, visible focus, semantic controls and
reduced-motion support. Selected shadcn primitives are adapted locally; Radix Dialog supplies modal behavior and React Flow supplies graph interaction. The modal scroll lock inserts reviewed CSS at runtime,
so the server's CSP permits inline styles while keeping scripts restricted to local
assets without inline script or evaluation. All bundled resources are local.

See [DEPENDENCIES.md](DEPENDENCIES.md) for the exact dependency review policy and
[`third_party/inventory.json`](third_party/inventory.json) for provenance and legal
material. Ledgence-owned Console code is MIT; third-party licenses remain their own.
