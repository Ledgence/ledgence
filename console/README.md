# Ledgence Console

The Console is the static operator interface for one self-hosted Ledgence instance.
It provides execution inspection and submission, durable workflow observations,
a registered program catalog, and worker process observations. The orchestrator
serves the production assets at `/console/`; a deployed Console needs neither a
Node.js process nor a hosted vendor service.

## Live lifecycle history (development source)

The next release adds a live lifecycle feed to a task’s **Trace → Lifecycle** and
a workflow’s **General → Lifecycle** section. It reads committed Ledgence history;
it is not a model-token stream, application log, or replacement for the execution
graph. Console contract 15 advertises this through `execution_progress`. Adapters
without the capability keep the existing paginated history.

A view catches up in bounded batches and then waits for new pages. Each page
advances its cursor only after validation; transient reconnects reuse that cursor
and duplicate event identities are not rendered twice. Hidden/offline tabs,
navigation, paused updates, and access changes stop observation without changing
the execution. Failed reads retain the last accepted events and expose a retry;
missing or retired history is reported explicitly. A changed instance or verified
identity discards the previous feed.

The view retains at most 500 events and reads at most five backlog pages before
asking to load the next batch. **Browse all history** opens the paginated view for
earlier events. A completed execution stops automatic updates; **Check for
updates** reads any later cleanup records. A full page reload deliberately
rebuilds retained history; no cursor, event, or credential is stored by this feed
in browser storage.

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
orchestrator separately. In operator-trusted mode, include
`http://127.0.0.1:5173` in that development instance's `allowed_origins`: the proxy
preserves the browser Origin. You may retain `http://127.0.0.1:8080` too when using
the directly served Console in that mode. Credential mode requires exactly one
selected origin. Prefer the built Console served by the orchestrator at the API's
own origin; choose the Vite origin alone when testing through its development
proxy. An unlisted Origin causes requests to be rejected. The 0.9.0 server contains no demo API
or mock data.

<span id="instance-access-development"></span>

## Instance access

Credential-enabled instances present an instance-credential sign-in form. Use an
individual operator credential issued by your administrator. Console stores it
in a host-only HttpOnly cookie; application JavaScript does not retain it in
localStorage, sessionStorage or IndexedDB. The sidebar identifies the verified
operator. Worker credentials cannot sign in to Console.

The server checks expiry, installation, scope and capabilities on each request.
The UI hides unavailable actions and presents permission errors for restricted
payloads. The server remains authoritative. Production access requires HTTPS
on a dedicated hostname; local HTTP is for trusted loopback development only.
Cookies are shared across ports on the same hostname, so different ports are not
an isolation boundary. Configure the exact Console origin, including its port.

Each Console request carries the actor of the displayed view. A changed cookie
from another tab cannot submit that view's command as the replacement actor.
Tabs broadcast only an access-invalidation signal and verify access again when
becoming visible. Content and dialogs are hidden during that check. Unsent form
drafts remain in memory when the verified actor, instance and permissions are
unchanged; a changed actor, instance or permission set discards the old view.
Read requests are cancelled or wait for verification, then refresh. Old responses
cannot repopulate the new actor's cache. No write is automatically resubmitted
after authentication changes.

**Sign out of this browser** clears its cookie and pending decisions; it does
not revoke other copies of the credential. Administrators revoke credentials by
updating the verifier configuration and restarting all server replicas; the
configuration is loaded at startup. Cookie retention
does not replace server-enforced credential expiry.

A denied first attempt is reported as rejected. A later denied replay does not
resolve an earlier uncertain outcome. An uncertain approval request remains
frozen in memory, bound to its instance, during the current page
lifetime. After reauthentication as the same actor with the same instance and
permissions,
**Try again** deliberately resends the same bytes and decision ID. Explicit
sign-out or switching actors discards that pending request. A full browser
reload can inspect a persisted server decision, but cannot restore an unsaved
local command. The reviewer field remains a caller-supplied label; verified
acceptance attribution is shown separately when the instance supports audit.

Instances without credential configuration retain their explicit
operator-trusted deployment mode and do not invent an authenticated actor.
These access capabilities are available in Ledgence 0.9.0 packages.

## Brand assets

The Curvatura master artwork lives in [`../assets/brand/`](../assets/brand/).
`public/brand/` contains unchanged copies of the mark, theme-aware favicon,
and Apple touch icon. The shell uses the mark as a CSS mask in the current text
color for its expanded sidebar, collapsed sidebar, and mobile navigation. Keep
these copies aligned with the masters; do not redraw the mark in CSS.

All brand files are served locally under `/console/brand/` and included in the
generated asset manifest. Console requires no external image or font service.

## Checks and build

```sh
pnpm --dir console format:check
pnpm --dir console lint
pnpm --dir console typecheck
pnpm --dir console contracts:check
pnpm --dir console test
pnpm --dir console test:e2e
pnpm --dir console licenses:check
# Build assets separately when browser checks are not needed:
pnpm --dir console build
```

`test` includes unit tests and real Chromium/WebKit component tests. `test:e2e`
runs the full production build, then serves those fresh static assets through a
loopback preview server and checks browser interaction against Rust-generated
contract fixtures. No Vite development client or automatic reconnect reload runs
in these pages. The server has a strict port and never reuses an existing server;
if port 5173 is occupied, set `LEDGENCE_CONSOLE_TEST_PORT` to an unused port. Its
120-second startup budget includes the build; test timeouts and retries are
unchanged. CI uses the same checked `dist/` for the subsequent native acceptance
gate. These controlled transport tests complement the repository's real server,
database and worker acceptance gate; they do not establish end-to-end
orchestration behavior. To open the browser reports from the repository root, use
`pnpm --dir console exec playwright show-report playwright-report/e2e` for the
fixture suite or `pnpm --dir console exec playwright show-report playwright-report/live`
for the native suite. Their test artifacts use separate directories so running
one suite does not erase the other suite's traces.

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
and `LEDGENCE_CONSOLE_STACK`, then runs `pnpm --dir console test:live`. The stack
file and its adjacent `results.json` identify the exact native task and workflow
seeds; the browser suite checks their identities through the real API instead of
assuming they remain on the first execution-history page. When attaching manually
to a held acceptance stack, provide both variables from that same run. This separate
Chromium/WebKit suite uses
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
Running it without an explicit Console URL or the required native seed handoff
fails; it never silently falls back to mocks or another run's evidence.

## Contracts and behavior

The only browser API is `/v1/console/*`. Instance configuration controls public
capabilities, limits and polling. The browser never supplies an internal tenant,
namespace or scope. Every response is checked against the configured instance
identity and Console contract version before entering query state.

The canonical Rust serialization fixture is
[`console-v5.json`](../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json).
Strict runtime decoders consume that fixture in tests. Unknown fields or result
variants fail explicitly. Metadata u64 values remain decimal strings; JSON input,
output and CloudEvents use `lossless-json` to preserve integer/floating tokens and
negative zero across inspection, formatting and submission. Every JSON property
name is treated as user data, including `__proto__`. Duplicate property names
in one object are rejected.

Lists use bounded keyset pages, exact filters and server observation timestamps.
Execution history appends the next page automatically near the end of the list,
without row-count selectors or page-navigation controls. **Refresh executions**
restarts at the live first page with the same filters. Failed loads retain the
visible rows and offer a retry. Filters start a new traversal; returning
from a detail restores the cached rows and scroll position. The initial page polls
until loading older executions starts, then the loaded traversal stays stable.
Each row retains its own page's observation timestamp. Cached inactive queries
expire after five minutes; a browser reload starts a fresh traversal.
Workflow detail keeps observation timestamps and navigation for additional pages,
without Refresh or row-count selectors. Other paginated views retain their manual
controls. Previous cursors are held in bounded process memory rather than
accumulated in URLs. Active
resource polling stops when a terminal outcome is observed and pauses while the
page is hidden. Local checkpoints keep the selected activation when the workflow
advances or finishes; use **Load steps** to select another activation. The first
page of a selected activation's local checkpoints polls
while its workflow is active and receives a final refresh when the workflow
becomes terminal. **Load steps** also reloads the selected activation; navigating
to older checkpoint pages keeps that page stable. Query cancellation is passed through to `fetch`; query keys include
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

Workflow General includes Approvals with the immutable action name, version,
effective arguments, optional original proposed arguments, deadline and recorded
decision. The backend owns pending, approved, rejected, expired and cancelled
states; the browser never infers expiry from its clock. A decision echoes the
exact request binding and action. An uncertain response can only retry the same
decision bytes and ID. Reviewer is caller-supplied audit attribution, not an
authenticated identity. Credential-enabled instances separately authenticate the caller; operator-trusted instances retain their deployment access boundary.
Approval permits a continuation; it does not prove that the action executed.

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
Trace rows are virtualized using the visible viewport height, including resize
and full-screen changes, with a complete accessible work list for the loaded
page. Routing is checked against card obstacles; extreme density or overlapping
manual positions can still need Reorganize or Trace. Unit geometry checks cover
the canonical four-branch workflow and bounded synthetic pages of 100 records.
Ledgence 0.9.0 uses Console contract **14** for native admission waits,
accepted operations and weighted resource offers; deploy matching server and assets together.
Upgrade server and assets together when migrating from 0.8.0 (C7).
Old Explorer cursors are invalidated by the coordinated backend upgrade.

Resources shows per-attempt runtime and Python-process CPU where recorded.
Child processes are excluded from that CPU scope. Process lifetime peak memory
includes earlier warm invocations; it is not an invocation peak and is never
summed across attempts. Missing measurements remain unavailable, not zero; terminal
attempts do not poll indefinitely for absent reports. Worker telemetry is
observational: fresh, stale, expired and unsupported details are distinct, and
missing reports never become fabricated process slots.

## Interface and dependencies

On desktop, **Collapse sidebar** makes room for the content. Select the Ledgence
mark to expand the sidebar again. In 0.9.0, choose **System**,
**Light** or **Dark** from **Appearance** in the sidebar. On small screens, the
same control appears beside the navigation links. **System** is the default
and follows changes to the device appearance. The browser remembers the selected
preference when local storage is available.

List pages open directly with their content. Execution details keep the title,
Back button and ancestry together, with status and timing below.

Primary date/type/status filters stay visible and advanced exact filters expand on demand. The explorer can expand to
full screen with Escape to exit, keeps selection and canvas state, and opens its
inspector only when work is selected. Secondary evidence remains available in an
expandable work list.

In 0.9.0, the expanded sidebar shows the connected server version.
Execution history groups its filters and results together. Names and states lead each
row, with technical IDs beneath the names. State badges pair a label with a symbol.
Program and worker entries group identity, version or capacity, and related actions.
The Graph legend distinguishes fork membership from other execution links. The Trace
ruler shows elapsed time across the visible range of recorded work.

The interface uses responsive tables, local system fonts, visible focus, semantic controls and
reduced-motion support. Selected shadcn primitives are adapted locally; Radix Dialog supplies modal behavior and React Flow supplies graph interaction. The modal scroll lock inserts reviewed CSS at runtime,
so the server's CSP permits inline styles while keeping scripts restricted to local
assets without inline script or evaluation. All bundled resources are local.

See [DEPENDENCIES.md](DEPENDENCIES.md) for the exact dependency review policy and
[`third_party/inventory.json`](third_party/inventory.json) for provenance and legal
material. Ledgence-owned Console code is MIT; third-party licenses remain their own.

### Approval inbox

Ledgence 0.9.0 includes **Approvals** in the main navigation. It lists the
instance's pending requests across workflows, earliest deadline first. Filter
by status, exact program ID and **Due before (UTC)**. A deadline exactly at the
chosen instant is excluded. Opening a request
loads its recorded effective action for review. After a decision or expiry, the
review page checks for the resumed activation while the workflow remains open
and reads the request once more when the workflow closes. If the request is
retired during review, the page disables new decisions.

Follow the workflow link to open Graph at the recorded approval wait, including
waits deep in a long workflow. Select **View workflow from start** to see the
original first page. The inbox uses the instance's existing access boundary and
decision contract.

Approval details label requests created through
`request_effect_approval` as **Protected external write**. The submitted
decision and any retry preserve this requirement. Receipt validation rejects
a changed protection marker. Use Console assets from the same build as the
orchestrator for this extended contract.

<span id="verified-approval-attribution-development"></span>

### Verified approval attribution

On credential-enabled native instances, an accepted approval's review page and
submitted-decision dialog show **Accepted by**, the identity issuer, actor type
and acceptance event ID. This is the original verified operator or service,
which may differ from the current viewer or the **Reviewer label (caller
supplied)**. The original acceptance time remains the decision time; later
inspection does not replace it.

The Console makes one exact lookup for that approval occurrence and decision.
It displays **No verified actor recorded** only after a successful empty query,
for example for a historical decision accepted before audit was enabled.
Unsupported audit, denied access, failed or inconsistent responses remain
unavailable. **Retry attribution** retries only the metadata read. It never
resubmits a decision. Reading attribution requires `metadata_read`; the approval
payload retains its `sensitive_read` requirement.

Accepted evidence is cached in memory within the current instance and access
view. Workflow approval lists link to detail without fetching every card's audit
history. Actor, instance, permission or audit-capability changes discard the
view; late responses cannot repopulate it. Credential rotation for the same actor
preserves a pending command only under the same instance and permissions and
still requires a deliberate identical retry. Operator-trusted deployments do not
invent verified identities. Verified attribution preserves the existing approval
and decision representation. Ledgence 0.9.0 uses matching Console contract 14
assets and server builds.

### Inspect a durable operation

In a workflow's **Graph** or **Trace**, select a local step with an accepted
result and expand **Inspect durable operation**. Opening this section fetches
the step’s recorded input and accepted result. Ordinary local steps show the input
supplied by the workflow. Model and tool calls show the effective arguments
and operation version bound to the durable call. The accepting attempt and
acceptance time come from the durable record. A replay observation describes
one observed attempt; it does not count every reuse. If only timing or failure
was observed, the node has no durable result to inspect.

C9 additionally identifies protected provider subjects in admission waits. The
on-demand admission inspector separates capacity settlement from provider
business completion and displays the original provider ticket. Provider grants
are not rendered as successful local-operation nodes. Use matching C14 assets
and backend.

C10 adds standalone task resource admission. Task details show the resource,
charged retries and admission deferrals; open **Resource admission** to inspect
one generation's offer, worker source and cleanup evidence. This is an audited,
read-only view. A task can already have a result while its resource remains
occupied. [Contract and limitations](../docs/resource-admission.md#standalone-task-admission).

C11 introduced **Queues** for credential-backed instances with queue concurrency
policies enabled. The page lists configured limits, occupied capacity and
uncertain holds. `metadata_read` grants policy inspection; `resource_control`
grants policy edits. Inspecting the attempts behind holds is an explicit audited
`sensitive_read` operation. An operator with both permissions can release an
uncertain hold with its exact task, attempt, revision and a reason.

Set a limit, pause new attempts, or select unlimited. A stop timeout waits for
closure by default. Forced release after an explicitly configured grace period
can let another task start while the original still runs. The page distinguishes
the time the stop was requested from the release time and reason. Lowering a
limit lets existing holds drain. A lost response can be reconciled in the
current browser session by retrying the original request ID, revision and exact
body; inspect the current policy before creating a new command.

To run the credential-backed queue-policy browser acceptance, point
`LEDGENCE_CONSOLE_URL` at an explicitly disposable loopback Console served by
the backend, set `LEDGENCE_QUEUE_OPERATOR_FILE` and `LEDGENCE_QUEUE_READER_FILE`
to current same-origin operator/metadata-reader credential files, then run
`LEDGENCE_CONSOLE_LIVE_SUITE=queue-policies pnpm --dir console test:live`.
The test creates a uniquely named policy, pauses and edits it, verifies
metadata-only access, and leaves it unlimited for database teardown. This
suite disables retained browser traces and uses separate output directories.

<span id="resource-offers-development-c14"></span>

### Resource offers

Open **Resources**, then select a named resource. Its policy, occupancy and default queue group appear above the offer history. Each weighted offer records its source execution, policy revision and why that group was selected. Select the execution to see the task or workflow. This view requires authenticated resource admission and `metadata_read`. FIFO resources have no new weighted decisions.

### Manage resource policies and inspect charged sources

Open **Resources** using a credential with `metadata_read` to inspect capacity, rate, timeouts and occupancy. An adapter can support ordinary FIFO policies independently of weighted offer history. If your credential also has `resource_control`, enter the resource name and choose **Create policy**, or open an existing resource and choose **Edit policy**. Configure occupancy, timeouts and an optional start rate. On adapters that support weighted offers, enter each group's weight and source queues in turn order and select a default group for unmapped queues. Enter the reason and choose **Save policy**.

A create expects the resource to be absent. An edit uses the revision you opened. If two operators edit the same revision, one receives a conflict. Choose **Separate this operation**, reload the current policy and review it before preparing a new change. If the response is lost, choose **Retry exact request** to retrieve the receipt with the original request ID. The Console keeps the request in memory for the same signed-in browser session during navigation. Copy the original Console request before closing or reloading the page if reconciliation is still needed.

On adapters that support source discovery, **Inspect retained sources** lists charged task attempts, workflow operations and provider dispatches for the resource. Filter by **Possibly dispatched** or **Unknown occupied**. **Inspect exact record** reads the retained admission record by its original generation or occurrence, even after ordinary execution history is collected. Each lookup, including an empty source page, requires `sensitive_read` and is audited. Reading the aggregate policy and occupancy requires only `metadata_read`.

A credential with only `sensitive_read` opens **Resources** with a generic instance label and no suggested queues. Enter a known resource name and choose **Inspect retained sources**. Listing configured resource names, reading offer history and viewing occupancy totals each require `metadata_read`. If the adapter supports source discovery without exact task admission inspection, the Console shows the task identity and generation in the source list and explains the unavailable detail.

<span id="shared-run-allowances-development-c14"></span>

### Shared run allowances

On a budgeted workflow or task, open the budget view to inspect its root, calls, delegations, limit refusals and usage observations. Inspecting a child resolves to its root. These metadata reads require authenticated budget support and `metadata_read`.
