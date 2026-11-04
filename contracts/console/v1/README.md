# Console HTTP contract

Portable DTOs are defined in `ledgence-orchestration-api::console`. Existing
client/worker HTTP contracts retain their previous serialization.

The canonical cross-language fixture is
[`console-v13.json`](../../../crates/ledgence-orchestration-api/tests/fixtures/console-v13.json).
Rust constructs it from the public DTOs and verifies the committed bytes; Console
runtime-decoder tests consume that same file. It is also included in the Rust
source archive so its tests remain independent of the monorepo.

Regenerate explicitly after an intentional contract change:

```sh
cargo run -p ledgence-orchestration-api --example console-fixtures -- --write
cargo test -p ledgence-orchestration-api --test console_contracts
pnpm --dir console contracts:check
```

All `/v1/console/*` responses identify the fixed instance with
`Ledgence-Instance-Id` and `Ledgence-Console-Contract: 13`. The browser rejects
responses that disagree with the configuration used for its query keys.
Queries and commands reject unknown and duplicate fields. The server binds requests to its configured instance and scope. Responses
include authoritative identity where their contract requires it.

New revisions, sequences and unsigned metadata counters are canonical decimal
strings. Timestamps are UTC epoch milliseconds within the existing four-digit
year range. User JSON keeps the task API's exact integer and finite floating
value categories; clients must not round-trip it through JavaScript Number.

Pages are live keyset reads. Their opaque cursor binds endpoint, internal
instance scope, parent and filters. A page is internally consistent; consecutive
pages can observe later committed changes. The worker slot cursor uses the
stable slot position, not a changing snapshot sequence.

The HTTP namespace remains `/v1/console`; the Console contract version is 11.
Explorer nodes carry backend-projected typed relations with references to existing
workflow-scoped or activation-scoped identities. Relations remain present when an
endpoint is outside the current page. See the [query model](../../../docs/console-query-model.md)
for relation directions, evidence, byte bounds and upgrade behavior. Historical
C2/C3 fixtures are retained explicitly for compatibility rejection tests.

C5 adds `POST /v1/console/approvals/inspect`, `/list`, and `/decide`. Requests
omit scope; the server binds the installation. Approval revisions use decimal
strings and argument JSON is preserved losslessly. Graph wait kinds include
`approval`, with wake reasons `approved`, `rejected`, or `expired`. Apply the
approval database migration before starting matching components. See
[the approval contract](../../../docs/workflow-approvals.md).

Since Ledgence 0.7.0, Console includes the additive `POST /v1/console/approvals/inbox`
read route. It accepts an optional status, exact program ID and cursor, plus a
page limit. The fixed instance scope governs which workflows are visible.
Summary rows omit application arguments. Their workflow ID and key identify
the existing inspection route, whose full action is required for a decision.
The cursor orders by deadline, workflow ID and approval key.
[Inbox fields and availability](../../../docs/workflow-approvals.md#approval-inbox).

Matching components also support an optional exclusive `due_before`
(UTC epoch milliseconds) on the approval inbox and the exact-key
`GET /v1/console/workflows/local-steps/inspect` detail read. Both are additive.
[Local-step response and errors](../../../docs/console-query-model.md#inspect-one-local-step).

<span id="native-effect-resolution-in-development"></span>

## Native effect resolution

C6 introduced native effect waits, which use their parking
activation identity, with explicit registration and resumption edges. The
original effect activation remains a navigation reference. Assessment outcomes
include recorded success, recorded failure, closed unknown and disputed. Sensitive
results and reasons are available through audited effect inspection.

Ledgence 0.8.0 uses Console contract C7. It adds required, nullable `provider_wait`
to workflow detail and wait records, and `provider_probe` to task detail/status
and execution-history rows (null for workflows).
Provider wait nodes link their parking entrypoint to the one actually scheduled
resumption. Their occurrence points to the original approved operation. Metadata
contains selection kinds and revisions; provider results remain in audited Effects
inspection. Probe task completion records delivery of a report, not provider success.
Historical C6 fixtures remain unchanged for explicit compatibility rejection.

C6 adds a separate native-wait field to workflow detail and wait projections.
Its wait cursor distinguishes native and legacy waits even when their logical
keys and timestamps match. Apply the native effect-resolution migration and use
matching server and Console assets. Historical C5 fixtures remain unchanged.
Ledgence 0.7.0 uses C5.

## Durable resource admission (development)

C8 adds required, nullable `admission_wait` to workflow detail and wait history.
A native admission wait is keyed by its parking activation and carries its
original logical occurrence and resource. Its registration and actual scheduled
resumption establish graph relationships. An offer reserves capacity; it does
not prove that the callback ran.

An `admission_operation` node exists only after the original worker source
accepts a result. Its invocation edge belongs to that source's activation;
its reference keeps the original occurrence across rewaits and result replay.
The timeline shows result acceptance as a milestone, without inventing a callback
interval. Metadata omits effective arguments and output. Expanding the operation
uses audited, scope-free `POST /v1/console/admissions/inspect` and validates the
returned source against the selected node. Cancellation and administrative
release do not fabricate accepted-operation nodes. Historical C7 fixtures remain
unchanged and are rejected by a C8 browser.


C9 requires an admission wait `subject`: `{ "kind": "operation" }` or
`{ "kind": "provider", "ticket": { "occurrence": ..., "binding_digest": ... } }`.
Provider subjects retain the original approval occurrence. The selection kind
`provider` means a non-executable provider ticket was selected; it is not business
success. The on-demand native admission inspector accepts the strict disjoint
provider binding and linked provider evidence, keeping report receipts separate
from generic `finish` results. C8 fixtures remain historical and unchanged.

C10 adds a `task_admission` installation capability, the nullable task requirement,
admission deferral counts, `deferred` attempts and a scoped, audited task admission
inspector. C10 task read projections include `admission`/`admission_deferrals`
where applicable; ungoverned values are null/zero. Both Console and portable
task submission commands omit an unused requirement; explicit null is invalid. [`task-admission-v1.json`](../../../crates/ledgence-orchestration-api/tests/fixtures/task-admission-v1.json)
is the shared Rust ledger fixture used by Python and Console tests. C9 remains a
historical fixture; released Ledgence 0.8.0 remains C7.

The application service recomputes the source binding digest from the immutable
occurrence, resource and program before returning an inspected ledger. Console
validates the response shape, instance, generation, resource and cleanup evidence;
it displays the server-validated digest and does not independently hash the binding
in the browser. The inspection is read-only and cannot authorize a handler.

C11 adds the `queue_policies` capability and scoped queue policy, occupancy and hold projections. Policy and hold revisions and counters are decimal strings. Queue policy pages resume after a queue name and hold pages after an attempt ID. Mutation bodies bind the inspected revision and request ID. The Console server binds the instance scope and authenticated actor. The C10 fixture is retained for compatibility tests.

C12 adds the authenticated `resource_fair_sharing` capability and `POST /v1/console/resources/{list,inspect,decisions}`. The server binds the scope. These are metadata reads. Decisions are ordered by descending per-resource sequence. Use `next` as the exclusive `before` cursor. Sequence, offer generation and decision policy revision are decimal strings. The C11 fixture remains available.

C13 adds the authenticated `run_budgets` capability, root budget submission, and scoped `POST /v1/console/budgets/{inspect,entries}` metadata routes. The server binds instance scope and resolves the root using execution ownership. An immutable C12 fixture remains in the test corpus.
