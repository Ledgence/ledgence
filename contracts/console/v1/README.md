# Console HTTP contract

Portable DTOs are defined in `ledgence-orchestration-api::console`. Existing
client/worker HTTP contracts retain their previous serialization.

The canonical cross-language fixture is
[`console-v7.json`](../../../crates/ledgence-orchestration-api/tests/fixtures/console-v7.json).
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
`Ledgence-Instance-Id` and `Ledgence-Console-Contract: 7`. The browser rejects
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

The HTTP namespace remains `/v1/console`; the Console contract version is 7.
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
