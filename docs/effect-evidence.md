# Retained external-effect evidence

Available in Ledgence 0.9.0 with matching components and the required database
migrations. Existing ordinary programs retain their execution contract.

An approved external write can outlive its worker, workflow or execution-history
retention period. Ledgence keeps the approved occurrence, its program binding,
worker claims, reported results and operator assessments in an independent evidence ledger. Use it to
investigate what was authorized, which worker obtained permission and which
results were reported.

## Enable authenticated evidence

Stop the old workers and API replicas, back up PostgreSQL and run `ledgence
orchestrator migrate` using the configured database. Start matching 0.9.0
components with [instance access](instance-access.md) configured. The native
server requires the complete evidence, reader and operator-audit capabilities.

A credentialed worker calls `GET /v1/access` before starting its delivery driver.
It checks the configured scope and worker permissions, learns the explicit
instance/authority identity and binds subsequent evidence receipts to that
identity. The configured server URL is its initial connection destination.
The response advertises `runtime_capabilities.approved_effect_evidence_v2` only
when the complete capability is available.

A valid older authenticated server response can select the existing v1 protocol.
Invalid credentials, malformed discovery or an operator-trusted response to a
credentialed connection fail startup. Once v2 is selected, an unsupported or
failed v2 request remains an error. A mixed-version deployment must upgrade its
replicas consistently.

The workflow still calls `ctx.approved_effect(dispatch, lookup=read_receipt,
version="1")`. The Rust host passes the negotiated flag in the outer invocation
message. The activation schema and existing program artifacts keep their
original shape. Actual 0.7.0 helper bytes support `approved_local`. The later v1
helper supports `approved_effect`. Both historical helpers accept the additive
outer message while keeping their existing protocol.

## Read the evidence

The [CLI](cli.md), [Python client](../sdk/python-client/README.md) and
[MCP server](mcp.md) expose an inbox and inspection operation. Inbox metadata is
available in operator-trusted mode or with `metadata_read`. Inspection contains
approved arguments and reported output, so it requires configured credentials,
`sensitive_read` and a durable [operator audit](operator-audit.md) observation
before response bytes are released.

```sh
ledgence effect inbox --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --filter unconfirmed \
  --credentials-file ./operator.json
```

Copy the workflow ID, activation ID and approval key from the selected inbox
item. The activation identifies the original approved effect occurrence.

```sh
ledgence effect inspect --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing \
  --workflow wf_example --activation task_approved --key refund \
  --credentials-file ./operator.json --limit 4
```

The inbox supports `all`, `unconfirmed`, `confirmed`, `discrepant` and `disputed` filters,
with a limit from 1 to 100. Inspection accepts a page limit from 1 to 4 and
returns up to that many entries, including zero when appropriate. A 1 MiB
serialized response budget also applies. Follow `next_cursor` until it is null.
A short page can still have another page. Cursors bind the scope and query.

Generic HTTP routes are `POST /v1/workflows/approved-effects/inbox` and
`POST /v1/workflows/approved-effects/inspect`. Console aliases insert `/console`
after `/v1`. Console contract 6 adds an Effects inbox, inspection, assessment
review and links to verified audit events. Generic HTTP uses exact unsigned
64-bit JSON numbers. Console and MCP use canonical decimal strings for platform
revisions, sequences and counters (including program size); timestamps stay
numbers and application JSON remains unchanged.

## Interpret each record

- An occurrence uses the installation scope, workflow ID, original effect
  activation ID and approval key. New attempts retain that occurrence.
- A `claim_id` identifies one owned invocation. Its initial permission is retained.
  The original successful grant can permit dispatch. A repeat is lookup-only
  while the original live authority still holds.
- A `replay` claim receipt returns an already accepted journal record. The helper
  restores that record and returns its output without creating a callback report.
- An `observation_id` identifies a frozen report. Confirmation commits the local
  journal and its report together. Exact retries preserve the original receipt identity,
  timestamp and revision, and set `already_accepted` to true.
- `verified_worker_claim` identifies the authenticated worker and its reported
  callback mode, `dispatch` or `lookup`. An application adapter is responsible
  for interpreting the provider's response accurately.
- `legacy_journal`, `legacy_attempt` and `legacy_fence` describe the historical
  facts that survived upgrade. Unknown historical worker identity and callback
  mode remain unknown.

Credential rotation can preserve the worker's stable authority, instance, scope,
issuer and subject. Receipts retain the credential ID used when the record was
accepted. A different worker obtains its own live lookup claim.

`journal_state=confirmed` means Ledgence accepted a local result. A differing
later report sets `has_discrepancy` and remains visible beside the earlier
result. Inspection exposes safe identities and payloads. Lease ownership and
private frozen command bodies stay internal.

## Cancellation and separate observations

A managed workflow drains its owned calls before checkpointing and releases its
process under the existing cancellation rules. A killed process may lose a
provider result before it reports it.

An independently surviving caller with a real retained claim can use authenticated
`POST /v1/workflows/approved-effects/v2/observe`, or the portable Rust
`EffectEvidenceClient`, to preserve a frozen report after cancellation or lease
expiry. The server verifies the current worker identity against that claim.
This operation appends evidence only. Journal acceptance, execution permission
and continuation scheduling are separate operations.

The v2 worker protocol also exposes `claim` and `confirm` under the same path
prefix. Observation IDs share a namespace across confirmation and observation.
Changing the operation, payload, callback mode or stable actor under an existing
ID conflicts. Claim replies and confirmation acknowledgements are validated
against their complete request bindings before the helper uses them.

## Record an operator assessment

An uncertain external write may require investigation beyond a provider lookup.
A verified operator or service can record one of three explicit assessments:

| Assessment | Recorded meaning |
| --- | --- |
| `record_success` | The operator selected a JSON result for the reviewed action. |
| `record_failure` | The operator recorded a failure kind and message. |
| `close_unknown` | The operator closed handling without selecting success or failure. |

These are operator judgments, not independent verification of provider truth.
They never create another dispatch grant or confirm the execution journal.
Reported JSON remains opaque; Ledgence does not interpret keys such as `state`.

Resolution requires authenticated `effect_resolve`, separately from
`approval_decide`, execution control and `sensitive_read`. A worker credential
cannot resolve. New `access init` operator bundles include the capability;
existing credentials keep their previous permissions. A resolver restricted to
`effect_resolve` can submit a previously reviewed command and receive a
payload-free receipt. Reviewing evidence separately requires `sensitive_read`.

Use Console's review form or
`client.effects.prepare_resolution(detail, resolution_id=..., assessment=...,
reason=..., evidence_references=[...])`. The command freezes the original
occurrence, effective action, `expected_effect_revision` and nullable
`supersedes_resolution_id`. A reason of at most 4096 UTF-8 bytes is required.
Success/failure require at least one reference; up to eight ordered references
of 1024 UTF-8 bytes each are allowed. `close_unknown` permits none. Results are
bounded to 128 KiB, commands to 256 KiB and metadata receipts to 16 KiB.

Save `command.to_json()` before sending. This application-owned envelope contains
`format: "ledgence.effect-resolution.v1"`, the canonical `base_url`, and the
exact UTF-8 JSON body as a string. It contains sensitive reviewed action/result
data but no credential. The envelope is at most 1 MiB. Restoring validates the
original destination and scope without refreshing the action or revision.
`to_dict()` is an inspection view, not the saved format. The same saved file can
be sent through the CLI:

```sh
ledgence effect resolve --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file ./resolver.json \
  --file effect-assessment.json
```

The generic route is `POST /v1/workflows/approved-effects/resolve`; the Console
alias inserts `/console` after `/v1`, uses a scope-free occurrence and decimal
revision strings. MCP's `ledgence_effect_resolve` requires the explicit process
flag `--allow-effect-resolution` plus the credential capability. It is absent
from default/read-only catalogs; combining that flag with `--read-only` is invalid.
[Python examples](../sdk/python-client/README.md#operator-effect-assessments-development)
· [CLI](cli.md#submit-a-saved-operator-assessment) · [MCP](mcp.md#operator-effect-resolution-in-development).

Acceptance appends an immutable assessment, advances the effect revision and
writes its verified actor audit event in the same transaction. The receipt's
`acceptance_event_id` links that event. `client.audit.event(id)` or the audit
query's exact `event` filter (`limit=2`, no cursor) reads it without scanning an
entire execution's history. An unavailable audit is an error, never an unverified
successful assessment. A receipt proves acceptance, not resumed workflow execution.

If a reply is lost, resend the identical saved command using the same verified
actor. Credential rotation is allowed; the original acceptance retains the
original credential ID. A changed command or actor conflicts. Exact replay works
before revision checks, even after supersession, cancellation or collection,
provided the caller remains currently authorized. A new assessment must use a
new resolution ID, the latest reviewed revision and current predecessor ID.

The detail's `current_assessment` provides the latest full assessment even when
its entry is outside the requested timeline page. `operator_resolution` is the
payload-free summary. Neither field changes `journal_state`. All older entries
remain available. A current success assessment becomes `disputed` only when a
new, later JSON report differs from its selected result. Existing reports are
already covered by the reviewed revision. Failure and unknown assessments cannot
be semantically contradicted by interpreting arbitrary report contents.
`has_discrepancy`/`discrepant` remain historical reported-value disagreement.

## Park without occupying a worker

A matching 0.9.0 workflow returns
`ctx.wait_effect_resolution(key, resume=Entry.REVIEW, state=...)` after catching
`EffectOutcomeUnknown`. It checkpoints the state and drains owned calls through
the normal runtime lifecycle. Once parked, no worker process or open request
stays assigned. Ordinary work can use the same worker slot.

`ctx.effect_occurrence` names the original effect from the approved protected-effect or resolution
activation. Carry that typed occurrence explicitly when parking from a later
entrypoint. The server verifies the occurrence belongs to the workflow. Each
intentional native wait uses a new stable logical key. If a workflow is resuming
an assessment for the same occurrence, the helper defaults `after_resolution_id`
to that assessment, so a new wait requests a later assessment. It does not reuse
the earlier result indefinitely.

Acceptance can occur before or after the durable park. The coordinator selects
one assessment and the then-current evidence revision, then creates one resumed
activation with an immutable `ctx.effect_resolution`. Its typed outcome is
`RECORDED_SUCCESS`, `RECORDED_FAILURE`, `CLOSED_UNKNOWN` or `DISPUTED`. Later
assessments or reports never rewrite an already selected wake. A new wait is
required for a later selection. Attempt retries replay the same selected wake.
Generic workflow events use a separate namespace: even an equal textual key
cannot satisfy a native effect wait.

Cancellation and terminal execution prevent resumption. Historical assessment
and late-report acceptance can still enrich retained evidence without reviving
work. The platform stores the result and routes the native wake; application code
decides what each operator assessment means for its business workflow.
[Worker helper](../sdk/python/README.md#native-operator-rendezvous-development).

Native support is negotiated with `Ledgence-Worker-Features: 1` on the existing
worker access request. A complete deployment returns the versioned
`worker_features` set containing `native_effect_resolution.v1`. The Rust worker
passes the additive outer `runtime_features` field to the helper. Older servers
omit the manifest and older helpers ignore the new outer field. Calling the
native wait without negotiated support fails explicitly; there is no fallback
to generic events. The existing `runtime_capabilities` shape stays unchanged.

## Upgrade and retention

Migration runs a bounded, resumable backfill before startup. It preserves the
original approval, immutable program descriptor and exact accepted journal bytes.
A confirmed historical occurrence with missing or inconsistent proof fails
migration and requires restoring or repairing that proof. Data collected before
this upgrade is recoverable only from an existing backup.

Retained evidence uses independent tables and survives execution collection.
Native waits and their graph records follow execution-history retention; the
approved binding, reports, assessments and acceptance receipts remain available.
An unconfirmed occurrence without an assessment keeps its unresolved execution
retention hold. Any accepted assessment, including `close_unknown`, releases
that handling hold independently of journal confirmation. Later disagreement
does not reopen it or revive execution. Ordinary terminal state, age, owned-work,
quiescence and reference checks still apply before collection. The initial evidence ledger has no automatic
expiry, so operators should include its growth and backups in capacity planning.
Ordinary tasks, local results and queue polling use their existing paths.

## Verify the capability

The automated suites cover permission races, response loss, exact replay,
credential rotation, cancellation, retained reads, interrupted migration and
write rollback. Historical helper fixtures include their original MIT license
and source hashes. The full-stack evidence gate exercises real PostgreSQL,
authenticated workers and a synthetic provider with an independent mutation
counter. Provider-specific guarantees require qualification of that adapter's
own contract.

To reproduce the native gate, build the all-features CLI and install the current
Python client wheel in a dedicated environment. Set `LEDGENCE_POSTGRES_URL` to a
disposable PostgreSQL 18 server with permission to create test databases. The gate
creates its own database and removes it and the test credentials afterward.

```sh
python3.13 tools/check-effect-evidence.py \
  --binaries target/debug --python-client /path/to/client-env/bin/python \
  --evidence ./effect-evidence-check
```

The evidence directory records each completed scenario, cleanup and the binary
digest. The killed-worker scenario tests recovery by a distinct authenticated
worker. The separate late-observation scenario uses an independently surviving
caller after cancellation. It reports that caller's own provider lookup.


The native assessment gate additionally verifies worker-slot release at concurrency
one, generic event collision isolation, operator acceptance before and after park,
disputed selection and explicit follow-up waits, lost responses and same-actor
credential rotation, CLI/MCP/installed Python interoperability, cancellation and
real collection without revival. It uses a local provider with an independent
request counter; it does not claim provider-terminal interpretation or scalability.

```sh
python3.13 tools/check-effect-resolution.py \
  --binaries target/debug --python-client /path/to/client-env/bin/python \
  --evidence ./effect-resolution-check
```


<span id="typed-provider-evidence-in-development"></span>

## Typed provider evidence

Provider-protected operations share this Effects inbox, sensitive inspection,
operator assessment and audit contract. Their optional `provider` summary adds
knowledge, selected/conflicting report references, recovery budget/schedule and
handling state. The detail adds a `provider.current_report` header that agrees
exactly with the referenced retained report even when it falls outside the
current evidence page.

Timeline entries distinguish provider dispatch admission, probe scheduling,
probe callback admission, classified reports, probe finalization and recovery
stoppage. A source binds one actual admitted callback and one immutable report.
Cross-ingress replay retains its original acceptance source and revision.
A transport error cannot fabricate a classified provider outcome.

C operations leave `journal_state` at `unconfirmed`. Console labels this
**No local journal result**. Provider knowledge and handling are shown alongside
that independent field. A consistent terminal report or operator assessment can
close the independent provider handling hold. Evidence survives eligible
execution-history collection.

Provider results may dispute success or failure assessments when genuinely new
incompatible terminal facts follow their evidence basis. Duplicate old facts do
not create a new dispute. `close_unknown` makes no terminal outcome claim. An
already selected provider wake remains immutable.

Use the optional `provider_filter` with base filter `all`. Provider-filtered
cursors use version 2 and bind that filter. Omitting it preserves the existing
cursor contract. Inspect the complete provider contract for startup policy,
finite probe limits, wait selection, retention and declared-restore procedure.
[Provider operations](provider-operations.md).
