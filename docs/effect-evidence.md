# Retained external-effect evidence

This capability is available in matching development components. The published
0.7.0 packages retain their existing execution contract.

An approved external write can outlive its worker, workflow or execution-history
retention period. Ledgence keeps the approved occurrence, its program binding,
worker claims and reported results in an independent evidence ledger. Use it to
investigate what was authorized, which worker obtained permission and which
results were reported.

## Enable authenticated evidence

Stop the old workers and API replicas, back up PostgreSQL and run `ledgence
orchestrator migrate` using the configured database. Start matching development
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

The inbox supports `all`, `unconfirmed`, `confirmed` and `discrepant` filters,
with a limit from 1 to 100. Inspection accepts a page limit from 1 to 4 and
returns up to that many entries, including zero when appropriate. A 1 MiB
serialized response budget also applies. Follow `next_cursor` until it is null.
A short page can still have another page. Cursors bind the scope and query.

Generic HTTP routes are `POST /v1/workflows/approved-effects/inbox` and
`POST /v1/workflows/approved-effects/inspect`. Console aliases insert `/console`
after `/v1`; their integer revisions and sequences use decimal strings. The
current Console UI continues to show approval attribution. The read endpoints
are available for clients building an evidence view.

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

## Upgrade and retention

Migration runs a bounded, resumable backfill before startup. It preserves the
original approval, immutable program descriptor and exact accepted journal bytes.
A confirmed historical occurrence with missing or inconsistent proof fails
migration and requires restoring or repairing that proof. Data collected before
this upgrade is recoverable only from an existing backup.

Unconfirmed effects retain the existing execution-retention hold. Confirmation
allows ordinary execution history to collect. Retained evidence uses independent
tables and survives that collection. The initial evidence ledger has no automatic
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
