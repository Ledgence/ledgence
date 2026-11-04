# Shared execution budgets

Ledgence 0.9.0 supports a run-wide allowance declared by its root submission. A workflow, its child workflows, and their owned tasks share one PostgreSQL ledger. A standalone task can start its own budget when its handler uses task admission. The program's CloudEvent `data` remains application-owned. An owned task created with `ctx.task` inherits the root identity for inspection and costs one delegation. That helper does not expose standalone task admission, so arbitrary network requests inside the task are outside the governed Start boundary. To charge a physical call made by a child, invoke an owned workflow with a registered admitted operation, as in the example.

## Declare an allowance

Use the optional root input `budget` object. Set at least one of `max_calls`, `max_delegations` or `deadline_at_ms`. An omitted dimension is unlimited. A zero count prevents that action. The absolute deadline uses Unix epoch milliseconds. An omitted budget preserves an ordinary submission's identity. Retrying the same submission with a different budget under the same idempotency key conflicts. The server derives ownership, including through nested forks, from accepted execution records.

## When an allowance is spent

| Event | Accounting |
| --- | --- |
| A workflow durably creates an owned task or subworkflow | Charge one delegation per new child in its creation transaction. |
| A worker receives an offer, prepares its process or replays a retained result | No new call charge. |
| The orchestrator accepts a governed Start or grants instrumented provider dispatch | Charge one call in the transaction authorizing that physical dispatch. |
| The worker loses the Start reply or external completion is uncertain | Preserve the original charge. A replay cannot grant a second dispatch. |
| A fresh physical attempt receives a new authorization | Record a separate charge. |
| A deadline or count prevents new governed work | Record a typed refusal. Waiting for resource capacity and exhausting a run allowance have different outcomes. |

Applications can inspect the root's decisions to identify the execution that spent the last unit. Provider-reported and estimated token use are distinct from unknown use. There is no universal price or token ceiling for arbitrary Python programs. The operator must control all physical sends for a physical-call guarantee. Retries or redirects inside a vendor SDK and direct network calls outside a governed boundary are outside its accounting. A deadline fences future actions. Work authorized before the deadline can still complete, and settlement can still be recorded.

## Inspect a run

`POST /v1/budgets/inspect` reads one scoped snapshot. For the native route the complete body is `{"scope":{"tenant_id":"acme","namespace":"demo"},"subject":{"kind":"workflow","workflow_id":"wf_123"}}`. Console injects scope from its authenticated instance. An ordinary or pre-budget execution has no budget record and returns 404. The server resolves the immutable root. `POST /v1/budgets/entries` adds `before` and `limit` to the same query. Start with `before: null` and follow the exclusive `next` cursor. Positive sequences serialize as decimal strings. Inspection is a metadata operation.

[Run two real ticket reviews with a single allowed call](../examples/shared-run-budget/README.md).

## Migration and recovery

Upgrade all orchestrator replicas, apply migrations and enable the matching admission configuration before accepting budgeted roots. Use compatible workers for governed programs. A previous orchestrator cannot enforce limits introduced after its release. Do not downgrade a live budgeted tree. Preserve the budget authority, immutable ownership and receipts in backup/retention. A restore may omit newer external operations. Quarantine governed dispatch before bringing a restored database online and reconcile it before re-enabling new calls. A missing charge in an older backup cannot prove that a provider call never happened.

A budgeted native provider or approved-effect claim requires a worker that enforces the returned send window. Legacy workers can read existing evidence. They cannot obtain fresh budgeted grants on those routes. The scope of a reported token observation is the adapter’s report; Ledgence does not independently meter a vendor account. The immutable budget membership and journal outlive retired execution details and grow with the authorized and refused operations.

Configured count limits are integers from zero to 1,000,000. The snapshot serializes accumulated calls and delegations as canonical decimal strings so clients preserve large counts. The Python SDK exposes both as integers. The decision for a generic approved effect identifies its original approval occurrence, including workflow, activation and approval key. Console distinguishes workflow budget support from standalone task budget submission on custom adapters.

Cancelling a budgeted root fences further governed starts and delegations in its owned tree. Cancelling one child fences its subtree while its siblings can continue. Finishing an already authorized call can still record usage. An unused numeric balance on a stopped execution does not permit another Start.

The journal records `calls_exhausted`, `delegations_exhausted`, `deadline_exceeded` and `cancelled`. A negotiated HTTP rejection returns 409 with `code: "run_budget_denied"` and the corresponding journal reason in `message`. Worker diagnostics use `budget_calls_exhausted`, `budget_delegations_exhausted`, `budget_deadline_exceeded` and `budget_cancelled`. A legacy worker receives a permanent `invalid_input` with that diagnostic code as its message. These are permanent refusals for that action. Console advertises `run_budgets` for workflow budgets and metadata and `task_run_budgets` for independently submitted budgeted tasks.
