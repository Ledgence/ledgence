# Execution retention and bounded cleanup

Ledgence retains active execution state and at least **90 days after terminal
state**. Cleanup is opt-in, scoped to one tenant and namespace, and never starts
merely because an API reads an old execution. The server does not schedule it.
Operators can invoke the maintenance command from their existing job scheduler.

## Preview and apply

Apply the current schema explicitly before using maintenance:

```sh
ledgence-orchestrator migrate
ledgence-orchestrator retain --tenant acme --namespace billing
ledgence-orchestrator retain --tenant acme --namespace billing --apply
```

`DATABASE_URL` selects the PostgreSQL deployment. Both scope flags are required;
there is no wildcard or whole-database retention command. The first `retain`
command is read-only. It logs bounded samples of old task/workflow candidates,
expired sessions, and already-retiring executions. A candidate can still have
protective references and is not a promise that it will be removed.

`--apply` irreversibly accepts retirement and performs bounded cleanup. Options:

| Option | Default | Accepted range |
| --- | --- | --- |
| `--retain-days` | 90 | At least 90 whole days, within the timestamp range |
| `--batch-size` | 128 | 1–256 dependent rows per deletion page |
| `--batches` | 100 | 1–100000 bounded transactions per invocation |

One transaction examines or advances one target/session; discovery and physical
collection alternate. A page removes at most `batch-size` history/receipt rows.
Admission may additionally remove the existing bounded set of at most 16 terminal
subscriptions; final collection removes at most three identity/link records.
Progress logs report examined/retired records, deleted rows/executions/sessions,
and completed transactions. Scan metadata is not included in deletion counts.
An empty/protected page does not establish that the scope is fully collected.
Rerun the same command to continue; durable scan positions prevent permanently
blocked old records from starving unrelated eligible work.

Each database operation keeps the normal 30-second total request budget and
statement/lock limits. SIGINT/SIGTERM stops further admission; an interrupted
transaction rolls back, while previously committed retirement continues on the
next invocation. A lost commit reply can undercount progress in that invocation;
rerunning cannot reexecute a program. Changing the age policy does not undo
already accepted retirement.

## What cleanup protects

- Queued/active tasks, active attempts, and unprocessed workflow coordination work.
- Current consumer cursor references. A live idle consumer keeps the complete
  task/attempt snapshot until it advances. Its sequence is never reset.
- Expired-session cursors until their active ownership has been reconciled.
  Historical dispatch claim receipts retain their target's execution lifetime.
- A workflow's durable local results, event receipts, waits and owned tasks while
  the workflow or any ancestor root is active. An owned run is eligible only once
  both it and its root are terminal and old enough; deletion proceeds leaf-first.
- Completion subscriptions in waiting, pending, delivering or retrying states.
  Delivered/exhausted subscriptions and their target remain for at least the
  configured period after the latest delivery/exhaustion timestamp. Thus manual
  redelivery remains possible during that window and restarts protection when
  accepted. Missing destination configuration never makes pending work disposable.

Retirement locks the execution before its subscriptions. Late registration and
manual redelivery either commit first and protect the target, or observe that its
retention lifetime ended. They cannot succeed against removed obligations.
Cleanup never changes an execution outcome or runs its program again.

Retention is per target, **not whole-tree archival**. Once the root is terminal
and old enough, an eligible descendant can expire while a root callback keeps the
root's own self-contained result available. The root's status/result remain
unchanged; lineage IDs and retained history can refer to a child whose inspection
now returns `NotFound`. A child's own pending/recent callback protects that child.

## API and idempotency boundary

Retirement first hides the expired target as a whole, then removes its dependent
rows in pages. Task/workflow status, result, history, events and new subscriptions
return the existing `NotFound` error for that target. Task discovery excludes it.
Expired completion subscriptions also return `NotFound`, including manual retry.
A read whose database snapshot precedes retirement may still return the complete
old snapshot; readers never receive a partially collected success result.

During physical collection, the submission key still occupies its unique binding;
a same-key submit/lookup can return `NotFound`. Once final identity deletion commits,
the same submission key may create a **new task or root workflow**. Deduplication
therefore covers active lifetime and the retained terminal window; it is not
permanent business idempotency. Applications that need permanent protection must
record their effect key in the business system.

Old consumer sequences cannot become new assignments. For a live consumer whose
historical broker receipt expired, replay returns `Conflict`/`ObsoleteOperation`;
an expired removed session returns `UnknownSession`. A broker record for a deleted
execution returns `NotFound` and conveys no execution or acknowledgment authority.
Keep broker delivery/reconciliation lifetimes within execution retention and use
its operational dead-letter handling for records retained beyond that boundary.

## Storage and operations

Cleanup uses identity-prefixed indexes, keyset scans, a small per-scope coordination
row, and target row locks. Cooperating collectors serialize within a scope and
independent scopes can proceed concurrently. There are no unbounded cascading
transactions, graph walks, `OFFSET` scans, per-wait processes, or extra writes on
normal execution paths. Index maintenance and compact retirement metadata still
have a storage/write cost; this feature makes no throughput claim.

Migration `20260923000000_retention.sql` adds marker columns and indexes. Index
creation may take time and block writes on an existing large deployment: run the
explicit migration in a planned maintenance window with an appropriate budget.
The serving binary still verifies the exact schema before starting.

PostgreSQL autovacuum reclaims dead tuples for reuse; deletion does not promise an
immediate reduction in filesystem size. Monitor table/index growth, autovacuum and
transaction age. This command does not run `VACUUM FULL`, change database tuning,
archive records, remove published program packages, delete worker artifact caches,
or change logs/traces stored in another system. Those stores need their own
operator-controlled lifecycle.

## Verification

Real PostgreSQL tests cover the 90-day floor, read-only scoped preview, bounded
pages and index plans, restart by reopening the adapter, rollback, concurrent
collectors, cursor/active-attempt protection, historical claim/event receipts,
late subscriptions, redelivery races, and owned workflow/local-journal cleanup.
A regression verifies that a pending root callback keeps an identical public
root status/result and still delivers after expired descendants are removed.
