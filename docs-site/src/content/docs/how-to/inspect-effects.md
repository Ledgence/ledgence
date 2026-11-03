---
title: Inspect external-effect evidence
description: Find approved external writes and inspect retained worker claims and reported results.
---

Available in matching development components. The published 0.7.0 packages keep
their existing execution contract.

When a worker disappears during an external write, start with its retained
evidence. The inbox identifies the approved occurrence. Inspection shows the
approved arguments, program, worker claims and reported results, including
reports retained after execution history has been collected.

## Prepare access

Upgrade and migrate all API replicas and workers together. Configure an operator
credential with `metadata_read` and `sensitive_read`, and give the worker its
own `worker_execute` credential. The native server records a durable audit event
before returning sensitive inspection data.
[Configure instance access](/how-to/configure-instance-access).

## Find an unconfirmed write

Run this command from the directory containing your operator credential file.
Replace the tenant and namespace with your installation's values.

```sh
ledgence effect inbox --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing \
  --credentials-file ./operator.json --filter unconfirmed
```

Each item contains an `occurrence`. Copy its `workflow_id`, `activation_id` and
`approval_key` into the next command. The activation identifies where this
approved effect began, even when a later worker recovered its result.

```sh
ledgence effect inspect --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file ./operator.json \
  --workflow wf_example --activation task_approved --key refund --limit 4
```

Use `--cursor` with the returned `next_cursor` to read another page. Continue
until the cursor is null. Large entries can make a page shorter than its limit.

## Understand the result

| Field or entry | Meaning |
| --- | --- |
| `initial_permission` | The dispatch or lookup permission originally recorded for that claim. |
| `verified_worker_claim` | The authenticated worker and the callback mode it reported. |
| `legacy_journal` | A historical accepted result whose original worker identity was unavailable. |
| `journal_state` | Whether Ledgence accepted a durable local result for the occurrence. |
| `has_discrepancy` | Different reported values exist and need investigation. |

A dispatch grant records permission. A provider's actual outcome comes from the
application adapter's observations. An unavailable lookup or failed connection
can leave that outcome unknown. Cancelling an execution keeps its retained
evidence available.

Python exposes `client.effects.inbox()` and `client.effects.inspect()`.
MCP exposes `ledgence_effect_inbox` and `ledgence_effect_inspect` with the same
access checks. Metadata inbox queries also work in operator-trusted deployments.
Sensitive inspection requires credential mode and durable audit.
[Python client](/reference/python-client) · [MCP setup](/how-to/connect-mcp).

The evidence ledger has no automatic expiry in this development capability.
Include it in storage planning and database backups. See the source contract
for migration and callback details.
[Retained evidence contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md).
