---
title: From an empty page to a reviewed fix
description: Follow a small Codex change through an animated workflow replay, three independent branches, and a human decision.
---

**The problem:** a document search offers two pages for exactly 100 results.
Every result fits on the first page; the second page is empty.

**The solution:** Codex proposes a small pagination fix. Ledgence coordinates
three independent checks, measures the behavior, and waits for a person's
decision tied to that exact candidate.

The exported `review.html` leads with this problem and solution, then explains
the workflow through an animated diagram and short code excerpts. You can pause
or step through the explanation. Selecting a node pauses playback at that step;
use **Play** to resume. This is an **illustrative replay of saved evidence**,
not a live service connection.
Offline fixture output has its own visible label.

**Availability:** this tutorial follows application packages **1.2.0** included
in [Ledgence 0.3.1](https://github.com/Ledgence/ledgence/tree/v0.3.1/examples/codex-change-review).
The original `v0.2.0` tag used application 1.0.0; application 1.1.0 used the previous
fixture. Prepare fresh packages instead of replacing an existing immutable version.
See [releases and packages](/reference/releases).

## Understand the change

The bundled `pagination.py` uses a fixed page size of 100. Its original
`page_count(item_count)` adds an extra page at exact boundaries:

```python
return item_count // 100 + 1     # Original counting logic
return (item_count + 99) // 100   # Intended counting logic
```

Codex proposes the candidate; the application runs fixed tests and measures both
versions in bounded subprocesses. The comparison covers 99, 100 and 101
documents: the intended page counts are **1, 1 and 2** respectively. Regression
tests also cover zero documents and invalid inputs.

## Follow the workflow

| Step | What happens | What you can inspect |
| --- | --- | --- |
| Propose | A task asks Codex for the small change. | Source and the exact patch. |
| Fork | Three branches run tests, an independent Codex review and a Codex release-note draft. | A separate result for each branch. |
| Continue locally | The parent compares both source versions after its fork is acknowledged. | Measured page counts before and after. |
| Join | The parent checkpoints and waits for every branch's terminal outcome. | All reports bound to the same candidate. |
| Decide | A passing candidate waits for an approval or rejection event. | The reviewed candidate and the recorded decision. |
| Finish | A final task assembles the evidence. | The outcome and a portable presentation. |

Failed tests, an incorrect measured result or requested review changes produce
`needs_changes` before the human wait. Provider or execution failures remain
failed executions. No result is promoted to a passing check for the demo.

## Fork, then keep working

The [workflow source](https://github.com/Ledgence/ledgence/blob/v0.3.1/examples/codex-change-review/program.py)
uses explicit entrypoints. The central pattern is short:

```python
checks = await ctx.fork("checks:0", branches=[
    ctx.branch("tests:0", entrypoint=Entry.RUN_TESTS, queue=CONTROL_QUEUE,
               data={"candidate": candidate}),
    ctx.branch("review:0", entrypoint=Entry.REVIEW_CODE, queue=AGENT_QUEUE,
               data={"candidate": candidate, "model": model}),
    ctx.branch("note:0", entrypoint=Entry.DRAFT_NOTE, queue=AGENT_QUEUE,
               data={"candidate": candidate, "model": model}),
])
comparison = await ctx.local("compare:0", compare_candidate, candidate=candidate)
```

The runnable handlers add attempt limits, timeouts and validation. `fork`
acknowledges durable registration before local work continues. It does not
promise that all branches have already started. One control slot and two agent
slots allow work to overlap; limited capacity can serialize branches.
See [fork and join](/how-to/fork-workflow-branches).

## Join the evidence

```python
return ctx.join(checks, resume=Entry.PREPARE_REVIEW,
                state={"candidate": candidate, "comparison": comparison})
```

`join` releases the parent invocation and resumes the named entrypoint after
every branch is terminal, including failed or cancelled branches. The handler
checks those outcomes before building the review packet. Every report validates
the same candidate SHA-256.

## Wait for a person's decision

After successful checks, the parent waits for a bound event:

```python
return ctx.wait_event("approval:0", continuation=Entry.ON_DECISION,
                      state={"bundle": bundle}, timeout_ms=approval_timeout_ms)
```

The next entrypoint checks the workflow identity, candidate digest and boolean
decision against the saved packet. Approval cannot replace the candidate or
override failed evidence. A rejection or expired deadline never enables
publication.

The full parent path has six entrypoints: `start`, `check_candidate`,
`prepare_review`, `await_decision`, `on_decision` and `finish`. The branch
entrypoints—`run_tests`, `review_code` and `draft_note`—run one level below it.

## Run and inspect it

Follow the [example README](https://github.com/Ledgence/ledgence/blob/v0.3.1/examples/codex-change-review/README.md)
for packaging, local PostgreSQL, worker commands and the companion client.
It uses CPython 3.13, the public Python client, and the standard library. Codex
is an optional host integration.

- **Offline acceptance** uses real Ledgence services and a labelled Codex
  protocol fixture to check orchestration and recovery without a provider.
- **Live Codex** uses the authenticated host CLI to propose, review and describe
  the change. The synthetic source and prompts are sent to OpenAI; account
  access and usage limits apply. The normal path has three CLI invocations.

In [Console](/tutorials/use-console), follow actual execution in the parent graph
and enter a branch to inspect its level. When `prepare:0` succeeds, copy its
Children task ID and run `client.py review --task TASK_ID --output NEW_DIRECTORY`.
This retrieves a public task result while the parent waits for approval.

Open `review.html` and explore the guided replay. The presentation keeps full
evidence in an expandable section, with source, patch and JSON files alongside
it. Its controls only navigate the explanation. Use the companion `approve` or
`reject` command with the exact workflow ID, candidate digest and a stable event
ID to record a decision. Console's generic **Send event** action is not a
dedicated approval UI.

Export `result` afterward to view the final presentation. The earlier pending
HTML remains a snapshot and does not fetch a newer result. The animation uses
editorial pacing and must not be presented as real execution timing.

## What this demonstrates

Workers do not share a mutable checkout. Codex returns structured source and
text; the application runs predefined tests and a comparison in bounded
subprocesses. This assumes operator-trusted code, not an untrusted-code sandbox.

Waiting for branches or a human event holds no parent invocation slot, although
a healthy process can remain warm. Accepted results survive recovery; a provider
call interrupted before acceptance may already have consumed usage. Stable
command identities and recovery checks do not make external effects exactly-once.

Approval stops at the local evidence bundle by default. An explicit target
configuration can publish a reconciled draft PR to a dedicated sample repository
after human approval. Nothing merges or deploys automatically.

The [recording guide](https://github.com/Ledgence/ledgence/blob/v0.3.1/examples/codex-change-review/DEMO.md)
turns the brief problem, workflow and code excerpts into a short demo. It keeps
illustrative playback, offline fixtures and actual Console execution distinct.
