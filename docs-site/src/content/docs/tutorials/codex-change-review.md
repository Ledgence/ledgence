---
title: From bug report to approved change
description: Follow a Codex candidate through three workflow branches, a measured comparison, and a durable human approval step.
---

An order of exactly **$100** should receive free shipping, but a small calculator
charges **$5**. Codex proposes a fix. Ledgence coordinates the checks, gathers
the evidence, and waits for a decision tied to that exact candidate.

The business problem fits on one screen. The execution shows what happens when
an agent needs more than a single model response: independent work, explicit
entrypoints, durable results and a human decision.

**Availability:** this tutorial follows application packages **1.1.0** in the
[current source](https://github.com/Ledgence/ledgence/tree/develop/examples/codex-change-review),
using the **Ledgence 0.2.0 runtime and client**. These demo changes were added
after the 0.2.0 release. The original example in the `v0.2.0` source tag uses
application 1.0.0 and has a different, smaller flow. Use the current example
source and prepare fresh packages; see [releases and packages](/reference/releases).

## Follow one candidate

| Moment | What runs | What becomes inspectable |
| --- | --- | --- |
| Propose | A task asks Codex for a small change to the bundled module. | Exact source, canonical patch, base/candidate SHA-256 and reported Codex usage. |
| Fork | Three owned workflow branches run tests, an independent Codex review and a Codex release-note draft. | Separate execution histories, all bound to the same candidate. |
| Continue locally | The parent measures both source versions after its fork is acknowledged. | Actual shipping costs at $99.99, $100 and $100.01. |
| Join | The parent checkpoints and waits for every branch's terminal outcome. | All three reports together with the measured comparison. |
| Inspect | The `prepare:0` task assembles a review packet. | A portable HTML report available before the parent finishes. |
| Decide | An event approves or rejects the exact workflow/candidate pair; a deadline can expire. | A recorded decision that resumes a named entrypoint. |
| Finish | A final task assembles the evidence. | Approved, rejected or expired result; optional draft PR only with explicit configuration and approval. |

Failed tests, a wrong measured result or requested review changes produce
`needs_changes` before the human wait. Provider or execution failures remain
failed executions; they are not represented as successful checks.

## Read the orchestration

The [workflow source](https://github.com/Ledgence/ledgence/blob/develop/examples/codex-change-review/program.py)
has six main entrypoints: `start`, `check_candidate`, `prepare_review`,
`await_decision`, `on_decision` and `finish`. Three branch entrypoints run one
level below the parent: `run_tests`, `review_code` and `draft_note`.

The central pattern is:

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
return ctx.join(checks, resume=Entry.PREPARE_REVIEW,
                state={"candidate": candidate, "comparison": comparison})
```

The runnable handlers add explicit attempt limits, timeouts and validation.
`fork` acknowledges durable registration before local work continues. It does
not promise that all branches have already started. `join` releases the parent
invocation and resumes only after every branch is terminal, including failures.
The setup uses one control slot and two agent slots; limited capacity can
serialize branches. See [fork and join](/how-to/fork-workflow-branches).

After successful checks, the parent waits for a bound event:

```python
return ctx.wait_event("approval:0", continuation=Entry.ON_DECISION,
                      state={"bundle": bundle}, timeout_ms=approval_timeout_ms)
```

The next entrypoint checks the workflow identity, candidate SHA-256 and boolean
decision against the saved packet. Approval cannot replace the candidate or
override failed evidence. Rejection or timeout never enables publication.

## Run and inspect it

Follow the [example README](https://github.com/Ledgence/ledgence/blob/develop/examples/codex-change-review/README.md)
for packaging, local PostgreSQL, worker commands and the companion client.
It uses CPython 3.13, the public Python client, and no application dependencies
beyond the standard library. Codex is an optional host integration.

- **Offline acceptance** uses real Ledgence services and a clearly labelled
  Codex protocol fixture. It checks orchestration and recovery without a provider.
- **Live Codex** uses the authenticated host CLI to propose, review and describe
  the change. The synthetic source and prompts are sent to OpenAI, and account
  access and usage limits apply. The normal path has three CLI invocations.

In [Console](/tutorials/use-console), follow the parent graph and enter each
branch to inspect its own level. When `prepare:0` succeeds, copy that task's ID
from Children and run `client.py review --task TASK_ID --output NEW_DIRECTORY`.
This reads an ordinary public task result while the workflow waits for approval.
Open `review.html`, inspect the evidence, then use the documented `approve` or
`reject` command with the exact workflow ID, candidate digest and stable event ID.
Console's generic **Send event** action is not a dedicated approval UI.

The report shows measured before/after values, the patch, six predefined tests,
independent review, draft note and decision identity. Source, patch, complete
JSON evidence and a review description accompany it. Export `result` after the
decision to see the final report; the pending HTML is a static snapshot.

## What this demonstrates

Workers do not share a mutable checkout. Each report validates the immutable
candidate digest. Codex does not write the product repository; it returns
structured source and text, while the application runs the predefined tests and
comparison in bounded subprocesses. This example assumes operator-trusted code,
not an untrusted-code sandbox.

Waiting for branches or a human event holds no parent invocation slot, although
a healthy process can remain warm. Accepted results survive recovery; a provider
call interrupted before acceptance may already have consumed usage. Stable
command identities and explicit recovery checks do not turn external effects
into exactly-once operations.

By default, approval stops at the local evidence bundle. An explicit target
configuration can publish a reconciled draft PR to a dedicated sample repository
after human approval. Nothing merges or deploys automatically.

The [recording guide](https://github.com/Ledgence/ledgence/blob/develop/examples/codex-change-review/DEMO.md)
provides a short shot list using this real execution. It separates edited video
duration from actual model latency and keeps fixture output visibly labelled.
