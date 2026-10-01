---
title: From bug report to reviewed change
description: Let Codex propose a fix while Ledgence coordinates local tests, an independent distributed review, and an optional draft pull request.
---

A useful coding agent needs more than a convincing patch. It needs tests against the intended behavior, an independent review of the same code, and a clear result a person can inspect.

This example fixes a small shipping calculator: orders of exactly **$100** should receive free shipping, but the original implementation charges **$5**. The workflow creates a candidate, runs two checks in parallel, joins their results, and prepares the change for human review.

**Availability:** this example is included in Ledgence 0.2.0. Use its [`examples/codex-change-review/`](https://github.com/Ledgence/ledgence/tree/v0.2.0/examples/codex-change-review) directory, matching workers and orchestrator, and all database migrations. See [releases and packages](/reference/releases).

## Follow the work

| Stage | Execution | Result |
| --- | --- | --- |
| Implement | A task asks Codex for a small change to the bundled Python module. | Candidate source, canonical patch, base and candidate digests, and reported Codex usage. |
| Test | The parent runs predefined regression tests locally. | Actual test results for the exact candidate. |
| Review | A distributed branch starts a fresh Codex session. | Structured findings about that same candidate. |
| Join | Ledgence saves the local result and waits for the review outcome. | Both results available to the next entrypoint. |
| Finalize | A final task assembles the evidence and PR description. | `ready_for_review` or `needs_changes`; optionally a GitHub draft PR. |

The local test branch occupies its current worker slot while testing. The review has its own workflow identity and runs on the agent queue. Two available worker slots permit overlap; a fork acknowledgment confirms durable registration, not that a reviewer has already started. Once the parent returns its join, waiting does not occupy a parent invocation slot.

## Read the workflow

The [complete workflow](https://github.com/Ledgence/ledgence/blob/v0.2.0/examples/codex-change-review/program.py) uses enum-addressed handlers for `START`, `VALIDATE`, `REVIEW`, `COLLECT`, and `FINISH`. Implementation, testing, review, and publication helpers are separate from orchestration.

The central pattern is:

```python
reviews = await ctx.fork("validate:0", branches=[
    ctx.branch("review:0", entrypoint=Entry.REVIEW, queue=AGENT_QUEUE,
               data={"candidate": candidate, "model": model}),
])
tests = await ctx.local("tests:0", run_tests, candidate=candidate)
return ctx.join(reviews, resume=Entry.COLLECT,
                state={"candidate": candidate, "tests": tests})
```

The runnable handler also sets an explicit review attempt limit and timeout. `ctx.local()` persists the test result before returning. `ctx.join()` saves that result and waits for the remote branch; local work is not a second queue item. A review that finishes before the parent joins is still observed. Read the [fork and join contract](/how-to/fork-workflow-branches) for the precise lifecycle.

The candidate travels as bounded JSON, tied to a bundled base and verified by digest in each step. Workers need no shared writable checkout. The tests are supplied by the example, independently of the generated code, and each execution uses a fresh temporary directory. The final task verifies that the test and review reports identify the same candidate before deciding its status.

## Run it

Follow the [example README](https://github.com/Ledgence/ledgence/blob/v0.2.0/examples/codex-change-review/README.md) for preparation, local PostgreSQL, worker commands, the client, and verification.

The example has two explicit execution modes:

- **Offline acceptance:** real Ledgence orchestration and workers, with a deterministic Codex protocol fixture. It checks coordination and recovery without a provider account.
- **Live Codex:** the real host-installed CLI proposes and reviews the change using its existing ChatGPT sign-in and configured model. Account access and usage limits apply. The synthetic calculator and instructions are sent to OpenAI.

The standard-library application packages need no application dependency installation. The host supplies CPython 3.13 and Codex. The companion uses the public `ledgence.client` SDK.

## Inspect the outcome

The exported review bundle contains the proposed `shipping.py`, `change.patch`, `review.json`, and `pull-request.md`. Passing tests plus an approving review yield `ready_for_review`. A failed assertion or requested change yields `needs_changes`. Provider or execution failures remain explicit failed tasks or branches; they are not reported as successful reviews.

Inspect the workflow in [Console](/tutorials/use-console) to follow the implementation task, owned review branch, acknowledged local test result, finalization task, and terminal result. Execution and tracing identifiers stay in Ledgence's envelope; `change_id` is application data and also the submission's correlation key.

## Publish deliberately

The default result is a local review bundle. An explicit publication configuration enables a draft PR against a dedicated GitHub sample repository and a pinned base commit. The publisher changes only `shipping.py`, checks its base content, and reconciles a deterministic branch and matching PR after an uncertain response. Human review and merging remain separate actions.

Accepted task and local-step results survive workflow recovery. A provider call interrupted before its result is accepted can still have consumed usage. External GitHub effects require reconciliation; a durable workflow does not turn them into exactly-once operations.

Start with one candidate per run. A later repair iteration should create a new candidate identity and repeat both validation branches so test evidence and review always describe the code being proposed.
