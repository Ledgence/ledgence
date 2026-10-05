# Recording the fulfillment investigation

**Problem:** delivery performance appears to have dropped. Is the carrier late,
or is the report missing warehouse data?

**Solution:** Ledgence brings the sources together, checks the evidence, and
coordinates independent investigations. A person approves the exact report
before it is published.

Keep those two statements ahead of code and infrastructure. The demo is about
getting a reliable answer from imperfect data. All source data is synthetic.
Fixture mode uses a scripted agent; Codex mode uses a real model on that same
synthetic evidence. Keep the appropriate label visible throughout the video.

## Prepare the recording

1. Follow [the tutorial](README.md#tutorial-run-the-investigation) with a fresh
   output directory, current-source components, and matching Console assets.
2. Run the example's checks. Retain their evidence and label native service
   validation separately from the offline simulated driver.
3. Use `data-gap` for the main story and `real-delay` for the comparison. Keep
   their workflow IDs and snapshots separate. Use a new idempotency key for a
   new take; retry the original key only to reconcile the same run.
4. A worker with one slot is sufficient. Additional slots allow independent
   branches to overlap, but footage must show their actual execution order.
5. Open the parent workflow in Console, collapse the sidebar, and expand the
   graph. Navigate into one branch at a time. Keep the actual source-ready
   signal and approval action available in a second window.
6. Inspect the report before approving it. Save the frozen decision command and
   the resulting local publication reference with the source footage.

There is no generated marketing video or live animated presentation in this
example. Use the real Console and saved report. If an edited animation is added
later, label it as an illustrative replay and retain the original evidence.

## Main story: about 90 seconds of edited footage

| Edited time | Show | Explain |
| --- | --- | --- |
| 0–10 s | One question: “Are deliveries late, or is the data late?” | A report can sound confident while working from an incomplete source. |
| 10–24 s | Four source branches in the parent graph. Briefly open warehouse. | Orders, warehouse, carrier, and support arrive independently. |
| 24–38 s | Quality result: 12 warehouse records missing; the parent waits. | The investigation stops before inventing a business explanation. The wait frees its invocation slot. |
| 38–48 s | Send the source-ready event; highlight warehouse reingestion. | Only the incomplete source is reloaded; acknowledged work is retained. |
| 48–64 s | Certified snapshot followed by four analysis branches. | Carrier, warehouse, source health, and support use the same dataset identity. |
| 64–77 s | One agent tool call and the verified report. | The agent receives bounded evidence. Computed metrics and references are checked before review. |
| 77–90 s | The report-bound approval, then local publication. | The reviewer approves this exact artifact. The saved decision resumes the workflow. |

These are editing targets, not execution-latency claims. Use visible cuts or a
“time condensed” caption when shortening waits. Do not manufacture simultaneous
branch completion or speed up a real model call without a caption.

## A short comparison

Run `real-delay` as a second segment: the source checks pass, and the independent
analyses support a constructed carrier-delay finding. Show the different result
using the same pipeline. This comparison establishes the value of the quality
gate: it can distinguish missing evidence from the fixture's real operational
signal. It does not validate a production carrier or establish a general
causal-diagnosis capability.

## Code close-ups

Use excerpts from the current checked-out source, with syntax highlighting and
at most one idea per frame:

- `ctx.fork` and `ctx.join`: independent source and investigation work.
- `ctx.wait_event`: stop for the missing source and resume a named entrypoint.
- `ctx.operation`: record a model or tool request and its accepted result.
- Report verification: bind numeric claims and references to the snapshot.
- `ctx.request_approval` and `ctx.approved_local`: review effective publication
  arguments and execute the saved action after approval.

Show the local artifact paths and digest only when they explain evidence
identity. Keep DSNs, credentials, setup logs, and long execution IDs out of the
main story.

## Evidence and claims to preserve

- Retain `prepared.json`, all referenced source/snapshot/report artifacts,
  workflow IDs, the decision file, and the original screen recording.
- Label a scripted agent as a fixture. Never relabel fixture output as a real
  Codex result or silently replace failed model output.
- The output is a local report, not a sent customer message or a deployed fix.
- Passing verification proves the implemented structured checks, not universal
  factual correctness of language-model reasoning.
- Recovery reuses acknowledged operations. A crash before acknowledgment can
  repeat an external call; no exactly-once provider guarantee is shown.
- Approval binds an artifact and action. Reviewer attribution alone is not
  authentication or an authorization policy.
- Branch overlap depends on worker capacity. A successful single-slot run is
  useful evidence that joins and waits do not require an extra parent slot.

A follow-up technical article can compare the two scenarios and show a fault
matrix: duplicate input, stale update, missing source, failed claim verification,
rejected approval, and repeated publication. Publish measured results only for
faults exercised by the retained test evidence.
