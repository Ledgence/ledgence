# Recording the Ledgence workflow demo

**Problem:** a document search offers a second, empty page for exactly 100 results.
**Solution:** Codex proposes a pagination fix; Ledgence runs independent checks,
collects the evidence, and waits for a person's decision.

Lead with those two sentences. Let the workflow explain the product, and reveal
code when it explains a step. Keep the full evidence available without making
viewers read a report before they understand the example.

## Prepare the presentation

1. Follow the [README setup](README.md) using application packages `1.2.0`, a
   control worker with one slot and an agent worker with two slots.
2. Pass the offline acceptance gate. For a video demonstrating real Codex output,
   use the authenticated Codex executable for the source run. A fixture-based
   preview must retain its offline label.
3. Export the pending packet with `client.py review`, inspect the candidate, and
   record a decision with the companion `approve` or `reject` command. Export
   the final bundle with `client.py result`.
4. Open the final `review.html` in a browser. Its animated diagram is an
   **illustrative replay** of saved evidence. It does not connect to the running
   workflow, reproduce actual timing, or send approval events. Keep that label
   visible when recording the animation.
5. Use Console if the video includes actual execution. Collapse its sidebar,
   expand the graph and inspect one workflow level at a time. Keep this footage
   distinct from the guided replay.
6. Record at a consistent, readable size such as 1440 × 900. Leave optional
   GitHub publication unconfigured: the story ends at the reviewed change.

Retain the source bundle, workflow ID and original footage beside the edited
video. Use fresh change IDs and idempotency keys for intentional new takes.
Repeating identical submission arguments reconciles the same run.

## A short, clear story

Aim for an edited video of roughly **90–100 seconds**, not a claim about model
or workflow latency. The viewer should understand the problem before seeing an
execution identifier or a code block.

| Edited time | Show | Narration |
| --- | --- | --- |
| 0–12 s | The brief problem and solution: 100 documents, one page of results, an unnecessary empty second page. | “All the results fit on one page. The app still offers an empty page. Let's fix that.” |
| 12–25 s | Start the guided replay. Select the candidate node and reveal the small pagination change. | “Codex proposes a change. Ledgence carries this exact candidate through every check.” |
| 25–48 s | Animate the three branches: tests, independent review and a draft note. Show the parent's comparison continuing alongside them. Reveal the fork/local snippet. | “Three branches work independently. The parent keeps going and measures the behavior before and after.” |
| 48–63 s | Show the results meeting at the join. Reveal the short `ctx.join` snippet. | “The workflow brings the results together. Each result belongs to the same version of the change.” |
| 63–82 s | Show the human-decision step and `ctx.wait_event`. Briefly cut to the recorded approval command if useful. | “When the checks pass, it waits for a person's decision. Approval applies to the candidate we just inspected.” |
| 82–100 s | Resume the replay to the saved result. Show 100 documents → one page, checks and decision. | “One understandable path from an agent's proposal to a reviewed change—with the evidence ready to inspect.” |

Pause or use the step controls when explaining a snippet. Selecting a node
pauses playback at that step; use **Play** to resume. Inspect one step at a time
so its explanation has space. Keep the full patch, hashes and raw test logs
for a brief optional close-up or a companion technical video.

The replay's pacing is editorial. Do not describe animated nodes as live service
activity or use their movement as evidence of measured concurrency. In actual
Console footage, retain the real branch order; use a visible cut or “time
condensed” caption when shortening waits.

## The code worth showing

The presentation should focus on these orchestration ideas, with the relevant
excerpt next to the selected step:

- **Fork and keep working:** `await ctx.fork(...)` registers the three branches;
  `await ctx.local(...)` then compares behavior on the parent.
- **Bring evidence together:** `ctx.join(..., resume=Entry.PREPARE_REVIEW, ...)`
  checkpoints the parent and resumes after every branch is terminal.
- **Wait for a decision:** `ctx.wait_event(..., continuation=Entry.ON_DECISION, ...)`
  resumes a named entrypoint when a candidate-bound event or timeout arrives.

The small pagination patch explains the fix. The workflow snippets explain
Ledgence. Neither needs an entire source file on screen.

## Commands for the source run

The setup commands are in the README. Replace the explicit placeholders with
IDs returned by this run.

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py submit \
  --change-id pagination-film-1 --idempotency-key pagination-film-1:1
```

Once `prepare:0` succeeds, select it in Console's **Graph** or find it under
**General → Recorded work**, then copy its task ID:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py review \
  --task PREPARE_TASK_ID --output "$CHANGE_HOME/film-1-review"
```

Open `film-1-review/review.html`, inspect the evidence and copy the full candidate
SHA-256 printed by the command. Approve only that candidate:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py approve \
  --workflow WORKFLOW_ID --candidate-sha256 CANDIDATE_SHA256 \
  --event-id pagination-film-1:decision:1
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py result \
  --workflow WORKFLOW_ID --timeout 300 --output "$CHANGE_HOME/film-1-result"
```

Open `film-1-result/review.html` for the final presentation. The earlier file
remains a pending snapshot. Playback buttons do not alter workflow state.
This example uses **Send event**; the companion command supplies the bound
decision. Its application-validated event wait does not appear in Console's
**General → Approvals**, which is for durable action-approval requests.

## Keep the claims precise

- Three independent branches can overlap when worker capacity is available.
- Six predefined regression tests and the measured comparison establish these
  checks, not every possible behavior.
- Waiting releases the parent invocation slot; a healthy process may stay warm.
- Accepted results survive recovery. An interrupted external call may already
  have consumed usage and can require reconciliation.
- Approval records a decision; it does not deploy the fix.
- Codex returns structured source and reports. Ledgence runs the fixed tests
  and comparison. The example does not modify the Ledgence repository.

If the model's candidate fails a check or the reviewer requests changes, preserve
that outcome. Inspect it before starting a new intentional take. Do not relabel
failed evidence or show an approval that did not happen. Rejection, expiry and
recovery can be separate technical videos.

## Later landing integration

Use a **Watch demo** poster showing the three-branch workflow and the headline
“From a small bug to a reviewed fix.” Keep the problem description short and
lead into the animated diagram. Add captions, an accessible transcript and a
visible duration. Identify a recorded run or an illustrative replay accurately.
Keep setup commands and credentials out of the landing video.

This example provides the runnable workflow, guided presentation and recording
plan. It does not create or publish a video.
