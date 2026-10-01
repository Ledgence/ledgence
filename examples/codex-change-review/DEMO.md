# Recording the Ledgence change-review demo

The story: **a $100 order still pays $5 shipping. An agent proposes the fix;
Ledgence coordinates independent checks and waits for your decision.**

Target an edited video of roughly **100 seconds**. This is an editorial duration,
not a workflow latency claim. Record a real run first, then shorten waiting
periods with clear cuts. The live Codex run may take longer or return findings.
Do not show an offline fixture as live AI execution.

## Prepare the recording

1. Complete the [README setup](README.md) with the current application packages
   `1.1.0`, a control worker with one slot and an agent worker with two slots.
2. Pass the offline gate. For the recorded run, use the real authenticated
   Codex executable, not `tests/fake_codex.py` or an acceptance wrapper.
3. Enable Console, select the demo workflow and use the readable dark theme.
   Collapse the sidebar and expand the graph when showing execution. Keep one
   graph level visible; enter a branch briefly, then return via the breadcrumb.
4. Prepare two browser views: Console and the exported `review.html`. Use a
   consistent 1440 × 900 or 1920 × 1080 recording frame with readable labels.
5. Leave optional GitHub publication unconfigured. The demo ends at an approved
   evidence bundle. No merge, deployment or customer communication is needed.
6. Keep terminals free of credentials and unrelated history. Record only the
   synthetic example, the scoped workflow, the decision command and its result.

Keep the full original capture alongside the edited video. Start with a fresh
change ID and idempotency key for each intentional take; save the returned
workflow ID. Repeating the same key and arguments reconciles that run rather
than creating a new one.

## Shot list

| Edited time | Show | Narration |
| --- | --- | --- |
| 0–10 s | The issue: $100 order → $5 shipping. The original source has `> 10000`. | “An order hits a hundred dollars, but it still pays for shipping.” |
| 10–23 s | Submit the change, then show the implementation task in Console. Briefly reveal the candidate patch. | “Codex proposes a small fix. Ledgence keeps that exact version attached to everything that happens next.” |
| 23–44 s | The root graph: three fork branches for tests, independent review and a release note. Show `compare:0` continuing on the parent. | “Tests, a second review and a draft note can run independently. Meanwhile, the workflow checks what actually changes for the customer.” |
| 44–56 s | Enter one branch, inspect its result, then return to the parent. | “Each branch has its own execution history. Every result refers to the same candidate.” |
| 56–70 s | Branches joined, `prepare:0` succeeded, parent waiting for approval. Open the pending review report. | “The evidence comes together. Now the workflow waits for a person to decide.” |
| 70–85 s | The measured $5 → $0 change, exact one-line patch, six tests and independent review. Send the bound approval command. | “I can inspect the result before I approve this specific change.” |
| 85–100 s | Resume in Console, then open the final report with its Approved badge and recorded decision. | “The workflow resumes with the same evidence. One visible path from an agent's proposal to a reviewed, approved change.” |

The timestamps describe the edit. Branch scheduling and node arrival should
remain from the same real run. Do not animate fabricated graph nodes or imply
that every branch starts at exactly the same instant. If elapsed time is
compressed, use a cut or a small “time condensed” caption.

## Commands during the take

The setup commands live in the README. These are the only operations that need
to appear during the demonstration. Replace the explicit placeholders with the
IDs from this run.

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py submit \
  --change-id shipping-film-1 --idempotency-key shipping-film-1:1
```

Once `prepare:0` succeeds, copy its task ID from Console's Children tab:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py review \
  --task PREPARE_TASK_ID --output "$CHANGE_HOME/film-1-review"
```

Open `film-1-review/review.html`, inspect it and copy the returned candidate SHA:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py approve \
  --workflow WORKFLOW_ID --candidate-sha256 CANDIDATE_SHA256 \
  --event-id shipping-film-1:decision:1
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py result \
  --workflow WORKFLOW_ID --timeout 300 --output "$CHANGE_HOME/film-1-result"
```

Open `film-1-result/review.html` for the final frame. The pending report is a
snapshot and does not update itself. The Console action is generic **Send event**;
there is no dedicated Approve button in the product today. Show the actual
companion command rather than inventing a product control.

## Keep the claims precise

- Say **“three independent branches”**; overlap depends on available worker slots.
- Say **“six predefined regression tests”**; passing them is not a proof of all
  possible behavior.
- Say **“waits durably and resumes”**; waiting releases the invocation slot, while
  a healthy Python process can remain warm.
- Say **“recorded results survive recovery”**; interrupted external calls may
  already have consumed usage and can require reconciliation.
- Say **“approved candidate”**, not “deployed fix.” Nothing ships by default.
- Codex generates structured source and reports in this example. Ledgence runs
  the actual fixed test suite and comparison. It is not an autonomous agent
  editing the Ledgence repository.

If Codex proposes an invalid fix or requests changes, preserve that result. For
a positive demonstration, inspect the reason and start a new intentional take
with a fresh key; do not alter the stored evidence or relabel a failed check.
Rejection, expiry and worker recovery are useful follow-up videos, but would
make this first story harder to follow.

## Later landing integration

Capture and edit the real run before adding a **Watch demo** link. Use a poster
with the measured $5 → $0 result and a small crop of the three-branch graph. Add
captions, an accessible transcript and a visible playback duration. Avoid
placing infrastructure setup or credential screens in the landing video.
The implementation here provides the runnable workflow and report; it does not
create or publish a video.
