"""Portable, escaped review evidence for the shipping demo (MIT)."""
from html import escape

from .steps import validate_bundle

STYLE = """
:root{color-scheme:dark;--bg:#0c0d10;--panel:#171a21;--raised:#20242c;--line:#343945;--fg:#f2f2f4;--muted:#b0b4bf;--green:#adc1b4;--amber:#debf87;--red:#e4a8aa}

*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}
main{max-width:1140px;padding:42px 40px 64px;margin:auto}
a{color:var(--fg);text-underline-offset:4px}
a:focus-visible,summary:focus-visible{outline:2px solid var(--green);outline-offset:6px;border-radius:3px}
header,.brand,.header-right,.section-head,.meta,.amounts,.file-head{display:flex;align-items:center;gap:16px}
header{justify-content:space-between;margin-bottom:68px}
.brand{font-size:19px;font-weight:650;letter-spacing:-.6px}
.mark{width:29px;height:29px;border-radius:9px;background:linear-gradient(145deg,#e7e9ed,#939daa);color:#15171d;display:grid;place-items:center;font-size:18px}
.header-right{flex-wrap:wrap;justify-content:flex-end;font-size:12px;color:var(--muted)}
.pill{border:1px solid var(--line);border-radius:999px;padding:5px 11px;font-size:12px;white-space:nowrap}
.pill.good{color:var(--green);border-color:#465950;background:#18211e}
.pill.warn{color:var(--amber);border-color:#594d38}
.pill.bad{color:var(--red);border-color:#604145}
.eyebrow{font-size:11px;letter-spacing:1.8px;text-transform:uppercase;color:var(--muted);font-weight:650}
.hero{display:grid;grid-template-columns:1.15fr 1fr;gap:54px;align-items:center;margin-bottom:44px}
h1{font-size:clamp(32px,4.3vw,51px);line-height:1.08;letter-spacing:-2px;margin:14px 0 18px;font-weight:620;max-width:600px}
p{margin:0;color:var(--muted)}
.lead{max-width:500px;font-size:17px}
.comparison{background:radial-gradient(ellipse at top right,#2d333d,transparent 75%),var(--panel);border:1px solid #3b414d;border-radius:22px;padding:28px 28px 23px;box-shadow:0 20px 60px #0003}
.comparison .eyebrow{color:#c4c8d1}
.amounts{justify-content:space-between;margin:22px 0}
.amounts>div{flex:1}
.amounts small{display:block;color:var(--muted);font-size:12px;margin-bottom:3px}
.amount{font-size:42px;font-weight:600;line-height:1.2;letter-spacing:-1.5px}
.amount.after{color:var(--green)}
.arrow{font-size:22px;color:#9097a5;padding-right:14px}
.comparison-note{border-top:1px solid #3b414d;padding-top:15px;font-size:12px;color:#c3c7d0}
.section-head{justify-content:space-between;margin:32px 0 15px}
.section-head h2{font-size:18px;font-weight:550;letter-spacing:-.4px;margin:0}
.section-head span{font-size:12px;color:var(--muted)}
.branches{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.branch{border:1px solid var(--line);border-radius:16px;padding:23px;background:var(--panel);min-width:0}
.branch-top{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:22px}
.number{font-variant-numeric:tabular-nums;color:#c0c5cf;font-size:12px}
.branch h3{font-size:18px;margin:0 0 7px;letter-spacing:-.4px}
.branch p{font-size:13px;line-height:1.65;overflow-wrap:anywhere}
.branch .origin{display:block;font-size:11px;color:#a6adb9;margin-top:18px}
.decision{margin:20px 0 30px;display:flex;align-items:center;gap:18px;padding:22px 24px;border:1px solid var(--line);border-radius:16px;background:#12151b}
.decision-icon{flex-shrink:0;display:grid;place-items:center;width:36px;height:36px;border:1px solid #4b525e;border-radius:50%;font-size:18px;color:var(--green)}
.decision h2{font-size:16px;margin:0 0 4px;font-weight:600}
.decision p{font-size:13px;max-width:780px}
.details-grid{display:grid;grid-template-columns:1.18fr 1fr;gap:20px;align-items:start}
.panel{border:1px solid var(--line);border-radius:16px;overflow:hidden;background:var(--panel);min-width:0}
.file-head{justify-content:space-between;padding:15px 20px;border-bottom:1px solid var(--line);font-size:13px}
.file-head span{color:var(--muted);font-size:11px}
.diff{margin:0;padding:18px 0;overflow-x:auto;font:12px/1.8 ui-monospace,SFMono-Regular,Consolas,monospace}
.diff-line{display:block;white-space:pre;min-width:max-content;padding:0 20px}
.diff-line.add{color:#d4e5d9;background:#24332a}
.diff-line.remove{color:#efc4c6;background:#36292e}
.diff-line.location{color:#b5c0d4}
.note{padding:22px}
.note h3{font-size:17px;margin:12px 0 9px;letter-spacing:-.3px}
.note p{font-size:14px;white-space:pre-wrap;overflow-wrap:anywhere}
.note .caption{margin-top:22px;font-size:11px;color:#b7becb}
.table-scroll{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:14px 20px;text-align:left;white-space:nowrap;border-bottom:1px solid #30353f}
th{font-size:11px;color:#b9bec9;font-weight:500}
tr:last-child td{border:0}
.cell-good{color:var(--green)}
.cell-bad{color:var(--red)}
details{border-top:1px solid var(--line)}
details:first-child{border:0}
summary{padding:17px 20px;cursor:pointer;font-size:13px;color:#e0e3e9}
details pre{margin:0;padding:0 20px 20px;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.6 ui-monospace,SFMono-Regular,Consolas,monospace;color:#b9bfca}
ul.findings{font-size:13px;padding:0 28px 18px 38px;color:var(--muted)}
.identity{margin:30px 0 0;padding-top:23px;border-top:1px solid var(--line);display:grid;grid-template-columns:1fr 1fr;gap:18px}
.identity p{font-size:11px}
.identity code{display:block;font:11px/1.7 ui-monospace,SFMono-Regular,Consolas,monospace;color:#c7ccd6;overflow-wrap:anywhere;margin-top:5px}
.downloads{display:flex;flex-wrap:wrap;gap:20px;margin:25px 0 14px;font-size:12px}
footer{font-size:11px;color:#979eac;max-width:880px}
.offline{color:var(--amber)}

@media(max-width:760px){main{padding:24px 20px 40px}
header{margin-bottom:42px}
.header-right{gap:7px}
header .eyebrow{display:none}
.hero{grid-template-columns:1fr;gap:25px;margin-bottom:26px}
h1{font-size:39px;letter-spacing:-1.5px}
.lead{font-size:15px}
.comparison{padding:24px}
.branches{grid-template-columns:1fr}
.branch{padding:19px;display:grid;grid-template-columns:1fr auto;gap:5px 20px}
.branch-top{grid-column:1/-1;margin-bottom:10px}
.branch p{grid-column:1/-1}
.branch .origin{grid-column:1/-1;margin-top:9px}
.details-grid,.identity{grid-template-columns:1fr}
.section-head{align-items:flex-start;gap:12px}
.section-head span{text-align:right;max-width:130px}
.decision{padding:20px;align-items:flex-start}
.amount{font-size:38px}
.comparison-note{font-size:11px}
}

@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto}
}

@media print{:root{color-scheme:light;--bg:white;--panel:#f5f6f8;--fg:#171a21;--muted:#414853;--line:#ccc;--green:#28543d;--red:#823236}
main{padding:20px;max-width:none}
header{margin-bottom:25px}
.comparison,.decision{background:#f5f6f8;box-shadow:none}
.comparison *, .decision *{color:#171a21!important}
.pill{color:#171a21!important}
.branch,.panel{break-inside:avoid}
.diff-line{color:#171a21!important}
.identity code,.file-head span,.number,.eyebrow{color:#414853}
details:not([open]){display:none}
.downloads{display:none}
h1{font-size:36px}
}

"""


def money(cents):
    dollars, remainder = divmod(abs(cents), 100)
    return f"{'-' if cents < 0 else ''}${dollars:,}.{remainder:02d}"


def observed(outcome):
    return money(outcome["value"]) if outcome["error"] is None else "Error"


def render_report(bundle):
    """Render one validated snapshot; no scripts, remote assets, or HTML from agents."""
    bundle = validate_bundle(bundle)
    candidate = bundle["candidate"]
    tests, review, note = (bundle[key] for key in ("tests", "review", "note"))
    status = bundle["status"]
    labels = {
        "waiting_for_approval": ("Awaiting your decision", "Ready for your decision", "The checks are complete. Inspect this candidate, then approve or reject its exact SHA-256 using the companion client. The workflow is waiting; this file is a snapshot.", "warn", "‖"),
        "approved": ("Approved", "This candidate was approved", "Ledgence recorded approval for this exact candidate. The evidence is complete; approval does not mean the change was merged or deployed.", "good", "✓"),
        "rejected": ("Rejected", "The reviewer rejected this candidate", "The human decision is recorded with the candidate identity. No publication was requested by this outcome.", "bad", "×"),
        "expired": ("Approval expired", "The approval window closed", "No decision arrived before the configured deadline. The evidence remains available; a new run can propose a new candidate.", "warn", "—"),
        "needs_changes": ("Needs changes", "The checks need attention", "Tests, the measured comparison, or independent review did not pass. This run did not request human approval or publish the candidate. Inspect the evidence below before trying again.", "bad", "!"),
    }
    label, decision_title, decision_text, tone, symbol = labels[status]
    fixture_flags = ["fixture" in value["execution"]["cli_version"] for value in (candidate, review, note)]
    if all(fixture_flags):
        execution_label = '<span class="offline">Offline fixture · no provider call</span>'
    elif any(fixture_flags):
        execution_label = '<span class="offline">Mixed execution · includes fixture output</span>'
    else:
        execution_label = '<span>Recorded execution</span>'
    at_threshold = next(row for row in bundle["comparison"]["cases"] if row["total_cents"] == 10000)
    after_ok = at_threshold["after"] == {"value": 0, "error": None}
    after_class = " after" if after_ok else ""
    test_label = f'{tests["total"] - tests["failures"] - tests["errors"]}/{tests["total"]} passed'
    review_label = "Approved by Codex" if review["verdict"] == "approve" else "Changes requested"
    note_label = "Included" if status == "approved" else "Draft"
    note_help = "Included with the approved candidate. No release was published." if status == "approved" else "Draft wording. This is not a published release note."
    lines = []
    for line in candidate["patch"].splitlines():
        kind = "add" if line.startswith("+") and not line.startswith("+++") else "remove" if line.startswith("-") and not line.startswith("---") else "location" if line.startswith("@@") else ""
        lines.append(f'<span class="diff-line {kind}">{escape(line)}</span>')
    patch = "\n".join(lines) or '<span class="diff-line">No source changes.</span>'
    rows, comparison_errors = [], []
    for row in bundle["comparison"]["cases"]:
        for side in ("before", "after"):
            if row[side]["error"] is not None:
                comparison_errors.append(f"{money(row['total_cents'])} order / {side}: {row[side]['error']}")
        correct = row["after"] == {"value": row["expected_cents"], "error": None}
        rows.append(f'<tr><td>{money(row["total_cents"])}</td><td>{escape(observed(row["before"]))}</td><td class="cell-{ "good" if correct else "bad" }">{escape(observed(row["after"]))}</td><td>{money(row["expected_cents"])}</td></tr>')
    errors = "\n".join(comparison_errors) or "Both versions returned an integer amount for all three cases."
    findings = ''.join(f'<li>{escape(item["severity"].capitalize())} · line {item["line"]}: {escape(item["message"])}</li>' for item in review["findings"])
    finding_section = f'<ul class="findings">{findings}</ul>' if findings else '<pre>No findings reported by the independent Codex session.</pre>'
    decision_id = bundle["decision"]["event_id"] if bundle["decision"] else None
    evidence = f'Workflow: {bundle["workflow_id"]}\nCandidate: {candidate["sha256"]}\nBase: {candidate["base_sha256"]}\nDecision event: {decision_id or "None"}'
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="dark light"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'"><title>Shipping change · Ledgence review</title><style>{STYLE}</style></head>
<body><main>
<header><div class="brand"><span class="mark" aria-hidden="true">L</span>Ledgence <span class="eyebrow">/ Change review</span></div><div class="header-right">{execution_label}<span class="pill {tone}">{label}</span></div></header>
<section class="hero" aria-labelledby="title"><div><div class="eyebrow">One issue. One candidate.</div><h1 id="title">Free shipping should<br>start at $100.</h1><p class="lead">The original calculator charges $5 at the threshold. Codex proposes a change; Ledgence coordinates the checks and your decision.</p></div><div class="comparison"><div class="eyebrow">Shipping on a $100 order</div><div class="amounts"><div><small>Original code</small><div class="amount">{escape(observed(at_threshold["before"]))}</div></div><span class="arrow" aria-hidden="true">→</span><div><small>This candidate</small><div class="amount{after_class}">{escape(observed(at_threshold["after"]))}</div></div></div><div class="comparison-note">Measured from both versions · expected shipping $0.00</div></div></section>
<div class="section-head"><h2>Three branches. The same candidate.</h2><span>Independent work, joined by Ledgence</span></div>
<section class="branches" aria-label="Branch results">
<article class="branch"><div class="branch-top"><span class="number">01 / Tests</span><span class="pill {'good' if tests['passed'] else 'bad'}">{test_label}</span></div><h3>Verify the behavior</h3><p>Six predefined tests check the threshold, amounts around it, and invalid inputs.</p><span class="origin">Fixed regression suite · separate branch</span></article>
<article class="branch"><div class="branch-top"><span class="number">02 / Review</span><span class="pill {'good' if review['verdict']=='approve' else 'bad'}">{review_label}</span></div><h3>Get a second look</h3><p>{escape(review['summary'])}</p><span class="origin">Fresh Codex session · separate branch</span></article>
<article class="branch"><div class="branch-top"><span class="number">03 / Release note</span><span class="pill">{note_label}</span></div><h3>Explain the change</h3><p>A short note describes this candidate while the other branches check it.</p><span class="origin">Fresh Codex session · separate branch</span></article>
</section>
<section class="decision" aria-labelledby="decision-title"><span class="decision-icon" aria-hidden="true">{symbol}</span><div><h2 id="decision-title">{decision_title}</h2><p>{decision_text}</p></div></section>
<div class="details-grid"><section class="panel" aria-label="Exact proposed patch"><div class="file-head"><strong>shipping.py</strong><span>Exact proposed patch</span></div><pre class="diff"><code>{patch}</code></pre></section><section class="panel note" aria-labelledby="note-title"><div class="eyebrow">Release note / {note_label}</div><h3 id="note-title">{escape(note['title'])}</h3><p>{escape(note['body'])}</p><p class="caption">{note_help}</p></section></div>
<div class="section-head"><h2>Check the boundary</h2><span>Measured local step after the fork</span></div>
<div class="panel table-scroll"><table><caption style="position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%)">Shipping cost measured before and after the proposed change</caption><thead><tr><th scope="col">Order total</th><th scope="col">Original shipping</th><th scope="col">Candidate shipping</th><th scope="col">Expected shipping</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<div class="section-head"><h2>Keep the evidence</h2><span>Bound to the candidate SHA-256</span></div>
<div class="panel"><details><summary>Comparison observations</summary><pre>{escape(errors)}</pre></details><details><summary>Regression test output</summary><pre>{escape(tests['output'])}</pre></details><details><summary>Independent review findings</summary>{finding_section}</details><details><summary>Execution and decision identity</summary><pre>{escape(evidence)}</pre></details></div>
<div class="identity"><div><p>Candidate SHA-256</p><code>{escape(candidate['sha256'])}</code></div><div><p>Workflow</p><code>{escape(bundle['workflow_id'])}</code></div></div>
<nav class="downloads" aria-label="Evidence files"><a href="review.json" download>Complete evidence JSON ↗</a><a href="change.patch" download>Exact patch ↗</a><a href="shipping.py" download>Candidate source ↗</a><a href="pull-request.md" download>Review description ↗</a></nav>
<footer>Snapshot of a Ledgence demo run. Passing checks and a recorded decision are evidence for this candidate, not a guarantee of correctness. Parallel branches may execute sequentially when worker capacity is limited. No live browser connection is used.</footer>
</main></body></html>'''
