"""Guided workflow playback and portable evidence for the document demo (MIT)."""
import base64
import hashlib
from html import escape
import json
from pathlib import Path

from .steps import validate_bundle

ASSETS = Path(__file__).with_name('presentation')

FORK_CODE = '''checks = await ctx.fork("checks:0", branches=branches)
comparison = await ctx.local(
    "compare:0", compare_candidate, candidate=candidate,
)
return ctx.join(
    checks, resume=Entry.PREPARE_REVIEW,
    state={"candidate": candidate, "comparison": comparison},
)'''
WAIT_CODE = '''return ctx.wait_event(
    "approval:0",
    continuation=Entry.ON_DECISION,
    state={"bundle": bundle},
    timeout_ms=data["approval_timeout_ms"],
)'''
ICONS = {
    'propose':'<path d="m4 12 7-7 3 3-7 7H4zM10 6l3 3M4 3h2M5 2v2"/>',
    'tests':'<path d="m3 8 3 3 7-7M3 14h10"/>',
    'review':'<circle cx="7" cy="7" r="4"/><path d="m10 10 4 4M5 7h4"/>',
    'note':'<path d="M4 2h6l3 3v9H4zM10 2v4h3M6 9h5M6 11h3"/>',
    'compare':'<path d="M2 5h11m-3-3 3 3-3 3M14 11H3m3-3-3 3 3 3"/>',
    'join':'<path d="M2 3h3l3 5h6M2 13h3l3-5m3-3 3 3-3 3"/>',
    'decide':'<path d="M8 2 3 4v4c0 3 5 6 5 6s5-3 5-6V4zM5 8l2 2 4-4"/>',
}


def script_json(value):
    """Embed JSON as inert text without allowing a model string to close its tag."""
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(',', ':')).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')


def render_report(value):
    bundle = validate_bundle(value)
    candidate = bundle['candidate']
    style = (ASSETS / 'style.css').read_text(encoding='utf-8')
    script = (ASSETS / 'player.js').read_text(encoding='utf-8')
    script_hash = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode('ascii')
    csp = "default-src 'none'; style-src 'unsafe-inline'; script-src 'sha256-" + script_hash + "'; base-uri 'none'; form-action 'none'"
    flags = ['fixture' in item['execution']['cli_version'] for item in (candidate, bundle['review'], bundle['note'])]
    provenance = 'Simulated agent output' if all(flags) else 'Includes simulated output' if any(flags) else 'Recorded Codex output'
    payload = {**bundle, 'snippets': {'fix':candidate['patch'] or 'No source changes.', 'fork':FORK_CODE, 'wait':WAIT_CODE}}
    node_defs = [('propose','Propose a fix',0),('tests','Run tests',2),('review','Review code',2),
                 ('note','Draft note',2),('compare','Compare behavior',2),('join','Join evidence',3),('decide','Your decision',4)]
    nodes = ''.join(f'''<button class="node" id="node-{identity}" type="button" data-node="{identity}" data-step="{step}" data-state="idle" aria-label="Inspect {title.lower()}"><span class="node-icon" aria-hidden="true"><svg viewBox="0 0 16 16">{ICONS[identity]}</svg></span><span class="node-copy"><span class="node-title">{title}</span><span class="node-meta"><i class="status-dot" aria-hidden="true"></i><span class="node-status">Waiting</span></span></span></button>''' for identity,title,step in node_defs)
    rows = []
    errors = []
    for row in bundle['comparison']['cases']:
        observed = lambda side: str(row[side]['value']) if row[side]['error'] is None else 'Error'
        rows.append(f'<tr><td>{row["item_count"]} documents</td><td>{observed("before")}</td><td>{observed("after")}</td><td>{row["expected_pages"]}</td></tr>')
        for side in ('before','after'):
            if row[side]['error']:
                errors.append(f'{row["item_count"]} documents / {side}: {row[side]["error"]}')
    findings = ''.join('<li>' + escape(f'{item["severity"]} · line {item["line"]}: {item["message"]}') + '</li>' for item in bundle['review']['findings'])
    review_details = '<ul>' + findings + '</ul>' if findings else '<p>No findings in the saved independent review.</p>'
    decision = bundle['decision']
    decision_text = 'No decision recorded.' if decision is None else f'{decision["outcome"]}; event: {decision["event_id"] or "deadline"}'
    error_text = '\n'.join(errors) if errors else 'Both versions returned a page count for each measured case.'
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="dark"><meta http-equiv="Content-Security-Policy" content="{csp}"><title>From a bug to a reviewed fix · Ledgence demo</title><style>{style}</style></head>
<body><main id="demo">
<header><div class="brand"><span class="brand-mark" aria-hidden="true">L</span>Ledgence <span class="breadcrumb">/ Workflow demo</span></div><div class="header-tools"><span><i class="mode-dot" aria-hidden="true"></i>Demo playback</span><a class="evidence-link" href="#evidence">View evidence ↗</a></div></header>
<section class="hero" aria-labelledby="demo-title"><div><div class="eyebrow">The problem</div><h1 id="demo-title">Fix an empty page<br>in search results.</h1><p class="intro">All 100 search results fit on one page. The app still offers an empty second page.</p></div><div class="solution"><div class="eyebrow">The solution</div><p><strong>Codex proposes the fix.</strong> Ledgence checks it in parallel, brings the evidence together, and waits for your decision.</p><div class="outcome"><span>The target</span><span class="result-chip">100 documents → 1 page</span></div></div></section>
<section class="workspace" aria-label="Animated workflow demo">
<div class="workspace-bar"><div class="workspace-name">Document search <span id="entrypoint">start</span></div><div class="playback-tools" aria-label="Playback controls"><button class="icon-button" id="previous" type="button" aria-label="Previous step">←</button><button class="play-button" id="play-pause" type="button" aria-label="Pause demo playback">Ⅱ Pause</button><button class="icon-button" id="next" type="button" aria-label="Next step">→</button><button class="icon-button" id="restart" type="button" aria-label="Restart demo playback">↺</button></div></div>
<div class="graph-shell"><div class="graph" id="workflow"><svg class="connections" id="connections" aria-hidden="true"></svg>{nodes}<div class="graph-note">Three independent branches.<br>One shared candidate.</div></div><div class="legend"><span><i class="dashed" aria-hidden="true"></i>Forked branch</span><span><i aria-hidden="true"></i>Parent workflow</span><span class="speed-note">{provenance} · illustrative timing, not a live connection</span></div></div>
<div class="focus"><div class="focus-story"><div class="step-label" id="step-label">01 / 06 · Demo playback</div><h2 id="focus-title">An agent proposes the fix</h2><p id="focus-body">Codex receives the bug and the small module. Ledgence tracks the candidate through every check.</p><div class="fact"><span class="fact-dot" aria-hidden="true"></span><span id="focus-fact">One candidate shared across three branches</span></div></div><section class="code-pane" aria-label="Relevant workflow code"><div class="code-toolbar" role="group" aria-label="Code examples"><button class="code-tab" data-code="fix" aria-pressed="true" aria-controls="code" type="button">The fix</button><button class="code-tab" data-code="fork" aria-pressed="false" aria-controls="code" type="button">Fork &amp; join</button><button class="code-tab" data-code="wait" aria-pressed="false" aria-controls="code" type="button">Wait &amp; resume</button></div><pre tabindex="0" aria-label="Code excerpt"><code id="code">{escape(payload['snippets']['fix'])}</code></pre><p class="code-note" id="code-note">The actual candidate diff in this saved run.</p></section></div>
<noscript><p class="no-script">Playback needs JavaScript. The workflow diagram, candidate diff and complete evidence below remain available.</p></noscript></section>
<div class="closing"><span>Click a node to inspect a step. Playback pauses while you explore.</span><span>This page never submits work or sends approval.</span></div>
<details class="evidence" id="evidence"><summary>Saved execution evidence · {escape(bundle['status'].replace('_',' '))}</summary><div class="evidence-content"><div><h3>Measured page counts</h3><table><thead><tr><th scope="col">Search results</th><th scope="col">Before</th><th scope="col">Candidate</th><th scope="col">Expected</th></tr></thead><tbody>{''.join(rows)}</tbody></table><p>{escape(error_text)}</p></div><div><h3>Fixed regression suite</h3><pre>{escape(bundle['tests']['output'])}</pre></div><div><h3>Independent review</h3><p>{escape(bundle['review']['summary'])}</p>{review_details}</div><div><h3>Draft release note</h3><p>{escape(bundle['note']['title'])}</p><p>{escape(bundle['note']['body'])}</p></div><div><h3>Candidate and decision</h3><pre>Workflow: {escape(bundle['workflow_id'])}\nCandidate SHA-256: {escape(candidate['sha256'])}\nDecision: {escape(decision_text)}</pre></div><nav class="evidence-links" aria-label="Evidence files"><a href="review.json" download>Evidence JSON ↗</a><a href="change.patch" download>Exact patch ↗</a><a href="pagination.py" download>Candidate source ↗</a><a href="pull-request.md" download>Review description ↗</a></nav></div></details>
</main><script id="demo-data" type="application/json">{script_json(payload)}</script><script>{script}</script></body></html>'''
