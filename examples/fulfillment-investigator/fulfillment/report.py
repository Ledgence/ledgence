"""Immutable, verified review packets and deterministic local publication (MIT)."""
import copy
from html import escape
import json

from .agent import HYPOTHESES, TOPICS, evidence_catalog, verify_answer

CANDIDATE_SCHEMA = "ledgence.fulfillment.candidate.v1"
REPORT_SCHEMA = "ledgence.fulfillment.report.v1"


def _verified_evidence(*, store, snapshot, evidence):
    from .storage import read_json, resolve_reference
    from .data import analyze_snapshot
    if type(snapshot) is not dict or set(snapshot) != {"snapshot_id", "manifest", "database"}:
        raise ValueError("Invalid pinned snapshot")
    if snapshot["snapshot_id"] != snapshot["manifest"].get("sha256"):
        raise ValueError("Snapshot identity does not match its manifest")
    read_json(store, snapshot["manifest"])
    resolve_reference(store, snapshot["database"])
    records = list(evidence.values()) if type(evidence) is dict else evidence
    evidence_catalog(records, snapshot["snapshot_id"])
    ordered = {record["topic"]: record for record in records}
    for topic in TOPICS:
        actual = analyze_snapshot(store=store, snapshot=snapshot, topic=topic)
        # SQL evidence is regenerated from the pinned data, not vouched for by an LLM.
        if actual != ordered[topic]:
            raise ValueError("Evidence differs from the deterministic snapshot query")
    return [ordered[topic] for topic in TOPICS]


def _display(*, snapshot, evidence, answer, mode):
    if mode not in {"fixture", "codex"}:
        raise ValueError("Unknown candidate model mode")
    answer = verify_answer(answer, snapshot_id=snapshot["snapshot_id"], evidence=evidence)
    execution = answer.get("execution")
    if type(execution) is not dict or execution.get("provider") != ("scripted" if mode == "fixture" else "codex"):
        raise ValueError("Candidate provider does not match the declared mode")
    if mode == "fixture" and execution.get("simulated") is not True:
        raise ValueError("Offline output must be explicitly marked simulated")
    topics, _, claims = evidence_catalog(evidence, snapshot["snapshot_id"])
    return {"headline": answer["headline"], "summary": answer["summary"], "conclusion": answer["conclusion"],
            "claims": [claims[key] for key in answer["claim_ids"]],
            "hypotheses": [{"id": key, "text": HYPOTHESES[key]} for key in answer["hypotheses"]],
            "metrics": {topic: topics[topic]["metrics"] for topic in TOPICS}}


def build_candidate(*, store, snapshot, evidence, answer, mode):
    """Save the exact packet that a review decision will bind by digest."""
    from .storage import write_json
    evidence = _verified_evidence(store=store, snapshot=snapshot, evidence=evidence)
    display = _display(snapshot=snapshot, evidence=evidence, answer=answer, mode=mode)
    candidate = {"schema": CANDIDATE_SCHEMA, "snapshot": snapshot, "mode": mode,
                 "answer": answer, "evidence_refs": {record["topic"]: write_json(store, record) for record in evidence},
                 **display}
    return write_json(store, candidate)


def load_candidate(*, store, candidate_ref):
    """Reverify immutable bytes, snapshot SQL, citations and all displayed facts."""
    from .storage import read_json
    candidate = read_json(store, candidate_ref)
    expected_fields = {"schema", "snapshot", "mode", "answer", "evidence_refs", "headline", "summary", "conclusion", "claims", "hypotheses", "metrics"}
    if type(candidate) is not dict or set(candidate) != expected_fields or candidate["schema"] != CANDIDATE_SCHEMA:
        raise ValueError("Invalid candidate packet")
    refs = candidate["evidence_refs"]
    if type(refs) is not dict or set(refs) != set(TOPICS):
        raise ValueError("Candidate needs all four evidence artifacts")
    evidence = [read_json(store, refs[topic]) for topic in TOPICS]
    evidence = _verified_evidence(store=store, snapshot=candidate["snapshot"], evidence=evidence)
    display = _display(snapshot=candidate["snapshot"], evidence=evidence, answer=candidate["answer"], mode=candidate["mode"])
    if any(candidate[key] != value for key, value in display.items()):
        raise ValueError("Candidate display differs from verified evidence")
    return candidate


def publish_report(*, store, candidate_ref):
    """Idempotently publish exactly the approved candidate to local artifacts.

    Workflow approval binds these arguments. This callable performs no remote
    publication and does not receive replacement model text or report arguments.
    Replays produce the same immutable artifact references.
    """
    from .storage import write_bytes, write_json
    candidate = load_candidate(store=store, candidate_ref=candidate_ref)
    report = {"schema": REPORT_SCHEMA, "candidate_ref": candidate_ref, "snapshot_id": candidate["snapshot"]["snapshot_id"],
              "mode": candidate["mode"], "publication": "local-artifact",
              **{key: copy.deepcopy(candidate[key]) for key in ("headline", "summary", "conclusion", "claims", "hypotheses", "metrics", "evidence_refs")}}
    report_ref = write_json(store, report)
    html_ref = write_bytes(store, render_report(candidate, published=True).encode("utf-8"), ".html")
    return {"status": "published", "publication": "local-artifact", "snapshot_id": candidate["snapshot"]["snapshot_id"],
            "candidate_sha256": candidate_ref["sha256"], "report_ref": report_ref, "html_ref": html_ref}


def render_report(candidate, *, published=False):
    """Render a packet returned by load_candidate; never interpolate raw HTML."""
    e = lambda value: escape(str(value), quote=True)
    metrics = candidate["metrics"]
    carrier, warehouse, health, support = (metrics[topic] for topic in TOPICS)
    baseline = carrier["baseline_late"] * 100 / carrier["baseline_orders"]
    current = carrier["current_late"] * 100 / carrier["current_orders"]
    delta = current - baseline
    snapshot_id = candidate["snapshot"]["snapshot_id"]
    is_gap = candidate["conclusion"] == "data_gap"
    mode = "Simulated agent · deterministic offline fixture" if candidate["mode"] == "fixture" else "Codex agent · structured evidence review"
    status = "Published locally" if published else "Review candidate"
    title = "Data recovered" if is_gap else "Delivery change confirmed"
    evidence_sections = []
    labels = {"carrier": ("Carrier", "Delivery outcomes, compared with the baseline"),
              "warehouse": ("Warehouse", "Dispatch records and source completeness"),
              "source_health": ("Source health", "Quality checks before drawing a conclusion"),
              "support": ("Support", "Delivery questions provide a separate signal")}
    for topic in TOPICS:
        label, description = labels[topic]
        claims = "".join(f'<li>{e(claim["text"])}<code>{e(claim["id"])}</code></li>' for claim in candidate["claims"] if claim["topic"] == topic)
        reference = candidate["evidence_refs"][topic]
        evidence_sections.append(f'<article class="evidence" id="{e(topic)}"><div class="eyebrow">{e(label)}</div><h3>{e(description)}</h3><ul>{claims}</ul><details><summary>Evidence identity</summary><p>Snapshot <code>{e(snapshot_id)}</code></p><p>Artifact <code>{e(reference["sha256"])}</code></p></details></article>')
    suggestions = "".join(f'<li>{e(item["text"])}</li>' for item in candidate["hypotheses"])
    recovered = ", ".join(health.get("corrected_sources", [])) or "None required"
    # All chart widths and metric values come from validated integer SQL counts.
    chart = f'''<div class="comparison"><div><span>Baseline</span><strong>{baseline:g}%</strong></div><div class="track"><i style="width:{baseline:.4f}%"></i></div><small>{carrier['baseline_late']} of {carrier['baseline_orders']} orders arrived late</small><div><span>Current snapshot</span><strong>{current:g}%</strong></div><div class="track current"><i style="width:{current:.4f}%"></i></div><small>{carrier['current_late']} of {carrier['current_orders']} orders arrived late</small></div>'''
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<meta name="color-scheme" content="light dark"><title>{e(candidate['headline'])} · Ledgence</title>
<style>
:root{{color-scheme:light dark;--bg:light-dark(#f5f5f7,#0c0d10);--surface:light-dark(#fff,#171a21);--ink:light-dark(#202126,#f2f2f4);--muted:light-dark(#656974,#a0a3ad);--line:light-dark(#d9dce2,#30343d);--accent:light-dark(#476750,#adc1b4);--soft:light-dark(#e9ede9,#252f2b)}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}}main{{max-width:1120px;margin:auto;padding:32px 44px 64px}}header{{display:flex;align-items:center;justify-content:space-between;gap:20px;margin-bottom:72px}}.brand{{font-weight:650;font-size:20px;letter-spacing:-.7px}}.brand span{{display:inline-grid;place-items:center;border:1px solid var(--line);width:28px;height:28px;border-radius:8px;margin-right:9px;font-size:15px}}.badge{{font-size:12px;border:1px solid var(--line);border-radius:30px;padding:6px 12px;color:var(--muted)}}.eyebrow{{font-size:11px;text-transform:uppercase;letter-spacing:1.8px;font-weight:650;color:var(--muted)}}h1{{max-width:780px;font-size:clamp(38px,5.8vw,65px);line-height:1.04;letter-spacing:-2.7px;font-weight:600;margin:18px 0 24px}}.lede{{max-width:700px;font-size:18px;line-height:1.65;color:var(--muted);margin:0}}.provenance{{margin-top:22px;font-size:12px;color:var(--muted)}}.overview{{display:grid;grid-template-columns:1.3fr 1fr;gap:24px;margin:44px 0 24px}}.panel{{background:var(--surface);border:1px solid var(--line);border-radius:20px;padding:28px}}h2{{font-size:22px;font-weight:580;letter-spacing:-.55px;line-height:1.3;margin:0 0 12px}}.verdict{{color:var(--accent);font-size:12px;font-weight:600;background:var(--soft);border-radius:20px;padding:5px 10px;display:inline-block;margin-bottom:18px}}.detail{{color:var(--muted);font-size:14px;margin:0}}.facts{{margin:24px 0 0}}.facts div{{display:flex;justify-content:space-between;gap:12px;padding:9px 0;border-top:1px solid var(--line);font-size:13px}}.facts dt{{color:var(--muted)}}.facts dd{{margin:0;font-weight:550;text-align:right}}.comparison>div:not(.track){{display:flex;justify-content:space-between;font-size:13px;margin:22px 0 8px}}.comparison strong{{font-size:23px;line-height:1.1;letter-spacing:-.7px}}.track{{height:8px;background:var(--line);border-radius:9px;overflow:hidden}}.track i{{display:block;background:var(--muted);height:100%;min-width:2px;border-radius:9px}}.track.current i{{background:var(--accent)}}small{{display:block;font-size:11px;color:var(--muted);margin-top:6px}}.section-title{{display:flex;justify-content:space-between;align-items:baseline;gap:16px;margin:60px 0 18px}}.section-title p{{margin:0;color:var(--muted);font-size:13px}}.evidence-grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}.evidence{{padding:25px;border:1px solid var(--line);border-radius:18px;background:var(--surface);min-width:0}}h3{{font-weight:550;font-size:18px;line-height:1.35;letter-spacing:-.35px;margin:9px 0 20px}}ul{{margin:0;padding-left:18px}}li{{margin:12px 0;font-size:14px}}li code{{display:block;color:var(--muted);font-size:10px;margin-top:5px}}code{{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;overflow-wrap:anywhere;font-size:11px}}details{{margin-top:20px;color:var(--muted);font-size:12px}}summary{{cursor:pointer}}details p{{margin:10px 0}}details code{{display:block}}.next{{margin-top:24px}}.next p{{font-size:13px;color:var(--muted)}}footer{{font-size:11px;color:var(--muted);margin-top:44px}}footer p{{margin:6px 0}}footer code{{font-size:10px}}@media(max-width:720px){{main{{padding:24px 22px 44px}}header{{margin-bottom:46px}}.overview,.evidence-grid{{grid-template-columns:1fr}}h1{{letter-spacing:-1.7px}}.lede{{font-size:16px}}.panel,.evidence{{padding:23px}}.section-title{{display:block;margin-top:42px}}.section-title h2{{margin-bottom:6px}}.overview{{margin-top:30px}}}}@media print{{:root{{color-scheme:light}}main{{max-width:none;padding:0}}header{{margin-bottom:35px}}.panel,.evidence{{break-inside:avoid}}details{{display:none}}}}
</style></head><body><main>
<header><div class="brand"><span>L</span>Ledgence</div><span class="badge">{e(status)}</span></header>
<section><div class="eyebrow">Fulfillment investigation</div><h1>{e(candidate['headline'])}</h1><p class="lede">{e(candidate['summary'])}</p><p class="provenance">{e(mode)}<br>Synthetic example data · all factual numbers come from deterministic queries</p></section>
<section class="overview" aria-label="Verified findings"><article class="panel"><span class="verdict">{e(title)}</span><h2>Start with a complete picture.</h2><p class="detail">The source check runs before the investigation. An incomplete-source estimate cannot be presented as delivery performance.</p><dl class="facts"><div><dt>Recovered source</dt><dd>{e(recovered)}</dd></div><div><dt>Missing dispatch records</dt><dd>{e(health['missing_dispatches'])}</dd></div><div><dt>Compared orders</dt><dd>{carrier['baseline_orders']} baseline / {carrier['current_orders']} current</dd></div><div><dt>Late-rate change</dt><dd>{delta:+g} percentage points</dd></div></dl></article><article class="panel"><div class="eyebrow">Verified delivery metric</div>{chart}</article></section>
<div class="section-title"><h2>The evidence, from each angle.</h2><p>One pinned snapshot. Four independent checks.</p></div><section class="evidence-grid">{''.join(evidence_sections)}</section>
<section class="panel next"><div class="eyebrow">Suggested follow-up</div><h2>Where to look next.</h2><p>These are investigation suggestions, not established causes or instructions that have been executed.</p><ul>{suggestions or '<li>No additional investigation suggested.</li>'}</ul></section>
<footer><p>Snapshot <code>{e(snapshot_id)}</code></p><p>Ledgence coordinates source recovery, parallel checks, evidence tools and a durable publication decision. This report is a local artifact; no customer message is sent.</p></footer>
</main></body></html>'''
