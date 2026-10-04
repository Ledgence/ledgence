"""Snapshot-bound report verification, escaping, and idempotent publication (MIT)."""
import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fulfillment.agent import TOPICS, TOOLS, model_turn
from fulfillment.data import analyze_snapshot, assemble_snapshot, ingest_source, prepare_data
from fulfillment.fixtures import SOURCES
from fulfillment.report import build_candidate, load_candidate, publish_report, render_report
from fulfillment.storage import read_bytes, read_json, store_path, write_json


class ReportTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="fulfillment-report-")
        self.addCleanup(directory.cleanup)
        self.store = directory.name
        prepare_data(self.store)

    def packet(self, scenario="real-delay"):
        receipts = {source: ingest_source(store=self.store, scenario=scenario, source=source,
                    revision="corrected" if scenario == "data-gap" and source == "warehouse" else "initial") for source in SOURCES}
        snapshot = assemble_snapshot(store=self.store, scenario=scenario, receipts=receipts)["snapshot"]
        evidence = [analyze_snapshot(store=self.store, snapshot=snapshot, topic=topic) for topic in TOPICS]
        answer = asyncio.run(model_turn(mode="fixture", model="unused", snapshot_id=snapshot["snapshot_id"],
                                      messages=[{"role": "tool", "content": record} for record in evidence], tools=TOOLS))
        ref = build_candidate(store=self.store, snapshot=snapshot, evidence=evidence, answer=answer, mode="fixture")
        return ref, snapshot, evidence, answer

    def test_both_scenarios_have_snapshot_bound_report_facts(self):
        for scenario, expected in (("data-gap", "data_gap"), ("real-delay", "delivery_delay")):
            ref, snapshot, _, _ = self.packet(scenario)
            candidate = load_candidate(store=self.store, candidate_ref=ref)
            self.assertEqual(candidate["conclusion"], expected)
            self.assertEqual(candidate["snapshot"], snapshot)
            self.assertLess(ref["bytes"], 32 * 1024)
            html = render_report(candidate)
            self.assertIn("Simulated agent", html)
            self.assertIn("Review candidate", html)
            self.assertIn(snapshot["snapshot_id"], html)
            self.assertIn("Four independent checks", html)
            self.assertNotIn("<script", html)

    def test_repeated_and_parallel_publication_reconcile_exact_bytes(self):
        ref, _, _, _ = self.packet()
        with ThreadPoolExecutor(max_workers=3) as executor:
            receipts = list(executor.map(lambda _: publish_report(store=self.store, candidate_ref=ref), range(3)))
        self.assertEqual(receipts[0], receipts[1])
        self.assertEqual(receipts[1], receipts[2])
        report = read_json(self.store, receipts[0]["report_ref"])
        html = read_bytes(self.store, receipts[0]["html_ref"]).decode()
        self.assertEqual(report["candidate_ref"], ref)
        self.assertEqual(report["publication"], "local-artifact")
        self.assertIn("Published locally", html)
        self.assertEqual(list(Path(self.store).rglob(".pending-*")), [])

    def test_candidate_bytes_tampering_prevents_publication(self):
        ref, _, _, _ = self.packet()
        path = store_path(self.store, ref["path"])
        path.write_bytes(path.read_bytes() + b"tampered")
        with self.assertRaisesRegex(ValueError, "digest or length"):
            publish_report(store=self.store, candidate_ref=ref)
        self.assertEqual(list(Path(self.store).rglob("*.html")), [])

    def test_self_consistent_fabricated_evidence_is_requeried_and_rejected(self):
        ref, _, _, _ = self.packet()
        candidate = read_json(self.store, ref)
        forged = read_json(self.store, candidate["evidence_refs"]["carrier"])
        forged["claims"][0]["text"] = "Late deliveries: 99 of 100 orders."
        forged["metrics"]["current_late"] = 99
        candidate["evidence_refs"]["carrier"] = write_json(self.store, forged)
        forged_ref = write_json(self.store, candidate)
        with self.assertRaisesRegex(ValueError, "deterministic snapshot query"):
            load_candidate(store=self.store, candidate_ref=forged_ref)

    def test_altered_display_numbers_cannot_be_published(self):
        ref, _, _, _ = self.packet()
        candidate = read_json(self.store, ref)
        candidate["metrics"]["carrier"]["current_late"] = 99
        changed = write_json(self.store, candidate)
        with self.assertRaisesRegex(ValueError, "display differs"):
            publish_report(store=self.store, candidate_ref=changed)

    def test_snapshot_database_and_evidence_mismatch_rejected(self):
        ref, _, _, _ = self.packet()
        alternate, snapshot, _, _ = self.packet("data-gap")
        candidate = read_json(self.store, ref)
        candidate["snapshot"] = snapshot
        with self.assertRaisesRegex(ValueError, "different snapshot"):
            load_candidate(store=self.store, candidate_ref=write_json(self.store, candidate))
        candidate = read_json(self.store, alternate)
        database_path = store_path(self.store, candidate["snapshot"]["database"]["path"])
        database_path.write_bytes(database_path.read_bytes() + b"tamper")
        with self.assertRaisesRegex(ValueError, "digest or length"):
            load_candidate(store=self.store, candidate_ref=alternate)

    def test_missing_citations_and_fabricated_numbers_fail_before_packet_saved(self):
        _, snapshot, evidence, answer = self.packet()
        answer["summary"] = "The agent measured 99 late deliveries."
        with self.assertRaisesRegex(ValueError, "verified conclusion wording"):
            build_candidate(store=self.store, snapshot=snapshot, evidence=evidence, answer=answer, mode="fixture")

    def test_renderer_escapes_every_text_surface(self):
        ref, _, _, _ = self.packet()
        candidate = load_candidate(store=self.store, candidate_ref=ref)
        attack = '<script>alert("bad")</script>'
        candidate["headline"] = attack
        candidate["summary"] = attack
        candidate["claims"][0]["text"] = attack
        candidate["hypotheses"][0]["text"] = attack
        html = render_report(candidate)
        self.assertNotIn(attack, html)
        self.assertGreaterEqual(html.count("&lt;script&gt;"), 5)
        self.assertIn("default-src 'none'", html)

    def test_fixture_cannot_be_relabelled_real_codex(self):
        ref, _, _, _ = self.packet()
        candidate = read_json(self.store, ref)
        candidate["mode"] = "codex"
        with self.assertRaisesRegex(ValueError, "provider"):
            load_candidate(store=self.store, candidate_ref=write_json(self.store, candidate))


if __name__ == "__main__":
    unittest.main()
